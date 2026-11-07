package com.flyingzc.azir.gc;

import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.tso.TimestampOracle;

import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * MVCC 历史版本垃圾回收管理器。
 */
public final class GcManager {
    private final TransactionMetadataStore metadataStore;
    private final TimestampOracle timestampOracle;
    private final SafePointStore safePointStore;
    private final Map<String, StorageAdapter> adapters;
    private final int metadataBatchSize;
    private final long finalizedRetentionMillis;
    private final long timeBudgetNanos;
    private final LongSupplier monotonicClock;
    private int workCursor;

    /**
     * 构造 GcManager 实例。
     */
    public GcManager(TransactionMetadataStore metadataStore,
                     TimestampOracle timestampOracle,
                     SafePointStore safePointStore,
                     Map<String, StorageAdapter> adapters) {
        this(metadataStore, timestampOracle, safePointStore, adapters, 256, 60_000, 1000);
    }

    /** 配置租约及元数据清理批次、终态保留毫秒数和合作式单轮时间预算。 */
    public GcManager(TransactionMetadataStore metadataStore, TimestampOracle timestampOracle,
                     SafePointStore safePointStore, Map<String, StorageAdapter> adapters,
                     int metadataBatchSize, long finalizedRetentionMillis, long timeBudgetMillis) {
        this(metadataStore, timestampOracle, safePointStore, adapters, metadataBatchSize,
                finalizedRetentionMillis, timeBudgetMillis, System::nanoTime);
    }

    /** 注入单调纳秒时钟以定义合作式预算，与租约使用的权威墙钟相互独立。 */
    public GcManager(TransactionMetadataStore metadataStore, TimestampOracle timestampOracle,
                     SafePointStore safePointStore, Map<String, StorageAdapter> adapters,
                     int metadataBatchSize, long finalizedRetentionMillis, long timeBudgetMillis, LongSupplier monotonicClock) {
        if (metadataBatchSize < 1 || finalizedRetentionMillis < 0 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid GC limits");
        }
        this.metadataStore = metadataStore;
        this.timestampOracle = timestampOracle;
        this.safePointStore = safePointStore;
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.finalizedRetentionMillis = finalizedRetentionMillis;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /**
     * 根据活跃事务推进安全点。
     */
    public long advanceSafePoint() {
        return metadataStore.withPublicationLock(() -> {
            metadataStore.assertNoPendingPublications();
            // 过期租约删除、读取保护下界和推进安全点与 begin/register 共用发布屏障。
            metadataStore.expireSnapshots(metadataStore.currentTimeMillis(), metadataBatchSize);
            long currentSafePoint = safePointStore.currentSafePoint();
            long candidate = metadataStore.oldestActiveStartTs()
                    .map(oldest -> Math.max(currentSafePoint, oldest > 0 ? oldest - 1 : 0))
                    .orElseGet(() -> Math.max(currentSafePoint, timestampOracle.nextTimestamp() - 1));
            return safePointStore.advanceTo(candidate);
        });
    }

    /**
     * 执行一次历史版本垃圾回收。
     */
    public synchronized int gcOnce(int limitPerShard) {
        if (limitPerShard < 1) {
            throw new IllegalArgumentException("limitPerShard must be positive");
        }
        long safePoint = advanceSafePoint();
        List<String> shards = adapters.keySet().stream().sorted().toList();
        int workCount = shards.size() + 1;
        long started = monotonicClock.getAsLong();
        int reclaimed = 0;
        for (int attempted = 0; attempted < workCount
                && (attempted == 0 || monotonicClock.getAsLong() - started < timeBudgetNanos); attempted++) {
            // 清理元数据和各分片使用同一轮转游标；即使上次失败或耗尽预算，下轮也推进到下一项。
            int work = workCursor;
            workCursor = (workCursor + 1) % workCount;
            if (work < shards.size()) {
                reclaimed += adapters.get(shards.get(work)).gc(safePoint, limitPerShard);
            } else {
                metadataStore.withPublicationLock(() -> {
                    long now = metadataStore.currentTimeMillis();
                    long cutoff = now < finalizedRetentionMillis ? 0 : now - finalizedRetentionMillis;
                    return metadataStore.purgeFinalized(safePoint, cutoff, metadataBatchSize);
                });
            }
        }
        // 保留旧返回值语义：只统计参与者回收量，不混入租约或中心元数据行数。
        return reclaimed;
    }
}

package mvcc.recovery;

import mvcc.storage.StorageAdapter;
import mvcc.metadata.TxRecord;
import mvcc.common.BackgroundTaskStats;

import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 后台恢复执行器，按分片扫描并恢复悬挂事务。
 */
public final class BackgroundRecoveryRunner {
    private final RecoveryManager recoveryManager;
    private final Map<String, StorageAdapter> adapters;
    private final int metadataBatchSize;
    private final int maxTransactionsPerRun;
    private final long timeBudgetNanos;
    private final BackgroundTaskStats transactionStats = new BackgroundTaskStats();
    private final BackgroundTaskStats scanStats = new BackgroundTaskStats();
    private final Map<Long, Set<String>> pending = new LinkedHashMap<>();
    private long metadataCursor;
    private int shardCursor;

    /**
     * 构造 BackgroundRecoveryRunner 实例。
     */
    public BackgroundRecoveryRunner(RecoveryManager recoveryManager, Map<String, StorageAdapter> adapters) {
        this(recoveryManager, adapters, 128, 256, 1000);
    }

    /** 配置元数据批次、单轮事务上限和合作式时间预算，保留原构造器。 */
    public BackgroundRecoveryRunner(RecoveryManager recoveryManager, Map<String, StorageAdapter> adapters,
                                    int metadataBatchSize, int maxTransactionsPerRun, long timeBudgetMillis) {
        if (metadataBatchSize < 1 || maxTransactionsPerRun < 1 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("recovery limits must be positive");
        }
        this.recoveryManager = recoveryManager;
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.maxTransactionsPerRun = maxTransactionsPerRun;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
    }

    /**
     * 执行一轮悬挂事务恢复。
     */
    public synchronized int recoverOnce(int perShardLimit) {
        if (perShardLimit < 1) {
            throw new IllegalArgumentException("perShardLimit must be positive");
        }
        long started = System.nanoTime();
        if (pending.isEmpty()) {
            discoverMetadata();
            discoverIntents(perShardLimit, started);
        }
        int recovered = 0;
        int attempted = 0;
        var iterator = pending.entrySet().iterator();
        while (iterator.hasNext() && attempted < maxTransactionsPerRun && hasBudget(started)) {
            var entry = iterator.next();
            try {
                boolean completed = recoveryManager.recover(entry.getKey(), entry.getValue());
                transactionStats.recordSuccess(completed ? 1 : 0);
                recovered += completed ? 1 : 0;
            } catch (RuntimeException ex) {
                // 单事务错误不能阻断同批其他事务，失败项在下一轮游标扫描时重试。
                transactionStats.recordFailure();
            }
            iterator.remove();
            attempted++;
        }
        return recovered;
    }

    /** 持续前移元数据游标，避免坏事务或没有 intent 的单分片提交饿死后续恢复。 */
    private void discoverMetadata() {
        try {
            List<TxRecord> candidates = recoveryManager.findRecoveryCandidates(metadataCursor,
                    Math.min(metadataBatchSize, maxTransactionsPerRun));
            for (TxRecord candidate : candidates) {
                pending.computeIfAbsent(candidate.txId(), ignored -> new LinkedHashSet<>()).addAll(candidate.participants());
                metadataCursor = candidate.txId();
            }
            if (candidates.size() < Math.min(metadataBatchSize, maxTransactionsPerRun)) {
                metadataCursor = 0;
            }
            scanStats.recordSuccess(candidates.size());
        } catch (RuntimeException ex) {
            scanStats.recordFailure();
        }
    }

    /** 轮转扫描分片并保留实际发现位置；有界队列中的未处理项跨轮保留。 */
    private void discoverIntents(int perShardLimit, long started) {
        List<String> shards = new ArrayList<>(adapters.keySet());
        shards.sort(String::compareTo);
        for (int scanned = 0; scanned < shards.size() && hasBudget(started); scanned++) {
            String shard = shards.get(shardCursor++ % shards.size());
            shardCursor %= shards.size();
            try {
                Set<Long> ids = adapters.get(shard).findPendingTransactionIds(Math.min(perShardLimit, maxTransactionsPerRun));
                for (long txId : ids) {
                    if (pending.containsKey(txId) || pending.size() < maxTransactionsPerRun) {
                        pending.computeIfAbsent(txId, ignored -> new LinkedHashSet<>()).add(shard);
                    }
                }
                scanStats.recordSuccess(ids.size());
            } catch (RuntimeException ex) {
                scanStats.recordFailure();
            }
        }
    }

    /** 检查批次间时间预算；单次 JDBC 调用仍由驱动连接和查询超时限制。 */
    private boolean hasBudget(long started) {
        return System.nanoTime() - started < timeBudgetNanos;
    }

    /** 返回单事务失败和完成统计。 */
    public BackgroundTaskStats transactionStats() {
        return transactionStats;
    }

    /** 返回发现批次的失败和处理统计。 */
    public BackgroundTaskStats scanStats() {
        return scanStats;
    }
}

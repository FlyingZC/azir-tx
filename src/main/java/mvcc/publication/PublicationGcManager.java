package mvcc.publication;

import mvcc.metadata.TransactionMetadataStore;
import mvcc.publication.mysql.MysqlPublicationLedger;
import mvcc.storage.StorageAdapter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/** 新协议的有界 GC 编排器，使用账本安全点回收版本和终态记录。 */
public final class PublicationGcManager {
    private final MysqlPublicationLedger ledger;
    private final TransactionMetadataStore metadataStore;
    private final Map<String, StorageAdapter> adapters;
    private final int metadataBatchSize;
    private final long retentionMillis;
    private final long timeBudgetNanos;
    private final LongSupplier monotonicClock;
    private int workCursor;
    private Long cycleSafePoint;

    /** 创建默认批次和保留窗口的账本 GC 管理器。 */
    public PublicationGcManager(MysqlPublicationLedger ledger, TransactionMetadataStore metadataStore,
                                Map<String, StorageAdapter> adapters) {
        this(ledger, metadataStore, adapters, 256, 60_000, 1000, System::nanoTime);
    }

    /** 配置可观测的批次上限、终态保留时间和合作式执行预算。 */
    public PublicationGcManager(MysqlPublicationLedger ledger, TransactionMetadataStore metadataStore,
                                Map<String, StorageAdapter> adapters, int metadataBatchSize,
                                long retentionMillis, long timeBudgetMillis, LongSupplier monotonicClock) {
        if (metadataBatchSize < 1 || metadataBatchSize > 4096 || retentionMillis < 0 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid publication GC limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.retentionMillis = retentionMillis;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /** 每轮先建立新的安全点，再轮转回收分片、中心终态和账本终态。 */
    public synchronized int gcOnce(int limitPerShard) {
        if (limitPerShard < 1) {
            throw new IllegalArgumentException("limitPerShard must be positive");
        }
        ledger.expireSnapshots(metadataBatchSize);
        ledger.expireCoordinatorLeases(metadataBatchSize);
        // 一个周期跨多轮执行时沿用同一目标；分片本地栅栏全部完成前不得按更高水位回收中心。
        if (cycleSafePoint == null) {
            cycleSafePoint = ledger.advanceSafePoint();
        }
        long safePoint = cycleSafePoint;
        List<String> shards = adapters.keySet().stream().sorted().toList();
        int workCount = shards.size() + 2;
        long started = monotonicClock.getAsLong();
        int reclaimed = 0;
        for (int attempted = 0; attempted < workCount
                && (attempted == 0 || monotonicClock.getAsLong() - started < timeBudgetNanos); attempted++) {
            int work = workCursor;
            if (work < shards.size()) {
                reclaimed += adapters.get(shards.get(work)).gc(safePoint, limitPerShard);
            } else {
                long now = metadataStore.currentTimeMillis();
                long cutoff = now < retentionMillis ? 0 : now - retentionMillis;
                if (work == shards.size()) {
                    metadataStore.purgeFinalized(safePoint, cutoff, metadataBatchSize);
                } else {
                    ledger.purgeTerminal(safePoint, cutoff, metadataBatchSize);
                }
            }
            // 失败时游标和目标不变；全部阶段完成后下一轮才读取更高全局安全点。
            workCursor = (workCursor + 1) % workCount;
            if (workCursor == 0) {
                cycleSafePoint = null;
                break;
            }
        }
        return reclaimed;
    }

    /** 返回与参与者 GC 使用相同的持久化安全点。 */
    public long currentSafePoint() {
        return ledger.currentSafePoint();
    }
}

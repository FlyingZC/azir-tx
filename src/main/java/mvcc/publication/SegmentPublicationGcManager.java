package mvcc.publication;

import mvcc.metadata.TransactionMetadataStore;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger;
import mvcc.storage.StorageAdapter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/** 用同一持久安全点轮转回收分片版本、分段槽、中心终态与 owner 记录。 */
public final class SegmentPublicationGcManager {
    private final MysqlSegmentPublicationLedger ledger;
    private final TransactionMetadataStore metadataStore;
    private final Map<String, StorageAdapter> adapters;
    private final int metadataBatchSize;
    private final long retentionMillis;
    private final long timeBudgetNanos;
    private final LongSupplier monotonicClock;
    private int workCursor;
    private Long cycleSafePoint;

    /** 创建默认批次、保留窗口和合作式时间预算。 */
    public SegmentPublicationGcManager(MysqlSegmentPublicationLedger ledger,
                                       TransactionMetadataStore metadataStore,
                                       Map<String, StorageAdapter> adapters) {
        this(ledger, metadataStore, adapters, 256, 60_000, 1_000, System::nanoTime);
    }

    /** 对长期运行配置有限批次和每轮后台预算。 */
    public SegmentPublicationGcManager(MysqlSegmentPublicationLedger ledger,
                                       TransactionMetadataStore metadataStore,
                                       Map<String, StorageAdapter> adapters,
                                       int metadataBatchSize, long retentionMillis,
                                       long timeBudgetMillis, LongSupplier monotonicClock) {
        if (metadataBatchSize < 1 || metadataBatchSize > 4096
                || retentionMillis < 0 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid segmented GC limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.retentionMillis = retentionMillis;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /** 先清理到期租约，再按分片、槽、中心、owner 的顺序使用同一安全点。 */
    public synchronized int gcOnce(int limitPerShard) {
        if (limitPerShard < 1 || limitPerShard > 4096) {
            throw new IllegalArgumentException("invalid segmented shard GC limit");
        }
        ledger.expireSnapshots(metadataBatchSize);
        ledger.expireCoordinatorLeases(metadataBatchSize);
        if (cycleSafePoint == null) {
            cycleSafePoint = ledger.advanceSafePoint();
        }
        long safePoint = cycleSafePoint;
        List<String> shards = adapters.keySet().stream().sorted().toList();
        int workCount = shards.size() + 3;
        long started = monotonicClock.getAsLong();
        int reclaimed = 0;
        for (int attempted = 0; attempted < workCount
                && (attempted == 0 || monotonicClock.getAsLong() - started < timeBudgetNanos); attempted++) {
            int stage = workCursor;
            if (stage < shards.size()) {
                reclaimed += adapters.get(shards.get(stage)).gc(safePoint, limitPerShard);
            } else {
                long now = metadataStore.currentTimeMillis();
                long cutoff = now < retentionMillis ? 0 : now - retentionMillis;
                if (stage == shards.size()) {
                    reclaimed += ledger.purgeTerminalSlots(safePoint, cutoff, metadataBatchSize);
                } else if (stage == shards.size() + 1) {
                    reclaimed += ledger.purgeFinalizedMetadata(safePoint, cutoff, metadataBatchSize);
                } else {
                    reclaimed += ledger.purgeSealedSegments(safePoint, cutoff, metadataBatchSize);
                }
            }
            // 失败保留阶段和目标安全点；全阶段完成后下一轮才读取更高水位。
            workCursor = (workCursor + 1) % workCount;
            if (workCursor == 0) {
                cycleSafePoint = null;
                break;
            }
        }
        return reclaimed;
    }

    /** 返回分片与中心回收共同遵守的持久安全点。 */
    public long currentSafePoint() {
        return ledger.currentSafePoint();
    }
}

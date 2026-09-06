package mvcc.gc;

import mvcc.metadata.TransactionMetadataStore;
import mvcc.storage.StorageAdapter;
import mvcc.tso.TimestampOracle;

import java.util.Map;

/**
 * MVCC 历史版本垃圾回收管理器。
 */
public final class GcManager {
    private final TransactionMetadataStore metadataStore;
    private final TimestampOracle timestampOracle;
    private final SafePointStore safePointStore;
    private final Map<String, StorageAdapter> adapters;

    /**
     * 构造 GcManager 实例。
     */
    public GcManager(TransactionMetadataStore metadataStore,
                     TimestampOracle timestampOracle,
                     SafePointStore safePointStore,
                     Map<String, StorageAdapter> adapters) {
        this.metadataStore = metadataStore;
        this.timestampOracle = timestampOracle;
        this.safePointStore = safePointStore;
        this.adapters = Map.copyOf(adapters);
    }

    /**
     * 根据活跃事务推进安全点。
     */
    public long advanceSafePoint() {
        long currentSafePoint = safePointStore.currentSafePoint();
        long candidate = metadataStore.oldestActiveStartTs()
                .map(oldest -> Math.max(currentSafePoint, oldest - 1))
                .orElseGet(() -> Math.max(currentSafePoint, timestampOracle.nextTimestamp() - 1));
        return safePointStore.advanceTo(candidate);
    }

    /**
     * 执行一次历史版本垃圾回收。
     */
    public int gcOnce(int limitPerShard) {
        long safePoint = advanceSafePoint();
        int reclaimed = 0;
        for (StorageAdapter adapter : adapters.values()) {
            reclaimed += adapter.gc(safePoint, limitPerShard);
        }
        return reclaimed;
    }
}

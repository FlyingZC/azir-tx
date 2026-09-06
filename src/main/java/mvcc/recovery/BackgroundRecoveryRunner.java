package mvcc.recovery;

import mvcc.storage.StorageAdapter;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 后台恢复执行器，按分片扫描并恢复悬挂事务。
 */
public final class BackgroundRecoveryRunner {
    private final RecoveryManager recoveryManager;
    private final Map<String, StorageAdapter> adapters;

    /**
     * 构造 BackgroundRecoveryRunner 实例。
     */
    public BackgroundRecoveryRunner(RecoveryManager recoveryManager, Map<String, StorageAdapter> adapters) {
        this.recoveryManager = recoveryManager;
        this.adapters = Map.copyOf(adapters);
    }

    /**
     * 执行一轮悬挂事务恢复。
     */
    public int recoverOnce(int perShardLimit) {
        Set<Long> pendingTxIds = new LinkedHashSet<>();
        for (StorageAdapter adapter : adapters.values()) {
            pendingTxIds.addAll(adapter.findPendingTransactionIds(perShardLimit));
        }
        for (Long txId : pendingTxIds) {
            recoveryManager.recover(txId);
        }
        return pendingTxIds.size();
    }
}

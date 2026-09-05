package mvcc.recovery;

import mvcc.storage.StorageAdapter;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class BackgroundRecoveryRunner {
    private final RecoveryManager recoveryManager;
    private final Map<String, StorageAdapter> adapters;

    public BackgroundRecoveryRunner(RecoveryManager recoveryManager, Map<String, StorageAdapter> adapters) {
        this.recoveryManager = recoveryManager;
        this.adapters = Map.copyOf(adapters);
    }

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

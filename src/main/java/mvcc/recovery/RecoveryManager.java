package mvcc.recovery;

import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.storage.StorageAdapter;

import java.util.Map;

/**
 * 事务恢复管理器，按事务元数据补偿未完成提交或回滚。
 */
public final class RecoveryManager {
    private final TransactionMetadataStore metadataStore;
    private final Map<String, StorageAdapter> adapters;

    /**
     * 构造 RecoveryManager 实例。
     */
    public RecoveryManager(TransactionMetadataStore metadataStore, Map<String, StorageAdapter> adapters) {
        this.metadataStore = metadataStore;
        this.adapters = Map.copyOf(adapters);
    }

    /**
     * 恢复指定事务的未完成状态。
     */
    public void recover(long txId) {
        TxRecord record = metadataStore.get(txId)
                .orElseThrow(() -> new IllegalArgumentException("transaction not found: " + txId));
        if (record.status() == TxStatus.COMMITTED) {
            for (String participant : record.participants()) {
                adapters.get(participant).commit(txId, record.commitTs());
            }
            return;
        }

        if (record.status() == TxStatus.ABORTED || record.status() == TxStatus.PREPARING || record.status() == TxStatus.ACTIVE) {
            metadataStore.markAborted(txId);
            for (String participant : record.participants()) {
                adapters.get(participant).abort(txId);
            }
        }
    }
}

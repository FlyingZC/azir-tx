package mvcc.recovery;

import mvcc.common.TxStatus;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.storage.StorageAdapter;
import mvcc.storage.ParticipantRecord;

import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import static mvcc.metadata.TransactionMetadataValidation.validateParticipant;

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
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        // 先校验所有路由，再绑定权威，避免非法映射造成部分适配器已被初始化。
        this.adapters.forEach((shardId, adapter) -> {
            validateParticipant(0, shardId);
            if (!shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("recovery map key must match adapter shardId: " + shardId);
            }
        });
        this.adapters.values().forEach(adapter -> adapter.configureMetadataStore(this.metadataStore));
    }

    /**
     * 恢复指定事务的未完成状态。
     */
    public void recover(long txId) {
        recover(txId, Set.of());
    }

    /** 在屏障内决定真实终态，释放屏障后物化；返回是否完成了该事务。 */
    public boolean recover(long txId, Set<String> discoveredParticipants) {
        TxRecord decision = metadataStore.withPublicationLock(() -> decide(txId, discoveredParticipants));
        if (decision == null) {
            return false;
        }
        RuntimeException failure = null;
        for (String participant : decision.participants()) {
            try {
                StorageAdapter adapter = requireAdapter(participant, txId);
                if (decision.status() == TxStatus.COMMITTED) {
                    if (decision.commitTs() == null) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "committed transaction has no timestamp", false, txId);
                    }
                    adapter.commit(txId, decision.commitTs());
                } else {
                    adapter.abort(txId);
                }
            } catch (RuntimeException ex) {
                if (failure == null) {
                    failure = ex;
                } else if (failure != ex) {
                    failure.addSuppressed(ex);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        metadataStore.withPublicationLock(() -> {
            metadataStore.releaseSnapshot(txId);
            metadataStore.markFinalized(txId);
            return null;
        });
        return true;
    }

    /** 重新读取元数据及权威墙钟，禁止接管有效租约或误杀尚未登记元数据的事务。 */
    private TxRecord decide(long txId, Set<String> discoveredParticipants) {
        TxRecord record = metadataStore.get(txId).orElse(null);
        if (record == null) {
            // 缺失记录可能属于正在 lazy 登记的事务，intent 扫描不构成回滚授权。
            return null;
        }
        Set<String> participants = new LinkedHashSet<>(record.participants());
        participants.addAll(discoveredParticipants);
        if (record.status() == TxStatus.ACTIVE || record.status() == TxStatus.PREPARING) {
            long now = metadataStore.currentTimeMillis();
            if (metadataStore.snapshotExpiry(txId).map(expiry -> expiry > now).orElse(false)) {
                return null;
            }
            record = metadataStore.registerParticipants(txId, participants);
            if (participants.size() == 1) {
                String participant = participants.iterator().next();
                StorageAdapter adapter = requireAdapter(participant, txId);
                if (adapter.supportsAtomicDirectCommit() || record.commitTs() != null) {
                    // 本地事务记录锁会等待 directCommit；原子提交参与者必须先提供匹配的持久化终态。
                    adapter.abort(txId);
                    ParticipantRecord local = adapter.participantRecord(txId).orElseThrow(() ->
                            new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                    "local publication fence has no durable participant decision", false, txId));
                    if (local.txId() != txId || local.startTs() != record.startTs()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant fence record mismatch", false, txId);
                    }
                    if (local.status() == TxStatus.COMMITTED) {
                        if (local.commitTs() == null || record.commitTs() != null && !record.commitTs().equals(local.commitTs())) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant commit record mismatch", false, txId);
                        }
                        record = metadataStore.markSingleShardCommitted(txId, participant, local.commitTs());
                    } else if (local.status() != TxStatus.ABORTED) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "participant fence did not reach a terminal decision", false, txId);
                    }
                }
            }
            if (record.status() == TxStatus.ACTIVE || record.status() == TxStatus.PREPARING) {
                record = metadataStore.markAborted(txId);
            }
        }
        return record.withParticipants(participants);
    }

    /** 按权威时钟返回一个恢复候选批次，调用者仍须在恢复屏障内重新判断租约。 */
    public List<TxRecord> findRecoveryCandidates(long afterTxId, int limit) {
        return metadataStore.findRecoveryCandidates(metadataStore.currentTimeMillis(), afterTxId, limit);
    }

    /** 缺失参与者配置时明确失败，禁止提前标记完成物化。 */
    private StorageAdapter requireAdapter(String participant, long txId) {
        StorageAdapter adapter = adapters.get(participant);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "recovery participant unavailable: " + participant, false, txId);
        }
        return adapter;
    }
}

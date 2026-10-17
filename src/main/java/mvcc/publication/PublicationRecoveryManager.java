package mvcc.publication;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.publication.mysql.MysqlPublicationLedger;
import mvcc.storage.ParticipantRecord;
import mvcc.storage.StorageAdapter;

import java.util.Map;
import java.util.Objects;

import static mvcc.metadata.TransactionMetadataValidation.validateParticipant;

/** 根据账本、中心决议和参与者持久终态恢复新协议事务。 */
public final class PublicationRecoveryManager {
    private final MysqlPublicationLedger ledger;
    private final TransactionMetadataStore metadataStore;
    private final Map<String, StorageAdapter> adapters;

    /** 校验恢复路由并绑定与参与者读取一致的元数据权威。 */
    public PublicationRecoveryManager(MysqlPublicationLedger ledger, TransactionMetadataStore metadataStore,
                                      Map<String, StorageAdapter> adapters) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.adapters.forEach((shardId, adapter) -> {
            validateParticipant(0, shardId);
            if (!shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("recovery route must match adapter shard id: " + shardId);
            }
        });
        this.adapters.values().forEach(adapter -> adapter.configureMetadataStore(metadataStore));
    }

    /** 恢复一个事务；有效租约下的 PENDING 写事务仍归前台所有。 */
    public boolean recover(long txId) {
        PublicationEntry entry = ledger.findByTxId(txId).orElse(null);
        if (entry == null) {
            return recoverUnreserved(txId);
        }
        TxRecord record = requireRecord(txId);
        if (entry.status() == PublicationEntryStatus.PENDING) {
            long now = metadataStore.currentTimeMillis();
            if (entry.expiresAtMillis() > now
                    || ledger.snapshotExpiry(txId).stream().anyMatch(expiry -> expiry > now)) {
                return false;
            }
            decideExpired(entry, record);
            entry = ledger.findByTxId(txId).orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication entry disappeared during recovery", false, txId));
            record = requireRecord(txId);
        }
        materialize(entry, record);
        ledger.advancePublished(256);
        metadataStore.markFinalized(txId);
        ledger.releaseSnapshot(txId);
        return true;
    }

    /** 崩溃发生在预写与账本预留之间时，按新租约接管并清理所有参与者。 */
    private boolean recoverUnreserved(long txId) {
        long now = metadataStore.currentTimeMillis();
        if (ledger.snapshotExpiry(txId).stream().anyMatch(expiry -> expiry > now)) {
            return false;
        }
        TxRecord record = metadataStore.get(txId).orElse(null);
        if (record == null) {
            ledger.releaseSnapshot(txId);
            return false;
        }
        if (record.status() == TxStatus.COMMITTED || record.commitTs() != null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "committed or local pending metadata has no publication reservation", false, txId);
        }
        if (record.status() != TxStatus.ABORTED) {
            TxRecord decided = metadataStore.markAborted(txId);
            if (decided.status() == TxStatus.COMMITTED) {
                if (ledger.findByTxId(txId).isPresent()) {
                    return recover(txId);
                }
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "foreground commit raced unreserved recovery", false, txId);
            }
        }
        abortParticipants(record);
        metadataStore.markFinalized(txId);
        ledger.releaseSnapshot(txId);
        return true;
    }

    /** 到期事务先建立不可逆终态，再允许水位越过该预留。 */
    private void decideExpired(PublicationEntry entry, TxRecord record) {
        long txId = entry.txId();
        if (record.status() == TxStatus.COMMITTED) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "committed metadata has unresolved publication entry", false, txId);
        }
        if (record.status() == TxStatus.ABORTED) {
            abortParticipants(record);
            ledger.decideAbortedAfterFencing(txId, entry.commitTs());
            return;
        }
        if (record.commitTs() != null) {
            resolveLocalPending(entry, record);
            return;
        }
        TxRecord decided = metadataStore.markAborted(txId);
        if (decided.status() == TxStatus.COMMITTED) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "foreground commit raced expired recovery", false, txId);
        }
        abortParticipants(record);
        ledger.decideAbortedAfterFencing(txId, entry.commitTs());
    }

    /** 单分片本地物理提交须先 fencing 迟到请求，再依参与者真实终态补决议。 */
    private void resolveLocalPending(PublicationEntry entry, TxRecord record) {
        long txId = entry.txId();
        if (record.participants().size() != 1 || !record.commitTs().equals(entry.commitTs())) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local publication has no unique matching participant", false, txId);
        }
        String shardId = record.participants().iterator().next();
        StorageAdapter adapter = requireAdapter(shardId, txId);
        adapter.abort(txId);
        ParticipantRecord local = adapter.participantRecord(txId).orElseThrow(() ->
                new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local participant has no durable terminal decision", false, txId));
        if (local.txId() != txId || local.startTs() != record.startTs()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local participant proof has wrong transaction identity", false, txId);
        }
        if (local.status() == TxStatus.COMMITTED) {
            ledger.completeLocalCommitAfterFencing(local, entry.commitTs(), shardId);
        } else if (local.status() == TxStatus.ABORTED) {
            if (metadataStore.markAborted(txId).status() != TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local abort raced a committed center decision", false, txId);
            }
            ledger.decideAbortedAfterFencing(txId, entry.commitTs());
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local participant is not terminal", false, txId);
        }
    }

    /** 逐分片幂等 fencing，任一失败都保留账本 PENDING 供下一轮重试。 */
    private void abortParticipants(TxRecord record) {
        RuntimeException failure = null;
        for (String participant : record.participants()) {
            try {
                requireAdapter(participant, record.txId()).abort(record.txId());
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
    }

    /** 全部分片完成幂等终态物化后才能标记中心可回收。 */
    private void materialize(PublicationEntry entry, TxRecord record) {
        if (entry.status() == PublicationEntryStatus.COMMITTED) {
            if (record.status() != TxStatus.COMMITTED || record.commitTs() == null
                    || record.commitTs() != entry.commitTs()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "committed publication differs from transaction metadata", false, entry.txId());
            }
        } else if (entry.status() == PublicationEntryStatus.ABORTED) {
            if (record.status() != TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "aborted publication differs from transaction metadata", false, entry.txId());
            }
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "publication remains pending after recovery", false, entry.txId());
        }
        RuntimeException failure = null;
        for (String participant : record.participants()) {
            try {
                StorageAdapter adapter = requireAdapter(participant, entry.txId());
                if (entry.status() == PublicationEntryStatus.COMMITTED) {
                    adapter.commit(entry.txId(), entry.commitTs());
                } else {
                    adapter.abort(entry.txId());
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
    }

    /** 缺少分片配置时禁止提前推进终态完成标记。 */
    private StorageAdapter requireAdapter(String shardId, long txId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication recovery participant unavailable: " + shardId, false, txId);
        }
        return adapter;
    }

    /** 缺少中心元数据时不能凭账本单独推断参与者去向。 */
    private TxRecord requireRecord(long txId) {
        return metadataStore.get(txId).orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE,
                "publication recovery transaction metadata missing", false, txId));
    }
}

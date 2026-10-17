package mvcc.publication;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger;
import mvcc.storage.ParticipantRecord;
import mvcc.storage.StorageAdapter;

import java.util.Map;
import java.util.Objects;

import static mvcc.metadata.TransactionMetadataValidation.validateParticipant;

/** 按分段槽与中心元数据的同一持久终态恢复参与者和过期未决事务。 */
public final class SegmentPublicationRecoveryManager {
    private final MysqlSegmentPublicationLedger ledger;
    private final TransactionMetadataStore metadataStore;
    private final Map<String, StorageAdapter> adapters;

    /** 校验恢复路由并让参与者读取使用相同中心元数据权威。 */
    public SegmentPublicationRecoveryManager(MysqlSegmentPublicationLedger ledger,
                                             TransactionMetadataStore metadataStore,
                                             Map<String, StorageAdapter> adapters) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.adapters.forEach((shardId, adapter) -> {
            validateParticipant(0, shardId);
            if (!shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("segment recovery route differs from adapter: " + shardId);
            }
            adapter.configureMetadataStore(metadataStore);
        });
    }

    /** 有槽终态时幂等补物化；无槽且租约已失效时安全中止遗留意向。 */
    public boolean recover(long txId) {
        PublicationEntry entry = ledger.findByTxId(txId).orElse(null);
        TxRecord record = metadataStore.get(txId).orElse(null);
        if (entry != null) {
            if (record == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented terminal slot has no center transaction", false, txId);
            }
            if (entry.status() == PublicationEntryStatus.PENDING) {
                return recoverLocalPending(entry, record);
            }
            materialize(entry, record);
            ledger.advancePublished(256);
            metadataStore.markFinalized(txId);
            ledger.releaseSnapshot(txId);
            return true;
        }
        long now = metadataStore.currentTimeMillis();
        if (ledger.snapshotExpiry(txId).stream().anyMatch(expiry -> expiry > now)) {
            return false;
        }
        if (record == null) {
            ledger.releaseSnapshot(txId);
            return false;
        }
        if (record.status() == TxStatus.COMMITTED || record.commitTs() != null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "committed segmented metadata has no matching terminal slot", false, txId);
        }
        if (record.status() != TxStatus.ABORTED) {
            TxRecord decided = metadataStore.markAborted(txId);
            if (decided.status() == TxStatus.COMMITTED) {
                if (ledger.findByTxId(txId).isPresent()) {
                    return recover(txId);
                }
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "foreground segmented commit raced expired recovery", false, txId);
            }
        }
        abortParticipants(record);
        metadataStore.markFinalized(txId);
        ledger.releaseSnapshot(txId);
        return true;
    }

    /** 本地候选到期或 owner 被 fencing 后，以分片终态证明补同一个中心时间槽。 */
    private boolean recoverLocalPending(PublicationEntry entry, TxRecord record) {
        long txId = entry.txId();
        long now = metadataStore.currentTimeMillis();
        if (!ledger.localPendingRecoverable(entry.commitTs(), now)) {
            return false;
        }
        if (record.status() != TxStatus.PREPARING || record.commitTs() == null
                || record.commitTs() != entry.commitTs() || record.startTs() >= entry.commitTs()
                || record.participants().size() != 1) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented local candidate differs from center metadata", false, txId);
        }
        String participant = record.participants().iterator().next();
        StorageAdapter adapter = requireAdapter(participant, txId);
        // 本地 abort 与在途 directCommit 在参与者事务行上串行，再读取真正持久终态。
        adapter.abort(txId);
        ParticipantRecord local = adapter.participantRecord(txId).orElseThrow(() ->
                new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "segmented local candidate has no participant proof", false, txId));
        if (local.txId() != txId || local.startTs() != record.startTs()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented local proof has wrong transaction identity", false, txId);
        }
        if (local.status() == TxStatus.COMMITTED) {
            if (!Objects.equals(local.commitTs(), entry.commitTs())) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented local commit proof has another timestamp", false, txId);
            }
            ledger.completeLocalCommitAfterFencing(local, entry.commitTs(), participant);
        } else if (local.status() == TxStatus.ABORTED) {
            ledger.completeLocalAbortAfterFencing(local, entry.commitTs(), participant);
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented local participant remains undecided", false, txId);
        }
        return recover(txId);
    }

    /** 中心与槽必须同为 COMMITTED 或 ABORTED，才可重放相应的参与者终态。 */
    private void materialize(PublicationEntry entry, TxRecord record) {
        if (entry.status() == PublicationEntryStatus.COMMITTED) {
            if (record.status() != TxStatus.COMMITTED || record.commitTs() == null
                    || record.commitTs() != entry.commitTs()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented committed slot differs from center decision", false, entry.txId());
            }
        } else if (entry.status() == PublicationEntryStatus.ABORTED) {
            if (record.status() != TxStatus.ABORTED || record.commitTs() != null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented aborted slot differs from center decision", false, entry.txId());
            }
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented slot remains pending during recovery", false, entry.txId());
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
                ParticipantRecord local = adapter.participantRecord(entry.txId()).orElse(null);
                if (local != null && (local.startTs() != record.startTs()
                        || local.status() != (entry.status() == PublicationEntryStatus.COMMITTED
                                ? TxStatus.COMMITTED : TxStatus.ABORTED)
                        || (entry.status() == PublicationEntryStatus.COMMITTED
                                && !Objects.equals(local.commitTs(), entry.commitTs())))) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "participant terminal proof differs from segmented decision", false, entry.txId());
                }
            } catch (RuntimeException ex) {
                failure = accumulate(failure, ex);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** 中心未决事务到期后逐分片 fencing，任一失败保留未 finalized 记录供下轮重试。 */
    private void abortParticipants(TxRecord record) {
        RuntimeException failure = null;
        for (String participant : record.participants()) {
            try {
                StorageAdapter adapter = requireAdapter(participant, record.txId());
                adapter.abort(record.txId());
                ParticipantRecord local = adapter.participantRecord(record.txId()).orElse(null);
                if (local != null && (local.startTs() != record.startTs()
                        || local.status() != TxStatus.ABORTED)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "expired undecided participant was not fenced", false, record.txId());
                }
            } catch (RuntimeException ex) {
                failure = accumulate(failure, ex);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** 缺少物理分片路由时禁止把中心记录提前标记为已完成。 */
    private StorageAdapter requireAdapter(String shardId, long txId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented recovery participant unavailable: " + shardId, false, txId);
        }
        return adapter;
    }

    /** 在保留首个故障的同时收集其余分片异常。 */
    private RuntimeException accumulate(RuntimeException first, RuntimeException next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }
}

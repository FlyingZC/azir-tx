package mvcc.metadata;

import mvcc.common.IsolationLevel;
import mvcc.common.TxStatus;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 事务元数据记录，保存事务时间戳、隔离级别、参与分片与提交状态。
 */
public final class TxRecord {
    private final long txId;
    private final long startTs;
    private final IsolationLevel isolationLevel;
    private final Set<String> participants;
    private final TxStatus status;
    private final Long commitTs;

    /**
     * 构造 TxRecord 实例。
     */
    public TxRecord(long txId,
                    long startTs,
                    IsolationLevel isolationLevel,
                    Set<String> participants,
                    TxStatus status,
                    Long commitTs) {
        if (status == null || isolationLevel == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction status and isolation level must not be null", false, txId);
        }
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (status == TxStatus.COMMITTED || commitTs != null) {
            TransactionMetadataValidation.validateCommitTs(txId, startTs, commitTs);
        }
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
        this.participants = new LinkedHashSet<>(participants);
        this.status = status;
        this.commitTs = commitTs;
    }

    /**
     * 返回事务编号。
     */
    public long txId() {
        return txId;
    }

    /**
     * 返回事务开始时间戳。
     */
    public long startTs() {
        return startTs;
    }

    /**
     * 返回事务隔离级别。
     */
    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    /**
     * 返回事务参与分片集合。
     */
    public Set<String> participants() {
        return Set.copyOf(participants);
    }

    /**
     * 返回事务状态。
     */
    public TxStatus status() {
        return status;
    }

    /**
     * 返回事务提交时间戳。
     */
    public Long commitTs() {
        return commitTs;
    }

    /**
     * 返回替换参与分片后的新事务记录。
     */
    public TxRecord withParticipants(Set<String> newParticipants) {
        return new TxRecord(txId, startTs, isolationLevel, newParticipants, status, commitTs);
    }

    /**
     * 返回替换状态后的新事务记录。
     */
    public TxRecord withStatus(TxStatus newStatus) {
        return new TxRecord(txId, startTs, isolationLevel, participants, newStatus, commitTs);
    }

    /**
     * 返回已提交状态的新事务记录。
     */
    public TxRecord withCommitted(long newCommitTs) {
        return new TxRecord(txId, startTs, isolationLevel, participants, TxStatus.COMMITTED, newCommitTs);
    }
}

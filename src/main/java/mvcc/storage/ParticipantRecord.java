package mvcc.storage;

import mvcc.common.TxStatus;

import java.util.Objects;

/**
 * 参与者本地事务记录，与本分片版本或回滚标记原子持久化。
 */
public record ParticipantRecord(long txId, long startTs, TxStatus status, Long commitTs) {
    /**
     * 校验终态和提交时间戳一致性；开始时间为零表示缺少可回收的开始时间证据。
     */
    public ParticipantRecord {
        Objects.requireNonNull(status, "status");
        if ((status == TxStatus.COMMITTED) != (commitTs != null)) {
            throw new IllegalArgumentException("only COMMITTED records must have commitTs");
        }
    }

    /**
     * 返回事务编号。
     */
    @Override
    public long txId() {
        return txId;
    }

    /**
     * 返回事务开始时间戳。
     */
    @Override
    public long startTs() {
        return startTs;
    }

    /**
     * 返回参与者持久化状态。
     */
    @Override
    public TxStatus status() {
        return status;
    }

    /**
     * 返回不可变的提交时间戳。
     */
    @Override
    public Long commitTs() {
        return commitTs;
    }
}

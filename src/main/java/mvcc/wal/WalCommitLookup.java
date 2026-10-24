package mvcc.wal;

/** 区分可确认提交与查询保留信息不足，任何缺失结果都不能推导业务回滚。 */
public record WalCommitLookup(Status status, WalReceipt receipt, long retiredThroughSequence) {
    /** 无凭据且已回收部分历史时，只能保守表示未知或已过查询窗口。 */
    public enum Status { COMMITTED, NOT_CONFIRMED, UNKNOWN_OR_EXPIRED }

    /** 保证只有已确认状态携带凭据，回收水位始终非负。 */
    public WalCommitLookup {
        if (status == null || (status == Status.COMMITTED) != (receipt != null) || retiredThroughSequence < 0) {
            throw new IllegalArgumentException("invalid commit lookup outcome");
        }
    }
}

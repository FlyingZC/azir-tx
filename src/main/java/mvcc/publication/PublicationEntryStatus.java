package mvcc.publication;

/** 提交时间戳预留后的持久化发布状态。 */
public enum PublicationEntryStatus {
    PENDING,
    COMMITTED,
    ABORTED
}

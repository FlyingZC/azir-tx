package mvcc.publication;

/** 一次协调器租约保护下的已发布读水位和数据库截止时间。 */
public record CoordinatorSnapshotView(long readTs, long deadlineMillis,
                                      long leaseReadTs, long leaseExpiryMillis) {
    /** 拒绝未发布或已失去租约下界保护的快照。 */
    public CoordinatorSnapshotView {
        if (readTs < 0 || deadlineMillis < 1 || leaseReadTs < 0
                || leaseReadTs > readTs || leaseExpiryMillis < deadlineMillis) {
            throw new IllegalArgumentException("invalid coordinator protected snapshot");
        }
    }
}

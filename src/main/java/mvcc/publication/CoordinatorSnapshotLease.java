package mvcc.publication;

/** 一条由控制数据库持久化的协调器级最老快照保护与 fencing 代数。 */
public record CoordinatorSnapshotLease(long coordinatorId, long readTs,
                                       long expiresAtMillis, long generation) {
    /** 拒绝缺失编号、负快照和不可用的持久代数。 */
    public CoordinatorSnapshotLease {
        if (coordinatorId < 1 || readTs < 0 || expiresAtMillis < 1 || generation < 1) {
            throw new IllegalArgumentException("invalid coordinator snapshot lease");
        }
    }
}

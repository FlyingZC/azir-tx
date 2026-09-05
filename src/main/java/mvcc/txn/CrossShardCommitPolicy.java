package mvcc.txn;

public record CrossShardCommitPolicy(boolean parallelPrepare,
                                     boolean parallelCommit,
                                     boolean parallelRollback) {
    public static CrossShardCommitPolicy serial() {
        return new CrossShardCommitPolicy(false, false, false);
    }

    public static CrossShardCommitPolicy parallel() {
        return new CrossShardCommitPolicy(true, true, true);
    }
}

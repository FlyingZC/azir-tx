package mvcc.txn;

public record CrossShardCommitPolicy(boolean parallelPrewrite,
                                     boolean parallelPrepare,
                                     boolean parallelCommit,
                                     boolean parallelRollback,
                                     boolean asyncCommit,
                                     int asyncCommitWorkers) {
    public CrossShardCommitPolicy {
        if (asyncCommitWorkers < 1) {
            throw new IllegalArgumentException("asyncCommitWorkers must be >= 1");
        }
    }

    public static CrossShardCommitPolicy serial() {
        return new CrossShardCommitPolicy(false, false, false, false, false, 1);
    }

    public static CrossShardCommitPolicy parallel() {
        return new CrossShardCommitPolicy(true, true, true, true, false, 1);
    }

    public static CrossShardCommitPolicy asyncParallel(int asyncCommitWorkers) {
        return new CrossShardCommitPolicy(true, true, true, true, true, asyncCommitWorkers);
    }
}

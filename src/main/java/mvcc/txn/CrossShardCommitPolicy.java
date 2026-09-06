package mvcc.txn;

/**
 * 跨分片提交策略，定义各阶段并发和异步提交行为。
 */
public record CrossShardCommitPolicy(boolean parallelPrewrite,
                                     boolean parallelPrepare,
                                     boolean parallelCommit,
                                     boolean parallelRollback,
                                     boolean asyncCommit,
                                     int asyncCommitWorkers) {
    /**
     * 构造 CrossShardCommitPolicy 实例。
     */
    public CrossShardCommitPolicy {
        if (asyncCommitWorkers < 1) {
            throw new IllegalArgumentException("asyncCommitWorkers must be >= 1");
        }
    }

    /**
     * 返回串行跨分片提交策略。
     */
    public static CrossShardCommitPolicy serial() {
        return new CrossShardCommitPolicy(false, false, false, false, false, 1);
    }

    /**
     * 返回并行跨分片提交策略。
     */
    public static CrossShardCommitPolicy parallel() {
        return new CrossShardCommitPolicy(true, true, true, true, false, 1);
    }

    /**
     * 返回异步并行跨分片提交策略。
     */
    public static CrossShardCommitPolicy asyncParallel(int asyncCommitWorkers) {
        return new CrossShardCommitPolicy(true, true, true, true, true, asyncCommitWorkers);
    }
}

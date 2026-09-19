package mvcc.txn;

import java.util.Objects;

/**
 * 事务执行选项，集中配置锁等待、重试和跨分片提交策略。
 */
public record TransactionOptions(LockWaitPolicy lockWaitPolicy,
                                 TransactionRetryPolicy retryPolicy,
                                 CrossShardCommitPolicy crossShardCommitPolicy,
                                 boolean bufferWrites,
                                 boolean lazyBeginMetadata,
                                 boolean deferIntentCleanup) {
    /**
     * 构造 TransactionOptions 实例。
     */
    public TransactionOptions {
        Objects.requireNonNull(lockWaitPolicy, "lockWaitPolicy");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        Objects.requireNonNull(crossShardCommitPolicy, "crossShardCommitPolicy");
    }

    /**
     * 返回默认事务选项。
     */
    public static TransactionOptions defaults() {
        return safePerformanceDefaults();
    }

    /**
     * 返回 ACID 安全的性能优先默认配置。
     */
    public static TransactionOptions safePerformanceDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.mysqlLikeDefaults(),
                TransactionRetryPolicy.mysqlLikeDefaults(),
                CrossShardCommitPolicy.parallel(),
                true,
                false,
                false
        );
    }

    /**
     * 返回失败优先的显式配置。
     */
    public static TransactionOptions failFastDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(),
                false,
                false,
                false
        );
    }

    /**
     * 返回接近 MySQL 行为的默认配置。
     */
    public static TransactionOptions mysqlLikeDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.mysqlLikeDefaults(),
                TransactionRetryPolicy.mysqlLikeDefaults(),
                CrossShardCommitPolicy.asyncParallel(16),
                true,
                true,
                false
        );
    }
}

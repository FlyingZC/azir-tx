package mvcc.txn;

import java.util.Objects;

public record TransactionOptions(LockWaitPolicy lockWaitPolicy,
                                 TransactionRetryPolicy retryPolicy,
                                 CrossShardCommitPolicy crossShardCommitPolicy) {
    public TransactionOptions {
        Objects.requireNonNull(lockWaitPolicy, "lockWaitPolicy");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        Objects.requireNonNull(crossShardCommitPolicy, "crossShardCommitPolicy");
    }

    public static TransactionOptions defaults() {
        return new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial()
        );
    }

    public static TransactionOptions mysqlLikeDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.mysqlLikeDefaults(),
                TransactionRetryPolicy.mysqlLikeDefaults(),
                CrossShardCommitPolicy.parallel()
        );
    }
}

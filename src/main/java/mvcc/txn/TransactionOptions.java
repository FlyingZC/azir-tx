package mvcc.txn;

import java.util.Objects;

public record TransactionOptions(LockWaitPolicy lockWaitPolicy,
                                 TransactionRetryPolicy retryPolicy) {
    public TransactionOptions {
        Objects.requireNonNull(lockWaitPolicy, "lockWaitPolicy");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
    }

    public static TransactionOptions defaults() {
        return new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry());
    }

    public static TransactionOptions mysqlLikeDefaults() {
        return new TransactionOptions(LockWaitPolicy.mysqlLikeDefaults(), TransactionRetryPolicy.mysqlLikeDefaults());
    }
}

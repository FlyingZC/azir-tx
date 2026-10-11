package mvcc.txn;

/** 事务及后台执行的资源预算，避免慢分片导致无界积压。 */
public record TransactionLimits(long timeoutMillis, int maxWriteKeys, long maxWriteBytes,
                                int maxActiveTransactions, int shardConcurrency,
                                int asyncQueueCapacity, int scanPageSize, long shutdownTimeoutMillis,
                                int maxMaterializedRows, long maxMaterializedBytes) {
    /** 校验所有预算均为有限正数。 */
    public TransactionLimits {
        if (timeoutMillis < 1 || timeoutMillis > 86_400_000L || maxWriteKeys < 1 || maxWriteBytes < 1
                || maxActiveTransactions < 1 || shardConcurrency < 1 || asyncQueueCapacity < 1
                || scanPageSize < 1 || scanPageSize > 4096 || shutdownTimeoutMillis < 1 || shutdownTimeoutMillis > 86_400_000L
                || maxMaterializedRows < 1 || maxMaterializedBytes < 1) {
            throw new IllegalArgumentException("invalid transaction resource limits");
        }
    }

    /** 为原有资源预算构造入口补充独立的物化读取预算。 */
    public TransactionLimits(long timeoutMillis, int maxWriteKeys, long maxWriteBytes,
                              int maxActiveTransactions, int shardConcurrency, int asyncQueueCapacity,
                              int scanPageSize, long shutdownTimeoutMillis) {
        this(timeoutMillis, maxWriteKeys, maxWriteBytes, maxActiveTransactions, shardConcurrency,
                asyncQueueCapacity, scanPageSize, shutdownTimeoutMillis, 100_000, 64L * 1024 * 1024);
    }

    /** 返回保守的有界默认值。 */
    public static TransactionLimits defaults() {
        return new TransactionLimits(60_000L, 10_000, 16L * 1024 * 1024, 1024, 16, 1024, 256, 5_000L);
    }
}

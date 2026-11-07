package com.flyingzc.azir.wal;

/** 独立 WAL 模式的有界资源配置，时间单位为毫秒，容量单位为字节。 */
public record WalOptions(int maxActiveTransactions, int maxWriteKeys, long maxWriteBytes,
                         int queueCapacity, int commitBatchSize, long batchDelayMillis,
                         long maxWalBytes, long maxMemoryBytes, long transactionTimeoutMillis,
                         int flushBatchSize, long flushIntervalMillis,
                         long maxHotCacheBytes, long maxMaterializationLag) {
    /** 保持新模式已有调用兼容，默认热点预算不超过总内存四分之一或 8 MiB。 */
    public WalOptions(int maxActiveTransactions, int maxWriteKeys, long maxWriteBytes,
                      int queueCapacity, int commitBatchSize, long batchDelayMillis,
                      long maxWalBytes, long maxMemoryBytes, long transactionTimeoutMillis,
                      int flushBatchSize, long flushIntervalMillis) {
        this(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity, commitBatchSize,
                batchDelayMillis, maxWalBytes, maxMemoryBytes, transactionTimeoutMillis,
                flushBatchSize, flushIntervalMillis, Math.min(8L * 1024 * 1024, maxMemoryBytes / 4), 100_000);
    }

    /** 拒绝不合理配置，单笔及合批日志受固定格式上限保护。 */
    public WalOptions {
        if (maxActiveTransactions < 1 || maxWriteKeys < 1 || maxWriteKeys > 100_000
                || maxWriteBytes < 1 || maxWriteBytes > 8 * 1024 * 1024
                || queueCapacity < 1 || commitBatchSize < 1 || commitBatchSize > 1024
                || batchDelayMillis < 0 || batchDelayMillis > 1000
                || maxWalBytes < 1024 || maxMemoryBytes < 1
                || transactionTimeoutMillis < 1 || transactionTimeoutMillis > 86_400_000
                || flushBatchSize < 1 || flushBatchSize > 1024 || flushIntervalMillis < 1
                || maxHotCacheBytes < 0 || maxHotCacheBytes > maxMemoryBytes || maxMaterializationLag < 1) {
            throw new IllegalArgumentException("invalid WAL mode limits");
        }
    }

    /** 返回用于单写者 MVP 的默认限制。 */
    public static WalOptions defaults() {
        return new WalOptions(4096, 1024, 1024 * 1024, 4096, 64, 1,
                256L * 1024 * 1024, 64L * 1024 * 1024, 60_000, 1024, 1);
    }

    /** 创建仅修改事务期限的配置，供应用和确定性验收使用。 */
    public WalOptions withTimeout(long millis) {
        return new WalOptions(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity,
                commitBatchSize, batchDelayMillis, maxWalBytes, maxMemoryBytes, millis,
                flushBatchSize, flushIntervalMillis, maxHotCacheBytes, maxMaterializationLag);
    }

    /** 调整可淘汰热点缓存，零表示仅保留未物化增量，冷读直接访问后端。 */
    public WalOptions withHotCacheBytes(long bytes) {
        return new WalOptions(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity,
                commitBatchSize, batchDelayMillis, maxWalBytes, maxMemoryBytes, transactionTimeoutMillis,
                flushBatchSize, flushIntervalMillis, bytes, maxMaterializationLag);
    }

    /** 限制最慢分片尚未物化的提交数，达到预算时在 WAL 追加前背压。 */
    public WalOptions withMaterializationLag(long commits) {
        return new WalOptions(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity,
                commitBatchSize, batchDelayMillis, maxWalBytes, maxMemoryBytes, transactionTimeoutMillis,
                flushBatchSize, flushIntervalMillis, maxHotCacheBytes, commits);
    }
}

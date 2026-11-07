package com.flyingzc.azir.wal;

/** 新模式的检查点与提交凭据回收策略，不改变原有 Azir 的配置或入口。 */
public record WalRetentionOptions(long receiptRetentionMillis, long checkpointIntervalMillis,
                                  long checkpointWalBytes, long maxReceiptArchiveBytes,
                                  int maxPendingReceipts) {
    /** 零保留期限表示永久保留，容量达到预算时明确背压而不是丢弃凭据。 */
    public WalRetentionOptions {
        if (receiptRetentionMillis < 0 || checkpointIntervalMillis < 1 || checkpointWalBytes < 1024
                || maxReceiptArchiveBytes < 4096 || maxPendingReceipts < 1 || maxPendingReceipts > 1_000_000) {
            throw new IllegalArgumentException("invalid WAL retention policy");
        }
    }

    /** 保持既有结果查询合同，只有调用方明确设置期限才允许过期回收。 */
    public static WalRetentionOptions preserveAll() {
        return new WalRetentionOptions(0, 60_000, 4L * 1024 * 1024, 512L * 1024 * 1024, 100_000);
    }

    /** 返回显式指定凭据保留窗口的策略，其余资源门槛保持一致。 */
    public WalRetentionOptions withReceiptRetentionMillis(long millis) {
        return new WalRetentionOptions(millis, checkpointIntervalMillis, checkpointWalBytes,
                maxReceiptArchiveBytes, maxPendingReceipts);
    }
}

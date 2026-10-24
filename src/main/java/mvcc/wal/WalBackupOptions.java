package mvcc.wal;

/** 独立备份和恢复的字节、批次及单调时间预算，不改变正常事务配置。 */
public record WalBackupOptions(long maxBytes, int batchKeys, long batchBytes, long timeoutMillis) {
    /** 拒绝无法有界执行的配置，单批数据不超过 16 MiB。 */
    public WalBackupOptions {
        if (maxBytes < 1024 || batchKeys < 1 || batchKeys > 1024 || batchBytes < 1 || batchBytes > 16L * 1024 * 1024
                || timeoutMillis < 1 || timeoutMillis > 86_400_000) { throw new IllegalArgumentException("invalid backup limits"); }
    }

    /** 默认最多 16 GiB、256 键及 16 MiB 一批，维护窗口最多五分钟，应用可显式调整。 */
    public static WalBackupOptions defaults() { return new WalBackupOptions(16L * 1024 * 1024 * 1024, 256, 16L * 1024 * 1024, 300_000); }
}

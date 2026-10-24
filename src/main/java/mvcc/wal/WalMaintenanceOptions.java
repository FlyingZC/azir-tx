package mvcc.wal;

/** 托管维护的固定预算，避免自动任务变成无界扫描或忙循环。 */
public record WalMaintenanceOptions(long intervalMillis, int keyLimit, int rowLimit, long workMillis,
                                    long staleAfterMillis, int warningPercent) {
    /** 维护最多检查四千键、删除四千版本，状态过期阈值必须大于正常周期。 */
    public WalMaintenanceOptions {
        if (intervalMillis < 10 || intervalMillis > 3_600_000 || keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096
                || workMillis < 1 || workMillis > 5000 || staleAfterMillis <= intervalMillis || staleAfterMillis > 86_400_000
                || warningPercent < 1 || warningPercent > 99) { throw new IllegalArgumentException("invalid maintenance limits"); }
    }

    /** 每秒最多检查六十四键、删除一千版本，并预留充足的 I/O 超时观测窗口。 */
    public static WalMaintenanceOptions defaults() { return new WalMaintenanceOptions(1000, 64, 1024, 100, 15_000, 80); }
}

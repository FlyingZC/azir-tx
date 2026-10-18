package mvcc.maintenance;

/** 常驻维护的有限周期、单轮预算和关闭等待时间。 */
public record MaintenanceOptions(long intervalMillis, int batchSize, long closeTimeoutMillis) {
    /** 防止零周期忙循环或无限关闭等待。 */
    public MaintenanceOptions {
        if (intervalMillis < 10 || intervalMillis > 86_400_000 || batchSize < 1 || batchSize > 4096
                || closeTimeoutMillis < 1 || closeTimeoutMillis > 60_000) {
            throw new IllegalArgumentException("invalid maintenance options");
        }
    }
    /** 默认每五秒执行一轮，每轮每分片最多处理一百二十八条历史记录。 */
    public static MaintenanceOptions defaults() { return new MaintenanceOptions(5000, 128, 10_000); }
}

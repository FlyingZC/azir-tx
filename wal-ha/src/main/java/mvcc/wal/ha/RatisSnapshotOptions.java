package mvcc.wal.ha;

/** 自动镜像、日志水位和保留尾部配置，时间单位为毫秒，容量单位为字节；尾部保留在高水位时让位于安全回收。 */
public record RatisSnapshotOptions(long intervalMillis, long entryGap, long maxIntervalMillis, long logHighWaterBytes, long preserveEntries) {
    /** 上界用于协作背压，实际磁盘限额还须包含在途日志队列与镜像文件。 */
    public RatisSnapshotOptions {
        if (intervalMillis < 100 || intervalMillis > 60_000 || entryGap < 1 || entryGap > 10_000_000
                || maxIntervalMillis < intervalMillis || maxIntervalMillis > 86_400_000 || logHighWaterBytes < 32L * 1024 * 1024
                || logHighWaterBytes > 1L << 50 || preserveEntries < 0 || preserveEntries > 10_000_000) { throw new IllegalArgumentException("invalid snapshot lifecycle limits"); }
    }
    /** 每秒观察，一万条日志或五分钟触发，日志高水位 256 MiB，保留一千零二十四条尾部。 */
    public static RatisSnapshotOptions defaults() { return new RatisSnapshotOptions(1000, 10_000, 300_000, 256L * 1024 * 1024, 1024); }
}

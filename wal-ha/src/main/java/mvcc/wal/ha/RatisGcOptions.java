package mvcc.wal.ha;

/** 副本版本回收的单轮预算，安全点由活跃事务计算，不能由配置强行指定。 */
public record RatisGcOptions(long intervalMillis, int keyLimit, int rowLimit, long workMillis, long staleAfterMillis) {
    /** 时间预算为协作预算，数据库调用还须受正式适配器的 JDBC 超时约束。 */
    public RatisGcOptions {
        if (intervalMillis < 100 || intervalMillis > 60_000 || keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096
                || workMillis < 1 || workMillis > 5000 || staleAfterMillis <= intervalMillis || staleAfterMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid replica version GC limits");
        }
    }
    /** 每秒最多检查六十四键，数据库和内存各最多回收一千零二十四个版本。 */
    public static RatisGcOptions defaults() { return new RatisGcOptions(1000, 64, 1024, 100, 15_000); }
}

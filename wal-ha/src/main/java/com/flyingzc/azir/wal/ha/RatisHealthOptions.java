package com.flyingzc.azir.wal.ha;

/** 后端健康管理的失败防抖、恢复防抖与协作等待预算。 */
public record RatisHealthOptions(long intervalMillis, int failures, int recoveries, long probeMillis, long transferMillis) {
    /** 探测和管理等待均有限，完整切主预算须容纳至少一次目标探测。 */
    public RatisHealthOptions {
        if (intervalMillis < 100 || intervalMillis > 60_000 || failures < 1 || failures > 100 || recoveries < 1 || recoveries > 100
                || probeMillis < 100 || probeMillis > 10_000 || transferMillis < probeMillis + 500 || transferMillis > 120_000) { throw new IllegalArgumentException("invalid backend health limits"); }
    }
    /** 每秒探测，连续三次失败隔离、连续三次成功恢复资格。 */
    public static RatisHealthOptions defaults() { return new RatisHealthOptions(1000, 3, 3, 1000, 10_000); }
}

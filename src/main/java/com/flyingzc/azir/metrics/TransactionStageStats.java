package com.flyingzc.azir.metrics;

/**
 * 事务阶段指标快照。
 */
public record TransactionStageStats(long invocationCount,
                                    long successCount,
                                    long failureCount,
                                    long totalNanos) {
    /**
     * 返回平均耗时纳秒数。
     */
    public long averageNanos() {
        return invocationCount == 0L ? 0L : totalNanos / invocationCount;
    }
}

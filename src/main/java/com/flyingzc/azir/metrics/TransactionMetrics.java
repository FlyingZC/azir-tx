package com.flyingzc.azir.metrics;

/**
 * 事务指标记录接口。
 */
public interface TransactionMetrics {
    /**
     * 记录阶段成功执行。
     */
    void recordSuccess(TransactionStage stage, long elapsedNanos);

    /**
     * 记录阶段执行失败。
     */
    void recordFailure(TransactionStage stage, long elapsedNanos);
}

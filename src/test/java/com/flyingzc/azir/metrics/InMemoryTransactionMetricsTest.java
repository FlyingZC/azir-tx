package com.flyingzc.azir.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 内存事务指标聚合测试。
 */
class InMemoryTransactionMetricsTest {
    /**
     * 验证成功阶段会累加调用次数、成功次数和耗时。
     */
    @Test
    void recordSuccessShouldAccumulateStageStats() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        metrics.recordSuccess(TransactionStage.COMMIT_TSO, 100L);
        metrics.recordSuccess(TransactionStage.COMMIT_TSO, 300L);

        TransactionStageStats stats = metrics.snapshot(TransactionStage.COMMIT_TSO);
        assertEquals(2L, stats.invocationCount());
        assertEquals(2L, stats.successCount());
        assertEquals(0L, stats.failureCount());
        assertEquals(400L, stats.totalNanos());
        assertEquals(200L, stats.averageNanos());
    }

    /**
     * 验证失败阶段会累加失败次数和耗时。
     */
    @Test
    void recordFailureShouldAccumulateStageStats() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        metrics.recordFailure(TransactionStage.BUFFERED_PREWRITE, 500L);

        TransactionStageStats stats = metrics.snapshot(TransactionStage.BUFFERED_PREWRITE);
        assertEquals(1L, stats.invocationCount());
        assertEquals(0L, stats.successCount());
        assertEquals(1L, stats.failureCount());
        assertEquals(500L, stats.totalNanos());
    }

    /**
     * 验证未记录阶段返回空快照。
     */
    @Test
    void unrecordedStageShouldReturnZeroSnapshot() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        TransactionStageStats stats = metrics.snapshot(TransactionStage.ASYNC_PARTICIPANT_COMMIT);

        assertEquals(0L, stats.invocationCount());
        assertEquals(0L, stats.successCount());
        assertEquals(0L, stats.failureCount());
        assertEquals(0L, stats.totalNanos());
        assertEquals(0L, stats.averageNanos());
    }
}

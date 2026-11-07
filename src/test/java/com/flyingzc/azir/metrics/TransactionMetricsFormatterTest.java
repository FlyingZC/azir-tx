package com.flyingzc.azir.metrics;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务指标格式化测试。
 */
class TransactionMetricsFormatterTest {
    /**
     * 验证只输出已经记录过的事务阶段。
     */
    @Test
    void formatNonEmptyShouldSkipEmptyStages() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        List<String> lines = TransactionMetricsFormatter.formatNonEmpty(metrics);

        assertTrue(lines.isEmpty());
    }

    /**
     * 验证阶段指标会格式化为稳定的基准测试输出行。
     */
    @Test
    void formatNonEmptyShouldRenderStableStageLine() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        metrics.recordSuccess(TransactionStage.COMMIT_TSO, 1_500_000L);
        metrics.recordFailure(TransactionStage.COMMIT_TSO, 500_000L);

        List<String> lines = TransactionMetricsFormatter.formatNonEmpty(metrics);

        assertEquals(1, lines.size());
        assertEquals(
                "TX_STAGE stage=COMMIT_TSO invocations=2 success=1 failure=1 total_ms=2.00 avg_us=1000.00",
                lines.getFirst()
        );
    }
}

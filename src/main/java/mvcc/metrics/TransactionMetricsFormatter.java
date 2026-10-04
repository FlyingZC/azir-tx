package mvcc.metrics;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 事务指标格式化工具，用于基准测试输出可解析的阶段耗时行。
 */
public final class TransactionMetricsFormatter {
    /**
     * 构造事务指标格式化工具。
     */
    private TransactionMetricsFormatter() {
    }

    /**
     * 格式化所有非空阶段指标。
     */
    public static List<String> formatNonEmpty(InMemoryTransactionMetrics metrics) {
        Objects.requireNonNull(metrics, "metrics");
        return metrics.snapshots()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().invocationCount() > 0L)
                .sorted(MapEntryStageComparator.INSTANCE)
                .map(entry -> format(entry.getKey(), entry.getValue()))
                .toList();
    }

    /**
     * 格式化单个事务阶段指标。
     */
    private static String format(TransactionStage stage, TransactionStageStats stats) {
        return String.format(
                Locale.ROOT,
                "TX_STAGE stage=%s invocations=%d success=%d failure=%d total_ms=%.2f avg_us=%.2f",
                stage.name(),
                stats.invocationCount(),
                stats.successCount(),
                stats.failureCount(),
                stats.totalNanos() / 1_000_000.0,
                stats.averageNanos() / 1_000.0
        );
    }

    /**
     * 按枚举声明顺序排序阶段输出。
     */
    private enum MapEntryStageComparator implements java.util.Comparator<java.util.Map.Entry<TransactionStage, TransactionStageStats>> {
        INSTANCE;

        /**
         * 比较两个阶段指标条目。
         */
        @Override
        public int compare(java.util.Map.Entry<TransactionStage, TransactionStageStats> left,
                           java.util.Map.Entry<TransactionStage, TransactionStageStats> right) {
            return Integer.compare(left.getKey().ordinal(), right.getKey().ordinal());
        }
    }
}

package mvcc.metrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 线程安全的内存事务指标聚合器。
 */
public final class InMemoryTransactionMetrics implements TransactionMetrics {
    private final Map<TransactionStage, MutableStageStats> statsByStage = new EnumMap<>(TransactionStage.class);

    /**
     * 构造内存事务指标聚合器。
     */
    public InMemoryTransactionMetrics() {
        for (TransactionStage stage : TransactionStage.values()) {
            statsByStage.put(stage, new MutableStageStats());
        }
    }

    /**
     * 记录阶段成功执行。
     */
    @Override
    public void recordSuccess(TransactionStage stage, long elapsedNanos) {
        statsByStage.get(stage).recordSuccess(elapsedNanos);
    }

    /**
     * 记录阶段执行失败。
     */
    @Override
    public void recordFailure(TransactionStage stage, long elapsedNanos) {
        statsByStage.get(stage).recordFailure(elapsedNanos);
    }

    /**
     * 返回指定阶段的指标快照。
     */
    public TransactionStageStats snapshot(TransactionStage stage) {
        return statsByStage.get(stage).snapshot();
    }

    /**
     * 返回所有阶段的指标快照。
     */
    public Map<TransactionStage, TransactionStageStats> snapshots() {
        EnumMap<TransactionStage, TransactionStageStats> snapshots = new EnumMap<>(TransactionStage.class);
        for (Map.Entry<TransactionStage, MutableStageStats> entry : statsByStage.entrySet()) {
            snapshots.put(entry.getKey(), entry.getValue().snapshot());
        }
        return Map.copyOf(snapshots);
    }

    /**
     * 可并发更新的阶段统计。
     */
    private static final class MutableStageStats {
        private final LongAdder invocationCount = new LongAdder();
        private final LongAdder successCount = new LongAdder();
        private final LongAdder failureCount = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();

        /**
         * 记录一次成功阶段。
         */
        private void recordSuccess(long elapsedNanos) {
            invocationCount.increment();
            successCount.increment();
            totalNanos.add(Math.max(0L, elapsedNanos));
        }

        /**
         * 记录一次失败阶段。
         */
        private void recordFailure(long elapsedNanos) {
            invocationCount.increment();
            failureCount.increment();
            totalNanos.add(Math.max(0L, elapsedNanos));
        }

        /**
         * 创建当前统计快照。
         */
        private TransactionStageStats snapshot() {
            return new TransactionStageStats(
                    invocationCount.sum(),
                    successCount.sum(),
                    failureCount.sum(),
                    totalNanos.sum()
            );
        }
    }
}

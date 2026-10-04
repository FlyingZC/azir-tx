package mvcc.metrics;

/**
 * 默认空事务指标实现，避免未启用指标时引入聚合成本。
 */
public enum NoOpTransactionMetrics implements TransactionMetrics {
    INSTANCE;

    /**
     * 忽略阶段成功记录。
     */
    @Override
    public void recordSuccess(TransactionStage stage, long elapsedNanos) {
    }

    /**
     * 忽略阶段失败记录。
     */
    @Override
    public void recordFailure(TransactionStage stage, long elapsedNanos) {
    }
}

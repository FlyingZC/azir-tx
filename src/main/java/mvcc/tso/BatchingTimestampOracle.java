package mvcc.tso;

import java.util.Objects;

/**
 * 保留批量包装器 API 的兼容入口，实际逐次委托权威 TSO。
 * 客户端预取不仅破坏跨实例实时顺序，还会把其他调用者的号错误包含在区间中，因此停用缓存。
 */
public final class BatchingTimestampOracle implements TimestampOracle {
    private final TimestampOracle delegate;

    /**
     * 构造 BatchingTimestampOracle 实例。
     */
    public BatchingTimestampOracle(TimestampOracle delegate, int batchSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
    }

    /**
     * 分配下一个全局单调递增时间戳。
     */
    @Override
    public long nextTimestamp() {
        return delegate.nextTimestamp();
    }
}

package mvcc.tso;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * 带调用计数的时间戳分配器包装器。
 */
public final class CountingTimestampOracle implements TimestampOracle {
    private final TimestampOracle delegate;
    private final LongAdder invocationCount = new LongAdder();

    /**
     * 构造 CountingTimestampOracle 实例。
     */
    public CountingTimestampOracle(TimestampOracle delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * 分配下一个全局单调递增时间戳。
     */
    @Override
    public long nextTimestamp() {
        invocationCount.increment();
        return delegate.nextTimestamp();
    }

    /**
     * 返回底层时间戳分配器调用次数。
     */
    public long invocationCount() {
        return invocationCount.sum();
    }
}

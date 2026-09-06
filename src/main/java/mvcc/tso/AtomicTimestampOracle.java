package mvcc.tso;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于原子变量的本地时间戳分配器。
 */
public final class AtomicTimestampOracle implements TimestampOracle {
    private final AtomicLong sequence;

    /**
     * 构造 AtomicTimestampOracle 实例。
     */
    public AtomicTimestampOracle(long initialValue) {
        this.sequence = new AtomicLong(initialValue);
    }

    /**
     * 分配下一个全局单调递增时间戳。
     */
    @Override
    public long nextTimestamp() {
        return sequence.incrementAndGet();
    }
}

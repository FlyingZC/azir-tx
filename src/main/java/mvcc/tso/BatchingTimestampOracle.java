package mvcc.tso;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量缓存时间戳的分配器，用于降低底层 TSO 调用次数。
 */
public final class BatchingTimestampOracle implements TimestampOracle {
    private final TimestampOracle delegate;
    private final int batchSize;
    private final AtomicLong current = new AtomicLong();
    private final AtomicLong max = new AtomicLong(-1L);

    /**
     * 构造 BatchingTimestampOracle 实例。
     */
    public BatchingTimestampOracle(TimestampOracle delegate, int batchSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = batchSize;
        this.current.set(1L);
        this.max.set(0L);
    }

    /**
     * 分配下一个全局单调递增时间戳。
     */
    @Override
    public long nextTimestamp() {
        while (true) {
            long next = current.getAndIncrement();
            if (next <= max.get()) {
                return next;
            }
            refill();
        }
    }

    /**
     * 从底层分配器补充本地时间戳缓存。
     */
    private synchronized void refill() {
        if (current.get() <= max.get()) {
            return;
        }
        long start = delegate.nextTimestamp();
        long end = start;
        for (int i = 1; i < batchSize; i++) {
            end = delegate.nextTimestamp();
        }
        current.set(start);
        max.set(end);
    }
}

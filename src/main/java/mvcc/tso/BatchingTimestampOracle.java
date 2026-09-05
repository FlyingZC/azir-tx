package mvcc.tso;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class BatchingTimestampOracle implements TimestampOracle {
    private final TimestampOracle delegate;
    private final int batchSize;
    private final AtomicLong current = new AtomicLong();
    private final AtomicLong max = new AtomicLong(-1L);

    public BatchingTimestampOracle(TimestampOracle delegate, int batchSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = batchSize;
        this.current.set(1L);
        this.max.set(0L);
    }

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

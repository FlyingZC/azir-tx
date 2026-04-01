package mvcc.tso;

import java.util.concurrent.atomic.AtomicLong;

public final class AtomicTimestampOracle implements TimestampOracle {
    private final AtomicLong sequence;

    public AtomicTimestampOracle(long initialValue) {
        this.sequence = new AtomicLong(initialValue);
    }

    @Override
    public long nextTimestamp() {
        return sequence.incrementAndGet();
    }
}

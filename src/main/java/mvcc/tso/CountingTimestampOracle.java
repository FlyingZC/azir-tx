package mvcc.tso;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public final class CountingTimestampOracle implements TimestampOracle {
    private final TimestampOracle delegate;
    private final LongAdder invocationCount = new LongAdder();

    public CountingTimestampOracle(TimestampOracle delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public long nextTimestamp() {
        invocationCount.increment();
        return delegate.nextTimestamp();
    }

    public long invocationCount() {
        return invocationCount.sum();
    }
}

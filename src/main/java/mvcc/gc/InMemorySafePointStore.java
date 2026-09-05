package mvcc.gc;

import java.util.concurrent.atomic.AtomicLong;

public final class InMemorySafePointStore implements SafePointStore {
    private final AtomicLong safePoint;

    public InMemorySafePointStore(long initialSafePoint) {
        this.safePoint = new AtomicLong(initialSafePoint);
    }

    @Override
    public long currentSafePoint() {
        return safePoint.get();
    }

    @Override
    public long advanceTo(long candidateSafePoint) {
        return safePoint.updateAndGet(current -> Math.max(current, candidateSafePoint));
    }
}

package mvcc.gc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于内存的安全点存储实现。
 */
public final class InMemorySafePointStore implements SafePointStore {
    private final AtomicLong safePoint;

    /**
     * 构造 InMemorySafePointStore 实例。
     */
    public InMemorySafePointStore(long initialSafePoint) {
        this.safePoint = new AtomicLong(initialSafePoint);
    }

    /**
     * 返回当前安全点时间戳。
     */
    @Override
    public long currentSafePoint() {
        return safePoint.get();
    }

    /**
     * 尝试将安全点推进到候选时间戳。
     */
    @Override
    public long advanceTo(long candidateSafePoint) {
        return safePoint.updateAndGet(current -> Math.max(current, candidateSafePoint));
    }
}

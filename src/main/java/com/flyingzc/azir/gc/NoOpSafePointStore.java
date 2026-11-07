package com.flyingzc.azir.gc;

/**
 * 不会推进安全点的空实现。
 */
public final class NoOpSafePointStore implements SafePointStore {
    public static final NoOpSafePointStore INSTANCE = new NoOpSafePointStore();

    /**
     * 构造 NoOpSafePointStore 实例。
     */
    private NoOpSafePointStore() {
    }

    /**
     * 返回当前安全点时间戳。
     */
    @Override
    public long currentSafePoint() {
        return 0L;
    }

    /**
     * 尝试将安全点推进到候选时间戳。
     */
    @Override
    public long advanceTo(long candidateSafePoint) {
        return candidateSafePoint;
    }
}

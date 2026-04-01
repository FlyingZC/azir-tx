package mvcc.gc;

public final class NoOpSafePointStore implements SafePointStore {
    public static final NoOpSafePointStore INSTANCE = new NoOpSafePointStore();

    private NoOpSafePointStore() {
    }

    @Override
    public long currentSafePoint() {
        return 0L;
    }

    @Override
    public long advanceTo(long candidateSafePoint) {
        return candidateSafePoint;
    }
}

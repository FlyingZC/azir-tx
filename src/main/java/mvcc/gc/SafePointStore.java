package mvcc.gc;

public interface SafePointStore {
    long currentSafePoint();

    long advanceTo(long candidateSafePoint);
}

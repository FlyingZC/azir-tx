package mvcc.txn;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public record LockWaitPolicy(ConflictMode conflictMode,
                             long lockWaitTimeoutMs,
                             long initialBackoffMs,
                             long maxBackoffMs,
                             double backoffMultiplier,
                             boolean jitter) {
    public LockWaitPolicy {
        Objects.requireNonNull(conflictMode, "conflictMode");
        if (lockWaitTimeoutMs < 0) {
            throw new IllegalArgumentException("lockWaitTimeoutMs must be >= 0");
        }
        if (initialBackoffMs < 0) {
            throw new IllegalArgumentException("initialBackoffMs must be >= 0");
        }
        if (maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException("maxBackoffMs must be >= initialBackoffMs");
        }
        if (backoffMultiplier < 1.0d) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
        }
    }

    public static LockWaitPolicy failFast() {
        return new LockWaitPolicy(ConflictMode.FAIL_FAST, 0L, 0L, 0L, 1.0d, false);
    }

    public static LockWaitPolicy mysqlLikeDefaults() {
        return new LockWaitPolicy(ConflictMode.WAIT, 5_000L, 5L, 100L, 2.0d, true);
    }

    long nextDelayMillis(int attempt) {
        if (conflictMode == ConflictMode.FAIL_FAST || maxBackoffMs == 0L) {
            return 0L;
        }
        double computed = initialBackoffMs * Math.pow(backoffMultiplier, Math.max(0, attempt));
        long capped = Math.min(maxBackoffMs, Math.max(initialBackoffMs, (long) computed));
        if (!jitter || capped <= 1L) {
            return capped;
        }
        return ThreadLocalRandom.current().nextLong(1L, capped + 1L);
    }
}

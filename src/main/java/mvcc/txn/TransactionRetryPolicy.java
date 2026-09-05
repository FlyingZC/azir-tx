package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public record TransactionRetryPolicy(int maxAttempts,
                                     long initialBackoffMs,
                                     long maxBackoffMs,
                                     double backoffMultiplier,
                                     boolean jitter) {
    public TransactionRetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
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

    public static TransactionRetryPolicy noRetry() {
        return new TransactionRetryPolicy(1, 0L, 0L, 1.0d, false);
    }

    public static TransactionRetryPolicy mysqlLikeDefaults() {
        return new TransactionRetryPolicy(8, 5L, 100L, 2.0d, true);
    }

    public boolean shouldRetry(MvccException exception, int attempt) {
        Objects.requireNonNull(exception, "exception");
        if (attempt >= maxAttempts) {
            return false;
        }
        return switch (exception.errorCode()) {
            case LOCK_CONFLICT, LOCK_WAIT_TIMEOUT, WRITE_CONFLICT -> exception.retryable();
            default -> false;
        };
    }

    long nextDelayMillis(int attempt) {
        if (maxBackoffMs == 0L) {
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

package mvcc.txn;

import mvcc.common.MvccException;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 事务重试策略，定义重试次数和退避规则。
 */
public record TransactionRetryPolicy(int maxAttempts,
                                     long initialBackoffMs,
                                     long maxBackoffMs,
                                     double backoffMultiplier,
                                     boolean jitter) {
    /**
     * 构造 TransactionRetryPolicy 实例。
     */
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

    /**
     * 返回不重试策略。
     */
    public static TransactionRetryPolicy noRetry() {
        return new TransactionRetryPolicy(1, 0L, 0L, 1.0d, false);
    }

    /**
     * 返回接近 MySQL 行为的默认配置。
     */
    public static TransactionRetryPolicy mysqlLikeDefaults() {
        return new TransactionRetryPolicy(8, 5L, 100L, 2.0d, true);
    }

    /**
     * 判断指定异常和尝试次数是否允许重试。
     */
    public boolean shouldRetry(MvccException exception, int attempt) {
        Objects.requireNonNull(exception, "exception");
        if (attempt >= maxAttempts) {
            return false;
        }
        return TransientStorageConflictDetector.isTransientConflict(exception);
    }

    /**
     * 计算下一次重试前的等待时间。
     */
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

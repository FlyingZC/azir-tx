/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.MvccException;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Transaction retry policy, defining the retry frequency and avoidance rule.
 *
 * @param maxAttempts max attempts
 * @param initialBackoffMs initial backoff ms
 * @param maxBackoffMs max backoff ms
 * @param backoffMultiplier backoff multiplier
 * @param jitter jitter
 */
public record TransactionRetryPolicy(int maxAttempts,
        long initialBackoffMs,
        long maxBackoffMs,
        double backoffMultiplier,
        boolean jitter) {

    /**
     * Construct a TransactionRetryPolicy instance.
     *
     * @param maxAttempts max attempts
     * @param initialBackoffMs initial backoff ms
     * @param maxBackoffMs max backoff ms
     * @param backoffMultiplier backoff multiplier
     * @param jitter jitter
     * @throws IllegalArgumentException if the operation cannot complete
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
     * Returns no retry policy.
     *
     * @return the operation result
     */
    public static TransactionRetryPolicy noRetry() {
        return new TransactionRetryPolicy(1, 0L, 0L, 1.0d, false);
    }

    /**
     * Return defaults approximating MySQL conflict handling.
     *
     * @return the operation result
     */
    public static TransactionRetryPolicy mysqlLikeDefaults() {
        return new TransactionRetryPolicy(8, 5L, 100L, 2.0d, true);
    }

    /**
     * Determines whether to specify an anomaly and the number of attempts that allow retry.
     *
     * @param exception exception
     * @param attempt attempt
     * @return the operation result
     */
    public boolean shouldRetry(final MvccException exception, final int attempt) {
        Objects.requireNonNull(exception, "exception");
        if (attempt >= maxAttempts) {
            return false;
        }
        return TransientStorageConflictDetector.isTransientConflict(exception);
    }

    /**
     * Compute the delay before the next retry.
     *
     * @param attempt attempt
     * @return the operation result
     */
    long nextDelayMillis(final int attempt) {
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

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

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Locks the waiting policy, defines the conflict waiting mode and timeout time.
 *
 * @param conflictMode conflict mode
 * @param lockWaitTimeoutMs lock wait timeout ms
 * @param initialBackoffMs initial backoff ms
 * @param maxBackoffMs max backoff ms
 * @param backoffMultiplier backoff multiplier
 * @param jitter jitter
 */
public record LockWaitPolicy(ConflictMode conflictMode,
        long lockWaitTimeoutMs,
        long initialBackoffMs,
        long maxBackoffMs,
        double backoffMultiplier,
        boolean jitter) {

    /**
     * Construct a LockWaitPolicy instance.
     *
     * @param conflictMode conflict mode
     * @param lockWaitTimeoutMs lock wait timeout ms
     * @param initialBackoffMs initial backoff ms
     * @param maxBackoffMs max backoff ms
     * @param backoffMultiplier backoff multiplier
     * @param jitter jitter
     * @throws IllegalArgumentException if the operation cannot complete
     */
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

    /**
     * Return fail-fast options.
     *
     * @return the operation result
     */
    public static LockWaitPolicy failFast() {
        return new LockWaitPolicy(ConflictMode.FAIL_FAST, 0L, 0L, 0L, 1.0d, false);
    }

    /**
     * Return defaults approximating MySQL conflict handling.
     *
     * @return the operation result
     */
    public static LockWaitPolicy mysqlLikeDefaults() {
        return new LockWaitPolicy(ConflictMode.WAIT, 5_000L, 5L, 100L, 2.0d, true);
    }

    /**
     * Compute the delay before the next retry.
     *
     * @param attempt attempt
     * @return the operation result
     */
    long nextDelayMillis(final int attempt) {
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

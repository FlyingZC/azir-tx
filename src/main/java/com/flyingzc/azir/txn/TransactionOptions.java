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

/**
 * Transaction executes the option to centralize lock-and-wait, retry and cross-shard commit policies.
 *
 * @param lockWaitPolicy lock wait policy
 * @param retryPolicy retry policy
 * @param crossShardCommitPolicy cross shard commit policy
 * @param bufferWrites buffer writes
 * @param lazyBeginMetadata lazy begin metadata
 * @param deferIntentCleanup defer intent cleanup
 * @param limits limits
 */
public record TransactionOptions(LockWaitPolicy lockWaitPolicy,
        TransactionRetryPolicy retryPolicy,
        CrossShardCommitPolicy crossShardCommitPolicy,
        boolean bufferWrites,
        boolean lazyBeginMetadata,
        boolean deferIntentCleanup,
        TransactionLimits limits) {

    /**
     * Construct a TransactionOptions instance.
     *
     * @param lockWaitPolicy lock wait policy
     * @param retryPolicy retry policy
     * @param crossShardCommitPolicy cross shard commit policy
     * @param bufferWrites buffer writes
     * @param lazyBeginMetadata lazy begin metadata
     * @param deferIntentCleanup defer intent cleanup
     * @param limits limits
     */
    public TransactionOptions {
        Objects.requireNonNull(lockWaitPolicy, "lockWaitPolicy");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        Objects.requireNonNull(crossShardCommitPolicy, "crossShardCommitPolicy");
        Objects.requireNonNull(limits, "limits");
    }

    /**
     * Keeps the original configuration entry and applies the bounded resource default.
     *
     * @param lockWaitPolicy lock wait policy
     * @param retryPolicy retry policy
     * @param crossShardCommitPolicy cross shard commit policy
     * @param bufferWrites buffer writes
     * @param lazyBeginMetadata lazy begin metadata
     * @param deferIntentCleanup defer intent cleanup
     */
    public TransactionOptions(final LockWaitPolicy lockWaitPolicy, final TransactionRetryPolicy retryPolicy,
                              final CrossShardCommitPolicy crossShardCommitPolicy, final boolean bufferWrites,
                              final boolean lazyBeginMetadata, final boolean deferIntentCleanup) {
        this(lockWaitPolicy, retryPolicy, crossShardCommitPolicy, bufferWrites, lazyBeginMetadata,
                deferIntentCleanup, TransactionLimits.defaults());
    }

    /**
     * Returns the default transaction option.
     *
     * @return the operation result
     */
    public static TransactionOptions defaults() {
        return safePerformanceDefaults();
    }

    /**
     * Returns the ACID security performance priority default configuration.
     *
     * @return the operation result
     */
    public static TransactionOptions safePerformanceDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.mysqlLikeDefaults(),
                TransactionRetryPolicy.mysqlLikeDefaults(),
                CrossShardCommitPolicy.parallel(),
                true,
                false,
                false);
    }

    /**
     * Returns the visible configuration that failed.
     *
     * @return the operation result
     */
    public static TransactionOptions failFastDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(),
                false,
                false,
                false);
    }

    /**
     * Return defaults approximating MySQL conflict handling.
     *
     * @return the operation result
     */
    public static TransactionOptions mysqlLikeDefaults() {
        return new TransactionOptions(
                LockWaitPolicy.mysqlLikeDefaults(),
                TransactionRetryPolicy.mysqlLikeDefaults(),
                CrossShardCommitPolicy.asyncParallel(16),
                true,
                true,
                false);
    }
}

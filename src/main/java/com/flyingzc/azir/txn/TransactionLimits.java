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

/**
 * Transaction and background implemented resources budget, avoiding slow shard leading to a backlog of unbounded.
 *
 * @param timeoutMillis timeout millis
 * @param maxWriteKeys max write keys
 * @param maxWriteBytes max write bytes
 * @param maxActiveTransactions max active transactions
 * @param shardConcurrency shard concurrency
 * @param asyncQueueCapacity async queue capacity
 * @param scanPageSize scan page size
 * @param shutdownTimeoutMillis shutdown timeout millis
 * @param maxMaterializedRows max materialized rows
 * @param maxMaterializedBytes max materialized bytes
 */
public record TransactionLimits(long timeoutMillis, int maxWriteKeys, long maxWriteBytes,
        int maxActiveTransactions, int shardConcurrency,
        int asyncQueueCapacity, int scanPageSize, long shutdownTimeoutMillis,
        int maxMaterializedRows, long maxMaterializedBytes) {

    /**
     * Validate All budget is a limited positive number.
     *
     * @param timeoutMillis timeout millis
     * @param maxWriteKeys max write keys
     * @param maxWriteBytes max write bytes
     * @param maxActiveTransactions max active transactions
     * @param shardConcurrency shard concurrency
     * @param asyncQueueCapacity async queue capacity
     * @param scanPageSize scan page size
     * @param shutdownTimeoutMillis shutdown timeout millis
     * @param maxMaterializedRows max materialized rows
     * @param maxMaterializedBytes max materialized bytes
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public TransactionLimits {
        if (timeoutMillis < 1 || timeoutMillis > 86_400_000L || maxWriteKeys < 1 || maxWriteBytes < 1
                || maxActiveTransactions < 1 || shardConcurrency < 1 || asyncQueueCapacity < 1
                || scanPageSize < 1 || scanPageSize > 4096 || shutdownTimeoutMillis < 1 || shutdownTimeoutMillis > 86_400_000L
                || maxMaterializedRows < 1 || maxMaterializedBytes < 1) {
            throw new IllegalArgumentException("invalid transaction resource limits");
        }
    }

    /**
     * Complement a separate materialization reading budget for the original resource budget construction portal.
     *
     * @param timeoutMillis timeout millis
     * @param maxWriteKeys max write keys
     * @param maxWriteBytes max write bytes
     * @param maxActiveTransactions max active transactions
     * @param shardConcurrency shard concurrency
     * @param asyncQueueCapacity async queue capacity
     * @param scanPageSize scan page size
     * @param shutdownTimeoutMillis shutdown timeout millis
     */
    public TransactionLimits(final long timeoutMillis, final int maxWriteKeys, final long maxWriteBytes,
                             final int maxActiveTransactions, final int shardConcurrency, final int asyncQueueCapacity,
                             final int scanPageSize, final long shutdownTimeoutMillis) {
        this(timeoutMillis, maxWriteKeys, maxWriteBytes, maxActiveTransactions, shardConcurrency,
                asyncQueueCapacity, scanPageSize, shutdownTimeoutMillis, 100_000, 64L * 1024 * 1024);
    }

    /**
     * Returns the conservative bounded default value.
     *
     * @return the operation result
     */
    public static TransactionLimits defaults() {
        return new TransactionLimits(60_000L, 10_000, 16L * 1024 * 1024, 1024, 16, 1024, 256, 5_000L);
    }
}

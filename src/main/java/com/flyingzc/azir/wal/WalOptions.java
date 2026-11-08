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

package com.flyingzc.azir.wal;

/**
 * The bounded resource allocation for a stand-alone WAL mode is in milliseconds and the capacity unit is byte.
 *
 * @param maxActiveTransactions max active transactions
 * @param maxWriteKeys max write keys
 * @param maxWriteBytes max write bytes
 * @param queueCapacity queue capacity
 * @param commitBatchSize commit batch size
 * @param batchDelayMillis batch delay millis
 * @param maxWalBytes max wal bytes
 * @param maxMemoryBytes max memory bytes
 * @param transactionTimeoutMillis transaction timeout millis
 * @param flushBatchSize flush batch size
 * @param flushIntervalMillis flush interval millis
 * @param maxHotCacheBytes max hot cache bytes
 * @param maxMaterializationLag max materialization lag
 */
public record WalOptions(int maxActiveTransactions, int maxWriteKeys, long maxWriteBytes,
        int queueCapacity, int commitBatchSize, long batchDelayMillis,
        long maxWalBytes, long maxMemoryBytes, long transactionTimeoutMillis,
        int flushBatchSize, long flushIntervalMillis,
        long maxHotCacheBytes, long maxMaterializationLag) {

    /**
     * Keeps the new mode called compatible, with the default hotspot budget not exceeding a quarter of the total memory or 8 MiB.
     *
     * @param maxActiveTransactions max active transactions
     * @param maxWriteKeys max write keys
     * @param maxWriteBytes max write bytes
     * @param queueCapacity queue capacity
     * @param commitBatchSize commit batch size
     * @param batchDelayMillis batch delay millis
     * @param maxWalBytes max wal bytes
     * @param maxMemoryBytes max memory bytes
     * @param transactionTimeoutMillis transaction timeout millis
     * @param flushBatchSize flush batch size
     * @param flushIntervalMillis flush interval millis
     */
    public WalOptions(final int maxActiveTransactions, final int maxWriteKeys, final long maxWriteBytes,
                      final int queueCapacity, final int commitBatchSize, final long batchDelayMillis,
                      final long maxWalBytes, final long maxMemoryBytes, final long transactionTimeoutMillis,
                      final int flushBatchSize, final long flushIntervalMillis) {
        this(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity, commitBatchSize,
                batchDelayMillis, maxWalBytes, maxMemoryBytes, transactionTimeoutMillis,
                flushBatchSize, flushIntervalMillis, Math.min(8L * 1024 * 1024, maxMemoryBytes / 4), 100_000);
    }

    /**
     * Rejects an unreasonable configuration, and single and batch logs are protected by the fixed format ceiling.
     *
     * @param maxActiveTransactions max active transactions
     * @param maxWriteKeys max write keys
     * @param maxWriteBytes max write bytes
     * @param queueCapacity queue capacity
     * @param commitBatchSize commit batch size
     * @param batchDelayMillis batch delay millis
     * @param maxWalBytes max wal bytes
     * @param maxMemoryBytes max memory bytes
     * @param transactionTimeoutMillis transaction timeout millis
     * @param flushBatchSize flush batch size
     * @param flushIntervalMillis flush interval millis
     * @param maxHotCacheBytes max hot cache bytes
     * @param maxMaterializationLag max materialization lag
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalOptions {
        if (maxActiveTransactions < 1 || maxWriteKeys < 1 || maxWriteKeys > 100_000
                || maxWriteBytes < 1 || maxWriteBytes > 8 * 1024 * 1024
                || queueCapacity < 1 || commitBatchSize < 1 || commitBatchSize > 1024
                || batchDelayMillis < 0 || batchDelayMillis > 1000
                || maxWalBytes < 1024 || maxMemoryBytes < 1
                || transactionTimeoutMillis < 1 || transactionTimeoutMillis > 86_400_000
                || flushBatchSize < 1 || flushBatchSize > 1024 || flushIntervalMillis < 1
                || maxHotCacheBytes < 0 || maxHotCacheBytes > maxMemoryBytes || maxMaterializationLag < 1) {
            throw new IllegalArgumentException("invalid WAL mode limits");
        }
    }

    /**
     * Returns the default limit for a single writer's MVP.
     *
     * @return the operation result
     */
    public static WalOptions defaults() {
        return new WalOptions(4096, 1024, 1024 * 1024, 4096, 64, 1,
                256L * 1024 * 1024, 64L * 1024 * 1024, 60_000, 1024, 1);
    }

    /**
     * Creates a configuration to modify the transaction period only, supply and confirm acceptance use.
     *
     * @param millis millis
     * @return the operation result
     */
    public WalOptions withTimeout(final long millis) {
        return new WalOptions(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity,
                commitBatchSize, batchDelayMillis, maxWalBytes, maxMemoryBytes, millis,
                flushBatchSize, flushIntervalMillis, maxHotCacheBytes, maxMaterializationLag);
    }

    /**
     * Adjusts to phase out the hotspot cache. Zero means that only materialization increments are retained, and cold reading directly accesses backends.
     *
     * @param bytes bytes
     * @return the operation result
     */
    public WalOptions withHotCacheBytes(final long bytes) {
        return new WalOptions(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity,
                commitBatchSize, batchDelayMillis, maxWalBytes, maxMemoryBytes, transactionTimeoutMillis,
                flushBatchSize, flushIntervalMillis, bytes, maxMaterializationLag);
    }

    /**
     * Limit the slowest shard number of commit s not yet materialization to add a frontal back pressure to WAL at budget.
     *
     * @param commits commits
     * @return the operation result
     */
    public WalOptions withMaterializationLag(final long commits) {
        return new WalOptions(maxActiveTransactions, maxWriteKeys, maxWriteBytes, queueCapacity,
                commitBatchSize, batchDelayMillis, maxWalBytes, maxMemoryBytes, transactionTimeoutMillis,
                flushBatchSize, flushIntervalMillis, maxHotCacheBytes, commits);
    }
}

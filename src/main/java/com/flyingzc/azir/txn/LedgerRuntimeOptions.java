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
 * Recovery, GC and batch decision resources budget when new book is active.
 *
 * @param recoveryBatchSize recovery batch size
 * @param gcBatchSize gc batch size
 * @param recoveryIntervalMillis recovery interval millis
 * @param gcIntervalMillis gc interval millis
 * @param batchControlCommit batch control commit
 * @param batchWorkers batch workers
 * @param directSingleShard direct single shard
 * @param batchBegin batch begin
 * @param beginWorkers begin workers
 * @param coordinatorSnapshots coordinator snapshots
 * @param coordinatorLeaseMillis coordinator lease millis
 * @param coordinatorRefreshIntervalMillis coordinator refresh interval millis
 */
public record LedgerRuntimeOptions(int recoveryBatchSize, int gcBatchSize,
        long recoveryIntervalMillis, long gcIntervalMillis,
        boolean batchControlCommit, int batchWorkers,
        boolean directSingleShard,
        boolean batchBegin, int beginWorkers,
        boolean coordinatorSnapshots, long coordinatorLeaseMillis,
        long coordinatorRefreshIntervalMillis) {

    /**
     * Rejects unbounded batch, too short or off-cycle, and invalid batch lines of work.
     *
     * @param recoveryBatchSize recovery batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchControlCommit batch control commit
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     * @param batchBegin batch begin
     * @param beginWorkers begin workers
     * @param coordinatorSnapshots coordinator snapshots
     * @param coordinatorLeaseMillis coordinator lease millis
     * @param coordinatorRefreshIntervalMillis coordinator refresh interval millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public LedgerRuntimeOptions {
        if (recoveryBatchSize < 1 || recoveryBatchSize > 4096 || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || gcIntervalMillis < 1
                || batchWorkers < 1 || batchWorkers > 8
                || beginWorkers < 1 || beginWorkers > 8
                || coordinatorLeaseMillis < 1 || coordinatorRefreshIntervalMillis < 1
                || (coordinatorSnapshots && batchBegin)) {
            throw new IllegalArgumentException("invalid ledger runtime resource limits");
        }
    }

    /**
     * Compatible with a call already made, and the coordinator snapshot optimization needs to be visible.
     *
     * @param recoveryBatchSize recovery batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchControlCommit batch control commit
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     * @param batchBegin batch begin
     * @param beginWorkers begin workers
     */
    public LedgerRuntimeOptions(final int recoveryBatchSize, final int gcBatchSize,
                                final long recoveryIntervalMillis, final long gcIntervalMillis,
                                final boolean batchControlCommit, final int batchWorkers,
                                final boolean directSingleShard, final boolean batchBegin, final int beginWorkers) {
        this(recoveryBatchSize, gcBatchSize, recoveryIntervalMillis, gcIntervalMillis,
                batchControlCommit, batchWorkers, directSingleShard, batchBegin, beginWorkers,
                false, 120_000, 1_000);
    }

    /**
     * Returns bounded background rotations with two batch decision threads, single shard direct commit is not enabled by default.
     *
     * @return the operation result
     */
    public static LedgerRuntimeOptions defaults() {
        return new LedgerRuntimeOptions(128, 128, 100, 1_000, true, 2, false, false, 1);
    }
}

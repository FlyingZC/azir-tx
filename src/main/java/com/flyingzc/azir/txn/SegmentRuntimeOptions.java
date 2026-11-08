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
 * The visible bounded resource budget for segment owner, publisher, recovery and GC.
 *
 * @param segmentSize segment size
 * @param ownerLeaseMillis owner lease millis
 * @param ownerRefreshMillis owner refresh millis
 * @param idleSealMillis idle seal millis
 * @param recoveryBatchSize recovery batch size
 * @param publicationBatchSize publication batch size
 * @param gcBatchSize gc batch size
 * @param recoveryIntervalMillis recovery interval millis
 * @param publicationIntervalMillis publication interval millis
 * @param gcIntervalMillis gc interval millis
 * @param batchDecisions batch decisions
 * @param batchWorkers batch workers
 * @param directSingleShard direct single shard
 * @param eventDrivenPublication event driven publication
 * @param overlapMaterialization overlap materialization
 * @param fastMaterialization fast materialization
 * @param fastCommitProcedure fast commit procedure
 */
public record SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
        long ownerRefreshMillis, long idleSealMillis,
        int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
        long recoveryIntervalMillis, long publicationIntervalMillis,
        long gcIntervalMillis,
        boolean batchDecisions, int batchWorkers,
        boolean directSingleShard, boolean eventDrivenPublication,
        boolean overlapMaterialization, boolean fastMaterialization,
        boolean fastCommitProcedure) {

    /**
     * Rejects unbounded, short-term lease balances and invalid background cycle.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchDecisions batch decisions
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     * @param eventDrivenPublication event driven publication
     * @param overlapMaterialization overlap materialization
     * @param fastMaterialization fast materialization
     * @param fastCommitProcedure fast commit procedure
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public SegmentRuntimeOptions {
        if (segmentSize < 1 || segmentSize > 4096
                || ownerLeaseMillis < 3 || ownerLeaseMillis > 86_400_000
                || ownerRefreshMillis < 1 || ownerRefreshMillis >= ownerLeaseMillis / 2
                || idleSealMillis < 1
                || recoveryBatchSize < 1 || recoveryBatchSize > 4096
                || publicationBatchSize < 1 || publicationBatchSize > 4096
                || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || publicationIntervalMillis < 1
                || gcIntervalMillis < 1 || batchWorkers < 1 || batchWorkers > 8
                || fastMaterialization && (batchDecisions || overlapMaterialization)) {
            throw new IllegalArgumentException("invalid segmented runtime resource limits");
        }
    }

    /**
     * Maintains the centre receipt experimental parameter, and the second version of the shard routine is still controlled by the visible option.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchDecisions batch decisions
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     * @param eventDrivenPublication event driven publication
     * @param overlapMaterialization overlap materialization
     * @param fastMaterialization fast materialization
     */
    public SegmentRuntimeOptions(final int segmentSize, final long ownerLeaseMillis,
                                 final long ownerRefreshMillis, final long idleSealMillis,
                                 final int recoveryBatchSize, final int publicationBatchSize, final int gcBatchSize,
                                 final long recoveryIntervalMillis, final long publicationIntervalMillis,
                                 final long gcIntervalMillis, final boolean batchDecisions, final int batchWorkers,
                                 final boolean directSingleShard, final boolean eventDrivenPublication,
                                 final boolean overlapMaterialization, final boolean fastMaterialization) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, eventDrivenPublication,
                overlapMaterialization, fastMaterialization, false);
    }

    /**
     * Maintains that there is an overlap materialization parameter, and the center decision receipt fast path must be separately enabled.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchDecisions batch decisions
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     * @param eventDrivenPublication event driven publication
     * @param overlapMaterialization overlap materialization
     */
    public SegmentRuntimeOptions(final int segmentSize, final long ownerLeaseMillis,
                                 final long ownerRefreshMillis, final long idleSealMillis,
                                 final int recoveryBatchSize, final int publicationBatchSize, final int gcBatchSize,
                                 final long recoveryIntervalMillis, final long publicationIntervalMillis,
                                 final long gcIntervalMillis, final boolean batchDecisions, final int batchWorkers,
                                 final boolean directSingleShard, final boolean eventDrivenPublication,
                                 final boolean overlapMaterialization) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, eventDrivenPublication,
                overlapMaterialization, false, false);
    }

    /**
     * Maintains the existing physical 1PC running time parameters, and releases a wake-up call to be independently visible.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchDecisions batch decisions
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     */
    public SegmentRuntimeOptions(final int segmentSize, final long ownerLeaseMillis,
                                 final long ownerRefreshMillis, final long idleSealMillis,
                                 final int recoveryBatchSize, final int publicationBatchSize, final int gcBatchSize,
                                 final long recoveryIntervalMillis, final long publicationIntervalMillis,
                                 final long gcIntervalMillis, final boolean batchDecisions, final int batchWorkers,
                                 final boolean directSingleShard) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, false, false, false, false);
    }

    /**
     * Maintains the release of a wake-up test parameter, and shard materialization overlaps with the release will need to be enabled in an additional visible way.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchDecisions batch decisions
     * @param batchWorkers batch workers
     * @param directSingleShard direct single shard
     * @param eventDrivenPublication event driven publication
     */
    public SegmentRuntimeOptions(final int segmentSize, final long ownerLeaseMillis,
                                 final long ownerRefreshMillis, final long idleSealMillis,
                                 final int recoveryBatchSize, final int publicationBatchSize, final int gcBatchSize,
                                 final long recoveryIntervalMillis, final long publicationIntervalMillis,
                                 final long gcIntervalMillis, final boolean batchDecisions, final int batchWorkers,
                                 final boolean directSingleShard, final boolean eventDrivenPublication) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, eventDrivenPublication,
                false, false, false);
    }

    /**
     * Compatible with existing batch decision experimental parameters, local Physics 1PC still needs to be visible.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param batchDecisions batch decisions
     * @param batchWorkers batch workers
     */
    public SegmentRuntimeOptions(final int segmentSize, final long ownerLeaseMillis,
                                 final long ownerRefreshMillis, final long idleSealMillis,
                                 final int recoveryBatchSize, final int publicationBatchSize, final int gcBatchSize,
                                 final long recoveryIntervalMillis, final long publicationIntervalMillis,
                                 final long gcIntervalMillis, final boolean batchDecisions, final int batchWorkers) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, false, false, false, false, false);
    }

    /**
     * Batch decision must still be opened separately while maintaining an active run.
     *
     * @param segmentSize segment size
     * @param ownerLeaseMillis owner lease millis
     * @param ownerRefreshMillis owner refresh millis
     * @param idleSealMillis idle seal millis
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     */
    public SegmentRuntimeOptions(final int segmentSize, final long ownerLeaseMillis,
                                 final long ownerRefreshMillis, final long idleSealMillis,
                                 final int recoveryBatchSize, final int publicationBatchSize, final int gcBatchSize,
                                 final long recoveryIntervalMillis, final long publicationIntervalMillis,
                                 final long gcIntervalMillis) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                false, 2, false, false, false, false, false);
    }

    /**
     * Returns a conservative initial budget for isolation verify, and the business entrance still needs to select the segment mode in a visible manner.
     *
     * @return the operation result
     */
    public static SegmentRuntimeOptions defaults() {
        return new SegmentRuntimeOptions(32, 5_000, 1_000, 100,
                128, 256, 128, 100, 10, 1_000);
    }
}

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

package com.flyingzc.azir.publication;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.SegmentReservation;
import com.flyingzc.azir.storage.mysql.MysqlShardSegmentFence;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bounded Rotation recovery Disconnected local, Physical Fencing, Seal Cell and collection candidate have been announced.
 */
public final class LocalFirstPublicationRecoveryManager {

    private final MysqlLocalFirstPublicationLedger ledger;

    private final Map<String, MysqlShardSegmentFence> shards;

    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();

    private long segmentAfterStart;

    private long gcAfterSegmentStart;

    /**
     * Recovery route must match the target identity for each shard durable sequence.
     *
     * @param ledger ledger
     * @param shards shards
     */
    public LocalFirstPublicationRecoveryManager(final MysqlLocalFirstPublicationLedger ledger,
                                                final Map<String, MysqlShardSegmentFence> shards) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.shards = Map.copyOf(shards);
        this.shards.forEach((shardId, physical) -> {
            if (!shardId.equals(physical.shardId())) {
                throw new IllegalArgumentException("local-first recovery route differs from shard fence");
            }
        });
    }

    /**
     * Finish the physical fence and center terminal state, release the watermark and clear the closed shard candidate.
     *
     * @param segmentLimit segment limit
     * @param candidateLimit candidate limit
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized RecoveryResult recoverOnce(final int segmentLimit, final int candidateLimit) {
        if (segmentLimit < 1 || segmentLimit > 4096 || candidateLimit < 1 || candidateLimit > 4096) {
            throw new IllegalArgumentException("invalid local-first recovery batch limit");
        }
        int sealed = 0;
        int aborted = 0;
        int purged = 0;
        int failures = 0;
        List<SegmentReservation> segments = ledger.findRecoverableSegments(segmentAfterStart, segmentLimit);
        segmentAfterStart = segments.size() < segmentLimit ? 0 : segments.getLast().lease().firstTs();
        for (SegmentReservation segment : segments) {
            try {
                var request = ledger.beginFencing(segment, false);
                if (MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE.equals(segment.shardId())) {
                    aborted += ledger.sealIntentSegment(segment);
                } else {
                    MysqlShardSegmentFence physical = requireShard(segment.shardId());
                    physical.fence(request.physicalLease());
                    aborted += ledger.sealFencedSegment(segment, physical);
                }
                sealed++;
            } catch (final MvccException ex) {
                if (ex.errorCode() != ErrorCode.TRANSACTION_ABORTED) {
                    lastFailure.set(ex);
                    failures++;
                }
            } catch (final RuntimeException ex) {
                lastFailure.set(ex);
                failures++;
            }
        }
        long published;
        try {
            published = ledger.advancePublishedFast(4096);
        } catch (final RuntimeException ex) {
            lastFailure.set(ex);
            failures++;
            published = ledger.currentPublished();
        }
        List<SegmentReservation> gcSegments = ledger.findPurgeableSegments(gcAfterSegmentStart, candidateLimit);
        gcAfterSegmentStart = gcSegments.size() < candidateLimit
                ? 0
                : gcSegments.getLast().lease().firstTs();
        for (SegmentReservation segment : gcSegments) {
            try {
                var proof = ledger.segmentGcProof(segment.lease().firstTs());
                if (proof.isEmpty()) {
                    continue;
                }
                MysqlShardSegmentFence physical = requireShard(proof.get().shardId());
                physical.purgeFencedSegmentCandidates(proof.get());
                purged += ledger.markSegmentCandidatesPurged(proof.get(), physical);
            } catch (final RuntimeException ex) {
                lastFailure.set(ex);
                failures++;
            }
        }
        return new RecoveryResult(sealed, aborted, purged, failures, published);
    }

    /**
     * Keep FENCING and candidate GC records for route after recovery when target shard is missing.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private MysqlShardSegmentFence requireShard(final String shardId) {
        MysqlShardSegmentFence physical = shards.get(shardId);
        if (physical == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local-first recovery shard unavailable: " + shardId, false, null);
        }
        return physical;
    }

    /**
     * Returns the most recent failure so as to allow for the identification of the oldest part that cannot be released.
     *
     * @return the operation result
     */
    public RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /**
     * Round of independent statistical physical bands, aborted tailings, candidates collection and failure numbers.
     *
     * @param sealedSegments sealed segments
     * @param abortedSlots aborted slots
     * @param purgedCandidates purged candidates
     * @param failures failures
     * @param publishedTs published ts
     */
    public record RecoveryResult(int sealedSegments, int abortedSlots, int purgedCandidates,
            int failures, long publishedTs) {

        /**
         * Recovery statistics cannot show negative counts or retreat watermark.
         *
         * @param sealedSegments sealed segments
         * @param abortedSlots aborted slots
         * @param purgedCandidates purged candidates
         * @param failures failures
         * @param publishedTs published ts
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public RecoveryResult {
            if (sealedSegments < 0 || abortedSlots < 0 || purgedCandidates < 0
                    || failures < 0 || publishedTs < 0) {
                throw new IllegalArgumentException("invalid local-first recovery result");
            }
        }
    }
}

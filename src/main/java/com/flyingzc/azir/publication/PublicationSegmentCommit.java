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

import com.flyingzc.azir.metadata.TransactionMetadataValidation;

import java.util.Objects;
import java.util.Set;

/**
 * An transaction with durable slots and complete participant, waiting centre batch decision.
 *
 * @param segment segment
 * @param commitTs durable commit timestamp
 * @param txId transaction identifier
 * @param deadlineMillis deadline millis
 * @param participants participants
 */
public record PublicationSegmentCommit(PublicationSegmentLease segment, long commitTs,
        long txId, long deadlineMillis, Set<String> participants) {

    /**
     * Fixed request identity and refused to cross the timestamp and empty participant collections.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationSegmentCommit {
        Objects.requireNonNull(segment, "segment");
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (commitTs < segment.firstTs() || commitTs > segment.lastTs()
                || deadlineMillis < 1 || participants.isEmpty()) {
            throw new IllegalArgumentException("invalid segmented batch decision request");
        }
        participants = Set.copyOf(participants);
    }
}

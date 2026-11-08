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

package com.flyingzc.azir.metadata;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.util.Set;

/**
 * Harmonize timestamp and participant paths for memory and durable storage metadata.
 */
public final class TransactionMetadataValidation {

    /**
     * Provides only metadata that are not variable validate, and does not save variable status.
     */
    private TransactionMetadataValidation() {
    }

    /**
     * Commit decision and single shard positions timestamp must be strictly later than the start time, with no retours or equivalent.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @throws MvccException if the operation cannot complete
     */
    public static void validateCommitTs(final long txId, final long startTs, final Long commitTs) {
        if (commitTs == null || commitTs <= startTs) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp must be greater than start timestamp", false, txId);
        }
    }

    /**
     * An empty collection means no participant; the assembly itself and its route markings must be valid.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @throws MvccException if the operation cannot complete
     */
    public static void validateParticipants(final long txId, final Set<String> participants) {
        if (participants == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant set must not be null", false, txId);
        }
        participants.forEach(participant -> validateParticipant(txId, participant));
    }

    /**
     * Keeps the old CSV format and prohibits a blank markup or comma to create recovery path symmetry.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @throws MvccException if the operation cannot complete
     */
    public static void validateParticipant(final long txId, final String participant) {
        if (participant == null || participant.isBlank() || participant.indexOf(',') >= 0) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant must be nonblank and must not contain a comma", false, txId);
        }
    }
}

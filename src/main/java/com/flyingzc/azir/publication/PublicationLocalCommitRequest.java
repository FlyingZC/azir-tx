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

/**
 * A single shard local direct commit waiting for atomic reserved transaction identity.
 *
 * @param txId transaction identifier
 * @param expiresAtMillis expires at millis
 * @param participant participant
 */
public record PublicationLocalCommitRequest(long txId, long expiresAtMillis, String participant) {

    /**
     * Validate Only positive lease deadline for participant and immutable.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participant participant
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationLocalCommitRequest {
        TransactionMetadataValidation.validateParticipant(txId, participant);
        if (expiresAtMillis <= 0) {
            throw new IllegalArgumentException("local commit deadline must be positive");
        }
    }
}

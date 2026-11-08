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

import java.util.Set;

/**
 * An transaction completed participant prewrite, waiting for control library atomic decision.
 *
 * @param txId transaction identifier
 * @param expiresAtMillis expires at millis
 * @param participants participants
 */
public record PublicationIntentCommit(long txId, long expiresAtMillis, Set<String> participants) {

    /**
     * Fixed participant collection and rejection of commit request without participant.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participants participants
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationIntentCommit {
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (participants.isEmpty() || expiresAtMillis <= 0) {
            throw new IllegalArgumentException("intent commit requires participants and a positive deadline");
        }
        participants = Set.copyOf(participants);
    }
}

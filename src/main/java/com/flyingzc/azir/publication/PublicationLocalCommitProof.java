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

import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.TransactionMetadataValidation;
import com.flyingzc.azir.storage.ParticipantRecord;

import java.util.Objects;

/**
 * An durable certificate has been added to the single shard physical commit while waiting for decision in the central ledger.
 *
 * @param local local
 * @param commitTs durable commit timestamp
 * @param participant participant
 */
public record PublicationLocalCommitProof(ParticipantRecord local, long commitTs, String participant) {

    /**
     * Only timestamp and single participant consistent with local COMMITTED terminal state are accepted.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationLocalCommitProof {
        Objects.requireNonNull(local, "local");
        TransactionMetadataValidation.validateParticipant(local.txId(), participant);
        if (local.status() != TxStatus.COMMITTED || local.commitTs() == null
                || local.commitTs() != commitTs || commitTs <= local.startTs()) {
            throw new IllegalArgumentException("local completion requires matching committed participant proof");
        }
    }
}

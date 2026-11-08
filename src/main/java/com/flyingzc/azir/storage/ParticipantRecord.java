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

package com.flyingzc.azir.storage;

import com.flyingzc.azir.common.TxStatus;

import java.util.Objects;

/**
 * Participant local transaction, record with this shard version or rollback tag atomic durable storage.
 *
 * @param txId transaction identifier
 * @param startTs transaction start timestamp
 * @param status status
 * @param commitTs durable commit timestamp
 */
public record ParticipantRecord(long txId, long startTs, TxStatus status, Long commitTs) {

    /**
     * Validate terminal state and commit timestamp Consistency; zero starting time indicates a lack of evidence to start collection.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param status status
     * @param commitTs durable commit timestamp
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public ParticipantRecord {
        Objects.requireNonNull(status, "status");
        if ((status == TxStatus.COMMITTED) != (commitTs != null)) {
            throw new IllegalArgumentException("only COMMITTED records must have commitTs");
        }
    }

    /**
     * Return the transaction identifier.
     *
     * @return the operation result
     */
    @Override
    public long txId() {
        return txId;
    }

    /**
     * Return the transaction start timestamp.
     *
     * @return the operation result
     */
    @Override
    public long startTs() {
        return startTs;
    }

    /**
     * Returns participant durable storage status.
     *
     * @return the operation result
     */
    @Override
    public TxStatus status() {
        return status;
    }

    /**
     * Returns immutable commit timestamp.
     *
     * @return the operation result
     */
    @Override
    public Long commitTs() {
        return commitTs;
    }
}

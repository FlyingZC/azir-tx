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

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Transaction metadata Records, keep transaction timestamp, level of isolation, participation shard and commit status.
 */
public final class TxRecord {

    private final long txId;

    private final long startTs;

    private final IsolationLevel isolationLevel;

    private final Set<String> participants;

    private final TxStatus status;

    private final Long commitTs;

    /**
     * Construct a TxRecord instance.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param status status
     * @param commitTs durable commit timestamp
     * @throws MvccException if the operation cannot complete
     */
    public TxRecord(final long txId,
                    final long startTs,
                    final IsolationLevel isolationLevel,
                    final Set<String> participants,
                    final TxStatus status,
                    final Long commitTs) {
        if (status == null || isolationLevel == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction status and isolation level must not be null", false, txId);
        }
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (status == TxStatus.COMMITTED || commitTs != null) {
            TransactionMetadataValidation.validateCommitTs(txId, startTs, commitTs);
        }
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
        this.participants = new LinkedHashSet<>(participants);
        this.status = status;
        this.commitTs = commitTs;
    }

    /**
     * Return the transaction identifier.
     *
     * @return the operation result
     */
    public long txId() {
        return txId;
    }

    /**
     * Return the transaction start timestamp.
     *
     * @return the operation result
     */
    public long startTs() {
        return startTs;
    }

    /**
     * Return the transaction isolation level.
     *
     * @return the operation result
     */
    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    /**
     * Return the participating shards.
     *
     * @return the operation result
     */
    public Set<String> participants() {
        return Set.copyOf(participants);
    }

    /**
     * Return the transaction state.
     *
     * @return the operation result
     */
    public TxStatus status() {
        return status;
    }

    /**
     * Returns transaction commit timestamp.
     *
     * @return the operation result
     */
    public Long commitTs() {
        return commitTs;
    }

    /**
     * Returns new transaction records after participating shard.
     *
     * @param newParticipants new participants
     * @return the operation result
     */
    public TxRecord withParticipants(final Set<String> newParticipants) {
        return new TxRecord(txId, startTs, isolationLevel, newParticipants, status, commitTs);
    }

    /**
     * Returns new transaction records after replacement status.
     *
     * @param newStatus new status
     * @return the operation result
     */
    public TxRecord withStatus(final TxStatus newStatus) {
        return new TxRecord(txId, startTs, isolationLevel, participants, newStatus, commitTs);
    }

    /**
     * Returns the new transaction record of an commit state.
     *
     * @param newCommitTs new commit ts
     * @return the operation result
     */
    public TxRecord withCommitted(final long newCommitTs) {
        return new TxRecord(txId, startTs, isolationLevel, participants, TxStatus.COMMITTED, newCommitTs);
    }
}

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

package com.flyingzc.azir.metadata.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Shard Read side read-only parsing local first commit timeline real-time center decision and writing to the required slot atomic protocol.
 */
public final class MysqlLocalFirstMetadataView implements TransactionMetadataStore {

    private final MysqlLocalFirstPublicationLedger ledger;

    /**
     * Ties the same centre control library, does not cache ACTIVE, missing or terminal state rows.
     *
     * @param ledger ledger
     */
    public MysqlLocalFirstMetadataView(final MysqlLocalFirstPublicationLedger ledger) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    /**
     * Foreground prewrite receipt can only be used to create an example of the same centre book.
     *
     * @param candidate candidate
     * @return the operation result
     */
    public boolean usesLedger(final MysqlLocalFirstPublicationLedger candidate) {
        return ledger == candidate;
    }

    /**
     * Shard Physics rollback must first pass the center transaction line lock and database period fence.
     *
     * @param txId transaction identifier
     */
    public void assertIntentAbortAllowed(final long txId) {
        ledger.assertIntentAbortAllowed(txId);
    }

    /**
     * Recovery and reading side durations use control database clocks.
     *
     * @return the operation result
     */
    @Override
    public long currentTimeMillis() {
        return ledger.currentDatabaseTimeMillis();
    }

    /**
     * Pending intent is only recognized and not visible for the other transaction each time you read a real durable state.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public Optional<TxRecord> get(final long txId) {
        return ledger.metadataRecord(txId);
    }

    /**
     * Shard GC retains the pending intent and COMMITTED lower bounds that still require local recovery.
     *
     * @return the operation result
     */
    @Override
    public Optional<Long> oldestActiveStartTs() {
        return ledger.oldestUnfinalizedStartTs();
    }

    /**
     * New transaction can only be created by a control account entry with an snapshot period.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    @Override
    public TxRecord create(final long txId, final long startTs, final IsolationLevel isolationLevel) {
        throw unsupported(txId);
    }

    /**
     * PREPARING cannot circumvent durable participant to create directly with the time slot protocol.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    @Override
    public TxRecord createPreparing(final long txId, final long startTs, final IsolationLevel isolationLevel,
                                    final Set<String> participants) {
        throw unsupported(txId);
    }

    /**
     * COMMITTED can only be dropped by atomic in centre transaction with the same subtraction.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord createCommitted(final long txId, final long startTs, final IsolationLevel isolationLevel,
                                    final Set<String> participants, final long commitTs) {
        throw unsupported(txId);
    }

    /**
     * Normal metadata status rewriting cannot replace cross-shard Center decision.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     */
    @Override
    public TxRecord markPreparing(final long txId, final Set<String> participants) {
        throw unsupported(txId);
    }

    /**
     * Commit Time must be determined by the sole slot of the new ledger.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord markCommitted(final long txId, final long commitTs) {
        throw unsupported(txId);
    }

    /**
     * The participant list cannot be rewrited on the read-only solver.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord markCommitted(final long txId, final Set<String> participants, final long commitTs) {
        throw unsupported(txId);
    }

    /**
     * Single shard Direct commit uses the local candidate certificate and does not change the generic status.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord markSingleShardCommitted(final long txId, final String participant, final long commitTs) {
        throw unsupported(txId);
    }

    /**
     * Participant All physics fencing before the center is allowed to write ABORTED.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public TxRecord markAborted(final long txId) {
        throw unsupported(txId);
    }

    /**
     * The finalisation must be validate all shard durable terminal state and the default no-op cannot be used.
     *
     * @param txId transaction identifier
     */
    @Override
    public void markFinalized(final long txId) {
        throw unsupported(txId);
    }

    /**
     * CGC only follows the established order of new account slots, metadata and shard certificates.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param finalizedBeforeMillis finalized before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public int purgeFinalized(final long safePoint, final long finalizedBeforeMillis, final int limit) {
        throw unsupported(null);
    }

    /**
     * Prevents general metadata API circumventing segments atomic commit and recovery fence.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private MvccException unsupported(final Long txId) {
        return new MvccException(ErrorCode.ILLEGAL_STATE,
                "local-first metadata view does not authorize direct state mutation", false, txId);
    }
}

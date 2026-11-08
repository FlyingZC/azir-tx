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
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Transaction metadata Storage interface, defines the durable storage operation for transaction life cycle states.
 */
public interface TransactionMetadataStore {

    /**
     * Returns a wall clock that is used in conjunction with recovery; distribution realization should cover the control side of the authority clock.
     *
     * @return the operation result
     */
    default long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Durable storage single shard local commit position; the only registered participant and fixed groupTs must be more strict than startTs.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @throws MvccException if the operation cannot complete
     */
    default void markLocalCommitPending(final long txId, final long commitTs) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "local publication fencing is required", false, txId);
    }

    /**
     * New snapshot and GC must confirm that there is no unknown slot local commit; continued publication is prohibited if not supported. Unknown slots sacrifice full library readability
     * until the original lease expires and participant terminal state closing is released.
     *
     * @throws MvccException if the operation cannot complete
     */
    default void assertNoPendingPublications() {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "pending publication inspection is required", false, null);
    }

    /**
     * Execute snapshot registration, commit decision or safe point propulsion within re-enactable barrier; default guarantees only cross-repeated with the example.
     *
     * @param <T> value type
     * @param action action
     * @return the operation result
     */
    default <T> T withPublicationLock(final Supplier<T> action) {
        synchronized (this) {
            return action.get();
        }
    }

    /**
     * A fixed deadline of lease for independent registration of snapshot; duplicate registration may not extend the deadline or raise the lower level of protection.
     *
     * @param txId transaction identifier
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     * @throws MvccException if the operation cannot complete
     */
    default void registerSnapshot(final long txId, final long readTs, final long expiresAtMillis) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot leases are required", false, txId);
    }

    /**
     * Query lease original deadline, including expired lease which has not been cleared; if not supported, the recovery takeover is forbidden.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    default Optional<Long> snapshotExpiry(final long txId) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot leases are required", false, txId);
    }

    /**
     * Validate durable lease is still with the original cut-off date and not due; storage achieves combined cut-off time and authority clock reading.
     *
     * @param txId transaction identifier
     * @param expectedExpiresAtMillis expected expires at millis
     * @return the operation result
     */
    default boolean isSnapshotLeaseValid(final long txId, final long expectedExpiresAtMillis) {
        Optional<Long> expiry = snapshotExpiry(txId);
        return expiry.isPresent() && expiry.get() == expectedExpiresAtMillis
                && currentTimeMillis() < expectedExpiresAtMillis;
    }

    /**
     * Release transaction snapshot protection, repeated release without side effects.
     *
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     */
    default void releaseSnapshot(final long txId) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot leases are required", false, txId);
    }

    /**
     * Merges non-empty and comma-free participant identification before prewrite; terminal state records remain unchanged and return to real status.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    default TxRecord registerParticipants(final long txId, final Set<String> participants) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant registration is required", false, txId);
    }

    /**
     * Returns the terminal state and lease missing or expired pending transaction in txId ascending order.
     *
     * @param nowMillis now millis
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     */
    default List<TxRecord> findRecoveryCandidates(final long nowMillis, final long afterTxId, final int limit) {
        return List.of();
    }

    /**
     * Mark terminal state materialization only after all participant successful; the deletion of history decision is not authorized by default.
     *
     * @param txId transaction identifier
     */
    default void markFinalized(final long txId) {
    }

    /**
     * Bounded Deletes the terminal state earlier than safe point and the reservation deadline, which is already materialization; by default a conservative reservation.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param finalizedBeforeMillis finalized before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    default int purgeFinalized(final long safePoint, final long finalizedBeforeMillis, final int limit) {
        return 0;
    }

    /**
     * Bounded Delete the lease deadline not later than now Millis without changing transaction decision.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    default int expireSnapshots(final long nowMillis, final int limit) {
        return 0;
    }

    /**
     * Create the component or transaction record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    TxRecord create(long txId, long startTs, IsolationLevel isolationLevel);

    /**
     * Complete participant at first durable storage transaction must be completed before participant prewrite.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    default TxRecord createWithParticipants(final long txId, final long startTs, final IsolationLevel isolationLevel,
                                            final Set<String> participants) {
        create(txId, startTs, isolationLevel);
        return registerParticipants(txId, participants);
    }

    /**
     * Create a PREPARING transaction metadata record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants);

    /**
     * Creates a transaction metadata record of COMMITTED status, the committeeTs must be more strict than startTs.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs);

    /**
     * Read the currently visible data for the key or transaction.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    Optional<TxRecord> get(long txId);

    /**
     * Mark the transaction PREPARING.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     */
    TxRecord markPreparing(long txId, Set<String> participants);

    /**
     * Mark the transaction COMMITTED; commitTs must be strictly greater than the stored startTs.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    TxRecord markCommitted(long txId, long commitTs);

    /**
     * Mark the transaction COMMITTED; commitTs must be strictly greater than the stored startTs.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    TxRecord markCommitted(long txId, Set<String> participants, long commitTs);

    /**
     * Persist PREPARING through the fast path.
     *
     * @param txId transaction identifier
     * @param participants participants
     */
    default void persistPreparing(final long txId, final Set<String> participants) {
        markPreparing(txId, participants);
    }

    /**
     * Persist the COMMITTED state through the fast path.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    default void persistCommitted(final long txId, final long commitTs) {
        markCommitted(txId, commitTs);
    }

    /**
     * Persist the COMMITTED state through the fast path.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     */
    default void persistCommitted(final long txId, final Set<String> participants, final long commitTs) {
        markCommitted(txId, participants, commitTs);
    }

    /**
     * Mark the single-shard transaction committed.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    TxRecord markSingleShardCommitted(long txId, String participant, long commitTs);

    /**
     * Durable storage Single shard decision; allows the storage of an atomic condition that achieves the use of the equivalent when the record is not required to be returned.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @param commitTs durable commit timestamp
     */
    default void persistSingleShardCommitted(final long txId, final String participant, final long commitTs) {
        markSingleShardCommitted(txId, participant, commitTs);
    }

    /**
     * Mark the transaction ABORTED.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    TxRecord markAborted(long txId);

    /**
     * Query the oldest active snapshot or have not completed the lower materialization transaction boundary, including independent lease and unfinalized terminal state.
     *
     * @return the operation result
     */
    Optional<Long> oldestActiveStartTs();
}

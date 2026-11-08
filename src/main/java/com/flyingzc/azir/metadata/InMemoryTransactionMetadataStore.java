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
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateCommitTs;
import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipant;
import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipants;

/**
 * Memory-based transaction metadata storage achieved, mainly for local testing and prototype verify.
 */
public final class InMemoryTransactionMetadataStore implements TransactionMetadataStore {

    private final Map<Long, TxRecord> records = new HashMap<>();

    private final Map<Long, SnapshotLease> snapshots = new HashMap<>();

    private final Map<Long, Long> finalizedAt = new HashMap<>();

    /**
     * All metadata operations share the same example monitor to allow re-entry within barrier.
     *
     * @param <T> value type
     * @param action action
     * @return the operation result
     */
    @Override
    public synchronized <T> T withPublicationLock(final Supplier<T> action) {
        return action.get();
    }

    /**
     * Durable storage single participant local commit position, and the same timestamp must be used for repeated calls.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized void markLocalCommitPending(final long txId, final long commitTs) {
        TxRecord record = requireRecord(txId, records.get(txId));
        validateCommitTs(txId, record.startTs(), commitTs);
        if (record.participants().size() != 1 || record.status() == TxStatus.ABORTED
                || record.status() == TxStatus.COMMITTED
                || record.commitTs() != null && record.commitTs() != commitTs) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "invalid local publication state", false, txId);
        }
        records.put(txId, new TxRecord(txId, record.startTs(), record.isolationLevel(), record.participants(),
                TxStatus.PREPARING, commitTs));
    }

    /**
     * Stop sending new snapshot when local commit is unknown until recovery has blocked late requests and written real terminal state.
     */
    @Override
    public synchronized void assertNoPendingPublications() {
        records.values().stream().filter(record -> record.status() == TxStatus.PREPARING && record.commitTs() != null)
                .findFirst().ifPresent(record -> {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "local publication requires recovery", false, record.txId());
                });
    }

    /**
     * Registers are fixed lease and duplicate registrations allow only lower reading timestamp.
     *
     * @param txId transaction identifier
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized void registerSnapshot(final long txId, final long readTs, final long expiresAtMillis) {
        SnapshotLease previous = snapshots.get(txId);
        if (previous != null && previous.expiresAtMillis() != expiresAtMillis) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot lease cannot be renewed", false, txId);
        }
        snapshots.put(txId, new SnapshotLease(previous == null ? readTs : Math.min(previous.readTs(), readTs), expiresAtMillis));
    }

    /**
     * Returns a fixed deadline of lease.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public synchronized Optional<Long> snapshotExpiry(final long txId) {
        return Optional.ofNullable(snapshots.get(txId)).map(SnapshotLease::expiresAtMillis);
    }

    /**
     * Release the snapshot lease idempotently.
     *
     * @param txId transaction identifier
     */
    @Override
    public synchronized void releaseSnapshot(final long txId) {
        snapshots.remove(txId);
    }

    /**
     * Merge pending transaction participant, without changing its status or overwhelming terminal state.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized TxRecord registerParticipants(final long txId, final Set<String> participants) {
        validateParticipants(txId, participants);
        TxRecord current = requireRecord(txId, records.get(txId));
        if (current.status() == TxStatus.COMMITTED || current.status() == TxStatus.ABORTED) {
            return current;
        }
        if (current.commitTs() != null && !current.participants().containsAll(participants)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, txId);
        }
        TxRecord next = current.withParticipants(mergeParticipants(current, participants));
        records.put(txId, next);
        return next;
    }

    /**
     * Pending transaction with sequence cursor found lease invalid and terminal state without materialization completed.
     *
     * @param nowMillis now millis
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public synchronized List<TxRecord> findRecoveryCandidates(final long nowMillis, final long afterTxId, final int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return records.values().stream().filter(record -> record.txId() > afterTxId)
                .filter(record -> !finalizedAt.containsKey(record.txId()))
                .filter(record -> record.status() == TxStatus.COMMITTED || record.status() == TxStatus.ABORTED
                        || snapshotExpiry(record.txId()).map(expiry -> expiry <= nowMillis).orElse(true))
                .sorted(Comparator.comparingLong(TxRecord::txId)).limit(limit).toList();
    }

    /**
     * Saves the first materialization completion time and repeated notifications do not extend the duration of a reservation.
     *
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized void markFinalized(final long txId) {
        TxRecord record = records.get(txId);
        if (record == null) {
            return;
        }
        if (record.status() != TxStatus.COMMITTED && record.status() != TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot finalize undecided tx", false, txId);
        }
        finalizedAt.putIfAbsent(txId, currentTimeMillis());
    }

    /**
     * Deletes only records that are already materialization and are strictly earlier than safe point and the time they are kept.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param finalizedBeforeMillis finalized before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public synchronized int purgeFinalized(final long safePoint, final long finalizedBeforeMillis, final int limit) {
        if (limit <= 0) {
            return 0;
        }
        List<Long> ids = finalizedAt.entrySet().stream()
                .filter(entry -> entry.getValue() < finalizedBeforeMillis)
                .filter(entry -> !snapshots.containsKey(entry.getKey()))
                .filter(entry -> {
                    TxRecord record = records.get(entry.getKey());
                    return record != null && record.startTs() < safePoint
                            && (record.status() == TxStatus.ABORTED
                                    || record.status() == TxStatus.COMMITTED && record.commitTs() < safePoint);
                }).map(Map.Entry::getKey).sorted().limit(limit).toList();
        ids.forEach(id -> {
            records.remove(id);
            finalizedAt.remove(id);
        });
        return ids.size();
    }

    /**
     * Batch clearance expired lease, and the uncleaned part continues to hold back GC.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public synchronized int expireSnapshots(final long nowMillis, final int limit) {
        if (limit <= 0) {
            return 0;
        }
        List<Long> ids = snapshots.entrySet().stream().filter(entry -> entry.getValue().expiresAtMillis() <= nowMillis)
                .map(Map.Entry::getKey).sorted().limit(limit).toList();
        ids.forEach(snapshots::remove);
        return ids.size();
    }

    /**
     * Create the component or transaction record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized TxRecord create(final long txId, final long startTs, final IsolationLevel isolationLevel) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    /**
     * Create a PREPARING transaction metadata record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized TxRecord createPreparing(final long txId, final long startTs, final IsolationLevel isolationLevel, final Set<String> participants) {
        validateParticipants(txId, participants);
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, new LinkedHashSet<>(participants), TxStatus.PREPARING, null);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    /**
     * Create a COMMITTED transaction metadata record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized TxRecord createCommitted(final long txId, final long startTs, final IsolationLevel isolationLevel, final Set<String> participants, final long commitTs) {
        validateCommitTs(txId, startTs, commitTs);
        validateParticipants(txId, participants);
        TxRecord existing = records.get(txId);
        if (existing != null && existing.status() == TxStatus.COMMITTED && existing.startTs() == startTs
                && existing.isolationLevel() == isolationLevel && existing.participants().equals(participants)) {
            return markCommitted(txId, commitTs);
        }
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, new LinkedHashSet<>(participants), TxStatus.COMMITTED, commitTs);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    /**
     * Read the currently visible data for the key or transaction.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public synchronized Optional<TxRecord> get(final long txId) {
        return Optional.ofNullable(records.get(txId));
    }

    /**
     * Mark the transaction PREPARING.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     */
    @Override
    public synchronized TxRecord markPreparing(final long txId, final Set<String> participants) {
        validateParticipants(txId, participants);
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.COMMITTED || existing.status() == TxStatus.ABORTED) {
                return existing;
            }
            if (existing.commitTs() != null && !existing.participants().containsAll(participants)) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, id);
            }
            return new TxRecord(
                    existing.txId(),
                    existing.startTs(),
                    existing.isolationLevel(),
                    mergeParticipants(existing, participants),
                    TxStatus.PREPARING,
                    existing.commitTs());
        });
    }

    /**
     * Mark the transaction COMMITTED.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public synchronized TxRecord markCommitted(final long txId, final long commitTs) {
        return markCommitted(txId, null, commitTs);
    }

    /**
     * Mark the transaction COMMITTED.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public synchronized TxRecord markCommitted(final long txId, final Set<String> participants, final long commitTs) {
        if (participants != null) {
            validateParticipants(txId, participants);
        }
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            validateCommitTs(id, existing.startTs(), commitTs);
            if (existing.status() == TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot commit aborted tx: " + id, false, id);
            }
            if (existing.commitTs() != null && existing.commitTs() != commitTs) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp mismatch: " + id, false, id);
            }
            if (existing.status() == TxStatus.COMMITTED) {
                if (existing.commitTs() == null || existing.commitTs() != commitTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp mismatch: " + id, false, id);
                }
                return existing;
            }
            TxRecord next = existing.withCommitted(commitTs);
            if (participants != null) {
                if (existing.commitTs() != null && !existing.participants().containsAll(participants)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, id);
                }
                next = next.withParticipants(mergeParticipants(existing, participants));
            }
            return next;
        });
    }

    /**
     * Mark the single-shard transaction committed.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public synchronized TxRecord markSingleShardCommitted(final long txId, final String participant, final long commitTs) {
        validateParticipant(txId, participant);
        return markCommitted(txId, Set.of(participant), commitTs);
    }

    /**
     * Mark the transaction ABORTED.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public synchronized TxRecord markAborted(final long txId) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.COMMITTED) {
                return existing;
            }
            if (existing.status() == TxStatus.ABORTED) {
                return existing;
            }
            return new TxRecord(id, existing.startTs(), existing.isolationLevel(), existing.participants(), TxStatus.ABORTED, null);
        });
    }

    /**
     * Query for all snapshot and incomplete materialization transaction minimum protected lower bounds and complete history records without blocking GC.
     *
     * @return the operation result
     */
    @Override
    public synchronized Optional<Long> oldestActiveStartTs() {
        Optional<Long> active = records.values().stream()
                .filter(record -> !finalizedAt.containsKey(record.txId()))
                .map(TxRecord::startTs)
                .min(Long::compareTo);
        Optional<Long> snapshot = snapshots.values().stream().map(SnapshotLease::readTs).min(Long::compareTo);
        return java.util.stream.Stream.concat(active.stream(), snapshot.stream()).min(Long::compareTo);
    }

    /**
     * Merge participant, and avoid the loss of registered shard calls at the duplicate stage.
     *
     * @param record record
     * @param participants participants
     * @return the operation result
     */
    private static Set<String> mergeParticipants(final TxRecord record, final Set<String> participants) {
        Set<String> merged = new LinkedHashSet<>(record.participants());
        if (participants != null) {
            merged.addAll(participants);
        }
        return merged;
    }

    /**
     * Validate and returns the transaction record, dropping an anomaly when it does not exist.
     *
     * @param txId transaction identifier
     * @param current current
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private static TxRecord requireRecord(final long txId, final TxRecord current) {
        if (current == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId);
        }
        return current;
    }

    /**
     * Saves fixed cut-off times and constant snapshot protection of the lower boundary.
     *
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     */
    private record SnapshotLease(long readTs, long expiresAtMillis) {
    }
}

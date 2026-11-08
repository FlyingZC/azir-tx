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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.StorageAdapter;

import java.util.Map;
import java.util.Objects;

import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipant;

/**
 * New protocol transaction based on account book, centre decision and participant durable terminal state recovery.
 */
public final class PublicationRecoveryManager {

    private final MysqlPublicationLedger ledger;

    private final TransactionMetadataStore metadataStore;

    private final Map<String, StorageAdapter> adapters;

    /**
     * Validate recovery route and bind to participant for reading metadata authority.
     *
     * @param ledger ledger
     * @param metadataStore metadata store
     * @param adapters adapters
     */
    public PublicationRecoveryManager(final MysqlPublicationLedger ledger, final TransactionMetadataStore metadataStore,
                                      final Map<String, StorageAdapter> adapters) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.adapters.forEach((shardId, adapter) -> {
            validateParticipant(0, shardId);
            if (!shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("recovery route must match adapter shard id: " + shardId);
            }
        });
        this.adapters.values().forEach(adapter -> adapter.configureMetadataStore(metadataStore));
    }

    /**
     * Recovery An transaction; valid lease PENDING writing transaction still belongs to foreground.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public boolean recover(final long txId) {
        PublicationEntry entry = ledger.findByTxId(txId).orElse(null);
        if (entry == null) {
            return recoverUnreserved(txId);
        }
        TxRecord record = requireRecord(txId);
        if (entry.status() == PublicationEntryStatus.PENDING) {
            long now = metadataStore.currentTimeMillis();
            if (entry.expiresAtMillis() > now
                    || ledger.snapshotExpiry(txId).stream().anyMatch(expiry -> expiry > now)) {
                return false;
            }
            decideExpired(entry, record);
            entry = ledger.findByTxId(txId).orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication entry disappeared during recovery", false, txId));
            record = requireRecord(txId);
        }
        materialize(entry, record);
        ledger.advancePublished(256);
        metadataStore.markFinalized(txId);
        ledger.releaseSnapshot(txId);
        return true;
    }

    /**
     * The crash occurred between the prewrite and the book setup, taking over and cleaning all participant s by new lease.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private boolean recoverUnreserved(final long txId) {
        long now = metadataStore.currentTimeMillis();
        if (ledger.snapshotExpiry(txId).stream().anyMatch(expiry -> expiry > now)) {
            return false;
        }
        TxRecord record = metadataStore.get(txId).orElse(null);
        if (record == null) {
            ledger.releaseSnapshot(txId);
            return false;
        }
        if (record.status() == TxStatus.COMMITTED || record.commitTs() != null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "committed or local pending metadata has no publication reservation", false, txId);
        }
        if (record.status() != TxStatus.ABORTED) {
            TxRecord decided = metadataStore.markAborted(txId);
            if (decided.status() == TxStatus.COMMITTED) {
                if (ledger.findByTxId(txId).isPresent()) {
                    return recover(txId);
                }
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "foreground commit raced unreserved recovery", false, txId);
            }
        }
        abortParticipants(record);
        metadataStore.markFinalized(txId);
        ledger.releaseSnapshot(txId);
        return true;
    }

    /**
     * Transaction expires first create irreversible terminal state and then allow watermark to cross the reservation.
     *
     * @param entry entry
     * @param record record
     * @throws MvccException if the operation cannot complete
     */
    private void decideExpired(final PublicationEntry entry, final TxRecord record) {
        long txId = entry.txId();
        if (record.status() == TxStatus.COMMITTED) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "committed metadata has unresolved publication entry", false, txId);
        }
        if (record.status() == TxStatus.ABORTED) {
            abortParticipants(record);
            ledger.decideAbortedAfterFencing(txId, entry.commitTs());
            return;
        }
        if (record.commitTs() != null) {
            resolveLocalPending(entry, record);
            return;
        }
        TxRecord decided = metadataStore.markAborted(txId);
        if (decided.status() == TxStatus.COMMITTED) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "foreground commit raced expired recovery", false, txId);
        }
        abortParticipants(record);
        ledger.decideAbortedAfterFencing(txId, entry.commitTs());
    }

    /**
     * Single shard local physics commit must be followed by a request for fencing and participant real terminal state to decision.
     *
     * @param entry entry
     * @param record record
     * @throws MvccException if the operation cannot complete
     */
    private void resolveLocalPending(final PublicationEntry entry, final TxRecord record) {
        long txId = entry.txId();
        if (record.participants().size() != 1 || !record.commitTs().equals(entry.commitTs())) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local publication has no unique matching participant", false, txId);
        }
        String shardId = record.participants().iterator().next();
        StorageAdapter adapter = requireAdapter(shardId, txId);
        adapter.abort(txId);
        ParticipantRecord local = adapter.participantRecord(txId).orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "local participant has no durable terminal decision", false, txId));
        if (local.txId() != txId || local.startTs() != record.startTs()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local participant proof has wrong transaction identity", false, txId);
        }
        if (local.status() == TxStatus.COMMITTED) {
            ledger.completeLocalCommitAfterFencing(local, entry.commitTs(), shardId);
        } else if (local.status() == TxStatus.ABORTED) {
            if (metadataStore.markAborted(txId).status() != TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local abort raced a committed center decision", false, txId);
            }
            ledger.decideAbortedAfterFencing(txId, entry.commitTs());
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local participant is not terminal", false, txId);
        }
    }

    /**
     * Shard idempotent closing, either failed to keep a PENDING book for the next round retry.
     *
     * @param record record
     */
    private void abortParticipants(final TxRecord record) {
        RuntimeException failure = null;
        for (String participant : record.participants()) {
            try {
                requireAdapter(participant, record.txId()).abort(record.txId());
            } catch (final RuntimeException ex) {
                if (failure == null) {
                    failure = ex;
                } else if (failure != ex) {
                    failure.addSuppressed(ex);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * All shard completed idempotent terminal state materialization before the Center can be marked collection.
     *
     * @param entry entry
     * @param record record
     * @throws MvccException if the operation cannot complete
     */
    private void materialize(final PublicationEntry entry, final TxRecord record) {
        if (entry.status() == PublicationEntryStatus.COMMITTED) {
            if (record.status() != TxStatus.COMMITTED || record.commitTs() == null
                    || record.commitTs() != entry.commitTs()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "committed publication differs from transaction metadata", false, entry.txId());
            }
        } else if (entry.status() == PublicationEntryStatus.ABORTED) {
            if (record.status() != TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "aborted publication differs from transaction metadata", false, entry.txId());
            }
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "publication remains pending after recovery", false, entry.txId());
        }
        RuntimeException failure = null;
        for (String participant : record.participants()) {
            try {
                StorageAdapter adapter = requireAdapter(participant, entry.txId());
                if (entry.status() == PublicationEntryStatus.COMMITTED) {
                    adapter.commit(entry.txId(), entry.commitTs());
                } else {
                    adapter.abort(entry.txId());
                }
            } catch (final RuntimeException ex) {
                if (failure == null) {
                    failure = ex;
                } else if (failure != ex) {
                    failure.addSuppressed(ex);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Allows advance advancement of terminal state to mark when shard configuration is missing.
     *
     * @param shardId logical shard identifier
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private StorageAdapter requireAdapter(final String shardId, final long txId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication recovery participant unavailable: " + shardId, false, txId);
        }
        return adapter;
    }

    /**
     * If central metadata is missing, participant should not be deduced from the books alone.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private TxRecord requireRecord(final long txId) {
        return metadataStore.get(txId).orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE,
                "publication recovery transaction metadata missing", false, txId));
    }
}

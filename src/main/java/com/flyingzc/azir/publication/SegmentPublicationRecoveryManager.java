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
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.StorageAdapter;

import java.util.Map;
import java.util.Objects;

import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipant;

/**
 * The same durable terminal state recovery participant and expired pending transaction by segment slot with central metadata.
 */
public final class SegmentPublicationRecoveryManager {

    private final MysqlSegmentPublicationLedger ledger;

    private final TransactionMetadataStore metadataStore;

    private final Map<String, StorageAdapter> adapters;

    /**
     * Validate recovery route and allow participant to read the same central metadata authority.
     *
     * @param ledger ledger
     * @param metadataStore metadata store
     * @param adapters adapters
     */
    public SegmentPublicationRecoveryManager(final MysqlSegmentPublicationLedger ledger,
                                             final TransactionMetadataStore metadataStore,
                                             final Map<String, StorageAdapter> adapters) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.adapters.forEach((shardId, adapter) -> {
            validateParticipant(0, shardId);
            if (!shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("segment recovery route differs from adapter: " + shardId);
            }
            adapter.configureMetadataStore(metadataStore);
        });
    }

    /**
     * Idempotent to materialization with slot terminal state; security aborted intent when no slot and lease expired.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean recover(final long txId) {
        PublicationEntry entry = ledger.findByTxId(txId).orElse(null);
        TxRecord record = metadataStore.get(txId).orElse(null);
        if (entry != null) {
            if (record == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented terminal slot has no center transaction", false, txId);
            }
            if (entry.status() == PublicationEntryStatus.PENDING) {
                return recoverLocalPending(entry, record);
            }
            materialize(entry, record);
            ledger.advancePublished(256);
            metadataStore.markFinalized(txId);
            ledger.releaseSnapshot(txId);
            return true;
        }
        long now = metadataStore.currentTimeMillis();
        if (ledger.snapshotExpiry(txId).stream().anyMatch(expiry -> expiry > now)) {
            return false;
        }
        if (record == null) {
            ledger.releaseSnapshot(txId);
            return false;
        }
        if (record.status() == TxStatus.COMMITTED || record.commitTs() != null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "committed segmented metadata has no matching terminal slot", false, txId);
        }
        if (record.status() != TxStatus.ABORTED) {
            TxRecord decided = metadataStore.markAborted(txId);
            if (decided.status() == TxStatus.COMMITTED) {
                if (ledger.findByTxId(txId).isPresent()) {
                    return recover(txId);
                }
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "foreground segmented commit raced expired recovery", false, txId);
            }
        }
        abortParticipants(record);
        metadataStore.markFinalized(txId);
        ledger.releaseSnapshot(txId);
        return true;
    }

    /**
     * Local Candidates are due or owner is replaced by the same central slot with shard terminal proof.
     *
     * @param entry entry
     * @param record record
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private boolean recoverLocalPending(final PublicationEntry entry, final TxRecord record) {
        long txId = entry.txId();
        long now = metadataStore.currentTimeMillis();
        if (!ledger.localPendingRecoverable(entry.commitTs(), now)) {
            return false;
        }
        if (record.status() != TxStatus.PREPARING || record.commitTs() == null
                || record.commitTs() != entry.commitTs() || record.startTs() >= entry.commitTs()
                || record.participants().size() != 1) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented local candidate differs from center metadata", false, txId);
        }
        String participant = record.participants().iterator().next();
        StorageAdapter adapter = requireAdapter(participant, txId);
        // Local abort and directcommittee in transit line participant transaction to read the real durable terminal state.
        adapter.abort(txId);
        ParticipantRecord local = adapter.participantRecord(txId).orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "segmented local candidate has no participant proof", false, txId));
        if (local.txId() != txId || local.startTs() != record.startTs()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented local proof has wrong transaction identity", false, txId);
        }
        if (local.status() == TxStatus.COMMITTED) {
            if (!Objects.equals(local.commitTs(), entry.commitTs())) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented local commit proof has another timestamp", false, txId);
            }
            ledger.completeLocalCommitAfterFencing(local, entry.commitTs(), participant);
        } else if (local.status() == TxStatus.ABORTED) {
            ledger.completeLocalAbortAfterFencing(local, entry.commitTs(), participant);
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented local participant remains undecided", false, txId);
        }
        return recover(txId);
    }

    /**
     * Centers and slots must be the same as COMMITTED or ABORTED for replay to match participant terminal state.
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
                        "segmented committed slot differs from center decision", false, entry.txId());
            }
        } else if (entry.status() == PublicationEntryStatus.ABORTED) {
            if (record.status() != TxStatus.ABORTED || record.commitTs() != null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented aborted slot differs from center decision", false, entry.txId());
            }
        } else {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented slot remains pending during recovery", false, entry.txId());
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
                ParticipantRecord local = adapter.participantRecord(entry.txId()).orElse(null);
                if (local != null && (local.startTs() != record.startTs()
                        || local.status() != (entry.status() == PublicationEntryStatus.COMMITTED
                                ? TxStatus.COMMITTED
                                : TxStatus.ABORTED)
                        || (entry.status() == PublicationEntryStatus.COMMITTED
                                && !Objects.equals(local.commitTs(), entry.commitTs())))) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "participant terminal proof differs from segmented decision", false, entry.txId());
                }
            } catch (final RuntimeException ex) {
                failure = accumulate(failure, ex);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Center pending transaction expires by shard closing, with any failure to keep unfinalized records for next round retry.
     *
     * @param record record
     * @throws MvccException if the operation cannot complete
     */
    private void abortParticipants(final TxRecord record) {
        RuntimeException failure = null;
        for (String participant : record.participants()) {
            try {
                StorageAdapter adapter = requireAdapter(participant, record.txId());
                adapter.abort(record.txId());
                ParticipantRecord local = adapter.participantRecord(record.txId()).orElse(null);
                if (local != null && (local.startTs() != record.startTs()
                        || local.status() != TxStatus.ABORTED)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "expired undecided participant was not fenced", false, record.txId());
                }
            } catch (final RuntimeException ex) {
                failure = accumulate(failure, ex);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * In the absence of physical shard route, it is prohibited to mark the centre record as completed.
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
                    "segmented recovery participant unavailable: " + shardId, false, txId);
        }
        return adapter;
    }

    /**
     * Keeps the first failure while collecting the remaining shard anomalies.
     *
     * @param first first
     * @param next next
     * @return the operation result
     */
    private RuntimeException accumulate(final RuntimeException first, final RuntimeException next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }
}

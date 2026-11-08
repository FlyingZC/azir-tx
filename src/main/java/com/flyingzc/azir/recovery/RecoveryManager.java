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

package com.flyingzc.azir.recovery;

import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.ParticipantRecord;

import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipant;

/**
 * Transaction recovery Manager, press transaction metadata to compensate for incomplete commit or rollback.
 */
public final class RecoveryManager {

    private final TransactionMetadataStore metadataStore;

    private final Map<String, StorageAdapter> adapters;

    /**
     * Construct a RecoveryManager instance.
     *
     * @param metadataStore metadata store
     * @param adapters adapters
     */
    public RecoveryManager(final TransactionMetadataStore metadataStore, final Map<String, StorageAdapter> adapters) {
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        // First all validate routes, then binding authority to avoid illegal mapping resulting in the initialization of some adaptors.
        this.adapters.forEach((shardId, adapter) -> {
            validateParticipant(0, shardId);
            if (!shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("recovery map key must match adapter shardId: " + shardId);
            }
        });
        this.adapters.values().forEach(adapter -> adapter.configureMetadataStore(this.metadataStore));
    }

    /**
     * Recovery Specifies the incomplete state of transaction.
     *
     * @param txId transaction identifier
     */
    public void recover(final long txId) {
        recover(txId, Set.of());
    }

    /**
     * Determines the real terminal state in barrier; materialization after barrier is released; returns whether the transaction has been completed.
     *
     * @param txId transaction identifier
     * @param discoveredParticipants discovered participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean recover(final long txId, final Set<String> discoveredParticipants) {
        TxRecord decision = metadataStore.withPublicationLock(() -> decide(txId, discoveredParticipants));
        if (decision == null) {
            return false;
        }
        RuntimeException failure = null;
        for (String participant : decision.participants()) {
            try {
                StorageAdapter adapter = requireAdapter(participant, txId);
                if (decision.status() == TxStatus.COMMITTED) {
                    if (decision.commitTs() == null) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "committed transaction has no timestamp", false, txId);
                    }
                    adapter.commit(txId, decision.commitTs());
                } else {
                    adapter.abort(txId);
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
        metadataStore.withPublicationLock(() -> {
            metadataStore.releaseSnapshot(txId);
            metadataStore.markFinalized(txId);
            return null;
        });
        return true;
    }

    /**
     * Rereading metadata and authority Wall clocks to prevent the taking over of valid lease or by mistake killing transaction that has not been registered.
     *
     * @param txId transaction identifier
     * @param discoveredParticipants discovered participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private TxRecord decide(final long txId, final Set<String> discoveredParticipants) {
        TxRecord record = metadataStore.get(txId).orElse(null);
        if (record == null) {
            // Missing records may belong to transaction which is being registered with Lazy, incent scan does not constitute an rollback authorization.
            return null;
        }
        Set<String> participants = new LinkedHashSet<>(record.participants());
        participants.addAll(discoveredParticipants);
        if (record.status() == TxStatus.ACTIVE || record.status() == TxStatus.PREPARING) {
            long now = metadataStore.currentTimeMillis();
            if (metadataStore.snapshotExpiry(txId).map(expiry -> expiry > now).orElse(false)) {
                return null;
            }
            record = metadataStore.registerParticipants(txId, participants);
            if (participants.size() == 1) {
                String participant = participants.iterator().next();
                StorageAdapter adapter = requireAdapter(participant, txId);
                if (adapter.supportsAtomicDirectCommit() || record.commitTs() != null) {
                    // Local transaction record lock will wait for directcommittee; atomic commit participant must first provide a matching durable storage terminal state.
                    adapter.abort(txId);
                    ParticipantRecord local = adapter.participantRecord(txId).orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "local publication fence has no durable participant decision", false, txId));
                    if (local.txId() != txId || local.startTs() != record.startTs()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant fence record mismatch", false, txId);
                    }
                    if (local.status() == TxStatus.COMMITTED) {
                        if (local.commitTs() == null || record.commitTs() != null && !record.commitTs().equals(local.commitTs())) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant commit record mismatch", false, txId);
                        }
                        record = metadataStore.markSingleShardCommitted(txId, participant, local.commitTs());
                    } else if (local.status() != TxStatus.ABORTED) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "participant fence did not reach a terminal decision", false, txId);
                    }
                }
            }
            if (record.status() == TxStatus.ACTIVE || record.status() == TxStatus.PREPARING) {
                record = metadataStore.markAborted(txId);
            }
        }
        return record.withParticipants(participants);
    }

    /**
     * Press authority clock to return an recovery candidate batch, and the caller must re-evaluate lease within recovery barrier.
     *
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     */
    public List<TxRecord> findRecoveryCandidates(final long afterTxId, final int limit) {
        return metadataStore.findRecoveryCandidates(metadataStore.currentTimeMillis(), afterTxId, limit);
    }

    /**
     * The absence of participant configuration failed explicitly, and the pre-marking of materialization was forbidden.
     *
     * @param participant participant
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private StorageAdapter requireAdapter(final String participant, final long txId) {
        StorageAdapter adapter = adapters.get(participant);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "recovery participant unavailable: " + participant, false, txId);
        }
        return adapter;
    }
}

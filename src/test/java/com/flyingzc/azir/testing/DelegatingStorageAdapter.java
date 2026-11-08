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

package com.flyingzc.azir.testing;

import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Full forwarding of a genuine storage protocol, failure packaging covers only calls that need to be injected into failure.
 */
abstract class DelegatingStorageAdapter implements StorageAdapter {

    protected final StorageAdapter delegate;

    /**
     * Construct a test wrapper for binding a genuine adapter.
     *
     * @param delegate delegate
     */
    protected DelegatingStorageAdapter(final StorageAdapter delegate) {
        this.delegate = delegate;
    }

    /**
     * Forwards the binding of the transaction centre to keep the real adapter's prohibition on binding.
     *
     * @param store store
     */
    @Override
    public void configureMetadataStore(final TransactionMetadataStore store) {
        delegate.configureMetadataStore(store);
    }

    /**
     * Returns the commit intent resolution capability of a real adapter.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsCommittedIntentResolution() {
        return delegate.supportsCommittedIntentResolution();
    }

    /**
     * Returns the atomic direct commit capability of a real adapter.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsAtomicDirectCommit() {
        return delegate.supportsAtomicDirectCommit();
    }

    /**
     * Query for real participant records and do not forge local terminal state through packaging.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public Optional<ParticipantRecord> participantRecord(final long txId) {
        return delegate.participantRecord(txId);
    }

    /**
     * Returns the real shard number.
     *
     * @return the operation result
     */
    @Override
    public String shardId() {
        return delegate.shardId();
    }

    /**
     * Initializes the real storage structure.
     */
    @Override
    public void initialize() {
        delegate.initialize();
    }

    /**
     * Returns the visible preparation requirement for a real adapter.
     *
     * @return the operation result
     */
    @Override
    public boolean requiresExplicitPreparePhase() {
        return delegate.requiresExplicitPreparePhase();
    }

    /**
     * Forward snapshot dots.
     *
     * @param context context
     * @param key business key
     * @return the operation result
     */
    @Override
    public Optional<VersionedValue> get(final ReadContext context, final String key) {
        return delegate.get(context, key);
    }

    /**
     * Forward a full range scan.
     *
     * @param context context
     * @param keyRange key range
     * @return the operation result
     */
    @Override
    public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
        return delegate.scan(context, keyRange);
    }

    /**
     * Retransmitting real pages is achieved, retention page boundaries and scanned resource limitations.
     *
     * @param context context
     * @param keyRange key range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public ScanPage scanPage(final ReadContext context, final KeyRange keyRange, final String afterKeyExclusive, final int limit) {
        return delegate.scanPage(context, keyRange, afterKeyExclusive, limit);
    }

    /**
     * Forward intent prewrite.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutations transaction write set
     */
    @Override
    public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
        delegate.prewrite(txId, startTs, mutations);
    }

    /**
     * Retransmit the visible preparation.
     *
     * @param txId transaction identifier
     */
    @Override
    public void prepare(final long txId) {
        delegate.prepare(txId);
    }

    /**
     * Press transaction identifier to forward commit.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    @Override
    public void commit(final long txId, final long commitTs) {
        delegate.commit(txId, commitTs);
    }

    /**
     * Forward commit by known write set.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     */
    @Override
    public void commit(final long txId, final long commitTs, final List<Mutation> mutations) {
        delegate.commit(txId, commitTs, mutations);
    }

    /**
     * Forwards the commit and intent cleanup policy.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     * @param cleanupIntents cleanup intents
     */
    @Override
    public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        delegate.commit(txId, commitTs, mutations, cleanupIntents);
    }

    /**
     * Forward version and participant recorded atomic directly commit.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     */
    @Override
    public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
        delegate.directCommit(txId, startTs, commitTs, mutations);
    }

    /**
     * Press write set to forward intent cleanup.
     *
     * @param txId transaction identifier
     * @param mutations transaction write set
     */
    @Override
    public void cleanupIntents(final long txId, final List<Mutation> mutations) {
        delegate.cleanupIntents(txId, mutations);
    }

    /**
     * Press transaction identifier to forward intent cleanup.
     *
     * @param txId transaction identifier
     */
    @Override
    public void cleanupIntents(final long txId) {
        delegate.cleanupIntents(txId);
    }

    /**
     * Prove the pen-by-punction center decision as the original local COMMITTED.
     *
     * @param txId transaction identifier
     */
    @Override
    public void abort(final long txId) {
        delegate.abort(txId);
    }

    /**
     * Forward to recovery transaction scan.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public Set<Long> findPendingTransactionIds(final int limit) {
        return delegate.findPendingTransactionIds(limit);
    }

    /**
     * Forward safe point garbage collection.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public int gc(final long safePoint, final int limit) {
        return delegate.gc(safePoint, limit);
    }
}

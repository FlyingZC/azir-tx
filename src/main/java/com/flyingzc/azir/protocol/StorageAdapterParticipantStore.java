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

package com.flyingzc.azir.protocol;

import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * X-0 storage surface based on StorageAdapter.
 */
public final class StorageAdapterParticipantStore implements ParticipantStore {

    private final StorageAdapter delegate;

    /**
     * Creates the participant storage front.
     *
     * @param delegate Bottom Storage Adapter.
     */
    public StorageAdapterParticipantStore(final StorageAdapter delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * Returns the participant -owned shard number.
     *
     * @return  Shard Number.
     */
    @Override
    public String shardId() {
        return delegate.shardId();
    }

    /**
     * Read the visible version of the specified key by reading context.
     *
     * @param context Read Context.
     * @param key Business Keys.
     * @return Visible version.
     */
    @Override
    public Optional<VersionedValue> get(final ReadContext context, final String key) {
        return delegate.get(context, key);
    }

    /**
     * Scan the visible version of the specified key range by reading context.
     *
     * @param context Read Context.
     * @param keyRange Key Range.
     * @return Visible version list.
     */
    @Override
    public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
        return delegate.scan(context, keyRange);
    }

    /**
     * Write transaction intent and detect write conflict.
     *
     * @param txId  Transaction identifier.
     * @param startTs  Start timestamp.
     * @param mutations  Write set.
     */
    @Override
    public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
        delegate.prewrite(txId, startTs, mutations);
    }

    /**
     * Push transaction into the preparatory phase.
     *
     * @param txId  Transaction identifier.
     */
    @Override
    public void prepare(final long txId) {
        delegate.prepare(txId);
    }

    /**
     * Finalize transaction intents as committed versions.
     *
     * @param txId  Transaction identifier.
     * @param commitTs  Commit timestamp.
     */
    @Override
    public void commit(final long txId, final long commitTs) {
        delegate.commit(txId, commitTs);
    }

    /**
     * Based on the known write set commit transaction intent and can choose to clear intent.
     *
     * @param txId  Transaction identifier.
     * @param commitTs  Commit timestamp.
     * @param mutations  Write set.
     * @param cleanupIntents Whether to clear intent.
     */
    @Override
    public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        delegate.commit(txId, commitTs, mutations, cleanupIntents);
    }

    /**
     * Writes directly to commit version, skips the visible preparation phase.
     *
     * @param txId  Transaction identifier.
     * @param startTs  Start timestamp.
     * @param commitTs  Commit timestamp.
     * @param mutations  Write set.
     */
    @Override
    public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
        delegate.directCommit(txId, startTs, commitTs, mutations);
    }

    /**
     * Based on known write set clean-up of transaction residual intent.
     *
     * @param txId  Transaction identifier.
     * @param mutations  Write set.
     */
    @Override
    public void cleanupIntents(final long txId, final List<Mutation> mutations) {
        delegate.cleanupIntents(txId, mutations);
    }

    /**
     * Roll back the transaction and remove its intents.
     *
     * @param txId  Transaction identifier.
     */
    @Override
    public void abort(final long txId) {
        delegate.abort(txId);
    }

    /**
     * Find transaction identifiers with pending intents.
     *
     * @param limit Maximum number of queries.
     * @return Pending transaction identifier setup.
     */
    @Override
    public Set<Long> findPendingTransactionIds(final int limit) {
        return delegate.findPendingTransactionIds(limit);
    }

    /**
     * Collect obsolete versions before the safe point.
     *
     * @param safePoint  Safe point.
     * @param limit Maximum number of cleanups.
     * @return Number of cleanups.
     */
    @Override
    public int gc(final long safePoint, final int limit) {
        return delegate.gc(safePoint, limit);
    }
}

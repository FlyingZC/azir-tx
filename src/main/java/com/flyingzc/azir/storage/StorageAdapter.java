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

import com.flyingzc.azir.metadata.TransactionMetadataStore;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Shard Storage Adapter interface, encapsulating MVCC reading, writing and cleaning operations.
 */
public interface StorageAdapter {

    /**
     * Query participant 's durable storage local decision; adapter does not support this ability to return empty values.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    default Optional<ParticipantRecord> participantRecord(final long txId) {
        return Optional.empty();
    }

    /**
     * Determines whether adapters can save atomic sheets shard commit decision and version.
     *
     * @return the operation result
     */
    default boolean supportsAtomicDirectCommit() {
        return false;
    }

    /**
     * Tie transaction Center authority to read side resolution commit intent and materialization to check decision.
     *
     * @param store store
     */
    default void configureMetadataStore(final TransactionMetadataStore store) {
    }

    /**
     * Determines whether the adapter is capable of parsing other transaction already commit intent.
     *
     * @return the operation result
     */
    default boolean supportsCommittedIntentResolution() {
        return false;
    }

    /**
     * Return the storage adapter shard identifier.
     *
     * @return the operation result
     */
    String shardId();

    /**
     * Initialize the required database tables or runtime environment.
     */
    void initialize();

    /**
     * Determine whether participants require an explicit prepare phase.
     *
     * @return the operation result
     */
    default boolean requiresExplicitPreparePhase() {
        return false;
    }

    /**
     * Read the currently visible data for the key or transaction.
     *
     * @param context context
     * @param key business key
     * @return the operation result
     */
    Optional<VersionedValue> get(ReadContext context, String key);

    /**
     * Scan the key range using the current snapshot.
     *
     * @param context context
     * @param keyRange key range
     * @return the operation result
     */
    List<VersionedValue> scan(ReadContext context, KeyRange keyRange);

    /**
     * Read a page in the order of the UTF-8 binary; compatibility will load the full scan result and store it overwhelms to flow pages.
     *
     * @param context context
     * @param keyRange key range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    default ScanPage scanPage(final ReadContext context, final KeyRange keyRange, final String afterKeyExclusive, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("limit must be in 1..4096");
        }
        List<VersionedValue> values = scan(context, keyRange).stream()
                .filter(value -> afterKeyExclusive == null || Utf8KeyOrder.compare(value.key(), afterKeyExclusive) > 0)
                .sorted((left, right) -> Utf8KeyOrder.compare(left.key(), right.key()))
                .limit((long) limit + 1)
                .toList();
        boolean hasMore = values.size() > limit;
        List<VersionedValue> page = hasMore ? values.subList(0, limit) : values;
        return new ScanPage(page, hasMore ? page.getLast().key() : null);
    }

    /**
     * Write transaction intent and detect write conflict.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutations transaction write set
     */
    void prewrite(long txId, long startTs, List<Mutation> mutations);

    /**
     * Verify transaction intents and enter the prepare phase.
     *
     * @param txId transaction identifier
     */
    void prepare(long txId);

    /**
     * Finalize transaction intents as committed versions.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    void commit(long txId, long commitTs);

    /**
     * Finalize transaction intents as committed versions.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     */
    default void commit(final long txId, final long commitTs, final List<Mutation> mutations) {
        commit(txId, commitTs);
    }

    /**
     * Finalize transaction intents as committed versions.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     * @param cleanupIntents cleanup intents
     */
    default void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        commit(txId, commitTs, mutations);
    }

    /**
     * Writes directly to commit version, skips the visible prepare phase.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     */
    default void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
        commit(txId, commitTs, mutations, false);
    }

    /**
     * Clean up remaining transaction intents.
     *
     * @param txId transaction identifier
     * @param mutations transaction write set
     */
    default void cleanupIntents(final long txId, final List<Mutation> mutations) {
    }

    /**
     * Clean up remaining transaction intents.
     *
     * @param txId transaction identifier
     */
    default void cleanupIntents(final long txId) {
        cleanupIntents(txId, List.of());
    }

    /**
     * Roll back the transaction and remove its intents.
     *
     * @param txId transaction identifier
     */
    void abort(long txId);

    /**
     * Find transaction identifiers with pending intents.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    Set<Long> findPendingTransactionIds(int limit);

    /**
     * Collect obsolete versions before the safe point.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     */
    int gc(long safePoint, int limit);
}

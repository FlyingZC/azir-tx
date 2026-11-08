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

package com.flyingzc.azir.api;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.storage.Mutation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Transaction context object, save transaction run-time state and buffered write set combination.
 */
public final class Transaction {

    private final long txId;

    private final long startTs;

    private final IsolationLevel isolationLevel;

    private final Set<String> participants = new LinkedHashSet<>();

    private final Map<String, NavigableMap<String, Mutation>> bufferedMutationsByShard = new LinkedHashMap<>();

    private boolean metadataPersisted;

    private TxStatus status = TxStatus.ACTIVE;

    private final long deadlineMillis;

    private final Object owner;

    private boolean rollbackOnly;

    private boolean decisionStarted;

    private final Map<String, Map<String, Integer>> writeSizes = new LinkedHashMap<>();

    private int writeKeyCount;

    private long writeBytes;

    /**
     * Construct a Transaction instance.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     */
    public Transaction(final long txId, final long startTs, final IsolationLevel isolationLevel) {
        this(txId, startTs, isolationLevel, Long.MAX_VALUE, null);
    }

    /**
     * Construct an transaction with a fixed lease and a private ownership identifier for the manager.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param deadlineMillis deadline millis
     * @param owner owner
     */
    public Transaction(final long txId, final long startTs, final IsolationLevel isolationLevel, final long deadlineMillis, final Object owner) {
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
        this.deadlineMillis = deadlineMillis;
        this.owner = owner;
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
        return Collections.unmodifiableSet(participants);
    }

    /**
     * Register a participating shard.
     *
     * @param shardId logical shard identifier
     */
    public void addParticipant(final String shardId) {
        participants.add(shardId);
    }

    /**
     * Write transaction local buffer.
     *
     * @param shardId logical shard identifier
     * @param mutation mutation
     */
    public void bufferMutation(final String shardId, final Mutation mutation) {
        participants.add(shardId);
        bufferedMutationsByShard
                .computeIfAbsent(shardId, ignored -> new TreeMap<>(com.flyingzc.azir.storage.Utf8KeyOrder::compare))
                .put(mutation.key(), mutation);
    }

    /**
     * Ask for a change of the specified key in local buffer.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    public Optional<Mutation> bufferedMutation(final String shardId, final String key) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mutations.get(key));
    }

    /**
     * Returns the local buffered write set combination of specified shard.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     */
    public List<Mutation> bufferedMutations(final String shardId) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return List.copyOf(mutations.values());
    }

    /**
     * Returns the local buffered write set combination within the specified key range.
     *
     * @param shardId logical shard identifier
     * @param startKeyInclusive start key inclusive
     * @param endKeyExclusive end key exclusive
     * @return the operation result
     */
    public List<Mutation> bufferedMutationsInRange(final String shardId, final String startKeyInclusive, final String endKeyExclusive) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return new ArrayList<>(mutations.subMap(startKeyInclusive, true, endKeyExclusive, false).values());
    }

    /**
     * Press cursor to get a limited buffer key and avoid copying or going through the entire transaction write set page by page.
     *
     * @param shardId logical shard identifier
     * @param startKeyInclusive start key inclusive
     * @param endKeyExclusive end key exclusive
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Mutation> bufferedMutationsPage(final String shardId, final String startKeyInclusive, final String endKeyExclusive,
                                                final String afterKeyExclusive, final int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("buffer page limit must be positive");
        }
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        String lower = startKeyInclusive;
        boolean inclusive = true;
        if (afterKeyExclusive != null && (lower == null || com.flyingzc.azir.storage.Utf8KeyOrder.compare(afterKeyExclusive, lower) >= 0)) {
            lower = afterKeyExclusive;
            inclusive = false;
        }
        if (lower != null && endKeyExclusive != null && com.flyingzc.azir.storage.Utf8KeyOrder.compare(lower, endKeyExclusive) >= 0) {
            return List.of();
        }
        NavigableMap<String, Mutation> view = lower == null ? mutations : mutations.tailMap(lower, inclusive);
        if (endKeyExclusive != null) {
            view = view.headMap(endKeyExclusive, false);
        }
        return view.values().stream().limit(limit).toList();
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
     * Return the transaction state.
     *
     * @param status status
     */
    public void status(final TxStatus status) {
        this.status = status;
    }

    /**
     * Read and write to transaction metadata already marked with durable storage.
     *
     * @return the operation result
     */
    public boolean metadataPersisted() {
        return metadataPersisted;
    }

    /**
     * Read and write to transaction metadata already marked with durable storage.
     *
     * @param metadataPersisted metadata persisted
     */
    public void metadataPersisted(final boolean metadataPersisted) {
        this.metadataPersisted = metadataPersisted;
    }

    /**
     * Returns transaction fixed lease due date.
     *
     * @return the operation result
     */
    public long deadlineMillis() {
        return deadlineMillis;
    }

    /**
     * Check for private ownership markings and do not expose identification values for forgery by the caller.
     *
     * @param owner owner
     * @return the operation result
     */
    public boolean ownedBy(final Object owner) {
        return this.owner != null && this.owner == owner;
    }

    /**
     * The prewrite result allows only rollback after an anomaly to prevent the commit incomplete participant assembly.
     */
    public void markRollbackOnly() {
        rollbackOnly = true;
    }

    /**
     * Returns whether transaction is only allowed for rollback.
     *
     * @return the operation result
     */
    public boolean rollbackOnly() {
        return rollbackOnly;
    }

    /**
     * Mark commit has entered the decision phase that may cause durable storage side effects.
     *
     * @param decisionStarted decision started
     */
    public void decisionStarted(final boolean decisionStarted) {
        this.decisionStarted = decisionStarted;
    }

    /**
     * Determines whether commit decision must be analyzed to deal with anomalies.
     *
     * @return the operation result
     */
    public boolean decisionStarted() {
        return decisionStarted;
    }

    /**
     * The write set fee after the coverage is charged and the transaction limit is rejected before the buffer change.
     *
     * @param shardId logical shard identifier
     * @param mutation mutation
     * @param maxKeys max keys
     * @param maxBytes max bytes
     * @throws com.flyingzc.azir.common.MvccException if the operation cannot complete
     */
    public void reserveWrite(final String shardId, final Mutation mutation, final int maxKeys, final long maxBytes) {
        Map<String, Integer> sizes = writeSizes.computeIfAbsent(shardId, ignored -> new LinkedHashMap<>());
        Integer oldSize = sizes.get(mutation.key());
        long newSize = mutation.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + (long) (mutation.value() == null ? 0 : mutation.value().length);
        int nextKeys = writeKeyCount + (oldSize == null ? 1 : 0);
        long nextBytes = writeBytes - (oldSize == null ? 0 : oldSize) + newSize;
        if (nextKeys > maxKeys || nextBytes > maxBytes || newSize > Integer.MAX_VALUE) {
            throw new com.flyingzc.azir.common.MvccException(com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED,
                    "transaction write budget exceeded", false, txId);
        }
        sizes.put(mutation.key(), (int) newSize);
        writeKeyCount = nextKeys;
        writeBytes = nextBytes;
    }
}

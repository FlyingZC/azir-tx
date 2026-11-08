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

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Storage adapter participant Door Test, verify protocol interface correctly entrusts existing storage adapter.
 */
class StorageAdapterParticipantStoreTest {

    /**
     * Verify Writing, commit and Query operations are commissioned to the bottom level StorageAdapter.
     */
    @Test
    void shouldDelegateOperationsToStorageAdapter() {
        RecordingStorageAdapter adapter = new RecordingStorageAdapter("shard-1");
        ParticipantStore store = new StorageAdapterParticipantStore(adapter);
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(Mutation.put("account:1", new byte[]{1}));

        store.prewrite(10L, 10L, mutations);
        store.prepare(10L);
        store.commit(10L, 11L, mutations, true);
        store.directCommit(12L, 12L, 13L, mutations);
        store.cleanupIntents(10L, mutations);
        store.abort(14L);
        store.findPendingTransactionIds(5);
        store.gc(20L, 100);

        Assertions.assertEquals("shard-1", store.shardId());
        assertTrue(adapter.calls.contains("prewrite:10"));
        assertTrue(adapter.calls.contains("prepare:10"));
        assertTrue(adapter.calls.contains("commit-known:10:cleanup=true"));
        assertTrue(adapter.calls.contains("directCommit:12"));
        assertTrue(adapter.calls.contains("cleanup-known:10"));
        assertTrue(adapter.calls.contains("abort:14"));
        assertTrue(adapter.calls.contains("findPending:5"));
        assertTrue(adapter.calls.contains("gc:20:100"));
    }

    /**
     * The verify reading path operation is entrusted to the bottom stageStorageAdapter.
     */
    @Test
    void shouldDelegateReadOperationsToStorageAdapter() {
        RecordingStorageAdapter adapter = new RecordingStorageAdapter("shard-1");
        ParticipantStore store = new StorageAdapterParticipantStore(adapter);
        ReadContext context = new ReadContext(1L, 1L, IsolationLevel.READ_COMMITTED);

        store.get(context, "account:1");
        store.scan(context, new KeyRange("account:0", "account:9"));

        assertTrue(adapter.calls.contains("get:account:1"));
        assertTrue(adapter.calls.contains("scan:account:0:account:9"));
    }

    /**
     * Records the callable storage adapter test stake.
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {

        private final String shardId;

        private final List<String> calls = new ArrayList<>();

        /**
         * Construct a record storage adapter.
         *
         * @param shardId  Shard Number.
         */
        private RecordingStorageAdapter(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Returns shard number.
         *
         * @return  Shard Number.
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * Initialization test stakes.
         */
        @Override
        public void initialize() {
            calls.add("initialize");
        }

        /**
         * Record a single key to read the call.
         *
         * @param context Read Context.
         * @param key Business Keys.
         * @return Empty Results.
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            calls.add("get:" + key);
            return Optional.empty();
        }

        /**
         * Record range scanned for call.
         *
         * @param context Read Context.
         * @param keyRange Key Range.
         * @return Empty list.
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
            calls.add("scan:" + keyRange.startKeyInclusive() + ":" + keyRange.endKeyExclusive());
            return new ArrayList<>();
        }

        /**
         * Record prewrite call.
         *
         * @param txId  Transaction identifier.
         * @param startTs  Start timestamp.
         * @param mutations  Write set.
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            calls.add("prewrite:" + txId);
        }

        /**
         * Record the prepare call.
         *
         * @param txId  Transaction identifier.
         */
        @Override
        public void prepare(final long txId) {
            calls.add("prepare:" + txId);
        }

        /**
         * Record commit call.
         *
         * @param txId  Transaction identifier.
         * @param commitTs  Commit timestamp.
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            calls.add("commit:" + txId);
        }

        /**
         * Records the known write set commit call.
         *
         * @param txId  Transaction identifier.
         * @param commitTs  Commit timestamp.
         * @param mutations  Write set.
         * @param cleanupIntents Whether to clear intent.
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            calls.add("commit-known:" + txId + ":cleanup=" + cleanupIntents);
        }

        /**
         * Records the direct commit call.
         *
         * @param txId  Transaction identifier.
         * @param startTs  Start timestamp.
         * @param commitTs  Commit timestamp.
         * @param mutations  Write set.
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            calls.add("directCommit:" + txId);
        }

        /**
         * Records the cleanup of known intent calls.
         *
         * @param txId  Transaction identifier.
         * @param mutations  Write set.
         */
        @Override
        public void cleanupIntents(final long txId, final List<Mutation> mutations) {
            calls.add("cleanup-known:" + txId);
        }

        /**
         * Record rollback call.
         *
         * @param txId  Transaction identifier.
         */
        @Override
        public void abort(final long txId) {
            calls.add("abort:" + txId);
        }

        /**
         * Record to be called by recovery transaction query.
         *
         * @param limit Maximum number of queries.
         * @return Empty assembly.
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            calls.add("findPending:" + limit);
            return new LinkedHashSet<>();
        }

        /**
         * Record GC call.
         *
         * @param safePoint  Safe point.
         * @param limit Maximum number of cleanups.
         * @return Number of cleanups.
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            calls.add("gc:" + safePoint + ":" + limit);
            return 0;
        }
    }
}

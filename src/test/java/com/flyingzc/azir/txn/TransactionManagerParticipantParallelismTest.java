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

package com.flyingzc.azir.txn;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transaction Manager participant performs the test in parallel.
 */
class TransactionManagerParticipantParallelismTest {

    /**
     * When verify opens the parallel prewrite, two participating shard will also execute concurrent.
     */
    @Test
    void twoParticipantPrewriteShouldRunConcurrentlyWhenParallelPrewriteEnabled() {
        CountDownLatch enteredPrewrite = new CountDownLatch(2);
        BlockingPrewriteStorageAdapter shard1 = new BlockingPrewriteStorageAdapter("shard-1", enteredPrewrite);
        BlockingPrewriteStorageAdapter shard2 = new BlockingPrewriteStorageAdapter("shard-2", enteredPrewrite);
        TransactionOptions options = new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2),
                true,
                true,
                false);
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "parallel:1", new byte[]{1});
        manager.put(tx, "shard-2", "parallel:2", new byte[]{2});

        assertDoesNotThrow(() -> manager.commit(tx));
        Assertions.assertEquals(1, shard1.prewriteCalls());
        Assertions.assertEquals(1, shard2.prewriteCalls());
    }

    /**
     * Verify Parallel participant Throw Error will not be swallowed.
     */
    @Test
    void parallelParticipantErrorShouldPropagateToCaller() {
        ErrorPrewriteStorageAdapter shard1 = new ErrorPrewriteStorageAdapter("shard-1");
        BlockingPrewriteStorageAdapter shard2 = new BlockingPrewriteStorageAdapter("shard-2", new CountDownLatch(1));
        TransactionOptions options = new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2),
                true,
                true,
                false);
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "parallel:error:1", new byte[]{1});
        manager.put(tx, "shard-2", "parallel:error:2", new byte[]{2});

        AssertionError actual = assertThrows(AssertionError.class, () -> manager.commit(tx));
        Assertions.assertEquals("participant error", actual.getMessage());
    }

    /**
     * Verify Parallel participant Throws RuntimeException will not be swallowed by Completable Future.
     */
    @Test
    void parallelParticipantRuntimeExceptionShouldPropagateToCaller() {
        RuntimeExceptionPrewriteStorageAdapter shard1 = new RuntimeExceptionPrewriteStorageAdapter("shard-1");
        BlockingPrewriteStorageAdapter shard2 = new BlockingPrewriteStorageAdapter("shard-2", new CountDownLatch(1));
        TransactionOptions options = new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2),
                true,
                true,
                false);
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "parallel:runtime:1", new byte[]{1});
        manager.put(tx, "shard-2", "parallel:runtime:2", new byte[]{2});

        IllegalStateException actual = assertThrows(IllegalStateException.class, () -> manager.commit(tx));
        Assertions.assertEquals("participant runtime failure", actual.getMessage());
    }

    /**
     * Waits for another storage adapter to go into shard at the same time in phase prewrite.
     */
    private static final class BlockingPrewriteStorageAdapter implements StorageAdapter {

        private final String shardId;

        private final CountDownLatch enteredPrewrite;

        private final AtomicInteger prewriteCalls = new AtomicInteger();

        /**
         * Construct a block prewrite adapter.
         *
         * @param shardId logical shard identifier
         * @param enteredPrewrite entered prewrite
         */
        private BlockingPrewriteStorageAdapter(final String shardId, final CountDownLatch enteredPrewrite) {
            this.shardId = shardId;
            this.enteredPrewrite = enteredPrewrite;
        }

        /**
         * Simulates cross-shard Visibility capability, verify parallel prewrite schedule only.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * Return the shard identifier.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * Initialization test adapter.
         */
        @Override
        public void initialize() {
        }

        /**
         * Reads the specified key.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            return Optional.empty();
        }

        /**
         * Scans the specified range.
         *
         * @param context context
         * @param keyRange key range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
            return List.of();
        }

        /**
         * Prewrite and wait for another shard to enter at the same time.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         * @throws AssertionError if the operation cannot complete
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            prewriteCalls.incrementAndGet();
            enteredPrewrite.countDown();
            try {
                if (!enteredPrewrite.await(300, TimeUnit.MILLISECONDS)) {
                    throw new AssertionError("prewrite did not run concurrently");
                }
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for concurrent prewrite", ex);
            }
        }

        /**
         * Prepare the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
        }

        /**
         * Commit Test transaction.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
        }

        /**
         * Commit Known Test write set.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        }

        /**
         * Roll back the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void abort(final long txId) {
        }

        /**
         * Find to recovery transaction identifier.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return Set.of();
        }

        /**
         * Clear history version.
         *
         * @param safePoint oldest retained snapshot timestamp
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            return 0;
        }

        /**
         * Returns the number of calls for prewrite.
         *
         * @return the operation result
         */
        private int prewriteCalls() {
            return prewriteCalls.get();
        }
    }

    /**
     * The storage adaptor will be thrown out of Error during the prewrite stage.
     */
    private static final class ErrorPrewriteStorageAdapter implements StorageAdapter {

        private final String shardId;

        /**
         * Tectonic error prewrite adapter.
         *
         * @param shardId logical shard identifier
         */
        private ErrorPrewriteStorageAdapter(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Simulates the cross-shard Visibility capability that allows an prewrite error to enter the original abnormal transmission path.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * Return the shard identifier.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * Initialization test adapter.
         */
        @Override
        public void initialize() {
        }

        /**
         * Reads the specified key.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            return Optional.empty();
        }

        /**
         * Scans the specified range.
         *
         * @param context context
         * @param keyRange key range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
            return List.of();
        }

        /**
         * Throws a test error at prewrite.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         * @throws AssertionError if the operation cannot complete
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            throw new AssertionError("participant error");
        }

        /**
         * Prepare the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
        }

        /**
         * Commit Test transaction.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
        }

        /**
         * Commit Known Test write set.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        }

        /**
         * Roll back the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void abort(final long txId) {
        }

        /**
         * Find to recovery transaction identifier.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return Set.of();
        }

        /**
         * Clear history version.
         *
         * @param safePoint oldest retained snapshot timestamp
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            return 0;
        }
    }

    /**
     * The storage adaptor will be thrown out of RuntimeException at the prewrite stage.
     */
    private static final class RuntimeExceptionPrewriteStorageAdapter implements StorageAdapter {

        private final String shardId;

        /**
         * Construct an abnormal prewrite adapter when running.
         *
         * @param shardId logical shard identifier
         */
        private RuntimeExceptionPrewriteStorageAdapter(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Simulates the cross-shard Visibility capabilities that allow prewrite anomalies to enter the original abnormal transmission path.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * Return the shard identifier.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * Initialization test adapter.
         */
        @Override
        public void initialize() {
        }

        /**
         * Reads the specified key.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            return Optional.empty();
        }

        /**
         * Scans the specified range.
         *
         * @param context context
         * @param keyRange key range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
            return List.of();
        }

        /**
         * Drops the test running anomaly when prewrite is in.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            throw new IllegalStateException("participant runtime failure");
        }

        /**
         * Prepare the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
        }

        /**
         * Commit Test transaction.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
        }

        /**
         * Commit Known Test write set.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        }

        /**
         * Roll back the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void abort(final long txId) {
        }

        /**
         * Find to recovery transaction identifier.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return Set.of();
        }

        /**
         * Clear history version.
         *
         * @param safePoint oldest retained snapshot timestamp
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            return 0;
        }
    }
}

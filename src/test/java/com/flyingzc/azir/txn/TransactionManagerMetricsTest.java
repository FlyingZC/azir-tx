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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metrics.InMemoryTransactionMetrics;
import com.flyingzc.azir.metrics.TransactionMetrics;
import com.flyingzc.azir.metrics.TransactionStage;
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
import java.util.concurrent.TimeUnit;

/**
 * Transaction Manager indicator log test.
 */
class TransactionManagerMetricsTest {

    /**
     * Verify does not support single shard adapter prewrite and core commit stages of atomic direct commit.
     */
    @Test
    void singleShardBufferedCommitShouldRecordStages() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard),
                metrics);

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.commit(tx);

        assertSuccess(metrics, TransactionStage.BEGIN_TSO, 1L);
        assertSuccess(metrics, TransactionStage.BEGIN_METADATA, 1L);
        assertSuccess(metrics, TransactionStage.BUFFERED_PREWRITE, 1L);
        Assertions.assertEquals(1, shard.prewriteAttempts());
        assertSuccess(metrics, TransactionStage.COMMIT_TSO, 1L);
        assertSuccess(metrics, TransactionStage.SINGLE_SHARD_METADATA_COMMIT, 1L);
        assertSuccess(metrics, TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT, 1L);
    }

    /**
     * Verify cross-shard commit records the prewrite, metadata and master shard commit stages.
     */
    @Test
    void crossShardCommitShouldRecordStages() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter shard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                metrics);

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.put(tx, "shard-2", "k2", new byte[]{2});
        manager.commit(tx);

        assertSuccess(metrics, TransactionStage.BUFFERED_PREWRITE, 1L);
        assertSuccess(metrics, TransactionStage.COMMIT_TSO, 1L);
        assertSuccess(metrics, TransactionStage.CROSS_SHARD_METADATA_COMMIT, 1L);
        assertSuccess(metrics, TransactionStage.PRIMARY_PARTICIPANT_COMMIT, 1L);
        assertSuccess(metrics, TransactionStage.SECONDARY_PARTICIPANT_COMMIT, 1L);
    }

    /**
     * Verify Non- buffer writing path records the eager prewrite phase.
     */
    @Test
    void eagerPrewriteShouldRecordStageWhenWritesAreNotBuffered() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.failFastDefaults(),
                Map.of(shard.shardId(), shard),
                metrics);

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        assertSuccess(metrics, TransactionStage.EAGER_PREWRITE, 1L);
    }

    /**
     * The verify eager prewrite phase will include the full stage of conflict waiting for retry.
     */
    @Test
    void eagerPrewriteShouldRecordOneStageAcrossTransientRetry() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        shard.failPrewriteAttempts(1);
        TransactionOptions options = new TransactionOptions(
                new LockWaitPolicy(ConflictMode.WAIT, 100L, 0L, 0L, 1.0d, false),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(),
                false,
                false,
                false);
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard.shardId(), shard),
                metrics);

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        Assertions.assertEquals(2, shard.prewriteAttempts());
        Assertions.assertEquals(1L, metrics.snapshot(TransactionStage.EAGER_PREWRITE).invocationCount());
        assertSuccess(metrics, TransactionStage.EAGER_PREWRITE, 1L);
    }

    /**
     * The verify indicator record of success will not change the result of transaction commit.
     */
    @Test
    void metricsSuccessFailureShouldNotAffectTransactionFlow() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard),
                new ThrowingTransactionMetrics(true, false));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        assertDoesNotThrow(() -> manager.commit(tx));
    }

    /**
     * The verify indicator failure anomaly does not obscure the original business anomaly.
     */
    @Test
    void metricsFailureFailureShouldNotMaskTransactionFailure() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        shard.failCommit(true);
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard),
                new ThrowingTransactionMetrics(false, true));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        MvccException ex = assertThrows(MvccException.class, () -> manager.commit(tx));
        Assertions.assertEquals(com.flyingzc.azir.common.ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
        assertFalse(ex.retryable());
        MvccException cause = Assertions.assertInstanceOf(MvccException.class, ex.getCause());
        Assertions.assertEquals(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, cause.errorCode());
    }

    /**
     * Verify Step commit background missions record successes and failures.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void asyncCommitShouldRecordBackgroundSuccessAndFailure() throws Exception {
        InMemoryTransactionMetrics successMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter successShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter successShard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager successManager = manager(
                TransactionOptions.mysqlLikeDefaults(),
                successMetrics,
                successShard1,
                successShard2);

        Transaction successTx = successManager.begin(IsolationLevel.REPEATABLE_READ);
        successManager.put(successTx, "shard-1", "k1", new byte[]{1});
        successManager.put(successTx, "shard-2", "k2", new byte[]{2});
        successManager.commit(successTx);
        assertEventually(successMetrics, TransactionStage.ASYNC_PARTICIPANT_COMMIT, 2L, 0L);

        InMemoryTransactionMetrics failureMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter failingShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter failingShard2 = new RecordingStorageAdapter("shard-2");
        failingShard2.failCommit(true);
        TransactionManager failureManager = manager(
                TransactionOptions.mysqlLikeDefaults(),
                failureMetrics,
                failingShard1,
                failingShard2);

        Transaction failureTx = failureManager.begin(IsolationLevel.REPEATABLE_READ);
        failureManager.put(failureTx, "shard-1", "k3", new byte[]{3});
        failureManager.put(failureTx, "shard-2", "k4", new byte[]{4});
        failureManager.commit(failureTx);
        assertEventually(failureMetrics, TransactionStage.ASYNC_PARTICIPANT_COMMIT, 1L, 1L);
    }

    /**
     * Verify background inent cleanup will record successes and failures.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void deferredIntentCleanupShouldRecordBackgroundSuccessAndFailure() throws Exception {
        InMemoryTransactionMetrics successMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter successShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter successShard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager successManager = manager(
                asyncDeferCleanupOptions(),
                successMetrics,
                successShard1,
                successShard2);

        Transaction successTx = successManager.begin(IsolationLevel.REPEATABLE_READ);
        successManager.put(successTx, "shard-1", "k1", new byte[]{1});
        successManager.put(successTx, "shard-2", "k2", new byte[]{2});
        successManager.commit(successTx);
        assertEventually(successMetrics, TransactionStage.INTENT_CLEANUP, 2L, 0L);

        InMemoryTransactionMetrics failureMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter failingShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter failingShard2 = new RecordingStorageAdapter("shard-2");
        failingShard2.failCleanup(true);
        TransactionManager failureManager = manager(
                asyncDeferCleanupOptions(),
                failureMetrics,
                failingShard1,
                failingShard2);

        Transaction failureTx = failureManager.begin(IsolationLevel.REPEATABLE_READ);
        failureManager.put(failureTx, "shard-1", "k3", new byte[]{3});
        failureManager.put(failureTx, "shard-2", "k4", new byte[]{4});
        failureManager.commit(failureTx);
        assertEventually(failureMetrics, TransactionStage.INTENT_CLEANUP, 1L, 1L);
    }

    /**
     * Verify Turns off the transaction manager waits for async committee before cleanup indicators are visible.
     */
    @Test
    void closeShouldDrainAsyncCommitAndIntentCleanupMetrics() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter shard2 = new RecordingStorageAdapter("shard-2");
        shard1.commitDelayMillis(50L);
        shard2.commitDelayMillis(50L);
        TransactionManager manager = manager(asyncDeferCleanupOptions(), metrics, shard1, shard2);

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.put(tx, "shard-2", "k2", new byte[]{2});
        manager.commit(tx);
        manager.close();

        assertSuccess(metrics, TransactionStage.ASYNC_PARTICIPANT_COMMIT, 2L);
        assertSuccess(metrics, TransactionStage.INTENT_CLEANUP, 2L);
    }

    /**
     * Creates an transaction manager with indicators.
     *
     * @param options options
     * @param metrics metrics
     * @param shard1 shard1
     * @param shard2 shard2
     * @return the operation result
     */
    private TransactionManager manager(final TransactionOptions options,
                                       final InMemoryTransactionMetrics metrics,
                                       final RecordingStorageAdapter shard1,
                                       final RecordingStorageAdapter shard2) {
        return new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                metrics);
    }

    /**
     * Creates the test option for walk commit and delays cleaning inent.
     *
     * @return the operation result
     */
    private TransactionOptions asyncDeferCleanupOptions() {
        return new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2),
                true,
                false,
                true);
    }

    /**
     * The number of successes in the designated phase is asserted.
     *
     * @param metrics metrics
     * @param stage stage
     * @param expectedSuccessCount expected success count
     */
    private void assertSuccess(final InMemoryTransactionMetrics metrics, final TransactionStage stage, final long expectedSuccessCount) {
        Assertions.assertEquals(expectedSuccessCount, metrics.snapshot(stage).successCount(), stage.name());
        Assertions.assertEquals(0L, metrics.snapshot(stage).failureCount(), stage.name());
    }

    /**
     * Waiting for a step-by-step period of statistics to meet expectations.
     *
     * @param metrics metrics
     * @param stage stage
     * @param expectedSuccessCount expected success count
     * @param expectedFailureCount expected failure count
     * @throws Exception if the operation cannot complete
     */
    private void assertEventually(final InMemoryTransactionMetrics metrics,
                                  final TransactionStage stage,
                                  final long expectedSuccessCount,
                                  final long expectedFailureCount) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadline) {
            if (metrics.snapshot(stage).successCount() == expectedSuccessCount
                    && metrics.snapshot(stage).failureCount() == expectedFailureCount) {
                return;
            }
            Thread.sleep(10L);
        }
        Assertions.assertEquals(expectedSuccessCount, metrics.snapshot(stage).successCount(), stage.name());
        Assertions.assertEquals(expectedFailureCount, metrics.snapshot(stage).failureCount(), stage.name());
    }

    /**
     * Records the callable and failed test storage adaptor.
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {

        private final String shardId;

        private boolean failCommit;

        private boolean failCleanup;

        private int failPrewriteAttempts;

        private int prewriteAttempts;

        private long commitDelayMillis;

        /**
         * Construct to test storage adaptor.
         *
         * @param shardId logical shard identifier
         */
        private RecordingStorageAdapter(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Simulation of cross-shard Visibility only, with test ranges for indicator recording and abnormal transmission.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * Sets whether commit failed.
         *
         * @param failCommit fail commit
         */
        private void failCommit(final boolean failCommit) {
            this.failCommit = failCommit;
        }

        /**
         * Sets whether the cleanup failed.
         *
         * @param failCleanup fail cleanup
         */
        private void failCleanup(final boolean failCleanup) {
            this.failCleanup = failCleanup;
        }

        /**
         * Sets the number of temporary prewrite failures.
         *
         * @param failPrewriteAttempts fail prewrite attempts
         */
        private void failPrewriteAttempts(final int failPrewriteAttempts) {
            this.failPrewriteAttempts = failPrewriteAttempts;
        }

        /**
         * Returns the number of attempts at prewrite.
         *
         * @return the operation result
         */
        private int prewriteAttempts() {
            return prewriteAttempts;
        }

        /**
         * Sets the number of milliseconds delayed by commit.
         *
         * @param commitDelayMillis commit delay millis
         */
        private void commitDelayMillis(final long commitDelayMillis) {
            this.commitDelayMillis = commitDelayMillis;
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
         * Initialised test storage.
         */
        @Override
        public void initialize() {
        }

        /**
         * Reads the test value.
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
         * Scan test values.
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
         * Writing test inent.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            prewriteAttempts++;
            if (failPrewriteAttempts > 0) {
                failPrewriteAttempts--;
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.LOCK_CONFLICT, "prewrite conflict", true, txId);
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
         * Commit Test inent.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            commit(txId, commitTs, List.of(), true);
        }

        /**
         * Commit Test write set.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            sleepQuietly(commitDelayMillis);
            if (failCommit) {
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "commit failed", true, txId);
            }
        }

        /**
         * Direct commit test write set.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @throws AssertionError if the operation cannot complete
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            throw new AssertionError("metrics adapter does not implement atomic direct commit");
        }

        /**
         * Clear test indent.
         *
         * @param txId transaction identifier
         * @param mutations transaction write set
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void cleanupIntents(final long txId, final List<Mutation> mutations) {
            if (failCleanup) {
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "cleanup failed", true, txId);
            }
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
         * Clear test history version.
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
         * Quietly wait for the specified milliseconds.
         *
         * @param delayMillis delay millis
         * @throws MvccException if the operation cannot complete
         */
        private void sleepQuietly(final long delayMillis) {
            if (delayMillis <= 0L) {
                return;
            }
            try {
                Thread.sleep(delayMillis);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "interrupted", true, null, ex);
            }
        }
    }

    /**
     * Simulationable indicators have been achieved to record abnormally tested indicators.
     *
     * @param failSuccess fail success
     * @param failFailure fail failure
     */
    private record ThrowingTransactionMetrics(boolean failSuccess, boolean failFailure) implements TransactionMetrics {

        /**
         * Record success indicators.
         *
         * @param stage stage
         * @param elapsedNanos elapsed nanos
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void recordSuccess(final TransactionStage stage, final long elapsedNanos) {
            if (failSuccess) {
                throw new IllegalStateException("metrics success failed");
            }
        }

        /**
         * Record failure indicators.
         *
         * @param stage stage
         * @param elapsedNanos elapsed nanos
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void recordFailure(final TransactionStage stage, final long elapsedNanos) {
            if (failFailure) {
                throw new IllegalStateException("metrics failure failed");
            }
        }
    }
}

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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Transaction manager no-op metrics thermal path testing.
 */
class TransactionManagerNoOpHotPathTest {

    /**
     * The verify transaction manager no longer uses the old recordStage package heat path.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void noOpHotPathShouldNotUseGenericRecordStageWrapper() throws Exception {
        String source = readTransactionManagerSource();

        assertFalse(source.contains("recordStage("));
    }

    /**
     * The verify transaction entrance does not enter directly to measure packing methods.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void publicHotPathShouldNotCallMeasuredStageDirectly() throws Exception {
        String source = readTransactionManagerSource();
        String publicAndBusinessSource = source.substring(0, source.indexOf("private boolean metricsEnabled()"));

        assertFalse(publicAndBusinessSource.contains("recordMeasuredStage("));
    }

    /**
     * Verify All measuring helper default no-op metrics.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void measuredStageHelpersShouldGuardNoOpMetricsBeforeWrappingAction() throws Exception {
        String source = readTransactionManagerSource();

        assertMeasuredStageGuard(source, "nextTimestamp(TransactionStage stage)");
        assertMeasuredStageGuard(source, "acceleratedBeginStage(Supplier<Optional<BeginReservation>> action)");
        assertMeasuredStageGuard(source, "acceleratedCommitStage(Supplier<OptionalLong> action)");
        assertMeasuredStageGuard(source, "ledgerBeginStage(IsolationLevel isolationLevel)");
        assertMeasuredStageGuard(source, "ledgerReserveStage(Transaction tx)");
        assertMeasuredStageGuard(source, "ledgerDecisionStage(Transaction tx, long commitTs)");
        assertMeasuredStageGuard(source, "ledgerAtomicCommitStage(Transaction tx)");
        assertMeasuredStageGuard(source, "private long ledgerPublishStage()");
        assertMeasuredStageGuard(source, "ledgerAwaitStage(Transaction tx, long commitTs)");
        assertMeasuredStageGuard(source, "createBeginMetadata(long txId, long startTs, IsolationLevel isolationLevel)");
        assertMeasuredStageGuard(source, "markSingleShardCommittedStage(Transaction tx, String participant, long commitTs)");
        assertMeasuredStageGuard(source, "prewriteBufferedMutationsStage(Transaction tx)");
        assertMeasuredStageGuard(source, "prewriteEagerMutationStage(Transaction tx, String shardId, Mutation mutation, StorageAdapter adapter)");
        assertMeasuredStageGuard(source, "prepareParticipantsStage(Transaction tx)");
        assertMeasuredStageGuard(source, "persistCommittedMetadataStage(Transaction tx, long commitTs, boolean explicitPrepareRequired)");
        assertMeasuredStageGuard(source, "commitParticipantStage(TransactionStage stage, Transaction tx, String shardId, long commitTs)");
        assertMeasuredStageGuard(source, "directCommitSingleShardParticipantStage(Transaction tx, String shardId, long commitTs)");
        assertMeasuredStageGuard(source, "rollbackStage(Transaction tx)");
        assertMeasuredStageGuard(source, "cleanupIntentsStage(Transaction tx, String shardId)");
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void defaultNoOpMetricsShouldStillCommitSingleShardTransaction() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.commit(tx);

        Assertions.assertEquals(0, shard.directCommitCount());
        Assertions.assertEquals(1, shard.prewriteCount);
        Assertions.assertEquals(1, shard.commitCount());
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void defaultNoOpMetricsShouldStillCommitCrossShardTransaction() {
        RecordingStorageAdapter shard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter shard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.put(tx, "shard-2", "k2", new byte[]{2});
        manager.commit(tx);

        Assertions.assertEquals(1, shard1.commitCount());
        Assertions.assertEquals(1, shard2.commitCount());
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void defaultNoOpMetricsShouldStillRollbackParticipants() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.failFastDefaults(),
                Map.of(shard.shardId(), shard));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.rollback(tx);

        Assertions.assertEquals(1, shard.abortCount());
    }

    /**
     * Reads the transaction manager source code.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String readTransactionManagerSource() throws Exception {
        return Files.readString(Path.of("src/main/java/com/flyingzc/azir/txn/TransactionManager.java"));
    }

    /**
     * It is asserted that the measurement of the helper judge no-op metrics before the packaging business action.
     *
     * @param source database connection source
     * @param signature signature
     */
    private static void assertMeasuredStageGuard(final String source, final String signature) {
        String methodSource = extractMethodSource(source, signature);
        int guardIndex = methodSource.indexOf("if (!metricsEnabled())");
        int measuredIndex = methodSource.indexOf("recordMeasuredStage(");

        assertTrue(guardIndex >= 0, signature + " should guard no-op metrics");
        assertTrue(measuredIndex >= 0, signature + " should record measured metrics when enabled");
        assertTrue(guardIndex < measuredIndex, signature + " should guard before recordMeasuredStage");
    }

    /**
     * Intercepts the specified method from the source code.
     *
     * @param source database connection source
     * @param signature signature
     * @return the operation result
     * @throws AssertionError if the operation cannot complete
     */
    private static String extractMethodSource(final String source, final String signature) {
        int signatureIndex = source.indexOf(signature);
        assertTrue(signatureIndex >= 0, signature + " should exist");
        int bodyStart = source.indexOf('{', signatureIndex);
        assertTrue(bodyStart >= 0, signature + " should have method body");

        int depth = 0;
        for (int index = bodyStart; index < source.length(); index++) {
            char current = source.charAt(index);
            if (current == '{') {
                depth++;
            } else if (current == '}') {
                depth--;
                if (depth == 0) {
                    return source.substring(signatureIndex, index + 1);
                }
            }
        }
        throw new AssertionError(signature + " should have balanced braces");
    }

    /**
     * Test storage adapter to record the number of calls.
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {

        private final String shardId;

        private int prewriteCount;

        private int commitCount;

        private int directCommitCount;

        private int abortCount;

        /**
         * Construct to test storage adaptor.
         *
         * @param shardId logical shard identifier
         */
        private RecordingStorageAdapter(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Simulates only the cross-shard Visibility capability to enable count tests to enter the participant call path.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * Returns the number of direct commit times.
         *
         * @return the operation result
         */
        private int directCommitCount() {
            return directCommitCount;
        }

        /**
         * Returns normal commit times.
         *
         * @return the operation result
         */
        private int commitCount() {
            return commitCount;
        }

        /**
         * Returns rollback times.
         *
         * @return the operation result
         */
        private int abortCount() {
            return abortCount;
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
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            prewriteCount++;
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
            commitCount++;
        }

        /**
         * Commit Test write set.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            commitCount++;
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
            directCommitCount++;
            throw new AssertionError("counting adapter does not implement atomic direct commit");
        }

        /**
         * Clear test indent.
         *
         * @param txId transaction identifier
         * @param mutations transaction write set
         */
        @Override
        public void cleanupIntents(final long txId, final List<Mutation> mutations) {
        }

        /**
         * Roll back the test transaction.
         *
         * @param txId transaction identifier
         */
        @Override
        public void abort(final long txId) {
            abortCount++;
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
    }
}

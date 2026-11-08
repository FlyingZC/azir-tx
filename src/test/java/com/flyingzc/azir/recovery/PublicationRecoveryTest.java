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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Verify lease Takeover, Single shard local commit Remedial Centre and recovery Error segregation.
 */
class PublicationRecoveryTest {

    /**
     * Independent recovery must bind the old MySQL adapter to centre authority without starting database.
     */
    @Test
    void standaloneRecoveryBindsLegacyMysqlAdapter() {
        var metadata = new InMemoryTransactionMetadataStore();
        try (var unopenedPool = new HikariDataSource()) {
            var adapter = new MysqlStorageAdapter("a", unopenedPool);
            assertFalse(adapter.supportsCommittedIntentResolution());
            new RecoveryManager(metadata, Map.of("a", adapter));
            assertTrue(adapter.supportsCommittedIntentResolution());
            assertDoesNotThrow(() -> adapter.configureMetadataStore(metadata));
            assertThrows(IllegalStateException.class, () -> adapter.configureMetadataStore(new InMemoryTransactionMetadataStore()));
        }
    }

    /**
     * The illegal route was denied prior to binding, and the same meta-data example was validly mapped.
     */
    @Test
    void recoveryValidatesRoutesBeforeBindingAuthority() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        assertThrows(IllegalArgumentException.class, () -> new RecoveryManager(metadata, Map.of("other", adapter)));
        assertThrows(com.flyingzc.azir.common.MvccException.class, () -> new RecoveryManager(metadata, Map.of(" ", adapter)));
        assertThrows(com.flyingzc.azir.common.MvccException.class, () -> new RecoveryManager(metadata, Map.of("a,b", adapter)));
        assertNull(adapter.configuredMetadata);
        new RecoveryManager(metadata, Map.of("a", adapter));
        Assertions.assertSame(metadata, adapter.configuredMetadata);
    }

    /**
     * None of the empty atomic commit participant, pending records, or mismatch terminal state removes the centre unknown slot.
     */
    @Test
    void atomicFenceRequiresMatchingDurableTerminalRecord() {
        List<Optional<ParticipantRecord>> invalidRecords = List.of(Optional.empty(),
                Optional.of(new ParticipantRecord(1, 1, TxStatus.ACTIVE, null)),
                Optional.of(new ParticipantRecord(1, 1, TxStatus.PREPARING, null)),
                Optional.of(new ParticipantRecord(2, 1, TxStatus.ABORTED, null)),
                Optional.of(new ParticipantRecord(1, 0, TxStatus.ABORTED, null)),
                Optional.of(new ParticipantRecord(1, 2, TxStatus.COMMITTED, 10L)),
                Optional.of(new ParticipantRecord(1, 1, TxStatus.COMMITTED, 11L)));
        for (Optional<ParticipantRecord> invalid : invalidRecords) {
            var metadata = new InMemoryTransactionMetadataStore();
            var adapter = new RecordingAdapter(metadata);
            adapter.atomicDirect = true;
            adapter.preserveFenceRecord = true;
            adapter.local = invalid.orElse(null);
            metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
            metadata.registerParticipants(1, Set.of("a"));
            metadata.markLocalCommitPending(1, 10);
            var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
            assertThrows(com.flyingzc.azir.common.MvccException.class, () -> recovery.recover(1));
            Assertions.assertEquals(TxStatus.PREPARING, metadata.get(1).orElseThrow().status());
            Assertions.assertEquals(10L, metadata.get(1).orElseThrow().commitTs());
            assertThrows(com.flyingzc.azir.common.MvccException.class, metadata::assertNoPendingPublications);
            assertTrue(adapter.aborted.isEmpty());
        }
    }

    /**
     * No independent commit authority old adapter uses centre rollback first, lock out the path.
     */
    @Test
    void nonAtomicParticipantDoesNotNeedLocalFenceRecord() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        new RecoveryManager(metadata, Map.of("a", adapter)).recover(1);
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(1).orElseThrow().status());
        assertTrue(adapter.fenced.isEmpty());
        Assertions.assertEquals(List.of(1L), adapter.aborted);
    }

    /**
     * Local terminal state funcing must be kept in the center when it fails, not first write ABORTED to release new snapshot.
     */
    @Test
    void failedParticipantFenceKeepsPendingPublication() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        adapter.failFence = true;
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.markLocalCommitPending(1, 10);
        var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
        assertThrows(IllegalStateException.class, () -> recovery.recover(1));
        Assertions.assertEquals(TxStatus.PREPARING, metadata.get(1).orElseThrow().status());
        Assertions.assertEquals(10L, metadata.get(1).orElseThrow().commitTs());
        assertThrows(com.flyingzc.azir.common.MvccException.class, metadata::assertNoPendingPublications);
        assertTrue(adapter.committed.isEmpty());
        assertTrue(adapter.aborted.isEmpty());
    }

    /**
     * Effective lease protection of ACTIVE; when taken over after maturity, missing inents of metadata do not constitute an rollback authorization.
     */
    @Test
    void activeLeaseAndMissingMetadataPreventAbort() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.registerSnapshot(1, 1, Long.MAX_VALUE);
        assertFalse(recovery.recover(1, Set.of("a")));
        assertFalse(recovery.recover(2, Set.of("a")));
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(1).orElseThrow().status());
        assertTrue(adapter.aborted.isEmpty());
        metadata.releaseSnapshot(1);
        metadata.registerSnapshot(1, 1, 1);
        assertTrue(recovery.recover(1, Set.of("a")));
        Assertions.assertEquals(List.of(1L), adapter.aborted);
    }

    /**
     * The local commit single shard can only be used as the commit timestamp patch centre, not rollback.
     */
    @Test
    void localCommitCompletesCentralDecisionOutsideFanoutLock() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        adapter.local = new ParticipantRecord(1, 1, TxStatus.COMMITTED, 10L);
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.markLocalCommitPending(1, 10);
        new RecoveryManager(metadata, Map.of("a", adapter)).recover(1);
        Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(1).orElseThrow().status());
        Assertions.assertEquals(10L, metadata.get(1).orElseThrow().commitTs());
        Assertions.assertEquals(List.of(1L), adapter.committed);
        assertTrue(adapter.aborted.isEmpty());
        Assertions.assertEquals(List.of(1L), adapter.fenced);
        assertDoesNotThrow(metadata::assertNoPendingPublications);
        assertTrue(metadata.findRecoveryCandidates(Long.MAX_VALUE, 0, 10).isEmpty());
    }

    /**
     * Metadata candidate does not rely on inent, bad transaction fails and subsequent batch transaction will still be completed.
     */
    @Test
    void metadataCandidatesRecoverWithoutIntentsAndIsolateFailures() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        metadata.createCommitted(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a"), 10);
        metadata.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, Set.of("a"), 11);
        adapter.failCommitTxId = 1;
        var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
        var runner = new BackgroundRecoveryRunner(recovery, Map.of("a", adapter), 2, 2, 10_000);
        Assertions.assertEquals(1, runner.recoverOnce(2));
        Assertions.assertEquals(1, runner.transactionStats().failureCount());
        Assertions.assertEquals(List.of(2L), adapter.committed);
        Assertions.assertEquals(1L, metadata.findRecoveryCandidates(Long.MAX_VALUE, 0, 10).getFirst().txId());
        Assertions.assertEquals(1L, metadata.oldestActiveStartTs().orElseThrow());
    }

    /**
     * Keeps scanned shard, ABORTED even if the metadata participant is empty.
     */
    @Test
    void discoveredShardIsRetainedForTerminalCleanup() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        adapter.pending = Set.of(1L);
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.markAborted(1);
        var runner = new BackgroundRecoveryRunner(new RecoveryManager(metadata, Map.of("a", adapter)), Map.of("a", adapter));
        Assertions.assertEquals(1, runner.recoverOnce(10));
        Assertions.assertEquals(List.of(1L), adapter.aborted);
    }

    /**
     * Record recovery side effects and assert that commit and rollback materialization occurred outside the release lock.
     */
    private static final class RecordingAdapter implements StorageAdapter {

        private final InMemoryTransactionMetadataStore metadata;

        private final List<Long> committed = new ArrayList<>();

        private final List<Long> aborted = new ArrayList<>();

        private final List<Long> fenced = new ArrayList<>();

        private ParticipantRecord local;

        private Set<Long> pending = Set.of();

        private long failCommitTxId = -1;

        private boolean failFence;

        private boolean atomicDirect;

        private boolean preserveFenceRecord;

        private TransactionMetadataStore configuredMetadata;

        /**
         * Injecting a real metadata instance to check the monitor ' s hold.
         *
         * @param metadata metadata
         */
        private RecordingAdapter(final InMemoryTransactionMetadataStore metadata) {
            this.metadata = metadata;
        }

        /**
         * Returns test shard number.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return "a";
        }

        /**
         * The test adaptor does not need to be initialized.
         */
        @Override
        public void initialize() {
        }

        /**
         * Record authority bound by recovery for independent tectonic testing verify.
         *
         * @param store store
         */
        @Override
        public void configureMetadataStore(final TransactionMetadataStore store) {
            configuredMetadata = store;
        }

        /**
         * Test the scene to state whether atomic local commit authority is owned.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsAtomicDirectCommit() {
            return atomicDirect;
        }

        /**
         * This test does not perform reading.
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
         * This test does not perform a range scan.
         *
         * @param context context
         * @param range range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange range) {
            return List.of();
        }

        /**
         * This test does not perform prewrite.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
        }

        /**
         * This test does not perform prepare.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
        }

        /**
         * Returns the simulated durable storage local decision.
         *
         * @param txId transaction identifier
         * @return the operation result
         */
        @Override
        public Optional<ParticipantRecord> participantRecord(final long txId) {
            return Optional.ofNullable(local);
        }

        /**
         * The materialization assertion does not hold the release lock and the configuration simulator transaction is wrong.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            assertFalse(Thread.holdsLock(metadata));
            if (txId == failCommitTxId) {
                throw new IllegalStateException("participant unavailable");
            }
            committed.add(txId);
        }

        /**
         * Barrier contains lateness, barrier records materialization; local commit transaction rollback maintains no side effects.
         *
         * @param txId transaction identifier
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void abort(final long txId) {
            if (Thread.holdsLock(metadata)) {
                if (failFence) {
                    throw new IllegalStateException("participant fence unavailable");
                }
                fenced.add(txId);
            } else {
                aborted.add(txId);
            }
            if (!preserveFenceRecord && (local == null || local.status() != TxStatus.COMMITTED)) {
                local = new ParticipantRecord(txId, txId, TxStatus.ABORTED, null);
            }
        }

        /**
         * Returns the results of the simulation indent scan.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return pending;
        }

        /**
         * This test does not store collection data.
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

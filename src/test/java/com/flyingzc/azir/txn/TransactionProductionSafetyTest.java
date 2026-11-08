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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.gc.GcManager;
import com.flyingzc.azir.gc.InMemorySafePointStore;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.recovery.RecoveryManager;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import com.flyingzc.azir.tso.TimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The source code for the definitive testing of the production boundary is not implemented in this round as required by the user.
 */
class TransactionProductionSafetyTest {

    /**
     * The snapshot distribution must wait for the commit timestamp taken but not yet released commit decision.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void snapshotMustWaitForCommitPublication() throws Exception {
        var metadata = new InMemoryTransactionMetadataStore();
        var allocated = new CountDownLatch(1);
        var publish = new CountDownLatch(1);
        var sequence = new AtomicTimestampOracle(100);
        var calls = new AtomicInteger();
        TimestampOracle tso = () -> {
            long timestamp = sequence.nextTimestamp();
            if (calls.incrementAndGet() == 2) {
                allocated.countDown();
                await(publish);
            }
            return timestamp;
        };
        var shard = new MemoryParticipant("s1");
        try (
                var manager = new TransactionManager(tso, metadata, Map.of("s1", shard));
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "k", new byte[]{7});
            var commit = executor.submit(() -> manager.commit(writer));
            assertTrue(allocated.await(5, TimeUnit.SECONDS));
            var started = new CountDownLatch(1);
            var reader = executor.submit(() -> {
                started.countDown();
                return manager.begin(IsolationLevel.REPEATABLE_READ);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            try {
                assertThrows(TimeoutException.class, () -> reader.get(100, TimeUnit.MILLISECONDS));
            } finally {
                publish.countDown();
            }
            long committedTs = commit.get(5, TimeUnit.SECONDS).commitTs();
            Transaction tx = reader.get(5, TimeUnit.SECONDS);
            assertTrue(tx.startTs() > committedTs);
            assertArrayEquals(new byte[]{7}, manager.get(tx, "s1", "k").orElseThrow());
            assertArrayEquals(new byte[]{7}, manager.get(tx, "s1", "k").orElseThrow());
            manager.commit(tx);
        } finally {
            publish.countDown();
        }
    }

    /**
     * Failed after durable storage commit decision cannot be left with execute template rollback and the rest of participant.
     */
    @Test
    void committedDecisionMustSurviveExecuteFailure() {
        var metadata = new InMemoryTransactionMetadataStore();
        var first = new MemoryParticipant("a");
        var second = new MemoryParticipant("b");
        second.failCommit.set(true);
        Map<String, StorageAdapter> shards = Map.of("a", first, "b", second);
        var txId = new AtomicReference<Long>();
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, shards)) {
            MvccException failure = assertThrows(MvccException.class, () -> manager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                txId.set(tx.txId());
                manager.put(tx, "a", "left", new byte[]{1});
                manager.put(tx, "b", "right", new byte[]{2});
                return null;
            }));
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
            assertFalse(failure.retryable());
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(txId.get()).orElseThrow().status());
            Assertions.assertEquals(0, first.aborts.get() + second.aborts.get());
            assertTrue(second.intents.containsKey(txId.get()));
            new RecoveryManager(metadata, shards).recover(txId.get());
            assertFalse(second.intents.containsKey(txId.get()));
        }
    }

    /**
     * Recovery cannot take over the normal writing that still has valid snapshot lease transaction.
     */
    @Test
    void recoveryMustNotAbortLiveTransaction() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        try (
                var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata,
                        TransactionOptions.failFastDefaults(), Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            new RecoveryManager(metadata, Map.of("s1", shard)).recover(tx.txId());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(tx.txId()).orElseThrow().status());
            Assertions.assertEquals(0, shard.aborts.get());
            manager.rollback(tx);
        }
    }

    /**
     * Lazy read-only transaction still needs to register snapshot and prevent GC from crossing its visible border.
     */
    @Test
    void lazyReaderMustPinSafePoint() {
        var metadata = new InMemoryTransactionMetadataStore();
        var tso = new AtomicTimestampOracle(100);
        var safePoint = new InMemorySafePointStore(0);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(), true, true, false);
        try (var manager = new TransactionManager(tso, metadata, safePoint, options, Map.of())) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(metadata.get(tx.txId()).isEmpty());
            assertTrue(metadata.snapshotExpiry(tx.txId()).isPresent());
            assertTrue(new GcManager(metadata, tso, safePoint, Map.of()).advanceSafePoint() < tx.startTs());
            manager.commit(tx);
            assertTrue(metadata.snapshotExpiry(tx.txId()).isEmpty());
        }
    }

    /**
     * The rejected writing should not be inserted into buffer when exceeding write set budget.
     */
    @Test
    void writeBudgetMustRejectBeforeBufferMutation() {
        var limits = new TransactionLimits(60_000, 1, 1024, 8, 2, 2, 2, 1000);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(), true, true, false, limits);
        try (
                var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                        options, Map.of("s1", new MemoryParticipant("s1")))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "a", new byte[]{1});
            MvccException failure = assertThrows(MvccException.class, () -> manager.put(tx, "s1", "b", new byte[]{2}));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(tx.bufferedMutation("s1", "b").isEmpty());
            manager.rollback(tx);
        }
    }

    /**
     * The background queue is filled with a synchronized retreat and maintains a fixed queue capacity.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void asyncQueueMustBeBoundedAndFallbackSynchronously() throws Exception {
        var first = new MemoryParticipant("a");
        var second = new MemoryParticipant("b");
        first.blockNext.set(true);
        var limits = new TransactionLimits(60_000, 10, 1024, 8, 2, 1, 2, 1000);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(1), true, false, false, limits);
        try (
                var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                        options, Map.of("a", first, "b", second))) {
            try {
                writePair(manager, "1");
                assertTrue(first.entered.await(5, TimeUnit.SECONDS));
                writePair(manager, "2");
                writePair(manager, "3");
                assertTrue(manager.runtimeStats().asyncQueued() <= 1);
                Assertions.assertEquals(1L, manager.runtimeStats().synchronousFallbacks());
            } finally {
                first.proceed.countDown();
            }
        }
    }

    /**
     * When local commit is successful but central pending, rollback must use local COMMITTED results.
     */
    @Test
    void rollbackMustResolveLocallyCommittedSingleShard() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        shard.atomicDirect = true;
        shard.failAfterLocalCommit.set(true);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{3});
            MvccException failure = assertThrows(MvccException.class, () -> manager.commit(tx));
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
            assertThrows(MvccException.class, metadata::assertNoPendingPublications);
            manager.rollback(tx);
            Assertions.assertEquals(TxStatus.COMMITTED, tx.status());
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
            Assertions.assertEquals(0, shard.aborts.get());
            assertDoesNotThrow(metadata::assertNoPendingPublications);
            Transaction reader = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertArrayEquals(new byte[]{3}, manager.get(reader, "s1", "k").orElseThrow());
            manager.commit(reader);
        }
    }

    /**
     * The same txId cannot bypass manager ownership validate in a different transaction domain.
     */
    @Test
    void rollbackMustRejectTransactionFromAnotherManager() {
        var firstMetadata = new InMemoryTransactionMetadataStore();
        var secondMetadata = new InMemoryTransactionMetadataStore();
        try (
                var first = new TransactionManager(new AtomicTimestampOracle(100), firstMetadata, Map.of());
                var second = new TransactionManager(new AtomicTimestampOracle(100), secondMetadata, Map.of())) {
            Transaction a = first.begin(IsolationLevel.REPEATABLE_READ);
            Transaction b = second.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals(a.txId(), b.txId());
            MvccException error = assertThrows(MvccException.class, () -> second.rollback(a));
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, error.errorCode());
            Assertions.assertEquals(TxStatus.ACTIVE, secondMetadata.get(b.txId()).orElseThrow().status());
            assertTrue(secondMetadata.snapshotExpiry(b.txId()).isPresent());
        }
    }

    /**
     * The commit missing durable intent participant collection is not allowed after the eager prewrite failed.
     */
    @Test
    void failedEagerPrewriteMustInvalidateTransaction() {
        var metadata = new InMemoryTransactionMetadataStore();
        var first = new MemoryParticipant("a");
        var second = new MemoryParticipant("b");
        second.failPrewrite.set(true);
        try (
                var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata,
                        TransactionOptions.failFastDefaults(), Map.of("a", first, "b", second))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "a", "k", new byte[]{1});
            assertThrows(MvccException.class, () -> manager.put(tx, "b", "k", new byte[]{2}));
            assertTrue(tx.rollbackOnly());
            MvccException error = assertThrows(MvccException.class, () -> manager.commit(tx));
            Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED, error.errorCode());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(tx.txId()).orElseThrow().status());
            manager.rollback(tx);
            Assertions.assertEquals(TxStatus.ABORTED, metadata.get(tx.txId()).orElseThrow().status());
        }
    }

    /**
     * Returns a group of pages cannot be an entry for changes to transaction writing buffer.
     */
    @Test
    void pageValuesMustNotAliasBufferedWrites() {
        try (
                var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                        Map.of("s1", new MemoryParticipant("s1")))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            var snapshot = manager.beginReadStatement(tx);
            var page = manager.scanPage(tx, "s1", "a", "z", snapshot, null, 1);
            page.values().getFirst().value()[0] = 9;
            assertArrayEquals(new byte[]{1}, manager.get(tx, "s1", "k", snapshot).orElseThrow());
            manager.rollback(tx);
        }
    }

    /**
     * The issuing barrier exit failure shall not leave a ghost transaction or an additional release permit for occupation.
     */
    @Test
    void failedBeginPublicationMustReleaseExactlyOnePermit() {
        var delegate = new InMemoryTransactionMetadataStore();
        var failExit = new AtomicBoolean(true);
        TransactionMetadataStore metadata = (TransactionMetadataStore) java.lang.reflect.Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(), new Class<?>[]{TransactionMetadataStore.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(delegate, args);
                        if (method.getName().equals("withPublicationLock") && failExit.getAndSet(false)) {
                            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected publication exit failure", false, null);
                        }
                        return result;
                    } catch (final java.lang.reflect.InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
        var limits = new TransactionLimits(60_000, 10, 1024, 1, 1, 1, 1, 1000);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(), true, true, false, limits);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, options, Map.of())) {
            assertThrows(MvccException.class, () -> manager.begin(IsolationLevel.REPEATABLE_READ));
            Assertions.assertEquals(0, manager.runtimeStats().activeTransactions());
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            MvccException error = assertThrows(MvccException.class, () -> manager.begin(IsolationLevel.REPEATABLE_READ));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, error.errorCode());
            manager.rollback(tx);
        }
    }

    /**
     * Non- atomic single shard adaptor must reject commit before the decision release when commit intent resolution is missing.
     */
    @Test
    void nonAtomicSingleShardMustRequireIntentResolution() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        shard.resolutionEnabled = false;
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            MvccException error = assertThrows(MvccException.class, () -> manager.commit(tx));
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, error.errorCode());
            Assertions.assertNotEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
            assertTrue(shard.intents.isEmpty());
            manager.rollback(tx);
        }
    }

    /**
     * Participant cannot turn unknown local commit into centre rollback when terminal state evidence cannot be provided.
     */
    @Test
    void unknownLocalFencingMustNotPublishAbort() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        shard.atomicDirect = true;
        shard.failAfterLocalCommit.set(true);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            assertThrows(MvccException.class, () -> manager.commit(tx));
            shard.hideLocalDecision = true;
            MvccException error = assertThrows(MvccException.class, () -> manager.rollback(tx));
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
            Assertions.assertEquals(TxStatus.PREPARING, metadata.get(tx.txId()).orElseThrow().status());
            shard.hideLocalDecision = false;
            for (TxStatus status : List.of(TxStatus.ACTIVE, TxStatus.PREPARING)) {
                shard.reportedLocalStatus = status;
                assertThrows(MvccException.class, () -> manager.rollback(tx));
                Assertions.assertEquals(TxStatus.PREPARING, metadata.get(tx.txId()).orElseThrow().status());
            }
            shard.reportedLocalStatus = null;
            manager.rollback(tx);
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
        }
    }

    /**
     * Small pages keep cursor forward while you add, overwrite and remove them.
     */
    @Test
    void bufferedPaginationMustMergeDeletesAndNewKeys() {
        try (
                var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                        Map.of("s1", new MemoryParticipant("s1")))) {
            Transaction seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (String key : List.of("a", "b", "c")) {
                manager.put(seed, "s1", key, new byte[]{1});
            }
            manager.commit(seed);
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.delete(tx, "s1", "a");
            manager.put(tx, "s1", "b", new byte[]{2});
            manager.put(tx, "s1", "d", new byte[]{4});
            manager.delete(tx, "s1", "e");
            manager.put(tx, "s1", "z", new byte[]{9});
            var snapshot = manager.beginReadStatement(tx);
            var keys = new ArrayList<String>();
            String after = null;
            int pages = 0;
            do {
                var page = manager.scanPage(tx, "s1", "a", "m", snapshot, after, 1);
                page.values().forEach(value -> keys.add(value.key()));
                after = page.nextKey();
                assertTrue(++pages < 10, "cursor must make progress through empty pages");
            } while (after != null);
            Assertions.assertEquals(List.of("b", "c", "d"), keys);
            assertArrayEquals(new byte[]{2}, manager.get(tx, "s1", "b", snapshot).orElseThrow());
            manager.rollback(tx);
        }
    }

    /**
     * Commit is a conflict-free double shard transaction.
     *
     * @param manager manager
     * @param suffix suffix
     */
    private static void writePair(final TransactionManager manager, final String suffix) {
        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "a", "a" + suffix, new byte[]{1});
        manager.put(tx, "b", "b" + suffix, new byte[]{2});
        manager.commit(tx);
    }

    /**
     * Bounded Waiting to determine the wrong point of control for sexual intercourse.
     *
     * @param latch latch
     * @throws AssertionError if the operation cannot complete
     */
    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timeout");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }

    /**
     * Simulate the durable storage protocol with a real version and intent set, and the failure switch only controls materialization timing.
     */
    private static final class MemoryParticipant implements StorageAdapter {

        private final String shardId;

        private final Map<Long, List<Mutation>> intents = new ConcurrentHashMap<>();

        private final Map<String, Map<Long, VersionedValue>> versions = new ConcurrentHashMap<>();

        private final AtomicInteger aborts = new AtomicInteger();

        private final AtomicBoolean failCommit = new AtomicBoolean();

        private final AtomicBoolean failPrewrite = new AtomicBoolean();

        private final AtomicBoolean failAfterLocalCommit = new AtomicBoolean();

        private final Map<Long, ParticipantRecord> localRecords = new ConcurrentHashMap<>();

        private boolean atomicDirect;

        private boolean resolutionEnabled = true;

        private boolean hideLocalDecision;

        private TxStatus reportedLocalStatus;

        private final AtomicBoolean blockNext = new AtomicBoolean();

        private final CountDownLatch entered = new CountDownLatch(1);

        private final CountDownLatch proceed = new CountDownLatch(1);

        private TransactionMetadataStore metadata;

        /**
         * Creates a simulator participant for specified shard.
         *
         * @param shardId logical shard identifier
         */
        private MemoryParticipant(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Returns shard number.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * Memory storage does not require a form.
         */
        @Override
        public void initialize() {
        }

        /**
         * Tie authority to read commit decision.
         *
         * @param store store
         */
        @Override
        public void configureMetadataStore(final TransactionMetadataStore store) {
            metadata = store;
        }

        /**
         * Logic commit intent readability only after binding metadata.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return metadata != null && resolutionEnabled;
        }

        /**
         * Enable local atomic commit by testing scene.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsAtomicDirectCommit() {
            return atomicDirect;
        }

        /**
         * Returns real durable storage local terminal state.
         *
         * @param txId transaction identifier
         * @return the operation result
         */
        @Override
        public Optional<ParticipantRecord> participantRecord(final long txId) {
            if (hideLocalDecision) {
                return Optional.empty();
            }
            return reportedLocalStatus == null ? Optional.ofNullable(localRecords.get(txId))
                    : Optional.of(new ParticipantRecord(txId, txId, reportedLocalStatus, null));
        }

        /**
         * Read the latest version or logic commit intent according to fixed timestamp.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            VersionedValue result = versions.getOrDefault(key, Map.of()).values().stream()
                    .filter(value -> value.versionTs() <= context.readTs())
                    .max(java.util.Comparator.comparingLong(VersionedValue::versionTs)).orElse(null);
            for (var entry : intents.entrySet()) {
                var record = metadata.get(entry.getKey());
                long timestamp = entry.getKey() == context.txId() ? context.readTs()
                        : record.filter(value -> value.status() == TxStatus.COMMITTED).map(value -> value.commitTs()).orElse(Long.MAX_VALUE);
                if (timestamp > context.readTs()) {
                    continue;
                }
                for (Mutation mutation : entry.getValue()) {
                    if (mutation.key().equals(key) && (result == null || timestamp >= result.versionTs())) {
                        result = new VersionedValue(key, mutation.value(), mutation.type() == MutationType.DELETE,
                                timestamp, entry.getKey() == context.txId());
                    }
                }
            }
            return Optional.ofNullable(result);
        }

        /**
         * Scan snapshot version match range.
         *
         * @param context context
         * @param range range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange range) {
            var keys = new TreeMap<String, Boolean>();
            versions.keySet().forEach(key -> keys.put(key, true));
            intents.values().forEach(values -> values.forEach(value -> keys.put(value.key(), true)));
            var result = new ArrayList<VersionedValue>();
            keys.subMap(range.startKeyInclusive(), true, range.endKeyExclusive(), false).keySet().forEach(key -> get(context, key).filter(value -> !value.deleted()).ifPresent(result::add));
            return result;
        }

        /**
         * Saves the transaction intent collection.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            if (failPrewrite.getAndSet(false)) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected prewrite failure", false, txId);
            }
            intents.put(txId, List.copyOf(mutations));
            localRecords.putIfAbsent(txId, new ParticipantRecord(txId, startTs, TxStatus.ACTIVE, null));
        }

        /**
         * Memory intent is ready to be completed when saved.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
        }

        /**
         * Press the same timestamp idempotent materialization intent.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            if (failCommit.getAndSet(false)) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected materialization failure", true, txId);
            }
            if (blockNext.getAndSet(false)) {
                entered.countDown();
                await(proceed);
            }
            for (Mutation mutation : intents.getOrDefault(txId, List.of())) {
                versions.computeIfAbsent(mutation.key(), ignored -> new ConcurrentHashMap<>()).put(commitTs,
                        new VersionedValue(mutation.key(), mutation.value(), mutation.type() == MutationType.DELETE, commitTs, false));
            }
            intents.remove(txId);
            localRecords.put(txId, new ParticipantRecord(txId, txId, TxStatus.COMMITTED, commitTs));
        }

        /**
         * Simulates version and local decision atomic commit, re-injecting the response loss.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            prewrite(txId, startTs, mutations);
            commit(txId, commitTs);
            if (failAfterLocalCommit.getAndSet(false)) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected local commit response loss", false, txId);
            }
        }

        /**
         * Clears rollback transaction and records the actual number of calls.
         *
         * @param txId transaction identifier
         */
        @Override
        public void abort(final long txId) {
            ParticipantRecord record = localRecords.get(txId);
            if (record != null && record.status() == TxStatus.COMMITTED) {
                return;
            }
            aborts.incrementAndGet();
            intents.remove(txId);
            localRecords.put(txId, new ParticipantRecord(txId, txId, TxStatus.ABORTED, null));
        }

        /**
         * Returns transaction which still has intent.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return Set.copyOf(intents.keySet());
        }

        /**
         * This test storage does not execute version collection.
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

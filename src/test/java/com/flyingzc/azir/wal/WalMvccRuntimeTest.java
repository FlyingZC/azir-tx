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

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verify, separate mode snapshot, atomic, conflicts, resource boundaries and materialization collection staggered.
 */
class WalMvccRuntimeTest {

    @TempDir
    Path directory;

    /**
     * Distinguishing RC from RR, with verify read-your-own-writes and non- commit data not visible.
     */
    @Test
    void rcRrAndOwnWritesHaveDistinctVisibility() {
        try (var runtime = memory()) {
            write(runtime, "a", "k", "one");
            var rr = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var rc = runtime.begin(IsolationLevel.READ_COMMITTED);
            var writer = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(writer, "a", "k", bytes("two"));
            Assertions.assertEquals("two", text(runtime.get(writer, "a", "k").orElseThrow()));
            Assertions.assertEquals("one", text(runtime.get(rc, "a", "k").orElseThrow()));
            runtime.commit(writer);
            Assertions.assertEquals("one", text(runtime.get(rr, "a", "k").orElseThrow()));
            Assertions.assertEquals("two", text(runtime.get(rc, "a", "k").orElseThrow()));
            runtime.rollback(rr);
            runtime.rollback(rc);
        }
    }

    /**
     * Verify The same RC statement shares cross-shard tangents, and part of the new value cannot be seen when the brush is not published.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void publicationIsAtomicAcrossShardsAndStatements() throws Exception {
        var armed = new AtomicBoolean();
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE && armed.compareAndSet(true, false)) {
                forced.countDown();
                await(release);
            }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "k", bytes("0"));
            runtime.put(seed, "b", "k", bytes("0"));
            runtime.commit(seed);
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
            var snapshot = runtime.beginReadStatement(reader);
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("1"));
            runtime.put(tx, "b", "k", bytes("1"));
            armed.set(true);
            var commit = tasks.submit(() -> runtime.commit(tx));
            try {
                assertTrue(forced.await(5, TimeUnit.SECONDS));
                assertTrue(runtime.findCommitted(tx.id()).isEmpty());
                Assertions.assertEquals("0", text(runtime.get(reader, "a", "k").orElseThrow()));
                Assertions.assertEquals("0", text(runtime.get(reader, "b", "k").orElseThrow()));
            } finally {
                release.countDown();
            }
            commit.get(5, TimeUnit.SECONDS);
            assertTrue(runtime.findCommitted(tx.id()).isPresent());
            Assertions.assertEquals("0", text(runtime.get(reader, "b", "k", snapshot).orElseThrow()));
            var fresh = runtime.beginReadStatement(reader);
            Assertions.assertEquals("1", text(runtime.get(reader, "a", "k", fresh).orElseThrow()));
            Assertions.assertEquals("1", text(runtime.get(reader, "b", "k", fresh).orElseThrow()));
            runtime.rollback(reader);
        } finally {
            release.countDown();
        }
    }

    /**
     * Concurrent Same key commit can only have one success, including access to the same brush batch.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentWritersCannotLoseUpdates() throws Exception {
        try (var runtime = memory(); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var left = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var right = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(left, "a", "k", bytes("left"));
            runtime.put(right, "a", "k", bytes("right"));
            var start = new CountDownLatch(1);
            var successes = new AtomicInteger();
            var conflicts = new AtomicInteger();
            var futures = List.of(left, right).stream().map(tx -> tasks.submit(() -> {
                await(start);
                try {
                    runtime.commit(tx);
                    successes.incrementAndGet();
                } catch (final WalMvccException ex) {
                    Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, ex.errorCode());
                    conflicts.incrementAndGet();
                }
            })).toList();
            start.countDown();
            for (var future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(1, successes.get());
            Assertions.assertEquals(1, conflicts.get());
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
        }
    }

    /**
     * Tombstone, reconstruction after deletion, scanned sequences and array defence copies are consistent.
     */
    @Test
    void tombstonesScansAndByteOwnershipAreCorrect() {
        try (var runtime = memory()) {
            byte[] value = bytes("old");
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "a", value);
            value[0] = 'X';
            runtime.put(seed, "a", "z", bytes("last"));
            runtime.commit(seed);
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "a");
            assertFalse(runtime.scan(deletion, "a", null, null).containsKey("a"));
            runtime.commit(deletion);
            var now = runtime.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(runtime.get(now, "a", "a").isEmpty());
            Assertions.assertEquals(List.of("z"), new ArrayList<>(runtime.scan(now, "a", null, null).keySet()));
            byte[] returned = runtime.get(old, "a", "a").orElseThrow();
            returned[0] = 'Y';
            Assertions.assertEquals("old", text(runtime.get(old, "a", "a").orElseThrow()));
            write(runtime, "a", "a", "new");
            Assertions.assertEquals("new", text(runtime.get(now, "a", "a").orElseThrow()));
            Assertions.assertEquals("old", text(runtime.get(old, "a", "a").orElseThrow()));
            runtime.rollback(old);
            runtime.rollback(now);
        }
    }

    /**
     * Rollback terminal state cannot be delayed to commit and repeat commit returns the same durable receipt.
     */
    @Test
    void terminalStatesAndReadOnlyCommitAreStable() {
        try (var runtime = memory()) {
            var aborted = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(aborted, "a", "k", bytes("bad"));
            runtime.rollback(aborted);
            runtime.rollback(aborted);
            Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(WalMvccException.class,
                    () -> runtime.commit(aborted)).errorCode());
            var readOnly = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.commit(readOnly);
            Assertions.assertEquals(0, runtime.stats().groupForces());
            Assertions.assertEquals(60, runtime.stats().walBytes());
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("ok"));
            var receipt = runtime.commit(tx);
            Assertions.assertEquals(receipt, runtime.commit(tx));
            runtime.rollback(tx);
            Assertions.assertEquals(receipt, runtime.findCommitted(tx.id()).orElseThrow());
            Assertions.assertEquals(0, runtime.stats().bufferedBytes());
        }
    }

    /**
     * Backend pause and materialization confirm loss with correct readout, recovery allows safe phase-out.
     */
    @Test
    void partialMaterializationAndLostAcknowledgementAreSafe() {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        b.failApply = true;
        a.loseApplyResponse = true;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("7"));
            runtime.put(tx, "b", "k", bytes("7"));
            runtime.commit(tx);
            assertThrows(WalMvccException.class, runtime::flush);
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("7", text(runtime.get(reader, "a", "k").orElseThrow()));
            Assertions.assertEquals("7", text(runtime.get(reader, "b", "k").orElseThrow()));
            b.failApply = false;
            runtime.flush();
            Assertions.assertEquals(1, a.checkpoint().sequence());
            Assertions.assertEquals(1, b.checkpoint().sequence());
            Assertions.assertEquals("7", text(runtime.get(reader, "b", "k").orElseThrow()));
            runtime.rollback(reader);
        }
    }

    /**
     * Long RR can still read old values after brushing and collection; only the history before the anchor is removed after completion.
     */
    @Test
    void oldSnapshotSurvivesEvictionAndGarbageCollection() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "10");
            runtime.flush();
            var rr = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "20");
            write(runtime, "a", "k", "30");
            runtime.flush();
            runtime.collectGarbage(100);
            Assertions.assertEquals("10", text(runtime.get(rr, "a", "k").orElseThrow()));
            Assertions.assertEquals(1, runtime.stats().safePoint());
            runtime.put(rr, "a", "k", bytes("stale"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class,
                    () -> runtime.commit(rr)).errorCode());
            Assertions.assertEquals(2, runtime.collectGarbage(100));
            Assertions.assertEquals(1, store.versions.get("k").size());
        }
    }

    /**
     * Check the duration again after the storage reading has been completed to avoid long time-consuming queries returning the extension snapshot.
     */
    @Test
    void expiryAfterBackendReadIsRejected() {
        var store = new TestMaterializedStore();
        var clock = new java.util.concurrent.atomic.AtomicLong();
        try (
                var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store),
                        WalOptions.defaults(), stage -> {
                        }, WalLog.NORMAL_IO, clock::get)) {
            write(runtime, "a", "k", "v");
            runtime.flush();
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "new");
            runtime.flush();
            store.onRead = () -> clock.addAndGet(TimeUnit.SECONDS.toNanos(61));
            Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT, assertThrows(WalMvccException.class,
                    () -> runtime.get(tx, "a", "k")).errorCode());
        }
    }

    /**
     * Error transaction ownership and cross transaction statement snapshot cannot be bypassed.
     */
    @Test
    void handlesAndSnapshotsCannotCrossOwners() {
        try (var first = memory(); var second = WalMvccRuntime.open(directory.resolve("other"), Set.of("a"), WalOptions.defaults())) {
            var tx = first.begin(IsolationLevel.READ_COMMITTED);
            var other = first.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(WalMvccException.class,
                    () -> second.commit(tx)).errorCode());
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(WalMvccException.class,
                    () -> first.get(other, "a", "k", first.beginReadStatement(tx))).errorCode());
        }
    }

    /**
     * The disk level is depleted only to reject the current commit, without disrupting the previous transaction and running time.
     */
    @Test
    void diskBudgetRejectsBeforeAppendAndLeavesRuntimeUsable() {
        var defaults = WalOptions.defaults();
        var limits = new WalOptions(100, 10, 1000, 10, 1, 0, 1024,
                10_000, 5000, 10, defaults.flushIntervalMillis());
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), limits)) {
            var first = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(first, "a", "k", new byte[512]);
            runtime.commit(first);
            var rejected = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(rejected, "a", "k", new byte[512]);
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                    () -> runtime.commit(rejected)).errorCode());
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals(512, runtime.get(reader, "a", "k").orElseThrow().length);
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
            Assertions.assertEquals(0, runtime.stats().bufferedBytes());
        }
    }

    /**
     * When the commit queue is full, new requests are denied and transaction already exists in the queue.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void boundedQueueRejectsOverflowWithoutLosingAcceptedRequests() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var once = new AtomicBoolean(true);
        var o = WalOptions.defaults();
        var limits = new WalOptions(100, 10, 1024, 1, 1, 0, o.maxWalBytes(), o.maxMemoryBytes(), 5000, 10, 20);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), limits, stage -> {
            if (stage == WalMvccRuntime.Stage.BEFORE_APPEND && once.compareAndSet(true, false)) {
                entered.countDown();
                await(release);
            }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(first, "a", "1", bytes("x"));
            var one = tasks.submit(() -> runtime.commit(first));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var next = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(next, "a", "2", bytes("x"));
            var two = tasks.submit(() -> runtime.commit(next));
            try {
                waitFor(() -> runtime.stats().queuedCommits() == 1);
                var overflow = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.put(overflow, "a", "3", bytes("x"));
                Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                        () -> runtime.commit(overflow)).errorCode());
            } finally {
                release.countDown();
            }
            one.get(5, TimeUnit.SECONDS);
            two.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(2, runtime.stats().publishedSequence());
        } finally {
            release.countDown();
        }
    }

    /**
     * RC multiple pages read shared statement snapshot, during which the result of inserting and deleting the follow-up page does not change.
     */
    @Test
    void pagesShareOneSnapshotAcrossConcurrentInsertAndDelete() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            for (String key : List.of("a", "b", "中", "😀")) {
                write(runtime, "a", key, key);
            }
            runtime.flush();
            var tx = runtime.begin(IsolationLevel.READ_COMMITTED);
            var snapshot = runtime.beginReadStatement(tx);
            var first = runtime.scanPage(tx, "a", null, null, null, 1, snapshot);
            Assertions.assertEquals(List.of("a"), new ArrayList<>(first.rows().keySet()));
            var change = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(change, "a", "b");
            runtime.put(change, "a", "aa", bytes("new"));
            runtime.commit(change);
            runtime.flush();
            var seen = new ArrayList<>(first.rows().keySet());
            String cursor = first.nextAfterKey();
            while (cursor != null) {
                var page = runtime.scanPage(tx, "a", null, null, cursor, 1, snapshot);
                seen.addAll(page.rows().keySet());
                cursor = page.nextAfterKey();
            }
            Assertions.assertEquals(List.of("a", "b", "中", "😀"), seen);
            Assertions.assertEquals(List.of("a", "aa", "中", "😀"), new ArrayList<>(runtime.scan(tx, "a", null, null).keySet()));
        }
    }

    /**
     * Saves a new key index memory during the brush, and the other transaction cannot squeeze this part of your capacity.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void memoryBudgetIncludesPendingIndexReservations() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var o = WalOptions.defaults();
        var limits = new WalOptions(10, 10, 256, 10, 1, 0, o.maxWalBytes(), 300, 5000, 10, 20);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), limits, stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) {
                forced.countDown();
                await(release);
            }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", new byte[]{1});
            var commit = tasks.submit(() -> runtime.commit(tx));
            try {
                assertTrue(forced.await(5, TimeUnit.SECONDS));
                var other = runtime.begin(IsolationLevel.REPEATABLE_READ);
                Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                        () -> runtime.put(other, "a", "j", new byte[]{1})).errorCode());
                runtime.rollback(other);
            } finally {
                release.countDown();
            }
            commit.get(5, TimeUnit.SECONDS);
            assertTrue(runtime.stats().retainedBytes() <= 300);
            Assertions.assertEquals(0, runtime.stats().bufferedBytes());
        } finally {
            release.countDown();
        }
    }

    /**
     * The commit waiting to be interrupted only returns the unknown result and cannot cancel a brushed transaction or convert it to rollback.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void interruptedCommitWaitCanBeResolvedByTransactionId() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) {
                forced.countDown();
                await(release);
            }
        })) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("v"));
            var failure = new java.util.concurrent.atomic.AtomicReference<WalMvccException>();
            Thread waiter = Thread.ofVirtual().start(() -> {
                try {
                    runtime.commit(tx);
                } catch (final WalMvccException ex) {
                    failure.set(ex);
                }
            });
            try {
                assertTrue(forced.await(5, TimeUnit.SECONDS));
                waiter.interrupt();
                waiter.join(5000);
                assertNotNull(failure.get());
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.get().errorCode());
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                        () -> runtime.rollback(tx)).errorCode());
            } finally {
                release.countDown();
            }
            waitFor(() -> runtime.stats().publishedSequence() == 1);
            Assertions.assertEquals(runtime.findCommitted(tx.id()).orElseThrow(), runtime.commit(tx));
        } finally {
            release.countDown();
        }
    }

    /**
     * Check snapshot results with an independent multi- version reference model for the mixed history of fixed random seeds.
     */
    @Test
    void randomizedHistoriesMatchReferenceModel() {
        var store = new TestMaterializedStore();
        var random = new Random(20261010);
        var model = new TreeMap<String, TreeMap<Long, String>>();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            var readers = new ArrayList<WalTransaction>();
            for (int step = 0; step < 200; step++) {
                if (step % 20 == 0) {
                    readers.add(runtime.begin(IsolationLevel.REPEATABLE_READ));
                }
                String key = "k" + random.nextInt(8);
                String value = random.nextBoolean() ? "v" + step : null;
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                if (value == null) {
                    runtime.delete(tx, "a", key);
                } else {
                    runtime.put(tx, "a", key, bytes(value));
                }
                long sequence = runtime.commit(tx).commitSequence();
                model.computeIfAbsent(key, ignored -> new TreeMap<>()).put(sequence, value);
                runtime.flush();
                runtime.collectGarbage(32);
                for (WalTransaction reader : readers) {
                    var actual = runtime.scan(reader, "a", null, null);
                    var expected = new TreeMap<String, String>();
                    model.forEach((k, chain) -> {
                        var version = chain.floorEntry(reader.startSequence());
                        if (version != null && version.getValue() != null) {
                            expected.put(k, version.getValue());
                        }
                    });
                    Assertions.assertEquals(expected.keySet(), actual.keySet());
                    expected.forEach((k, v) -> Assertions.assertEquals(v, text(actual.get(k))));
                }
            }
            readers.forEach(runtime::rollback);
        }
    }

    /**
     * Concurrent cross-shard transfers remain the same as statement-level readings.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentTransfersNeverExposeFracturedReads() throws Exception {
        try (var runtime = memory(); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "balance", bytes("500"));
            runtime.put(seed, "b", "balance", bytes("500"));
            runtime.commit(seed);
            var done = new AtomicBoolean();
            var writer = tasks.submit(() -> {
                try {
                    for (int i = 0; i < 200; i++) {
                        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                        runtime.put(tx, "a", "balance", bytes(Integer.toString(499 - i)));
                        runtime.put(tx, "b", "balance", bytes(Integer.toString(501 + i)));
                        runtime.commit(tx);
                    }
                } finally {
                    done.set(true);
                }
            });
            var reader = tasks.submit(() -> {
                int reads = 0;
                do {
                    var tx = runtime.begin(IsolationLevel.READ_COMMITTED);
                    var snapshot = runtime.beginReadStatement(tx);
                    int a = Integer.parseInt(text(runtime.get(tx, "a", "balance", snapshot).orElseThrow()));
                    int b = Integer.parseInt(text(runtime.get(tx, "b", "balance", snapshot).orElseThrow()));
                    Assertions.assertEquals(1000, a + b);
                    runtime.rollback(tx);
                    reads++;
                } while (!done.get());
                return reads;
            });
            writer.get(10, TimeUnit.SECONDS);
            assertTrue(reader.get(10, TimeUnit.SECONDS) > 0);
        }
    }

    /**
     * Creates a back-end transaction kernel.
     *
     * @return the operation result
     */
    private WalMvccRuntime memory() {
        return WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults());
    }

    /**
     * Write and commit single key transaction.
     *
     * @param runtime runtime
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     * @return the operation result
     */
    static WalReceipt write(final WalMvccRuntime runtime, final String shard, final String key, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, shard, key, bytes(value));
        return runtime.commit(tx);
    }

    /**
     * Encoding Test Values.
     *
     * @param value value to write
     * @return the operation result
     */
    static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decoding test value.
     *
     * @param value value to write
     * @return the operation result
     */
    static String text(final byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * Bounded Waits for certainty concurrent barrier.
     *
     * @param latch latch
     * @throws AssertionError if the operation cannot complete
     */
    static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("barrier timeout");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }

    /**
     * Bounded Waits for background status, and does not use open-ended questionnaires.
     *
     * @param condition condition
     * @throws Exception if the operation cannot complete
     */
    static void waitFor(final java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition timeout");
            }
            Thread.sleep(5);
        }
    }
}

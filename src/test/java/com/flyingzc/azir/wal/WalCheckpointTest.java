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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Verify Checkpoint Switches, Old snapshot, receipt Reservations and Memory collection, the old log cannot be deleted before the full new generation.
 */
class WalCheckpointTest {

    @TempDir
    Path directory;

    /**
     * The pure local old RR requires history to be preserved with mirrors, and the receipt and global serial numbers remain continuous after cut.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void localCheckpointRetainsOldSnapshotAndArchivedReceipts() throws Exception {
        WalReceipt old;
        WalReceipt current;
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            old = pair(runtime, "old");
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            current = pair(runtime, "new");
            runtime.collectGarbage(32);
            var result = runtime.checkpoint();
            assertTrue(result.created());
            Assertions.assertEquals(2, result.sequence());
            Assertions.assertEquals(100, result.walBytesAfter());
            assertFalse(Files.exists(directory.resolve("transactions.wal")));
            Assertions.assertEquals(0, runtime.stats().pendingReceipts());
            Assertions.assertEquals(old, runtime.findCommitted(old.transactionId()).orElseThrow());
            assertPair(runtime, reader, "old");
            runtime.rollback(reader);
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(2, reopened.stats().publishedSequence());
            Assertions.assertEquals(2, reopened.stats().checkpointSequence());
            Assertions.assertEquals(current, reopened.findCommitted(current.transactionId()).orElseThrow());
            var reader = reopened.begin(IsolationLevel.REPEATABLE_READ);
            assertPair(reopened, reader, "new");
            reopened.rollback(reader);
            Assertions.assertEquals(3, pair(reopened, "after").commitSequence());
            assertTrue(reopened.checkpoint().created());
            Assertions.assertEquals(old, reopened.findCommitted(old.transactionId()).orElseThrow());
        }
    }

    /**
     * Two pieces of the old RR can be read from materialization history and cut off not to erase the conflict with the old writer fence.
     */
    @Test
    void materializedCheckpointPreservesSnapshotsAndWriteConflicts() {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        UUID oldId;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            oldId = pair(runtime, "old").transactionId();
            runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            pair(runtime, "new");
            runtime.flush();
            assertTrue(runtime.checkpoint().created());
            runtime.collectGarbage(32);
            assertPair(runtime, old, "old");
            runtime.put(old, "a", "k", bytes("stale"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
        }
        try (var reopened = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, reopened.lookupCommit(oldId).status());
            var reader = reopened.begin(IsolationLevel.READ_COMMITTED);
            assertPair(reopened, reader, "new");
            reopened.rollback(reader);
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory,
                Map.of("a", new TestMaterializedStore(), "b", new TestMaterializedStore()), WalOptions.defaults()));
    }

    /**
     * You cannot push the check point when you're behind, and recovery can still commit after and create a whole new generation.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void laggingBackendCannotAuthorizeLogDeletion() throws Exception {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        b.failApply = true;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            pair(runtime, "one");
            assertThrows(WalMvccException.class, runtime::checkpoint);
            assertFalse(Files.exists(directory.resolve("CURRENT")));
            Assertions.assertEquals(2, pair(runtime, "two").commitSequence());
            b.failApply = false;
            runtime.flush();
            assertTrue(runtime.checkpoint().created());
        }
    }

    /**
     * Short save window only for testing; collection missing result is clearly unknown or expired and cannot be considered rollback.
     */
    @Test
    void receiptExpiryDoesNotBecomeAnAbortDecision() {
        var time = new AtomicLong();
        var policy = new WalRetentionOptions(100, 3_600_000, 4_000_000, 1_000_000, 1000);
        UUID first;
        UUID second;
        try (var runtime = timed(policy, time)) {
            first = pair(runtime, "first").transactionId();
            runtime.checkpoint();
            time.set(50);
            second = pair(runtime, "second").transactionId();
            runtime.checkpoint();
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(first).status());
            time.set(101);
            runtime.checkpoint();
            Assertions.assertEquals(1, runtime.stats().retiredReceiptThrough());
            Assertions.assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, runtime.lookupCommit(first).status());
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(second).status());
            Assertions.assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, runtime.lookupCommit(UUID.randomUUID()).status());
            Assertions.assertEquals(1, runtime.stats().receiptArchives());
        }
        try (var reopened = timed(policy, time)) {
            Assertions.assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, reopened.lookupCommit(first).status());
            assertTrue(reopened.findCommitted(second).isPresent());
            Assertions.assertEquals(3, pair(reopened, "after").commitSequence());
        }
    }

    /**
     * The tombstone deletion that has not yet been done with materialization cannot result in an old post-restoration end value because of the collection index.
     */
    @Test
    void unmaterializedTombstoneCannotBeRetired() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            store.failApply = true;
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k");
            runtime.commit(deletion);
            runtime.collectGarbage(32);
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(runtime.get(reader, "a", "k").isEmpty());
            runtime.rollback(reader);
            assertTrue(runtime.stats().retainedBytes() > 0);
            store.failApply = false;
            runtime.flush();
            runtime.collectGarbage(32);
            Assertions.assertEquals(0, runtime.stats().retainedBytes());
            reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(runtime.get(reader, "a", "k").isEmpty());
            runtime.rollback(reader);
        }
    }

    /**
     * Fixed set of work 120 actual breaks, log index and version history without cumulative commit growth.
     */
    @Test
    void repeatedCheckpointsBoundActiveLogAndMemory() {
        var time = new AtomicLong();
        var policy = new WalRetentionOptions(10, 3_600_000, 4_000_000, 1_000_000, 1000);
        try (var runtime = timed(policy, time)) {
            for (int round = 0; round < 120; round++) {
                pair(runtime, "v" + round);
                runtime.collectGarbage(32);
                time.addAndGet(11);
                runtime.checkpoint();
                Assertions.assertEquals(100, runtime.stats().walBytes());
                Assertions.assertEquals(0, runtime.stats().pendingReceipts());
                Assertions.assertEquals(1, runtime.stats().receiptArchives());
                assertTrue(runtime.stats().retainedBytes() < 2048);
            }
            Assertions.assertEquals(120, runtime.stats().publishedSequence());
        }
        try (var reopened = timed(policy, time)) {
            var reader = reopened.begin(IsolationLevel.REPEATABLE_READ);
            assertPair(reopened, reader, "v119");
            reopened.rollback(reader);
            Assertions.assertEquals(121, pair(reopened, "next").commitSequence());
        }
    }

    /**
     * Checkpoint must wait until decision is over when the commit brush has not been released, and cannot be cut off in transit transaction.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void checkpointWaitsForWholeInFlightDecision() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) {
                forced.countDown();
                await(release);
            }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var committing = tasks.submit(() -> pair(runtime, "whole"));
            assertTrue(forced.await(30, TimeUnit.SECONDS));
            var checkpoint = tasks.submit(runtime::checkpoint);
            assertFalse(Files.exists(directory.resolve("CURRENT")));
            release.countDown();
            var receipt = committing.get(30, TimeUnit.SECONDS);
            assertTrue(checkpoint.get(30, TimeUnit.SECONDS).created());
            Assertions.assertEquals(receipt, runtime.findCommitted(receipt.transactionId()).orElseThrow());
        } finally {
            release.countDown();
        }
    }

    /**
     * The pointer or mirror damage must be rejected as recovery and cannot automatically retreat to an old generation that may have failed transaction.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptedPointerAndImageFailClosedWithoutCleanup() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            pair(runtime, "safe");
            runtime.checkpoint();
        }
        Path pointer = directory.resolve("CURRENT");
        byte[] original = Files.readAllBytes(pointer);
        byte[] damaged = original.clone();
        damaged[10] ^= 1;
        Files.write(pointer, damaged);
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults()));
        assertArrayEquals(damaged, Files.readAllBytes(pointer));
        Files.write(pointer, original);
        Path image;
        try (var children = Files.list(directory)) {
            image = children.filter(Files::isDirectory).findFirst().orElseThrow().resolve("snapshot.bin");
        }
        byte[] payload = Files.readAllBytes(image);
        payload[payload.length - 1] ^= 1;
        Files.write(image, payload);
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults()));
        assertArrayEquals(payload, Files.readAllBytes(image));
    }

    /**
     * Receipt Archive budget cannot delete confirmed results or stop normal reading if only new writing is pressed back when the receipt archive is insufficient.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void archiveBudgetBackpressureNeverDropsProof() throws Exception {
        var policy = new WalRetentionOptions(0, 3_600_000, 4_000_000, 4096, 2);
        WalReceipt first;
        WalReceipt second;
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults(), policy)) {
            first = pair(runtime, "one");
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class, runtime::checkpoint).errorCode());
            second = pair(runtime, "two");
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class, () -> pair(runtime, "three")).errorCode());
            assertFalse(Files.exists(directory.resolve("CURRENT")));
            Assertions.assertEquals(2, runtime.stats().publishedSequence());
            Assertions.assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow());
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertPair(runtime, reader, "two");
            runtime.rollback(reader);
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(first, reopened.findCommitted(first.transactionId()).orElseThrow());
            Assertions.assertEquals(second, reopened.findCommitted(second.transactionId()).orElseThrow());
            assertTrue(reopened.checkpoint().created());
        }
    }

    /**
     * Constructs a visible test retention window on an independent clock, and the production default is not affected by the test window.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unicodeAndZeroByteKeysSurviveCheckpointCodec() throws Exception {
        var keys = java.util.List.of("", "a\u0000b", "中文", "x\uE000", "x😀");
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < keys.size(); i++) {
                runtime.put(tx, "a", keys.get(i), bytes("v" + i));
            }
            runtime.commit(tx);
            runtime.checkpoint();
        }
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = restored.beginReadStatement(tx);
            for (int i = 0; i < keys.size(); i++) {
                Assertions.assertEquals("v" + i, text(restored.get(tx, "a", keys.get(i), snapshot).orElseThrow()));
            }
            var ordered = keys.stream().sorted(com.flyingzc.azir.storage.Utf8KeyOrder::compare).toList();
            Assertions.assertEquals(ordered, new java.util.ArrayList<>(restored.scan(tx, "a", null, null, snapshot).keySet()));
            restored.rollback(tx);
        }
    }

    /**
     * Constructs a visible test retention window on an independent clock, and the production default is not affected by the test window.
     *
     * @param policy policy
     * @param clock clock
     * @return the operation result
     */
    private WalMvccRuntime timed(final WalRetentionOptions policy, final AtomicLong clock) {
        return WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
        },
                WalLog.NORMAL_IO, System::nanoTime, policy, clock::get);
    }

    /**
     * Commit complete cross-shard write set and returns stable receipt.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt pair(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", "k", bytes(value));
        runtime.put(tx, "b", "k", bytes(value));
        return runtime.commit(tx);
    }

    /**
     * Check two pieces with the same phrase snapshot.
     *
     * @param runtime runtime
     * @param tx transaction context
     * @param value value to write
     */
    private static void assertPair(final WalMvccRuntime runtime, final WalTransaction tx, final String value) {
        var snapshot = runtime.beginReadStatement(tx);
        Assertions.assertEquals(value, text(runtime.get(tx, "a", "k", snapshot).orElseThrow()));
        Assertions.assertEquals(value, text(runtime.get(tx, "b", "k", snapshot).orElseThrow()));
    }
}

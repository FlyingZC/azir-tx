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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Proves that the cache capacity does not determine database size, phase-out, cold indexing, collection and recovery remain transaction snapshot compliant.
 */
class WalBoundedCacheTest {

    @TempDir
    Path directory;

    /**
     * 400 Alive Keys far above hotspot budget, checkpoint does not save full library header, reopening to use smaller memory budget.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void largeLiveSetCanBeCheckpointedAndReopenedWithTinyMemory() throws Exception {
        var stores = Map.<String, WalMaterializedStore>of("a", new TestMaterializedStore(), "b", new TestMaterializedStore());
        var limits = limits(16_384, 2048);
        try (var runtime = WalMvccRuntime.open(directory, stores, limits)) {
            for (int i = 0; i < 400; i++) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : stores.keySet()) {
                    runtime.put(tx, shard, "key:" + i, payload(i));
                }
                runtime.commit(tx);
                runtime.flush();
                assertTrue(runtime.stats().hotCacheBytes() <= 2048);
                assertTrue(runtime.stats().retainedBytes() <= 2048);
            }
            assertTrue(runtime.stats().cacheEvictions() > 700);
            assertTrue(runtime.checkpoint().created());
            try (var paths = Files.walk(directory)) {
                Path image = paths.filter(path -> path.getFileName().toString().equals("snapshot.bin")).findFirst().orElseThrow();
                assertTrue(Files.size(image) < 8192, "checkpoint must not contain the 400 KiB live set");
            }
        }
        try (var restored = WalMvccRuntime.open(directory, stores, limits(1024, 0))) {
            Assertions.assertEquals(0, restored.stats().retainedBytes());
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = restored.beginReadStatement(tx);
            for (int i = 0; i < 400; i++) {
                for (String shard : stores.keySet()) {
                    assertArrayEquals(payload(i), restored.get(tx, shard, "key:" + i, snapshot).orElseThrow());
                }
            }
            Assertions.assertEquals(400, restored.scan(tx, "a", null, null, snapshot).size());
            restored.rollback(tx);
            Assertions.assertEquals(0, restored.stats().retainedBytes());
        }
    }

    /**
     * After the cache has been emptied and the log stopped, the old RC/RR writer must still check for updates via durable index.
     */
    @Test
    void coldConflictFenceSurvivesEvictionAndCheckpoint() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(4096, 0))) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            var rr = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var rc = runtime.begin(IsolationLevel.READ_COMMITTED);
            write(runtime, "a", "k", "new");
            runtime.flush();
            runtime.checkpoint();
            runtime.collectGarbage(32);
            Assertions.assertEquals(0, runtime.stats().retainedBytes());
            Assertions.assertEquals("old", text(runtime.get(rr, "a", "k").orElseThrow()));
            Assertions.assertEquals("new", text(runtime.get(rc, "a", "k").orElseThrow()));
            for (var stale : new WalTransaction[]{rr, rc}) {
                runtime.put(stale, "a", "k", bytes("stale"));
                Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(stale)).errorCode());
            }
            assertTrue(runtime.stats().conflictStoreLookups() >= 4);
        }
    }

    /**
     * The old RR return value cannot contaminate the latest cache, leaving the old writer missing commit update.
     */
    @Test
    void historicalColdReadCannotInstallAStaleConflictFence() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 1024))) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "new");
            runtime.flush();
            for (int i = 0; i < 12; i++) {
                write(runtime, "a", "other" + i, "padding");
                runtime.flush();
            }
            Assertions.assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            runtime.put(old, "a", "k", bytes("wrong"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
        }
    }

    /**
     * Cold queries can raise hot spots, but LRU phase-out sequence and total budget is still valid.
     */
    @Test
    void coldReadsPromoteHotValuesWithoutExceedingBudget() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 1024))) {
            for (int i = 0; i < 10; i++) {
                write(runtime, "a", "key" + i, "value");
                runtime.flush();
            }
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
            long misses = runtime.stats().cacheMisses();
            runtime.get(reader, "a", "key0");
            Assertions.assertEquals(misses + 1, runtime.stats().cacheMisses());
            long hits = runtime.stats().cacheHits();
            runtime.get(reader, "a", "key0");
            Assertions.assertEquals(hits + 1, runtime.stats().cacheHits());
            for (int i = 0; i < 10; i++) {
                Assertions.assertEquals("value", text(runtime.get(reader, "a", "key" + i).orElseThrow()));
            }
            assertTrue(runtime.stats().hotCacheBytes() <= 1024);
            runtime.rollback(reader);
        }
    }

    /**
     * Unset value and deletion cannot be eliminated to satisfy cache budget, nor can old values be read back.
     */
    @Test
    void dirtyVersionsRemainAuthoritativeWhenBackendIsPaused() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(4096, 0))) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            store.failApply = true;
            write(runtime, "a", "k", "new");
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k");
            runtime.commit(deletion);
            runtime.collectGarbage(32);
            Assertions.assertEquals("new", text(runtime.get(reader, "a", "k").orElseThrow()));
            var fresh = runtime.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(runtime.get(fresh, "a", "k").isEmpty());
            runtime.rollback(reader);
            runtime.rollback(fresh);
            store.failApply = false;
            runtime.flush();
            runtime.collectGarbage(32);
            Assertions.assertEquals(0, runtime.stats().retainedBytes());
            assertTrue(store.versions.isEmpty());
        }
    }

    /**
     * Tombstone collection is executed only after the old snapshot has ended, and retirement key replay cannot be revived and can still be recreated.
     */
    @Test
    void retiredDeleteCannotResurrectOnWalReplay() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k");
            runtime.commit(deletion);
            runtime.flush();
            runtime.collectGarbage(32);
            Assertions.assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            runtime.put(old, "a", "k", bytes("wrong"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
            Assertions.assertEquals(2, runtime.collectGarbage(32));
            assertTrue(store.versions.isEmpty());
        }
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            var reader = restored.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(restored.get(reader, "a", "k").isEmpty());
            restored.rollback(reader);
            Assertions.assertEquals(0, restored.stats().retainedBytes());
            write(restored, "a", "k", "reborn");
            restored.flush();
            restored.checkpoint();
        }
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            var reader = restored.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals("reborn", text(restored.get(reader, "a", "k").orElseThrow()));
            restored.rollback(reader);
        }
    }

    /**
     * The slowest shard triggers a backlog back pressure, and the other shard homing cannot be bypassed; recovery can continue with commit.
     */
    @Test
    void slowestShardBackpressuresBeforeAppendAndCanRecover() {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        b.failApply = true;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), limits(8192, 0).withMaterializationLag(2))) {
            write(runtime, "a", "k", "one");
            write(runtime, "a", "k", "two");
            Assertions.assertEquals(2, runtime.stats().materializationLag());
            long before = runtime.stats().walBytes();
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class, () -> write(runtime, "a", "k", "three")).errorCode());
            Assertions.assertEquals(before, runtime.stats().walBytes());
            Assertions.assertEquals(2, runtime.stats().publishedSequence());
            b.failApply = false;
            runtime.flush();
            Assertions.assertEquals(0, runtime.stats().materializationLag());
            write(runtime, "a", "k", "after");
            Assertions.assertEquals(3, runtime.stats().publishedSequence());
        }
    }

    /**
     * The cold index is not available without Zero version commit; confirm that after unwritten log recovery service can continue working.
     */
    @Test
    void unavailableColdIndexRejectsWithoutFailingOpen() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            long before = runtime.stats().walBytes();
            store.onRead = () -> {
                throw new IllegalStateException("index unavailable");
            };
            Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, assertThrows(WalMvccException.class, () -> write(runtime, "a", "k", "wrong")).errorCode());
            Assertions.assertEquals(before, runtime.stats().walBytes());
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
            store.onRead = () -> {
            };
            write(runtime, "a", "k", "after");
            Assertions.assertEquals(2, runtime.stats().publishedSequence());
        }
    }

    /**
     * The hot point keys quoted in the decision route may not be cold-readed out, otherwise unencumbered index memory will be added during installation.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void evictionCannotInvalidateInFlightIndexReservations() throws Exception {
        var store = new TestMaterializedStore();
        var armed = new AtomicBoolean();
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store), limits(4096, 300), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE && armed.get()) {
                forced.countDown();
                await(release);
            }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            write(runtime, "a", "other", "old");
            runtime.flush();
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k");
            runtime.commit(deletion);
            runtime.flush();
            armed.set(true);
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("new"));
            var future = tasks.submit(() -> runtime.commit(tx));
            try {
                assertTrue(forced.await(10, TimeUnit.SECONDS));
                runtime.collectGarbage(32);
                assertFalse(store.versions.containsKey("k"), "backend tombstone is safe to retire, in-flight memory index is still reserved");
                var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
                long evictions = runtime.stats().cacheEvictions();
                for (int i = 0; i < 20; i++) {
                    Assertions.assertEquals("old", text(runtime.get(reader, "a", "other").orElseThrow()));
                }
                assertTrue(runtime.stats().cacheEvictions() > evictions, "cold promotions must actually exercise cache eviction");
                store.onRead = () -> {
                    throw new IllegalStateException("backend unavailable after GC");
                };
                try {
                    assertTrue(runtime.get(reader, "a", "k").isEmpty(), "protected tombstone must remain in memory");
                } finally {
                    store.onRead = () -> {
                    };
                }
                runtime.rollback(reader);
                assertTrue(runtime.stats().retainedBytes() + runtime.stats().bufferedBytes() <= 4096);
            } finally {
                release.countDown();
            }
            future.get(10, TimeUnit.SECONDS);
            assertTrue(runtime.stats().retainedBytes() <= 4096);
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals("new", text(runtime.get(reader, "a", "k").orElseThrow()));
            runtime.rollback(reader);
        } finally {
            release.countDown();
        }
    }

    /**
     * The single authority version is not allowed to be phased out as a hotspot without backend, and the syntax of the local mode remains unchanged.
     */
    @Test
    void localOnlyVersionsAreNeverTreatedAsDisposableCache() {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), limits(8192, 0))) {
            write(runtime, "a", "k", "local");
            runtime.flush();
            runtime.checkpoint();
            assertTrue(runtime.stats().retainedBytes() > 0);
            Assertions.assertEquals(0, runtime.stats().hotCacheBytes());
        }
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), limits(8192, 0))) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("local", text(restored.get(tx, "a", "k").orElseThrow()));
            restored.rollback(tx);
        }
    }

    /**
     * Hotspot budget is not allowed to cross the total budget, backlog budget must be positive, timeout modified cannot quietly reset the new configuration.
     */
    @Test
    void limitsAreValidatedAndPreserved() {
        assertThrows(IllegalArgumentException.class, () -> limits(1024, 1025));
        assertThrows(IllegalArgumentException.class, () -> limits(1024, -1));
        assertThrows(IllegalArgumentException.class, () -> limits(1024, 0).withMaterializationLag(0));
        var options = limits(8192, 1234).withMaterializationLag(7).withTimeout(12345);
        Assertions.assertEquals(1234, options.maxHotCacheBytes());
        Assertions.assertEquals(7, options.maxMaterializationLag());
    }

    /**
     * The full amount in the old format mirror is negligible at materialization, and low memory upgrades still get complete value from tied backend.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void legacyFullHeadImageCanBeOpenedWithBoundedCache() throws Exception {
        var store = new TestMaterializedStore();
        try (var log = new WalLog(directory, Set.of("a"), 8L * 1024 * 1024)) {
            store.initialize(log.engineId(), "a");
            var mutations = new java.util.ArrayList<WalMutation>();
            for (int i = 0; i < 100; i++) {
                mutations.add(new WalMutation("a", "key:" + i, payload(i)));
            }
            var commit = new WalCommit(java.util.UUID.randomUUID(), 1, 0, mutations);
            log.append(java.util.List.of(commit));
            store.apply(java.util.List.of(commit));
            var heads = mutations.stream().map(mutation -> new WalCheckpointState.Version(1, mutation)).toList();
            log.checkpoint(Set.of("a"), Set.of("a"), 0, heads, System.currentTimeMillis(), stage -> {
            });
        }
        writeLegacyImageFixture();
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", store), limits(1024, 0))) {
            Assertions.assertEquals(0, restored.stats().retainedBytes());
            var tx = restored.begin(IsolationLevel.READ_COMMITTED);
            for (int i = 0; i < 100; i++) {
                assertArrayEquals(payload(i), restored.get(tx, "a", "key:" + i).orElseThrow());
            }
            restored.rollback(tx);
            assertTrue(restored.checkpoint().created());
            var current = WalCheckpointFiles.read(directory);
            Assertions.assertEquals(2, java.nio.ByteBuffer.wrap(Files.readAllBytes(current.generation().resolve("snapshot.bin"))).getInt(4));
        }
    }

    /**
     * The unrun test directory constructs a single image and synchronizes the summary of the pointer and end log and CRC to form a real old format plugin.
     *
     * @throws Exception if the operation cannot complete
     */
    private void writeLegacyImageFixture() throws Exception {
        var current = WalCheckpointFiles.read(directory);
        Path image = current.generation().resolve("snapshot.bin");
        byte[] body = Files.readAllBytes(image);
        java.nio.ByteBuffer.wrap(body).putInt(4, 1);
        Files.write(image, body);
        byte[] digest = java.util.HexFormat.of().parseHex(WalCodec.digest(body));
        Path pointer = directory.resolve("CURRENT");
        byte[] bytes = Files.readAllBytes(pointer);
        java.nio.ByteBuffer.wrap(bytes).position(24).put(digest);
        java.nio.ByteBuffer.wrap(bytes).putInt(56, WalFiles.crc(bytes, 0, 56));
        Files.write(pointer, bytes);
        Path tail = current.generation().resolve("tail.wal");
        bytes = Files.readAllBytes(tail);
        Assertions.assertEquals(100, bytes.length);
        java.nio.ByteBuffer.wrap(bytes).position(64).put(digest);
        java.nio.ByteBuffer.wrap(bytes).putInt(96, WalFiles.crc(bytes, 0, 96));
        Files.write(tail, bytes);
        Assertions.assertEquals(100, WalCheckpointFiles.read(directory).state().versions().size());
    }

    /**
     * When the cold conflict check has run out of transaction deadlines, it is not possible to continue writing WAL using the pre-survey deadline judgement.
     */
    @Test
    void leaseIsRecheckedAfterColdIndexLookup() {
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var store = new TestMaterializedStore();
        try (
                var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store), limits(8192, 0).withTimeout(100),
                        stage -> {
                        }, WalLog.NORMAL_IO, clock::get)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("late"));
            store.onRead = () -> clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(101));
            Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT, assertThrows(WalMvccException.class, () -> runtime.commit(tx)).errorCode());
            Assertions.assertEquals(0, runtime.stats().publishedSequence());
            Assertions.assertEquals(60, runtime.stats().walBytes());
        }
    }

    /**
     * Provides budget smaller than a complete data set and closes the HF automatic check points to control the order of execution of tests.
     *
     * @param memory memory
     * @param cache cache
     * @return the operation result
     */
    private static WalOptions limits(final long memory, final long cache) {
        return new WalOptions(32, 32, 8192, 32, 8, 1, 8L * 1024 * 1024, memory, 60_000, 1024, 20).withHotCacheBytes(cache);
    }

    /**
     * Constructs a fixed payload that allows byte verify to avoid the existence of only check keys.
     *
     * @param ordinal ordinal
     * @return the operation result
     */
    private static byte[] payload(final int ordinal) {
        byte[] bytes = new byte[512];
        java.util.Arrays.fill(bytes, (byte) (ordinal % 251));
        java.nio.ByteBuffer.wrap(bytes).putInt(ordinal);
        return bytes;
    }
}

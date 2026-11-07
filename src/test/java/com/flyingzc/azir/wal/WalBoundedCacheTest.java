package com.flyingzc.azir.wal;

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
import static org.junit.jupiter.api.Assertions.*;

/** 证明缓存容量不决定数据库规模，淘汰、冷索引、回收和恢复仍遵守事务快照。 */
class WalBoundedCacheTest {
    @TempDir Path directory;

    /** 四百个存活键远超热点预算，检查点不保存全库头，重开可使用更小的内存预算。 */
    @Test void largeLiveSetCanBeCheckpointedAndReopenedWithTinyMemory() throws Exception {
        var stores = Map.<String, WalMaterializedStore>of("a", new TestMaterializedStore(), "b", new TestMaterializedStore());
        var limits = limits(16_384, 2048);
        try (var runtime = WalMvccRuntime.open(directory, stores, limits)) {
            for (int i = 0; i < 400; i++) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : stores.keySet()) { runtime.put(tx, shard, "key:" + i, payload(i)); }
                runtime.commit(tx); runtime.flush();
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
            assertEquals(0, restored.stats().retainedBytes());
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ); var snapshot = restored.beginReadStatement(tx);
            for (int i = 0; i < 400; i++) {
                for (String shard : stores.keySet()) { assertArrayEquals(payload(i), restored.get(tx, shard, "key:" + i, snapshot).orElseThrow()); }
            }
            assertEquals(400, restored.scan(tx, "a", null, null, snapshot).size());
            restored.rollback(tx); assertEquals(0, restored.stats().retainedBytes());
        }
    }

    /** 缓存清空并截断日志后，旧 RC/RR 写者仍必须通过持久索引检出更新。 */
    @Test void coldConflictFenceSurvivesEvictionAndCheckpoint() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(4096, 0))) {
            write(runtime, "a", "k", "old"); runtime.flush();
            var rr = runtime.begin(IsolationLevel.REPEATABLE_READ); var rc = runtime.begin(IsolationLevel.READ_COMMITTED);
            write(runtime, "a", "k", "new"); runtime.flush(); runtime.checkpoint(); runtime.collectGarbage(32);
            assertEquals(0, runtime.stats().retainedBytes());
            assertEquals("old", text(runtime.get(rr, "a", "k").orElseThrow()));
            assertEquals("new", text(runtime.get(rc, "a", "k").orElseThrow()));
            for (var stale : new WalTransaction[]{rr, rc}) {
                runtime.put(stale, "a", "k", bytes("stale"));
                assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(stale)).errorCode());
            }
            assertTrue(runtime.stats().conflictStoreLookups() >= 4);
        }
    }

    /** 旧 RR 的回源值不能污染最新缓存，让旧写者漏掉已提交更新。 */
    @Test void historicalColdReadCannotInstallAStaleConflictFence() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 1024))) {
            write(runtime, "a", "k", "old"); runtime.flush(); var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "new"); runtime.flush();
            for (int i = 0; i < 12; i++) { write(runtime, "a", "other" + i, "padding"); runtime.flush(); }
            assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            runtime.put(old, "a", "k", bytes("wrong"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
        }
    }

    /** 冷查询可提升热点，但 LRU 淘汰顺序和总预算仍有效。 */
    @Test void coldReadsPromoteHotValuesWithoutExceedingBudget() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 1024))) {
            for (int i = 0; i < 10; i++) { write(runtime, "a", "key" + i, "value"); runtime.flush(); }
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
            long misses = runtime.stats().cacheMisses(); runtime.get(reader, "a", "key0");
            assertEquals(misses + 1, runtime.stats().cacheMisses());
            long hits = runtime.stats().cacheHits(); runtime.get(reader, "a", "key0");
            assertEquals(hits + 1, runtime.stats().cacheHits());
            for (int i = 0; i < 10; i++) { assertEquals("value", text(runtime.get(reader, "a", "key" + i).orElseThrow())); }
            assertTrue(runtime.stats().hotCacheBytes() <= 1024); runtime.rollback(reader);
        }
    }

    /** 未落盘的值和删除不能为满足缓存预算被淘汰，也不能回源读出旧值。 */
    @Test void dirtyVersionsRemainAuthoritativeWhenBackendIsPaused() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(4096, 0))) {
            write(runtime, "a", "k", "old"); runtime.flush(); store.failApply = true;
            write(runtime, "a", "k", "new");
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k"); runtime.commit(deletion);
            runtime.collectGarbage(32);
            assertEquals("new", text(runtime.get(reader, "a", "k").orElseThrow()));
            var fresh = runtime.begin(IsolationLevel.READ_COMMITTED); assertTrue(runtime.get(fresh, "a", "k").isEmpty());
            runtime.rollback(reader); runtime.rollback(fresh); store.failApply = false; runtime.flush(); runtime.collectGarbage(32);
            assertEquals(0, runtime.stats().retainedBytes()); assertTrue(store.versions.isEmpty());
        }
    }

    /** 墓碑回收只在旧快照结束后执行，已退休的键重放不能复活，仍可重新创建。 */
    @Test void retiredDeleteCannotResurrectOnWalReplay() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            write(runtime, "a", "k", "old"); runtime.flush(); var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k"); runtime.commit(deletion); runtime.flush();
            runtime.collectGarbage(32); assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            runtime.put(old, "a", "k", bytes("wrong"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
            assertEquals(2, runtime.collectGarbage(32)); assertTrue(store.versions.isEmpty());
        }
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            var reader = restored.begin(IsolationLevel.READ_COMMITTED); assertTrue(restored.get(reader, "a", "k").isEmpty()); restored.rollback(reader);
            assertEquals(0, restored.stats().retainedBytes()); write(restored, "a", "k", "reborn"); restored.flush(); restored.checkpoint();
        }
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            var reader = restored.begin(IsolationLevel.READ_COMMITTED); assertEquals("reborn", text(restored.get(reader, "a", "k").orElseThrow())); restored.rollback(reader);
        }
    }

    /** 最慢分片触发积压背压，其他分片追平不能绕过；恢复后可继续提交。 */
    @Test void slowestShardBackpressuresBeforeAppendAndCanRecover() {
        var a = new TestMaterializedStore(); var b = new TestMaterializedStore(); b.failApply = true;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), limits(8192, 0).withMaterializationLag(2))) {
            write(runtime, "a", "k", "one"); write(runtime, "a", "k", "two");
            assertEquals(2, runtime.stats().materializationLag()); long before = runtime.stats().walBytes();
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class, () -> write(runtime, "a", "k", "three")).errorCode());
            assertEquals(before, runtime.stats().walBytes()); assertEquals(2, runtime.stats().publishedSequence());
            b.failApply = false; runtime.flush(); assertEquals(0, runtime.stats().materializationLag());
            write(runtime, "a", "k", "after"); assertEquals(3, runtime.stats().publishedSequence());
        }
    }

    /** 冷索引不可用不能按零版本提交；确认未写日志后恢复服务可以继续工作。 */
    @Test void unavailableColdIndexRejectsWithoutFailingOpen() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), limits(8192, 0))) {
            write(runtime, "a", "k", "old"); runtime.flush(); long before = runtime.stats().walBytes();
            store.onRead = () -> { throw new IllegalStateException("index unavailable"); };
            assertEquals(ErrorCode.STORAGE_UNAVAILABLE, assertThrows(WalMvccException.class, () -> write(runtime, "a", "k", "wrong")).errorCode());
            assertEquals(before, runtime.stats().walBytes()); assertEquals(1, runtime.stats().publishedSequence());
            store.onRead = () -> { }; write(runtime, "a", "k", "after"); assertEquals(2, runtime.stats().publishedSequence());
        }
    }

    /** 在途决议引用的热点键不得被冷读淘汰，否则安装阶段会新增未预留的索引内存。 */
    @Test void evictionCannotInvalidateInFlightIndexReservations() throws Exception {
        var store = new TestMaterializedStore(); var armed = new AtomicBoolean();
        var forced = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store), limits(4096, 300), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE && armed.get()) { forced.countDown(); await(release); }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            write(runtime, "a", "other", "old"); runtime.flush();
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k"); runtime.commit(deletion); runtime.flush(); armed.set(true);
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes("new"));
            var future = tasks.submit(() -> runtime.commit(tx));
            try {
                assertTrue(forced.await(10, TimeUnit.SECONDS)); runtime.collectGarbage(32);
                assertFalse(store.versions.containsKey("k"), "backend tombstone is safe to retire, in-flight memory index is still reserved");
                var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
                long evictions = runtime.stats().cacheEvictions();
                for (int i = 0; i < 20; i++) { assertEquals("old", text(runtime.get(reader, "a", "other").orElseThrow())); }
                assertTrue(runtime.stats().cacheEvictions() > evictions, "cold promotions must actually exercise cache eviction");
                store.onRead = () -> { throw new IllegalStateException("backend unavailable after GC"); };
                try { assertTrue(runtime.get(reader, "a", "k").isEmpty(), "protected tombstone must remain in memory"); }
                finally { store.onRead = () -> { }; }
                runtime.rollback(reader); assertTrue(runtime.stats().retainedBytes() + runtime.stats().bufferedBytes() <= 4096);
            } finally { release.countDown(); }
            future.get(10, TimeUnit.SECONDS); assertTrue(runtime.stats().retainedBytes() <= 4096);
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED); assertEquals("new", text(runtime.get(reader, "a", "k").orElseThrow())); runtime.rollback(reader);
        } finally { release.countDown(); }
    }

    /** 无后端时不允许把唯一权威版本当作热点淘汰，已有本地模式语义保持不变。 */
    @Test void localOnlyVersionsAreNeverTreatedAsDisposableCache() {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), limits(8192, 0))) {
            write(runtime, "a", "k", "local"); runtime.flush(); runtime.checkpoint();
            assertTrue(runtime.stats().retainedBytes() > 0); assertEquals(0, runtime.stats().hotCacheBytes());
        }
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), limits(8192, 0))) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ); assertEquals("local", text(restored.get(tx, "a", "k").orElseThrow())); restored.rollback(tx);
        }
    }

    /** 热点预算不可越过总预算，积压预算必须为正，修改超时不能悄悄重置新配置。 */
    @Test void limitsAreValidatedAndPreserved() {
        assertThrows(IllegalArgumentException.class, () -> limits(1024, 1025));
        assertThrows(IllegalArgumentException.class, () -> limits(1024, -1));
        assertThrows(IllegalArgumentException.class, () -> limits(1024, 0).withMaterializationLag(0));
        var options = limits(8192, 1234).withMaterializationLag(7).withTimeout(12345);
        assertEquals(1234, options.maxHotCacheBytes()); assertEquals(7, options.maxMaterializationLag());
    }

    /** 旧格式镜像中的全量已物化头可以忽略，低内存升级仍从绑定后端得到完整值。 */
    @Test void legacyFullHeadImageCanBeOpenedWithBoundedCache() throws Exception {
        var store = new TestMaterializedStore();
        try (var log = new WalLog(directory, Set.of("a"), 8L * 1024 * 1024)) {
            store.initialize(log.engineId(), "a"); var mutations = new java.util.ArrayList<WalMutation>();
            for (int i = 0; i < 100; i++) { mutations.add(new WalMutation("a", "key:" + i, payload(i))); }
            var commit = new WalCommit(java.util.UUID.randomUUID(), 1, 0, mutations);
            log.append(java.util.List.of(commit)); store.apply(java.util.List.of(commit));
            var heads = mutations.stream().map(mutation -> new WalCheckpointState.Version(1, mutation)).toList();
            log.checkpoint(Set.of("a"), Set.of("a"), 0, heads, System.currentTimeMillis(), stage -> { });
        }
        writeLegacyImageFixture();
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", store), limits(1024, 0))) {
            assertEquals(0, restored.stats().retainedBytes()); var tx = restored.begin(IsolationLevel.READ_COMMITTED);
            for (int i = 0; i < 100; i++) { assertArrayEquals(payload(i), restored.get(tx, "a", "key:" + i).orElseThrow()); }
            restored.rollback(tx); assertTrue(restored.checkpoint().created());
            var current = WalCheckpointFiles.read(directory);
            assertEquals(2, java.nio.ByteBuffer.wrap(Files.readAllBytes(current.generation().resolve("snapshot.bin"))).getInt(4));
        }
    }

    /** 在未运行的测试目录构造格式一镜像，并同步更新指针和尾日志的摘要及 CRC，形成真实旧格式夹具。 */
    private void writeLegacyImageFixture() throws Exception {
        var current = WalCheckpointFiles.read(directory); Path image = current.generation().resolve("snapshot.bin");
        byte[] body = Files.readAllBytes(image); java.nio.ByteBuffer.wrap(body).putInt(4, 1); Files.write(image, body);
        byte[] digest = java.util.HexFormat.of().parseHex(WalCodec.digest(body));
        Path pointer = directory.resolve("CURRENT"); byte[] bytes = Files.readAllBytes(pointer);
        java.nio.ByteBuffer.wrap(bytes).position(24).put(digest); java.nio.ByteBuffer.wrap(bytes).putInt(56, WalFiles.crc(bytes, 0, 56)); Files.write(pointer, bytes);
        Path tail = current.generation().resolve("tail.wal"); bytes = Files.readAllBytes(tail); assertEquals(100, bytes.length);
        java.nio.ByteBuffer.wrap(bytes).position(64).put(digest); java.nio.ByteBuffer.wrap(bytes).putInt(96, WalFiles.crc(bytes, 0, 96)); Files.write(tail, bytes);
        assertEquals(100, WalCheckpointFiles.read(directory).state().versions().size());
    }

    /** 冷冲突检查耗尽事务期限时，不能沿用查询前的期限判断继续写 WAL。 */
    @Test void leaseIsRecheckedAfterColdIndexLookup() {
        var clock = new java.util.concurrent.atomic.AtomicLong(); var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store), limits(8192, 0).withTimeout(100),
                stage -> { }, WalLog.NORMAL_IO, clock::get)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes("late"));
            store.onRead = () -> clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(101));
            assertEquals(ErrorCode.TRANSACTION_TIMEOUT, assertThrows(WalMvccException.class, () -> runtime.commit(tx)).errorCode());
            assertEquals(0, runtime.stats().publishedSequence()); assertEquals(60, runtime.stats().walBytes());
        }
    }

    /** 提供小于完整数据集的预算，关闭高频自动检查点以控制测试的执行顺序。 */
    private static WalOptions limits(long memory, long cache) {
        return new WalOptions(32, 32, 8192, 32, 8, 1, 8L * 1024 * 1024, memory, 60_000, 1024, 20).withHotCacheBytes(cache);
    }

    /** 构造可逐字节验证的固定载荷，避免只核对键存在。 */
    private static byte[] payload(int ordinal) {
        byte[] bytes = new byte[512]; java.util.Arrays.fill(bytes, (byte) (ordinal % 251));
        java.nio.ByteBuffer.wrap(bytes).putInt(ordinal); return bytes;
    }
}

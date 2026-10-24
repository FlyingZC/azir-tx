package mvcc.wal;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
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
import static org.junit.jupiter.api.Assertions.*;

/** 验证独立模式的快照、原子发布、冲突、资源边界和物化回收交错。 */
class WalMvccRuntimeTest {
    @TempDir Path directory;

    /** 区分 RC 与 RR，同时验证读己之写和未提交数据不可见。 */
    @Test void rcRrAndOwnWritesHaveDistinctVisibility() {
        try (var runtime = memory()) {
            write(runtime, "a", "k", "one");
            var rr = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var rc = runtime.begin(IsolationLevel.READ_COMMITTED);
            var writer = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(writer, "a", "k", bytes("two"));
            assertEquals("two", text(runtime.get(writer, "a", "k").orElseThrow()));
            assertEquals("one", text(runtime.get(rc, "a", "k").orElseThrow()));
            runtime.commit(writer);
            assertEquals("one", text(runtime.get(rr, "a", "k").orElseThrow()));
            assertEquals("two", text(runtime.get(rc, "a", "k").orElseThrow()));
            runtime.rollback(rr);
            runtime.rollback(rc);
        }
    }

    /** 验证同一 RC 语句共享跨片切面，刷盘未发布时不能看到部分新值。 */
    @Test void publicationIsAtomicAcrossShardsAndStatements() throws Exception {
        var armed = new AtomicBoolean();
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE && armed.compareAndSet(true, false)) {
                forced.countDown(); await(release);
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
                assertEquals("0", text(runtime.get(reader, "a", "k").orElseThrow()));
                assertEquals("0", text(runtime.get(reader, "b", "k").orElseThrow()));
            } finally { release.countDown(); }
            commit.get(5, TimeUnit.SECONDS);
            assertTrue(runtime.findCommitted(tx.id()).isPresent());
            assertEquals("0", text(runtime.get(reader, "b", "k", snapshot).orElseThrow()));
            var fresh = runtime.beginReadStatement(reader);
            assertEquals("1", text(runtime.get(reader, "a", "k", fresh).orElseThrow()));
            assertEquals("1", text(runtime.get(reader, "b", "k", fresh).orElseThrow()));
            runtime.rollback(reader);
        } finally { release.countDown(); }
    }

    /** 并发同键提交只能有一个成功，包括进入同一刷盘批次的情况。 */
    @Test void concurrentWritersCannotLoseUpdates() throws Exception {
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
                try { runtime.commit(tx); successes.incrementAndGet(); }
                catch (WalMvccException ex) { assertEquals(ErrorCode.WRITE_CONFLICT, ex.errorCode()); conflicts.incrementAndGet(); }
            })).toList();
            start.countDown();
            for (var future : futures) { future.get(5, TimeUnit.SECONDS); }
            assertEquals(1, successes.get());
            assertEquals(1, conflicts.get());
            assertEquals(1, runtime.stats().publishedSequence());
        }
    }

    /** 墓碑、删除后重建、扫描顺序及数组防御性拷贝保持一致。 */
    @Test void tombstonesScansAndByteOwnershipAreCorrect() {
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
            assertEquals(List.of("z"), new ArrayList<>(runtime.scan(now, "a", null, null).keySet()));
            byte[] returned = runtime.get(old, "a", "a").orElseThrow();
            returned[0] = 'Y';
            assertEquals("old", text(runtime.get(old, "a", "a").orElseThrow()));
            write(runtime, "a", "a", "new");
            assertEquals("new", text(runtime.get(now, "a", "a").orElseThrow()));
            assertEquals("old", text(runtime.get(old, "a", "a").orElseThrow()));
            runtime.rollback(old); runtime.rollback(now);
        }
    }

    /** 回滚终态不能被迟到提交复活，重复提交返回相同持久凭据。 */
    @Test void terminalStatesAndReadOnlyCommitAreStable() {
        try (var runtime = memory()) {
            var aborted = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(aborted, "a", "k", bytes("bad"));
            runtime.rollback(aborted); runtime.rollback(aborted);
            assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(WalMvccException.class,
                    () -> runtime.commit(aborted)).errorCode());
            var readOnly = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.commit(readOnly);
            assertEquals(0, runtime.stats().groupForces());
            assertEquals(60, runtime.stats().walBytes());
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("ok"));
            var receipt = runtime.commit(tx);
            assertEquals(receipt, runtime.commit(tx));
            runtime.rollback(tx);
            assertEquals(receipt, runtime.findCommitted(tx.id()).orElseThrow());
            assertEquals(0, runtime.stats().bufferedBytes());
        }
    }

    /** 后端暂停和物化确认丢失时保留正确读结果，恢复后允许安全淘汰。 */
    @Test void partialMaterializationAndLostAcknowledgementAreSafe() {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        b.failApply = true;
        a.loseApplyResponse = true;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("7")); runtime.put(tx, "b", "k", bytes("7"));
            runtime.commit(tx);
            assertThrows(WalMvccException.class, runtime::flush);
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals("7", text(runtime.get(reader, "a", "k").orElseThrow()));
            assertEquals("7", text(runtime.get(reader, "b", "k").orElseThrow()));
            b.failApply = false;
            runtime.flush();
            assertEquals(1, a.checkpoint().sequence()); assertEquals(1, b.checkpoint().sequence());
            assertEquals("7", text(runtime.get(reader, "b", "k").orElseThrow()));
            runtime.rollback(reader);
        }
    }

    /** 长 RR 在刷库和回收后仍可读取旧值；结束后只删除锚点之前的历史。 */
    @Test void oldSnapshotSurvivesEvictionAndGarbageCollection() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "10"); runtime.flush();
            var rr = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "20"); write(runtime, "a", "k", "30"); runtime.flush();
            runtime.collectGarbage(100);
            assertEquals("10", text(runtime.get(rr, "a", "k").orElseThrow()));
            assertEquals(1, runtime.stats().safePoint());
            runtime.put(rr, "a", "k", bytes("stale"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class,
                    () -> runtime.commit(rr)).errorCode());
            assertEquals(2, runtime.collectGarbage(100));
            assertEquals(1, store.versions.get("k").size());
        }
    }

    /** 存储读完后再次检查期限，避免长耗时查询返回超期快照。 */
    @Test void expiryAfterBackendReadIsRejected() {
        var store = new TestMaterializedStore();
        var clock = new java.util.concurrent.atomic.AtomicLong();
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store),
                WalOptions.defaults(), stage -> { }, WalLog.NORMAL_IO, clock::get)) {
            write(runtime, "a", "k", "v"); runtime.flush();
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "new"); runtime.flush();
            store.onRead = () -> clock.addAndGet(TimeUnit.SECONDS.toNanos(61));
            assertEquals(ErrorCode.TRANSACTION_TIMEOUT, assertThrows(WalMvccException.class,
                    () -> runtime.get(tx, "a", "k")).errorCode());
        }
    }

    /** 错误事务归属和跨事务语句快照不能绕过隔离。 */
    @Test void handlesAndSnapshotsCannotCrossOwners() {
        try (var first = memory(); var second = WalMvccRuntime.open(directory.resolve("other"), Set.of("a"), WalOptions.defaults())) {
            var tx = first.begin(IsolationLevel.READ_COMMITTED);
            var other = first.begin(IsolationLevel.READ_COMMITTED);
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(WalMvccException.class,
                    () -> second.commit(tx)).errorCode());
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(WalMvccException.class,
                    () -> first.get(other, "a", "k", first.beginReadStatement(tx))).errorCode());
        }
    }

    /** 磁盘额度耗尽只拒绝当前提交，不破坏此前事务和运行时。 */
    @Test void diskBudgetRejectsBeforeAppendAndLeavesRuntimeUsable() {
        var defaults = WalOptions.defaults();
        var limits = new WalOptions(100, 10, 1000, 10, 1, 0, 1024,
                10_000, 5000, 10, defaults.flushIntervalMillis());
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), limits)) {
            var first = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(first, "a", "k", new byte[512]); runtime.commit(first);
            var rejected = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(rejected, "a", "k", new byte[512]);
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                    () -> runtime.commit(rejected)).errorCode());
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals(512, runtime.get(reader, "a", "k").orElseThrow().length);
            assertEquals(1, runtime.stats().publishedSequence());
            assertEquals(0, runtime.stats().bufferedBytes());
        }
    }

    /** 受限提交队列满时拒绝新请求，不覆盖队列内已有事务。 */
    @Test void boundedQueueRejectsOverflowWithoutLosingAcceptedRequests() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var once = new AtomicBoolean(true);
        var o = WalOptions.defaults();
        var limits = new WalOptions(100, 10, 1024, 1, 1, 0, o.maxWalBytes(), o.maxMemoryBytes(), 5000, 10, 20);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), limits, stage -> {
            if (stage == WalMvccRuntime.Stage.BEFORE_APPEND && once.compareAndSet(true, false)) {
                entered.countDown(); await(release);
            }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(first, "a", "1", bytes("x"));
            var one = tasks.submit(() -> runtime.commit(first));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var next = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(next, "a", "2", bytes("x"));
            var two = tasks.submit(() -> runtime.commit(next));
            try {
                waitFor(() -> runtime.stats().queuedCommits() == 1);
                var overflow = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(overflow, "a", "3", bytes("x"));
                assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                        () -> runtime.commit(overflow)).errorCode());
            } finally { release.countDown(); }
            one.get(5, TimeUnit.SECONDS); two.get(5, TimeUnit.SECONDS);
            assertEquals(2, runtime.stats().publishedSequence());
        } finally { release.countDown(); }
    }

    /** RC 多页读取共享语句切面，期间插入和删除不改变后续页的结果。 */
    @Test void pagesShareOneSnapshotAcrossConcurrentInsertAndDelete() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            for (String key : List.of("a", "b", "中", "😀")) { write(runtime, "a", key, key); }
            runtime.flush();
            var tx = runtime.begin(IsolationLevel.READ_COMMITTED);
            var snapshot = runtime.beginReadStatement(tx);
            var first = runtime.scanPage(tx, "a", null, null, null, 1, snapshot);
            assertEquals(List.of("a"), new ArrayList<>(first.rows().keySet()));
            var change = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(change, "a", "b"); runtime.put(change, "a", "aa", bytes("new")); runtime.commit(change);
            runtime.flush();
            var seen = new ArrayList<>(first.rows().keySet());
            String cursor = first.nextAfterKey();
            while (cursor != null) {
                var page = runtime.scanPage(tx, "a", null, null, cursor, 1, snapshot);
                seen.addAll(page.rows().keySet()); cursor = page.nextAfterKey();
            }
            assertEquals(List.of("a", "b", "中", "😀"), seen);
            assertEquals(List.of("a", "aa", "中", "😀"), new ArrayList<>(runtime.scan(tx, "a", null, null).keySet()));
        }
    }

    /** 刷盘期间预留新键索引内存，其他事务不能挤占这部分容量。 */
    @Test void memoryBudgetIncludesPendingIndexReservations() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var o = WalOptions.defaults();
        var limits = new WalOptions(10, 10, 256, 10, 1, 0, o.maxWalBytes(), 300, 5000, 10, 20);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), limits, stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) { forced.countDown(); await(release); }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", new byte[]{1});
            var commit = tasks.submit(() -> runtime.commit(tx));
            try {
                assertTrue(forced.await(5, TimeUnit.SECONDS));
                var other = runtime.begin(IsolationLevel.REPEATABLE_READ);
                assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                        () -> runtime.put(other, "a", "j", new byte[]{1})).errorCode());
                runtime.rollback(other);
            } finally { release.countDown(); }
            commit.get(5, TimeUnit.SECONDS);
            assertTrue(runtime.stats().retainedBytes() <= 300);
            assertEquals(0, runtime.stats().bufferedBytes());
        } finally { release.countDown(); }
    }

    /** 提交等待被中断仅返回未知结果，不能取消已刷盘事务或转为回滚。 */
    @Test void interruptedCommitWaitCanBeResolvedByTransactionId() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) { forced.countDown(); await(release); }
        })) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes("v"));
            var failure = new java.util.concurrent.atomic.AtomicReference<WalMvccException>();
            Thread waiter = Thread.ofVirtual().start(() -> {
                try { runtime.commit(tx); } catch (WalMvccException ex) { failure.set(ex); }
            });
            try {
                assertTrue(forced.await(5, TimeUnit.SECONDS)); waiter.interrupt(); waiter.join(5000);
                assertNotNull(failure.get()); assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.get().errorCode());
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                        () -> runtime.rollback(tx)).errorCode());
            } finally { release.countDown(); }
            waitFor(() -> runtime.stats().publishedSequence() == 1);
            assertEquals(runtime.findCommitted(tx.id()).orElseThrow(), runtime.commit(tx));
        } finally { release.countDown(); }
    }

    /** 对固定随机种子的混合历史，用独立多版本参考模型检查快照结果。 */
    @Test void randomizedHistoriesMatchReferenceModel() {
        var store = new TestMaterializedStore();
        var random = new Random(20261010);
        var model = new TreeMap<String, TreeMap<Long, String>>();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            var readers = new ArrayList<WalTransaction>();
            for (int step = 0; step < 200; step++) {
                if (step % 20 == 0) { readers.add(runtime.begin(IsolationLevel.REPEATABLE_READ)); }
                String key = "k" + random.nextInt(8);
                String value = random.nextBoolean() ? "v" + step : null;
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                if (value == null) { runtime.delete(tx, "a", key); } else { runtime.put(tx, "a", key, bytes(value)); }
                long sequence = runtime.commit(tx).commitSequence();
                model.computeIfAbsent(key, ignored -> new TreeMap<>()).put(sequence, value);
                runtime.flush(); runtime.collectGarbage(32);
                for (WalTransaction reader : readers) {
                    var actual = runtime.scan(reader, "a", null, null);
                    var expected = new TreeMap<String, String>();
                    model.forEach((k, chain) -> {
                        var version = chain.floorEntry(reader.startSequence());
                        if (version != null && version.getValue() != null) { expected.put(k, version.getValue()); }
                    });
                    assertEquals(expected.keySet(), actual.keySet());
                    expected.forEach((k, v) -> assertEquals(v, text(actual.get(k))));
                }
            }
            readers.forEach(runtime::rollback);
        }
    }

    /** 并发跨片转账与语句级读取始终保持总额不变。 */
    @Test void concurrentTransfersNeverExposeFracturedReads() throws Exception {
        try (var runtime = memory(); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "balance", bytes("500")); runtime.put(seed, "b", "balance", bytes("500"));
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
                } finally { done.set(true); }
            });
            var reader = tasks.submit(() -> {
                int reads = 0;
                do {
                    var tx = runtime.begin(IsolationLevel.READ_COMMITTED);
                    var snapshot = runtime.beginReadStatement(tx);
                    int a = Integer.parseInt(text(runtime.get(tx, "a", "balance", snapshot).orElseThrow()));
                    int b = Integer.parseInt(text(runtime.get(tx, "b", "balance", snapshot).orElseThrow()));
                    assertEquals(1000, a + b);
                    runtime.rollback(tx); reads++;
                } while (!done.get());
                return reads;
            });
            writer.get(10, TimeUnit.SECONDS);
            assertTrue(reader.get(10, TimeUnit.SECONDS) > 0);
        }
    }

    /** 创建无后端物化的事务内核。 */
    private WalMvccRuntime memory() { return WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults()); }

    /** 写入并提交一笔单键事务。 */
    static WalReceipt write(WalMvccRuntime runtime, String shard, String key, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, shard, key, bytes(value));
        return runtime.commit(tx);
    }

    /** 编码测试值。 */
    static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 解码测试值。 */
    static String text(byte[] value) { return new String(value, StandardCharsets.UTF_8); }

    /** 有界等待确定性并发屏障。 */
    static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) { throw new AssertionError("barrier timeout"); } }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new AssertionError(ex); }
    }

    /** 有界等待后台状态，不使用无期限轮询。 */
    static void waitFor(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) { fail("condition timeout"); }
            Thread.sleep(5);
        }
    }
}

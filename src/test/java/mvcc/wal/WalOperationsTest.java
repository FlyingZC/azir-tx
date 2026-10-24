package mvcc.wal;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 托管维护验证旧快照、预算、非阻塞健康状态和镜像预先拒绝。 */
class WalOperationsTest {
    @TempDir Path directory;

    /** 自动回收保留旧 RR，读者结束后版本内存逐步释放，最新值不丢。 */
    @Test void automaticMaintenanceProtectsOldRrAndEventuallyReclaimsVersions() throws Exception {
        var tuning = new WalMaintenanceOptions(10, 1, 2, 50, 1000, 80);
        try (var service = WalManagedRuntime.open(directory, Set.of("a"), WalOptions.defaults(), WalRetentionOptions.preserveAll(), tuning)) {
            var runtime = service.runtime(); write(runtime, "a", "k", "old"); var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 12; i++) { write(runtime, "a", "k", "v" + i); }
            waitFor(() -> service.health().maintenanceCycles() > 0);
            assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow())); runtime.rollback(old);
            waitFor(() -> runtime.stats().retainedBytes() < 1024);
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED); assertEquals("v11", text(runtime.get(reader, "a", "k").orElseThrow())); runtime.rollback(reader);
            assertTrue(service.metrics().contains("azir_wal_memory_retired_versions_total ")); assertTrue(service.health().ready());
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(13, reopened.stats().publishedSequence()); }
    }

    /** 后端故意卡住运行时读锁时，健康查询仍能返回过期状态，关闭只等待自有工作者。 */
    @Test void healthDoesNotWaitForBlockedBackendAndCloseWaitsSafely() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var delegate = new TestMaterializedStore();
        WalMaterializedStore blocked = (WalMaterializedStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{WalMaterializedStore.class}, (proxy, method, args) -> {
            if (method.getName().equals("supportsBoundedMaintenance")) { return true; }
            if (method.getName().equals("validateProduction")) { return null; }
            if (method.getName().equals("gcBounded")) { entered.countDown(); await(release); return new WalMaterializedStore.GcProgress(0, 0, false); }
            try { return method.invoke(delegate, args); } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
        });
        var service = WalManagedRuntime.open(directory, Map.of("a", blocked), WalOptions.defaults(), WalRetentionOptions.preserveAll(), new WalMaintenanceOptions(10, 1, 1, 10, 50, 80));
        try (var tasks = Executors.newFixedThreadPool(2)) {
            assertTrue(entered.await(30, TimeUnit.SECONDS)); TimeUnit.MILLISECONDS.sleep(80);
            var health = tasks.submit(service::health).get(2, TimeUnit.SECONDS);
            assertEquals(WalManagedRuntime.State.DEGRADED, health.state()); assertFalse(health.ready());
            assertTrue(tasks.submit(service::metrics).get(2, TimeUnit.SECONDS).contains("azir_wal_ready 0\n"));
            var closing = tasks.submit(service::close);
            waitFor(() -> service.health().state() == WalManagedRuntime.State.CLOSING); assertFalse(closing.isDone());
            release.countDown(); closing.get(30, TimeUnit.SECONDS); assertEquals(WalManagedRuntime.State.CLOSED, service.health().state());
        } finally { release.countDown(); service.close(); }
    }

    /** 单次内存 GC 不能删除超过预算的历史版本，分批结束后墓碑才可退休。 */
    @Test void memoryGcLimitsVersionsAndDoesNotResurrectTombstones() {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            for (int i = 0; i < 9; i++) { write(runtime, "a", "k", "v" + i); }
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(tx, "a", "k"); runtime.commit(tx);
            long retired = 0;
            for (int i = 0; i < 5; i++) {
                runtime.collectGarbageBounded(1, 2, 50); long current = runtime.stats().retiredMemoryVersions(); assertTrue(current - retired <= 2); retired = current;
                var reader = runtime.begin(IsolationLevel.READ_COMMITTED); assertTrue(runtime.get(reader, "a", "k").isEmpty()); runtime.rollback(reader);
            }
            assertEquals(10, retired); assertEquals(0, runtime.stats().retainedBytes());
        }
    }

    /** 全部字符串按 modified UTF 精确计费，估算与实际格式一致。 */
    @Test void checkpointBudgetMatchesUnicodeEncodingAndRejectsBeforeAllocating() throws Exception {
        var versions = List.of(new WalCheckpointState.Version(1, new WalMutation("a", "k\u0000😀", bytes("one"))),
                new WalCheckpointState.Version(2, new WalMutation("中文", "", null)));
        var archive = new WalReceiptArchive.Descriptor("receipts-" + UUID.randomUUID() + ".sst", 1, 1, 1, Long.MAX_VALUE, "0".repeat(64));
        var state = new WalCheckpointState(UUID.randomUUID(), Set.of("a", "中文"), Set.of(), 2, 0,
                new WalReceipt(UUID.randomUUID(), 2, "0".repeat(64)), versions, List.of(archive), new byte[128], 0);
        assertEquals(state.encode().length, WalCheckpointState.checkEncodingBudget(state.shards(), state.storedShards(), versions, 1, 128));
        var mutation = new WalMutation("a", "k", new byte[8 * 1024 * 1024]);
        var huge = java.util.stream.LongStream.rangeClosed(1, 20).mapToObj(seq -> new WalCheckpointState.Version(seq, mutation)).toList();
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                () -> WalCheckpointState.checkEncodingBudget(Set.of("a"), Set.of(), huge, 1, 128)).errorCode());
        try (var log = new WalLog(directory, Set.of("a"), 8L * 1024 * 1024)) {
            for (int i = 1; i <= 20; i++) { log.append(List.of(new WalCommit(UUID.randomUUID(), i, i - 1, List.of(new WalMutation("a", "k", bytes("small")))))); }
            assertThrows(WalMvccException.class, () -> log.checkpoint(Set.of("a"), Set.of(), 0, huge, 0, stage -> { }));
            try (var files = Files.list(directory)) { assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith("receipts-") || path.getFileName().toString().startsWith("checkpoint-"))); }
            assertEquals(20, log.lastSequence());
        }
    }

    /** 归档容量达到上限时健康状态明确背压，计数来自真实文件大小。 */
    @Test void archiveBudgetIsVisibleInHealthAndMetrics() throws Exception {
        long archiveSize = WalReceiptArchive.estimatedBytes(1);
        var retention = new WalRetentionOptions(0, 3_600_000, 4_000_000, archiveSize, 1000);
        try (var service = WalManagedRuntime.open(directory, Set.of("a"), WalOptions.defaults(), retention, new WalMaintenanceOptions(10, 1, 1, 50, 1000, 80))) {
            write(service.runtime(), "a", "k", "value"); service.runtime().checkpoint();
            waitFor(() -> service.health().state() == WalManagedRuntime.State.BACKPRESSURE);
            assertEquals(archiveSize, service.runtime().stats().receiptArchiveBytes()); assertTrue(service.health().warnings().contains("receipt_archive_budget"));
            try (var files = Files.list(directory)) {
                long actual = files.filter(path -> path.getFileName().toString().endsWith(".sst")).mapToLong(path -> {
                    try { return Files.size(path); } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
                }).sum();
                assertEquals(actual, service.runtime().stats().receiptArchiveBytes());
            }
            assertTrue(service.metrics().contains("azir_wal_receipt_archive_bytes " + archiveSize + "\n"));
        }
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(archiveSize, runtime.stats().receiptArchiveBytes()); }
    }

    /** 未验证后端不能在托管入口默认启用维护，参数也必须在创建目录前检查。 */
    @Test void unsupportedBackendsAndInvalidMaintenanceAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> WalManagedRuntime.open(directory.resolve("unsupported"), Map.of("a", new TestMaterializedStore()),
                WalOptions.defaults(), WalRetentionOptions.preserveAll(), WalMaintenanceOptions.defaults()));
        assertFalse(Files.exists(directory.resolve("unsupported")));
        assertThrows(IllegalArgumentException.class, () -> new WalMaintenanceOptions(1, 1, 1, 1, 100, 80));
        assertThrows(IllegalArgumentException.class, () -> new WalMaintenanceOptions(100, 1, 5000, 1, 1000, 80));
    }

    /** 只读状态拒绝已有写集和新修改，允许读取和回滚，解除后正常提交。 */
    @Test void readOnlyMaintenanceKeepsReadsAvailableAndRejectsNewWrites() {
        try (var service = WalManagedRuntime.open(directory, Set.of("a"), WalOptions.defaults(), WalRetentionOptions.preserveAll(), WalMaintenanceOptions.defaults())) {
            var runtime = service.runtime(); write(runtime, "a", "k", "old");
            var pending = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(pending, "a", "k", bytes("pending"));
            service.setReadOnly(true); assertEquals(WalManagedRuntime.State.READ_ONLY, service.health().state()); assertFalse(service.health().ready());
            assertThrows(WalMvccException.class, () -> runtime.commit(pending)); runtime.rollback(pending);
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED); assertEquals("old", text(runtime.get(reader, "a", "k").orElseThrow()));
            assertThrows(WalMvccException.class, () -> runtime.delete(reader, "a", "k")); runtime.commit(reader);
            assertEquals(1, runtime.stats().publishedSequence()); assertTrue(service.metrics().contains("azir_wal_read_only 1\n"));
            service.setReadOnly(false); write(runtime, "a", "k", "after"); assertEquals(2, runtime.stats().publishedSequence());
        }
    }

    /** 开关不撤销已经进入持久决议的事务，避免把可能已提交的请求误报成回滚。 */
    @Test void readOnlySwitchDrainsPreviouslyQueuedDecision() throws Exception {
        var reached = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) { reached.countDown(); await(release); }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes("value"));
            var future = tasks.submit(() -> runtime.commit(tx)); assertTrue(reached.await(30, TimeUnit.SECONDS)); runtime.setReadOnly(true);
            release.countDown(); assertEquals(1, future.get(30, TimeUnit.SECONDS).commitSequence()); assertTrue(runtime.findCommitted(tx.id()).isPresent());
            var reader = runtime.begin(IsolationLevel.READ_COMMITTED); assertEquals("value", text(runtime.get(reader, "a", "k").orElseThrow())); runtime.rollback(reader);
        } finally { release.countDown(); }
    }

    /** 维护失败提供安全诊断并按周期重试，恢复后清除降级状态且保留累计失败计数。 */
    @Test void maintenanceFailureIsObservableWithoutLeakingBusinessDataAndCanRecover() throws Exception {
        var failing = new java.util.concurrent.atomic.AtomicBoolean(true); var delegate = new TestMaterializedStore();
        WalMaterializedStore backend = (WalMaterializedStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{WalMaterializedStore.class}, (proxy, method, args) -> {
            if (method.getName().equals("supportsBoundedMaintenance")) { return true; }
            if (method.getName().equals("validateProduction")) { return null; }
            if (method.getName().equals("gcBounded")) {
                if (failing.get()) { throw new WalMvccException(ErrorCode.STORAGE_UNAVAILABLE, "private-business-value", null,
                        new java.sql.SQLException("private-business-value", "08006", 2013)); }
                return new WalMaterializedStore.GcProgress(0, 0, false);
            }
            try { return method.invoke(delegate, args); } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
        });
        try (var service = WalManagedRuntime.open(directory, Map.of("a", backend), WalOptions.defaults(), WalRetentionOptions.preserveAll(),
                new WalMaintenanceOptions(10, 1, 1, 10, 1000, 80))) {
            waitFor(() -> service.health().maintenanceFailures() > 0);
            assertEquals(WalManagedRuntime.State.DEGRADED, service.health().state()); assertTrue(service.health().failure().contains("sqlstate=08006:vendor=2013"));
            assertFalse(service.health().failure().contains("private-business-value")); assertFalse(service.metrics().contains("private-business-value"));
            failing.set(false); waitFor(() -> service.health().ready()); assertTrue(service.health().maintenanceFailures() > 0);
        }
    }
}

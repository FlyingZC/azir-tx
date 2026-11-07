package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
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
import java.util.concurrent.atomic.AtomicLong;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 验证检查点切换、旧快照、凭据保留及内存回收，旧日志不可先于完整新代被删除。 */
class WalCheckpointTest {
    @TempDir Path directory;

    /** 纯本地旧 RR 需要的历史随镜像保留，截断后凭据和全局序号仍连续。 */
    @Test void localCheckpointRetainsOldSnapshotAndArchivedReceipts() throws Exception {
        WalReceipt old; WalReceipt current;
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            old = pair(runtime, "old"); var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            current = pair(runtime, "new"); runtime.collectGarbage(32);
            var result = runtime.checkpoint(); assertTrue(result.created()); assertEquals(2, result.sequence());
            assertEquals(100, result.walBytesAfter()); assertFalse(Files.exists(directory.resolve("transactions.wal")));
            assertEquals(0, runtime.stats().pendingReceipts()); assertEquals(old, runtime.findCommitted(old.transactionId()).orElseThrow());
            assertPair(runtime, reader, "old"); runtime.rollback(reader);
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertEquals(2, reopened.stats().publishedSequence()); assertEquals(2, reopened.stats().checkpointSequence());
            assertEquals(current, reopened.findCommitted(current.transactionId()).orElseThrow());
            var reader = reopened.begin(IsolationLevel.REPEATABLE_READ); assertPair(reopened, reader, "new"); reopened.rollback(reader);
            assertEquals(3, pair(reopened, "after").commitSequence());
            assertTrue(reopened.checkpoint().created());
            assertEquals(old, reopened.findCommitted(old.transactionId()).orElseThrow());
        }
    }

    /** 两片已追平后旧 RR 可从物化历史读取，截断不能抹掉旧写者的冲突栅栏。 */
    @Test void materializedCheckpointPreservesSnapshotsAndWriteConflicts() {
        var a = new TestMaterializedStore(); var b = new TestMaterializedStore(); UUID oldId;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            oldId = pair(runtime, "old").transactionId(); runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ); pair(runtime, "new"); runtime.flush();
            assertTrue(runtime.checkpoint().created()); runtime.collectGarbage(32); assertPair(runtime, old, "old");
            runtime.put(old, "a", "k", bytes("stale"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
        }
        try (var reopened = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            assertEquals(WalCommitLookup.Status.COMMITTED, reopened.lookupCommit(oldId).status());
            var reader = reopened.begin(IsolationLevel.READ_COMMITTED); assertPair(reopened, reader, "new"); reopened.rollback(reader);
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory,
                Map.of("a", new TestMaterializedStore(), "b", new TestMaterializedStore()), WalOptions.defaults()));
    }

    /** 一片落后时不能强推检查点，恢复后仍能提交和创建完整新代。 */
    @Test void laggingBackendCannotAuthorizeLogDeletion() throws Exception {
        var a = new TestMaterializedStore(); var b = new TestMaterializedStore(); b.failApply = true;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults())) {
            pair(runtime, "one"); assertThrows(WalMvccException.class, runtime::checkpoint);
            assertFalse(Files.exists(directory.resolve("CURRENT"))); assertEquals(2, pair(runtime, "two").commitSequence());
            b.failApply = false; runtime.flush(); assertTrue(runtime.checkpoint().created());
        }
    }

    /** 短保留窗口只用于测试；回收缺失结果明确为未知或过期，不能当成回滚。 */
    @Test void receiptExpiryDoesNotBecomeAnAbortDecision() {
        var time = new AtomicLong(); var policy = new WalRetentionOptions(100, 3_600_000, 4_000_000, 1_000_000, 1000);
        UUID first; UUID second;
        try (var runtime = timed(policy, time)) {
            first = pair(runtime, "first").transactionId(); runtime.checkpoint();
            time.set(50); second = pair(runtime, "second").transactionId(); runtime.checkpoint();
            assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(first).status());
            time.set(101); runtime.checkpoint();
            assertEquals(1, runtime.stats().retiredReceiptThrough());
            assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, runtime.lookupCommit(first).status());
            assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(second).status());
            assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, runtime.lookupCommit(UUID.randomUUID()).status());
            assertEquals(1, runtime.stats().receiptArchives());
        }
        try (var reopened = timed(policy, time)) {
            assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, reopened.lookupCommit(first).status());
            assertTrue(reopened.findCommitted(second).isPresent()); assertEquals(3, pair(reopened, "after").commitSequence());
        }
    }

    /** 尚未物化的删除墓碑不能因回收索引而复活后端旧值。 */
    @Test void unmaterializedTombstoneCannotBeRetired() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "old"); runtime.flush(); store.failApply = true;
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k"); runtime.commit(deletion);
            runtime.collectGarbage(32);
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ); assertTrue(runtime.get(reader, "a", "k").isEmpty()); runtime.rollback(reader);
            assertTrue(runtime.stats().retainedBytes() > 0);
            store.failApply = false; runtime.flush(); runtime.collectGarbage(32);
            assertEquals(0, runtime.stats().retainedBytes());
            reader = runtime.begin(IsolationLevel.REPEATABLE_READ); assertTrue(runtime.get(reader, "a", "k").isEmpty()); runtime.rollback(reader);
        }
    }

    /** 固定工作集经历 120 次实际截断，日志索引和版本历史不随累计提交增长。 */
    @Test void repeatedCheckpointsBoundActiveLogAndMemory() {
        var time = new AtomicLong(); var policy = new WalRetentionOptions(10, 3_600_000, 4_000_000, 1_000_000, 1000);
        try (var runtime = timed(policy, time)) {
            for (int round = 0; round < 120; round++) {
                pair(runtime, "v" + round); runtime.collectGarbage(32); time.addAndGet(11); runtime.checkpoint();
                assertEquals(100, runtime.stats().walBytes()); assertEquals(0, runtime.stats().pendingReceipts());
                assertEquals(1, runtime.stats().receiptArchives()); assertTrue(runtime.stats().retainedBytes() < 2048);
            }
            assertEquals(120, runtime.stats().publishedSequence());
        }
        try (var reopened = timed(policy, time)) {
            var reader = reopened.begin(IsolationLevel.REPEATABLE_READ); assertPair(reopened, reader, "v119"); reopened.rollback(reader);
            assertEquals(121, pair(reopened, "next").commitSequence());
        }
    }

    /** 提交刷盘后尚未发布时检查点必须等待决议结束，不能把在途事务切走。 */
    @Test void checkpointWaitsForWholeInFlightDecision() throws Exception {
        var forced = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) { forced.countDown(); await(release); }
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var committing = tasks.submit(() -> pair(runtime, "whole")); assertTrue(forced.await(30, TimeUnit.SECONDS));
            var checkpoint = tasks.submit(runtime::checkpoint);
            assertFalse(Files.exists(directory.resolve("CURRENT"))); release.countDown();
            var receipt = committing.get(30, TimeUnit.SECONDS); assertTrue(checkpoint.get(30, TimeUnit.SECONDS).created());
            assertEquals(receipt, runtime.findCommitted(receipt.transactionId()).orElseThrow());
        } finally { release.countDown(); }
    }

    /** 指针或镜像损坏必须拒绝恢复，不能自动回退到可能缺失成功事务的旧代。 */
    @Test void corruptedPointerAndImageFailClosedWithoutCleanup() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) { pair(runtime, "safe"); runtime.checkpoint(); }
        Path pointer = directory.resolve("CURRENT"); byte[] original = Files.readAllBytes(pointer); byte[] damaged = original.clone(); damaged[10] ^= 1;
        Files.write(pointer, damaged);
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults()));
        assertArrayEquals(damaged, Files.readAllBytes(pointer)); Files.write(pointer, original);
        Path image;
        try (var children = Files.list(directory)) { image = children.filter(Files::isDirectory).findFirst().orElseThrow().resolve("snapshot.bin"); }
        byte[] payload = Files.readAllBytes(image); payload[payload.length - 1] ^= 1; Files.write(image, payload);
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults()));
        assertArrayEquals(payload, Files.readAllBytes(image));
    }

    /** 凭据归档预算不足时只背压新写入，不能删除已确认结果或停掉正常读取。 */
    @Test void archiveBudgetBackpressureNeverDropsProof() throws Exception {
        var policy = new WalRetentionOptions(0, 3_600_000, 4_000_000, 4096, 2);
        WalReceipt first; WalReceipt second;
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults(), policy)) {
            first = pair(runtime, "one");
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class, runtime::checkpoint).errorCode());
            second = pair(runtime, "two");
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class, () -> pair(runtime, "three")).errorCode());
            assertFalse(Files.exists(directory.resolve("CURRENT"))); assertEquals(2, runtime.stats().publishedSequence());
            assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow());
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ); assertPair(runtime, reader, "two"); runtime.rollback(reader);
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertEquals(first, reopened.findCommitted(first.transactionId()).orElseThrow());
            assertEquals(second, reopened.findCommitted(second.transactionId()).orElseThrow()); assertTrue(reopened.checkpoint().created());
        }
    }

    /** 在独立时钟上构造显式测试保留窗口，生产默认不受测试窗口影响。 */
    @Test void unicodeAndZeroByteKeysSurviveCheckpointCodec() throws Exception {
        var keys = java.util.List.of("", "a\u0000b", "中文", "x\uE000", "x😀");
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < keys.size(); i++) { runtime.put(tx, "a", keys.get(i), bytes("v" + i)); }
            runtime.commit(tx); runtime.checkpoint();
        }
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ); var snapshot = restored.beginReadStatement(tx);
            for (int i = 0; i < keys.size(); i++) { assertEquals("v" + i, text(restored.get(tx, "a", keys.get(i), snapshot).orElseThrow())); }
            var ordered = keys.stream().sorted(com.flyingzc.azir.storage.Utf8KeyOrder::compare).toList();
            assertEquals(ordered, new java.util.ArrayList<>(restored.scan(tx, "a", null, null, snapshot).keySet())); restored.rollback(tx);
        }
    }

    /** 在独立时钟上构造显式测试保留窗口，生产默认不受测试窗口影响。 */
    private WalMvccRuntime timed(WalRetentionOptions policy, AtomicLong clock) {
        return WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> { },
                WalLog.NORMAL_IO, System::nanoTime, policy, clock::get);
    }

    /** 提交完整跨片写集并返回稳定凭据。 */
    private static WalReceipt pair(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes(value)); runtime.put(tx, "b", "k", bytes(value)); return runtime.commit(tx);
    }

    /** 用同一语句快照核对两片内容。 */
    private static void assertPair(WalMvccRuntime runtime, WalTransaction tx, String value) {
        var snapshot = runtime.beginReadStatement(tx);
        assertEquals(value, text(runtime.get(tx, "a", "k", snapshot).orElseThrow()));
        assertEquals(value, text(runtime.get(tx, "b", "k", snapshot).orElseThrow()));
    }
}

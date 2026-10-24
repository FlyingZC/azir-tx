package mvcc.wal;

import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 完整本地备份验证数据、凭据、恢复发布、损坏拒绝及关闭竞争。 */
class WalBackupTest {
    @TempDir Path directory;

    /** 备份固定在自己的提交边界，源继续写入不会改变恢复后的值和凭据。 */
    @Test void localBackupRestoresDataReceiptsAndSequenceAtItsOwnCut() throws Exception {
        Path archive = directory.resolve("backup"); Path restored = directory.resolve("restored"); WalReceipt first; WalReceipt second;
        try (var runtime = WalMvccRuntime.open(directory.resolve("source"), Set.of("a", "b"), WalOptions.defaults())) {
            first = pair(runtime, "old"); var old = runtime.begin(IsolationLevel.REPEATABLE_READ); second = pair(runtime, "new");
            var info = runtime.backup(archive, WalBackupOptions.defaults()); assertEquals(2, info.sequence()); assertTrue(info.rows().isEmpty());
            assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow())); runtime.rollback(old);
            assertEquals(3, pair(runtime, "after-backup").commitSequence()); assertFalse(runtime.stats().backupRunning());
        }
        var inspected = WalBackup.inspect(archive, WalBackupOptions.defaults());
        assertEquals(2, inspected.sequence()); WalBackup.restore(archive, restored, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        try (var runtime = WalMvccRuntime.open(restored, Set.of("a", "b"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
            for (String shard : new String[]{"a", "b"}) { assertEquals("new", text(runtime.get(tx, shard, "k", snapshot).orElseThrow())); }
            runtime.rollback(tx); assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow());
            assertEquals(second, runtime.findCommitted(second.transactionId()).orElseThrow()); assertEquals(3, pair(runtime, "restored-write").commitSequence());
        }
    }

    /** 空引擎也能备份和恢复，不能把零提交边界误认为缺少恢复入口。 */
    @Test void emptyEngineCanBeBackedUpAndRestored() throws Exception {
        Path backup = directory.resolve("empty-backup"); Path target = directory.resolve("empty-target");
        try (var runtime = WalMvccRuntime.open(directory.resolve("empty"), Set.of("a", "b"), WalOptions.defaults())) {
            assertEquals(0, runtime.backup(backup, WalBackupOptions.defaults()).sequence());
        }
        WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) { assertEquals(1, pair(runtime, "first").commitSequence()); }
    }

    /** 备份保留已经退休的查询边界，恢复后缺失旧凭据仍是未知或过期，不能变成回滚。 */
    @Test void expiredReceiptHistoryRetainsUnknownSemanticsAfterRestore() throws Exception {
        var clock = new java.util.concurrent.atomic.AtomicLong(); var policy = new WalRetentionOptions(1, 3_600_000, 4_000_000, 1_000_000, 1000);
        Path backup = directory.resolve("expired-backup"); WalReceipt old; WalReceipt recent;
        try (var runtime = WalMvccRuntime.openForTest(directory.resolve("expired-source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> { },
                WalLog.NORMAL_IO, System::nanoTime, policy, clock::get)) {
            old = pair(runtime, "old"); runtime.checkpoint(); clock.set(2); recent = pair(runtime, "recent"); runtime.backup(backup, WalBackupOptions.defaults());
        }
        Path target = directory.resolve("expired-target"); WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) {
            assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, runtime.lookupCommit(old.transactionId()).status());
            assertEquals(recent, runtime.findCommitted(recent.transactionId()).orElseThrow());
        }
    }

    /** 损坏和缺少完成标记的备份必须在修改恢复目标之前拒绝。 */
    @Test void corruptionAndIncompleteBackupCannotPublishARecoveryDirectory() throws Exception {
        Path backup = localBackup(); Path target = directory.resolve("bad-target"); byte[] manifest = Files.readAllBytes(backup.resolve("COMPLETE"));
        byte[] bad = manifest.clone(); bad[20] ^= 1; Files.write(backup.resolve("COMPLETE"), bad);
        assertThrows(IOException.class, () -> WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
        assertFalse(Files.exists(target)); Files.write(backup.resolve("COMPLETE"), manifest);
        Path engine = backup.resolve("local/ENGINE"); byte[] identity = Files.readAllBytes(engine); identity[10] ^= 1; Files.write(engine, identity);
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults())); assertFalse(Files.exists(target));
        Files.delete(backup.resolve("COMPLETE")); assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
    }

    /** 已有目录永不被恢复覆盖，符号链接也不能引导读取外部数据。 */
    @Test void occupiedTargetsAndSymlinkFilesAreRejected() throws Exception {
        Path backup = localBackup(); Path target = directory.resolve("occupied"); Files.createDirectory(target); Files.writeString(target.resolve("user-file"), "keep");
        assertThrows(IOException.class, () -> WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
        assertEquals("keep", Files.readString(target.resolve("user-file")));
        Path engine = backup.resolve("local/ENGINE"); Files.delete(engine); Files.createSymbolicLink(engine, directory.resolve("missing-external-file"));
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
    }

    /** 字节预算耗尽不产生完成备份，也不能把健康源引擎停掉或丢失确认事务。 */
    @Test void budgetFailureLeavesSourceHealthyAndBackupIncomplete() throws Exception {
        Path backup = directory.resolve("too-small");
        try (var runtime = WalMvccRuntime.open(directory.resolve("budget-source"), Set.of("a", "b"), WalOptions.defaults())) {
            pair(runtime, "x".repeat(4096));
            assertThrows(WalMvccException.class, () -> runtime.backup(backup, new WalBackupOptions(1024, 32, 8192, 60_000)));
            assertFalse(Files.exists(backup.resolve("COMPLETE"))); assertFalse(runtime.stats().backupRunning()); assertEquals(2, pair(runtime, "still-live").commitSequence());
        }
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
    }

    /** 备份完成标记前后失败分别对应未完成和可核实的完整备份，源都应继续可用。 */
    @Test void completionBoundaryFailuresHaveExplicitRecoverableArtifacts() throws Exception {
        for (var stage : new WalMvccRuntime.Stage[]{WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE, WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE}) {
            Path source = directory.resolve("source-" + stage); Path backup = directory.resolve("backup-" + stage); var armed = new AtomicBoolean();
            try (var runtime = WalMvccRuntime.openForTest(source, Set.of("a", "b"), Map.of(), WalOptions.defaults(), hit -> {
                if (hit == stage && armed.compareAndSet(true, false)) { throw new IllegalStateException("injected backup publication failure"); }
            })) {
                pair(runtime, "one"); armed.set(true); assertThrows(IllegalStateException.class, () -> runtime.backup(backup, WalBackupOptions.defaults()));
                if (stage == WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE) { assertEquals(1, WalBackup.inspect(backup, WalBackupOptions.defaults()).sequence()); }
                else { assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults())); }
                assertEquals(2, pair(runtime, "two").commitSequence());
            }
        }
    }

    /** 原子移动后尚未清除恢复标记时拒绝服务，同一份备份可接续完成。 */
    @Test void restorePublicationFailureCanResumeWithoutExposingPartialState() throws Exception {
        Path backup = localBackup(); Path target = directory.resolve("resume-target");
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_MOVE) { throw new IllegalStateException("lost restore publication response"); }
        }));
        assertTrue(Files.exists(target.resolve("RESTORING")));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults()));
        assertFalse(Files.exists(target.resolve("writer.lock")));
        WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()); assertFalse(Files.exists(target.resolve("RESTORING")));
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) { assertEquals(2, pair(runtime, "after").commitSequence()); }
        assertThrows(IOException.class, () -> WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
    }

    /** 首次复制实际写出后刷盘失败，续做必须再次同步这个已校验完整的文件，不能只跳过它。 */
    @Test void resumedCompleteFileIsForcedAgainBeforePublication() throws Exception {
        Path backup = localBackup(); Path target = directory.resolve("force-resume"); var failed = new java.util.concurrent.atomic.AtomicReference<Path>();
        assertThrows(IOException.class, () -> WalBackup.restoreForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), stage -> { }, (path, channel) -> {
            failed.set(path); throw new IOException("injected force failure after file contents were written");
        }));
        assertNotNull(failed.get()); assertTrue(Files.size(failed.get()) > 0); assertFalse(Files.exists(target));
        var forced = new java.util.HashSet<Path>();
        WalBackup.restoreForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), stage -> { }, (path, channel) -> {
            channel.force(true); forced.add(path);
        });
        assertTrue(forced.contains(failed.get()), "a complete file left by an unsuccessful force must be forced again");
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) { assertEquals(2, pair(runtime, "after").commitSequence()); }
    }

    /** 备份与关闭串行，关闭等待不能提前释放写者锁或打断自有备份文件。 */
    @Test void closeWaitsForBackupAndNewTransactionsAreBackpressured() throws Exception {
        var reached = new CountDownLatch(1); var release = new CountDownLatch(1);
        var runtime = WalMvccRuntime.openForTest(directory.resolve("closing-source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE) { reached.countDown(); await(release); }
        });
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            pair(runtime, "value"); var backup = tasks.submit(() -> runtime.backup(directory.resolve("closing-backup"), WalBackupOptions.defaults()));
            assertTrue(reached.await(30, TimeUnit.SECONDS)); assertTrue(runtime.stats().backupRunning());
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.READ_COMMITTED));
            var closing = tasks.submit(runtime::close); assertFalse(closing.isDone()); release.countDown();
            assertEquals(1, backup.get(30, TimeUnit.SECONDS).sequence()); closing.get(30, TimeUnit.SECONDS);
            assertEquals(1, WalBackup.inspect(directory.resolve("closing-backup"), WalBackupOptions.defaults()).sequence());
        } finally { release.countDown(); runtime.close(); }
    }

    /** 制造一份实际提交后的完整本地备份。 */
    private Path localBackup() {
        Path backup = directory.resolve("local-backup");
        try (var runtime = WalMvccRuntime.open(directory.resolve("local-source"), Set.of("a", "b"), WalOptions.defaults())) {
            pair(runtime, "value"); runtime.backup(backup, WalBackupOptions.defaults());
        }
        return backup;
    }

    /** 使用同一 WAL 决议提交两个分片。 */
    private static WalReceipt pair(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes(value)); runtime.put(tx, "b", "k", bytes(value)); return runtime.commit(tx);
    }
}

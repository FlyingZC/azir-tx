package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 可迁移副本镜像的身份、恢复发布和凭据验证；受控端口不冒充真实共识。 */
class WalReplicaSnapshotTest {
    @TempDir Path directory;

    /** 接收节点使用自己的身份，数据、墓碑和全部确认凭据保留，之后继续相同业务序号。 */
    @Test void snapshotRebindsReceiverWithoutCloningSourceIdentityOrLosingReceipts() throws Exception {
        var source = identity("source"); var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path backup = directory.resolve("backup"); WalReceipt first; WalReceipt second;
        var a = new Authority();
        try (var runtime = open(directory.resolve("source"), source, a)) {
            first = pair(runtime, "old"); second = pair(runtime, "new");
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "deleted"); runtime.delete(deletion, "b", "deleted"); runtime.commit(deletion);
            assertEquals(3, runtime.backupReplica(a, backup, WalBackupOptions.defaults()).sequence()); assertEquals(4, pair(runtime, "later").commitSequence());
        }
        Path target = directory.resolve("target"); WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver);
        assertEquals(receiver, WalReplicaIdentity.read(target)); assertEquals(source, WalReplicaIdentity.read(backup.resolve("local")));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults()));
        var authority = new Authority();
        try (var runtime = open(target, receiver, authority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var view = runtime.beginReadStatement(tx);
            for (String shard : Set.of("a", "b")) {
                assertEquals("new", text(runtime.get(tx, shard, "k", view).orElseThrow())); assertTrue(runtime.get(tx, shard, "deleted", view).isEmpty());
            }
            runtime.rollback(tx); assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow()); assertEquals(second, runtime.findCommitted(second.transactionId()).orElseThrow());
            assertEquals(4, pair(runtime, "receiver-write").commitSequence());
        }
    }

    /** 普通恢复与跨组恢复均在创建目标或接触数据库之前拒绝，不能隐式转换模式。 */
    @Test void wrongModeAndWrongGroupCannotStartRestore() throws Exception {
        var source = identity("source"); Path backup = snapshot(source); Path normal = directory.resolve("normal");
        assertThrows(IOException.class, () -> WalBackup.restore(backup, normal, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults())); assertFalse(Files.exists(normal));
        var wrong = new WalReplicaIdentity(source.engineId(), UUID.randomUUID(), "receiver"); Path target = directory.resolve("wrong-group");
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), wrong)); assertFalse(Files.exists(target));
        Path standalone = directory.resolve("standalone-backup");
        try (var runtime = WalMvccRuntime.open(directory.resolve("standalone"), Set.of("a", "b"), WalOptions.defaults())) { pair(runtime, "plain"); runtime.backup(standalone, WalBackupOptions.defaults()); }
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(standalone, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), source)); assertFalse(Files.exists(target));
    }

    /** 复制镜像内的身份损坏不能靠完整但错误的目录名称绕过校验。 */
    @Test void corruptedSourceIdentityFailsBeforeTargetChanges() throws Exception {
        var source = identity("source"); Path backup = snapshot(source); byte[] bytes = Files.readAllBytes(backup.resolve("local/REPLICA")); bytes[20] ^= 1; Files.write(backup.resolve("local/REPLICA"), bytes);
        Path target = directory.resolve("target"); assertThrows(IOException.class, () -> WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), source));
        assertFalse(Files.exists(target));
    }

    /** 恢复发布后尚未解除标记时不可服务，只允许同一接收身份安全续做。 */
    @Test void publicationCrashCannotServeAndCannotChangeReceiverOnRetry() throws Exception {
        var source = identity("source"); Path backup = snapshot(source); var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver"); Path target = directory.resolve("target");
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreReplicaForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver,
                stage -> { if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_MOVE) { throw new IllegalStateException("injected publication crash"); } }, (path, channel) -> channel.force(true)));
        assertTrue(Files.exists(target.resolve("RESTORING"))); assertThrows(WalMvccException.class, () -> open(target, receiver, new Authority()));
        var other = new WalReplicaIdentity(source.engineId(), source.groupId(), "other");
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), other));
        WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver); assertFalse(Files.exists(target.resolve("RESTORING")));
        try (var runtime = open(target, receiver, new Authority())) { assertEquals(1, runtime.stats().publishedSequence()); }
    }

    /** 前次已复制的完整文件重试仍须刷盘，接收身份本身也必须刷盘后才能解除恢复标记。 */
    @Test void retryForcesExistingFilesAndReceiverIdentity() throws Exception {
        var source = identity("source"); Path backup = snapshot(source); Path target = directory.resolve("target"); var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        var engines = new AtomicInteger(); var replicas = new AtomicInteger(); WalBackup.ForceHook force = (path, channel) -> {
            channel.force(true); if (path.getFileName().toString().equals("ENGINE")) { engines.incrementAndGet(); } if (path.getFileName().toString().equals("REPLICA")) { replicas.incrementAndGet(); }
        };
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreReplicaForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver,
                stage -> { if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_FILE_COPY && engines.get() > 0) { throw new IllegalStateException("copied before crash"); } }, force));
        assertEquals(1, engines.get()); WalBackup.restoreReplicaForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver, stage -> { }, force);
        assertEquals(2, engines.get()); assertEquals(1, replicas.get()); assertEquals(receiver, WalReplicaIdentity.read(target));
    }

    /** 零业务提交但已有复制身份的节点同样可以导出，不能丢失模式保护。 */
    @Test void emptyReplicaSnapshotRestoresAsReplica() throws Exception {
        var source = identity("source"); Path backup = directory.resolve("empty-backup"); var authority = new Authority();
        try (var runtime = open(directory.resolve("source"), source, authority)) {
            assertThrows(IllegalArgumentException.class, () -> runtime.backupReplica(new Authority(), directory.resolve("untrusted"), WalBackupOptions.defaults()));
            assertEquals(0, runtime.backupReplica(authority, backup, WalBackupOptions.defaults()).sequence());
        }
        Path target = directory.resolve("target"); WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), source);
        try (var runtime = open(target, source, new Authority())) { assertEquals(1, pair(runtime, "first").commitSequence()); }
    }

    /** 创建一份完整源镜像，后续测试只能改变自己拥有的文件。 */
    private Path snapshot(WalReplicaIdentity identity) throws Exception {
        Path backup = directory.resolve("backup"); var authority = new Authority();
        try (var runtime = open(directory.resolve("source"), identity, authority)) { pair(runtime, "value"); runtime.backupReplica(authority, backup, WalBackupOptions.defaults()); }
        return backup;
    }

    /** 同组共享业务引擎 UUID，不同接收节点的节点身份保持独立。 */
    static WalReplicaIdentity identity(String node) { UUID group = UUID.randomUUID(); return new WalReplicaIdentity(group, group, node); }

    /** 测试直接装配可信端口，不代表 Raft 投票、选举或多数派已经得到验证。 */
    static WalMvccRuntime open(Path path, WalReplicaIdentity identity, Authority authority) {
        var runtime = WalMvccRuntime.openReplica(path, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority); authority.runtime = runtime; return runtime;
    }

    /** 写完整跨片事务，用实际 WAL 凭据核对序号和 UUID。 */
    static WalReceipt pair(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); for (String shard : Set.of("a", "b")) { runtime.put(tx, shard, "k", bytes(value)); } return runtime.commit(tx);
    }

    /** 仅用于镜像协议的同步已提交回调，真实 Raft 另有独立模块测试。 */
    static final class Authority implements WalCommitAuthority {
        WalMvccRuntime runtime;
        /** 固定单测任期。 */
        @Override public long epoch() { return 1; }
        /** 单测不建立网络连接。 */
        @Override public long readBarrier(long timeoutMillis) { return 1; }
        /** 安装完整批次后才让单测提交完成。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> records) { runtime.applyCommitted(this, records); return CompletableFuture.completedFuture(null); }
    }
}

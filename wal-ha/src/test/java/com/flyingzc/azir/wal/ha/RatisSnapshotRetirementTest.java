package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 完整快照保留数、保护边界、分轮删除和最新损坏拒绝专项。 */
class RatisSnapshotRetirementTest {
    @TempDir Path directory;
    private final RaftGroupId group = RaftGroupId.randomId();

    /** 一轮一个条目也能清理旧数据与孤立 MD5，始终保留最新两份及显式保护边界。 */
    @Test void boundedCleanupPreservesLatestAndProtectedSnapshot() throws Exception {
        Path backup = backup(); try (var storage = storage()) {
            var shelf = new RatisSnapshotStorage(group, limits()); shelf.init(storage); publish(shelf, backup);
            for (int i = 0; i < 10; i++) { var work = new RatisRetirementWork(options()); shelf.cleanup(work, Set.of(10L)); assertTrue(work.removed <= 1); }
            assertEquals(Set.of(10L, 13L, 14L), indices(shelf)); assertEquals(14, shelf.getLatestSnapshot().getIndex());
            for (int i = 0; i < 4; i++) { shelf.cleanup(new RatisRetirementWork(options()), Set.of()); }
            assertEquals(Set.of(13L, 14L), indices(shelf));
        }
    }

    /** 最新文件损坏时，清理不能回退到旧快照后继续删除其他证据。 */
    @Test void corruptNewestStopsCleanupBeforeAnyOldFileDeletion() throws Exception {
        Path backup = backup(); try (var storage = storage()) {
            var shelf = new RatisSnapshotStorage(group, limits()); shelf.init(storage); publish(shelf, backup); Path newest = shelf.getLatestSnapshot().getFiles().getFirst().getPath();
            byte[] bytes = Files.readAllBytes(newest); bytes[bytes.length / 2] ^= 1; Files.write(newest, bytes);
            assertThrows(java.io.IOException.class, () -> shelf.cleanup(new RatisRetirementWork(options()), Set.of())); assertEquals(5, indices(shelf).size());
        }
    }

    /** 活进程已经见过的最新快照消失，不能静默把旧文件重新标记为 latest。 */
    @Test void missingLatestCannotSilentlyRegress() throws Exception {
        Path backup = backup(); try (var storage = storage()) {
            var shelf = new RatisSnapshotStorage(group, limits()); shelf.init(storage); publish(shelf, backup); Path newest = shelf.getLatestSnapshot().getFiles().getFirst().getPath(); Files.delete(newest);
            assertThrows(java.io.IOException.class, () -> shelf.cleanup(new RatisRetirementWork(options()), Set.of())); assertEquals(4, indices(shelf).size()); assertEquals(14, shelf.getLatestSnapshot().getIndex());
        }
    }

    /** 使用正式 Raft 存储目录，位置人为给定，本专项不冒充实际共识生成快照。 */
    private RaftStorage storage() throws Exception { var storage = RaftStorage.newBuilder().setDirectory(directory.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.valueOf("1MB")).build(); storage.initialize(); return storage; }
    /** 连续五份合法封装用于验证保留策略。 */
    private void publish(RatisSnapshotStorage shelf, Path backup) throws Exception { var config = RaftConfigurationProto.newBuilder(); for (int i = 0; i < 3; i++) { config.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto()); } for (int i = 10; i <= 14; i++) { var position = TermIndex.valueOf(3, i); Path bundle = directory.resolve("bundle-" + i); RatisSnapshotBundle.pack(backup, bundle, group, position, 0, config.build(), limits()); shelf.publish(bundle, position); } }
    /** 原始文件计数独立于 shelf 返回的缓存值。 */
    private static Set<Long> indices(RatisSnapshotStorage shelf) throws Exception { var result = new HashSet<Long>(); try (var files = Files.list(shelf.getSnapshotDir().toPath())) { for (Path path : files.toList()) { if (org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches()) { result.add(org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()).getIndex()); } } } return result; }
    /** 真正刷盘的 Root 镜像作为输入，编码实现不使用手造业务文件。 */
    private Path backup() throws Exception { var authority = new Authority(); Path backup = directory.resolve("backup"); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source"); try (var core = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) { authority.core = core; var tx = core.begin(IsolationLevel.REPEATABLE_READ); core.put(tx, "a", "k", new byte[]{1}); core.commit(tx); core.backupReplica(authority, backup, limits()); } return backup; }
    /** 删除上限包含文件及目录项。 */
    private static RatisRetirementOptions options() { return new RatisRetirementOptions(1, 1, 512, 5000, 2); }
    /** 当前输入小型且总量有限。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(16_000_000, 8, 64_000, 60_000); }
    /** 受控端口只提供合法备份输入。 */
    private static final class Authority implements WalCommitAuthority {
        WalMvccRuntime core;
        /** 固定单测任期。 */
        @Override public long epoch() { return 1; }
        /** 不访问网络。 */
        @Override public long readBarrier(long timeout) { return 1; }
        /** 安装完成后才确认。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> records) { core.applyCommitted(this, records); return CompletableFuture.completedFuture(null); }
    }
}

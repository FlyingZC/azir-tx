package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.io.MD5Hash;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 完整镜像与传输摘要真实 I/O 失败、既有文件重试和同位置冲突拒绝。 */
class RatisSnapshotIoTest {
    @TempDir Path directory;
    private final RaftGroupId group = RaftGroupId.randomId();
    /** 操作前后均可能失败，正式镜像名发布前后的重试不同。 */
    enum Failure { PARTIAL_WRITE, BEFORE_DATA_FORCE, AFTER_DATA_FORCE, BEFORE_DIGEST_SAVE, AFTER_DIGEST_SAVE, BEFORE_DIGEST_FORCE, AFTER_DIGEST_FORCE, BEFORE_FIRST_DIR, AFTER_FIRST_DIR, BEFORE_MOVE, AFTER_MOVE, BEFORE_SECOND_DIR, AFTER_SECOND_DIR, BEFORE_PARENT_DIR, AFTER_PARENT_DIR }

    /** 失败不破坏旧完整镜像，重试同一输入必须同步镜像、摘要、目录后才成功。 */
    @ParameterizedTest @EnumSource(Failure.class)
    void incompletePublicationNeverBecomesSuccessfulUntilDurablyRetried(Failure failure) throws Exception {
        Path oldBundle = bundle(9, 1), requested = bundle(10, 2); var position = TermIndex.valueOf(3, 10);
        try (var raft = raft()) {
            var original = new RatisSnapshotStorage(group, limits()); original.init(raft); var old = original.publish(oldBundle, TermIndex.valueOf(3, 9)); byte[] oldBytes = Files.readAllBytes(old.getFile().getPath());
            var broken = new Fault(failure); var shelf = new RatisSnapshotStorage(group, limits(), broken); shelf.init(raft);
            assertThrows(IOException.class, () -> shelf.publish(requested, position)); assertTrue(broken.fired); assertArrayEquals(oldBytes, Files.readAllBytes(old.getFile().getPath()));
            Path target = shelf.getSnapshotDir().toPath().resolve("snapshot.3_10"); boolean present = Files.exists(target); assertEquals(afterMove(failure), present);
            var retry = new Fault(null); var recovered = new RatisSnapshotStorage(group, limits(), retry); recovered.init(raft); var result = recovered.publish(requested, position);
            assertEquals(position, result.getTermIndex()); assertArrayEquals(Files.readAllBytes(requested), Files.readAllBytes(result.getFile().getPath()));
            assertTrue(retry.operations.contains("FORCE_DATA")); assertTrue(retry.operations.contains("FORCE_DIGEST")); assertTrue(retry.directories >= 2);
            if (present) { assertFalse(retry.operations.contains("WRITE")); assertFalse(retry.operations.contains("MOVE")); }
            var reopened = new RatisSnapshotStorage(group, limits()); reopened.init(raft); assertEquals(position, reopened.syncReceived().getTermIndex());
        }
    }
    /** 既有完整镜像存在时再次同步仍失败，不允许 reload 缓存代替 force。 */
    @ParameterizedTest @EnumSource(value = Failure.class, names = {"BEFORE_DATA_FORCE", "AFTER_DATA_FORCE", "BEFORE_DIGEST_FORCE", "AFTER_DIGEST_FORCE", "BEFORE_FIRST_DIR", "AFTER_FIRST_DIR", "BEFORE_SECOND_DIR", "AFTER_SECOND_DIR"})
    void existingSnapshotMustNotSkipAnotherSyncFailure(Failure failure) throws Exception {
        Path requested = bundle(10, 1); var position = TermIndex.valueOf(3, 10);
        try (var raft = raft()) {
            var good = new RatisSnapshotStorage(group, limits()); good.init(raft); good.publish(requested, position);
            var fault = new Fault(failure); var retry = new RatisSnapshotStorage(group, limits(), fault); retry.init(raft); assertThrows(IOException.class, () -> retry.publish(requested, position)); assertTrue(fault.fired);
            assertArrayEquals(Files.readAllBytes(requested), Files.readAllBytes(good.getLatestSnapshot().getFiles().getFirst().getPath()));
        }
    }
    /** 同一位置的另一份有效内容、同索引异任期及低位置不能覆盖或回退已发布镜像。 */
    @Test void validButConflictingInputCannotReplaceSamePositionOrRegressLatest() throws Exception {
        Path first = bundle(10, 1), other = bundle(10, 2), lower = bundle(9, 1), wrongTerm = bundle(10, 3, 4); var position = TermIndex.valueOf(3, 10);
        try (var raft = raft()) {
            var shelf = new RatisSnapshotStorage(group, limits()); shelf.init(raft); var existing = shelf.publish(first, position); byte[] bytes = Files.readAllBytes(existing.getFile().getPath());
            assertThrows(IOException.class, () -> shelf.publish(other, position)); assertThrows(IOException.class, () -> shelf.publish(lower, TermIndex.valueOf(3, 9))); assertThrows(IOException.class, () -> shelf.publish(wrongTerm, TermIndex.valueOf(4, 10))); assertEquals(position, shelf.getLatestSnapshot().getTermIndex()); assertArrayEquals(bytes, Files.readAllBytes(existing.getFile().getPath()));
        }
    }
    /** 正式 Raft 目录初始化，但位置人为给定，不冒充多数派共识证明。 */
    private RaftStorage raft() throws Exception { var value = RaftStorage.newBuilder().setDirectory(directory.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.valueOf("1MB")).build(); value.initialize(); return value; }
    /** 各输入来自真实已提交 Root 备份，内容冲突仍具备合法编码和摘要。 */
    private Path bundle(long index, int value) throws Exception { return bundle(index, value, 3); }
    /** 异任期输入保持合法封装，用于证明拒绝来自位置合同而不是编码错误。 */
    private Path bundle(long index, int value, long term) throws Exception {
        String suffix = index + "-" + value + "-" + term; var authority = new Authority(); Path backup = directory.resolve("backup-" + suffix); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source");
        try (var core = WalMvccRuntime.openReplica(directory.resolve("source-" + suffix), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) { authority.core = core; var tx = core.begin(IsolationLevel.REPEATABLE_READ); core.put(tx, "a", "k", new byte[]{(byte) value}); core.commit(tx); core.backupReplica(authority, backup, limits()); }
        var peers = RaftConfigurationProto.newBuilder(); for (int i = 0; i < 3; i++) { peers.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto()); }
        Path file = directory.resolve("bundle-" + suffix); RatisSnapshotBundle.pack(backup, file, group, TermIndex.valueOf(term, index), 0, peers.build(), limits()); return file;
    }
    /** 有限样例预算，正式数据规模另行验收。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(16_000_000, 8, 64_000, 60_000); }
    /** 原子名称移动之后可能已经留下完整正式镜像。 */
    private static boolean afterMove(Failure failure) { return Set.of(Failure.AFTER_MOVE, Failure.BEFORE_SECOND_DIR, Failure.AFTER_SECOND_DIR, Failure.BEFORE_PARENT_DIR, Failure.AFTER_PARENT_DIR).contains(failure); }
    /** 真实备份输入的受控提交端口，单测不提供分布式多数派。 */
    private static final class Authority implements WalCommitAuthority {
        WalMvccRuntime core;
        /** 固定测试任期。 */
        @Override public long epoch() { return 1; }
        /** 仅用于生成合法备份。 */
        @Override public long readBarrier(long timeout) { return 1; }
        /** 真实安装后确认。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> records) { core.applyCommitted(this, records); return CompletableFuture.completedFuture(null); }
    }
    /** 按文件种类和目录阶段包裹真实操作，仅命中一次预期故障。 */
    private static final class Fault implements RatisSnapshotStorage.Io {
        final Failure failure; final List<String> operations = new ArrayList<>(); boolean fired; int directories;
        /** 无故障实例记录重试是否真正同步。 */
        Fault(Failure failure) { this.failure = failure; }
        /** 操作完成以后失败保留真实副作用。 */
        private void hit(Failure point) throws IOException { if (!fired && failure == point) { fired = true; throw new IOException("injected snapshot IO: " + point); } }
        /** 写出真实前半块后停止。 */
        @Override public void write(FileChannel channel, ByteBuffer bytes) throws IOException { operations.add("WRITE"); if (failure == Failure.PARTIAL_WRITE) { var half = bytes.slice(); half.limit(Math.max(1, half.remaining() / 2)); RatisGenerationRecord.write(channel, half); hit(Failure.PARTIAL_WRITE); } RatisSnapshotStorage.Io.super.write(channel, bytes); }
        /** 分别确认完整镜像内容和传输摘要。 */
        @Override public void forceFile(Path path, FileChannel channel) throws IOException { boolean digest = path.getFileName().toString().endsWith(".md5"); operations.add(digest ? "FORCE_DIGEST" : "FORCE_DATA"); hit(digest ? Failure.BEFORE_DIGEST_FORCE : Failure.BEFORE_DATA_FORCE); RatisSnapshotStorage.Io.super.forceFile(path, channel); hit(digest ? Failure.AFTER_DIGEST_FORCE : Failure.AFTER_DATA_FORCE); }
        /** 摘要生成真实发生，失败后允许留下待清理的孤立摘要。 */
        @Override public void saveDigest(Path target, MD5Hash digest) throws IOException { operations.add("DIGEST_SAVE"); hit(Failure.BEFORE_DIGEST_SAVE); RatisSnapshotStorage.Io.super.saveDigest(target, digest); hit(Failure.AFTER_DIGEST_SAVE); }
        /** 移动前后失败区分正式名称是否已出现。 */
        @Override public void move(Path source, Path target) throws IOException { operations.add("MOVE"); hit(Failure.BEFORE_MOVE); RatisSnapshotStorage.Io.super.move(source, target); hit(Failure.AFTER_MOVE); }
        /** 新发布先确认摘要名，再确认镜像名及父级目录；既有重试确认两个目录。 */
        @Override public void forceDirectory(Path path) throws IOException { directories++; Failure before = directories == 1 ? Failure.BEFORE_FIRST_DIR : directories == 2 ? Failure.BEFORE_SECOND_DIR : Failure.BEFORE_PARENT_DIR; Failure after = directories == 1 ? Failure.AFTER_FIRST_DIR : directories == 2 ? Failure.AFTER_SECOND_DIR : Failure.AFTER_PARENT_DIR; hit(before); RatisSnapshotStorage.Io.super.forceDirectory(path); hit(after); }
    }
}

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import static org.junit.jupiter.api.Assertions.*;

/** 独立验证快照文件的元数据与内容绑定，以及坏输入不能发布可安装目录。 */
class RatisSnapshotBundleTest {
    @TempDir Path directory;
    private final RaftGroupId group = RaftGroupId.randomId();
    private final TermIndex position = TermIndex.valueOf(3, 10);

    /** 完整封装解包后仍能绑定新副本身份恢复，业务序号不能与 Raft 索引混淆。 */
    @Test void bundleBindsRaftPositionConfigurationAndCompleteBusinessState() throws Exception {
        Path backup = backup(); Path bundle = directory.resolve("snapshot.bin"); var configuration = configuration();
        var packed = RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration, limits()); assertEquals(1, packed.boundary().sequence()); assertEquals(10, packed.position().getIndex());
        assertEquals(configuration, RatisSnapshotBundle.inspect(bundle, group, position, limits()).configuration());
        Path extracted = directory.resolve("extracted"); assertEquals(packed, RatisSnapshotBundle.extract(bundle, extracted, group, position, limits()));
        Path restored = directory.resolve("restored"); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "receiver");
        WalBackup.restoreReplica(extracted, restored, Map.of(), WalOptions.defaults(), limits(), identity); var authority = new Authority();
        try (var runtime = WalMvccRuntime.openReplica(restored, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime; var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var view = runtime.beginReadStatement(tx);
            assertEquals("a-value", new String(runtime.get(tx, "a", "k", view).orElseThrow(), StandardCharsets.UTF_8));
            assertEquals("b-value", new String(runtime.get(tx, "b", "k", view).orElseThrow(), StandardCharsets.UTF_8)); runtime.rollback(tx);
        }
    }

    /** 实际安装位置和组来自共识协议，不能接受另一组或同组不同位置的封装。 */
    @Test void wrongGroupPositionAndOccupiedTargetAreRejected() throws Exception {
        Path backup = backup(); Path bundle = directory.resolve("snapshot.bin"); RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), limits());
        Path target = directory.resolve("target");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, RaftGroupId.randomId(), position, limits())); assertFalse(Files.exists(target));
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, TermIndex.valueOf(3, 11), limits())); assertFalse(Files.exists(target));
        Files.createDirectory(target); Files.writeString(target.resolve("user-file"), "keep");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, position, limits())); assertEquals("keep", Files.readString(target.resolve("user-file")));
    }

    /** 截断、数据翻转和尾部多字节都不能通过封装校验，失败不发布目标。 */
    @Test void corruptionTruncationAndTrailingBytesCannotPublish() throws Exception {
        Path backup = backup(); Path bundle = directory.resolve("snapshot.bin"); RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), limits()); byte[] good = Files.readAllBytes(bundle);
        var corrupt = good.clone(); corrupt[corrupt.length / 2] ^= 1;
        for (byte[] bytes : List.of(Arrays.copyOf(good, 24), Arrays.copyOf(good, good.length - 1), Arrays.copyOf(good, good.length + 1), corrupt)) {
            Files.write(bundle, bytes); Path target = directory.resolve("rejected-" + UUID.randomUUID());
            assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, position, limits())); assertFalse(Files.exists(target));
        }
    }

    /** 即使重新计算外层摘要，路径穿越和伪造业务边界仍会被格式及内层镜像校验拒绝。 */
    @Test void recomputedChecksumCannotAuthorizeTraversalOrFalseBusinessBoundary() throws Exception {
        Path backup = backup(); Path bundle = directory.resolve("snapshot.bin"); RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), limits()); byte[] good = Files.readAllBytes(bundle);
        byte[] traversal = good.clone(); byte[] token = "COMPLETE".getBytes(StandardCharsets.US_ASCII); int offset = -1;
        for (int i = 0; i <= traversal.length - token.length; i++) { if (Arrays.equals(traversal, i, i + token.length, token, 0, token.length)) { offset = i; break; } }
        assertTrue(offset > 0); System.arraycopy("../evil!".getBytes(StandardCharsets.US_ASCII), 0, traversal, offset, token.length); checksum(traversal); Files.write(bundle, traversal);
        Path target = directory.resolve("traversal-target"); assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, position, limits())); assertFalse(Files.exists(target)); assertFalse(Files.exists(directory.resolve("evil!")));
        byte[] forged = good.clone(); int configurationLength = ByteBuffer.wrap(forged).getInt(48); int sequenceOffset = 52 + configurationLength + 16;
        ByteBuffer.wrap(forged).putLong(sequenceOffset, 999); checksum(forged); Files.write(bundle, forged);
        Path wrong = directory.resolve("wrong-sequence"); assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, wrong, group, position, limits())); assertFalse(Files.exists(wrong));
        try (var paths = Files.list(directory)) { assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith(".azir-unpack-"))); }
    }

    /** 编码文件头和配置也消耗字节预算；资源拒绝不覆盖原文件或影响源备份。 */
    @Test void limitsUnknownFilesAndInvalidConfigurationFailClosed() throws Exception {
        Path backup = backup(); Path bundle = directory.resolve("snapshot.bin");
        long bytes = WalBackup.inspect(backup, limits()).bytes(); assertTrue(bytes >= 1024);
        assertThrows(IOException.class, () -> RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), new WalBackupOptions(bytes + 1, 16, 1024, 60_000)));
        assertTrue(Files.exists(bundle)); assertThrows(IOException.class, () -> RatisSnapshotBundle.inspect(bundle, group, position, limits()));
        assertEquals(1, WalBackup.inspect(backup, limits()).sequence());
        Path invalid = directory.resolve("invalid.bin"); assertThrows(IllegalArgumentException.class, () -> RatisSnapshotBundle.pack(backup, invalid, group, position, 11, configuration(), limits())); assertFalse(Files.exists(invalid));
        Files.writeString(backup.resolve("local/unknown-file"), "must not travel");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.pack(backup, invalid, group, position, 0, configuration(), limits())); assertFalse(Files.exists(invalid));
    }

    /** 创建经过真实 WAL force 和备份校验的副本镜像；固定端口仅编码配置，不监听。 */
    private Path backup() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source"); var authority = new Authority(); Path backup = directory.resolve("backup");
        try (var runtime = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime; var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", "a-value".getBytes(StandardCharsets.UTF_8)); runtime.put(tx, "b", "k", "b-value".getBytes(StandardCharsets.UTF_8)); runtime.commit(tx);
            runtime.backupReplica(authority, backup, limits());
        }
        return backup;
    }

    /** 成员配置使用 Ratis 自身 protobuf，保留可供成员变更恢复的字段。 */
    private static RaftConfigurationProto configuration() {
        var builder = RaftConfigurationProto.newBuilder();
        for (int i = 0; i < 3; i++) { builder.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto()); }
        return builder.build();
    }

    /** 单次维护字节和时间限制，不用压缩包展开倍数突破总量预算。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(8_000_000, 16, 64_000, 60_000); }

    /** 负向测试模拟有合法外层摘要的恶意发送者，不能只测随机位翻转。 */
    private static void checksum(byte[] bytes) throws Exception { byte[] hash = java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes, bytes.length - 32)); System.arraycopy(hash, 0, bytes, bytes.length - 32, 32); }

    /** 单元测试的已提交端口仅提供镜像输入，不是生产共识实现。 */
    private static final class Authority implements WalCommitAuthority {
        WalMvccRuntime runtime;
        /** 固定单测任期。 */
        @Override public long epoch() { return 1; }
        /** 固定单测屏障。 */
        @Override public long readBarrier(long timeoutMillis) { return 1; }
        /** 先安装完整批次再完成单测提交。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> commits) { runtime.applyCommitted(this, commits); return CompletableFuture.completedFuture(null); }
    }
}

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
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
import static org.junit.jupiter.api.Assertions.*;

/** 错误组目录不能代替本组投票状态，缺失状态时必须在启动服务器前失败。 */
class RatisNodeIdentityTest {
    @TempDir Path directory;

    /** 即使别的组存在同名 raft-meta，也不能重新格式化已经运行过的本组投票身份。 */
    @Test void anotherGroupsMetadataCannotAuthorizeRecoveringAnExistingReplica() throws Exception {
        var group = group(); var identity = new WalReplicaIdentity(group.getGroupId().getUuid(), group.getGroupId().getUuid(), "n0");
        try (var ignored = WalMvccRuntime.openReplica(directory.resolve("wal"), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, new Unavailable())) { }
        byte[] engine = Files.readAllBytes(directory.resolve("wal/ENGINE"));
        Path wrong = directory.resolve("raft").resolve(UUID.randomUUID().toString()).resolve("current/raft-meta"); Files.createDirectories(wrong.getParent()); Files.writeString(wrong, "unrelated");
        assertThrows(IOException.class, () -> open(group)); assertArrayEquals(engine, Files.readAllBytes(directory.resolve("wal/ENGINE")));
        assertFalse(Files.exists(directory.resolve("raft").resolve(group.getGroupId().getUuid().toString())));
    }

    /** 只剩日志却没有投票元数据的旧目录，不能当成全新副本重新投票。 */
    @Test void orphanRaftFilesAreNotAValidFreshNode() throws Exception {
        var group = group(); Path segment = directory.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current/log_inprogress_0");
        Files.createDirectories(segment.getParent()); Files.writeString(segment, "old-log"); assertThrows(IOException.class, () -> open(group));
        assertFalse(Files.exists(directory.resolve("wal"))); assertEquals("old-log", Files.readString(segment));
    }

    /** 保留投票状态却丢失业务 WAL，同样拒绝把空业务状态作为正常副本服务。 */
    @Test void votingStateWithoutMvccCannotCreateAnEmptyEngine() throws Exception {
        var group = group(); Path metadata = directory.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta");
        Files.createDirectories(metadata.getParent()); Files.writeString(metadata, "guard-only"); assertThrows(IOException.class, () -> open(group));
        assertFalse(Files.exists(directory.resolve("wal")));
    }

    /** 路径链接不能把其他目录的投票状态借给当前副本。 */
    @Test void linkedVotingGroupDirectoryIsRejected() throws Exception {
        var group = group(); Path other = directory.resolve("other/current"); Files.createDirectories(other); Files.writeString(other.resolve("raft-meta"), "guard-only");
        Files.createDirectories(directory.resolve("raft")); Files.createSymbolicLink(directory.resolve("raft").resolve(group.getGroupId().getUuid().toString()), directory.resolve("other"));
        assertThrows(IOException.class, () -> open(group)); assertFalse(Files.exists(directory.resolve("wal")));
    }

    /** 错误现场必须在任何网络监听之前被检测，端口值只满足组配置格式。 */
    private RatisWalNode open(RaftGroup group) throws IOException { return RatisWalNode.openLoopback(directory, RaftPeerId.valueOf("n0"), group, Set.of("a"), Map.of(), WalOptions.defaults()); }

    /** 所有错误场景使用独立组身份，避免同一投票身份跨测试复用。 */
    private static RaftGroup group() {
        return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), List.of(
                RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:19001").build(),
                RaftPeer.newBuilder().setId("n1").setAddress("127.0.0.1:19002").build(),
                RaftPeer.newBuilder().setId("n2").setAddress("127.0.0.1:19003").build()));
    }

    /** 仅创建持久模式标记，不在本测试模拟任何真实多数派能力。 */
    private static final class Unavailable implements WalCommitAuthority {
        /** 没有领导者。 */
        @Override public long epoch() { return 0; }
        /** 错误目录测试不建立业务快照。 */
        @Override public long readBarrier(long timeoutMillis) throws IOException { throw new IOException("not running"); }
        /** 错误目录测试不允许提交。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> commits) { return CompletableFuture.failedFuture(new IOException("not running")); }
    }
}

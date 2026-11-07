package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 验证复制内核消费共识决议的边界；此受控端口不是生产 Raft 实现。 */
class WalReplicaCoreTest {
    @TempDir Path directory;

    /** 没有已提交回调时不写本地提交日志，不发布也不返回业务成功。 */
    @Test void onlyCommittedApplicationPublishesAndAcknowledgesWholeBatch() throws Exception {
        try (var cluster = new Cluster(directory); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var leader = cluster.nodes.getFirst(); var tx = pair(leader, "value");
            var future = tasks.submit(() -> leader.commit(tx)); assertTrue(cluster.submitted.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone()); for (var node : cluster.nodes) { assertEquals(0, node.stats().publishedSequence()); assertEquals(60, node.stats().walBytes()); }
            cluster.applyPending(); assertEquals(1, future.get(5, TimeUnit.SECONDS).commitSequence());
            for (var node : cluster.nodes) { assertEquals(1, node.stats().publishedSequence()); }
            cluster.moveLeader(1); var reader = cluster.nodes.get(1).begin(IsolationLevel.REPEATABLE_READ); var snapshot = cluster.nodes.get(1).beginReadStatement(reader);
            for (String shard : Set.of("a", "b")) { assertEquals("value", text(cluster.nodes.get(1).get(reader, shard, "k", snapshot).orElseThrow())); }
            cluster.nodes.get(1).rollback(reader); assertThrows(WalMvccException.class, () -> leader.begin(IsolationLevel.READ_COMMITTED));
        }
    }

    /** 共识等待失败不是回滚；晚到的已提交回调可解析 UUID，期间不能复用业务序号。 */
    @Test void unresolvedSubmissionBlocksNewDecisionsUntilCommittedResolution() throws Exception {
        try (var cluster = new Cluster(directory); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var leader = cluster.nodes.getFirst(); var tx = pair(leader, "late"); var future = tasks.submit(() -> leader.commit(tx));
            assertTrue(cluster.submitted.await(5, TimeUnit.SECONDS)); cluster.response.completeExceptionally(new IllegalStateException("lost consensus response"));
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ((WalMvccException) failure.getCause()).errorCode());
            waitFor(() -> leader.lifecycle().proposalBlocked()); assertEquals(0, leader.stats().publishedSequence());
            assertThrows(WalMvccException.class, () -> leader.begin(IsolationLevel.READ_COMMITTED));
            cluster.applyPending(); assertFalse(leader.lifecycle().proposalBlocked()); assertTrue(leader.findCommitted(tx.id()).isPresent());
            cluster.response = new CompletableFuture<>(); cluster.submitted = new CountDownLatch(1);
            var next = pair(leader, "next"); var committing = tasks.submit(() -> leader.commit(next)); assertTrue(cluster.submitted.await(5, TimeUnit.SECONDS));
            cluster.applyPending(); assertEquals(2, committing.get(5, TimeUnit.SECONDS).commitSequence());
        }
    }

    /** 任期变化退休旧句柄，延迟通知不能误清理新任期事务。 */
    @Test void fencingInvalidatesOldTransactionsAndPreservesNewEpochHandles() throws Exception {
        try (var cluster = new Cluster(directory)) {
            var old = cluster.nodes.getFirst().begin(IsolationLevel.REPEATABLE_READ); cluster.moveLeader(1);
            assertThrows(WalMvccException.class, () -> cluster.nodes.getFirst().get(old, "a", "k"));
            var current = cluster.nodes.get(1).begin(IsolationLevel.REPEATABLE_READ); cluster.nodes.get(1).fenceEpoch(1);
            assertTrue(cluster.nodes.get(1).get(current, "a", "k").isEmpty()); cluster.nodes.get(1).rollback(current);
        }
    }

    /** 模式、组及节点身份持久绑定；普通入口和另一个节点都不能接管目录。 */
    @Test void replicaModeAndIdentitySurviveCheckpointAndRejectStandaloneOpen() throws Exception {
        Path root = directory.resolve("identity"); var authority = new Immediate(); var identity = new WalReplicaIdentity(UUID.randomUUID(), UUID.randomUUID(), "node1");
        try (var runtime = WalMvccRuntime.openReplica(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime; runtime.commit(pair(runtime, "persisted")); assertTrue(runtime.checkpoint().created());
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(root, Set.of("a", "b"), WalOptions.defaults()));
        var wrong = new WalReplicaIdentity(identity.engineId(), identity.groupId(), "node2");
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.openReplica(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), wrong, authority));
        try (var runtime = WalMvccRuntime.openReplica(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime; assertEquals(1, runtime.stats().publishedSequence()); assertEquals(2, runtime.commit(pair(runtime, "after")).commitSequence());
            assertThrows(WalMvccException.class, () -> runtime.backup(directory.resolve("unsafe-replica-backup"), WalBackupOptions.defaults()));
        }
    }

    /** 丢失初始化目录的持久入口必须拒绝，不能把原副本误创建为零序号。 */
    @Test void replicaIdentityWithoutWalIsNotAnEmptyEngine() throws Exception {
        Path root = directory.resolve("missing"); var authority = new Immediate(); var identity = new WalReplicaIdentity(UUID.randomUUID(), UUID.randomUUID(), "node");
        try (var runtime = WalMvccRuntime.openReplica(root, Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) { authority.runtime = runtime; }
        Files.delete(root.resolve("ENGINE")); Files.delete(root.resolve("transactions.wal"));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.openReplica(root, Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority));
        assertFalse(Files.exists(root.resolve("transactions.wal")));
    }

    /** 重放匹配决议幂等，内容不同或序号跳跃时停止本副本而不泄露部分状态。 */
    @Test void committedReplayMustMatchExactDecision() throws Exception {
        var authority = new Immediate(); var identity = new WalReplicaIdentity(UUID.randomUUID(), UUID.randomUUID(), "node");
        try (var runtime = WalMvccRuntime.openReplica(directory, Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime; var record = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value"))));
            runtime.applyCommitted(authority, List.of(record)); runtime.applyCommitted(authority, List.of(record)); assertEquals(1, runtime.stats().publishedSequence());
            var bad = new WalCommit(record.transactionId(), 1, 0, List.of(new WalMutation("a", "k", bytes("wrong"))));
            assertThrows(WalMvccException.class, () -> runtime.applyCommitted(authority, List.of(bad))); assertNotNull(runtime.lifecycle().fatal());
        }
    }

    /** 构造同一事务的完整跨片写集。 */
    private static WalTransaction pair(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes(value)); runtime.put(tx, "b", "k", bytes(value)); return tx;
    }

    /** 受控端口允许测试分开观察提交请求和已提交回调。 */
    private static final class Cluster implements AutoCloseable {
        final List<WalMvccRuntime> nodes = new ArrayList<>(); final List<Authority> authorities = new ArrayList<>();
        volatile int leader; volatile long term = 1; volatile CountDownLatch submitted = new CountDownLatch(1);
        volatile CompletableFuture<Void> response = new CompletableFuture<>(); volatile List<WalCommit> pending;

        /** 每个副本拥有独立目录，共享逻辑引擎身份。 */
        Cluster(Path directory) {
            UUID engine = UUID.randomUUID(); UUID group = UUID.randomUUID();
            for (int i = 0; i < 3; i++) {
                var authority = new Authority(this, i); authorities.add(authority);
                nodes.add(WalMvccRuntime.openReplica(directory.resolve("node" + i), Set.of("a", "b"), Map.of(), WalOptions.defaults().withTimeout(5000),
                        WalRetentionOptions.preserveAll(), new WalReplicaIdentity(engine, group, "node" + i), authority));
            }
        }

        /** 模拟已由可信端口决定提交的批次，先安装两个副本再安装领导者。 */
        void applyPending() {
            for (int i = 0; i < 3; i++) { if (i != leader) { nodes.get(i).applyCommitted(authorities.get(i), pending); } }
            nodes.get(leader).applyCommitted(authorities.get(leader), pending); response.complete(null);
        }

        /** 模拟角色通知，不代表实现了生产选举协议。 */
        void moveLeader(int next) { long old = term; leader = next; term++; for (var node : nodes) { node.fenceEpoch(old); } }

        /** 先释放未完成请求，避免测试故障遗留等待线程。 */
        @Override public void close() { response.completeExceptionally(new IllegalStateException("test closed")); for (var node : nodes) { node.close(); } }
    }

    /** 每个内核使用独立的能力对象，不能用另一个节点的端口安装。 */
    private record Authority(Cluster cluster, int node) implements WalCommitAuthority {
        /** 本地角色检查不阻塞。 */
        @Override public long epoch() { return cluster.leader == node ? cluster.term : 0; }
        /** 测试的受控屏障，真实网络屏障在 Ratis 集成中另验。 */
        @Override public long readBarrier(long timeoutMillis) { return epoch(); }
        /** 保存候选批次，父测试显式决定何时回调安装。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> commits) {
            if (epoch != epoch()) { return CompletableFuture.failedFuture(new IllegalStateException("not leader")); }
            cluster.pending = commits; cluster.submitted.countDown(); return cluster.response;
        }
    }

    /** 同步回调检查核心没有在共识等待时持有不可重入的跨线程安装锁。 */
    private static final class Immediate implements WalCommitAuthority {
        WalMvccRuntime runtime;
        /** 固定测试任期。 */
        @Override public long epoch() { return 1; }
        /** 返回固定已验证测试任期。 */
        @Override public long readBarrier(long timeoutMillis) { return 1; }
        /** 安装完整记录后才确认。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> commits) { runtime.applyCommitted(this, commits); return CompletableFuture.completedFuture(null); }
    }
}

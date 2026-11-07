package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 独立后端探测、并发物化水位和应用失败后的水位冻结验证。 */
class RatisBackendProbeTest {
    @TempDir Path directory;
    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();
    /** 全类仅启动一个受限实例，逻辑副本仍有独立数据空间。 */
    @BeforeAll static void startDatabase() { DATABASE.start(); }
    /** 只停止本测试拥有的实例。 */
    @AfterAll static void stopDatabase() { DATABASE.stop(); }

    /** 健康 JDBC 等待不能挡住同一节点复制应用，超出探测预算也不能返回健康。 */
    @Test void slowDiagnosticDoesNotBlockCommittedApplication() throws Exception {
        try (var cluster = new Cluster(directory); var tasks = Executors.newVirtualThreadPerTaskExecutor(); var client = cluster.client()) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); cluster.write(node, 1); cluster.materialized(node, 1); cluster.gate.arm("n" + leader);
            var diagnostic = tasks.submit(() -> client.io().sendStaleRead(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisReplicaStatus.request())), 0, cluster.peers.get(leader).getId()));
            try {
                assertTrue(cluster.gate.entered.await(5, TimeUnit.SECONDS));
                var write = tasks.submit(() -> cluster.write(node, 2)); assertEquals(2, write.get(3, TimeUnit.SECONDS).commitSequence()); assertFalse(diagnostic.isDone());
                TimeUnit.MILLISECONDS.sleep(1100);
            } finally { cluster.gate.release.countDown(); }
            var reply = diagnostic.get(10, TimeUnit.SECONDS); assertTrue(reply.isSuccess()); assertFalse(RatisReplicaStatus.decode(reply.getMessage().getContent().toByteArray()).healthy());
            var fresh = client.io().sendStaleRead(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisReplicaStatus.request())), 0, cluster.peers.get(leader).getId()); assertTrue(RatisReplicaStatus.decode(fresh.getMessage().getContent().toByteArray()).healthy());
        }
    }

    /** 查询期间物化推进可以高于旧发布取样，必须用查询后的发布上界核对。 */
    @Test void concurrentMaterializationUsesPublicationAfterDatabaseRead() throws Exception {
        try (var cluster = new Cluster(directory); var worker = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("wal-health-probe-watermark").factory())) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); cluster.write(node, 1); cluster.materialized(node, 1); cluster.gate.arm("n" + leader);
            var state = state(node); var health = worker.submit(() -> state.backendHealthy(state.core(), 10_000));
            try { assertTrue(cluster.gate.entered.await(5, TimeUnit.SECONDS)); cluster.write(node, 2); cluster.write(node, 3); cluster.materialized(node, 3); }
            finally { cluster.gate.release.countDown(); }
            assertTrue(health.get(5, TimeUnit.SECONDS));
        }
    }

    /** 已提交日志应用失败以后，后续 no-op 不得把可观测应用水位跳过失败记录。 */
    @Test void failedApplicationFreezesFollowingIndexNotifications() throws Exception {
        UUID group = UUID.randomUUID(); var identity = new WalReplicaIdentity(group, group, "unit");
        var source = new WalCommitAuthority() {
            /** 仅提供复制模式关闭所需的合法任期。 */
            @Override public long epoch() { return 1; }
            /** 此专项没有业务事务，不发送网络请求。 */
            @Override public long readBarrier(long millis) { return 1; }
            /** 不用伪造多数派来验证畸形已提交记录。 */
            @Override public CompletionStage<Void> submit(long term, List<WalCommit> commits) { return CompletableFuture.failedFuture(new UnsupportedOperationException()); }
        };
        try (var core = WalMvccRuntime.openReplica(directory.resolve("wal"), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, source); var state = new RatisWalState(core); var machine = new RatisWalStateMachine(identity, state, null, WalBackupOptions.defaults())) {
            var entry = org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(2).setIndex(4).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(new byte[]{1}))).build();
            var context = org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(machine).setLogEntry(entry).build();
            assertThrows(ExecutionException.class, () -> machine.applyTransaction(context).get(5, TimeUnit.SECONDS)); assertTrue(machine.failed());
            var before = machine.getLastAppliedTermIndex(); machine.notifyTermIndexUpdated(2, 5); machine.query(Message.valueOf("drain")).get(5, TimeUnit.SECONDS); assertEquals(before, machine.getLastAppliedTermIndex());
            var status = RatisReplicaStatus.decode(machine.query(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisReplicaStatus.request()))).get(5, TimeUnit.SECONDS).getContent().toByteArray()); assertFalse(status.healthy());
        }
    }
    /** 包内验收只读取实际节点的代次管理器，不替代任何生产调用。 */
    private static RatisWalState state(RatisWalNode node) throws Exception { var field = RatisWalNode.class.getDeclaredField("state"); field.setAccessible(true); return (RatisWalState) field.get(node); }

    /** 可控暂停只影响指定节点的探测线程，不暂停数据库或复制线程。 */
    private static final class Gate {
        volatile String owner; volatile CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        /** 每次试验重建同步边界。 */
        void arm(String owner) { this.owner = owner; entered = new CountDownLatch(1); release = new CountDownLatch(1); }
        /** 真实 checkpoint 查询开始以前暂停。 */
        void hit(String owner) { if (!owner.equals(this.owner) || !Thread.currentThread().getName().startsWith("wal-health-probe")) { return; } this.owner = null; entered.countDown(); try { if (!release.await(15, TimeUnit.SECONDS)) { throw new IllegalStateException("probe gate observation expired"); } } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); } }
    }

    /** 三个真实 Ratis 节点与独立 MySQL 命名空间，受控包装只改变查询等待。 */
    private static final class Cluster implements AutoCloseable {
        final Gate gate = new Gate(); final List<RatisWalNode> nodes = new ArrayList<>(); final List<org.apache.ratis.protocol.RaftPeer> peers = new ArrayList<>(); final HikariDataSource control; final RaftGroup group;
        /** 每个场景新控制库和组身份，避免继承另一场景的已用空间。 */
        Cluster(Path root) throws Exception {
            String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASE, schema); control = MysqlGenerationSupport.control(DATABASE, schema); var reservations = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); reservations.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } }
            finally { for (var socket : reservations) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); var base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            WalReplicaBackendFactory factory = (identity, generation) -> { var lease = base.open(identity, generation); var stores = new HashMap<String, WalMaterializedStore>(); lease.stores().forEach((shard, store) -> stores.put(shard, new ProbeStore(store, identity.nodeId(), gate))); return new WalReplicaBackendFactory.Lease(stores, lease::close); };
            try { for (int i = 0; i < 3; i++) { nodes.add(RatisWalNode.openGenerationalLoopback(root.resolve("n" + i), peers.get(i).getId(), group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(5000), new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000))); } }
            catch (Exception failure) { close(); throw failure; }
        }
        /** 诊断有独立客户端等待期限，不依赖测试固定睡眠判断完成。 */
        RaftClient client() { var properties = new org.apache.ratis.conf.RaftProperties(); org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(properties, org.apache.ratis.util.TimeDuration.valueOf(10, TimeUnit.SECONDS)); return RaftClient.newBuilder().setRaftGroup(group).setProperties(properties).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build(); }
        /** 真实角色与读屏障共同确认领导者。 */
        int leader() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (System.nanoTime() < until) { for (int i = 0; i < nodes.size(); i++) { if (nodes.get(i).leaderEpoch() > 0) { var tx = nodes.get(i).begin(IsolationLevel.REPEATABLE_READ); nodes.get(i).rollback(tx); return i; } } TimeUnit.MILLISECONDS.sleep(20); } throw new AssertionError("leader missing"); }
        /** 两片完整写入同一事务。 */
        WalReceipt write(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { node.put(tx, shard, "k", (shard + value).getBytes(StandardCharsets.UTF_8)); } return node.commit(tx); }
        /** 等到真实数据库物化前缀推进以后才放行检查。 */
        void materialized(RatisWalNode node, long sequence) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10); while (node.localStats().materializedSequences().values().stream().anyMatch(value -> value < sequence) && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(10); } assertTrue(node.localStats().materializedSequences().values().stream().allMatch(value -> value >= sequence)); }
        /** 异常也解除本测试门闩，再关闭全部拥有的节点和控制池。 */
        @Override public void close() throws Exception { gate.release.countDown(); Exception failure = null; for (var node : nodes) { try { node.close(); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } control.close(); if (failure != null) { throw failure; } }
    }

    /** 只在后端探测线程暂停 checkpoint，其他数据与恢复操作委托真实 MySQL。 */
    private record ProbeStore(WalMaterializedStore delegate, String owner, Gate gate) implements WalMaterializedStore {
        /** 保留正式后端容量校验。 */
        @Override public void validateMutation(WalMutation mutation) { delegate.validateMutation(mutation); }
        /** 保留普通绑定拒绝规则。 */
        @Override public void initialize(UUID engine, String shard) { delegate.initialize(engine, shard); }
        /** 保留复制能力声明。 */
        @Override public boolean supportsReplicaOwnership() { return delegate.supportsReplicaOwnership(); }
        /** 保留接收副本所有权。 */
        @Override public void initializeReplica(WalReplicaIdentity identity, String shard) { delegate.initializeReplica(identity, shard); }
        /** 直接查询真实物化前缀。 */
        @Override public WalCheckpoint checkpoint() { gate.hit(owner); return delegate.checkpoint(); }
        /** 直接应用真实事务。 */
        @Override public WalCheckpoint apply(List<WalCommit> commits) { return delegate.apply(commits); }
        /** 不引入测试数据缓存。 */
        @Override public Optional<WalValue> get(String key, long sequence) { return delegate.get(key, sequence); }
        /** 保留真实写冲突查询。 */
        @Override public long latestSequence(String key) { return delegate.latestSequence(key); }
        /** 保留真实范围查询。 */
        @Override public NavigableMap<String, WalValue> scan(String from, String to, long sequence) { return delegate.scan(from, to, sequence); }
        /** 保留真实后端回收。 */
        @Override public int gc(long safe, int keys) { return delegate.gc(safe, keys); }
        /** 保留有界维护能力。 */
        @Override public boolean supportsBoundedMaintenance() { return delegate.supportsBoundedMaintenance(); }
        /** 保留真实有界回收。 */
        @Override public GcProgress gcBounded(long safe, int keys, int rows, long millis) { return delegate.gcBounded(safe, keys, rows, millis); }
        /** 连接和持久性校验仍由正式适配器执行。 */
        @Override public void validateProduction() { delegate.validateProduction(); }
        /** 导出仍使用真实流式实现。 */
        @Override public boolean supportsBackup() { return delegate.supportsBackup(); }
        /** 并发快照能力由实际后端提供。 */
        @Override public boolean supportsPinnedSnapshot() { return delegate.supportsPinnedSnapshot(); }
        /** 固定真实后端的只读事务，不用包装器模拟快照。 */
        @Override public SnapshotView openSnapshot(WalCheckpoint boundary) { return delegate.openSnapshot(boundary); }
        /** 恢复所有权能力不改变。 */
        @Override public boolean supportsReplicaRestore() { return delegate.supportsReplicaRestore(); }
        /** 导出真实冷数据。 */
        @Override public void exportSnapshot(WalCheckpoint boundary, BackupSink sink) throws IOException { delegate.exportSnapshot(boundary, sink); }
        /** 恢复隔离标记由正式实现持久化。 */
        @Override public boolean beginRestore(WalRestoreSpec spec) { return delegate.beginRestore(spec); }
        /** 明确在真实批次提交完成之后停住。 */
        @Override public void restoreBatch(WalRestoreSpec spec, List<WalBackupRow> rows) { delegate.restoreBatch(spec, rows); }
        /** 完整数据摘要与前缀发布仍由正式实现核对。 */
        @Override public void finishRestore(WalRestoreSpec spec, long rows, String hash) { delegate.finishRestore(spec, rows, hash); }
    }
}

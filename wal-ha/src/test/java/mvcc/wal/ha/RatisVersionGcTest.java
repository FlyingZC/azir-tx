package mvcc.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import mvcc.common.ErrorCode;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 MySQL 的副本独立安全点、长读保护、切主、策略重启及回收故障验证。 */
class RatisVersionGcTest {
    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();
    static final RatisGcOptions POLICY = new RatisGcOptions(100, 2, 2, 100, 5000);
    @TempDir Path directory;
    /** 单个受限实例内分配六份独立逻辑后端，测试版本语义而非主机容灾。 */
    @BeforeAll static void startDatabase() { DATABASE.start(); }
    /** 只释放本测试持有的数据库容器。 */
    @AfterAll static void stopDatabase() { DATABASE.stop(); }

    /** 领导者长 RR 保留旧锚点，跟随者独立回收；释放后实际 SQL 行数收敛并回收墓碑。 */
    @Test void longReaderPinsLeaderWhileFollowersReclaimAndConflictsRemainCorrect() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var first = cluster.write(node, 1);
            var reader = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(reader);
            var staleWriter = node.begin(IsolationLevel.REPEATABLE_READ); cluster.enable();
            for (int value = 2; value <= 8; value++) { cluster.write(node, value); } cluster.materialized(8);
            await(() -> node.gcStats().rounds() >= 2);
            assertTrue(node.gcStats().safePoint() <= reader.startSequence()); cluster.verify(node, reader, view, 1);
            for (int index = 0; index < 3; index++) { if (index != leader) { final int follower = index; await(() -> cluster.rows(follower) == 2 && cluster.nodes.get(follower).gcStats().safePoint() == 8); } }
            assertEquals(16, cluster.rows(leader)); node.put(staleWriter, "a", "k", bytes("stale"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> node.commit(staleWriter)).errorCode());
            cluster.verify(node, reader, view, 1); node.rollback(reader);
            await(() -> cluster.rows(leader) == 2 && node.gcStats().safePoint() == 8); cluster.verify(node, 8);
            var deleted = node.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { node.delete(deleted, shard, "k"); } node.commit(deleted); cluster.materialized(9);
            await(() -> cluster.rows(leader) == 0); var empty = node.begin(IsolationLevel.REPEATABLE_READ); var emptyView = node.beginReadStatement(empty);
            for (String shard : List.of("a", "b")) { assertTrue(node.get(empty, shard, "k", emptyView).isEmpty()); } node.rollback(empty);
            assertEquals(10, cluster.write(node, 10).commitSequence()); cluster.verify(node, 10); assertEquals(first, node.lookupCommit(first.transactionId()).receipt());
        }
    }

    /** 跟随者已回收旧历史后切主，只能创建新快照；原节点重启自动恢复 GC 策略。 */
    @Test void failoverAndRestartDoNotResurrectOldSnapshotHandles() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(); var old = cluster.nodes.get(first); var receipt = cluster.write(old, 1); var reader = old.begin(IsolationLevel.REPEATABLE_READ); var snapshot = old.beginReadStatement(reader);
            cluster.enable(); for (int value = 2; value <= 6; value++) { cluster.write(old, value); } cluster.materialized(6);
            for (int index = 0; index < 3; index++) { if (index != first) { final int follower = index; await(() -> cluster.rows(follower) == 2); } }
            cluster.stop(first); int second = cluster.leader(); var next = cluster.nodes.get(second); cluster.verify(next, 6);
            assertThrows(WalMvccException.class, () -> next.get(reader, "a", "k", snapshot)); var newer = cluster.write(next, 7);
            cluster.start(first); assertTrue(cluster.nodes.get(first).gcStats().enabled()); await(() -> cluster.nodes.get(first).ready() && cluster.nodes.get(first).gcStats().publishedSequence() >= 7);
            assertEquals(receipt, next.lookupCommit(receipt.transactionId()).receipt()); assertEquals(newer, next.lookupCommit(newer.transactionId()).receipt()); cluster.verify(next, 7);
        }
    }

    /** 回收错误可观测且脱敏，不能丢失健康数据；故障解除后继续真实有界回收。 */
    @Test void maintenanceFailuresAreVisibleAndRetryWithoutPublishingSecrets() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); cluster.write(node, 1); cluster.write(node, 2); cluster.materialized(2);
            cluster.gcFailure.set(true); cluster.enable(); await(() -> node.gcStats().failures() > 0);
            assertEquals("IllegalStateException", node.gcStats().error()); assertFalse(node.gcStats().toString().contains("private-password")); cluster.verify(node, 2); assertEquals(4, cluster.rows(leader));
            cluster.gcFailure.set(false); await(() -> cluster.rows(leader) == 2 && node.gcStats().error() == null && !node.gcStats().stale()); cluster.verify(node, 2);
        }
    }

    /** 纯内存副本也实际回收旧版本和墓碑，长 RR 释放前不能删掉其锚点。 */
    @Test void localVersionChainsAreBoundedAfterReaderReleaseAndDelete() throws Exception {
        try (var cluster = new Cluster(directory, true)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var receipt = cluster.write(node, 1);
            var reader = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(reader); cluster.enable();
            for (int value = 2; value <= 12; value++) { cluster.write(node, value); }
            for (int index = 0; index < 3; index++) { if (index != leader) { final int follower = index; await(() -> cluster.nodes.get(follower).gcStats().retiredMemoryVersions() >= 22); } }
            cluster.verify(node, reader, view, 1); assertEquals(0, node.gcStats().retiredMemoryVersions()); long before = node.localStats().retainedBytes();
            node.rollback(reader); await(() -> node.gcStats().retiredMemoryVersions() >= 22 && node.localStats().retainedBytes() < before); cluster.verify(node, 12);
            var deleted = node.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { node.delete(deleted, shard, "k"); } node.commit(deleted);
            await(() -> node.localStats().retainedBytes() == 0); assertEquals(14, cluster.write(node, 14).commitSequence()); cluster.verify(node, 14);
            assertEquals(receipt, node.lookupCommit(receipt.transactionId()).receipt());
        }
    }

    /** 回收配置精确绑定身份与预算，外来或损坏文件不能变成默认回收指令。 */
    @Test void persistedPolicyRejectsWrongOwnerChangedBudgetsAndCorruption() throws Exception {
        UUID group = UUID.randomUUID(); var identity = new WalReplicaIdentity(group, group, "x"); var policy = new RatisGcPolicy(identity, POLICY); policy.publish(directory); assertEquals(policy, RatisGcPolicy.load(directory, identity));
        assertThrows(java.io.IOException.class, () -> RatisGcPolicy.load(directory, new WalReplicaIdentity(group, group, "y")));
        assertThrows(java.io.IOException.class, () -> new RatisGcPolicy(identity, RatisGcOptions.defaults()).publish(directory));
        Path file = directory.resolve("GC_POLICY"); var bytes = Files.readAllBytes(file); bytes[bytes.length - 1] ^= 1; Files.write(file, bytes); assertThrows(java.io.IOException.class, () -> RatisGcPolicy.load(directory, identity));
    }

    /** 有限等待针对实际数据库行数与运行时采样，不把超时视为成功。 */
    private static void await(java.util.function.BooleanSupplier condition) throws Exception { long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (!condition.getAsBoolean() && System.nanoTime() < end) { TimeUnit.MILLISECONDS.sleep(20); } assertTrue(condition.getAsBoolean()); }
    /** 两片使用不同载荷，不能以彼此相等替代正确值。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 自有真实节点与数据库代次，GC 故障只注入适配器回收边界。 */
    private static final class Cluster implements AutoCloseable {
        final Path root; final HikariDataSource control; final RaftGroup group; final List<RaftPeer> peers = new ArrayList<>(); final List<RatisWalNode> nodes = new ArrayList<>(); final AtomicBoolean gcFailure = new AtomicBoolean(); final WalReplicaBackendFactory factory; final MysqlWalReplicaBackendFactory base;
        /** 在真实代次上包装回收故障，其余方法仍直接调用正式 MySQL 实现。 */
        Cluster(Path root) throws Exception { this(root, false); }
        /** 纯本地版本链与真实 MySQL 共用同一真实 Raft 集群装配。 */
        Cluster(Path root, boolean local) throws Exception {
            this.root = root; String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASE, schema); control = MysqlGenerationSupport.control(DATABASE, schema);
            var held = new ArrayList<java.net.ServerSocket>(); try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); nodes.add(null); } } finally { for (var socket : held) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            factory = local ? WalReplicaBackendFactory.localOnly() : (identity, generation) -> { var lease = base.open(identity, generation); var stores = new HashMap<String, WalMaterializedStore>(); lease.stores().forEach((shard, store) -> stores.put(shard, faultStore(store))); return new WalReplicaBackendFactory.Lease(stores, lease::close); };
            try { for (int i = 0; i < 3; i++) { start(i); } } catch (Exception failure) { close(); throw failure; }
        }
        /** 只改变 GC 边界，保留实际 SQL、所有权、备份和持久化行为。 */
        private WalMaterializedStore faultStore(WalMaterializedStore delegate) { return (WalMaterializedStore) java.lang.reflect.Proxy.newProxyInstance(WalMaterializedStore.class.getClassLoader(), new Class<?>[]{WalMaterializedStore.class}, (proxy, method, args) -> {
            if (method.getName().equals("gcBounded") && gcFailure.get()) { throw new IllegalStateException("private-password jdbc:unpublished"); }
            try { return method.invoke(delegate, args); } catch (java.lang.reflect.InvocationTargetException actual) { throw actual.getCause(); }
        }); }
        /** 重开沿用真实原身份和目录，策略必须从磁盘恢复。 */
        void start(int index) throws Exception { nodes.set(index, RatisWalNode.openGenerationalLoopback(root.resolve("n" + index), peers.get(index).getId(), group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(5000).withHotCacheBytes(0), WalBackupOptions.defaults())); }
        /** 当前阶段逐节点显式配置，同一策略可以幂等装配。 */
        void enable() throws Exception { for (var node : nodes) { node.enableVersionGc(POLICY); node.enableVersionGc(POLICY); } }
        /** 实际新鲜多数派读屏障成立后才认为领导者可服务。 */
        int leader() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (System.nanoTime() < until) { for (int i = 0; i < 3; i++) { var node = nodes.get(i); if (node == null || !node.ready() || node.leaderEpoch() <= 0) { continue; } try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException transientRole) { } } TimeUnit.MILLISECONDS.sleep(20); } throw new AssertionError("GC cluster leader missing"); }
        /** 每次修改两片同一键，真实后端应形成并回收多版本。 */
        WalReceipt write(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { node.put(tx, shard, "k", bytes(shard + value)); } return node.commit(tx); }
        /** 固定事务及语句边界核对两个完整载荷。 */
        void verify(RatisWalNode node, WalTransaction tx, WalReadSnapshot view, int value) { for (String shard : List.of("a", "b")) { assertArrayEquals(bytes(shard + value), node.get(tx, shard, "k", view).orElseThrow()); } }
        /** 新事务用于验证当前领导者的完整可见性。 */
        void verify(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); verify(node, tx, node.beginReadStatement(tx), value); node.rollback(tx); }
        /** SQL 计数只访问当前测试身份和 ACTIVE 代次派生的数据库。 */
        long rows(int index) {
            try { var active = RatisGenerationRecord.read(root.resolve("n" + index + "/business/ACTIVE")); long count = 0;
                try (var connection = control.getConnection(); var query = connection.createStatement()) { for (String shard : List.of("a", "b")) { String schema = base.databaseName(active.identity(), active.generation(), shard); try (var result = query.executeQuery("select count(*) from `" + schema + "`.azir_wal_version")) { result.next(); count += result.getLong(1); } } } return count;
            } catch (Exception failure) { throw new AssertionError(failure); }
        }
        /** GC 断言前先等待实际异步物化达到确认边界。 */
        void materialized(long sequence) throws Exception { for (var node : nodes) { await(() -> node.localStats().materializedSequences().values().stream().allMatch(value -> value >= sequence)); } }
        /** 只关闭本测试当前持有的进程内节点。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }
        /** 异常时也释放所有自有节点与控制池。 */
        @Override public void close() throws Exception { Exception failure = null; for (int i = 0; i < 3; i++) { try { stop(i); } catch (Exception error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } } control.close(); if (failure != null) { throw failure; } }
    }
}

package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** 三份独立 MySQL 冷副本验证实际快照安装、新代次、并发业务和切主后继续服务。 */
class RatisMysqlGenerationSnapshotTest {
    @TempDir Path directory;

    /** 落后节点必须从快照恢复冷数据，安装暂停期间其余多数派仍保证业务事务完整。 */
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void independentMysqlGenerationsInstallColdSnapshotDuringConcurrentBusiness(boolean authenticated) throws Exception {
        try (var cluster = new Cluster(directory, authenticated)) {
            int leader = cluster.leader(-1);
            if (authenticated) { for (var replica : cluster.nodes) { replica.enableVersionGc(new RatisGcOptions(200, 64, 32, 100, 15_000)); } }
            var node = cluster.nodes.get(leader); var initial = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : Set.of("a", "b")) {
                for (int i = 0; i < 32; i++) { node.put(initial, shard, "cold" + i, bytes(shard + ":" + i)); }
                node.put(initial, shard, "blob", new byte[]{0}); node.put(initial, shard, "live", bytes("0"));
            }
            var receipt = node.commit(initial); int lagging = (leader + 1) % 3; cluster.cold(lagging, 1);
            var before = cluster.active(lagging); cluster.stop(lagging); byte[] a = null, b = null;
            for (int i = 1; i <= 10; i++) {
                a = new byte[480 * 1024]; b = new byte[480 * 1024]; Arrays.fill(a, (byte) i); Arrays.fill(b, (byte) (i + 100));
                var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "blob", a); node.put(tx, "b", "blob", b); assertEquals(i + 1, node.commit(tx).commitSequence());
            }
            var deletion = node.begin(IsolationLevel.REPEATABLE_READ); for (String shard : Set.of("a", "b")) { node.delete(deletion, shard, "cold0"); } assertEquals(12, node.commit(deletion).commitSequence());
            long snapshot = node.snapshot(); assertEquals(0, node.localStats().retainedBytes()); assertEquals(100, node.localStats().walBytes());
            node.purgeForTest(snapshot); assertTrue(node.firstLogIndexForTest() > 2);
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var armed = new AtomicBoolean(true);
            cluster.start(lagging, stage -> {
                if (stage == RatisWalState.Stage.AFTER_INSTALL_RECORD && armed.compareAndSet(true, false)) {
                    entered.countDown(); try { if (!release.await(20, TimeUnit.SECONDS)) { throw new IOException("test did not release installation"); } }
                    catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
                }
            });
            try {
                assertTrue(entered.await(30, TimeUnit.SECONDS)); assertFalse(cluster.nodes.get(lagging).ready());
                try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                    var writer = tasks.submit(() -> { for (int value = 1; value <= 2; value++) { pair(node, value); TimeUnit.MILLISECONDS.sleep(100); } return 2; });
                    var reader = tasks.submit(() -> { for (int i = 0; i < 10; i++) { assertBalance(node); TimeUnit.MILLISECONDS.sleep(100); } return 10; });
                    assertEquals(2, writer.get(20, TimeUnit.SECONDS)); assertEquals(10, reader.get(20, TimeUnit.SECONDS));
                }
                assertFalse(cluster.nodes.get(lagging).ready()); assertEquals(14, node.appliedSequence());
            } finally { release.countDown(); }
            cluster.await(lagging, 14, snapshot); cluster.cold(lagging, 14); var installed = cluster.active(lagging);
            assertEquals(authenticated, cluster.nodes.get(lagging).gcStats().enabled());
            if (authenticated) {
                long gcDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (cluster.nodes.get(lagging).gcStats().publishedSequence() < 14 && System.nanoTime() < gcDeadline) { TimeUnit.MILLISECONDS.sleep(20); }
                assertTrue(cluster.nodes.get(lagging).gcStats().publishedSequence() >= 14);
                assertEquals(installed.generation(), cluster.nodes.get(lagging).gcStats().generation());
            }
            assertNotEquals(before.generation(), installed.generation()); assertEquals(snapshot, installed.index()); assertEquals(12, installed.sequence());
            assertFalse(Files.exists(directory.resolve("n" + lagging + "/business/INSTALLING")));
            var identity = cluster.identity(lagging); var factory = cluster.factories.get(lagging);
            for (String shard : Set.of("a", "b")) {
                String schema = factory.databaseName(identity, installed.generation(), shard);
                assertArrayEquals(shard.equals("a") ? a : b, physical(cluster.controls.get(lagging), schema, "blob"));
                assertNull(physical(cluster.controls.get(lagging), schema, "cold0"));
            }
            cluster.nodes.get(lagging).enableRetirementMaintenance(new RatisRetirementOptions(1, 4, 512, 5000, 2), 100);
            long retirementDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (cluster.nodes.get(lagging).retirementStats().generations() < 1 && System.nanoTime() < retirementDeadline) {
                var last = cluster.nodes.get(lagging).retirementStats().last(); if (last != null) { assertTrue(last.removedEntries() <= 4); assertTrue(last.backendShards() <= 1); } TimeUnit.MILLISECONDS.sleep(30);
            }
            assertEquals(1, cluster.nodes.get(lagging).retirementStats().generations()); assertNull(cluster.nodes.get(lagging).retirementStats().failure()); assertFalse(Files.exists(directory.resolve("n" + lagging + "/business/generation-" + before.generation())));
            try (var connection = cluster.controls.get(lagging).getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select used_slots from azir_wal_namespace_budget")) { assertTrue(row.next()); assertEquals(2, row.getInt(1)); }
            for (String shard : Set.of("a", "b")) { String oldSchema = factory.databaseName(identity, before.generation(), shard); try (var connection = cluster.controls.get(lagging).getConnection(); var query = connection.prepareStatement("select count(*) from information_schema.schemata where cast(schema_name as binary)=?")) { query.setBytes(1, bytes(oldSchema)); try (var row = query.executeQuery()) { row.next(); assertEquals(0, row.getInt(1)); } } }
            cluster.stop(lagging); cluster.start(lagging, stage -> { }); cluster.cold(lagging, 14);
            // 本地检查点已包含业务 14 不代表重启后的 Raft 已追上当前领导者。
            long requiredRaft = node.appliedRaftIndex(); long raftDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (cluster.nodes.get(lagging).appliedRaftIndex() < requiredRaft && System.nanoTime() < raftDeadline) { TimeUnit.MILLISECONDS.sleep(30); }
            assertTrue(cluster.nodes.get(lagging).appliedRaftIndex() >= requiredRaft);
            if (authenticated) { node.transferLeadership(RaftPeerId.valueOf("n" + lagging), 30_000); }
            else { try (var client = RaftClient.newBuilder().setRaftGroup(cluster.group).setProperties(new org.apache.ratis.conf.RaftProperties()).setLeaderId(RaftPeerId.valueOf("n" + leader)).build()) { assertTrue(client.admin().transferLeadership(RaftPeerId.valueOf("n" + lagging), 10_000).isSuccess()); } }
            cluster.awaitLeader(lagging); var serving = cluster.nodes.get(lagging); verifyAll(serving, a, b, 2);
            assertEquals(WalCommitLookup.Status.COMMITTED, serving.lookupCommit(receipt.transactionId()).status()); assertEquals(15, pair(serving, 3).commitSequence()); cluster.cold(lagging, 15);
            cluster.stop(leader); cluster.controls.get(leader).close(); cluster.databases.get(leader).stop();
            verifyAll(serving, a, b, 3); assertEquals(16, pair(serving, 4).commitSequence()); assertBalance(serving);
        }
    }

    /** 业务写集跨两片，数值之和恒为零。 */
    private static WalReceipt pair(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "live", bytes(Integer.toString(value))); node.put(tx, "b", "live", bytes(Integer.toString(-value))); return node.commit(tx); }

    /** 同一语句快照核对双片，不能分别建立快照后拼成假完整结果。 */
    private static void assertBalance(RatisWalNode node) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); var snapshot = node.beginReadStatement(tx);
        int a = Integer.parseInt(new String(node.get(tx, "a", "live", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        int b = Integer.parseInt(new String(node.get(tx, "b", "live", snapshot).orElseThrow(), StandardCharsets.UTF_8)); assertEquals(0, a + b); node.rollback(tx);
    }

    /** 切主后读取全部冷键、大载荷、墓碑和后续日志，缓存禁用不能掩盖缺失。 */
    private static void verifyAll(RatisWalNode node, byte[] a, byte[] b, int live) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); var snapshot = node.beginReadStatement(tx);
        for (String shard : Set.of("a", "b")) {
            for (int i = 0; i < 32; i++) { if (i == 0) { assertTrue(node.get(tx, shard, "cold0", snapshot).isEmpty()); } else { assertEquals(shard + ":" + i, new String(node.get(tx, shard, "cold" + i, snapshot).orElseThrow(), StandardCharsets.UTF_8)); } }
            assertArrayEquals(shard.equals("a") ? a : b, node.get(tx, shard, "blob", snapshot).orElseThrow());
            assertEquals(Integer.toString(shard.equals("a") ? live : -live), new String(node.get(tx, shard, "live", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        }
        node.rollback(tx);
    }

    /** 直接查询接收数据库，独立于事务引擎的缓存和归并代码。 */
    private static byte[] physical(HikariDataSource control, String schema, String key) throws Exception {
        try (var connection = control.getConnection(); var query = connection.prepareStatement("select payload from `" + schema + "`.azir_wal_version where user_key=? order by commit_seq desc limit 1")) {
            query.setBytes(1, bytes(key)); try (var row = query.executeQuery()) { return row.next() ? row.getBytes(1) : null; }
        }
    }

    /** 固定 UTF-8 业务值。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 三个 Raft 节点在同一 JVM，但每个节点拥有独立 MySQL 进程及所有后端代次。 */
    private static final class Cluster implements AutoCloseable {
        final Path directory; final List<org.testcontainers.containers.MySQLContainer<?>> databases = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
        final List<HikariDataSource> controls = new ArrayList<>(); final List<MysqlWalReplicaBackendFactory> factories = new ArrayList<>(); final List<RatisWalNode> nodes = new ArrayList<>(); final RaftGroup group;
        final List<RatisTlsSecurity> security = new ArrayList<>();

        /** 只启动自有三份限资源数据库，每片连接池由实际代次工厂分配。 */
        Cluster(Path directory, boolean authenticated) throws Exception {
            this.directory = directory; var peers = new ArrayList<RaftPeer>(); var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } }
            finally { for (var socket : held) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers);
            if (authenticated) {
                var pki = new RatisTlsTestCertificates(directory.resolve("pki")); var credentials = new ArrayList<RatisTlsTestCertificates.Entry>(); var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
                for (var peer : peers) { var entry = pki.issue(group.getGroupId().getUuid(), peer.getId().toString(), peer.getAddress()); credentials.add(entry); trust.add(entry.trust()); }
                for (var entry : credentials) { security.add(RatisTlsSecurity.load(group.getGroupId().getUuid(), entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust)); }
            }
            try {
                Startables.deepStart(databases.stream()).join();
                for (int i = 0; i < 3; i++) {
                    var pool = MysqlGenerationSupport.control(databases.get(i), databases.get(i).getDatabaseName()); controls.add(pool);
                    factories.add(new MysqlWalReplicaBackendFactory(Map.of("a", pool, "b", pool), MysqlWalReplicaBackendFactory.Options.defaults())); nodes.add(null); start(i, stage -> { });
                }
            } catch (Throwable failure) { close(); throw failure; }
        }

        /** 节点身份与实际组一致，每个新代次由同一工厂幂等重开。 */
        WalReplicaIdentity identity(int index) { return new WalReplicaIdentity(group.getGroupId().getUuid(), group.getGroupId().getUuid(), "n" + index); }

        /** 使用正式数据库工厂，包内钩子只控制安装的实际边界。 */
        void start(int index, RatisWalState.Hook hook) throws Exception {
            var root = directory.resolve("n" + index); var peer = RaftPeerId.valueOf("n" + index); var options = WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0); var limits = new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000);
            nodes.set(index, security.isEmpty() ? RatisWalNode.openGenerationalForTest(root, peer, group, Set.of("a", "b"), factories.get(index), options, limits, hook)
                    : RatisWalNode.openAuthenticatedForTest(root, peer, group, Set.of("a", "b"), factories.get(index), options, limits, security.get(index), hook));
        }

        /** 仅停止本轮节点，保留业务目录和数据库以供追平安装。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }

        /** 确认领导者能够实际通过多数派屏障。 */
        int leader(int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (i == excluded || node == null || node.leaderEpoch() <= 0) { continue; }
                    try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException transientRole) { }
                } TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("no MySQL generation leader");
        }

        /** 转移之后仍需等实际就绪，不能把管理回复当成已能执行事务。 */
        void awaitLeader(int index) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (nodes.get(index).leaderEpoch() <= 0 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(20); } assertTrue(nodes.get(index).leaderEpoch() > 0); }

        /** 安装期不可访问内核，三个水位同时满足才认为追平完成。 */
        void await(int index, long sequence, long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                var node = nodes.get(index); try { if (node.ready() && node.appliedSequence() == sequence && node.snapshotIndex() >= snapshot && node.durableSnapshotAckIndex() >= snapshot) { return; } } catch (IllegalStateException installing) { }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("MySQL replica did not finish installation and catch up");
        }

        /** 强制证据来自冷数据，检查物化水位、零缓存和一百字节尾日志。 */
        void cold(int index, long sequence) throws Exception {
            var node = nodes.get(index); long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!node.ready() || node.appliedSequence() < sequence || node.localStats().materializedSequences().values().stream().anyMatch(value -> value < sequence)) { if (System.nanoTime() > until) { throw new AssertionError("MySQL generation did not materialize"); } TimeUnit.MILLISECONDS.sleep(20); }
            assertTrue(node.checkpointLocal().created()); assertEquals(0, node.localStats().retainedBytes()); assertEquals(100, node.localStats().walBytes());
        }

        /** 从真实持久活动指针核对代次，而不是相信工厂分配的内存参数。 */
        RatisGenerationRecord active(int index) throws IOException { return RatisGenerationRecord.read(directory.resolve("n" + index + "/business/ACTIVE")); }

        /** 全部节点及自有资源逐一关闭，单个失败仍清理其他测试资源。 */
        @Override public void close() throws Exception {
            Throwable failure = null;
            for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } }
            for (var pool : controls) { try { pool.close(); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } }
            for (var database : databases) { try { database.stop(); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } }
            if (failure instanceof Exception exception) { throw exception; } if (failure instanceof Error error) { throw error; }
        }
    }
}

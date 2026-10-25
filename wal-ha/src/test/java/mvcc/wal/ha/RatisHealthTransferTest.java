package mvcc.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 三份独立 MySQL 与认证 Raft，验证数据库停止响应时的健康切主和恢复。 */
class RatisHealthTransferTest {
    static final List<org.testcontainers.containers.MySQLContainer<?>> DATABASES = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
    static final RatisHealthOptions HEALTH = new RatisHealthOptions(200, 2, 2, 750, 6000);
    @TempDir Path directory;
    /** 全类复用三份受限实例，每个场景独立控制库和组身份。 */
    @BeforeAll static void startDatabases() { Startables.deepStart(DATABASES.stream()).join(); }
    /** 只释放本类创建的数据库实例。 */
    @AfterAll static void stopDatabases() { for (var database : DATABASES) { database.stop(); } }

    /** 原主后端停止响应而 Raft 仍存活时转移，恢复后旧事务不能复用。 */
    @Test void leaderBackendFailureTransfersConfirmedColdDataAndRecovers() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int old = cluster.leader(Set.of()); var original = cluster.nodes.get(old); var receipt = cluster.write(original, 1); cluster.coldAll(1);
            var stale = original.begin(IsolationLevel.REPEATABLE_READ); original.put(stale, "a", "k", new byte[]{99}); original.put(stale, "b", "k", new byte[]{99});
            cluster.pause(old); int next = cluster.leader(Set.of(old)); await(() -> original.healthStats().quarantined() && original.healthStats().campaignBlocked());
            await(() -> original.healthStats().transfers() >= 1);
            assertThrows(WalMvccException.class, () -> original.commit(stale)); cluster.verify(cluster.nodes.get(next), 1, receipt); assertEquals(2, cluster.write(cluster.nodes.get(next), 2).commitSequence());
            assertEquals(WalCommitLookup.Status.NOT_CONFIRMED, cluster.nodes.get(next).lookupCommit(stale.id()).status());
            cluster.resume(old); await(() -> original.healthStats().state().equals("HEALTHY") && !original.healthStats().quarantined()); cluster.cold(original, 2);
            assertEquals(next, cluster.leader(Set.of(old))); cluster.nodes.get(next).transferLeadership(cluster.peers.get(old).getId(), 30_000); cluster.awaitLeader(old);
            cluster.verify(original, 2, receipt); assertThrows(WalMvccException.class, () -> original.commit(stale)); assertTrue(original.healthStats().isolations() >= 1); assertTrue(original.healthStats().recoveries() >= 1);
        }
    }

    /** 全部后端失效时不把未知目标当作健康，恢复后依法重新选举再提交。 */
    @Test void noHealthyTargetKeepsAllBusinessGatesClosedUntilRecovery() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(Set.of()); long oldTerm = cluster.nodes.get(first).leaderEpoch(); var receipt = cluster.write(cluster.nodes.get(first), 1); cluster.coldAll(1);
            long[] beforeDenials = cluster.nodes.stream().mapToLong(RatisWalNode::campaignDenialsForTest).toArray();
            for (int i = 0; i < 3; i++) { cluster.pause(i); } await(() -> cluster.nodes.stream().allMatch(node -> node.healthStats().quarantined() && node.healthStats().campaignBlocked()));
            for (var node : cluster.nodes) { assertThrows(WalMvccException.class, () -> node.begin(IsolationLevel.REPEATABLE_READ)); assertEquals(0, node.leaderEpoch()); }
            // 明确等待原生候选状态出现，覆盖旧 Pause 导致没有选举线程而无法恢复的时序。
            await(() -> java.util.stream.IntStream.range(0, 3).allMatch(i -> cluster.nodes.get(i).campaignDenialsForTest() > beforeDenials[i]));
            for (int i = 0; i < 3; i++) { cluster.resume(i); } int leader = cluster.leader(Set.of()); assertTrue(cluster.nodes.get(leader).leaderEpoch() > oldTerm); cluster.verify(cluster.nodes.get(leader), 1, receipt); assertEquals(2, cluster.write(cluster.nodes.get(leader), 2).commitSequence());
        }
    }

    /** 已启用策略重启时自动恢复，数据库仍失效不能绕过启动检查加入服务。 */
    @Test void quarantinedRestartRestoresPolicyAndCannotSkipBackendChecks() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(Set.of()), victim = (leader + 1) % 3; var receipt = cluster.write(cluster.nodes.get(leader), 1); cluster.coldAll(1);
            cluster.pause(victim); await(() -> cluster.nodes.get(victim).healthStats().quarantined()); cluster.stop(victim);
            assertThrows(IOException.class, () -> cluster.open(victim)); assertTrue(Files.exists(directory.resolve("n" + victim + "/HEALTH_POLICY"))); assertEquals(2, cluster.write(cluster.nodes.get(leader), 2).commitSequence());
            cluster.resume(victim); cluster.nodes.set(victim, cluster.open(victim)); var restarted = cluster.nodes.get(victim); assertTrue(restarted.healthStats().enabled());
            if (restarted.ready()) { assertTrue(restarted.healthStats().successes() >= HEALTH.recoveries()); }
            await(() -> restarted.healthStats().state().equals("HEALTHY") && !restarted.healthStats().quarantined()); cluster.cold(restarted, 2); cluster.nodes.get(leader).transferLeadership(cluster.peers.get(victim).getId(), 30_000); cluster.awaitLeader(victim); cluster.verify(restarted, 2, receipt);
        }
    }

    /** 规则或身份损坏不能在重启时变成无健康管理的默认模式。 */
    @Test void persistentPolicyBindsIdentityAndRejectsCorruption() throws Exception {
        UUID id = UUID.randomUUID(); var owner = new WalReplicaIdentity(id, id, "x"); var record = new RatisHealthRecord(owner, HEALTH); record.publish(directory); assertEquals(record, RatisHealthRecord.load(directory, owner));
        assertThrows(IOException.class, () -> RatisHealthRecord.load(directory, new WalReplicaIdentity(id, id, "y")));
        assertThrows(IOException.class, () -> new RatisHealthRecord(owner, RatisHealthOptions.defaults()).publish(directory));
        Path file = directory.resolve("HEALTH_POLICY"); byte[] bytes = Files.readAllBytes(file); bytes[bytes.length - 1] ^= 1; Files.write(file, bytes); assertThrows(IOException.class, () -> RatisHealthRecord.load(directory, owner));
    }
    /** 基于状态观察等待，不通过固定睡眠假定切主已经完成。 */
    private static void await(java.util.function.BooleanSupplier condition) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (!condition.getAsBoolean() && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } assertTrue(condition.getAsBoolean()); }

    /** 每节点独立控制池、数据库代次和 TLS 身份，健康管理显式启用后持久恢复。 */
    private static final class Cluster implements AutoCloseable {
        final Path root; final RaftGroup group; final List<RaftPeer> peers = new ArrayList<>(); final List<RatisWalNode> nodes = new ArrayList<>(); final List<HikariDataSource> controls = new ArrayList<>(); final List<MysqlWalReplicaBackendFactory> factories = new ArrayList<>(); final List<RatisTlsSecurity> tls = new ArrayList<>(); final boolean[] paused = new boolean[3];
        /** 证书与端口属于本轮验收，数据库状态变化只作用于自有实例。 */
        Cluster(Path root) throws Exception {
            this.root = root; var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } }
            finally { for (var socket : held) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); var pki = new RatisTlsTestCertificates(root.resolve("pki")); var credentials = new ArrayList<RatisTlsTestCertificates.Entry>(); var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (var peer : peers) { var entry = pki.issue(group.getGroupId().getUuid(), peer.getId().toString(), peer.getAddress()); credentials.add(entry); trust.add(entry.trust()); }
            try { for (int i = 0; i < 3; i++) {
                String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASES.get(i), schema); var pool = MysqlGenerationSupport.control(DATABASES.get(i), schema); controls.add(pool); factories.add(new MysqlWalReplicaBackendFactory(Map.of("a", pool, "b", pool), MysqlWalReplicaBackendFactory.Options.defaults()));
                var entry = credentials.get(i); tls.add(RatisTlsSecurity.load(group.getGroupId().getUuid(), entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust)); nodes.add(open(i)); nodes.get(i).enableHealthManagement(HEALTH);
            } await(() -> nodes.stream().allMatch(node -> node.healthStats().state().equals("HEALTHY"))); }
            catch (Exception failure) { close(); throw failure; }
        }
        /** 重开使用持久策略自动装配，不额外调用 enableHealthManagement。 */
        RatisWalNode open(int index) throws IOException { return RatisWalNode.openAuthenticated(root.resolve("n" + index), peers.get(index).getId(), group, Set.of("a", "b"), factories.get(index), WalOptions.defaults().withTimeout(5000).withHotCacheBytes(0), new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000), tls.get(index)); }
        /** 停止单个自有节点并保留其真实目录。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }
        /** 暂停数据库进程，Raft JVM 和网络均保持运行。 */
        void pause(int index) { var database = DATABASES.get(index); database.getDockerClient().pauseContainerCmd(database.getContainerId()).exec(); paused[index] = true; var state = database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec().getState(); assertTrue(state.getRunning()); assertTrue(state.getPaused()); }
        /** 原数据库原地恢复，不重建数据空间。 */
        void resume(int index) { if (!paused[index]) { return; } var database = DATABASES.get(index); database.getDockerClient().unpauseContainerCmd(database.getContainerId()).exec(); paused[index] = false; assertFalse(database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec().getState().getPaused()); }
        /** 选择已经能完成真实多数派读屏障的健康领导者。 */
        int leader(Set<Integer> excluded) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < until) { for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (excluded.contains(i) || node == null || node.leaderEpoch() <= 0) { continue; } try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException transientRole) { } } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("healthy leader missing"); }
        /** 管理转移后仍确认目标实际就绪。 */
        void awaitLeader(int index) throws Exception { await(() -> nodes.get(index).leaderEpoch() > 0); }
        /** 小型跨片事务仍有不同载荷，便于发现错误归并。 */
        WalReceipt write(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "k", new byte[]{(byte) value}); node.put(tx, "b", "k", new byte[]{(byte) (100 + value)}); return node.commit(tx); }
        /** 一条 RR 语句核对双片冷数据和已确认 UUID。 */
        void verify(RatisWalNode node, int value, WalReceipt receipt) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(tx); assertArrayEquals(new byte[]{(byte) value}, node.get(tx, "a", "k", view).orElseThrow()); assertArrayEquals(new byte[]{(byte) (100 + value)}, node.get(tx, "b", "k", view).orElseThrow()); node.rollback(tx); var result = node.lookupCommit(receipt.transactionId()); assertEquals(WalCommitLookup.Status.COMMITTED, result.status()); assertEquals(receipt, result.receipt()); }
        /** 三份都确认真实物化和本地截断，不依赖活跃缓存证明冷数据可靠。 */
        void coldAll(long sequence) throws Exception { for (var node : nodes) { cold(node, sequence); } }
        /** 数据库恢复以后等待物化完成并确认零缓存。 */
        void cold(RatisWalNode node, long sequence) throws Exception { await(() -> node.appliedSequence() >= sequence && node.localStats().materializedSequences().values().stream().allMatch(value -> value >= sequence)); assertTrue(node.checkpointLocal().created()); assertEquals(0, node.localStats().retainedBytes()); }
        /** 异常也先恢复自有暂停实例，再排空节点与控制池。 */
        @Override public void close() throws Exception { for (int i = 0; i < 3; i++) { resume(i); } Exception failure = null; for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } for (var control : controls) { control.close(); } if (failure != null) { throw failure; } }
    }
}

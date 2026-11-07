package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 旧节点本地文件与独立 MySQL 全部丢失后，新身份从真实冷快照重建及配置恢复。 */
class RatisMysqlMemberReplacementTest {
    @TempDir Path directory;

    /** 四个独立数据库代表原三副本与替换节点，新节点不依赖原主磁盘或原主数据库。 */
    @Test void newMysqlPeerRebuildsPermanentlyLostLeaderAndRestoresVotingRole() throws Exception {
        var databases = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
        var controls = new ArrayList<HikariDataSource>(); var factories = new ArrayList<MysqlWalReplicaBackendFactory>(); var nodes = new ArrayList<RatisWalNode>();
        var peers = new ArrayList<RaftPeer>(); var held = new ArrayList<java.net.ServerSocket>(); String newAddress;
        try {
            Startables.deepStart(databases.stream()).join();
            for (int i = 0; i < 4; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); if (i < 3) { peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } }
            newAddress = "127.0.0.1:" + held.getLast().getLocalPort(); var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); RatisMemberReplacement plan;
            for (int i = 0; i < 4; i++) { var control = MysqlGenerationSupport.control(databases.get(i), databases.get(i).getDatabaseName()); controls.add(control); factories.add(new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults())); nodes.add(null); }
            for (int i = 0; i < 3; i++) { held.get(i).close(); nodes.set(i, open(directory.resolve("n" + i), RaftPeerId.valueOf("n" + i), group, factories.get(i))); }
            int old = leader(nodes); var source = nodes.get(old); var initial = source.begin(IsolationLevel.REPEATABLE_READ);
            source.put(initial, "a", "balance", bytes("90")); source.put(initial, "b", "balance", bytes("110")); source.put(initial, "a", "blob", new byte[]{0}); source.put(initial, "b", "blob", new byte[]{0}); var receipt = source.commit(initial);
            byte[] a = null, b = null;
            for (int i = 1; i <= 10; i++) { a = new byte[480 * 1024]; b = new byte[480 * 1024]; Arrays.fill(a, (byte) i); Arrays.fill(b, (byte) (i + 100)); var tx = source.begin(IsolationLevel.REPEATABLE_READ); source.put(tx, "a", "blob", a); source.put(tx, "b", "blob", b); assertEquals(i + 1, source.commit(tx).commitSequence()); }
            source.close(); nodes.set(old, null); controls.get(old).close(); databases.get(old).stop(); discard(directory.resolve("n" + old)); assertFalse(Files.exists(directory.resolve("n" + old)));
            int current = leader(nodes); source = nodes.get(current); long snapshot = source.snapshot(); source.purgeForTest(snapshot); assertEquals(0, source.localStats().retainedBytes()); assertTrue(source.firstLogIndexForTest() > 2);
            plan = source.planReplacement(RaftPeerId.valueOf("n" + old), newAddress, 10_000); Files.write(directory.resolve("replacement.ticket"), plan.encode());
            held.get(3).close(); nodes.set(3, open(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), factories.get(3)));
            assertEquals(RatisWalNode.ReplacementPhase.LEARNER_ADDED, source.beginReplacement(plan, 40_000).phase());
            var completed = complete(source, plan); assertEquals(3, completed.membership().voters().size()); assertTrue(completed.membership().learners().isEmpty());
            transfer(groupFor(group.getGroupId(), completed.membership()), RaftPeerId.valueOf("n" + current), plan.replacement().getId()); awaitLeader(nodes.get(3));
            verify(nodes.get(3), a, b); assertEquals(WalCommitLookup.Status.COMMITTED, nodes.get(3).lookupCommit(receipt.transactionId()).status());
            cold(nodes.get(3), 11); long ownSnapshot = nodes.get(3).snapshot(); assertTrue(ownSnapshot > snapshot);
            var restoredConfiguration = nodes.get(3).membership(10_000); RaftPeerId freshId = plan.replacement().getId();
            assertTrue(restoredConfiguration.voters().stream().anyMatch(peer -> peer.getId().equals(freshId)));
            nodes.get(3).close(); nodes.set(3, null);
            // 故意沿用加入时的初始组，已持久的新成员配置必须优先恢复，而不是重新当成永久非投票副本。
            nodes.set(3, open(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), factories.get(3))); awaitSequence(nodes.get(3), 11);
            current = leader(nodes); if (current != 3) { transfer(groupFor(group.getGroupId(), completed.membership()), RaftPeerId.valueOf("n" + current), plan.replacement().getId()); awaitLeader(nodes.get(3)); }
            var membership = nodes.get(3).membership(10_000); assertTrue(membership.voters().stream().noneMatch(peer -> peer.getId().equals(RaftPeerId.valueOf("n" + old))));
            assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, nodes.get(3).completeReplacement(RatisMemberReplacement.decode(Files.readAllBytes(directory.resolve("replacement.ticket"))), 10_000).phase());
            verify(nodes.get(3), a, b); var continued = nodes.get(3).begin(IsolationLevel.REPEATABLE_READ); nodes.get(3).put(continued, "a", "balance", bytes("80")); nodes.get(3).put(continued, "b", "balance", bytes("120")); assertEquals(12, nodes.get(3).commit(continued).commitSequence());
        } finally {
            for (var socket : held) { socket.close(); }
            Throwable failure = null; for (var node : nodes) { if (node != null) { try { node.close(); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } } }
            for (var control : controls) { try { control.close(); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } }
            for (var database : databases) { try { database.stop(); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } }
            if (failure instanceof Exception exception) { throw exception; } if (failure instanceof Error error) { throw error; }
        }
    }

    /** 工厂与节点使用正式代次协议，当前专项仅回环且缓存禁用。 */
    private static RatisWalNode open(Path root, RaftPeerId id, RaftGroup group, WalReplicaBackendFactory factory) throws Exception { return RatisWalNode.openGenerationalLoopback(root, id, group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0), new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000)); }
    /** 找到能实际通过多数派读屏障的节点。 */
    private static int leader(List<RatisWalNode> nodes) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (System.nanoTime() < until) { for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (node == null || node.leaderEpoch() <= 0) { continue; } try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException changing) { } } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("no replacement group leader"); }
    /** 等待同一票据追平，不生成新身份或改写原成员集合。 */
    private static RatisWalNode.ReplacementProgress complete(RatisWalNode source, RatisMemberReplacement plan) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < until) { var result = source.completeReplacement(plan, 15_000); if (result.phase() == RatisWalNode.ReplacementPhase.COMPLETE) { return result; } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("MySQL learner did not become ready"); }
    /** 转移领导权使用明确源 ID 和有限无自动重试客户端。 */
    private static void transfer(RaftGroup group, RaftPeerId source, RaftPeerId target) throws Exception { try (var client = RaftClient.newBuilder().setRaftGroup(group).setLeaderId(source).setProperties(new org.apache.ratis.conf.RaftProperties()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) { assertTrue(client.admin().transferLeadership(target, 10_000).isSuccess()); } }
    /** 新节点确实成为可服务的投票领导者。 */
    private static void awaitLeader(RatisWalNode node) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (node.leaderEpoch() <= 0 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } assertTrue(node.leaderEpoch() > 0); var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); }
    /** 新节点重开后必须恢复数据水位。 */
    private static void awaitSequence(RatisWalNode node, long sequence) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (System.nanoTime() < until) { try { if (node.ready() && node.appliedSequence() == sequence) { return; } } catch (IllegalStateException loading) { } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("replacement did not restore sequence"); }
    /** 明确完成冷物化并截断本地日志，后续验证不能依赖活跃缓存。 */
    private static void cold(RatisWalNode node, long sequence) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (node.localStats().materializedSequences().values().stream().anyMatch(value -> value < sequence)) { if (System.nanoTime() > until) { throw new AssertionError("replacement cold state lagged"); } TimeUnit.MILLISECONDS.sleep(30); } node.checkpointLocal(); assertEquals(0, node.localStats().retainedBytes()); assertEquals(100, node.localStats().walBytes()); }
    /** 同一语句快照核对冷数据与完整二进制值。 */
    private static void verify(RatisWalNode node, byte[] a, byte[] b) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(tx); assertEquals("90", new String(node.get(tx, "a", "balance", view).orElseThrow(), StandardCharsets.UTF_8)); assertEquals("110", new String(node.get(tx, "b", "balance", view).orElseThrow(), StandardCharsets.UTF_8)); assertArrayEquals(a, node.get(tx, "a", "blob", view).orElseThrow()); assertArrayEquals(b, node.get(tx, "b", "blob", view).orElseThrow()); node.rollback(tx); }
    /** 管理客户端从已提交成员视图构造配置。 */
    private static RaftGroup groupFor(RaftGroupId id, RatisMembershipView view) { var peers = new ArrayList<>(view.voters()); peers.addAll(view.learners()); return RaftGroup.valueOf(id, peers); }
    /** 仅删除本轮已关闭旧节点的私有临时目录，模拟永久磁盘丢失。 */
    private static void discard(Path root) throws Exception { try (var paths = Files.walk(root)) { for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) { Files.delete(path); } } }
    /** 固定 UTF-8 业务值。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 真实三节点 Raft 集成实际 MVCC 内核，分别验证切主保留确认和少数派不能返回成功。 */
class RatisMvccClusterTest {
    @TempDir Path directory;

    /** 已确认跨片事务在停领导者、选新主及旧节点重开后仍然完整，序号继续递增。 */
    @Test void confirmedCrossShardCommitSurvivesLeaderChangeAndRestart() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(-1); var receipt = write(cluster.nodes.get(first), "one"); assertEquals(1, receipt.commitSequence());
            assertPair(cluster.nodes.get(first), "one"); int follower = (first + 1) % 3;
            assertThrows(WalMvccException.class, () -> cluster.nodes.get(follower).begin(IsolationLevel.READ_COMMITTED));
            cluster.stop(first); int second = cluster.leader(first); assertPair(cluster.nodes.get(second), "one");
            assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(second).lookupCommit(receipt.transactionId()).status());
            assertEquals(2, write(cluster.nodes.get(second), "two").commitSequence());
            cluster.start(first); cluster.awaitApplied(first, 2); assertPair(cluster.nodes.get(second), "two");
            assertTrue(cluster.nodes.get(first).appliedSequence() >= 2);
        }
    }

    /** 两个节点不可用后，一个原领导者既不能确认新事务，也不能凭热缓存建立新读快照。 */
    @Test void minorityCannotAcknowledgeWritesOrServeFreshSnapshots() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(-1); var node = cluster.nodes.get(leader); write(node, "committed");
            var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "k", bytes("unconfirmed")); node.put(tx, "b", "k", bytes("unconfirmed"));
            for (int i = 0; i < 3; i++) { if (i != leader) { cluster.stop(i); } }
            assertThrows(WalMvccException.class, () -> node.commit(tx)); assertEquals(1, node.appliedSequence());
            assertThrows(WalMvccException.class, () -> node.begin(IsolationLevel.READ_COMMITTED)); assertEquals(1, node.appliedSequence());
        }
    }

    /** 读写只经过节点业务入口，完整写集在同一事务中提交。 */
    private static WalReceipt write(RatisWalNode node, String value) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "k", bytes(value)); node.put(tx, "b", "k", bytes(value)); return node.commit(tx);
    }

    /** 同一经过屏障的语句快照核对两片，不能用两次独立 RC 查询掩盖半事务。 */
    private static void assertPair(RatisWalNode node, String expected) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); var snapshot = node.beginReadStatement(tx);
        assertEquals(expected, new String(node.get(tx, "a", "k", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        assertEquals(expected, new String(node.get(tx, "b", "k", snapshot).orElseThrow(), StandardCharsets.UTF_8)); node.rollback(tx);
    }

    /** 固定业务值，不把字符串引用或缓存命中当作持久化证明。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 三个真实 gRPC/Raft 服务各有独立日志和业务 WAL 目录。 */
    private static final class Cluster implements AutoCloseable {
        final Path directory; final RaftGroup group; final List<RatisWalNode> nodes = new ArrayList<>();

        /** 临时端口只用于当前回环测试，不连接其他服务。 */
        Cluster(Path directory) throws Exception {
            this.directory = directory; var peers = new ArrayList<RaftPeer>(); var sockets = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress()); sockets.add(socket);
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            } finally { for (var socket : sockets) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers);
            for (int i = 0; i < 3; i++) { nodes.add(null); }
            try { for (int i = 0; i < 3; i++) { start(i); } }
            catch (Throwable failure) { close(); throw failure; }
        }

        /** 使用完整持久状态重启原节点，不能删除投票或业务目录来伪造追平。 */
        void start(int index) throws Exception {
            nodes.set(index, RatisWalNode.openLoopback(directory.resolve("n" + index), RaftPeerId.valueOf("n" + index), group, Set.of("a", "b"), Map.of(), WalOptions.defaults().withTimeout(5000)));
        }

        /** 停止一个自有节点，保留它的持久文件用于恢复。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }

        /** 等待实际 Raft 角色而非固定睡眠时间推断谁是领导者。 */
        int leader(int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            do {
                for (int i = 0; i < nodes.size(); i++) { if (i != excluded && nodes.get(i) != null && nodes.get(i).leaderEpoch() > 0) { return i; } }
                TimeUnit.MILLISECONDS.sleep(20);
            } while (System.nanoTime() < until);
            throw new AssertionError("no ready leader");
        }

        /** 用实际本地应用水位证明旧节点追上，不把 RPC 建连当作追平。 */
        void awaitApplied(int index, long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (nodes.get(index).appliedSequence() < sequence && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(20); }
            assertEquals(sequence, nodes.get(index).appliedSequence());
        }

        /** 仅关闭本测试创建的节点，累积关闭错误而不遗留后续服务。 */
        @Override public void close() throws Exception {
            Exception failure = null;
            for (int i = 0; i < nodes.size(); i++) {
                try { stop(i); } catch (Exception error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } }
            }
            if (failure != null) { throw failure; }
        }
    }
}

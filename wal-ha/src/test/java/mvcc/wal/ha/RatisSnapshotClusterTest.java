package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.client.RaftClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 Raft 位置生成快照、进程内节点重开及日志缺口下的实际安装集成。 */
class RatisSnapshotClusterTest {
    @TempDir Path directory;

    /** 本地完整快照与活动代次绑定，重开原节点不丢确认，后续事务仍连续。 */
    @Test void actualSnapshotPositionSurvivesNodeRestartAndContinuedCommit() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(-1); var receipt = write(cluster.nodes.get(first), new byte[]{1, 11}, new byte[]{1, 22});
            long snapshot = cluster.nodes.get(first).snapshot(); assertTrue(snapshot > 0); assertEquals(snapshot, cluster.nodes.get(first).snapshotIndex());
            var pointer = RatisGenerationRecord.read(directory.resolve("n" + first + "/business/ACTIVE")); assertEquals(snapshot, pointer.index()); assertEquals(1, pointer.sequence());
            cluster.stop(first); int next = cluster.leader(first); assertEquals(2, write(cluster.nodes.get(next), new byte[]{2, 11}, new byte[]{2, 22}).commitSequence());
            cluster.start(first); cluster.await(first, 2, snapshot); assertPair(cluster.nodes.get(next), new byte[]{2, 11}, new byte[]{2, 22});
            assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(next).lookupCommit(receipt.transactionId()).status());
            var reopened = RatisGenerationRecord.read(directory.resolve("n" + first + "/business/ACTIVE")); assertEquals(pointer.generation(), reopened.generation());
        }
    }

    /** 已清理的日志缺口强迫跟随者通过 Ratis 安装完整快照，再将它切为主节点核对数据与凭据。 */
    @Test void laggingReplicaInstallsCompleteSnapshotAcrossRealRaftTransport() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(-1); var old = write(cluster.nodes.get(leader), new byte[]{0, 11}, new byte[]{0, 22});
            int lagging = (leader + 1) % 3; cluster.await(lagging, 1, -1);
            var before = RatisGenerationRecord.read(directory.resolve("n" + lagging + "/business/ACTIVE")); cluster.stop(lagging);
            byte[] a = null, b = null;
            // 累计超过八 MiB 关闭至少一个日志段，避免只调用 purge 却没有实际清理可追赶日志。
            for (int i = 1; i <= 10; i++) {
                a = new byte[480 * 1024]; b = new byte[480 * 1024]; Arrays.fill(a, (byte) i); Arrays.fill(b, (byte) (i + 100));
                assertEquals(i + 1, write(cluster.nodes.get(leader), a, b).commitSequence());
            }
            long snapshot = cluster.nodes.get(leader).snapshot(); cluster.nodes.get(leader).purgeForTest(snapshot);
            assertTrue(cluster.nodes.get(leader).firstLogIndexForTest() > 2, "a real unavailable log prefix is required");
            cluster.start(lagging); cluster.await(lagging, 11, snapshot);
            long ackDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (cluster.nodes.get(lagging).durableSnapshotAckIndex() < snapshot && System.nanoTime() < ackDeadline) { TimeUnit.MILLISECONDS.sleep(20); }
            assertTrue(cluster.nodes.get(lagging).durableSnapshotAckIndex() >= snapshot, "actual inbound RPC must run the durable ACK guard");
            var installed = RatisGenerationRecord.read(directory.resolve("n" + lagging + "/business/ACTIVE"));
            assertNotEquals(before.generation(), installed.generation()); assertEquals(snapshot, installed.index()); assertEquals(11, installed.sequence());
            assertFalse(Files.exists(directory.resolve("n" + lagging + "/business/INSTALLING")));
            try (var client = RaftClient.newBuilder().setRaftGroup(cluster.group).setProperties(new org.apache.ratis.conf.RaftProperties()).setLeaderId(RaftPeerId.valueOf("n" + leader)).build()) {
                var reply = client.admin().transferLeadership(RaftPeerId.valueOf("n" + lagging), 10_000); assertTrue(reply.isSuccess(), reply.toString());
            }
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (cluster.nodes.get(lagging).leaderEpoch() <= 0 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(20); }
            assertTrue(cluster.nodes.get(lagging).leaderEpoch() > 0); assertPair(cluster.nodes.get(lagging), a, b);
            assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(lagging).lookupCommit(old.transactionId()).status());
            assertEquals(12, write(cluster.nodes.get(lagging), new byte[]{12, 11}, new byte[]{12, 22}).commitSequence());
            cluster.stop(lagging); cluster.start(lagging); cluster.await(lagging, 12, snapshot);
            assertPair(cluster.nodes.get(cluster.leader(-1)), new byte[]{12, 11}, new byte[]{12, 22});
        }
    }

    /** 一次事务写不同的双片值，不能用相等结果掩盖误路由。 */
    private static WalReceipt write(RatisWalNode node, byte[] a, byte[] b) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "k", a); node.put(tx, "b", "k", b); return node.commit(tx);
    }

    /** 同一固定语句快照核对完整二进制载荷，且释放事务。 */
    private static void assertPair(RatisWalNode node, byte[] a, byte[] b) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); var snapshot = node.beginReadStatement(tx);
        assertArrayEquals(a, node.get(tx, "a", "k", snapshot).orElseThrow()); assertArrayEquals(b, node.get(tx, "b", "k", snapshot).orElseThrow()); node.rollback(tx);
    }

    /** 三个节点各有独立代次目录及实际 gRPC 服务，未使用共享业务内核。 */
    private static final class Cluster implements AutoCloseable {
        final Path directory; final RaftGroup group; final List<RatisWalNode> nodes = new ArrayList<>();

        /** 同时保留临时端口直至分配结束，降低测试内部冲突。 */
        Cluster(Path directory) throws Exception {
            this.directory = directory; var peers = new ArrayList<RaftPeer>(); var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = new java.net.ServerSocket(0); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } }
            finally { for (var socket : held) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers); for (int i = 0; i < 3; i++) { nodes.add(null); }
            try { for (int i = 0; i < 3; i++) { start(i); } } catch (Throwable failure) { close(); throw failure; }
        }

        /** 重开保留原投票身份和活动代次，不能删目录伪装为新节点。 */
        void start(int index) throws Exception {
            nodes.set(index, RatisWalNode.openGenerationalLoopback(directory.resolve("n" + index), RaftPeerId.valueOf("n" + index), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), WalOptions.defaults().withTimeout(30_000), new WalBackupOptions(128_000_000, 32, 4_000_000, 60_000)));
        }

        /** 停止单个自有节点并保留全部持久目录。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }

        /** 选出的节点还需实际 begin 屏障证明能服务，不能只相信角色缓存。 */
        int leader(int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (i == excluded || node == null || node.leaderEpoch() <= 0) { continue; }
                    try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException transientRole) { }
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("no ready leader with majority barrier");
        }

        /** 安装中内核不可访问，必须同时等完整代次就绪、快照位置和业务水位。 */
        void await(int index, long sequence, long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                var node = nodes.get(index);
                try { if (node.ready() && node.appliedSequence() == sequence && node.snapshotIndex() >= snapshot) { return; } } catch (IllegalStateException installing) { }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("replica did not complete snapshot installation and catch up");
        }

        /** 清理所有自有节点，单个关闭失败不能遗留其他测试服务。 */
        @Override public void close() throws Exception {
            Throwable failure = null;
            for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Throwable close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } }
            if (failure instanceof Exception exception) { throw exception; } if (failure instanceof Error error) { throw error; }
        }
    }
}

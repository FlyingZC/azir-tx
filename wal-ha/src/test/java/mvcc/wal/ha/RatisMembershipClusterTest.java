package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 Raft 成员替换、完整状态丢失重建、操作续做和旧节点复活隔离。 */
class RatisMembershipClusterTest {
    @TempDir Path directory;

    /** 旧主全部本地状态丢失后用新 ID 从快照重建，旧磁盘副本复活不能确认新事务。 */
    @Test void newIdentityRebuildsLostLeaderAndRetiredLeaderCannotServe() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int old = cluster.leader(-1); var receipt = write(cluster.nodes.get(old), new byte[]{0}, new byte[]{100}); byte[] a = null, b = null;
            for (int i = 1; i <= 10; i++) { a = new byte[480 * 1024]; b = new byte[480 * 1024]; Arrays.fill(a, (byte) i); Arrays.fill(b, (byte) (i + 100)); write(cluster.nodes.get(old), a, b); }
            cluster.stop(old); Path disk = directory.resolve("old-disk"); copy(directory.resolve("n" + old), disk); discard(directory.resolve("n" + old)); assertFalse(Files.exists(directory.resolve("n" + old)));
            int leader = cluster.leader(old); long snapshot = cluster.nodes.get(leader).snapshot(); cluster.nodes.get(leader).purgeForTest(snapshot); assertTrue(cluster.nodes.get(leader).firstLogIndexForTest() > 2);
            var plan = cluster.nodes.get(leader).planReplacement(RaftPeerId.valueOf("n" + old), cluster.newAddress, 10_000); Files.write(directory.resolve("replacement.ticket"), plan.encode());
            cluster.startReplacement(plan); assertThrows(WalMvccException.class, () -> cluster.replacement.begin(IsolationLevel.READ_COMMITTED));
            var staged = cluster.nodes.get(leader).beginReplacement(plan, 30_000); assertEquals(RatisWalNode.ReplacementPhase.LEARNER_ADDED, staged.phase()); assertEquals(3, staged.membership().voters().size());
            cluster.replacement.close(); cluster.replacement = null;
            assertEquals(RatisWalNode.ReplacementPhase.WAITING_FOR_LEARNER, cluster.nodes.get(leader).completeReplacement(plan, 10_000).phase());
            cluster.startReplacement(plan); cluster.awaitReplacement(11, snapshot);
            var completed = cluster.nodes.get(leader).completeReplacement(RatisMemberReplacement.decode(Files.readAllBytes(directory.resolve("replacement.ticket"))), 30_000);
            assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, completed.phase()); assertTrue(completed.membership().learners().isEmpty());
            assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, cluster.nodes.get(leader).completeReplacement(plan, 10_000).phase());
            cluster.transfer(cluster.nodes.get(leader), cluster.groupFor(completed.membership()), plan.replacement().getId()); cluster.awaitReplacementLeader();
            assertPair(cluster.replacement, a, b); assertEquals(WalCommitLookup.Status.COMMITTED, cluster.replacement.lookupCommit(receipt.transactionId()).status()); assertEquals(12, write(cluster.replacement, new byte[]{12}, new byte[]{112}).commitSequence());
            cluster.replacement.snapshot(); cluster.replacement.close(); cluster.replacement = null; cluster.startReplacement(plan); cluster.awaitReplacement(12, 0);
            leader = cluster.leader(-1); RatisWalNode serving = leader < 0 ? cluster.replacement : cluster.nodes.get(leader); assertPair(serving, new byte[]{12}, new byte[]{112});
            try (var retired = RatisWalNode.openGenerationalLoopback(disk, RaftPeerId.valueOf("n" + old), cluster.group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits())) {
                assertThrows(WalMvccException.class, () -> retired.begin(IsolationLevel.READ_COMMITTED));
                int current = cluster.leader(-1); serving = current < 0 ? cluster.replacement : cluster.nodes.get(current);
                assertEquals(13, write(serving, new byte[]{13}, new byte[]{113}).commitSequence()); assertPair(serving, new byte[]{13}, new byte[]{113});
            }
        }
    }

    /** 另一票据不能覆盖已加入的副本，协调者切主后原票据可以继续完成。 */
    @Test void sameTicketResumesAfterCoordinatorFailoverAndConflictingPlanIsRejected() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(-1); int removed = (first + 1) % 3; var receipt = write(cluster.nodes.get(first), new byte[]{1}, new byte[]{101});
            var plan = cluster.nodes.get(first).planReplacement(RaftPeerId.valueOf("n" + removed), cluster.newAddress, 10_000);
            var conflicting = cluster.nodes.get(first).planReplacement(RaftPeerId.valueOf("n" + removed), "127.0.0.1:19099", 10_000);
            try (var raw = RaftClient.newBuilder().setRaftGroup(cluster.group).setLeaderId(RaftPeerId.valueOf("n" + first)).setProperties(new org.apache.ratis.conf.RaftProperties()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) {
                boolean rejected; try { rejected = !raw.admin().setConfiguration(cluster.group.getPeers().stream().limit(2).toList()).isSuccess(); } catch (java.io.IOException forbidden) { rejected = true; }
                assertTrue(rejected, "raw configuration RPC must not bypass managed replacement checks");
            }
            cluster.startReplacement(plan); cluster.nodes.get(first).beginReplacement(plan, 30_000);
            assertThrows(java.io.IOException.class, () -> cluster.nodes.get(first).beginReplacement(conflicting, 10_000));
            byte[] saved = plan.encode(); cluster.stop(first); int next = cluster.leader(-1);
            if (next == removed) { int retained = (first + 2) % 3; cluster.transfer(cluster.nodes.get(next), plan.joiningGroup(), RaftPeerId.valueOf("n" + retained)); next = cluster.leader(removed); }
            var completed = cluster.nodes.get(next).completeReplacement(RatisMemberReplacement.decode(saved), 30_000); assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, completed.phase());
            assertEquals(2, write(cluster.nodes.get(next), new byte[]{2}, new byte[]{102}).commitSequence());
            assertThrows(WalMvccException.class, () -> cluster.nodes.get(removed).begin(IsolationLevel.READ_COMMITTED));
            cluster.start(first); cluster.await(first, 2); int current = cluster.leader(-1); var serving = current < 0 ? cluster.replacement : cluster.nodes.get(current);
            assertPair(serving, new byte[]{2}, new byte[]{102}); assertEquals(WalCommitLookup.Status.COMMITTED, serving.lookupCommit(receipt.transactionId()).status());
            assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, serving.completeReplacement(plan, 10_000).phase());
        }
    }

    /** 管理执行器阻塞时，接口仍在总期限内失败；释放后继续正常观察且不误改配置。 */
    @Test void stalledControlAdmissionHonorsDeadlineAndKeepsRoster() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(-1); var node = cluster.nodes.get(leader); var original = node.membership(10_000);
            var field = RatisWalNode.class.getDeclaredField("membershipExecutor"); field.setAccessible(true); var executor = (java.util.concurrent.ExecutorService) field.get(node);
            var entered = new java.util.concurrent.CountDownLatch(1); var release = new java.util.concurrent.CountDownLatch(1);
            executor.submit(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); } }); assertTrue(entered.await(5, TimeUnit.SECONDS));
            long started = System.nanoTime();
            try { assertThrows(java.io.IOException.class, () -> node.membership(100)); assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2)); }
            finally { release.countDown(); }
            RatisMembershipView current = null; long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (current == null && System.nanoTime() < until) { try { current = node.membership(1000); } catch (java.io.IOException drainingExpiredTask) { TimeUnit.MILLISECONDS.sleep(20); } }
            assertNotNull(current); assertTrue(RatisMembershipView.same(original.voters(), current.voters())); assertTrue(current.learners().isEmpty());
        }
    }

    /** 一笔事务完整更新两片。 */
    private static WalReceipt write(RatisWalNode node, byte[] a, byte[] b) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "k", a); node.put(tx, "b", "k", b); return node.commit(tx); }

    /** 在同一 RR 语句快照核对两片二进制内容。 */
    private static void assertPair(RatisWalNode node, byte[] a, byte[] b) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(tx); assertArrayEquals(a, node.get(tx, "a", "k", view).orElseThrow()); assertArrayEquals(b, node.get(tx, "b", "k", view).orElseThrow()); node.rollback(tx); }

    /** 当前专项不改资源预算，大写集仍在每事务一 MiB 内。 */
    private static WalOptions options() { return WalOptions.defaults().withTimeout(30_000); }

    /** 镜像维护设置有限总量和时间。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(128_000_000, 16, 4_000_000, 120_000); }

    /** 停止旧节点之后复制其私有测试磁盘，保存复活时的陈旧状态。 */
    private static void copy(Path source, Path target) throws Exception { try (var paths = Files.walk(source)) { for (Path path : paths.toList()) { Path output = target.resolve(source.relativize(path)); if (Files.isDirectory(path)) { Files.createDirectories(output); } else { Files.copy(path, output); } } } }

    /** 只删除本测试刚创建并已停止的节点目录，模拟所有本地持久状态永久丢失。 */
    private static void discard(Path target) throws Exception { try (var paths = Files.walk(target)) { for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) { Files.delete(path); } } }

    /** 初始三个节点与一个新身份，均为本轮私有回环服务。 */
    private static final class Cluster implements AutoCloseable {
        final Path directory; final RaftGroup group; final String newAddress; final List<RatisWalNode> nodes = new ArrayList<>(); RatisWalNode replacement; RatisMemberReplacement plan;
        /** 临时端口同时保留到分配完成，避免本测试内部重复。 */
        Cluster(Path directory) throws Exception {
            this.directory = directory; var peers = new ArrayList<RaftPeer>(); var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 4; i++) { var socket = new java.net.ServerSocket(0); held.add(socket); if (i < 3) { peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } } newAddress = "127.0.0.1:" + held.getLast().getLocalPort(); }
            finally { for (var socket : held) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); for (int i = 0; i < 3; i++) { nodes.add(null); }
            try { for (int i = 0; i < 3; i++) { start(i); } } catch (Throwable failure) { close(); throw failure; }
        }
        /** 初始成员重开保留自己的目录。 */
        void start(int index) throws Exception { nodes.set(index, RatisWalNode.openGenerationalLoopback(directory.resolve("n" + index), RaftPeerId.valueOf("n" + index), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits())); }
        /** 新节点始终沿用票据里的新 ID，第一次是空目录，重启沿用自己的持久状态。 */
        void startReplacement(RatisMemberReplacement plan) throws Exception { this.plan = plan; replacement = RatisWalNode.openGenerationalLoopback(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits()); }
        /** 只停止已创建的成员。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }
        /** 允许新身份成为领导者，返回负一表示新节点，调用方再使用多数派屏障。 */
        int leader(int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (i != excluded && node != null && available(node)) { return i; } }
                if (replacement != null && available(replacement)) { return -1; } TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("no member with majority read barrier");
        }
        /** 实际业务屏障而不是旧角色缓存证明可服务。 */
        private static boolean available(RatisWalNode node) { if (node.leaderEpoch() <= 0) { return false; } try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return true; } catch (WalMvccException transientRole) { return false; } }
        /** 新身份必须完成快照或日志追赶。 */
        void awaitReplacement(long sequence, long snapshot) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < until) { try { if (replacement.ready() && replacement.appliedSequence() == sequence && replacement.snapshotIndex() >= snapshot) { return; } } catch (IllegalStateException installing) { } TimeUnit.MILLISECONDS.sleep(20); } throw new AssertionError("replacement did not catch up"); }
        /** 原成员恢复后也必须追上已确认状态。 */
        void await(int index, long sequence) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(40); while (System.nanoTime() < until) { if (nodes.get(index).ready() && nodes.get(index).appliedSequence() == sequence) { return; } TimeUnit.MILLISECONDS.sleep(20); } throw new AssertionError("original member did not catch up"); }
        /** 领导权转移之后验证新成员真正能通过多数派屏障。 */
        void awaitReplacementLeader() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (System.nanoTime() < until) { if (available(replacement)) { return; } TimeUnit.MILLISECONDS.sleep(20); } throw new AssertionError("replacement did not become serving leader"); }
        /** 测试使用官方转移 API，所有节点仍属于本轮自有回环组。 */
        void transfer(RatisWalNode source, RaftGroup configuration, RaftPeerId target) throws Exception {
            RaftPeerId leader = source == replacement ? plan.replacement().getId() : RaftPeerId.valueOf("n" + nodes.indexOf(source));
            try (var client = RaftClient.newBuilder().setRaftGroup(configuration).setLeaderId(leader).setProperties(new org.apache.ratis.conf.RaftProperties()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) { assertTrue(client.admin().transferLeadership(target, 10_000).isSuccess()); }
        }
        /** 客户端配置来自实际完成后的成员集合。 */
        RaftGroup groupFor(RatisMembershipView view) { var peers = new ArrayList<>(view.voters()); peers.addAll(view.learners()); return RaftGroup.valueOf(group.getGroupId(), peers); }
        /** 清理所有自有服务，不因单个节点关闭失败遗留其他节点。 */
        @Override public void close() throws Exception { Throwable failure = null; if (replacement != null) { try { replacement.close(); } catch (Throwable close) { failure = close; } } for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Throwable close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } if (failure instanceof Exception exception) { throw exception; } if (failure instanceof Error error) { throw error; } }
    }
}

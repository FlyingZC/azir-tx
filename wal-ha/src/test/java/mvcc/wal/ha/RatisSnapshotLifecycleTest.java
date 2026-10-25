package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 真实认证集群的自动镜像、实际日志回收、失败背压与重启策略验证。 */
class RatisSnapshotLifecycleTest {
    @TempDir Path directory;
    private static final RatisSnapshotOptions POLICY = new RatisSnapshotOptions(100, 8, 500, 64L * 1024 * 1024, 0);

    /** 完整镜像覆盖后才回收真实段文件，日志缺口后的重开仍保留确认数据与 UUID。 */
    @Test void automaticSnapshotsPurgeClosedSegmentsAndRestartSafely() throws Exception {
        try (var cluster = new Cluster(directory, limits())) {
            cluster.enable(POLICY); int first = cluster.leader(); WalReceipt receipt = null;
            for (int value = 1; value <= 12; value++) { var committed = cluster.write(cluster.nodes.get(first), value); if (receipt == null) { receipt = committed; } }
            final int source = first; await(() -> cluster.nodes.get(source).snapshotStats().purges() >= 1 && cluster.nodes.get(source).snapshotStats().firstLogIndex() > 0);
            assertTrue(cluster.nodes.get(first).snapshotIndex() >= cluster.nodes.get(first).snapshotStats().firstLogIndex()); cluster.verify(cluster.nodes.get(first), 12); assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(first).lookupCommit(receipt.transactionId()).status());
            cluster.stop(first); int next = cluster.leader(); cluster.verify(cluster.nodes.get(next), 12); cluster.start(first, limits()); assertTrue(cluster.nodes.get(first).snapshotStats().enabled());
            await(() -> cluster.nodes.get(source).ready() && cluster.nodes.get(source).appliedSequence() >= 12); cluster.verify(cluster.nodes.get(next), 12); assertEquals(13, cluster.write(cluster.nodes.get(next), 13).commitSequence());
        }
    }

    /** 镜像预算失败必须保留日志，超水位关闭新业务；修复预算后由持久策略自动恢复。 */
    @Test void failedImagesKeepLogsAndBackpressureRecoversAfterRestart() throws Exception {
        var tiny = new WalBackupOptions(1024, 8, 64000, 30_000); var policy = new RatisSnapshotOptions(100, 8, 500, 32L * 1024 * 1024, 0); var acknowledged = new ArrayList<WalReceipt>(); var attempts = new HashMap<UUID, Integer>();
        try (var cluster = new Cluster(directory, tiny)) {
            cluster.enable(policy); int leader = cluster.leader();
            for (int value = 1; value <= 80; value++) {
                var node = cluster.nodes.get(leader);
                try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); attempts.put(tx.id(), value); put(node, tx, value); acknowledged.add(node.commit(tx)); }
                catch (WalMvccException blocked) { break; }
                if (node.snapshotStats().backpressure()) { break; }
            }
            final int source = leader; await(() -> cluster.nodes.get(source).snapshotStats().backpressure()); assertFalse(acknowledged.isEmpty());
            assertEquals(0, RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE")).sequence()); assertEquals(0, cluster.nodes.get(leader).firstLogIndexForTest()); assertEquals(0, cluster.nodes.get(leader).snapshotStats().purges()); assertThrows(WalMvccException.class, () -> cluster.nodes.get(source).begin(IsolationLevel.REPEATABLE_READ));
            for (int i = 0; i < 3; i++) { cluster.stop(i); } for (int i = 0; i < 3; i++) { cluster.start(i, limits()); }
            leader = cluster.leader(); var serving = cluster.nodes.get(leader); long latest = 0; int expected = 0;
            for (var entry : attempts.entrySet()) { var lookup = serving.lookupCommit(entry.getKey()); if (lookup.status() == WalCommitLookup.Status.COMMITTED && lookup.receipt().commitSequence() > latest) { latest = lookup.receipt().commitSequence(); expected = entry.getValue(); } }
            for (var receipt : acknowledged) { assertEquals(receipt, serving.lookupCommit(receipt.transactionId()).receipt()); } cluster.verify(serving, expected); assertTrue(serving.snapshotStats().enabled());
        }
    }

    /** 高水位时已覆盖的软保留尾部可回收，否则大日志条目会让默认保留条数永久阻塞恢复。 */
    @org.junit.jupiter.api.RepeatedTest(value = 5, failureThreshold = 1) void pressureCanReclaimCoveredPreferredTailAndResume() throws Exception {
        var policy = new RatisSnapshotOptions(100, 10_000_000, 60_000, 32L * 1024 * 1024, 1024); var receipts = new ArrayList<WalReceipt>();
        try (var cluster = new Cluster(directory, limits())) {
            cluster.enable(policy); int initial = cluster.leader(); var node = cluster.nodes.get(initial);
            for (int value = 1; value <= 80; value++) {
                long observed = node.snapshotStats().rounds();
                try { receipts.add(cluster.write(node, value)); } catch (WalMvccException pressure) { break; }
                // 每次等待一次水位观察，避免测试生产者把三份本地历史瞬间推过同 JVM 的内存预算。
                await(() -> node.snapshotStats().rounds() > observed);
                if (node.snapshotStats().backpressure()) { break; }
            }
            assertFalse(receipts.isEmpty());
            long recoveryEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(90), reportAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!(node.snapshotStats().purges() > 0 && !node.snapshotStats().backpressure() && node.ready()) && System.nanoTime() < recoveryEnd) {
                if (System.nanoTime() >= reportAt) { reportAt = Long.MAX_VALUE; for (int i = 0; i < 3; i++) { diagnostic(cluster.nodes.get(i), directory.resolve("n" + i)); } }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            assertTrue(node.snapshotStats().purges() > 0 && !node.snapshotStats().backpressure() && node.ready(), node.snapshotStats() + " " + node.healthStats());
            int current = cluster.leader(); for (var receipt : receipts) { assertEquals(receipt, cluster.nodes.get(current).lookupCommit(receipt.transactionId()).receipt()); }
            assertTrue(node.snapshotStats().firstLogIndex() > 0); cluster.write(cluster.nodes.get(current), 81); cluster.verify(cluster.nodes.get(current), 81);
        }
    }
    /** 崩溃暂存只能在归属正确且没有活动任务时有界清理，未知文件始终保留。 */
    @Test void buildGarbageCleanupIsBoundedAndRejectsUnknownOwnership() throws Exception {
        try (var cluster = new Cluster(directory, limits())) {
            int index = cluster.leader(); var node = cluster.nodes.get(index); var field = RatisWalNode.class.getDeclaredField("machine"); field.setAccessible(true); var machine = (RatisWalStateMachine) field.get(node); Path parent = machine.getStateMachineStorage().getSnapshotDir().toPath();
            UUID build = UUID.randomUUID(); Path root = parent.resolve(".build-" + build); Files.createDirectory(root); var identity = new WalReplicaIdentity(cluster.group.getGroupId().getUuid(), cluster.group.getGroupId().getUuid(), "n" + index); new RatisGenerationRecord(identity, build, -1, -1, 0, "").publish(root.resolve("BUILD")); Files.createDirectories(root.resolve("backup/data")); Files.write(root.resolve("backup/data/0000.bin"), new byte[]{1});
            for (int i = 0; i < 20 && Files.exists(root); i++) { var work = new RatisRetirementWork(new RatisRetirementOptions(1, 1, 512, 1000, 2)); machine.cleanupBuilds(work); assertTrue(work.removed <= 1); } assertFalse(Files.exists(root));
            UUID foreignId = UUID.randomUUID(); Path foreign = parent.resolve(".build-" + foreignId); Files.createDirectory(foreign); new RatisGenerationRecord(new WalReplicaIdentity(identity.engineId(), identity.groupId(), "foreign"), foreignId, -1, -1, 0, "").publish(foreign.resolve("BUILD")); Files.write(foreign.resolve("snapshot.bundle"), new byte[]{1});
            assertThrows(java.io.IOException.class, () -> machine.cleanupBuilds(new RatisRetirementWork(RatisRetirementOptions.defaults()))); assertTrue(Files.exists(foreign.resolve("snapshot.bundle"))); Files.delete(foreign.resolve("snapshot.bundle")); Files.delete(foreign.resolve("BUILD")); Files.delete(foreign);
            UUID unknown = UUID.randomUUID(); Path unowned = parent.resolve(".build-" + unknown); Files.createDirectory(unowned); new RatisGenerationRecord(identity, unknown, -1, -1, 0, "").publish(unowned.resolve("BUILD")); Files.writeString(unowned.resolve("user-file"), "keep");
            assertThrows(java.io.IOException.class, () -> machine.cleanupBuilds(new RatisRetirementWork(RatisRetirementOptions.defaults()))); assertEquals("keep", Files.readString(unowned.resolve("user-file")));
        }
    }
    /** 持久策略精确绑定节点和参数，损坏不能变成自动清理的默认配置。 */
    @Test void persistentSnapshotPolicyRejectsForeignIdentityAndCorruption() throws Exception {
        UUID group = UUID.randomUUID(); var owner = new WalReplicaIdentity(group, group, "x"); var policy = new RatisSnapshotPolicy(owner, POLICY); policy.publish(directory); assertEquals(policy, RatisSnapshotPolicy.load(directory, owner));
        assertThrows(java.io.IOException.class, () -> RatisSnapshotPolicy.load(directory, new WalReplicaIdentity(group, group, "y")));
        assertThrows(java.io.IOException.class, () -> new RatisSnapshotPolicy(owner, RatisSnapshotOptions.defaults()).publish(directory));
        Path file = directory.resolve("SNAPSHOT_POLICY"); byte[] corrupt = Files.readAllBytes(file); corrupt[corrupt.length - 1] ^= 1; Files.write(file, corrupt); assertThrows(java.io.IOException.class, () -> RatisSnapshotPolicy.load(directory, owner));
    }
    /** 仅用于故障专项记录私有测试镜像异常，正式监控仍只输出脱敏缓存。 */
    private static void diagnostic(RatisWalNode node, Path root) throws Exception {
        System.err.println("TEST_RECOVERY " + root.getFileName() + " " + node.snapshotStats() + " " + node.healthStats());
        var field = RatisWalNode.class.getDeclaredField("machine"); field.setAccessible(true); var machine = (RatisWalStateMachine) field.get(node);
        var jobField = RatisWalStateMachine.class.getDeclaredField("imageJob"); jobField.setAccessible(true);
        var job = (java.util.concurrent.CompletableFuture<?>) jobField.get(machine);
        if (job != null && job.isCompletedExceptionally()) { job.handle((ignored, failure) -> { failure.printStackTrace(System.err); return null; }).join(); }
        System.err.println("TEST_ACTIVE " + root.getFileName() + " " + RatisGenerationRecord.read(root.resolve("business/ACTIVE")));
    }

    /** 有限条件等待只观察实际回收和可服务状态。 */
    private static void await(java.util.function.BooleanSupplier condition) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90); while (!condition.getAsBoolean() && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } assertTrue(condition.getAsBoolean()); }
    /** 足够滚动真实八 MiB Raft 段文件的跨片载荷。 */
    private static void put(RatisWalNode node, WalTransaction tx, int value) { for (String shard : List.of("a", "b")) { byte[] payload = new byte[480 * 1024]; Arrays.fill(payload, (byte) (value + (shard.equals("a") ? 0 : 100))); node.put(tx, shard, "k", payload); } }
    /** 镜像大小覆盖本轮数据，生产规模另由容量验收决定。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000); }

    /** 三份本地副本与独立 TLS 身份，只操作当前测试的目录和监听端口。 */
    private static final class Cluster implements AutoCloseable {
        final Path root; final RaftGroup group; final List<RaftPeer> peers = new ArrayList<>(); final List<RatisWalNode> nodes = new ArrayList<>(); final List<RatisTlsSecurity> tls = new ArrayList<>();
        /** 凭据只保留在 JUnit 私有临时目录，逻辑副本不共享 WAL。 */
        Cluster(Path root, WalBackupOptions limits) throws Exception {
            this.root = root; var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); nodes.add(null); } } finally { for (var socket : held) { socket.close(); } }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); var pki = new RatisTlsTestCertificates(root.resolve("pki")); var entries = new ArrayList<RatisTlsTestCertificates.Entry>(); var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (var peer : peers) { var entry = pki.issue(group.getGroupId().getUuid(), peer.getId().toString(), peer.getAddress()); entries.add(entry); trust.add(entry.trust()); }
            try { for (int i = 0; i < 3; i++) { var entry = entries.get(i); tls.add(RatisTlsSecurity.load(group.getGroupId().getUuid(), entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust)); start(i, limits); } }
            catch (Exception failure) { close(); throw failure; }
        }
        /** 启用以后重启从两个持久策略恢复完整生命周期。 */
        void enable(RatisSnapshotOptions options) throws Exception { for (var node : nodes) { node.enableHealthManagement(new RatisHealthOptions(200, 2, 2, 750, 6000)); node.enableSnapshotMaintenance(options); } }
        /** 原投票身份沿用自己的实际目录，不空格式化历史。 */
        void start(int index, WalBackupOptions limits) throws Exception { nodes.set(index, RatisWalNode.openAuthenticated(root.resolve("n" + index), peers.get(index).getId(), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), WalOptions.defaults().withTimeout(5000), limits, tls.get(index))); }
        /** 只关闭测试持有的节点。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }
        /** 新鲜多数派屏障成功以后才使用发现的领导者。 */
        int leader() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90); while (System.nanoTime() < until) { for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (node == null || node.leaderEpoch() < 1 || !node.ready()) { continue; } try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException transientRole) { } } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("snapshot lifecycle leader missing"); }
        /** 通过真实事务取得确认凭据。 */
        WalReceipt write(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); put(node, tx, value); return node.commit(tx); }
        /** 检查两片完整大载荷，不能只看业务序号。 */
        void verify(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(tx); for (String shard : List.of("a", "b")) { byte[] expected = new byte[480 * 1024]; Arrays.fill(expected, (byte) (value + (shard.equals("a") ? 0 : 100))); assertArrayEquals(expected, node.get(tx, shard, "k", view).orElseThrow()); } node.rollback(tx); }
        /** 关闭全部自有节点，单个失败继续释放其余资源。 */
        @Override public void close() throws Exception { Exception failure = null; for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } if (failure != null) { throw failure; } }
    }
}

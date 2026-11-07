package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 大镜像导出期间真实 Raft 提交与关闭的并发验证，后端使用实际 MySQL 固定读视图。 */
class RatisSnapshotConcurrencyTest {
    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();
    @TempDir Path directory;
    /** 一个受限实例保存三个逻辑副本的独立空间。 */
    @BeforeAll static void startDatabase() { DATABASE.start(); }
    /** 只释放本类的实例。 */
    @AfterAll static void stopDatabase() { DATABASE.stop(); }

    /** 导出旧边界期间仍能提交和读取新边界，最终快照必须保留旧边界的位置。 */
    @Test void imageExportDoesNotBlockPublicationOrMixLaterTransactions() throws Exception {
        try (var cluster = new Cluster(directory); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var receipt = cluster.write(node, 1); cluster.gate.owner = "n" + leader;
            var snapshot = workers.submit(node::snapshot);
            try {
                assertTrue(cluster.gate.entered.await(10, TimeUnit.SECONDS)); node.enableVersionGc(new RatisGcOptions(100, 4, 4, 100, 5000));
                var update = workers.submit(() -> cluster.write(node, 2)); assertEquals(2, update.get(5, TimeUnit.SECONDS).commitSequence());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (node.gcStats().removedRows() < 2 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(20); }
                assertTrue(node.gcStats().removedRows() >= 2, node.gcStats().toString()); assertEquals(2, node.gcStats().safePoint());
                assertFalse(snapshot.isDone()); cluster.verify(node, 2);
            }
            finally { cluster.gate.release.countDown(); }
            long index = snapshot.get(30, TimeUnit.SECONDS); var active = RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE")); assertEquals(index, active.index()); assertEquals(1, active.sequence());
            // 源已物理删除旧 SQL 版本，固定读视图仍必须导出并恢复旧边界的完整双片值。
            var machineField = RatisWalNode.class.getDeclaredField("machine"); machineField.setAccessible(true); var machine = (RatisWalStateMachine) machineField.get(node);
            Path bundle = machine.getStateMachineStorage().getSnapshotDir().toPath().resolve("snapshot." + active.term() + "_" + index);
            Path backup = directory.resolve("exported"); RatisSnapshotBundle.extract(bundle, backup, RaftGroupId.valueOf(active.identity().groupId()), org.apache.ratis.server.protocol.TermIndex.valueOf(active.term(), index), limits());
            var receiver = new WalReplicaIdentity(active.identity().engineId(), active.identity().groupId(), "restored");
            var factory = new MysqlWalReplicaBackendFactory(Map.of("a", cluster.control, "b", cluster.control), MysqlWalReplicaBackendFactory.Options.defaults());
            try (var lease = factory.open(receiver, UUID.randomUUID())) {
                WalBackup.restoreReplica(backup, directory.resolve("restored-wal"), lease.stores(), WalOptions.defaults(), limits(), receiver);
                for (String shard : List.of("a", "b")) { assertArrayEquals((shard + "1").getBytes(StandardCharsets.UTF_8), lease.stores().get(shard).get("k", 1).orElseThrow().value()); }
            }
            assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(receipt.transactionId()).status()); assertEquals(3, cluster.write(node, 3).commitSequence()); cluster.verify(node, 3);
        }
    }

    /** 镜像已完整发布但激活报错后，无新事务的重试必须补齐 ACTIVE 而不重建另一个输入。 */
    @Test void existingPublishedSnapshotRetriesItsActivationWithoutNewCommit() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var receipt = cluster.write(node, 1);
            long stableSince = System.nanoTime(), last = node.appliedRaftIndex(), deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() - stableSince < TimeUnit.MILLISECONDS.toNanos(500) && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(20); long current = node.appliedRaftIndex(); if (current != last) { last = current; stableSince = System.nanoTime(); }
            }
            assertTrue(System.nanoTime() - stableSince >= TimeUnit.MILLISECONDS.toNanos(500));
            cluster.publicationFailureOwner = "n" + leader; cluster.failPublication.set(true); assertThrows(IOException.class, node::snapshot);
            long published = node.snapshotIndex(); assertTrue(published >= 0); var before = RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE")); assertTrue(before.index() < published);
            var field = RatisWalNode.class.getDeclaredField("machine"); field.setAccessible(true); var machine = (RatisWalStateMachine) field.get(node); Path sm = machine.getStateMachineStorage().getSnapshotDir().toPath(); Path image;
            try (var files = Files.list(sm)) { image = files.filter(path -> path.getFileName().toString().matches("snapshot\\.[0-9]+_" + published)).findFirst().orElseThrow(); }
            byte[] bytes = Files.readAllBytes(image); Object inode = Files.readAttributes(image, java.nio.file.attribute.BasicFileAttributes.class).fileKey();
            assertEquals(published, node.snapshot()); var active = RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE")); assertEquals(published, active.index()); assertEquals(1, active.sequence());
            assertEquals(inode, Files.readAttributes(image, java.nio.file.attribute.BasicFileAttributes.class).fileKey()); assertArrayEquals(bytes, Files.readAllBytes(image)); assertEquals(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)), active.hash());
            assertEquals(receipt, node.lookupCommit(receipt.transactionId()).receipt()); assertEquals(2, cluster.write(node, 2).commitSequence()); cluster.verify(node, 2);
        }
    }

    /** 节点关闭必须等待自己的导出视图释放，不能提前关闭底层连接池。 */
    @Test void nodeCloseDrainsOwnedExportWithoutInterruptingWal() throws Exception {
        try (var cluster = new Cluster(directory); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var receipt = cluster.write(node, 1); cluster.gate.owner = "n" + leader;
            var snapshot = workers.submit(node::snapshot); assertTrue(cluster.gate.entered.await(10, TimeUnit.SECONDS));
            var closing = workers.submit(() -> { node.close(); return true; });
            try { assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS)); }
            finally { cluster.gate.release.countDown(); }
            assertTrue(closing.get(30, TimeUnit.SECONDS)); cluster.nodes.set(leader, null);
            try { snapshot.get(30, TimeUnit.SECONDS); } catch (ExecutionException closedBeforePublish) { assertTrue(closedBeforePublish.getCause() instanceof IOException); }
            int next = cluster.leader(); cluster.verify(cluster.nodes.get(next), 1); assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(next).lookupCommit(receipt.transactionId()).status());
        }
    }

    /** 镜像进行中即使文件发布锁长时间占用，水位观察和业务提交仍能推进。 */
    @Test void snapshotStorageWaitDoesNotBlockWatermarkObservation() throws Exception {
        try (var cluster = new Cluster(directory); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); cluster.write(node, 1); cluster.gate.owner = "n" + leader;
            var snapshot = workers.submit(node::snapshot);
            try {
                assertTrue(cluster.gate.entered.await(10, TimeUnit.SECONDS));
                var machineField = RatisWalNode.class.getDeclaredField("machine"); machineField.setAccessible(true);
                var storageField = RatisWalStateMachine.class.getDeclaredField("snapshots"); storageField.setAccessible(true);
                Object storage = storageField.get(machineField.get(node));
                // 暂停真实发布使用的文件锁，不能让文件维护持有代次锁并挡住观察线程。
                synchronized (storage) {
                    node.enableHealthManagement(new RatisHealthOptions(200, 2, 2, 750, 6000));
                    node.enableSnapshotMaintenance(new RatisSnapshotOptions(100, 10_000_000, 60_000, 32L * 1024 * 1024, 1024));
                    long initial = node.snapshotStats().rounds(), until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (node.snapshotStats().rounds() < initial + 3 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(20); }
                    assertTrue(node.snapshotStats().rounds() >= initial + 3, node.snapshotStats().toString());
                    assertEquals(2, workers.submit(() -> cluster.write(node, 2)).get(5, TimeUnit.SECONDS).commitSequence());
                    assertEquals(0, node.healthStats().isolations()); assertFalse(snapshot.isDone());
                    assertTrue(node.retireOldState(RatisRetirementOptions.defaults()).pending());
                }
            } finally { cluster.gate.release.countDown(); }
            snapshot.get(30, TimeUnit.SECONDS); cluster.verify(node, 2);
        }
    }

    /** 暂停只作用于本测试指定节点的一次镜像输出。 */
    private static final class Gate {
        volatile String owner; final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        /** 真实视图已经固定以后才暂停输出。 */
        void hit(String node) throws IOException { if (!node.equals(owner)) { return; } owner = null; entered.countDown(); try { if (!release.await(15, TimeUnit.SECONDS)) { throw new IOException("image gate observation expired"); } } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); } }
    }
    /** 三个真实节点只共享控制服务，各自的数据库、WAL 与身份空间独立。 */
    private static final class Cluster implements AutoCloseable {
        final Gate gate = new Gate(); volatile String publicationFailureOwner; final java.util.concurrent.atomic.AtomicBoolean failPublication = new java.util.concurrent.atomic.AtomicBoolean(); final List<RatisWalNode> nodes = new ArrayList<>(); final HikariDataSource control;
        /** 只使用本测试新建的控制库与监听端口。 */
        Cluster(Path root) throws Exception {
            String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASE, schema); control = MysqlGenerationSupport.control(DATABASE, schema); var peers = new ArrayList<RaftPeer>(); var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } } finally { for (var socket : held) { socket.close(); } }
            var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers); var base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            WalReplicaBackendFactory factory = new WalReplicaBackendFactory() {
                /** 延迟只作用于真实导出视图，代次和后端仍由正式工厂创建。 */
                @Override public Lease open(WalReplicaIdentity identity, UUID generation) throws IOException { var lease = base.open(identity, generation); var stores = new HashMap<String, WalMaterializedStore>(); lease.stores().forEach((shard, store) -> stores.put(shard, new ImageStore(store, identity.nodeId(), gate))); return new Lease(stores, lease::close); }
                /** 自动生命周期所需退休能力直接来自正式工厂。 */
                @Override public boolean supportsRetirement() { return base.supportsRetirement(); }
                /** 不改变实际旧代次所有权检查和有限回收合同。 */
                @Override public RetirementProgress retire(WalGenerationRetirement permit, String cursor, int shardLimit) throws IOException { return base.retire(permit, cursor, shardLimit); }
            };
            try { for (int i = 0; i < 3; i++) { String owner = "n" + i; nodes.add(RatisWalNode.openGenerationalForTest(root.resolve(owner), peers.get(i).getId(), group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(5000), limits(), stage -> {
                if (stage == RatisWalState.Stage.AFTER_IMAGE_PUBLISHED && owner.equals(publicationFailureOwner) && failPublication.compareAndSet(true, false)) { throw new IOException("injected error after durable image publication"); }
            })); } }
            catch (Exception failure) { close(); throw failure; }
        }
        /** 角色和真实多数派屏障都满足才使用节点。 */
        int leader() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (System.nanoTime() < until) { for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (node == null || node.leaderEpoch() < 1) { continue; } try { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.rollback(tx); return i; } catch (WalMvccException roleChanged) { } } TimeUnit.MILLISECONDS.sleep(20); } throw new AssertionError("leader missing"); }
        /** 每笔在两个逻辑分片原子写入。 */
        WalReceipt write(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { node.put(tx, shard, "k", (shard + value).getBytes(StandardCharsets.UTF_8)); } return node.commit(tx); }
        /** 快照输出期间也使用独立 RR 语句读取完整双片值。 */
        void verify(RatisWalNode node, int value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(tx); for (String shard : List.of("a", "b")) { assertEquals(shard + value, new String(node.get(tx, shard, "k", view).orElseThrow(), StandardCharsets.UTF_8)); } node.rollback(tx); }
        /** 异常也解除导出门闩，关闭全部自有节点与控制池。 */
        @Override public void close() throws Exception { gate.release.countDown(); Exception failure = null; for (var node : nodes) { if (node != null) { try { node.close(); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } } control.close(); if (failure != null) { throw failure; } }
    }
    /** 镜像预算独立于正常事务超时。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000); }

    /** 只在镜像后台导出时暂停，事务、checkpoint 和恢复仍由真实 MySQL 执行。 */
    private record ImageStore(WalMaterializedStore delegate, String owner, Gate gate) implements WalMaterializedStore {
        /** 保留正式后端容量校验。 */
        @Override public void validateMutation(WalMutation mutation) { delegate.validateMutation(mutation); }
        /** 保留普通绑定拒绝规则。 */
        @Override public void initialize(UUID engine, String shard) { delegate.initialize(engine, shard); }
        /** 保留复制能力声明。 */
        @Override public boolean supportsReplicaOwnership() { return delegate.supportsReplicaOwnership(); }
        /** 保留接收副本所有权。 */
        @Override public void initializeReplica(WalReplicaIdentity identity, String shard) { delegate.initializeReplica(identity, shard); }
        /** 直接查询真实物化前缀。 */
        @Override public WalCheckpoint checkpoint() { return delegate.checkpoint(); }
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
        @Override public SnapshotView openSnapshot(WalCheckpoint boundary) { var view = delegate.openSnapshot(boundary); return new SnapshotView() {
            /** 在实际固定读视图上延迟流式输出。 */
            @Override public void export(BackupSink sink) throws IOException { gate.hit(owner); view.export(sink); }
            /** 只关闭本视图的实际数据库事务。 */
            @Override public void close() throws IOException { view.close(); }
        }; }
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

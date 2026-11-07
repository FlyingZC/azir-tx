package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.*;
import com.flyingzc.azir.wal.*;
import com.flyingzc.azir.wal.ha.example.IdempotentTransfer;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实三节点 TLS 和 MySQL 上验证托管生命周期、全局快照、未知结果及业务幂等。 */
class RatisManagedNodeTest {
    @TempDir Path directory;

    /** 清理失败必须保留句柄，重试仍失败时不能提前标记已释放。 */
    @Test void failedStartupCleanupRetainsOwnershipUntilActualClose() throws Exception {
        var count = new AtomicInteger(); var failure = new IOException("startup");
        var pending = new RatisManagedNode.StartupException(failure, () -> { if (count.incrementAndGet() == 1) { throw new IOException("close"); } });
        assertSame(failure, pending.getCause()); assertTrue(pending.cleanupPending()); assertThrows(IOException.class, pending::retryCleanup); assertTrue(pending.cleanupPending()); pending.retryCleanup(); assertFalse(pending.cleanupPending()); pending.retryCleanup(); assertEquals(2, count.get());
    }

    /** 不合法角色、路由、端点或 TLS 身份须在创建后端和目录之前被拒绝。 */
    @Test void invalidAssemblyFailsBeforeCreatingRuntimeData() throws Exception {
        var groupId = UUID.randomUUID(); var peers = new ArrayList<RaftPeer>(); var pki = new RatisTlsTestCertificates(directory.resolve("pki")); var entries = new ArrayList<RatisTlsTestCertificates.Entry>();
        for (int i = 0; i < 3; i++) { String address; try (var port = MysqlGenerationSupport.reservePeerPort()) { address = "localhost:" + port.getLocalPort(); } peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(address).build()); entries.add(pki.issue(groupId, "n" + i, address)); }
        var trust = new ArrayList<RatisTlsSecurity.PeerTrust>(); for (var entry : entries) { trust.add(entry.trust()); }
        var entry = entries.getFirst(); var security = RatisTlsSecurity.load(groupId, entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust); var group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
        try (var pool = new HikariDataSource()) {
            pool.setPassword("unlogged-test-password"); Path root = directory.resolve("unopened");
            assertThrows(IllegalArgumentException.class, () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), RaftGroup.valueOf(group.getGroupId(), peers.subList(0, 2)), Map.of("a", pool), security));
            var duplicate = new ArrayList<>(peers); duplicate.set(1, RaftPeer.newBuilder(peers.get(1)).setAddress(peers.getFirst().getAddress()).build());
            assertThrows(IllegalArgumentException.class, () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), RaftGroup.valueOf(group.getGroupId(), duplicate), Map.of("a", pool), security));
            assertThrows(IllegalArgumentException.class, () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), group, Map.of(), security));
            assertThrows(IllegalArgumentException.class, () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), group, Map.of("bad\u0000route", pool), security));
            var wrong = RatisManagedNode.Config.defaults(root, peers.get(1).getId(), group, Map.of("a", pool), security);
            assertThrows(IOException.class, () -> RatisManagedNode.open(wrong)); assertFalse(Files.exists(root)); assertFalse(pool.isClosed());
            var valid = RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), group, Map.of("a", pool), security); assertFalse(valid.toString().contains("unlogged-test-password"));
            Path other = directory.resolve("other"); Files.createDirectory(other); Files.createSymbolicLink(root, other); assertThrows(IOException.class, () -> RatisManagedNode.open(valid));
            try (var files = Files.list(other)) { assertEquals(0, files.count()); }
        }
    }

    /** 语句水位、异常退出、写冲突、只读凭据及维护策略重启均经正式装配。 */
    @Test void sessionsPreserveIsolationAndBorrowedPoolOwnership() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader);
            var first = node.execute(IsolationLevel.REPEATABLE_READ, tx -> { tx.put("a", "k", new byte[]{1}); tx.put("b", "k", new byte[]{1}); return "ok"; });
            assertEquals("ok", first.value()); assertTrue(first.durableDecision()); assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(first.receipt().transactionId()).status());
            try (var rc = node.begin(IsolationLevel.READ_COMMITTED); var rr = node.begin(IsolationLevel.REPEATABLE_READ)) {
                var old = rc.statement(); assertArrayEquals(new byte[]{1}, old.get("a", "k").orElseThrow()); assertArrayEquals(new byte[]{1}, rr.get("b", "k").orElseThrow());
                node.execute(IsolationLevel.REPEATABLE_READ, tx -> { tx.put("a", "k", new byte[]{2}); tx.put("b", "k", new byte[]{2}); return null; });
                assertArrayEquals(new byte[]{1}, old.get("b", "k").orElseThrow()); assertArrayEquals(new byte[]{2}, rc.get("b", "k").orElseThrow()); assertArrayEquals(new byte[]{1}, rr.get("a", "k").orElseThrow());
                rr.put("a", "k", new byte[]{3}); assertArrayEquals(new byte[]{3}, rr.get("a", "k").orElseThrow()); rr.delete("a", "k"); assertTrue(rr.get("a", "k").isEmpty());
            }
            var abandoned = node.begin(IsolationLevel.REPEATABLE_READ); var view = abandoned.statement(); abandoned.put("a", "missing", new byte[]{9}); abandoned.close(); abandoned.close(); assertEquals(RatisTransaction.State.ROLLED_BACK, abandoned.state()); assertThrows(IllegalStateException.class, () -> view.get("a", "k"));
            var callback = new AtomicInteger(); assertThrows(IOException.class, () -> node.execute(IsolationLevel.REPEATABLE_READ, tx -> { callback.incrementAndGet(); tx.put("b", "missing", new byte[]{9}); throw new IOException("business error"); })); assertEquals(1, callback.get());
            try (var tx = node.begin(IsolationLevel.REPEATABLE_READ)) { assertTrue(tx.get("a", "missing").isEmpty()); assertTrue(tx.get("b", "missing").isEmpty()); }
            try (var one = node.begin(IsolationLevel.REPEATABLE_READ); var two = node.begin(IsolationLevel.REPEATABLE_READ)) {
                one.put("a", "k", new byte[]{4}); two.put("a", "k", new byte[]{5}); var receipt = one.commit(); assertEquals(receipt, one.commit()); assertEquals(RatisTransaction.State.COMMITTED, one.state());
                assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, two::commit).errorCode()); assertEquals(RatisTransaction.State.ROLLED_BACK, two.state());
            }
            var read = node.execute(IsolationLevel.REPEATABLE_READ, tx -> tx.get("a", "k")); assertFalse(read.durableDecision()); assertEquals(WalCommitLookup.Status.NOT_CONFIRMED, node.lookupCommit(read.receipt().transactionId()).status());
            assertTrue(node.status().health().enabled()); assertTrue(node.status().gc().enabled()); assertTrue(node.status().snapshot().enabled());
            int follower = (leader + 1) % 3; assertFalse(cluster.nodes.get(follower).status().writableHint()); cluster.stop(follower); assertFalse(cluster.pools.get(follower).isClosed());
            var config = cluster.configs.get(follower); var oldMaintenance = config.maintenance();
            var changed = new RatisManagedNode.Maintenance(new RatisHealthOptions(2000, 3, 3, 1000, 10_000), oldMaintenance.gc(), oldMaintenance.snapshot(), 1000, 15_000, 1000);
            var wrong = new RatisManagedNode.Config(config.directory(), config.node(), config.group(), config.controls(), config.security(), config.transactions(), config.backups(), config.namespaces(), config.transport(), changed);
            assertThrows(IOException.class, () -> RatisManagedNode.open(wrong)); cluster.start(follower);
            cluster.await(() -> cluster.nodes.get(follower).status().localReady());
            cluster.await(() -> node.status().writableHint() && node.status().storage() != null && node.status().generation() != null);
            cluster.stop(leader); assertFalse(node.status().writableHint()); assertFalse(cluster.pools.get(leader).isClosed()); assertThrows(IllegalStateException.class, () -> node.begin(IsolationLevel.REPEATABLE_READ));
        }
    }

    /** 已多数派落盘而本地发布被延迟、客户端等待超时时，关闭会话不可回滚实际提交。 */
    @Test void committedButTimedOutSessionRetainsOriginalUuidAndCannotRollback() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var pause = cluster.pauses.get(leader); var session = node.begin(IsolationLevel.REPEATABLE_READ); UUID id = session.id();
            session.put("a", "unknown", new byte[]{7}); session.put("b", "unknown", new byte[]{8}); pause.arm();
            try (var executor = Executors.newSingleThreadExecutor()) {
                var attempt = executor.submit(() -> assertThrows(WalMvccException.class, session::commit));
                try {
                    assertTrue(pause.entered.await(15, TimeUnit.SECONDS)); var failure = attempt.get(15, TimeUnit.SECONDS); assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode()); assertEquals(id, failure.transactionId()); assertEquals(RatisTransaction.State.UNKNOWN, session.state());
                    assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class, session::rollback).errorCode()); session.close(); assertEquals(RatisTransaction.State.UNKNOWN, session.state());
                } finally { pause.release.countDown(); }
            }
            cluster.await(() -> node.status().writableHint()); assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(id).status());
            node.execute(IsolationLevel.REPEATABLE_READ, tx -> { var view = tx.statement(); assertArrayEquals(new byte[]{7}, view.get("a", "unknown").orElseThrow()); assertArrayEquals(new byte[]{8}, view.get("b", "unknown").orElseThrow()); return null; });
        }
    }

    /** 顺序、并发及提交回复丢失的同一业务单号均只扣一次，拒绝换参、超额及整数溢出。 */
    @Test void crossShardTransferCanSafelyRetryItsBusinessOperation() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var node = cluster.nodes.get(leader); var service = new IdempotentTransfer(node, "a");
            var from = new IdempotentTransfer.Account("a", "alice"); var to = new IdempotentTransfer.Account("b", "bob"); var full = new IdempotentTransfer.Account("b", "full");
            service.initializeAccounts(Map.of(from, 1000L, to, 1000L, full, Long.MAX_VALUE));
            var request = new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 25); var first = service.transfer(request); var repeat = service.transfer(request);
            assertEquals(first.applied(), repeat.applied()); assertTrue(first.receipt().isPresent()); assertTrue(repeat.receipt().isEmpty()); assertEquals(975, service.balance(from)); assertEquals(1025, service.balance(to));
            assertThrows(IllegalArgumentException.class, () -> service.transfer(new IdempotentTransfer.Request(request.operation(), from, to, 26)));
            assertThrows(IllegalArgumentException.class, () -> service.transfer(new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 2000)));
            assertThrows(ArithmeticException.class, () -> service.transfer(new IdempotentTransfer.Request(UUID.randomUUID(), from, full, 1)));
            assertThrows(IllegalStateException.class, () -> service.initializeAccounts(Map.of(from, 0L))); assertEquals(975, service.balance(from)); assertEquals(Long.MAX_VALUE, service.balance(full));
            var concurrent = new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 10); var start = new CountDownLatch(1);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var tasks = new ArrayList<Future<IdempotentTransfer.Result>>();
                for (int i = 0; i < 2; i++) { tasks.add(executor.submit(() -> { start.await(); return service.transfer(concurrent); })); }
                start.countDown(); int success = 0;
                for (var task : tasks) { try { assertEquals(concurrent, task.get(20, TimeUnit.SECONDS).applied().request()); success++; } catch (ExecutionException failure) { assertInstanceOf(WalMvccException.class, failure.getCause()); assertEquals(ErrorCode.WRITE_CONFLICT, ((WalMvccException) failure.getCause()).errorCode()); } }
                assertTrue(success >= 1);
            }
            var original = service.transfer(concurrent).applied(); assertEquals(965, service.balance(from)); assertEquals(1035, service.balance(to)); assertEquals(original, service.find(concurrent.operation()).orElseThrow());
            var unknown = new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 5); var pause = cluster.pauses.get(leader); pause.arm();
            try (var executor = Executors.newSingleThreadExecutor()) {
                var task = executor.submit(() -> assertThrows(WalMvccException.class, () -> service.transfer(unknown)));
                try { assertTrue(pause.entered.await(15, TimeUnit.SECONDS)); assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, task.get(15, TimeUnit.SECONDS).errorCode()); }
                finally { pause.release.countDown(); }
            }
            cluster.await(() -> node.status().writableHint()); var resolved = service.transfer(unknown); assertTrue(resolved.receipt().isEmpty()); assertEquals(960, service.balance(from)); assertEquals(1040, service.balance(to)); assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(resolved.applied().transaction()).status());
            cluster.stop(leader); int next = cluster.leader(); var successor = new IdempotentTransfer(cluster.nodes.get(next), "a"); assertEquals(resolved.applied(), successor.transfer(unknown).applied()); assertEquals(960, successor.balance(from)); assertEquals(1040, successor.balance(to));
        }
    }

    /** 只在真实多数派提交后、本地发布前延迟，不伪造共识成功。 */
    private static final class Pause implements RatisWalState.Hook {
        final AtomicBoolean armed = new AtomicBoolean(); CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        /** 每个测试至多启用一次延迟点。 */
        void arm() { armed.set(true); }
        /** 等待超过事务期限，调用方在 finally 中释放，清理不依赖中断 WAL。 */
        @Override public void hit(RatisWalState.Stage stage) throws IOException {
            if (stage != RatisWalState.Stage.BEFORE_COMMIT_APPLIED || !armed.compareAndSet(true, false)) { return; }
            entered.countDown(); try { if (!release.await(30, TimeUnit.SECONDS)) { throw new IOException("test apply release timed out"); } } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
        }
    }
    /** 一个限额 MySQL 仅用于 API 语义测试，每个节点独立 schema；此夹具不充当物理容灾证据。 */
    private static final class Cluster implements AutoCloseable {
        final List<RatisManagedNode> nodes = new ArrayList<>(); final List<RatisManagedNode.Config> configs = new ArrayList<>(); final List<HikariDataSource> pools = new ArrayList<>(); final List<Pause> pauses = new ArrayList<>();
        final org.testcontainers.containers.MySQLContainer<?> database = MysqlGenerationSupport.database();
        /** 临时 TLS、路由、目录均为本测试所有，失败也关闭已成功创建的资源。 */
        Cluster(Path directory) throws Exception {
            try {
                database.start(); var id = UUID.randomUUID(); var peers = new ArrayList<RaftPeer>(); var entries = new ArrayList<RatisTlsTestCertificates.Entry>(); var pki = new RatisTlsTestCertificates(directory.resolve("pki"));
                for (int i = 0; i < 3; i++) { String address; try (var port = MysqlGenerationSupport.reservePeerPort()) { address = "localhost:" + port.getLocalPort(); } peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(address).build()); entries.add(pki.issue(id, "n" + i, address)); nodes.add(null); pauses.add(new Pause()); }
                var group = RaftGroup.valueOf(RaftGroupId.valueOf(id), peers); var trust = new ArrayList<RatisTlsSecurity.PeerTrust>(); for (var entry : entries) { trust.add(entry.trust()); }
                for (int i = 0; i < 3; i++) {
                    String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(database, schema); var pool = MysqlGenerationSupport.control(database, schema); pools.add(pool); var entry = entries.get(i);
                    var security = RatisTlsSecurity.load(id, entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust);
                    var defaults = RatisManagedNode.Config.defaults(directory.resolve("n" + i), peers.get(i).getId(), group, Map.of("a", pool, "b", pool), security);
                    configs.add(new RatisManagedNode.Config(defaults.directory(), defaults.node(), group, defaults.controls(), security, defaults.transactions().withTimeout(5000), defaults.backups(), defaults.namespaces(), defaults.transport(), defaults.maintenance())); start(i);
                }
            } catch (Exception failure) { try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } throw failure; }
        }
        /** 使用与公开生产入口相同的装配，仅增加包内真实操作观察钩子。 */
        void start(int index) throws Exception { nodes.set(index, RatisManagedNode.open(configs.get(index), pauses.get(index))); }
        /** 不丢失关闭失败的资源句柄。 */
        void stop(int index) throws Exception { var node = nodes.get(index); if (node != null) { node.close(); nodes.set(index, null); } }
        /** 由近期多数派证明选取领导者，不能按启动先后猜测。 */
        int leader() throws Exception { var found = new AtomicInteger(-1); await(() -> { for (int i = 0; i < nodes.size(); i++) { if (nodes.get(i) != null && nodes.get(i).status().writableHint()) { found.set(i); return true; } } return false; }); return found.get(); }
        /** 有限期限内等待真实状态，不因开发机瞬时负载把固定睡眠当作同步。 */
        void await(java.util.function.BooleanSupplier predicate) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (!predicate.getAsBoolean() && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } assertTrue(predicate.getAsBoolean(), "managed cluster did not reach expected state"); }
        /** 先释放自有暂停钩子，再关闭节点、借出的池及数据库。 */
        @Override public void close() throws Exception {
            for (var pause : pauses) { pause.release.countDown(); } Exception error = null;
            for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Exception failure) { if (error == null) { error = failure; } else { error.addSuppressed(failure); } } }
            for (var pool : pools) { pool.close(); } database.stop(); if (error != null) { throw error; }
        }
    }
}

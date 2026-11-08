/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMvccException;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;

import com.flyingzc.azir.wal.ha.example.IdempotentTransfer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;

/**
 * Real three nodes TLS and MySQL on verify hosting life cycle, global snapshot, unknown results and operations idempotent.
 */
class RatisManagedNodeTest {

    @TempDir
    Path directory;

    /**
     * Clean-up failure must keep the handle, and retry cannot mark release in advance when it still fails.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failedStartupCleanupRetainsOwnershipUntilActualClose() throws Exception {
        var count = new AtomicInteger();
        var failure = new IOException("startup");
        var pending = new RatisManagedNode.StartupException(failure, () -> {
            if (count.incrementAndGet() == 1) {
                throw new IOException("close");
            }
        });
        Assertions.assertSame(failure, pending.getCause());
        assertTrue(pending.cleanupPending());
        assertThrows(IOException.class, pending::retryCleanup);
        assertTrue(pending.cleanupPending());
        pending.retryCleanup();
        assertFalse(pending.cleanupPending());
        pending.retryCleanup();
        Assertions.assertEquals(2, count.get());
    }

    /**
     * Invalid roles, routers, endpoints or TLS identity must be rejected before creating a backend and directory.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidAssemblyFailsBeforeCreatingRuntimeData() throws Exception {
        var groupId = UUID.randomUUID();
        var peers = new ArrayList<RaftPeer>();
        var pki = new RatisTlsTestCertificates(directory.resolve("pki"));
        var entries = new ArrayList<RatisTlsTestCertificates.Entry>();
        for (int i = 0; i < 3; i++) {
            String address;
            try (var port = MysqlGenerationSupport.reservePeerPort()) {
                address = "localhost:" + port.getLocalPort();
            }
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(address).build());
            entries.add(pki.issue(groupId, "n" + i, address));
        }
        var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
        for (var entry : entries) {
            trust.add(entry.trust());
        }
        var entry = entries.getFirst();
        var security = RatisTlsSecurity.load(groupId, entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust);
        var group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
        try (var pool = new HikariDataSource()) {
            pool.setPassword("unlogged-test-password");
            Path root = directory.resolve("unopened");
            assertThrows(IllegalArgumentException.class,
                    () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), RaftGroup.valueOf(group.getGroupId(), peers.subList(0, 2)), Map.of("a", pool), security));
            var duplicate = new ArrayList<>(peers);
            duplicate.set(1, RaftPeer.newBuilder(peers.get(1)).setAddress(peers.getFirst().getAddress()).build());
            assertThrows(IllegalArgumentException.class,
                    () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), RaftGroup.valueOf(group.getGroupId(), duplicate), Map.of("a", pool), security));
            assertThrows(IllegalArgumentException.class, () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), group, Map.of(), security));
            assertThrows(IllegalArgumentException.class, () -> RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), group, Map.of("bad\u0000route", pool), security));
            var wrong = RatisManagedNode.Config.defaults(root, peers.get(1).getId(), group, Map.of("a", pool), security);
            assertThrows(IOException.class, () -> RatisManagedNode.open(wrong));
            assertFalse(Files.exists(root));
            assertFalse(pool.isClosed());
            var valid = RatisManagedNode.Config.defaults(root, peers.getFirst().getId(), group, Map.of("a", pool), security);
            assertFalse(valid.toString().contains("unlogged-test-password"));
            Path other = directory.resolve("other");
            Files.createDirectory(other);
            Files.createSymbolicLink(root, other);
            assertThrows(IOException.class, () -> RatisManagedNode.open(valid));
            try (var files = Files.list(other)) {
                Assertions.assertEquals(0, files.count());
            }
        }
    }

    /**
     * Statements watermark, abnormal exit, write conflict, read-only receipt and maintenance policy restarts are formally assembled.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void sessionsPreserveIsolationAndBorrowedPoolOwnership() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var first = node.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                tx.put("a", "k", new byte[]{1});
                tx.put("b", "k", new byte[]{1});
                return "ok";
            });
            Assertions.assertEquals("ok", first.value());
            assertTrue(first.durableDecision());
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(first.receipt().transactionId()).status());
            try (var rc = node.begin(IsolationLevel.READ_COMMITTED); var rr = node.begin(IsolationLevel.REPEATABLE_READ)) {
                var old = rc.statement();
                assertArrayEquals(new byte[]{1}, old.get("a", "k").orElseThrow());
                assertArrayEquals(new byte[]{1}, rr.get("b", "k").orElseThrow());
                node.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                    tx.put("a", "k", new byte[]{2});
                    tx.put("b", "k", new byte[]{2});
                    return null;
                });
                assertArrayEquals(new byte[]{1}, old.get("b", "k").orElseThrow());
                assertArrayEquals(new byte[]{2}, rc.get("b", "k").orElseThrow());
                assertArrayEquals(new byte[]{1}, rr.get("a", "k").orElseThrow());
                rr.put("a", "k", new byte[]{3});
                assertArrayEquals(new byte[]{3}, rr.get("a", "k").orElseThrow());
                rr.delete("a", "k");
                assertTrue(rr.get("a", "k").isEmpty());
            }
            var abandoned = node.begin(IsolationLevel.REPEATABLE_READ);
            var view = abandoned.statement();
            abandoned.put("a", "missing", new byte[]{9});
            abandoned.close();
            abandoned.close();
            Assertions.assertEquals(RatisTransaction.State.ROLLED_BACK, abandoned.state());
            assertThrows(IllegalStateException.class, () -> view.get("a", "k"));
            var callback = new AtomicInteger();
            assertThrows(IOException.class, () -> node.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                callback.incrementAndGet();
                tx.put("b", "missing", new byte[]{9});
                throw new IOException("business error");
            }));
            Assertions.assertEquals(1, callback.get());
            try (var tx = node.begin(IsolationLevel.REPEATABLE_READ)) {
                assertTrue(tx.get("a", "missing").isEmpty());
                assertTrue(tx.get("b", "missing").isEmpty());
            }
            try (var one = node.begin(IsolationLevel.REPEATABLE_READ); var two = node.begin(IsolationLevel.REPEATABLE_READ)) {
                one.put("a", "k", new byte[]{4});
                two.put("a", "k", new byte[]{5});
                var receipt = one.commit();
                Assertions.assertEquals(receipt, one.commit());
                Assertions.assertEquals(RatisTransaction.State.COMMITTED, one.state());
                Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, two::commit).errorCode());
                Assertions.assertEquals(RatisTransaction.State.ROLLED_BACK, two.state());
            }
            var read = node.execute(IsolationLevel.REPEATABLE_READ, tx -> tx.get("a", "k"));
            assertFalse(read.durableDecision());
            Assertions.assertEquals(WalCommitLookup.Status.NOT_CONFIRMED, node.lookupCommit(read.receipt().transactionId()).status());
            assertTrue(node.status().health().enabled());
            assertTrue(node.status().gc().enabled());
            assertTrue(node.status().snapshot().enabled());
            int follower = (leader + 1) % 3;
            assertFalse(cluster.nodes.get(follower).status().writableHint());
            cluster.stop(follower);
            assertFalse(cluster.pools.get(follower).isClosed());
            var config = cluster.configs.get(follower);
            var oldMaintenance = config.maintenance();
            var changed = new RatisManagedNode.Maintenance(new RatisHealthOptions(2000, 3, 3, 1000, 10_000), oldMaintenance.gc(), oldMaintenance.snapshot(), 1000, 15_000, 1000);
            var wrong = new RatisManagedNode.Config(config.directory(), config.node(), config.group(), config.controls(), config.security(), config.transactions(), config.backups(),
                    config.namespaces(), config.transport(), changed);
            assertThrows(IOException.class, () -> RatisManagedNode.open(wrong));
            cluster.start(follower);
            cluster.await(() -> cluster.nodes.get(follower).status().localReady());
            cluster.await(() -> node.status().writableHint() && node.status().storage() != null && node.status().generation() != null);
            cluster.stop(leader);
            assertFalse(node.status().writableHint());
            assertFalse(cluster.pools.get(leader).isClosed());
            assertThrows(IllegalStateException.class, () -> node.begin(IsolationLevel.REPEATABLE_READ));
        }
    }

    /**
     * Session cannot be rollback actual commit when majority drops and local release is delayed and the client is waiting for timeout.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void committedButTimedOutSessionRetainsOriginalUuidAndCannotRollback() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var pause = cluster.pauses.get(leader);
            var session = node.begin(IsolationLevel.REPEATABLE_READ);
            UUID id = session.id();
            session.put("a", "unknown", new byte[]{7});
            session.put("b", "unknown", new byte[]{8});
            pause.arm();
            try (var executor = Executors.newSingleThreadExecutor()) {
                var attempt = executor.submit(() -> assertThrows(WalMvccException.class, session::commit));
                try {
                    assertTrue(pause.entered.await(15, TimeUnit.SECONDS));
                    var failure = attempt.get(15, TimeUnit.SECONDS);
                    Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
                    Assertions.assertEquals(id, failure.transactionId());
                    Assertions.assertEquals(RatisTransaction.State.UNKNOWN, session.state());
                    Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class, session::rollback).errorCode());
                    session.close();
                    Assertions.assertEquals(RatisTransaction.State.UNKNOWN, session.state());
                } finally {
                    pause.release.countDown();
                }
            }
            cluster.await(() -> node.status().writableHint());
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(id).status());
            node.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                var view = tx.statement();
                assertArrayEquals(new byte[]{7}, view.get("a", "unknown").orElseThrow());
                assertArrayEquals(new byte[]{8}, view.get("b", "unknown").orElseThrow());
                return null;
            });
        }
    }

    /**
     * The order, concurrent and commit returns the lost same business unit number only once, refusing to switch, over and integer spills.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void crossShardTransferCanSafelyRetryItsBusinessOperation() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var service = new IdempotentTransfer(node, "a");
            var from = new IdempotentTransfer.Account("a", "alice");
            var to = new IdempotentTransfer.Account("b", "bob");
            var full = new IdempotentTransfer.Account("b", "full");
            service.initializeAccounts(Map.of(from, 1000L, to, 1000L, full, Long.MAX_VALUE));
            var request = new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 25);
            var first = service.transfer(request);
            var repeat = service.transfer(request);
            Assertions.assertEquals(first.applied(), repeat.applied());
            assertTrue(first.receipt().isPresent());
            assertTrue(repeat.receipt().isEmpty());
            Assertions.assertEquals(975, service.balance(from));
            Assertions.assertEquals(1025, service.balance(to));
            assertThrows(IllegalArgumentException.class, () -> service.transfer(new IdempotentTransfer.Request(request.operation(), from, to, 26)));
            assertThrows(IllegalArgumentException.class, () -> service.transfer(new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 2000)));
            assertThrows(ArithmeticException.class, () -> service.transfer(new IdempotentTransfer.Request(UUID.randomUUID(), from, full, 1)));
            assertThrows(IllegalStateException.class, () -> service.initializeAccounts(Map.of(from, 0L)));
            Assertions.assertEquals(975, service.balance(from));
            Assertions.assertEquals(Long.MAX_VALUE, service.balance(full));
            var concurrent = new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 10);
            var start = new CountDownLatch(1);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var tasks = new ArrayList<Future<IdempotentTransfer.Result>>();
                for (int i = 0; i < 2; i++) {
                    tasks.add(executor.submit(() -> {
                        start.await();
                        return service.transfer(concurrent);
                    }));
                }
                start.countDown();
                int success = 0;
                for (var task : tasks) {
                    try {
                        Assertions.assertEquals(concurrent, task.get(20, TimeUnit.SECONDS).applied().request());
                        success++;
                    } catch (final ExecutionException failure) {
                        Assertions.assertInstanceOf(WalMvccException.class, failure.getCause());
                        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, ((WalMvccException) failure.getCause()).errorCode());
                    }
                }
                assertTrue(success >= 1);
            }
            var original = service.transfer(concurrent).applied();
            Assertions.assertEquals(965, service.balance(from));
            Assertions.assertEquals(1035, service.balance(to));
            Assertions.assertEquals(original, service.find(concurrent.operation()).orElseThrow());
            var unknown = new IdempotentTransfer.Request(UUID.randomUUID(), from, to, 5);
            var pause = cluster.pauses.get(leader);
            pause.arm();
            try (var executor = Executors.newSingleThreadExecutor()) {
                var task = executor.submit(() -> assertThrows(WalMvccException.class, () -> service.transfer(unknown)));
                try {
                    assertTrue(pause.entered.await(15, TimeUnit.SECONDS));
                    Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, task.get(15, TimeUnit.SECONDS).errorCode());
                } finally {
                    pause.release.countDown();
                }
            }
            cluster.await(() -> node.status().writableHint());
            var resolved = service.transfer(unknown);
            assertTrue(resolved.receipt().isEmpty());
            Assertions.assertEquals(960, service.balance(from));
            Assertions.assertEquals(1040, service.balance(to));
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(resolved.applied().transaction()).status());
            cluster.stop(leader);
            int next = cluster.leader();
            var successor = new IdempotentTransfer(cluster.nodes.get(next), "a");
            Assertions.assertEquals(resolved.applied(), successor.transfer(unknown).applied());
            Assertions.assertEquals(960, successor.balance(from));
            Assertions.assertEquals(1040, successor.balance(to));
        }
    }

    /**
     * Only after the real majority commit, and before the release of local, do not forge consensus success.
     */
    private static final class Pause implements RatisWalState.Hook {

        final AtomicBoolean armed = new AtomicBoolean();

        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);

        /**
         * Up to one delay point for each test.
         */
        void arm() {
            armed.set(true);
        }

        /**
         * Waits to exceed the transaction deadline, the caller is released in final and clean-up does not depend on interrupted WAL.
         *
         * @param stage stage
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void hit(final RatisWalState.Stage stage) throws IOException {
            if (stage != RatisWalState.Stage.BEFORE_COMMIT_APPLIED || !armed.compareAndSet(true, false)) {
                return;
            }
            entered.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("test apply release timed out");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        }
    }

    /**
     * A MySQL limit is used only for an API semantic test, and each node is independent of the schema; this binder does not serve as evidence of a physical disaster.
     */
    private static final class Cluster implements AutoCloseable {

        final List<RatisManagedNode> nodes = new ArrayList<>();

        final List<RatisManagedNode.Config> configs = new ArrayList<>();

        final List<HikariDataSource> pools = new ArrayList<>();

        final List<Pause> pauses = new ArrayList<>();

        final org.testcontainers.containers.MySQLContainer<?> database = MysqlGenerationSupport.database();

        /**
         * Temporary TLS, route and directory are all for this test and failed to shut down the resources that have been successfully created.
         *
         * @param directory directory
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path directory) throws Exception {
            try {
                database.start();
                var id = UUID.randomUUID();
                var peers = new ArrayList<RaftPeer>();
                var entries = new ArrayList<RatisTlsTestCertificates.Entry>();
                var pki = new RatisTlsTestCertificates(directory.resolve("pki"));
                for (int i = 0; i < 3; i++) {
                    String address;
                    try (var port = MysqlGenerationSupport.reservePeerPort()) {
                        address = "localhost:" + port.getLocalPort();
                    }
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(address).build());
                    entries.add(pki.issue(id, "n" + i, address));
                    nodes.add(null);
                    pauses.add(new Pause());
                }
                var group = RaftGroup.valueOf(RaftGroupId.valueOf(id), peers);
                var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
                for (var entry : entries) {
                    trust.add(entry.trust());
                }
                for (int i = 0; i < 3; i++) {
                    String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
                    MysqlGenerationSupport.createControl(database, schema);
                    var pool = MysqlGenerationSupport.control(database, schema);
                    pools.add(pool);
                    var entry = entries.get(i);
                    var security = RatisTlsSecurity.load(id, entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust);
                    var defaults = RatisManagedNode.Config.defaults(directory.resolve("n" + i), peers.get(i).getId(), group, Map.of("a", pool, "b", pool), security);
                    configs.add(new RatisManagedNode.Config(defaults.directory(), defaults.node(), group, defaults.controls(), security, defaults.transactions().withTimeout(5000), defaults.backups(),
                            defaults.namespaces(), defaults.transport(), defaults.maintenance()));
                    start(i);
                }
            } catch (final Exception failure) {
                try {
                    close();
                } catch (final Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        /**
         * Use the same assembly as open production entrances, with only real-action observation hooks added to the package.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void start(final int index) throws Exception {
            nodes.set(index, RatisManagedNode.open(configs.get(index), pauses.get(index)));
        }

        /**
         * Do not lose the failed resource handle.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void stop(final int index) throws Exception {
            var node = nodes.get(index);
            if (node != null) {
                node.close();
                nodes.set(index, null);
            }
        }

        /**
         * The selection of a leader by the recent majority does not follow the sequence of start-up.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        int leader() throws Exception {
            var found = new AtomicInteger(-1);
            await(() -> {
                for (int i = 0; i < nodes.size(); i++) {
                    if (nodes.get(i) != null && nodes.get(i).status().writableHint()) {
                        found.set(i);
                        return true;
                    }
                }
                return false;
            });
            return found.get();
        }

        /**
         * Waiting for the real state of affairs for a limited period of time does not allow fixed sleep to be synchronized as an instant load by the developer.
         *
         * @param predicate predicate
         * @throws Exception if the operation cannot complete
         */
        void await(final java.util.function.BooleanSupplier predicate) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (!predicate.getAsBoolean() && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(30);
            }
            assertTrue(predicate.getAsBoolean(), "managed cluster did not reach expected state");
        }

        /**
         * First release your own suspended hook, then close the node, the borrowed pool and database.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            for (var pause : pauses) {
                pause.release.countDown();
            }
            Exception error = null;
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    stop(i);
                } catch (final Exception failure) {
                    if (error == null) {
                        error = failure;
                    } else {
                        error.addSuppressed(failure);
                    }
                }
            }
            for (var pool : pools) {
                pool.close();
            }
            database.stop();
            if (error != null) {
                throw error;
            }
        }
    }
}

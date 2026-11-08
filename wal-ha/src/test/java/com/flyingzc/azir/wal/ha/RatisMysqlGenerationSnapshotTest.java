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

import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.client.RaftClient;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Three stand-alone MySQL cold replica verify actual snapshot installation, new generation and concurrent operations and master maintenance.
 */
class RatisMysqlGenerationSnapshotTest {

    @TempDir
    Path directory;

    /**
     * Backward nodes must be from snapshot recovery cold data, and the remaining majority will remain operational transaction complete during the time of installation.
     *
     * @param authenticated authenticated
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void independentMysqlGenerationsInstallColdSnapshotDuringConcurrentBusiness(final boolean authenticated) throws Exception {
        try (var cluster = new Cluster(directory, authenticated)) {
            int leader = cluster.leader(-1);
            if (authenticated) {
                for (var replica : cluster.nodes) {
                    replica.enableVersionGc(new RatisGcOptions(200, 64, 32, 100, 15_000));
                }
            }
            var node = cluster.nodes.get(leader);
            var initial = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : Set.of("a", "b")) {
                for (int i = 0; i < 32; i++) {
                    node.put(initial, shard, "cold" + i, bytes(shard + ":" + i));
                }
                node.put(initial, shard, "blob", new byte[]{0});
                node.put(initial, shard, "live", bytes("0"));
            }
            var receipt = node.commit(initial);
            int lagging = (leader + 1) % 3;
            cluster.cold(lagging, 1);
            var before = cluster.active(lagging);
            cluster.stop(lagging);
            byte[] a = null, b = null;
            for (int i = 1; i <= 10; i++) {
                a = new byte[480 * 1024];
                b = new byte[480 * 1024];
                Arrays.fill(a, (byte) i);
                Arrays.fill(b, (byte) (i + 100));
                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                node.put(tx, "a", "blob", a);
                node.put(tx, "b", "blob", b);
                Assertions.assertEquals(i + 1, node.commit(tx).commitSequence());
            }
            var deletion = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : Set.of("a", "b")) {
                node.delete(deletion, shard, "cold0");
            }
            Assertions.assertEquals(12, node.commit(deletion).commitSequence());
            long snapshot = node.snapshot();
            Assertions.assertEquals(0, node.localStats().retainedBytes());
            Assertions.assertEquals(100, node.localStats().walBytes());
            node.purgeForTest(snapshot);
            assertTrue(node.firstLogIndexForTest() > 2);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var armed = new AtomicBoolean(true);
            cluster.start(lagging, stage -> {
                if (stage == RatisWalState.Stage.AFTER_INSTALL_RECORD && armed.compareAndSet(true, false)) {
                    entered.countDown();
                    try {
                        if (!release.await(20, TimeUnit.SECONDS)) {
                            throw new IOException("test did not release installation");
                        }
                    } catch (final InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IOException(failure);
                    }
                }
            });
            try {
                assertTrue(entered.await(30, TimeUnit.SECONDS));
                assertFalse(cluster.nodes.get(lagging).ready());
                try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                    var writer = tasks.submit(() -> {
                        for (int value = 1; value <= 2; value++) {
                            pair(node, value);
                            TimeUnit.MILLISECONDS.sleep(100);
                        }
                        return 2;
                    });
                    var reader = tasks.submit(() -> {
                        for (int i = 0; i < 10; i++) {
                            assertBalance(node);
                            TimeUnit.MILLISECONDS.sleep(100);
                        }
                        return 10;
                    });
                    Assertions.assertEquals(2, writer.get(20, TimeUnit.SECONDS));
                    Assertions.assertEquals(10, reader.get(20, TimeUnit.SECONDS));
                }
                assertFalse(cluster.nodes.get(lagging).ready());
                Assertions.assertEquals(14, node.appliedSequence());
            } finally {
                release.countDown();
            }
            cluster.await(lagging, 14, snapshot);
            cluster.cold(lagging, 14);
            var installed = cluster.active(lagging);
            Assertions.assertEquals(authenticated, cluster.nodes.get(lagging).gcStats().enabled());
            if (authenticated) {
                long gcDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (cluster.nodes.get(lagging).gcStats().publishedSequence() < 14 && System.nanoTime() < gcDeadline) {
                    TimeUnit.MILLISECONDS.sleep(20);
                }
                assertTrue(cluster.nodes.get(lagging).gcStats().publishedSequence() >= 14);
                Assertions.assertEquals(installed.generation(), cluster.nodes.get(lagging).gcStats().generation());
            }
            Assertions.assertNotEquals(before.generation(), installed.generation());
            Assertions.assertEquals(snapshot, installed.index());
            Assertions.assertEquals(12, installed.sequence());
            assertFalse(Files.exists(directory.resolve("n" + lagging + "/business/INSTALLING")));
            var identity = cluster.identity(lagging);
            var factory = cluster.factories.get(lagging);
            for (String shard : Set.of("a", "b")) {
                String schema = factory.databaseName(identity, installed.generation(), shard);
                assertArrayEquals(shard.equals("a") ? a : b, physical(cluster.controls.get(lagging), schema, "blob"));
                assertNull(physical(cluster.controls.get(lagging), schema, "cold0"));
            }
            cluster.nodes.get(lagging).enableRetirementMaintenance(new RatisRetirementOptions(1, 4, 512, 5000, 2), 100);
            long retirementDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (cluster.nodes.get(lagging).retirementStats().generations() < 1 && System.nanoTime() < retirementDeadline) {
                var last = cluster.nodes.get(lagging).retirementStats().last();
                if (last != null) {
                    assertTrue(last.removedEntries() <= 4);
                    assertTrue(last.backendShards() <= 1);
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            Assertions.assertEquals(1, cluster.nodes.get(lagging).retirementStats().generations());
            assertNull(cluster.nodes.get(lagging).retirementStats().failure());
            assertFalse(Files.exists(directory.resolve("n" + lagging + "/business/generation-" + before.generation())));
            try (
                    var connection = cluster.controls.get(lagging).getConnection(); var query = connection.createStatement();
                    var row = query.executeQuery("select used_slots from azir_wal_namespace_budget")) {
                assertTrue(row.next());
                Assertions.assertEquals(2, row.getInt(1));
            }
            for (String shard : Set.of("a", "b")) {
                String oldSchema = factory.databaseName(identity, before.generation(), shard);
                try (
                        var connection = cluster.controls.get(lagging).getConnection();
                        var query = connection.prepareStatement("select count(*) from information_schema.schemata where cast(schema_name as binary)=?")) {
                    query.setBytes(1, bytes(oldSchema));
                    try (var row = query.executeQuery()) {
                        row.next();
                        Assertions.assertEquals(0, row.getInt(1));
                    }
                }
            }
            cluster.stop(lagging);
            cluster.start(lagging, stage -> {
            });
            cluster.cold(lagging, 14);
            // Local Checkpoint already contains business 14 does not mean that after restart Raft has caught up with the current leader.
            long requiredRaft = node.appliedRaftIndex();
            long raftDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (cluster.nodes.get(lagging).appliedRaftIndex() < requiredRaft && System.nanoTime() < raftDeadline) {
                TimeUnit.MILLISECONDS.sleep(30);
            }
            assertTrue(cluster.nodes.get(lagging).appliedRaftIndex() >= requiredRaft);
            if (authenticated) {
                node.transferLeadership(RaftPeerId.valueOf("n" + lagging), 30_000);
            } else {
                try (var client = RaftClient.newBuilder().setRaftGroup(cluster.group).setProperties(new org.apache.ratis.conf.RaftProperties()).setLeaderId(RaftPeerId.valueOf("n" + leader)).build()) {
                    assertTrue(client.admin().transferLeadership(RaftPeerId.valueOf("n" + lagging), 10_000).isSuccess());
                }
            }
            cluster.awaitLeader(lagging);
            var serving = cluster.nodes.get(lagging);
            verifyAll(serving, a, b, 2);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, serving.lookupCommit(receipt.transactionId()).status());
            Assertions.assertEquals(15, pair(serving, 3).commitSequence());
            cluster.cold(lagging, 15);
            cluster.stop(leader);
            cluster.controls.get(leader).close();
            cluster.databases.get(leader).stop();
            verifyAll(serving, a, b, 3);
            Assertions.assertEquals(16, pair(serving, 4).commitSequence());
            assertBalance(serving);
        }
    }

    /**
     * Business write set crosses two pieces, and the sum of values is zero.
     *
     * @param node node
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt pair(final RatisWalNode node, final int value) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        node.put(tx, "a", "live", bytes(Integer.toString(value)));
        node.put(tx, "b", "live", bytes(Integer.toString(-value)));
        return node.commit(tx);
    }

    /**
     * The same sentence snapshot cross-checks the two pieces and cannot be spelled into a false complete result after creating the snapshot separately.
     *
     * @param node node
     */
    private static void assertBalance(final RatisWalNode node) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var snapshot = node.beginReadStatement(tx);
        int a = Integer.parseInt(new String(node.get(tx, "a", "live", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        int b = Integer.parseInt(new String(node.get(tx, "b", "live", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        Assertions.assertEquals(0, a + b);
        node.rollback(tx);
    }

    /**
     * Read all cold keys, large payload, tombstone and subsequent logs after cutting the master. Cache disabled cannot mask missing.
     *
     * @param node node
     * @param a a
     * @param b b
     * @param live live
     */
    private static void verifyAll(final RatisWalNode node, final byte[] a, final byte[] b, final int live) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var snapshot = node.beginReadStatement(tx);
        for (String shard : Set.of("a", "b")) {
            for (int i = 0; i < 32; i++) {
                if (i == 0) {
                    assertTrue(node.get(tx, shard, "cold0", snapshot).isEmpty());
                } else {
                    Assertions.assertEquals(shard + ":" + i, new String(node.get(tx, shard, "cold" + i, snapshot).orElseThrow(), StandardCharsets.UTF_8));
                }
            }
            assertArrayEquals(shard.equals("a") ? a : b, node.get(tx, shard, "blob", snapshot).orElseThrow());
            Assertions.assertEquals(Integer.toString(shard.equals("a") ? live : -live), new String(node.get(tx, shard, "live", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        }
        node.rollback(tx);
    }

    /**
     * Directly query receiving database, independent of the cache and merge code for the transaction engine.
     *
     * @param control control
     * @param schema schema
     * @param key business key
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static byte[] physical(final HikariDataSource control, final String schema, final String key) throws Exception {
        try (
                var connection = control.getConnection();
                var query = connection.prepareStatement("select payload from `" + schema + "`.azir_wal_version where user_key=? order by commit_seq desc limit 1")) {
            query.setBytes(1, bytes(key));
            try (var row = query.executeQuery()) {
                return row.next() ? row.getBytes(1) : null;
            }
        }
    }

    /**
     * Fixed UTF-8 business value.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Three Raft nodes are the same JVM, but each has a separate MySQL process and all backends generation.
     */
    private static final class Cluster implements AutoCloseable {

        final Path directory;

        final List<org.testcontainers.containers.MySQLContainer<?>> databases = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());

        final List<HikariDataSource> controls = new ArrayList<>();

        final List<MysqlWalReplicaBackendFactory> factories = new ArrayList<>();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final RaftGroup group;

        final List<RatisTlsSecurity> security = new ArrayList<>();

        /**
         * Only three limited resources of database are activated, each connection pool is allocated by the actual generation plant.
         *
         * @param directory directory
         * @param authenticated authenticated
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path directory, final boolean authenticated) throws Exception {
            this.directory = directory;
            var peers = new ArrayList<RaftPeer>();
            var held = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = MysqlGenerationSupport.reservePeerPort();
                    held.add(socket);
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            } finally {
                for (var socket : held) {
                    socket.close();
                }
            }
            group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers);
            if (authenticated) {
                var pki = new RatisTlsTestCertificates(directory.resolve("pki"));
                var credentials = new ArrayList<RatisTlsTestCertificates.Entry>();
                var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
                for (var peer : peers) {
                    var entry = pki.issue(group.getGroupId().getUuid(), peer.getId().toString(), peer.getAddress());
                    credentials.add(entry);
                    trust.add(entry.trust());
                }
                for (var entry : credentials) {
                    security.add(
                            RatisTlsSecurity.load(group.getGroupId().getUuid(), entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust));
                }
            }
            try {
                Startables.deepStart(databases.stream()).join();
                for (int i = 0; i < 3; i++) {
                    var pool = MysqlGenerationSupport.control(databases.get(i), databases.get(i).getDatabaseName());
                    controls.add(pool);
                    factories.add(new MysqlWalReplicaBackendFactory(Map.of("a", pool, "b", pool), MysqlWalReplicaBackendFactory.Options.defaults()));
                    nodes.add(null);
                    start(i, stage -> {
                    });
                }
            } catch (final Throwable failure) {
                close();
                throw failure;
            }
        }

        /**
         * The node identity corresponds to the actual group and each new generation is reopened by the same plant idempotent.
         *
         * @param index index
         * @return the operation result
         */
        WalReplicaIdentity identity(final int index) {
            return new WalReplicaIdentity(group.getGroupId().getUuid(), group.getGroupId().getUuid(), "n" + index);
        }

        /**
         * Using a formal database plant, the hooks in the package control only the actual boundary installed.
         *
         * @param index index
         * @param hook hook
         * @throws Exception if the operation cannot complete
         */
        void start(final int index, final RatisWalState.Hook hook) throws Exception {
            var root = directory.resolve("n" + index);
            var peer = RaftPeerId.valueOf("n" + index);
            var options = WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0);
            var limits = new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000);
            nodes.set(index, security.isEmpty() ? RatisWalNode.openGenerationalForTest(root, peer, group, Set.of("a", "b"), factories.get(index), options, limits, hook)
                    : RatisWalNode.openAuthenticatedForTest(root, peer, group, Set.of("a", "b"), factories.get(index), options, limits, security.get(index), hook));
        }

        /**
         * Only the current round node is stopped, and the business directory and database are kept for a homing installation.
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
         * Confirm that the leader can actually pass majority barrier.
         *
         * @param excluded excluded
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader(final int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    var node = nodes.get(i);
                    if (i == excluded || node == null || node.leaderEpoch() <= 0) {
                        continue;
                    }
                    try {
                        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                        node.rollback(tx);
                        return i;
                    } catch (final WalMvccException transientRole) {
                    }
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("no MySQL generation leader");
        }

        /**
         * The transfer still needs to be finalized and management responses cannot be considered implemented as transaction.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void awaitLeader(final int index) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (nodes.get(index).leaderEpoch() <= 0 && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(20);
            }
            assertTrue(nodes.get(index).leaderEpoch() > 0);
        }

        /**
         * The installation period is not accessible to the kernel, and three watermark s are satisfied before they are considered to have completed.
         *
         * @param index index
         * @param sequence sequence
         * @param snapshot snapshot
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void await(final int index, final long sequence, final long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                var node = nodes.get(index);
                try {
                    if (node.ready() && node.appliedSequence() == sequence && node.snapshotIndex() >= snapshot && node.durableSnapshotAckIndex() >= snapshot) {
                        return;
                    }
                } catch (final IllegalStateException installing) {
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("MySQL replica did not finish installation and catch up");
        }

        /**
         * Forced evidence is obtained from cold data, checking materialization watermark, zero caches and a 100-byte log.
         *
         * @param index index
         * @param sequence sequence
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void cold(final int index, final long sequence) throws Exception {
            var node = nodes.get(index);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!node.ready() || node.appliedSequence() < sequence || node.localStats().materializedSequences().values().stream().anyMatch(value -> value < sequence)) {
                if (System.nanoTime() > until) {
                    throw new AssertionError("MySQL generation did not materialize");
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            assertTrue(node.checkpointLocal().created());
            Assertions.assertEquals(0, node.localStats().retainedBytes());
            Assertions.assertEquals(100, node.localStats().walBytes());
        }

        /**
         * Check generation from the real durable activity pointer instead of trusting in plant assigned memory parameters.
         *
         * @param index index
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        RatisGenerationRecord active(final int index) throws IOException {
            return RatisGenerationRecord.read(directory.resolve("n" + index + "/business/ACTIVE"));
        }

        /**
         * All nodes and their own resources are closed on a case-by-case basis, with individual failures still cleaning up other testing resources.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Throwable failure = null;
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    stop(i);
                } catch (final Throwable error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            for (var pool : controls) {
                try {
                    pool.close();
                } catch (final Throwable error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            for (var database : databases) {
                try {
                    database.stop();
                } catch (final Throwable error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }
}

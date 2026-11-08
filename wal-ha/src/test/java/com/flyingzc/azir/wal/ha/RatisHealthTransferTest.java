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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.io.IOException;

import java.util.concurrent.TimeUnit;

/**
 * Three separate MySQLs and authentication Raft, verify database Health Master & recovery when they stop responding.
 */
class RatisHealthTransferTest {

    static final List<org.testcontainers.containers.MySQLContainer<?>> DATABASES = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());

    static final RatisHealthOptions HEALTH = new RatisHealthOptions(200, 2, 2, 750, 6000);

    @TempDir
    Path directory;

    /**
     * Use three restricted cases for the whole category, with each scene independent control library and group identity.
     */
    @BeforeAll
    static void startDatabases() {
        Startables.deepStart(DATABASES.stream()).join();
    }

    /**
     * Only the database example created by this class is released.
     */
    @AfterAll
    static void stopDatabases() {
        for (var database : DATABASES) {
            database.stop();
        }
    }

    /**
     * Moves when the main backend stops responding and Raft is still alive, recovery after the old transaction cannot be reused.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void leaderBackendFailureTransfersConfirmedColdDataAndRecovers() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int old = cluster.leader(Set.of());
            var original = cluster.nodes.get(old);
            var receipt = cluster.write(original, 1);
            cluster.coldAll(1);
            var stale = original.begin(IsolationLevel.REPEATABLE_READ);
            original.put(stale, "a", "k", new byte[]{99});
            original.put(stale, "b", "k", new byte[]{99});
            cluster.pause(old);
            int next = cluster.leader(Set.of(old));
            await(() -> original.healthStats().quarantined() && original.healthStats().campaignBlocked());
            await(() -> original.healthStats().transfers() >= 1);
            assertThrows(WalMvccException.class, () -> original.commit(stale));
            cluster.verify(cluster.nodes.get(next), 1, receipt);
            Assertions.assertEquals(2, cluster.write(cluster.nodes.get(next), 2).commitSequence());
            Assertions.assertEquals(WalCommitLookup.Status.NOT_CONFIRMED, cluster.nodes.get(next).lookupCommit(stale.id()).status());
            cluster.resume(old);
            await(() -> original.healthStats().state().equals("HEALTHY") && !original.healthStats().quarantined());
            cluster.cold(original, 2);
            Assertions.assertEquals(next, cluster.leader(Set.of(old)));
            cluster.nodes.get(next).transferLeadership(cluster.peers.get(old).getId(), 30_000);
            cluster.awaitLeader(old);
            cluster.verify(original, 2, receipt);
            assertThrows(WalMvccException.class, () -> original.commit(stale));
            assertTrue(original.healthStats().isolations() >= 1);
            assertTrue(original.healthStats().recoveries() >= 1);
        }
    }

    /**
     * When all backends fail, the unknown target is not considered healthy and recovery re-elected in accordance with the law.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void noHealthyTargetKeepsAllBusinessGatesClosedUntilRecovery() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(Set.of());
            long oldTerm = cluster.nodes.get(first).leaderEpoch();
            var receipt = cluster.write(cluster.nodes.get(first), 1);
            cluster.coldAll(1);
            long[] beforeDenials = cluster.nodes.stream().mapToLong(RatisWalNode::campaignDenialsForTest).toArray();
            for (int i = 0; i < 3; i++) {
                cluster.pause(i);
            }
            await(() -> cluster.nodes.stream().allMatch(node -> node.healthStats().quarantined() && node.healthStats().campaignBlocked()));
            for (var node : cluster.nodes) {
                assertThrows(WalMvccException.class, () -> node.begin(IsolationLevel.REPEATABLE_READ));
                Assertions.assertEquals(0, node.leaderEpoch());
            }
            // An explicit waiting period for the emergence of a primary candidate to cover the old Pause time series without an electoral timeline that cannot be recovery.
            await(() -> java.util.stream.IntStream.range(0, 3).allMatch(i -> cluster.nodes.get(i).campaignDenialsForTest() > beforeDenials[i]));
            for (int i = 0; i < 3; i++) {
                cluster.resume(i);
            }
            int leader = cluster.leader(Set.of());
            assertTrue(cluster.nodes.get(leader).leaderEpoch() > oldTerm);
            cluster.verify(cluster.nodes.get(leader), 1, receipt);
            Assertions.assertEquals(2, cluster.write(cluster.nodes.get(leader), 2).commitSequence());
        }
    }

    /**
     * Auto recovery, when the policy is enabled to restart, database is still not valid and cannot bypass the startup check-in service.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void quarantinedRestartRestoresPolicyAndCannotSkipBackendChecks() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(Set.of()), victim = (leader + 1) % 3;
            var receipt = cluster.write(cluster.nodes.get(leader), 1);
            cluster.coldAll(1);
            cluster.pause(victim);
            await(() -> cluster.nodes.get(victim).healthStats().quarantined());
            cluster.stop(victim);
            assertThrows(IOException.class, () -> cluster.open(victim));
            assertTrue(Files.exists(directory.resolve("n" + victim + "/HEALTH_POLICY")));
            Assertions.assertEquals(2, cluster.write(cluster.nodes.get(leader), 2).commitSequence());
            cluster.resume(victim);
            cluster.nodes.set(victim, cluster.open(victim));
            var restarted = cluster.nodes.get(victim);
            assertTrue(restarted.healthStats().enabled());
            if (restarted.ready()) {
                assertTrue(restarted.healthStats().successes() >= HEALTH.recoveries());
            }
            await(() -> restarted.healthStats().state().equals("HEALTHY") && !restarted.healthStats().quarantined());
            cluster.cold(restarted, 2);
            cluster.nodes.get(leader).transferLeadership(cluster.peers.get(victim).getId(), 30_000);
            cluster.awaitLeader(victim);
            cluster.verify(restarted, 2, receipt);
        }
    }

    /**
     * Rule or identity damage cannot be converted into a default mode without health management when restarting.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void persistentPolicyBindsIdentityAndRejectsCorruption() throws Exception {
        UUID id = UUID.randomUUID();
        var owner = new WalReplicaIdentity(id, id, "x");
        var record = new RatisHealthRecord(owner, HEALTH);
        record.publish(directory);
        Assertions.assertEquals(record, RatisHealthRecord.load(directory, owner));
        assertThrows(IOException.class, () -> RatisHealthRecord.load(directory, new WalReplicaIdentity(id, id, "y")));
        assertThrows(IOException.class, () -> new RatisHealthRecord(owner, RatisHealthOptions.defaults()).publish(directory));
        Path file = directory.resolve("HEALTH_POLICY");
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 1;
        Files.write(file, bytes);
        assertThrows(IOException.class, () -> RatisHealthRecord.load(directory, owner));
    }

    /**
     * Waiting on the basis of state observation and not through a fixed sleep assumption, the master has been completed.
     *
     * @param condition condition
     * @throws Exception if the operation cannot complete
     */
    private static void await(final java.util.function.BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            TimeUnit.MILLISECONDS.sleep(30);
        }
        assertTrue(condition.getAsBoolean());
    }

    /**
     * Independent control pool, database generation and TLS identity for each node, durable recovery when health management is visible.
     */
    private static final class Cluster implements AutoCloseable {

        final Path root;

        final RaftGroup group;

        final List<RaftPeer> peers = new ArrayList<>();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final List<HikariDataSource> controls = new ArrayList<>();

        final List<MysqlWalReplicaBackendFactory> factories = new ArrayList<>();

        final List<RatisTlsSecurity> tls = new ArrayList<>();

        final boolean[] paused = new boolean[3];

        /**
         * Certificates and ports are accepted during this cycle, and database status changes only serve as examples.
         *
         * @param root root
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root) throws Exception {
            this.root = root;
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
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
            var pki = new RatisTlsTestCertificates(root.resolve("pki"));
            var credentials = new ArrayList<RatisTlsTestCertificates.Entry>();
            var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (var peer : peers) {
                var entry = pki.issue(group.getGroupId().getUuid(), peer.getId().toString(), peer.getAddress());
                credentials.add(entry);
                trust.add(entry.trust());
            }
            try {
                for (int i = 0; i < 3; i++) {
                    String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
                    MysqlGenerationSupport.createControl(DATABASES.get(i), schema);
                    var pool = MysqlGenerationSupport.control(DATABASES.get(i), schema);
                    controls.add(pool);
                    factories.add(new MysqlWalReplicaBackendFactory(Map.of("a", pool, "b", pool), MysqlWalReplicaBackendFactory.Options.defaults()));
                    var entry = credentials.get(i);
                    tls.add(RatisTlsSecurity.load(group.getGroupId().getUuid(), entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust));
                    nodes.add(open(i));
                    nodes.get(i).enableHealthManagement(HEALTH);
                }
                await(() -> nodes.stream().allMatch(node -> node.healthStats().state().equals("HEALTHY")));
            } catch (final Exception failure) {
                close();
                throw failure;
            }
        }

        /**
         * Reopens the durable policy for automatic assembly, without additional calls.
         *
         * @param index index
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        RatisWalNode open(final int index) throws IOException {
            return RatisWalNode.openAuthenticated(root.resolve("n" + index), peers.get(index).getId(), group, Set.of("a", "b"), factories.get(index),
                    WalOptions.defaults().withTimeout(5000).withHotCacheBytes(0), new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000), tls.get(index));
        }

        /**
         * Stops the individual nodes and keeps their true directories.
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
         * Pause database process, Raft JVM and the network are running.
         *
         * @param index index
         */
        void pause(final int index) {
            var database = DATABASES.get(index);
            database.getDockerClient().pauseContainerCmd(database.getContainerId()).exec();
            paused[index] = true;
            var state = database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec().getState();
            assertTrue(state.getRunning());
            assertTrue(state.getPaused());
        }

        /**
         * Original database in situ recovery does not recreate data space.
         *
         * @param index index
         */
        void resume(final int index) {
            if (!paused[index]) {
                return;
            }
            var database = DATABASES.get(index);
            database.getDockerClient().unpauseContainerCmd(database.getContainerId()).exec();
            paused[index] = false;
            assertFalse(database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec().getState().getPaused());
        }

        /**
         * The choice of a health leader who has been able to complete the real majority read barrier.
         *
         * @param excluded excluded
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader(final Set<Integer> excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    var node = nodes.get(i);
                    if (excluded.contains(i) || node == null || node.leaderEpoch() <= 0) {
                        continue;
                    }
                    try {
                        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                        node.rollback(tx);
                        return i;
                    } catch (final WalMvccException transientRole) {
                    }
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("healthy leader missing");
        }

        /**
         * The actual target is confirmed after the management shift.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void awaitLeader(final int index) throws Exception {
            await(() -> nodes.get(index).leaderEpoch() > 0);
        }

        /**
         * Small cross-shard transaction still has a different payload to facilitate the discovery of an error grouping.
         *
         * @param node node
         * @param value value to write
         * @return the operation result
         */
        WalReceipt write(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            node.put(tx, "a", "k", new byte[]{(byte) value});
            node.put(tx, "b", "k", new byte[]{(byte) (100 + value)});
            return node.commit(tx);
        }

        /**
         * A RR statement checking double cold data and confirmed UUID.
         *
         * @param node node
         * @param value value to write
         * @param receipt receipt
         */
        void verify(final RatisWalNode node, final int value, final WalReceipt receipt) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            var view = node.beginReadStatement(tx);
            assertArrayEquals(new byte[]{(byte) value}, node.get(tx, "a", "k", view).orElseThrow());
            assertArrayEquals(new byte[]{(byte) (100 + value)}, node.get(tx, "b", "k", view).orElseThrow());
            node.rollback(tx);
            var result = node.lookupCommit(receipt.transactionId());
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, result.status());
            Assertions.assertEquals(receipt, result.receipt());
        }

        /**
         * All three confirm the real materialization and local cut-off, without relying on active caches to prove cold data reliability.
         *
         * @param sequence sequence
         * @throws Exception if the operation cannot complete
         */
        void coldAll(final long sequence) throws Exception {
            for (var node : nodes) {
                cold(node, sequence);
            }
        }

        /**
         * Database recovery later waits for materialization to complete and confirm the zero cache.
         *
         * @param node node
         * @param sequence sequence
         * @throws Exception if the operation cannot complete
         */
        void cold(final RatisWalNode node, final long sequence) throws Exception {
            await(() -> node.appliedSequence() >= sequence && node.localStats().materializedSequences().values().stream().allMatch(value -> value >= sequence));
            assertTrue(node.checkpointLocal().created());
            Assertions.assertEquals(0, node.localStats().retainedBytes());
        }

        /**
         * The anomaly also starts with an recovery case of its own suspension, then emptys out the node and control pool.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            for (int i = 0; i < 3; i++) {
                resume(i);
            }
            Exception failure = null;
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    stop(i);
                } catch (final Exception close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
                    }
                }
            }
            for (var control : controls) {
                control.close();
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}

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
import com.flyingzc.azir.wal.WalTransaction;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.concurrent.TimeUnit;

/**
 * Automatic mirrors for authentic authentication clusters, actual log collection, failed backpression and restart policy verify.
 */
class RatisSnapshotLifecycleTest {

    @TempDir
    Path directory;

    private static final RatisSnapshotOptions POLICY = new RatisSnapshotOptions(100, 8, 500, 64L * 1024 * 1024, 0);

    /**
     * Full mirror overwrite collection real segment files, and reopening after log gap still retains confirmation data and UUID.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void automaticSnapshotsPurgeClosedSegmentsAndRestartSafely() throws Exception {
        try (var cluster = new Cluster(directory, limits())) {
            cluster.enable(POLICY);
            int first = cluster.leader();
            WalReceipt receipt = null;
            for (int value = 1; value <= 12; value++) {
                var committed = cluster.write(cluster.nodes.get(first), value);
                if (receipt == null) {
                    receipt = committed;
                }
            }
            final int source = first;
            await(() -> cluster.nodes.get(source).snapshotStats().purges() >= 1 && cluster.nodes.get(source).snapshotStats().firstLogIndex() > 0);
            assertTrue(cluster.nodes.get(first).snapshotIndex() >= cluster.nodes.get(first).snapshotStats().firstLogIndex());
            cluster.verify(cluster.nodes.get(first), 12);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(first).lookupCommit(receipt.transactionId()).status());
            cluster.stop(first);
            int next = cluster.leader();
            cluster.verify(cluster.nodes.get(next), 12);
            cluster.start(first, limits());
            assertTrue(cluster.nodes.get(first).snapshotStats().enabled());
            await(() -> cluster.nodes.get(source).ready() && cluster.nodes.get(source).appliedSequence() >= 12);
            cluster.verify(cluster.nodes.get(next), 12);
            Assertions.assertEquals(13, cluster.write(cluster.nodes.get(next), 13).commitSequence());
        }
    }

    /**
     * The mirror budget failed to keep a log, super watermark closes the new business; after repairing budget the durable policy automatically recovery.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failedImagesKeepLogsAndBackpressureRecoversAfterRestart() throws Exception {
        var tiny = new WalBackupOptions(1024, 8, 64000, 30_000);
        var policy = new RatisSnapshotOptions(100, 8, 500, 32L * 1024 * 1024, 0);
        var acknowledged = new ArrayList<WalReceipt>();
        var attempts = new HashMap<UUID, Integer>();
        try (var cluster = new Cluster(directory, tiny)) {
            cluster.enable(policy);
            int leader = cluster.leader();
            for (int value = 1; value <= 80; value++) {
                var node = cluster.nodes.get(leader);
                try {
                    var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                    attempts.put(tx.id(), value);
                    put(node, tx, value);
                    acknowledged.add(node.commit(tx));
                } catch (final WalMvccException blocked) {
                    break;
                }
                if (node.snapshotStats().backpressure()) {
                    break;
                }
            }
            final int source = leader;
            await(() -> cluster.nodes.get(source).snapshotStats().backpressure());
            assertFalse(acknowledged.isEmpty());
            Assertions.assertEquals(0, RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE")).sequence());
            Assertions.assertEquals(0, cluster.nodes.get(leader).firstLogIndexForTest());
            Assertions.assertEquals(0, cluster.nodes.get(leader).snapshotStats().purges());
            assertThrows(WalMvccException.class, () -> cluster.nodes.get(source).begin(IsolationLevel.REPEATABLE_READ));
            for (int i = 0; i < 3; i++) {
                cluster.stop(i);
            }
            for (int i = 0; i < 3; i++) {
                cluster.start(i, limits());
            }
            leader = cluster.leader();
            var serving = cluster.nodes.get(leader);
            long latest = 0;
            int expected = 0;
            for (var entry : attempts.entrySet()) {
                var lookup = serving.lookupCommit(entry.getKey());
                if (lookup.status() == WalCommitLookup.Status.COMMITTED && lookup.receipt().commitSequence() > latest) {
                    latest = lookup.receipt().commitSequence();
                    expected = entry.getValue();
                }
            }
            for (var receipt : acknowledged) {
                Assertions.assertEquals(receipt, serving.lookupCommit(receipt.transactionId()).receipt());
            }
            cluster.verify(serving, expected);
            assertTrue(serving.snapshotStats().enabled());
        }
    }

    /**
     * The soft left end of a high watermark -covered file can be collection, otherwise the large log entry will cause the default number to permanently block recovery.
     *
     * @throws Exception if the operation cannot complete
     */
    @org.junit.jupiter.api.RepeatedTest(value = 5, failureThreshold = 1)
    void pressureCanReclaimCoveredPreferredTailAndResume() throws Exception {
        var policy = new RatisSnapshotOptions(100, 10_000_000, 60_000, 32L * 1024 * 1024, 1024);
        var receipts = new ArrayList<WalReceipt>();
        try (var cluster = new Cluster(directory, limits())) {
            cluster.enable(policy);
            int initial = cluster.leader();
            var node = cluster.nodes.get(initial);
            for (int value = 1; value <= 80; value++) {
                long observed = node.snapshotStats().rounds();
                try {
                    receipts.add(cluster.write(node, value));
                } catch (final WalMvccException pressure) {
                    break;
                }
                // Waiting for an watermark observation each time to avoid testing the producer 's memory of three local histories over and above JVM budget.
                await(() -> node.snapshotStats().rounds() > observed);
                if (node.snapshotStats().backpressure()) {
                    break;
                }
            }
            assertFalse(receipts.isEmpty());
            long recoveryEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(90), reportAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!(node.snapshotStats().purges() > 0 && !node.snapshotStats().backpressure() && node.ready()) && System.nanoTime() < recoveryEnd) {
                if (System.nanoTime() >= reportAt) {
                    reportAt = Long.MAX_VALUE;
                    for (int i = 0; i < 3; i++) {
                        diagnostic(cluster.nodes.get(i), directory.resolve("n" + i));
                    }
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            assertTrue(node.snapshotStats().purges() > 0 && !node.snapshotStats().backpressure() && node.ready(), node.snapshotStats() + " " + node.healthStats());
            int current = cluster.leader();
            for (var receipt : receipts) {
                Assertions.assertEquals(receipt, cluster.nodes.get(current).lookupCommit(receipt.transactionId()).receipt());
            }
            assertTrue(node.snapshotStats().firstLogIndex() > 0);
            cluster.write(cluster.nodes.get(current), 81);
            cluster.verify(cluster.nodes.get(current), 81);
        }
    }

    /**
     * The crash temporary can only be cleaned with bounded when ownership is correct and no active task. Unknown files are kept.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void buildGarbageCleanupIsBoundedAndRejectsUnknownOwnership() throws Exception {
        try (var cluster = new Cluster(directory, limits())) {
            int index = cluster.leader();
            var node = cluster.nodes.get(index);
            var field = RatisWalNode.class.getDeclaredField("machine");
            field.setAccessible(true);
            var machine = (RatisWalStateMachine) field.get(node);
            Path parent = machine.getStateMachineStorage().getSnapshotDir().toPath();
            UUID build = UUID.randomUUID();
            Path root = parent.resolve(".build-" + build);
            Files.createDirectory(root);
            var identity = new WalReplicaIdentity(cluster.group.getGroupId().getUuid(), cluster.group.getGroupId().getUuid(), "n" + index);
            new RatisGenerationRecord(identity, build, -1, -1, 0, "").publish(root.resolve("BUILD"));
            Files.createDirectories(root.resolve("backup/data"));
            Files.write(root.resolve("backup/data/0000.bin"), new byte[]{1});
            for (int i = 0; i < 20 && Files.exists(root); i++) {
                var work = new RatisRetirementWork(new RatisRetirementOptions(1, 1, 512, 1000, 2));
                machine.cleanupBuilds(work);
                assertTrue(work.removed <= 1);
            }
            assertFalse(Files.exists(root));
            UUID foreignId = UUID.randomUUID();
            Path foreign = parent.resolve(".build-" + foreignId);
            Files.createDirectory(foreign);
            new RatisGenerationRecord(new WalReplicaIdentity(identity.engineId(), identity.groupId(), "foreign"), foreignId, -1, -1, 0, "").publish(foreign.resolve("BUILD"));
            Files.write(foreign.resolve("snapshot.bundle"), new byte[]{1});
            assertThrows(java.io.IOException.class, () -> machine.cleanupBuilds(new RatisRetirementWork(RatisRetirementOptions.defaults())));
            assertTrue(Files.exists(foreign.resolve("snapshot.bundle")));
            Files.delete(foreign.resolve("snapshot.bundle"));
            Files.delete(foreign.resolve("BUILD"));
            Files.delete(foreign);
            UUID unknown = UUID.randomUUID();
            Path unowned = parent.resolve(".build-" + unknown);
            Files.createDirectory(unowned);
            new RatisGenerationRecord(identity, unknown, -1, -1, 0, "").publish(unowned.resolve("BUILD"));
            Files.writeString(unowned.resolve("user-file"), "keep");
            assertThrows(java.io.IOException.class, () -> machine.cleanupBuilds(new RatisRetirementWork(RatisRetirementOptions.defaults())));
            Assertions.assertEquals("keep", Files.readString(unowned.resolve("user-file")));
        }
    }

    /**
     * Durable The policy is to precisely bind nodes and parameters, and the damage cannot be converted into a default configuration for automatic clean-up.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void persistentSnapshotPolicyRejectsForeignIdentityAndCorruption() throws Exception {
        UUID group = UUID.randomUUID();
        var owner = new WalReplicaIdentity(group, group, "x");
        var policy = new RatisSnapshotPolicy(owner, POLICY);
        policy.publish(directory);
        Assertions.assertEquals(policy, RatisSnapshotPolicy.load(directory, owner));
        assertThrows(java.io.IOException.class, () -> RatisSnapshotPolicy.load(directory, new WalReplicaIdentity(group, group, "y")));
        assertThrows(java.io.IOException.class, () -> new RatisSnapshotPolicy(owner, RatisSnapshotOptions.defaults()).publish(directory));
        Path file = directory.resolve("SNAPSHOT_POLICY");
        byte[] corrupt = Files.readAllBytes(file);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(file, corrupt);
        assertThrows(java.io.IOException.class, () -> RatisSnapshotPolicy.load(directory, owner));
    }

    /**
     * Private test mirror anomalies are used only for failure records, and official monitoring continues to eject only desensitization caches.
     *
     * @param node node
     * @param root root
     * @throws Exception if the operation cannot complete
     */
    private static void diagnostic(final RatisWalNode node, final Path root) throws Exception {
        System.err.println("TEST_RECOVERY " + root.getFileName() + " " + node.snapshotStats() + " " + node.healthStats());
        var field = RatisWalNode.class.getDeclaredField("machine");
        field.setAccessible(true);
        var machine = (RatisWalStateMachine) field.get(node);
        var jobField = RatisWalStateMachine.class.getDeclaredField("imageJob");
        jobField.setAccessible(true);
        var job = (java.util.concurrent.CompletableFuture<?>) jobField.get(machine);
        if (job != null && job.isCompletedExceptionally()) {
            job.handle((ignored, failure) -> {
                failure.printStackTrace(System.err);
                return null;
            }).join();
        }
        System.err.println("TEST_ACTIVE " + root.getFileName() + " " + RatisGenerationRecord.read(root.resolve("business/ACTIVE")));
    }

    /**
     * Limited waiting to see only actual collection and serviceable status.
     *
     * @param condition condition
     * @throws Exception if the operation cannot complete
     */
    private static void await(final java.util.function.BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            TimeUnit.MILLISECONDS.sleep(30);
        }
        assertTrue(condition.getAsBoolean());
    }

    /**
     * Enough to scroll the real eight MiB Raft segment file cross-shard payload.
     *
     * @param node node
     * @param tx transaction context
     * @param value value to write
     */
    private static void put(final RatisWalNode node, final WalTransaction tx, final int value) {
        for (String shard : List.of("a", "b")) {
            byte[] payload = new byte[480 * 1024];
            Arrays.fill(payload, (byte) (value + (shard.equals("a") ? 0 : 100)));
            node.put(tx, shard, "k", payload);
        }
    }

    /**
     * The mirror size covers the current round data, and the scale of production is determined by capacity acceptance.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000);
    }

    /**
     * Three local replica and stand-alone TLS identity, only operate the directories and listening ports for current tests.
     */
    private static final class Cluster implements AutoCloseable {

        final Path root;

        final RaftGroup group;

        final List<RaftPeer> peers = new ArrayList<>();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final List<RatisTlsSecurity> tls = new ArrayList<>();

        /**
         * Receipt is only kept in the JUnit private temporary directory, Logic replica does not share WAL.
         *
         * @param root root
         * @param limits limits
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root, final WalBackupOptions limits) throws Exception {
            this.root = root;
            var held = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = MysqlGenerationSupport.reservePeerPort();
                    held.add(socket);
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                    nodes.add(null);
                }
            } finally {
                for (var socket : held) {
                    socket.close();
                }
            }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
            var pki = new RatisTlsTestCertificates(root.resolve("pki"));
            var entries = new ArrayList<RatisTlsTestCertificates.Entry>();
            var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (var peer : peers) {
                var entry = pki.issue(group.getGroupId().getUuid(), peer.getId().toString(), peer.getAddress());
                entries.add(entry);
                trust.add(entry.trust());
            }
            try {
                for (int i = 0; i < 3; i++) {
                    var entry = entries.get(i);
                    tls.add(RatisTlsSecurity.load(group.getGroupId().getUuid(), entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust));
                    start(i, limits);
                }
            } catch (final Exception failure) {
                close();
                throw failure;
            }
        }

        /**
         * Restarts the full life cycle from two durable policies recovery when enabled.
         *
         * @param options options
         * @throws Exception if the operation cannot complete
         */
        void enable(final RatisSnapshotOptions options) throws Exception {
            for (var node : nodes) {
                node.enableHealthManagement(new RatisHealthOptions(200, 2, 2, 750, 6000));
                node.enableSnapshotMaintenance(options);
            }
        }

        /**
         * Original vote identity follows its own actual directory and does not empty the format history.
         *
         * @param index index
         * @param limits limits
         * @throws Exception if the operation cannot complete
         */
        void start(final int index, final WalBackupOptions limits) throws Exception {
            nodes.set(index, RatisWalNode.openAuthenticated(root.resolve("n" + index), peers.get(index).getId(), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(),
                    WalOptions.defaults().withTimeout(5000), limits, tls.get(index)));
        }

        /**
         * Closes only the node held by the test.
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
         * Fresh majority barrier uses the leader found after success.
         *
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    var node = nodes.get(i);
                    if (node == null || node.leaderEpoch() < 1 || !node.ready()) {
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
            throw new AssertionError("snapshot lifecycle leader missing");
        }

        /**
         * Confirm receipt by real transaction.
         *
         * @param node node
         * @param value value to write
         * @return the operation result
         */
        WalReceipt write(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            put(node, tx, value);
            return node.commit(tx);
        }

        /**
         * Checks two complete large payload, not just business serial numbers.
         *
         * @param node node
         * @param value value to write
         */
        void verify(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            var view = node.beginReadStatement(tx);
            for (String shard : List.of("a", "b")) {
                byte[] expected = new byte[480 * 1024];
                Arrays.fill(expected, (byte) (value + (shard.equals("a") ? 0 : 100)));
                assertArrayEquals(expected, node.get(tx, shard, "k", view).orElseThrow());
            }
            node.rollback(tx);
        }

        /**
         * The closure of all its own nodes continues to release the remaining resources in a single failure.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
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
            if (failure != null) {
                throw failure;
            }
        }
    }
}

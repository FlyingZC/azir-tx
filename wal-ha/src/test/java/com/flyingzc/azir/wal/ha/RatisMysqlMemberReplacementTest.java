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
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.client.RaftClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;

import java.nio.charset.StandardCharsets;

import java.util.concurrent.TimeUnit;

/**
 * New identity recreates and configures recovery from real cold snapshot after all old node local files and independent MySQL have been lost.
 */
class RatisMysqlMemberReplacementTest {

    @TempDir
    Path directory;

    /**
     * Four separate database s represent the original three replica and replacement nodes, with the new node not dependent on the original master disk or owner database.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void newMysqlPeerRebuildsPermanentlyLostLeaderAndRestoresVotingRole() throws Exception {
        var databases = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
        var controls = new ArrayList<HikariDataSource>();
        var factories = new ArrayList<MysqlWalReplicaBackendFactory>();
        var nodes = new ArrayList<RatisWalNode>();
        var peers = new ArrayList<RaftPeer>();
        var held = new ArrayList<java.net.ServerSocket>();
        String newAddress;
        try {
            Startables.deepStart(databases.stream()).join();
            for (int i = 0; i < 4; i++) {
                var socket = MysqlGenerationSupport.reservePeerPort();
                held.add(socket);
                if (i < 3) {
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            }
            newAddress = "127.0.0.1:" + held.getLast().getLocalPort();
            var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
            RatisMemberReplacement plan;
            for (int i = 0; i < 4; i++) {
                var control = MysqlGenerationSupport.control(databases.get(i), databases.get(i).getDatabaseName());
                controls.add(control);
                factories.add(new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults()));
                nodes.add(null);
            }
            for (int i = 0; i < 3; i++) {
                held.get(i).close();
                nodes.set(i, open(directory.resolve("n" + i), RaftPeerId.valueOf("n" + i), group, factories.get(i)));
            }
            int old = leader(nodes);
            var source = nodes.get(old);
            var initial = source.begin(IsolationLevel.REPEATABLE_READ);
            source.put(initial, "a", "balance", bytes("90"));
            source.put(initial, "b", "balance", bytes("110"));
            source.put(initial, "a", "blob", new byte[]{0});
            source.put(initial, "b", "blob", new byte[]{0});
            var receipt = source.commit(initial);
            byte[] a = null, b = null;
            for (int i = 1; i <= 10; i++) {
                a = new byte[480 * 1024];
                b = new byte[480 * 1024];
                Arrays.fill(a, (byte) i);
                Arrays.fill(b, (byte) (i + 100));
                var tx = source.begin(IsolationLevel.REPEATABLE_READ);
                source.put(tx, "a", "blob", a);
                source.put(tx, "b", "blob", b);
                Assertions.assertEquals(i + 1, source.commit(tx).commitSequence());
            }
            source.close();
            nodes.set(old, null);
            controls.get(old).close();
            databases.get(old).stop();
            discard(directory.resolve("n" + old));
            assertFalse(Files.exists(directory.resolve("n" + old)));
            int current = leader(nodes);
            source = nodes.get(current);
            long snapshot = source.snapshot();
            source.purgeForTest(snapshot);
            Assertions.assertEquals(0, source.localStats().retainedBytes());
            assertTrue(source.firstLogIndexForTest() > 2);
            plan = source.planReplacement(RaftPeerId.valueOf("n" + old), newAddress, 10_000);
            Files.write(directory.resolve("replacement.ticket"), plan.encode());
            held.get(3).close();
            nodes.set(3, open(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), factories.get(3)));
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.LEARNER_ADDED, source.beginReplacement(plan, 40_000).phase());
            var completed = complete(source, plan);
            Assertions.assertEquals(3, completed.membership().voters().size());
            assertTrue(completed.membership().learners().isEmpty());
            transfer(groupFor(group.getGroupId(), completed.membership()), RaftPeerId.valueOf("n" + current), plan.replacement().getId());
            awaitLeader(nodes.get(3));
            verify(nodes.get(3), a, b);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, nodes.get(3).lookupCommit(receipt.transactionId()).status());
            cold(nodes.get(3), 11);
            long ownSnapshot = nodes.get(3).snapshot();
            assertTrue(ownSnapshot > snapshot);
            var restoredConfiguration = nodes.get(3).membership(10_000);
            RaftPeerId freshId = plan.replacement().getId();
            assertTrue(restoredConfiguration.voters().stream().anyMatch(peer -> peer.getId().equals(freshId)));
            nodes.get(3).close();
            nodes.set(3, null);
            // The initial group at the time of accession is intentionally maintained and durable new member configurations must be preferred to recovery instead of reverting to permanent non-voting
            // replica .
            nodes.set(3, open(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), factories.get(3)));
            awaitSequence(nodes.get(3), 11);
            current = leader(nodes);
            if (current != 3) {
                transfer(groupFor(group.getGroupId(), completed.membership()), RaftPeerId.valueOf("n" + current), plan.replacement().getId());
                awaitLeader(nodes.get(3));
            }
            var membership = nodes.get(3).membership(10_000);
            assertTrue(membership.voters().stream().noneMatch(peer -> peer.getId().equals(RaftPeerId.valueOf("n" + old))));
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.COMPLETE,
                    nodes.get(3).completeReplacement(RatisMemberReplacement.decode(Files.readAllBytes(directory.resolve("replacement.ticket"))), 10_000).phase());
            verify(nodes.get(3), a, b);
            var continued = nodes.get(3).begin(IsolationLevel.REPEATABLE_READ);
            nodes.get(3).put(continued, "a", "balance", bytes("80"));
            nodes.get(3).put(continued, "b", "balance", bytes("120"));
            Assertions.assertEquals(12, nodes.get(3).commit(continued).commitSequence());
        } finally {
            for (var socket : held) {
                socket.close();
            }
            Throwable failure = null;
            for (var node : nodes) {
                if (node != null) {
                    try {
                        node.close();
                    } catch (final Throwable error) {
                        if (failure == null) {
                            failure = error;
                        } else {
                            failure.addSuppressed(error);
                        }
                    }
                }
            }
            for (var control : controls) {
                try {
                    control.close();
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

    /**
     * The plant and node use a formal generation protocol, which is currently dedicated only to loops and caches.
     *
     * @param root root
     * @param id id
     * @param group group
     * @param factory factory
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static RatisWalNode open(final Path root, final RaftPeerId id, final RaftGroup group, final WalReplicaBackendFactory factory) throws Exception {
        return RatisWalNode.openGenerationalLoopback(root, id, group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0),
                new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000));
    }

    /**
     * Finds a node that actually passes the majority read barrier.
     *
     * @param nodes nodes
     * @return the operation result
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static int leader(final List<RatisWalNode> nodes) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < until) {
            for (int i = 0; i < nodes.size(); i++) {
                var node = nodes.get(i);
                if (node == null || node.leaderEpoch() <= 0) {
                    continue;
                }
                try {
                    var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                    node.rollback(tx);
                    return i;
                } catch (final WalMvccException changing) {
                }
            }
            TimeUnit.MILLISECONDS.sleep(30);
        }
        throw new AssertionError("no replacement group leader");
    }

    /**
     * Waits for the same instrument to be even and does not generate a new identity or rewrite the group of original members.
     *
     * @param source database connection source
     * @param plan plan
     * @return the operation result
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static RatisWalNode.ReplacementProgress complete(final RatisWalNode source, final RatisMemberReplacement plan) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < until) {
            var result = source.completeReplacement(plan, 15_000);
            if (result.phase() == RatisWalNode.ReplacementPhase.COMPLETE) {
                return result;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("MySQL learner did not become ready");
    }

    /**
     * Transfer of leadership using a clear source ID and limited no automatic retry client.
     *
     * @param group group
     * @param source database connection source
     * @param target target
     * @throws Exception if the operation cannot complete
     */
    private static void transfer(final RaftGroup group, final RaftPeerId source, final RaftPeerId target) throws Exception {
        try (
                var client = RaftClient.newBuilder().setRaftGroup(group).setLeaderId(source).setProperties(new org.apache.ratis.conf.RaftProperties())
                        .setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) {
            assertTrue(client.admin().transferLeadership(target, 10_000).isSuccess());
        }
    }

    /**
     * The new node has indeed become a serviceable voting leader.
     *
     * @param node node
     * @throws Exception if the operation cannot complete
     */
    private static void awaitLeader(final RatisWalNode node) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (node.leaderEpoch() <= 0 && System.nanoTime() < until) {
            TimeUnit.MILLISECONDS.sleep(30);
        }
        assertTrue(node.leaderEpoch() > 0);
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        node.rollback(tx);
    }

    /**
     * Recovery data will be required after the new node has been reopened.
     *
     * @param node node
     * @param sequence sequence
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static void awaitSequence(final RatisWalNode node, final long sequence) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < until) {
            try {
                if (node.ready() && node.appliedSequence() == sequence) {
                    return;
                }
            } catch (final IllegalStateException loading) {
            }
            TimeUnit.MILLISECONDS.sleep(30);
        }
        throw new AssertionError("replacement did not restore sequence");
    }

    /**
     * Clear completion of cold materialization and cut off the local log, follow-up verify cannot rely on active caches.
     *
     * @param node node
     * @param sequence sequence
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static void cold(final RatisWalNode node, final long sequence) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (node.localStats().materializedSequences().values().stream().anyMatch(value -> value < sequence)) {
            if (System.nanoTime() > until) {
                throw new AssertionError("replacement cold state lagged");
            }
            TimeUnit.MILLISECONDS.sleep(30);
        }
        node.checkpointLocal();
        Assertions.assertEquals(0, node.localStats().retainedBytes());
        Assertions.assertEquals(100, node.localStats().walBytes());
    }

    /**
     * The same sentence snapshot reconciles cold data with full binary values.
     *
     * @param node node
     * @param a a
     * @param b b
     */
    private static void verify(final RatisWalNode node, final byte[] a, final byte[] b) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var view = node.beginReadStatement(tx);
        Assertions.assertEquals("90", new String(node.get(tx, "a", "balance", view).orElseThrow(), StandardCharsets.UTF_8));
        Assertions.assertEquals("110", new String(node.get(tx, "b", "balance", view).orElseThrow(), StandardCharsets.UTF_8));
        assertArrayEquals(a, node.get(tx, "a", "blob", view).orElseThrow());
        assertArrayEquals(b, node.get(tx, "b", "blob", view).orElseThrow());
        node.rollback(tx);
    }

    /**
     * Manages the configuration of the client from an commit member view.
     *
     * @param id id
     * @param view view
     * @return the operation result
     */
    private static RaftGroup groupFor(final RaftGroupId id, final RatisMembershipView view) {
        var peers = new ArrayList<>(view.voters());
        peers.addAll(view.learners());
        return RaftGroup.valueOf(id, peers);
    }

    /**
     * Only the private temporary directories of closed old nodes on this round are deleted, and the simulation of permanent disks is lost.
     *
     * @param root root
     * @throws Exception if the operation cannot complete
     */
    private static void discard(final Path root) throws Exception {
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
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
}

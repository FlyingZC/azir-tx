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
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

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

/**
 * The real Raft position generates the snapshot, reopening of nodes within the process and actual installation integration under log gaps.
 */
class RatisSnapshotClusterTest {

    @TempDir
    Path directory;

    /**
     * Local Full snapshot bound to active generation, reopening original node unrecognized, and follow-up transaction remains continuous.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void actualSnapshotPositionSurvivesNodeRestartAndContinuedCommit() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(-1);
            var receipt = write(cluster.nodes.get(first), new byte[]{1, 11}, new byte[]{1, 22});
            long snapshot = cluster.nodes.get(first).snapshot();
            assertTrue(snapshot > 0);
            Assertions.assertEquals(snapshot, cluster.nodes.get(first).snapshotIndex());
            var pointer = RatisGenerationRecord.read(directory.resolve("n" + first + "/business/ACTIVE"));
            Assertions.assertEquals(snapshot, pointer.index());
            Assertions.assertEquals(1, pointer.sequence());
            cluster.stop(first);
            int next = cluster.leader(first);
            Assertions.assertEquals(2, write(cluster.nodes.get(next), new byte[]{2, 11}, new byte[]{2, 22}).commitSequence());
            cluster.start(first);
            cluster.await(first, 2, snapshot);
            assertPair(cluster.nodes.get(next), new byte[]{2, 11}, new byte[]{2, 22});
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(next).lookupCommit(receipt.transactionId()).status());
            var reopened = RatisGenerationRecord.read(directory.resolve("n" + first + "/business/ACTIVE"));
            Assertions.assertEquals(pointer.generation(), reopened.generation());
        }
    }

    /**
     * Cleared log gaps force followers to install full snapshot through Ratis and then cut it to the main node to check data with receipt.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void laggingReplicaInstallsCompleteSnapshotAcrossRealRaftTransport() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(-1);
            var old = write(cluster.nodes.get(leader), new byte[]{0, 11}, new byte[]{0, 22});
            int lagging = (leader + 1) % 3;
            cluster.await(lagging, 1, -1);
            var before = RatisGenerationRecord.read(directory.resolve("n" + lagging + "/business/ACTIVE"));
            cluster.stop(lagging);
            byte[] a = null, b = null;
            // Cumulatively, more than eight MiBs close at least one log segment to avoid calling only purge without actually cleaning up to catch the log.
            for (int i = 1; i <= 10; i++) {
                a = new byte[480 * 1024];
                b = new byte[480 * 1024];
                Arrays.fill(a, (byte) i);
                Arrays.fill(b, (byte) (i + 100));
                Assertions.assertEquals(i + 1, write(cluster.nodes.get(leader), a, b).commitSequence());
            }
            long snapshot = cluster.nodes.get(leader).snapshot();
            cluster.nodes.get(leader).purgeForTest(snapshot);
            assertTrue(cluster.nodes.get(leader).firstLogIndexForTest() > 2, "a real unavailable log prefix is required");
            cluster.start(lagging);
            cluster.await(lagging, 11, snapshot);
            long ackDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (cluster.nodes.get(lagging).durableSnapshotAckIndex() < snapshot && System.nanoTime() < ackDeadline) {
                TimeUnit.MILLISECONDS.sleep(20);
            }
            assertTrue(cluster.nodes.get(lagging).durableSnapshotAckIndex() >= snapshot, "actual inbound RPC must run the durable ACK guard");
            var installed = RatisGenerationRecord.read(directory.resolve("n" + lagging + "/business/ACTIVE"));
            Assertions.assertNotEquals(before.generation(), installed.generation());
            Assertions.assertEquals(snapshot, installed.index());
            Assertions.assertEquals(11, installed.sequence());
            assertFalse(Files.exists(directory.resolve("n" + lagging + "/business/INSTALLING")));
            try (var client = RaftClient.newBuilder().setRaftGroup(cluster.group).setProperties(new org.apache.ratis.conf.RaftProperties()).setLeaderId(RaftPeerId.valueOf("n" + leader)).build()) {
                var reply = client.admin().transferLeadership(RaftPeerId.valueOf("n" + lagging), 10_000);
                assertTrue(reply.isSuccess(), reply.toString());
            }
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (cluster.nodes.get(lagging).leaderEpoch() <= 0 && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(20);
            }
            assertTrue(cluster.nodes.get(lagging).leaderEpoch() > 0);
            assertPair(cluster.nodes.get(lagging), a, b);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(lagging).lookupCommit(old.transactionId()).status());
            Assertions.assertEquals(12, write(cluster.nodes.get(lagging), new byte[]{12, 11}, new byte[]{12, 22}).commitSequence());
            cluster.stop(lagging);
            cluster.start(lagging);
            cluster.await(lagging, 12, snapshot);
            assertPair(cluster.nodes.get(cluster.leader(-1)), new byte[]{12, 11}, new byte[]{12, 22});
        }
    }

    /**
     * Transaction writes a different double value at a time and cannot mask the error route with an equal result.
     *
     * @param node node
     * @param a a
     * @param b b
     * @return the operation result
     */
    private static WalReceipt write(final RatisWalNode node, final byte[] a, final byte[] b) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        node.put(tx, "a", "k", a);
        node.put(tx, "b", "k", b);
        return node.commit(tx);
    }

    /**
     * The same fixed statement snapshot checks the complete binary payload and releases transaction.
     *
     * @param node node
     * @param a a
     * @param b b
     */
    private static void assertPair(final RatisWalNode node, final byte[] a, final byte[] b) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var snapshot = node.beginReadStatement(tx);
        assertArrayEquals(a, node.get(tx, "a", "k", snapshot).orElseThrow());
        assertArrayEquals(b, node.get(tx, "b", "k", snapshot).orElseThrow());
        node.rollback(tx);
    }

    /**
     * Each of the three nodes has a separate generation directory and an actual gRPC service, without using the shared business kernel.
     */
    private static final class Cluster implements AutoCloseable {

        final Path directory;

        final RaftGroup group;

        final List<RatisWalNode> nodes = new ArrayList<>();

        /**
         * At the same time, temporary ports are maintained until distribution ends and internal conflicts are reduced for testing.
         *
         * @param directory directory
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path directory) throws Exception {
            this.directory = directory;
            var peers = new ArrayList<RaftPeer>();
            var held = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = new java.net.ServerSocket(0);
                    held.add(socket);
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            } finally {
                for (var socket : held) {
                    socket.close();
                }
            }
            group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers);
            for (int i = 0; i < 3; i++) {
                nodes.add(null);
            }
            try {
                for (int i = 0; i < 3; i++) {
                    start(i);
                }
            } catch (final Throwable failure) {
                close();
                throw failure;
            }
        }

        /**
         * Reopen the original vote identity and activity generation and cannot be removed from the directory as a new node.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void start(final int index) throws Exception {
            nodes.set(index, RatisWalNode.openGenerationalLoopback(directory.resolve("n" + index), RaftPeerId.valueOf("n" + index), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(),
                    WalOptions.defaults().withTimeout(30_000), new WalBackupOptions(128_000_000, 32, 4_000_000, 60_000)));
        }

        /**
         * Stops the individual node and maintains all durable directories.
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
         * The selected node also requires a physical begin barrier certification service, and not just role caches.
         *
         * @param excluded excluded
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader(final int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
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
            throw new AssertionError("no ready leader with majority barrier");
        }

        /**
         * Installed kernels are unaccessible and must be complete generation readiness, snapshot position and operation watermark.
         *
         * @param index index
         * @param sequence sequence
         * @param snapshot snapshot
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void await(final int index, final long sequence, final long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                var node = nodes.get(index);
                try {
                    if (node.ready() && node.appliedSequence() == sequence && node.snapshotIndex() >= snapshot) {
                        return;
                    }
                } catch (final IllegalStateException installing) {
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("replica did not complete snapshot installation and catch up");
        }

        /**
         * Clearing all its own nodes and individual closure failure cannot leave any other testing services behind.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Throwable failure = null;
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    stop(i);
                } catch (final Throwable close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
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

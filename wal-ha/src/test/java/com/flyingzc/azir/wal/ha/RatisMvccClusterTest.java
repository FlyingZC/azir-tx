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

import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Real three nodes Raft integrates the actual MVCC kernel, with each verify cut main reservation confirmation and minority unable to return successfully.
 */
class RatisMvccClusterTest {

    @TempDir
    Path directory;

    /**
     * Cross-shard transaction has been confirmed to be complete and serial numbers continue to increase following the re-opening of stop leaders, new masters and old nodes.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void confirmedCrossShardCommitSurvivesLeaderChangeAndRestart() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(-1);
            var receipt = write(cluster.nodes.get(first), "one");
            Assertions.assertEquals(1, receipt.commitSequence());
            assertPair(cluster.nodes.get(first), "one");
            int follower = (first + 1) % 3;
            assertThrows(WalMvccException.class, () -> cluster.nodes.get(follower).begin(IsolationLevel.READ_COMMITTED));
            cluster.stop(first);
            int second = cluster.leader(first);
            assertPair(cluster.nodes.get(second), "one");
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(second).lookupCommit(receipt.transactionId()).status());
            Assertions.assertEquals(2, write(cluster.nodes.get(second), "two").commitSequence());
            cluster.start(first);
            cluster.awaitApplied(first, 2);
            assertPair(cluster.nodes.get(second), "two");
            assertTrue(cluster.nodes.get(first).appliedSequence() >= 2);
        }
    }

    /**
     * When two nodes are not available, one original leader is unable to confirm the new transaction or create a new read snapshot based on thermal cache.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void minorityCannotAcknowledgeWritesOrServeFreshSnapshots() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(-1);
            var node = cluster.nodes.get(leader);
            write(node, "committed");
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            node.put(tx, "a", "k", bytes("unconfirmed"));
            node.put(tx, "b", "k", bytes("unconfirmed"));
            for (int i = 0; i < 3; i++) {
                if (i != leader) {
                    cluster.stop(i);
                }
            }
            assertThrows(WalMvccException.class, () -> node.commit(tx));
            Assertions.assertEquals(1, node.appliedSequence());
            assertThrows(WalMvccException.class, () -> node.begin(IsolationLevel.READ_COMMITTED));
            Assertions.assertEquals(1, node.appliedSequence());
        }
    }

    /**
     * Read and write only through the node business entrance, complete write set in the same transaction commit.
     *
     * @param node node
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt write(final RatisWalNode node, final String value) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        node.put(tx, "a", "k", bytes(value));
        node.put(tx, "b", "k", bytes(value));
        return node.commit(tx);
    }

    /**
     * The same barrier statement snapshot checked two pieces and could not be used to mask half- transaction with two separate RC queries.
     *
     * @param node node
     * @param expected expected
     */
    private static void assertPair(final RatisWalNode node, final String expected) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var snapshot = node.beginReadStatement(tx);
        Assertions.assertEquals(expected, new String(node.get(tx, "a", "k", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        Assertions.assertEquals(expected, new String(node.get(tx, "b", "k", snapshot).orElseThrow(), StandardCharsets.UTF_8));
        node.rollback(tx);
    }

    /**
     * Fixed business value without string references or caches as durable storage proof.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Three real gRPC/Raft services each have separate logs and WAL directories.
     */
    private static final class Cluster implements AutoCloseable {

        final Path directory;

        final RaftGroup group;

        final List<RatisWalNode> nodes = new ArrayList<>();

        /**
         * The temporary port is only used for the current loop test, not connection other services.
         *
         * @param directory directory
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path directory) throws Exception {
            this.directory = directory;
            var peers = new ArrayList<RaftPeer>();
            var sockets = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
                    sockets.add(socket);
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            } finally {
                for (var socket : sockets) {
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
         * Use a full durable status to restart the original node and cannot be removed from voting or business directories to falsify the leveling.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void start(final int index) throws Exception {
            nodes.set(index, RatisWalNode.openLoopback(directory.resolve("n" + index), RaftPeerId.valueOf("n" + index), group, Set.of("a", "b"), Map.of(), WalOptions.defaults().withTimeout(5000)));
        }

        /**
         * Stop a own node and keep its durable file for recovery.
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
         * Waiting for the actual Raft role rather than a fixed sleep time to infer who is the leader.
         *
         * @param excluded excluded
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader(final int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            do {
                for (int i = 0; i < nodes.size(); i++) {
                    if (i != excluded && nodes.get(i) != null && nodes.get(i).leaderEpoch() > 0) {
                        return i;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(20);
            } while (System.nanoTime() < until);
            throw new AssertionError("no ready leader");
        }

        /**
         * Apply local to support old nodes and not use RPC connection as flat.
         *
         * @param index index
         * @param sequence sequence
         * @throws Exception if the operation cannot complete
         */
        void awaitApplied(final int index, final long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (nodes.get(index).appliedSequence() < sequence && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(20);
            }
            Assertions.assertEquals(sequence, nodes.get(index).appliedSequence());
        }

        /**
         * Only closes the node created by this test, and cumulatively closes without leaving a follow-up service.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    stop(i);
                } catch (final Exception error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}

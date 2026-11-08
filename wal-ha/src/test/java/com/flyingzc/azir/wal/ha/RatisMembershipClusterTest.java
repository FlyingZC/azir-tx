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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.client.RaftClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.concurrent.TimeUnit;

/**
 * Real Raft replacement, loss of integrity reconstruction, operation renewal and resuscitation of old nodes.
 */
class RatisMembershipClusterTest {

    @TempDir
    Path directory;

    /**
     * The old master 's entire local state was lost and rebuilt with a new ID from snapshot, the old disk replica is not confirmed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void newIdentityRebuildsLostLeaderAndRetiredLeaderCannotServe() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int old = cluster.leader(-1);
            var receipt = write(cluster.nodes.get(old), new byte[]{0}, new byte[]{100});
            byte[] a = null, b = null;
            for (int i = 1; i <= 10; i++) {
                a = new byte[480 * 1024];
                b = new byte[480 * 1024];
                Arrays.fill(a, (byte) i);
                Arrays.fill(b, (byte) (i + 100));
                write(cluster.nodes.get(old), a, b);
            }
            cluster.stop(old);
            Path disk = directory.resolve("old-disk");
            copy(directory.resolve("n" + old), disk);
            discard(directory.resolve("n" + old));
            assertFalse(Files.exists(directory.resolve("n" + old)));
            int leader = cluster.leader(old);
            long snapshot = cluster.nodes.get(leader).snapshot();
            cluster.nodes.get(leader).purgeForTest(snapshot);
            assertTrue(cluster.nodes.get(leader).firstLogIndexForTest() > 2);
            var plan = cluster.nodes.get(leader).planReplacement(RaftPeerId.valueOf("n" + old), cluster.newAddress, 10_000);
            Files.write(directory.resolve("replacement.ticket"), plan.encode());
            cluster.startReplacement(plan);
            assertThrows(WalMvccException.class, () -> cluster.replacement.begin(IsolationLevel.READ_COMMITTED));
            var staged = cluster.nodes.get(leader).beginReplacement(plan, 30_000);
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.LEARNER_ADDED, staged.phase());
            Assertions.assertEquals(3, staged.membership().voters().size());
            cluster.replacement.close();
            cluster.replacement = null;
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.WAITING_FOR_LEARNER, cluster.nodes.get(leader).completeReplacement(plan, 10_000).phase());
            cluster.startReplacement(plan);
            cluster.awaitReplacement(11, snapshot);
            var completed = cluster.nodes.get(leader).completeReplacement(RatisMemberReplacement.decode(Files.readAllBytes(directory.resolve("replacement.ticket"))), 30_000);
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, completed.phase());
            assertTrue(completed.membership().learners().isEmpty());
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, cluster.nodes.get(leader).completeReplacement(plan, 10_000).phase());
            cluster.transfer(cluster.nodes.get(leader), cluster.groupFor(completed.membership()), plan.replacement().getId());
            cluster.awaitReplacementLeader();
            assertPair(cluster.replacement, a, b);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.replacement.lookupCommit(receipt.transactionId()).status());
            Assertions.assertEquals(12, write(cluster.replacement, new byte[]{12}, new byte[]{112}).commitSequence());
            cluster.replacement.snapshot();
            cluster.replacement.close();
            cluster.replacement = null;
            cluster.startReplacement(plan);
            cluster.awaitReplacement(12, 0);
            leader = cluster.leader(-1);
            RatisWalNode serving = leader < 0 ? cluster.replacement : cluster.nodes.get(leader);
            assertPair(serving, new byte[]{12}, new byte[]{112});
            try (var retired = RatisWalNode.openGenerationalLoopback(disk, RaftPeerId.valueOf("n" + old), cluster.group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits())) {
                assertThrows(WalMvccException.class, () -> retired.begin(IsolationLevel.READ_COMMITTED));
                int current = cluster.leader(-1);
                serving = current < 0 ? cluster.replacement : cluster.nodes.get(current);
                Assertions.assertEquals(13, write(serving, new byte[]{13}, new byte[]{113}).commitSequence());
                assertPair(serving, new byte[]{13}, new byte[]{113});
            }
        }
    }

    /**
     * Another instrument could not cover replica that had been added, and the coordinator could continue to complete the original.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void sameTicketResumesAfterCoordinatorFailoverAndConflictingPlanIsRejected() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader(-1);
            int removed = (first + 1) % 3;
            var receipt = write(cluster.nodes.get(first), new byte[]{1}, new byte[]{101});
            var plan = cluster.nodes.get(first).planReplacement(RaftPeerId.valueOf("n" + removed), cluster.newAddress, 10_000);
            var conflicting = cluster.nodes.get(first).planReplacement(RaftPeerId.valueOf("n" + removed), "127.0.0.1:19099", 10_000);
            try (
                    var raw = RaftClient.newBuilder().setRaftGroup(cluster.group).setLeaderId(RaftPeerId.valueOf("n" + first)).setProperties(new org.apache.ratis.conf.RaftProperties())
                            .setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) {
                boolean rejected;
                try {
                    rejected = !raw.admin().setConfiguration(cluster.group.getPeers().stream().limit(2).toList()).isSuccess();
                } catch (final java.io.IOException forbidden) {
                    rejected = true;
                }
                assertTrue(rejected, "raw configuration RPC must not bypass managed replacement checks");
            }
            cluster.startReplacement(plan);
            cluster.nodes.get(first).beginReplacement(plan, 30_000);
            assertThrows(java.io.IOException.class, () -> cluster.nodes.get(first).beginReplacement(conflicting, 10_000));
            byte[] saved = plan.encode();
            cluster.stop(first);
            int next = cluster.leader(-1);
            if (next == removed) {
                int retained = (first + 2) % 3;
                cluster.transfer(cluster.nodes.get(next), plan.joiningGroup(), RaftPeerId.valueOf("n" + retained));
                next = cluster.leader(removed);
            }
            var completed = cluster.nodes.get(next).completeReplacement(RatisMemberReplacement.decode(saved), 30_000);
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, completed.phase());
            Assertions.assertEquals(2, write(cluster.nodes.get(next), new byte[]{2}, new byte[]{102}).commitSequence());
            assertThrows(WalMvccException.class, () -> cluster.nodes.get(removed).begin(IsolationLevel.READ_COMMITTED));
            cluster.start(first);
            cluster.await(first, 2);
            int current = cluster.leader(-1);
            var serving = current < 0 ? cluster.replacement : cluster.nodes.get(current);
            assertPair(serving, new byte[]{2}, new byte[]{102});
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, serving.lookupCommit(receipt.transactionId()).status());
            Assertions.assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, serving.completeReplacement(plan, 10_000).phase());
        }
    }

    /**
     * Interface failed during the total duration while managing the encapsulation; normal observation continues after release and configuration is not altered by error.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void stalledControlAdmissionHonorsDeadlineAndKeepsRoster() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(-1);
            var node = cluster.nodes.get(leader);
            var original = node.membership(10_000);
            var field = RatisWalNode.class.getDeclaredField("membershipExecutor");
            field.setAccessible(true);
            var executor = (java.util.concurrent.ExecutorService) field.get(node);
            var entered = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            executor.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (final InterruptedException failure) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            long started = System.nanoTime();
            try {
                assertThrows(java.io.IOException.class, () -> node.membership(100));
                assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
            } finally {
                release.countDown();
            }
            RatisMembershipView current = null;
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (current == null && System.nanoTime() < until) {
                try {
                    current = node.membership(1000);
                } catch (final java.io.IOException drainingExpiredTask) {
                    TimeUnit.MILLISECONDS.sleep(20);
                }
            }
            assertNotNull(current);
            assertTrue(RatisMembershipView.same(original.voters(), current.voters()));
            assertTrue(current.learners().isEmpty());
        }
    }

    /**
     * An transaction complete update of two pieces.
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
     * Checks the two binary contents in the same RR statement snapshot.
     *
     * @param node node
     * @param a a
     * @param b b
     */
    private static void assertPair(final RatisWalNode node, final byte[] a, final byte[] b) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var view = node.beginReadStatement(tx);
        assertArrayEquals(a, node.get(tx, "a", "k", view).orElseThrow());
        assertArrayEquals(b, node.get(tx, "b", "k", view).orElseThrow());
        node.rollback(tx);
    }

    /**
     * The current dedicated resource budget, Big write set is still within one MiB per transaction.
     *
     * @return the operation result
     */
    private static WalOptions options() {
        return WalOptions.defaults().withTimeout(30_000);
    }

    /**
     * Mirror maintenance settings are limited in total and time.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(128_000_000, 16, 4_000_000, 120_000);
    }

    /**
     * Copy its private test disk after stopping the old node and preserves the old state of Easter.
     *
     * @param source database connection source
     * @param target target
     * @throws Exception if the operation cannot complete
     */
    private static void copy(final Path source, final Path target) throws Exception {
        try (var paths = Files.walk(source)) {
            for (Path path : paths.toList()) {
                Path output = target.resolve(source.relativize(path));
                if (Files.isDirectory(path)) {
                    Files.createDirectories(output);
                } else {
                    Files.copy(path, output);
                }
            }
        }
    }

    /**
     * Only removes the node directory that this test has just been created and stopped, and simulates the permanent loss of all local durable status.
     *
     * @param target target
     * @throws Exception if the operation cannot complete
     */
    private static void discard(final Path target) throws Exception {
        try (var paths = Files.walk(target)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    /**
     * The initial three nodes and a new identity are private return services for the current round.
     */
    private static final class Cluster implements AutoCloseable {

        final Path directory;

        final RaftGroup group;

        final String newAddress;

        final List<RatisWalNode> nodes = new ArrayList<>();

        RatisWalNode replacement;

        RatisMemberReplacement plan;

        /**
         * The temporary port is kept to the distribution completion at the same time, avoiding internal duplication of this test.
         *
         * @param directory directory
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path directory) throws Exception {
            this.directory = directory;
            var peers = new ArrayList<RaftPeer>();
            var held = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 4; i++) {
                    var socket = new java.net.ServerSocket(0);
                    held.add(socket);
                    if (i < 3) {
                        peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                    }
                }
                newAddress = "127.0.0.1:" + held.getLast().getLocalPort();
            } finally {
                for (var socket : held) {
                    socket.close();
                }
            }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
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
         * Reopens the initial members to keep their directories.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void start(final int index) throws Exception {
            nodes.set(index, RatisWalNode.openGenerationalLoopback(directory.resolve("n" + index), RaftPeerId.valueOf("n" + index), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(),
                    options(), limits()));
        }

        /**
         * The new node always follows the new ID in the document, first using an empty directory and restarting its own durable status.
         *
         * @param plan plan
         * @throws Exception if the operation cannot complete
         */
        void startReplacement(final RatisMemberReplacement plan) throws Exception {
            this.plan = plan;
            replacement = RatisWalNode.openGenerationalLoopback(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), Set.of("a", "b"),
                    WalReplicaBackendFactory.localOnly(), options(), limits());
        }

        /**
         * Stops only the members that have been created.
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
         * Allows the new identity to become a leader, returns negative one for the new node and calls with majority barrier.
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
                    if (i != excluded && node != null && available(node)) {
                        return i;
                    }
                }
                if (replacement != null && available(replacement)) {
                    return -1;
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("no member with majority read barrier");
        }

        /**
         * Actual operations barrier instead of old role caches are serviceable.
         *
         * @param node node
         * @return the operation result
         */
        private static boolean available(final RatisWalNode node) {
            if (node.leaderEpoch() <= 0) {
                return false;
            }
            try {
                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                node.rollback(tx);
                return true;
            } catch (final WalMvccException transientRole) {
                return false;
            }
        }

        /**
         * New identity must complete the snapshot or log chase.
         *
         * @param sequence sequence
         * @param snapshot snapshot
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitReplacement(final long sequence, final long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                try {
                    if (replacement.ready() && replacement.appliedSequence() == sequence && replacement.snapshotIndex() >= snapshot) {
                        return;
                    }
                } catch (final IllegalStateException installing) {
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("replacement did not catch up");
        }

        /**
         * The original member recovery must also catch up with the confirmed state.
         *
         * @param index index
         * @param sequence sequence
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void await(final int index, final long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
            while (System.nanoTime() < until) {
                if (nodes.get(index).ready() && nodes.get(index).appliedSequence() == sequence) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("original member did not catch up");
        }

        /**
         * Verify new members can really pass majority barrier after the transfer of leadership.
         *
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitReplacementLeader() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                if (available(replacement)) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("replacement did not become serving leader");
        }

        /**
         * The tests use the official transfer API, and all nodes remain in this round ' s own return ring.
         *
         * @param source database connection source
         * @param configuration configuration
         * @param target target
         * @throws Exception if the operation cannot complete
         */
        void transfer(final RatisWalNode source, final RaftGroup configuration, final RaftPeerId target) throws Exception {
            RaftPeerId leader = source == replacement ? plan.replacement().getId() : RaftPeerId.valueOf("n" + nodes.indexOf(source));
            try (
                    var client = RaftClient.newBuilder().setRaftGroup(configuration).setLeaderId(leader).setProperties(new org.apache.ratis.conf.RaftProperties())
                            .setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) {
                assertTrue(client.admin().transferLeadership(target, 10_000).isSuccess());
            }
        }

        /**
         * Client configuration comes from the actual completed group of members.
         *
         * @param view view
         * @return the operation result
         */
        RaftGroup groupFor(final RatisMembershipView view) {
            var peers = new ArrayList<>(view.voters());
            peers.addAll(view.learners());
            return RaftGroup.valueOf(group.getGroupId(), peers);
        }

        /**
         * Clears all own services without leaving other nodes behind because of the failure to close a single node.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Throwable failure = null;
            if (replacement != null) {
                try {
                    replacement.close();
                } catch (final Throwable close) {
                    failure = close;
                }
            }
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

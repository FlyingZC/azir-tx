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

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Verify Copys the boundaries of kernel consumption consensus decision; this controlled port is not produced Raft.
 */
class WalReplicaCoreTest {

    @TempDir
    Path directory;

    /**
     * Commit returns do not write local commit logs and do not publish or return business success.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void onlyCommittedApplicationPublishesAndAcknowledgesWholeBatch() throws Exception {
        try (var cluster = new Cluster(directory); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var leader = cluster.nodes.getFirst();
            var tx = pair(leader, "value");
            var future = tasks.submit(() -> leader.commit(tx));
            assertTrue(cluster.submitted.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone());
            for (var node : cluster.nodes) {
                Assertions.assertEquals(0, node.stats().publishedSequence());
                Assertions.assertEquals(60, node.stats().walBytes());
            }
            cluster.applyPending();
            Assertions.assertEquals(1, future.get(5, TimeUnit.SECONDS).commitSequence());
            for (var node : cluster.nodes) {
                Assertions.assertEquals(1, node.stats().publishedSequence());
            }
            cluster.moveLeader(1);
            var reader = cluster.nodes.get(1).begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = cluster.nodes.get(1).beginReadStatement(reader);
            for (String shard : Set.of("a", "b")) {
                Assertions.assertEquals("value", text(cluster.nodes.get(1).get(reader, shard, "k", snapshot).orElseThrow()));
            }
            cluster.nodes.get(1).rollback(reader);
            assertThrows(WalMvccException.class, () -> leader.begin(IsolationLevel.READ_COMMITTED));
        }
    }

    /**
     * Consensus waiting to fail is not rollback; late commit call-back can resolve UUID and cannot reuse business serial numbers during this period.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unresolvedSubmissionBlocksNewDecisionsUntilCommittedResolution() throws Exception {
        try (var cluster = new Cluster(directory); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var leader = cluster.nodes.getFirst();
            var tx = pair(leader, "late");
            var future = tasks.submit(() -> leader.commit(tx));
            assertTrue(cluster.submitted.await(5, TimeUnit.SECONDS));
            cluster.response.completeExceptionally(new IllegalStateException("lost consensus response"));
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ((WalMvccException) failure.getCause()).errorCode());
            waitFor(() -> leader.lifecycle().proposalBlocked());
            Assertions.assertEquals(0, leader.stats().publishedSequence());
            assertThrows(WalMvccException.class, () -> leader.begin(IsolationLevel.READ_COMMITTED));
            cluster.applyPending();
            assertFalse(leader.lifecycle().proposalBlocked());
            assertTrue(leader.findCommitted(tx.id()).isPresent());
            cluster.response = new CompletableFuture<>();
            cluster.submitted = new CountDownLatch(1);
            var next = pair(leader, "next");
            var committing = tasks.submit(() -> leader.commit(next));
            assertTrue(cluster.submitted.await(5, TimeUnit.SECONDS));
            cluster.applyPending();
            Assertions.assertEquals(2, committing.get(5, TimeUnit.SECONDS).commitSequence());
        }
    }

    /**
     * Change of term retirement old handle, delayed notification cannot wrongly clear the new term transaction.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void fencingInvalidatesOldTransactionsAndPreservesNewEpochHandles() throws Exception {
        try (var cluster = new Cluster(directory)) {
            var old = cluster.nodes.getFirst().begin(IsolationLevel.REPEATABLE_READ);
            cluster.moveLeader(1);
            assertThrows(WalMvccException.class, () -> cluster.nodes.getFirst().get(old, "a", "k"));
            var current = cluster.nodes.get(1).begin(IsolationLevel.REPEATABLE_READ);
            cluster.nodes.get(1).fenceEpoch(1);
            assertTrue(cluster.nodes.get(1).get(current, "a", "k").isEmpty());
            cluster.nodes.get(1).rollback(current);
        }
    }

    /**
     * Modes, groups and nodes identity durable bind; neither the normal entrance nor another node can take over the directory.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void replicaModeAndIdentitySurviveCheckpointAndRejectStandaloneOpen() throws Exception {
        Path root = directory.resolve("identity");
        var authority = new Immediate();
        var identity = new WalReplicaIdentity(UUID.randomUUID(), UUID.randomUUID(), "node1");
        try (var runtime = WalMvccRuntime.openReplica(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime;
            runtime.commit(pair(runtime, "persisted"));
            assertTrue(runtime.checkpoint().created());
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(root, Set.of("a", "b"), WalOptions.defaults()));
        var wrong = new WalReplicaIdentity(identity.engineId(), identity.groupId(), "node2");
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.openReplica(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), wrong, authority));
        try (var runtime = WalMvccRuntime.openReplica(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime;
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
            Assertions.assertEquals(2, runtime.commit(pair(runtime, "after")).commitSequence());
            assertThrows(WalMvccException.class, () -> runtime.backup(directory.resolve("unsafe-replica-backup"), WalBackupOptions.defaults()));
        }
    }

    /**
     * The durable entry for missing initialised directories must be rejected and the original replica cannot be created by mistake as a zero serial number.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void replicaIdentityWithoutWalIsNotAnEmptyEngine() throws Exception {
        Path root = directory.resolve("missing");
        var authority = new Immediate();
        var identity = new WalReplicaIdentity(UUID.randomUUID(), UUID.randomUUID(), "node");
        try (var runtime = WalMvccRuntime.openReplica(root, Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime;
        }
        Files.delete(root.resolve("ENGINE"));
        Files.delete(root.resolve("transactions.wal"));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.openReplica(root, Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority));
        assertFalse(Files.exists(root.resolve("transactions.wal")));
    }

    /**
     * Replay matches decision idempotent, and stops this replica when the content is different or the serial number jumps without divulging part of it.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void committedReplayMustMatchExactDecision() throws Exception {
        var authority = new Immediate();
        var identity = new WalReplicaIdentity(UUID.randomUUID(), UUID.randomUUID(), "node");
        try (var runtime = WalMvccRuntime.openReplica(directory, Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime;
            var record = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value"))));
            runtime.applyCommitted(authority, List.of(record));
            runtime.applyCommitted(authority, List.of(record));
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
            var bad = new WalCommit(record.transactionId(), 1, 0, List.of(new WalMutation("a", "k", bytes("wrong"))));
            assertThrows(WalMvccException.class, () -> runtime.applyCommitted(authority, List.of(bad)));
            assertNotNull(runtime.lifecycle().fatal());
        }
    }

    /**
     * Construct the complete cross-shard write set of the same transaction.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static WalTransaction pair(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", "k", bytes(value));
        runtime.put(tx, "b", "k", bytes(value));
        return tx;
    }

    /**
     * Controlled ports allow testing to separate commit requests and commit calls.
     */
    private static final class Cluster implements AutoCloseable {

        final List<WalMvccRuntime> nodes = new ArrayList<>();

        final List<Authority> authorities = new ArrayList<>();

        volatile int leader;

        volatile long term = 1;

        volatile CountDownLatch submitted = new CountDownLatch(1);

        volatile CompletableFuture<Void> response = new CompletableFuture<>();

        volatile List<WalCommit> pending;

        /**
         * Each replica has a separate directory and shares the logic engine identity.
         *
         * @param directory directory
         */
        Cluster(final Path directory) {
            UUID engine = UUID.randomUUID();
            UUID group = UUID.randomUUID();
            for (int i = 0; i < 3; i++) {
                var authority = new Authority(this, i);
                authorities.add(authority);
                nodes.add(WalMvccRuntime.openReplica(directory.resolve("node" + i), Set.of("a", "b"), Map.of(), WalOptions.defaults().withTimeout(5000),
                        WalRetentionOptions.preserveAll(), new WalReplicaIdentity(engine, group, "node" + i), authority));
            }
        }

        /**
         * Simulates that batch of commit has been determined by a trusted port, with two replica first installed and the leader installed.
         */
        void applyPending() {
            for (int i = 0; i < 3; i++) {
                if (i != leader) {
                    nodes.get(i).applyCommitted(authorities.get(i), pending);
                }
            }
            nodes.get(leader).applyCommitted(authorities.get(leader), pending);
            response.complete(null);
        }

        /**
         * Simulation of role announcements does not mean that a production election agreement has been reached.
         *
         * @param next next
         */
        void moveLeader(final int next) {
            long old = term;
            leader = next;
            term++;
            for (var node : nodes) {
                node.fenceEpoch(old);
            }
        }

        /**
         * Release the outstanding request and avoid testing the failure legacy waiting thread.
         */
        @Override
        public void close() {
            response.completeExceptionally(new IllegalStateException("test closed"));
            for (var node : nodes) {
                node.close();
            }
        }
    }

    /**
     * Each kernel uses an independent capable object and cannot be installed at the port of another node.
     *
     * @param cluster cluster
     * @param node node
     */
    private record Authority(Cluster cluster, int node) implements WalCommitAuthority {

        /**
         * Local Role checks are not blocked.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return cluster.leader == node ? cluster.term : 0;
        }

        /**
         * Controlled barrier, real network barrier tested separately in Ratis integration.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeoutMillis) {
            return epoch();
        }

        /**
         * Saves candidate batch, the parent test visible determines when to call back for installation.
         *
         * @param epoch epoch
         * @param commits commits
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> commits) {
            if (epoch != epoch()) {
                return CompletableFuture.failedFuture(new IllegalStateException("not leader"));
            }
            cluster.pending = commits;
            cluster.submitted.countDown();
            return cluster.response;
        }
    }

    /**
     * Synchronization check core does not have a non-renewable cross-line installation lock while waiting for consensus.
     */
    private static final class Immediate implements WalCommitAuthority {

        WalMvccRuntime runtime;

        /**
         * Fixed test term.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return 1;
        }

        /**
         * Returns a fixed verify test term.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeoutMillis) {
            return 1;
        }

        /**
         * The complete record is installed for confirmation.
         *
         * @param epoch epoch
         * @param commits commits
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> commits) {
            runtime.applyCommitted(this, commits);
            return CompletableFuture.completedFuture(null);
        }
    }
}

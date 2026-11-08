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
import com.flyingzc.azir.wal.WalBackupRow;
import com.flyingzc.azir.wal.WalCheckpoint;
import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalCommitAuthority;
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRestoreSpec;
import com.flyingzc.azir.wal.WalRetentionOptions;
import com.flyingzc.azir.wal.WalValue;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.client.RaftClient;

import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

/**
 * Watermark Freeze verify after failure of independent backend detection, concurrent materialization watermark application.
 */
class RatisBackendProbeTest {

    @TempDir
    Path directory;

    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();

    /**
     * Only one restricted case is activated for the entire class, and logic replica still has independent data space.
     */
    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
    }

    /**
     * Only the examples available for this test are discontinued.
     */
    @AfterAll
    static void stopDatabase() {
        DATABASE.stop();
    }

    /**
     * Health JDBC waits for replicability at the same node and cannot return to health beyond detection budget.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void slowDiagnosticDoesNotBlockCommittedApplication() throws Exception {
        try (var cluster = new Cluster(directory); var tasks = Executors.newVirtualThreadPerTaskExecutor(); var client = cluster.client()) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            cluster.write(node, 1);
            cluster.materialized(node, 1);
            cluster.gate.arm("n" + leader);
            var diagnostic = tasks.submit(() -> client.io().sendStaleRead(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisReplicaStatus.request())), 0,
                    cluster.peers.get(leader).getId()));
            try {
                assertTrue(cluster.gate.entered.await(5, TimeUnit.SECONDS));
                var write = tasks.submit(() -> cluster.write(node, 2));
                Assertions.assertEquals(2, write.get(3, TimeUnit.SECONDS).commitSequence());
                assertFalse(diagnostic.isDone());
                TimeUnit.MILLISECONDS.sleep(1100);
            } finally {
                cluster.gate.release.countDown();
            }
            var reply = diagnostic.get(10, TimeUnit.SECONDS);
            assertTrue(reply.isSuccess());
            assertFalse(RatisReplicaStatus.decode(reply.getMessage().getContent().toByteArray()).healthy());
            var fresh =
                    client.io().sendStaleRead(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisReplicaStatus.request())), 0, cluster.peers.get(leader).getId());
            assertTrue(RatisReplicaStatus.decode(fresh.getMessage().getContent().toByteArray()).healthy());
        }
    }

    /**
     * The materialization boost during the query can be higher than the old release sample and must be checked with the post-survey release upper bounds.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentMaterializationUsesPublicationAfterDatabaseRead() throws Exception {
        try (var cluster = new Cluster(directory); var worker = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("wal-health-probe-watermark").factory())) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            cluster.write(node, 1);
            cluster.materialized(node, 1);
            cluster.gate.arm("n" + leader);
            var state = state(node);
            var health = worker.submit(() -> state.backendHealthy(state.core(), 10_000));
            try {
                assertTrue(cluster.gate.entered.await(5, TimeUnit.SECONDS));
                cluster.write(node, 2);
                cluster.write(node, 3);
                cluster.materialized(node, 3);
            } finally {
                cluster.gate.release.countDown();
            }
            assertTrue(health.get(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Following the failure of an commit log application, no-op should not skip a failed record for an observer application watermark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failedApplicationFreezesFollowingIndexNotifications() throws Exception {
        UUID group = UUID.randomUUID();
        var identity = new WalReplicaIdentity(group, group, "unit");
        var source = new WalCommitAuthority() {

            /**
             * Only the legal term required for a copy mode closure is provided.
             *
             * @return the operation result
             */
            @Override
            public long epoch() {
                return 1;
            }

            /**
             * This special purpose has no business transaction, do not send network requests.
             *
             * @param millis millis
             * @return the operation result
             */
            @Override
            public long readBarrier(final long millis) {
                return 1;
            }

            /**
             * There is no need to falsify the majority for verify malformation already recorded in commit.
             *
             * @param term term
             * @param commits commits
             * @return the operation result
             */
            @Override
            public CompletionStage<Void> submit(final long term, final List<WalCommit> commits) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        try (
                var core = WalMvccRuntime.openReplica(directory.resolve("wal"), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, source);
                var state = new RatisWalState(core); var machine = new RatisWalStateMachine(identity, state, null, WalBackupOptions.defaults())) {
            var entry = org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(2).setIndex(4)
                    .setStateMachineLogEntry(
                            org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(new byte[]{1})))
                    .build();
            var context = org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(machine).setLogEntry(entry).build();
            assertThrows(ExecutionException.class, () -> machine.applyTransaction(context).get(5, TimeUnit.SECONDS));
            assertTrue(machine.failed());
            var before = machine.getLastAppliedTermIndex();
            machine.notifyTermIndexUpdated(2, 5);
            machine.query(Message.valueOf("drain")).get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(before, machine.getLastAppliedTermIndex());
            var status = RatisReplicaStatus.decode(machine.query(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisReplicaStatus.request())))
                    .get(5, TimeUnit.SECONDS).getContent().toByteArray());
            assertFalse(status.healthy());
        }
    }

    /**
     * In the package, read-only takes the generation manager of the actual node and does not replace any production call.
     *
     * @param node node
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static RatisWalState state(final RatisWalNode node) throws Exception {
        var field = RatisWalNode.class.getDeclaredField("state");
        field.setAccessible(true);
        return (RatisWalState) field.get(node);
    }

    /**
     * Controlled pauses affect only the detection thread of a given node and do not suspend database or copy the thread.
     */
    private static final class Gate {

        volatile String owner;

        volatile CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);

        /**
         * Each experiment re-establishes the synchronized boundary.
         *
         * @param owner owner
         */
        void arm(final String owner) {
            this.owner = owner;
            entered = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        /**
         * The real checkpoint query is paused before it starts.
         *
         * @param owner owner
         * @throws IllegalStateException if the operation cannot complete
         */
        void hit(final String owner) {
            if (!owner.equals(this.owner) || !Thread.currentThread().getName().startsWith("wal-health-probe")) {
                return;
            }
            this.owner = null;
            entered.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("probe gate observation expired");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        }
    }

    /**
     * Three real Ratis nodes and stand-alone MySQL namespace, controlled packaging only changes queries waiting.
     */
    private static final class Cluster implements AutoCloseable {

        final Gate gate = new Gate();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final List<org.apache.ratis.protocol.RaftPeer> peers = new ArrayList<>();

        final HikariDataSource control;

        final RaftGroup group;

        /**
         * New control library and group identity for each scene to avoid inheriting the occupied space of another scene.
         *
         * @param root root
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root) throws Exception {
            String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
            MysqlGenerationSupport.createControl(DATABASE, schema);
            control = MysqlGenerationSupport.control(DATABASE, schema);
            var reservations = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = MysqlGenerationSupport.reservePeerPort();
                    reservations.add(socket);
                    peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            } finally {
                for (var socket : reservations) {
                    socket.close();
                }
            }
            group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
            var base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            WalReplicaBackendFactory factory = (identity, generation) -> {
                var lease = base.open(identity, generation);
                var stores = new HashMap<String, WalMaterializedStore>();
                lease.stores().forEach((shard, store) -> stores.put(shard, new ProbeStore(store, identity.nodeId(), gate)));
                return new WalReplicaBackendFactory.Lease(stores, lease::close);
            };
            try {
                for (int i = 0; i < 3; i++) {
                    nodes.add(RatisWalNode.openGenerationalLoopback(root.resolve("n" + i), peers.get(i).getId(), group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(5000),
                            new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000)));
                }
            } catch (final Exception failure) {
                close();
                throw failure;
            }
        }

        /**
         * The diagnosis has a waiting period for an independent client and does not depend on test-fixed sleep judgement.
         *
         * @return the operation result
         */
        RaftClient client() {
            var properties = new org.apache.ratis.conf.RaftProperties();
            org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(properties, org.apache.ratis.util.TimeDuration.valueOf(10, TimeUnit.SECONDS));
            return RaftClient.newBuilder().setRaftGroup(group).setProperties(properties).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build();
        }

        /**
         * The real role is to identify the leader with read barrier.
         *
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    if (nodes.get(i).leaderEpoch() > 0) {
                        var tx = nodes.get(i).begin(IsolationLevel.REPEATABLE_READ);
                        nodes.get(i).rollback(tx);
                        return i;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("leader missing");
        }

        /**
         * Both pieces are fully written to the same transaction.
         *
         * @param node node
         * @param value value to write
         * @return the operation result
         */
        WalReceipt write(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) {
                node.put(tx, shard, "k", (shard + value).getBytes(StandardCharsets.UTF_8));
            }
            return node.commit(tx);
        }

        /**
         * Only when the real database materialization prefix is pushed will it be released.
         *
         * @param node node
         * @param sequence sequence
         * @throws Exception if the operation cannot complete
         */
        void materialized(final RatisWalNode node, final long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (node.localStats().materializedSequences().values().stream().anyMatch(value -> value < sequence) && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            assertTrue(node.localStats().materializedSequences().values().stream().allMatch(value -> value >= sequence));
        }

        /**
         * The anomaly also removes the doorlock of this test and closes all owned nodes and control pools.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            gate.release.countDown();
            Exception failure = null;
            for (var node : nodes) {
                try {
                    node.close();
                } catch (final Exception close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
                    }
                }
            }
            control.close();
            if (failure != null) {
                throw failure;
            }
        }
    }

    /**
     * Checkpoint is suspended only for back-end detection threads, and other data and recovery operations entrust real MySQL.
     *
     * @param delegate delegate
     * @param owner owner
     * @param gate gate
     */
    private record ProbeStore(WalMaterializedStore delegate, String owner, Gate gate) implements WalMaterializedStore {

        /**
         * Keeps the full backend capacity validate.
         *
         * @param mutation mutation
         */
        @Override
        public void validateMutation(final WalMutation mutation) {
            delegate.validateMutation(mutation);
        }

        /**
         * The general binding rule of rejection is retained.
         *
         * @param engine engine
         * @param shard logical shard identifier
         */
        @Override
        public void initialize(final UUID engine, final String shard) {
            delegate.initialize(engine, shard);
        }

        /**
         * Retain a copy capability declaration.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsReplicaOwnership() {
            return delegate.supportsReplicaOwnership();
        }

        /**
         * Retains the receiving replica ownership.
         *
         * @param identity identity
         * @param shard logical shard identifier
         */
        @Override
        public void initializeReplica(final WalReplicaIdentity identity, final String shard) {
            delegate.initializeReplica(identity, shard);
        }

        /**
         * Directly query the real materialization prefix.
         *
         * @return the operation result
         */
        @Override
        public WalCheckpoint checkpoint() {
            gate.hit(owner);
            return delegate.checkpoint();
        }

        /**
         * Apply real transaction directly.
         *
         * @param commits commits
         * @return the operation result
         */
        @Override
        public WalCheckpoint apply(final List<WalCommit> commits) {
            return delegate.apply(commits);
        }

        /**
         * Do not include test data caches.
         *
         * @param key business key
         * @param sequence sequence
         * @return the operation result
         */
        @Override
        public Optional<WalValue> get(final String key, final long sequence) {
            return delegate.get(key, sequence);
        }

        /**
         * Keeps the real write conflict query.
         *
         * @param key business key
         * @return the operation result
         */
        @Override
        public long latestSequence(final String key) {
            return delegate.latestSequence(key);
        }

        /**
         * Keeps the true range query.
         *
         * @param from from
         * @param to to
         * @param sequence sequence
         * @return the operation result
         */
        @Override
        public NavigableMap<String, WalValue> scan(final String from, final String to, final long sequence) {
            return delegate.scan(from, to, sequence);
        }

        /**
         * Keeps the real backend collection.
         *
         * @param safe safe
         * @param keys keys
         * @return the operation result
         */
        @Override
        public int gc(final long safe, final int keys) {
            return delegate.gc(safe, keys);
        }

        /**
         * Retain bounded maintenance capability.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsBoundedMaintenance() {
            return delegate.supportsBoundedMaintenance();
        }

        /**
         * Keeps the real bounded collection.
         *
         * @param safe safe
         * @param keys keys
         * @param rows rows
         * @param millis millis
         * @return the operation result
         */
        @Override
        public GcProgress gcBounded(final long safe, final int keys, final int rows, final long millis) {
            return delegate.gcBounded(safe, keys, rows, millis);
        }

        /**
         * Connection and durable Sex validate are still implemented by official adapters.
         */
        @Override
        public void validateProduction() {
            delegate.validateProduction();
        }

        /**
         * Export still takes place using real flows.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsBackup() {
            return delegate.supportsBackup();
        }

        /**
         * The concurrent snapshot capability is provided by the physical backend.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsPinnedSnapshot() {
            return delegate.supportsPinnedSnapshot();
        }

        /**
         * Fixed read-only transaction for the real backend. Simulate snapshot without packaging.
         *
         * @param boundary boundary
         * @return the operation result
         */
        @Override
        public SnapshotView openSnapshot(final WalCheckpoint boundary) {
            return delegate.openSnapshot(boundary);
        }

        /**
         * Recovery Ownership capacity remains unchanged.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsReplicaRestore() {
            return delegate.supportsReplicaRestore();
        }

        /**
         * Exports real cold data.
         *
         * @param boundary boundary
         * @param sink sink
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void exportSnapshot(final WalCheckpoint boundary, final BackupSink sink) throws IOException {
            delegate.exportSnapshot(boundary, sink);
        }

        /**
         * Recovery Separator Mark is formally achieved durable storage.
         *
         * @param spec spec
         * @return the operation result
         */
        @Override
        public boolean beginRestore(final WalRestoreSpec spec) {
            return delegate.beginRestore(spec);
        }

        /**
         * Make it clear to stop after the real batch commit is finished.
         *
         * @param spec spec
         * @param rows rows
         */
        @Override
        public void restoreBatch(final WalRestoreSpec spec, final List<WalBackupRow> rows) {
            delegate.restoreBatch(spec, rows);
        }

        /**
         * Full data summaries and prefix releases are still formally reconciled.
         *
         * @param spec spec
         * @param rows rows
         * @param hash hash
         */
        @Override
        public void finishRestore(final WalRestoreSpec spec, final long rows, final String hash) {
            delegate.finishRestore(spec, rows, hash);
        }
    }
}

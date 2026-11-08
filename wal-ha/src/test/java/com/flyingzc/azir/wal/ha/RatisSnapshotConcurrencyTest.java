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

import com.flyingzc.azir.wal.WalBackup;
import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalBackupRow;
import com.flyingzc.azir.wal.WalCheckpoint;
import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRestoreSpec;
import com.flyingzc.azir.wal.WalValue;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

/**
 * The real Raft commit and closed concurrent verify during the big mirror export, using the actual MySQL fixed-read view at the back end.
 */
class RatisSnapshotConcurrencyTest {

    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();

    @TempDir
    Path directory;

    /**
     * A restricted case saves three logical replica separate spaces.
     */
    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
    }

    /**
     * Only examples of this category are released.
     */
    @AfterAll
    static void stopDatabase() {
        DATABASE.stop();
    }

    /**
     * Commit and read the new border during export of the old boundary, and ultimately snapshot must retain the location of the old border.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void imageExportDoesNotBlockPublicationOrMixLaterTransactions() throws Exception {
        try (var cluster = new Cluster(directory); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var receipt = cluster.write(node, 1);
            cluster.gate.owner = "n" + leader;
            var snapshot = workers.submit(node::snapshot);
            try {
                assertTrue(cluster.gate.entered.await(10, TimeUnit.SECONDS));
                node.enableVersionGc(new RatisGcOptions(100, 4, 4, 100, 5000));
                var update = workers.submit(() -> cluster.write(node, 2));
                Assertions.assertEquals(2, update.get(5, TimeUnit.SECONDS).commitSequence());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (node.gcStats().removedRows() < 2 && System.nanoTime() < until) {
                    TimeUnit.MILLISECONDS.sleep(20);
                }
                assertTrue(node.gcStats().removedRows() >= 2, node.gcStats().toString());
                Assertions.assertEquals(2, node.gcStats().safePoint());
                assertFalse(snapshot.isDone());
                cluster.verify(node, 2);
            } finally {
                cluster.gate.release.countDown();
            }
            long index = snapshot.get(30, TimeUnit.SECONDS);
            var active = RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE"));
            Assertions.assertEquals(index, active.index());
            Assertions.assertEquals(1, active.sequence());
            // The source has physically removed the old SQL version and a fixed read view must still export the full double value of the old recovery boundary.
            var machineField = RatisWalNode.class.getDeclaredField("machine");
            machineField.setAccessible(true);
            var machine = (RatisWalStateMachine) machineField.get(node);
            Path bundle = machine.getStateMachineStorage().getSnapshotDir().toPath().resolve("snapshot." + active.term() + "_" + index);
            Path backup = directory.resolve("exported");
            RatisSnapshotBundle.extract(bundle, backup, RaftGroupId.valueOf(active.identity().groupId()), org.apache.ratis.server.protocol.TermIndex.valueOf(active.term(), index), limits());
            var receiver = new WalReplicaIdentity(active.identity().engineId(), active.identity().groupId(), "restored");
            var factory = new MysqlWalReplicaBackendFactory(Map.of("a", cluster.control, "b", cluster.control), MysqlWalReplicaBackendFactory.Options.defaults());
            try (var lease = factory.open(receiver, UUID.randomUUID())) {
                WalBackup.restoreReplica(backup, directory.resolve("restored-wal"), lease.stores(), WalOptions.defaults(), limits(), receiver);
                for (String shard : List.of("a", "b")) {
                    assertArrayEquals((shard + "1").getBytes(StandardCharsets.UTF_8), lease.stores().get(shard).get("k", 1).orElseThrow().value());
                }
            }
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, node.lookupCommit(receipt.transactionId()).status());
            Assertions.assertEquals(3, cluster.write(node, 3).commitSequence());
            cluster.verify(node, 3);
        }
    }

    /**
     * The retry with no new transaction must be completed without re-establishing another input after the mirror has been fully released but the error is activated.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void existingPublishedSnapshotRetriesItsActivationWithoutNewCommit() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var receipt = cluster.write(node, 1);
            long stableSince = System.nanoTime(), last = node.appliedRaftIndex(), deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() - stableSince < TimeUnit.MILLISECONDS.toNanos(500) && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(20);
                long current = node.appliedRaftIndex();
                if (current != last) {
                    last = current;
                    stableSince = System.nanoTime();
                }
            }
            assertTrue(System.nanoTime() - stableSince >= TimeUnit.MILLISECONDS.toNanos(500));
            cluster.publicationFailureOwner = "n" + leader;
            cluster.failPublication.set(true);
            assertThrows(IOException.class, node::snapshot);
            long published = node.snapshotIndex();
            assertTrue(published >= 0);
            var before = RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE"));
            assertTrue(before.index() < published);
            var field = RatisWalNode.class.getDeclaredField("machine");
            field.setAccessible(true);
            var machine = (RatisWalStateMachine) field.get(node);
            Path sm = machine.getStateMachineStorage().getSnapshotDir().toPath();
            Path image;
            try (var files = Files.list(sm)) {
                image = files.filter(path -> path.getFileName().toString().matches("snapshot\\.[0-9]+_" + published)).findFirst().orElseThrow();
            }
            byte[] bytes = Files.readAllBytes(image);
            Object inode = Files.readAttributes(image, java.nio.file.attribute.BasicFileAttributes.class).fileKey();
            Assertions.assertEquals(published, node.snapshot());
            var active = RatisGenerationRecord.read(directory.resolve("n" + leader + "/business/ACTIVE"));
            Assertions.assertEquals(published, active.index());
            Assertions.assertEquals(1, active.sequence());
            Assertions.assertEquals(inode, Files.readAttributes(image, java.nio.file.attribute.BasicFileAttributes.class).fileKey());
            assertArrayEquals(bytes, Files.readAllBytes(image));
            Assertions.assertEquals(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)), active.hash());
            Assertions.assertEquals(receipt, node.lookupCommit(receipt.transactionId()).receipt());
            Assertions.assertEquals(2, cluster.write(node, 2).commitSequence());
            cluster.verify(node, 2);
        }
    }

    /**
     * Node closes must wait for your own export view to be released and cannot close the bottom connection pool earlier.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void nodeCloseDrainsOwnedExportWithoutInterruptingWal() throws Exception {
        try (var cluster = new Cluster(directory); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var receipt = cluster.write(node, 1);
            cluster.gate.owner = "n" + leader;
            var snapshot = workers.submit(node::snapshot);
            assertTrue(cluster.gate.entered.await(10, TimeUnit.SECONDS));
            var closing = workers.submit(() -> {
                node.close();
                return true;
            });
            try {
                assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            } finally {
                cluster.gate.release.countDown();
            }
            assertTrue(closing.get(30, TimeUnit.SECONDS));
            cluster.nodes.set(leader, null);
            try {
                snapshot.get(30, TimeUnit.SECONDS);
            } catch (final ExecutionException closedBeforePublish) {
                assertTrue(closedBeforePublish.getCause() instanceof IOException);
            }
            int next = cluster.leader();
            cluster.verify(cluster.nodes.get(next), 1);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(next).lookupCommit(receipt.transactionId()).status());
        }
    }

    /**
     * The watermark observation and operation commit can advance even if the file release lock is occupied for long periods of time.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void snapshotStorageWaitDoesNotBlockWatermarkObservation() throws Exception {
        try (var cluster = new Cluster(directory); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            cluster.write(node, 1);
            cluster.gate.owner = "n" + leader;
            var snapshot = workers.submit(node::snapshot);
            try {
                assertTrue(cluster.gate.entered.await(10, TimeUnit.SECONDS));
                var machineField = RatisWalNode.class.getDeclaredField("machine");
                machineField.setAccessible(true);
                var storageField = RatisWalStateMachine.class.getDeclaredField("snapshots");
                storageField.setAccessible(true);
                Object storage = storageField.get(machineField.get(node));
                // Suspends the file lock used for real distribution, and cannot allow file maintenance to hold an generation lock and block the observation thread.
                synchronized (storage) {
                    node.enableHealthManagement(new RatisHealthOptions(200, 2, 2, 750, 6000));
                    node.enableSnapshotMaintenance(new RatisSnapshotOptions(100, 10_000_000, 60_000, 32L * 1024 * 1024, 1024));
                    long initial = node.snapshotStats().rounds(), until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (node.snapshotStats().rounds() < initial + 3 && System.nanoTime() < until) {
                        TimeUnit.MILLISECONDS.sleep(20);
                    }
                    assertTrue(node.snapshotStats().rounds() >= initial + 3, node.snapshotStats().toString());
                    Assertions.assertEquals(2, workers.submit(() -> cluster.write(node, 2)).get(5, TimeUnit.SECONDS).commitSequence());
                    Assertions.assertEquals(0, node.healthStats().isolations());
                    assertFalse(snapshot.isDone());
                    assertTrue(node.retireOldState(RatisRetirementOptions.defaults()).pending());
                }
            } finally {
                cluster.gate.release.countDown();
            }
            snapshot.get(30, TimeUnit.SECONDS);
            cluster.verify(node, 2);
        }
    }

    /**
     * Mirror budget independent of normal transaction timeout.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000);
    }

    /**
     * Pauses to function only for a mirror output at the specified node of this test.
     */
    private static final class Gate {

        volatile String owner;

        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);

        /**
         * Verify More key direct committee check to use a single bar in query.
         *
         * @param node node
         * @throws IOException if the operation cannot complete
         */
        void hit(final String node) throws IOException {
            if (!node.equals(owner)) {
                return;
            }
            owner = null;
            entered.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) {
                    throw new IOException("image gate observation expired");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        }
    }

    /**
     * The three real nodes share control services only, and each database, WAL and identity space is independent.
     */
    private static final class Cluster implements AutoCloseable {

        final Gate gate = new Gate();

        volatile String publicationFailureOwner;

        final java.util.concurrent.atomic.AtomicBoolean failPublication = new java.util.concurrent.atomic.AtomicBoolean();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final HikariDataSource control;

        /**
         * Use only this test for the new control library and listening port.
         *
         * @param root root
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root) throws Exception {
            String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
            MysqlGenerationSupport.createControl(DATABASE, schema);
            control = MysqlGenerationSupport.control(DATABASE, schema);
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
            var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
            var base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            WalReplicaBackendFactory factory = new WalReplicaBackendFactory() {

                /**
                 * The delay only works for the real export view, and generation and backends are still created by the official plant.
                 *
                 * @param identity identity
                 * @param generation generation
                 * @return the operation result
                 * @throws IOException if the operation cannot complete
                 */
                @Override
                public Lease open(final WalReplicaIdentity identity, final UUID generation) throws IOException {
                    var lease = base.open(identity, generation);
                    var stores = new HashMap<String, WalMaterializedStore>();
                    lease.stores().forEach((shard, store) -> stores.put(shard, new ImageStore(store, identity.nodeId(), gate)));
                    return new Lease(stores, lease::close);
                }

                /**
                 * The retirement capabilities required for the automatic life cycle are derived directly from formal plants.
                 *
                 * @return the operation result
                 */
                @Override
                public boolean supportsRetirement() {
                    return base.supportsRetirement();
                }

                /**
                 * Do not change the actual old generation title check and limited collection contract.
                 *
                 * @param permit permit
                 * @param cursor cursor
                 * @param shardLimit shard limit
                 * @return the operation result
                 * @throws IOException if the operation cannot complete
                 */
                @Override
                public RetirementProgress retire(final WalGenerationRetirement permit, final String cursor, final int shardLimit) throws IOException {
                    return base.retire(permit, cursor, shardLimit);
                }
            };
            try {
                for (int i = 0; i < 3; i++) {
                    String owner = "n" + i;
                    nodes.add(RatisWalNode.openGenerationalForTest(root.resolve(owner), peers.get(i).getId(), group, Set.of("a", "b"), factory, WalOptions.defaults().withTimeout(5000), limits(),
                            stage -> {
                                if (stage == RatisWalState.Stage.AFTER_IMAGE_PUBLISHED && owner.equals(publicationFailureOwner) && failPublication.compareAndSet(true, false)) {
                                    throw new IOException("injected error after durable image publication");
                                }
                            }));
                }
            } catch (final Exception failure) {
                close();
                throw failure;
            }
        }

        /**
         * The role and real majority barrier are satisfied to use nodes.
         *
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    var node = nodes.get(i);
                    if (node == null || node.leaderEpoch() < 1) {
                        continue;
                    }
                    try {
                        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                        node.rollback(tx);
                        return i;
                    } catch (final WalMvccException roleChanged) {
                    }
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            throw new AssertionError("leader missing");
        }

        /**
         * Each pen is written in two logic shard atomic.
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
         * The full double value is also read in an independent RR statement during the snapshot output.
         *
         * @param node node
         * @param value value to write
         */
        void verify(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            var view = node.beginReadStatement(tx);
            for (String shard : List.of("a", "b")) {
                Assertions.assertEquals(shard + value, new String(node.get(tx, shard, "k", view).orElseThrow(), StandardCharsets.UTF_8));
            }
            node.rollback(tx);
        }

        /**
         * Unusuals also defused the outlet, closing all their own nodes and control pools.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            gate.release.countDown();
            Exception failure = null;
            for (var node : nodes) {
                if (node != null) {
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
            }
            control.close();
            if (failure != null) {
                throw failure;
            }
        }
    }

    /**
     * Suspended only at mirror background export, transaction, checkpoint and recovery are still executed by real MySQL.
     *
     * @param delegate delegate
     * @param owner owner
     * @param gate gate
     */
    private record ImageStore(WalMaterializedStore delegate, String owner, Gate gate) implements WalMaterializedStore {

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
            var view = delegate.openSnapshot(boundary);
            return new SnapshotView() {

                /**
                 * Delays streaming output on the actual fixed reading view.
                 *
                 * @param sink sink
                 * @throws IOException if the operation cannot complete
                 */
                @Override
                public void export(final BackupSink sink) throws IOException {
                    gate.hit(owner);
                    view.export(sink);
                }

                /**
                 * Only closes the actual database transaction view.
                 *
                 * @throws IOException if the operation cannot complete
                 */
                @Override
                public void close() throws IOException {
                    view.close();
                }
            };
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

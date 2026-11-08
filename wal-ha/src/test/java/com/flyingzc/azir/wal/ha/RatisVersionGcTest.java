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
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReadSnapshot;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalTransaction;
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
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.ErrorCode;

import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Replica for real MySQL independent safe point, long read protection, master, policy restart and collection failure verify.
 */
class RatisVersionGcTest {

    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();

    static final RatisGcOptions POLICY = new RatisGcOptions(100, 2, 2, 100, 5000);

    @TempDir
    Path directory;

    /**
     * Six stand-alone logical backends were distributed in individual restricted cases to test version syntax instead of host disaster.
     */
    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
    }

    /**
     * Only database containers held in this test are released.
     */
    @AfterAll
    static void stopDatabase() {
        DATABASE.stop();
    }

    /**
     * Leader leader RR maintains the old anchor, following the followers independently collection; actual SQL lines shrink after release and collection tombstone.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void longReaderPinsLeaderWhileFollowersReclaimAndConflictsRemainCorrect() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var first = cluster.write(node, 1);
            var reader = node.begin(IsolationLevel.REPEATABLE_READ);
            var view = node.beginReadStatement(reader);
            var staleWriter = node.begin(IsolationLevel.REPEATABLE_READ);
            cluster.enable();
            for (int value = 2; value <= 8; value++) {
                cluster.write(node, value);
            }
            cluster.materialized(8);
            await(() -> node.gcStats().rounds() >= 2);
            assertTrue(node.gcStats().safePoint() <= reader.startSequence());
            cluster.verify(node, reader, view, 1);
            for (int index = 0; index < 3; index++) {
                if (index != leader) {
                    final int follower = index;
                    await(() -> cluster.rows(follower) == 2 && cluster.nodes.get(follower).gcStats().safePoint() == 8);
                }
            }
            Assertions.assertEquals(16, cluster.rows(leader));
            node.put(staleWriter, "a", "k", bytes("stale"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> node.commit(staleWriter)).errorCode());
            cluster.verify(node, reader, view, 1);
            node.rollback(reader);
            await(() -> cluster.rows(leader) == 2 && node.gcStats().safePoint() == 8);
            cluster.verify(node, 8);
            var deleted = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) {
                node.delete(deleted, shard, "k");
            }
            node.commit(deleted);
            cluster.materialized(9);
            await(() -> cluster.rows(leader) == 0);
            var empty = node.begin(IsolationLevel.REPEATABLE_READ);
            var emptyView = node.beginReadStatement(empty);
            for (String shard : List.of("a", "b")) {
                assertTrue(node.get(empty, shard, "k", emptyView).isEmpty());
            }
            node.rollback(empty);
            Assertions.assertEquals(10, cluster.write(node, 10).commitSequence());
            cluster.verify(node, 10);
            Assertions.assertEquals(first, node.lookupCommit(first.transactionId()).receipt());
        }
    }

    /**
     * Follower is collection and can only create a new snapshot; original node restarts an automatic recovery GC policy.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failoverAndRestartDoNotResurrectOldSnapshotHandles() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int first = cluster.leader();
            var old = cluster.nodes.get(first);
            var receipt = cluster.write(old, 1);
            var reader = old.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = old.beginReadStatement(reader);
            cluster.enable();
            for (int value = 2; value <= 6; value++) {
                cluster.write(old, value);
            }
            cluster.materialized(6);
            for (int index = 0; index < 3; index++) {
                if (index != first) {
                    final int follower = index;
                    await(() -> cluster.rows(follower) == 2);
                }
            }
            cluster.stop(first);
            int second = cluster.leader();
            var next = cluster.nodes.get(second);
            cluster.verify(next, 6);
            assertThrows(WalMvccException.class, () -> next.get(reader, "a", "k", snapshot));
            var newer = cluster.write(next, 7);
            cluster.start(first);
            assertTrue(cluster.nodes.get(first).gcStats().enabled());
            await(() -> cluster.nodes.get(first).ready() && cluster.nodes.get(first).gcStats().publishedSequence() >= 7);
            Assertions.assertEquals(receipt, next.lookupCommit(receipt.transactionId()).receipt());
            Assertions.assertEquals(newer, next.lookupCommit(newer.transactionId()).receipt());
            cluster.verify(next, 7);
        }
    }

    /**
     * Collection errors are detectable and dissensitized, and health data cannot be lost; failure continues to be real bounded collection after it is deactivated.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void maintenanceFailuresAreVisibleAndRetryWithoutPublishingSecrets() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            cluster.write(node, 1);
            cluster.write(node, 2);
            cluster.materialized(2);
            cluster.gcFailure.set(true);
            cluster.enable();
            await(() -> node.gcStats().failures() > 0);
            Assertions.assertEquals("IllegalStateException", node.gcStats().error());
            assertFalse(node.gcStats().toString().contains("private-password"));
            cluster.verify(node, 2);
            Assertions.assertEquals(4, cluster.rows(leader));
            cluster.gcFailure.set(false);
            await(() -> cluster.rows(leader) == 2 && node.gcStats().error() == null && !node.gcStats().stale());
            cluster.verify(node, 2);
        }
    }

    /**
     * Pure memory replica is also actual collection old version and tombstone and cannot be removed from its anchor until RR is released.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void localVersionChainsAreBoundedAfterReaderReleaseAndDelete() throws Exception {
        try (var cluster = new Cluster(directory, true)) {
            int leader = cluster.leader();
            var node = cluster.nodes.get(leader);
            var receipt = cluster.write(node, 1);
            var reader = node.begin(IsolationLevel.REPEATABLE_READ);
            var view = node.beginReadStatement(reader);
            cluster.enable();
            for (int value = 2; value <= 12; value++) {
                cluster.write(node, value);
            }
            for (int index = 0; index < 3; index++) {
                if (index != leader) {
                    final int follower = index;
                    await(() -> cluster.nodes.get(follower).gcStats().retiredMemoryVersions() >= 22);
                }
            }
            cluster.verify(node, reader, view, 1);
            Assertions.assertEquals(0, node.gcStats().retiredMemoryVersions());
            long before = node.localStats().retainedBytes();
            node.rollback(reader);
            await(() -> node.gcStats().retiredMemoryVersions() >= 22 && node.localStats().retainedBytes() < before);
            cluster.verify(node, 12);
            var deleted = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) {
                node.delete(deleted, shard, "k");
            }
            node.commit(deleted);
            await(() -> node.localStats().retainedBytes() == 0);
            Assertions.assertEquals(14, cluster.write(node, 14).commitSequence());
            cluster.verify(node, 14);
            Assertions.assertEquals(receipt, node.lookupCommit(receipt.transactionId()).receipt());
        }
    }

    /**
     * Collection Configure the precise binding of identity and budget, an external or damaged file cannot be converted to a default collection command.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void persistedPolicyRejectsWrongOwnerChangedBudgetsAndCorruption() throws Exception {
        UUID group = UUID.randomUUID();
        var identity = new WalReplicaIdentity(group, group, "x");
        var policy = new RatisGcPolicy(identity, POLICY);
        policy.publish(directory);
        Assertions.assertEquals(policy, RatisGcPolicy.load(directory, identity));
        assertThrows(java.io.IOException.class, () -> RatisGcPolicy.load(directory, new WalReplicaIdentity(group, group, "y")));
        assertThrows(java.io.IOException.class, () -> new RatisGcPolicy(identity, RatisGcOptions.defaults()).publish(directory));
        Path file = directory.resolve("GC_POLICY");
        var bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 1;
        Files.write(file, bytes);
        assertThrows(java.io.IOException.class, () -> RatisGcPolicy.load(directory, identity));
    }

    /**
     * Limited waiting for actual database rows and run-time sampling does not consider timeout successful.
     *
     * @param condition condition
     * @throws Exception if the operation cannot complete
     */
    private static void await(final java.util.function.BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean() && System.nanoTime() < end) {
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertTrue(condition.getAsBoolean());
    }

    /**
     * The two pieces are different payload and cannot replace the correct values with equals.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * There is a real node with database generation, GC failure only injects the adaptor collection border.
     */
    private static final class Cluster implements AutoCloseable {

        final Path root;

        final HikariDataSource control;

        final RaftGroup group;

        final List<RaftPeer> peers = new ArrayList<>();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final AtomicBoolean gcFailure = new AtomicBoolean();

        final WalReplicaBackendFactory factory;

        final MysqlWalReplicaBackendFactory base;

        /**
         * Pack collection failure on real generation, the rest of the method is still done directly by calling a formal MySQL.
         *
         * @param root root
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root) throws Exception {
            this(root, false);
        }

        /**
         * Simple local version chain shares the same real Raft cluster assembly as real MySQL.
         *
         * @param root root
         * @param local local
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root, final boolean local) throws Exception {
            this.root = root;
            String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
            MysqlGenerationSupport.createControl(DATABASE, schema);
            control = MysqlGenerationSupport.control(DATABASE, schema);
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
            base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            factory = local ? WalReplicaBackendFactory.localOnly() : (identity, generation) -> {
                var lease = base.open(identity, generation);
                var stores = new HashMap<String, WalMaterializedStore>();
                lease.stores().forEach((shard, store) -> stores.put(shard, faultStore(store)));
                return new WalReplicaBackendFactory.Lease(stores, lease::close);
            };
            try {
                for (int i = 0; i < 3; i++) {
                    start(i);
                }
            } catch (final Exception failure) {
                close();
                throw failure;
            }
        }

        /**
         * Change the GC boundary only to retain actual SQL, ownership, backup and durable storage behaviour.
         *
         * @param delegate delegate
         * @return the operation result
         */
        private WalMaterializedStore faultStore(final WalMaterializedStore delegate) {
            return (WalMaterializedStore) java.lang.reflect.Proxy.newProxyInstance(WalMaterializedStore.class.getClassLoader(), new Class<?>[]{WalMaterializedStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("gcBounded") && gcFailure.get()) {
                    throw new IllegalStateException("private-password jdbc:unpublished");
                }
                try {
                    return method.invoke(delegate, args);
                } catch (final java.lang.reflect.InvocationTargetException actual) {
                    throw actual.getCause();
                }
            });
        }

        /**
         * Reopen to use real original identity and directory, the policy must be from disk recovery.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void start(final int index) throws Exception {
            nodes.set(index, RatisWalNode.openGenerationalLoopback(root.resolve("n" + index), peers.get(index).getId(), group, Set.of("a", "b"), factory,
                    WalOptions.defaults().withTimeout(5000).withHotCacheBytes(0), WalBackupOptions.defaults()));
        }

        /**
         * The current phase is a nodes-by-nodes configuration, and the same policy can be assembled idempotent.
         *
         * @throws Exception if the operation cannot complete
         */
        void enable() throws Exception {
            for (var node : nodes) {
                node.enableVersionGc(POLICY);
                node.enableVersionGc(POLICY);
            }
        }

        /**
         * The real fresh majority, read barrier, was created to see the leadership as available.
         *
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                for (int i = 0; i < 3; i++) {
                    var node = nodes.get(i);
                    if (node == null || !node.ready() || node.leaderEpoch() <= 0) {
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
            throw new AssertionError("GC cluster leader missing");
        }

        /**
         * The real backend should be formed and more than collection version for each change of two pieces.
         *
         * @param node node
         * @param value value to write
         * @return the operation result
         */
        WalReceipt write(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) {
                node.put(tx, shard, "k", bytes(shard + value));
            }
            return node.commit(tx);
        }

        /**
         * Fixed transaction and statement border check for two complete payload.
         *
         * @param node node
         * @param tx transaction context
         * @param view view
         * @param value value to write
         */
        void verify(final RatisWalNode node, final WalTransaction tx, final WalReadSnapshot view, final int value) {
            for (String shard : List.of("a", "b")) {
                assertArrayEquals(bytes(shard + value), node.get(tx, shard, "k", view).orElseThrow());
            }
        }

        /**
         * New transaction for the full visibility of current verify leaders.
         *
         * @param node node
         * @param value value to write
         */
        void verify(final RatisWalNode node, final int value) {
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            verify(node, tx, node.beginReadStatement(tx), value);
            node.rollback(tx);
        }

        /**
         * The SQL count only accesss the current identity and ACTIVE generation derived database.
         *
         * @param index index
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         */
        long rows(final int index) {
            try {
                var active = RatisGenerationRecord.read(root.resolve("n" + index + "/business/ACTIVE"));
                long count = 0;
                try (var connection = control.getConnection(); var query = connection.createStatement()) {
                    for (String shard : List.of("a", "b")) {
                        String schema = base.databaseName(active.identity(), active.generation(), shard);
                        try (var result = query.executeQuery("select count(*) from `" + schema + "`.azir_wal_version")) {
                            result.next();
                            count += result.getLong(1);
                        }
                    }
                }
                return count;
            } catch (final Exception failure) {
                throw new AssertionError(failure);
            }
        }

        /**
         * GC asserts that it waits for a real step materialization to reach the confirmed border.
         *
         * @param sequence sequence
         * @throws Exception if the operation cannot complete
         */
        void materialized(final long sequence) throws Exception {
            for (var node : nodes) {
                await(() -> node.localStats().materializedSequences().values().stream().allMatch(value -> value >= sequence));
            }
        }

        /**
         * Only the internal nodes of the process currently held by this test are closed.
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
         * Unusuals also release all their own nodes and control pools.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (int i = 0; i < 3; i++) {
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
            control.close();
            if (failure != null) {
                throw failure;
            }
        }
    }
}

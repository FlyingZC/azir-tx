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
import org.apache.ratis.protocol.RaftPeerId;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.client.RaftClient;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Three JVM/MySQL installation probe, stdin is only used for parental process test scheduling and not product access protocols.
 */
public final class RatisMysqlProcessProbe {

    /**
     * Prevent instantiation of this utility class.
     */
    private RatisMysqlProcessProbe() {
    }

    /**
     * Starts a real replica, receipt only read from your own test environment and does not write to the command line or log.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String id = args[1];
        var groupId = RaftGroupId.valueOf(UUID.fromString(args[2]));
        var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < 3; i++) {
            peers.add(RaftPeer.newBuilder().setId("p" + i).setAddress("127.0.0.1:" + args[3 + i]).build());
        }
        String release = System.getenv("AZIR_WAL_PROBE_RELEASE_FILE");
        if (release != null && !Path.of(release).toAbsolutePath().normalize().equals(root.toAbsolutePath().normalize().resolve(".auto-release"))) {
            throw new IllegalArgumentException("release marker must belong to this owned probe root");
        }
        var group = RaftGroup.valueOf(groupId, peers);
        var fault = new Fault(System.getenv("AZIR_WAL_PROBE_FAULT"));
        result("BOOT " + ProcessHandle.current().pid());
        try (var control = control()) {
            var base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            WalReplicaBackendFactory factory = new WalReplicaBackendFactory() {

                /**
                 * Backup and recovery are still fully commissioning the actual MySQL, with a pause point only for the parent 's failure schedule.
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
                    lease.stores().forEach((shard, store) -> stores.put(shard, new RestoreBoundaryStore(store, shard.equals("a") ? fault : new Fault(null))));
                    return new Lease(stores, lease::close);
                }

                /**
                 * The bounded retirement capability of a formal plant is used during the automated life cycle.
                 *
                 * @return the operation result
                 */
                @Override
                public boolean supportsRetirement() {
                    return base.supportsRetirement();
                }

                /**
                 * The actual licence of ownership is not bypassed by the test packaging.
                 *
                 * @param permit permit
                 * @param cursor cursor
                 * @param limit maximum number of items
                 * @return the operation result
                 * @throws IOException if the operation cannot complete
                 */
                @Override
                public RetirementProgress retire(final WalGenerationRetirement permit, final String cursor, final int limit) throws IOException {
                    return base.retire(permit, cursor, limit);
                }
            };
            try (
                    var node = RatisWalNode.openGenerationalForTest(root, RaftPeerId.valueOf(id), group, Set.of("a", "b"), factory,
                            WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0), new WalBackupOptions(128_000_000, 2, 4_000_000, 120_000), new RatisWalState.Hook() {

                                /**
                                 * Install and mirror the real border to share the same one-time failure schedule.
                                 *
                                 * @param stage stage
                                 */
                                @Override
                                public void hit(final RatisWalState.Stage stage) {
                                    fault.hit(stage.name());
                                }

                                /**
                                 * Proof of record before the suspension is sent before the father process can kill at the actual border.
                                 *
                                 * @param stage stage
                                 * @param first first
                                 * @param covered covered
                                 */
                                @Override
                                public void hit(final RatisWalState.Stage stage, final long first, final long covered) {
                                    fault.proof(stage.name(), first, covered);
                                    fault.hit(stage.name());
                                }
                            });
                    var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                result("STARTED " + ProcessHandle.current().pid());
                for (String command; (command = input.readLine()) != null;) {
                    String[] fields = command.split(" ", 2);
                    try {
                        switch (fields[0]) {
                            case "ARM" -> {
                                fault.arm(fields[1]);
                                result("ARMED " + fields[1]);
                            }
                            case "LIFECYCLE" -> {
                                node.enableVersionGc(new RatisGcOptions(200, 64, 32, 100, 15_000));
                                node.enableSnapshotMaintenance(new RatisSnapshotOptions(100, 1, 1000, 64L * 1024 * 1024, 0));
                                result("LIFECYCLE true true");
                            }
                            case "LOG" -> result("LOG " + node.firstLogIndexForTest());
                            case "POLICY" -> result("POLICY " + node.gcStats().enabled() + " " + node.snapshotStats().enabled());
                            case "STATUS" -> result("STATUS " + node.leaderEpoch() + " " + (node.ready() ? node.appliedSequence() : -1) + " " + node.ready() + " " + node.snapshotIndex());
                            case "PING" -> {
                                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                                long sequence = tx.startSequence();
                                node.rollback(tx);
                                result("PING " + sequence);
                            }
                            case "INIT" -> {
                                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                                result("INTENT " + tx.id());
                                for (String shard : List.of("a", "b")) {
                                    for (int i = 0; i < 32; i++) {
                                        node.put(tx, shard, "cold" + i, bytes(shard + ":" + i));
                                    }
                                    node.put(tx, shard, "blob", new byte[]{0});
                                    node.put(tx, shard, "live", bytes("0"));
                                }
                                committed(node.commit(tx));
                            }
                            case "WRITE" -> {
                                int value = Integer.parseInt(fields[1]);
                                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                                result("INTENT " + tx.id());
                                for (String shard : List.of("a", "b")) {
                                    var payload = new byte[480 * 1024];
                                    Arrays.fill(payload, (byte) (value + (shard.equals("a") ? 0 : 100)));
                                    node.put(tx, shard, "blob", payload);
                                }
                                committed(node.commit(tx));
                            }
                            case "DELETE" -> {
                                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                                result("INTENT " + tx.id());
                                for (String shard : List.of("a", "b")) {
                                    node.delete(tx, shard, "cold0");
                                }
                                committed(node.commit(tx));
                            }
                            case "LIVE" -> {
                                int value = Integer.parseInt(fields[1]);
                                var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                                result("INTENT " + tx.id());
                                node.put(tx, "a", "live", bytes(Integer.toString(value)));
                                node.put(tx, "b", "live", bytes(Integer.toString(-value)));
                                committed(node.commit(tx));
                            }
                            case "COLD" -> {
                                long sequence = Long.parseLong(fields[1]);
                                var stats = node.localStats();
                                if (stats.materializedSequences().values().stream().anyMatch(value -> value < sequence)) {
                                    result("NOT_COLD");
                                } else {
                                    node.checkpointLocal();
                                    stats = node.localStats();
                                    result("COLD " + stats.publishedSequence() + " " + stats.retainedBytes() + " " + stats.walBytes());
                                }
                            }
                            case "SNAPSHOT" -> result("SNAPSHOT " + node.snapshot());
                            case "PURGE" -> {
                                long index = Long.parseLong(fields[1]);
                                node.purgeForTest(index);
                                result("PURGED " + node.firstLogIndexForTest());
                            }
                            case "LOOKUP" -> {
                                var lookup = node.lookupCommit(UUID.fromString(fields[1]));
                                result("LOOKUP " + lookup.status() + (lookup.receipt() == null ? "" : " " + lookup.receipt().commitSequence() + " " + lookup.receipt().digest()));
                            }
                            case "READ" -> result(read(node));
                            case "TRANSFER" -> {
                                try (var client = RaftClient.newBuilder().setRaftGroup(group).setLeaderId(RaftPeerId.valueOf(id)).setProperties(new org.apache.ratis.conf.RaftProperties()).build()) {
                                    result("TRANSFER " + client.admin().transferLeadership(RaftPeerId.valueOf(fields[1]), 10_000).isSuccess());
                                }
                            }
                            case "STOP" -> {
                                result("STOPPING");
                                return;
                            }
                            default -> throw new IllegalArgumentException("unknown owned probe command");
                        }
                    } catch (final Exception failure) {
                        result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : ""));
                    }
                }
            }
        }
    }

    /**
     * Only the example of a return loop created by a parent connection process, the control library name must come from a fixed test format.
     *
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static HikariDataSource control() {
        String url = System.getenv("AZIR_WAL_PROBE_JDBC_URL"), schema = System.getenv("AZIR_WAL_PROBE_CONTROL_SCHEMA");
        if (url == null || !url.matches("jdbc:mysql://(localhost|127\\.0\\.0\\.1):[0-9]+/ha_control(?:\\?.*)?") || schema == null || !schema.matches("c_[a-f0-9]{32}")) {
            throw new IllegalArgumentException("probe requires owned loopback MySQL namespace");
        }
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setCatalog(schema);
        config.setUsername("root");
        config.setPassword(System.getenv("AZIR_WAL_PROBE_PASSWORD"));
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /**
     * The same RR statement snapshot goes through all key and tombstone and calculates the full recalculation summary by length code.
     *
     * @param node node
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String read(final RatisWalNode node) throws Exception {
        var hash = java.security.MessageDigest.getInstance("SHA-256");
        var output = new DataOutputStream(new java.security.DigestOutputStream(OutputStream.nullOutputStream(), hash));
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var snapshot = node.beginReadStatement(tx);
        for (String shard : List.of("a", "b")) {
            for (int i = 0; i < 32; i++) {
                entry(output, shard + "/cold" + i, node.get(tx, shard, "cold" + i, snapshot));
            }
            for (String key : List.of("blob", "live")) {
                entry(output, shard + "/" + key, node.get(tx, shard, key, snapshot));
            }
        }
        node.rollback(tx);
        output.flush();
        return "DATA " + snapshot.sequence() + " " + java.util.HexFormat.of().formatHex(hash.digest());
    }

    /**
     * Empty values are clearly encoded as negative lengths and cannot be confused with empty bytes or conjoined boundaries.
     *
     * @param output output
     * @param key business key
     * @param value value to write
     * @throws IOException if the operation cannot complete
     */
    private static void entry(final DataOutputStream output, final String key, final Optional<byte[]> value) throws IOException {
        byte[] name = bytes(key);
        output.writeInt(name.length);
        output.write(name);
        if (value.isEmpty()) {
            output.writeInt(-1);
        } else {
            output.writeInt(value.get().length);
            output.write(value.get());
        }
    }

    /**
     * Fixed UTF-8 business code.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Only actual commit receipt returns send COMMITTED, INTENT does not mean success.
     *
     * @param receipt receipt
     */
    private static void committed(final WalReceipt receipt) {
        result("COMMITTED " + receipt.transactionId() + " " + receipt.commitSequence() + " " + receipt.digest());
    }

    /**
     * Incidents may come from the installation and dispatch lines, with complete output per line and immediate updating.
     *
     * @param value value to write
     */
    private static synchronized void result(final String value) {
        System.out.println("AZIR " + value);
        System.out.flush();
    }

    /**
     * The parent process observes the actual boundary after killing, probe does not initiate halt or uses timeout to impersonate as a collapse.
     */
    private static final class Fault {

        volatile String wanted;

        final AtomicBoolean armed = new AtomicBoolean(true);

        /**
         * Process not configured does not inject failure.
         *
         * @param wanted wanted
         */
        Fault(final String wanted) {
            this.wanted = wanted;
        }

        /**
         * Only the current test stdin selects the actual failure boundary once after normal startup.
         *
         * @param stage stage
         */
        void arm(final String stage) {
            wanted = stage;
            armed.set(true);
        }

        /**
         * Collection Boundaries also need to maintain the actual primary index of the original birthday and full mirror coverage of the upper boundary.
         *
         * @param stage stage
         * @param first first
         * @param covered covered
         */
        void proof(final String stage, final long first, final long covered) {
            if (stage.equals(wanted) && armed.get()) {
                result("PROOF " + stage + " " + first + " " + covered);
            }
        }

        /**
         * The actual boundary of a single strike is frozen, and whether or not to kill again during the recovery period is decided separately by the father process.
         *
         * @param stage stage
         * @throws IllegalArgumentException if the operation cannot complete
         * @throws IllegalStateException if the operation cannot complete
         */
        void hit(final String stage) {
            if (wanted == null || !wanted.equals(stage) || !armed.compareAndSet(true, false)) {
                return;
            }
            result("BOUNDARY " + stage + " " + ProcessHandle.current().pid());
            try {
                String configured = System.getenv("AZIR_WAL_PROBE_RELEASE_FILE");
                if (configured == null) {
                    new CountDownLatch(1).await();
                } else {
                    // Only test recovery checkpoint allows the parent process to continue through its own temporary file without this entry for product code.
                    Path release = Path.of(configured);
                    if (!release.getFileName().toString().equals(".auto-release")) {
                        throw new IllegalArgumentException("invalid owned release marker");
                    }
                    while (!java.nio.file.Files.isRegularFile(release, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                        Thread.sleep(10);
                    }
                    if (!java.nio.file.Files.readString(release).equals("resume")) {
                        throw new IllegalArgumentException("invalid owned release command");
                    }
                    java.nio.file.Files.delete(release);
                }
            } catch (final InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("owned fault probe interrupted", failure);
            } catch (final IOException failure) {
                throw new IllegalStateException("owned fault release failed", failure);
            }
        }
    }

    /**
     * The other operation is fully entrusted to the official adaptor only after the real MySQL conditioneBatch has been frozen after commit.
     *
     * @param delegate delegate
     * @param fault fault
     */
    private record RestoreBoundaryStore(WalMaterializedStore delegate, Fault fault) implements WalMaterializedStore {

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
            fault.hit("MID_RESTORE_BATCH");
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

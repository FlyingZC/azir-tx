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
import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalTransaction;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import com.flyingzc.azir.common.IsolationLevel;

import java.nio.charset.StandardCharsets;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Automaintenance, real backend and authentication network failure probe, with the result being received by a parent process before entering the confirmation prefix.
 */
public final class RatisCombinationProcessProbe {

    /**
     * Probe does not allow for exemplification.
     */
    private RatisCombinationProcessProbe() {
    }

    /**
     * A container with a node, group, strategy and its own receipt are clearly provided by the parent test.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String id = args[1];
        UUID groupId = UUID.fromString(args[2]);
        var peers = new ArrayList<RaftPeer>();
        var trusted = new ArrayList<RatisTlsSecurity.PeerTrust>();
        var policy = new Properties();
        try (var input = Files.newInputStream(Path.of(args[3]))) {
            policy.load(input);
        }
        for (int i = 0; i < 3; i++) {
            String peer = "p" + i, address = policy.getProperty(peer + ".address");
            peers.add(RaftPeer.newBuilder().setId(peer).setAddress(address).build());
            trusted.add(new RatisTlsSecurity.PeerTrust(peer, address, Set.of(policy.getProperty(peer + ".pin"))));
        }
        result("BOOT " + ProcessHandle.current().pid());
        var security = RatisTlsSecurity.load(groupId, id, Path.of(args[4]), RatisTlsTestCertificates.PASSWORD, Path.of(args[5]), RatisTlsTestCertificates.PASSWORD, trusted);
        var fault = new Fault();
        try (
                var control = control(policy, id);
                var node = RatisWalNode.openAuthenticatedForTest(root, RaftPeerId.valueOf(id), RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers), Set.of("a", "b"),
                        new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults()),
                        WalOptions.defaults().withTimeout(5000).withHotCacheBytes(0), new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000), security, stage -> fault.hit(stage.name()));
                var worker = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().name("owned-combination-commit").factory());
                var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            result("STARTED " + ProcessHandle.current().pid());
            WalTransaction buffered = null;
            CompletableFuture<WalReceipt> submitted = null;
            UUID submittedId = null;
            for (String command; (command = input.readLine()) != null;) {
                String[] fields = command.split(" ", 2);
                try {
                    switch (fields[0]) {
                        case "ARM" -> {
                            fault.arm(fields[1]);
                            result("ARMED " + fields[1]);
                        }
                        case "RELEASE" -> {
                            fault.release();
                            result("RELEASED");
                        }
                        case "LIFECYCLE" -> {
                            node.enableHealthManagement(new RatisHealthOptions(200, 2, 2, 750, 6000));
                            node.enableVersionGc(new RatisGcOptions(200, 64, 32, 100, 15_000));
                            node.enableSnapshotMaintenance(new RatisSnapshotOptions(100, 8, 1000, 64L * 1024 * 1024, 0));
                            result("LIFECYCLE true true true");
                        }
                        case "MAINT" -> result("MAINT " + node.gcStats().rounds() + " " + node.snapshotStats().snapshots() + " " + node.snapshotStats().firstLogIndex() + " "
                                + node.healthStats().quarantined() + " " + node.healthStats().campaignBlocked() + " " + node.gcStats().removedRows());
                        case "COLD" -> {
                            long needed = Long.parseLong(fields[1]);
                            var stats = node.localStats();
                            if (!node.ready() || stats.materializedSequences().values().stream().anyMatch(value -> value < needed)) {
                                result("NOT_COLD");
                            } else {
                                node.checkpointLocal();
                                result("COLD " + node.appliedSequence());
                            }
                        }
                        case "FINISH_ASYNC" -> {
                            if (buffered == null || submitted != null) {
                                throw new IllegalStateException("missing buffer or commit already submitted");
                            }
                            var tx = buffered;
                            buffered = null;
                            submittedId = tx.id();
                            submitted = CompletableFuture.supplyAsync(() -> node.commit(tx), worker);
                            result("SUBMITTED " + submittedId);
                        }
                        case "RESULT" -> {
                            if (submitted == null) {
                                throw new IllegalStateException("no submitted commit");
                            }
                            if (!submitted.isDone()) {
                                result("PENDING " + submittedId);
                            } else {
                                try {
                                    committed(submitted.join());
                                } catch (final CompletionException failed) {
                                    Throwable cause = failed.getCause();
                                    result("ERROR " + cause.getClass().getSimpleName() + (cause instanceof WalMvccException wal ? " " + wal.errorCode() : "") + " " + submittedId);
                                }
                            }
                        }
                        case "STATUS" -> result("STATUS " + node.leaderEpoch() + " " + (node.ready() ? node.appliedSequence() : -1) + " " + node.appliedRaftIndex() + " " + node.snapshotIndex() + " "
                                + node.durableSnapshotAckIndex() + " " + node.ready());
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
                        case "BUFFER" -> {
                            if (buffered != null) {
                                throw new IllegalStateException("buffer already exists");
                            }
                            buffered = node.begin(IsolationLevel.REPEATABLE_READ);
                            node.put(buffered, "a", "live", bytes("999"));
                            node.put(buffered, "b", "live", bytes("-999"));
                            result("BUFFERED " + buffered.id());
                        }
                        case "FINISH" -> {
                            if (buffered == null) {
                                throw new IllegalStateException("buffer missing");
                            }
                            var tx = buffered;
                            buffered = null;
                            committed(node.commit(tx));
                        }
                        case "SNAPSHOT" -> result("SNAPSHOT " + node.snapshot());
                        case "PURGE" -> {
                            node.purgeForTest(Long.parseLong(fields[1]));
                            result("PURGED " + node.firstLogIndexForTest());
                        }
                        case "LOOKUP" -> {
                            var value = node.lookupCommit(UUID.fromString(fields[1]));
                            result("LOOKUP " + value.status() + (value.receipt() == null ? "" : " " + value.receipt().commitSequence() + " " + value.receipt().digest()));
                        }
                        case "READ" -> result(read(node));
                        case "TRANSFER" -> {
                            node.transferLeadership(RaftPeerId.valueOf(fields[1]), 30_000);
                            result("TRANSFERRED");
                        }
                        case "STOP" -> {
                            fault.release();
                            result("STOPPING");
                            return;
                        }
                        default -> throw new IllegalArgumentException("unknown probe command");
                    }
                } catch (final Exception failure) {
                    result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : ""));
                }
            }
        }
    }

    /**
     * Data connection uses only the exclusive backend network and control library of the parent process, receipt does not enter the command line or history.
     *
     * @param policy policy
     * @param id id
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static com.zaxxer.hikari.HikariDataSource control(final Properties policy, final String id) {
        String address = policy.getProperty(id + ".backend"), schema = System.getenv("AZIR_WAL_COMBINATION_SCHEMA");
        if (address == null || !address.matches("[0-9.]+:3306") || schema == null || !schema.matches("c_[a-f0-9]{32}")) {
            throw new IllegalArgumentException("owned combination backend required");
        }
        var config = new com.zaxxer.hikari.HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + address + "/ha_control?connectTimeout=2000&socketTimeout=2000&rewriteBatchedStatements=true");
        config.setCatalog(schema);
        config.setUsername("root");
        config.setPassword(System.getenv("AZIR_WAL_COMBINATION_PASSWORD"));
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new com.zaxxer.hikari.HikariDataSource(config);
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
     * The control input is still available independently, pending the addition of a true line or after the majority and local WAL applications.
     */
    private static final class Fault {

        volatile String wanted;

        final AtomicBoolean armed = new AtomicBoolean();

        volatile CountDownLatch released = new CountDownLatch(0);

        /**
         * Allows only one parent process to select the failure boundary at a time.
         *
         * @param stage stage
         */
        void arm(final String stage) {
            released = new CountDownLatch(1);
            wanted = stage;
            armed.set(true);
        }

        /**
         * Failure injection takes place only at the actual protocol location, does not hold JVM or is faked.
         *
         * @param stage stage
         * @throws IOException if the operation cannot complete
         */
        void hit(final String stage) throws IOException {
            if (!stage.equals(wanted) || !armed.compareAndSet(true, false)) {
                return;
            }
            result("BOUNDARY " + stage);
            try {
                released.await();
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        }

        /**
         * The parent process continues its original operation when released or normally cleaned, and the actual killing is controlled by Docker.
         */
        void release() {
            released.countDown();
        }
    }
}

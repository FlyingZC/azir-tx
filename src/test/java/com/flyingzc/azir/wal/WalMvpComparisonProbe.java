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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.gc.GcManager;
import com.flyingzc.azir.gc.mysql.MysqlSafePointStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import com.flyingzc.azir.txn.TransactionManager;
import com.flyingzc.azir.txn.TransactionOptions;
import org.testcontainers.containers.MySQLContainer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The isolation capacity probe allows for the addition of an original monogamous library and two-pool baselines, without regular testing to execute or modify old mode codes.
 */
public final class WalMvpComparisonProbe {

    private static final int CLIENTS = Integer.getInteger("azir.wal.clients", 50);

    private static final boolean LOW_RESOURCE = Boolean.getBoolean("azir.wal.lowResource");

    private static final int PAYLOAD_BYTES = Integer.getInteger("azir.wal.payloadBytes", 0);

    private static final Path PHASE_EVENTS = System.getProperty("azir.wal.phaseEvents") == null ? null
            : Path.of(System.getProperty("azir.wal.phaseEvents"));

    /**
     * Prohibits the exemplification of probe entrance.
     */
    private WalMvpComparisonProbe() {
    }

    /**
     * Initiation of the quarantine example, compares in positive and negative order the same double-reading load with a different commit contract.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path output = Path.of(args[0]);
        Files.createDirectories(output);
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        boolean walOnly = args.length > 2 && args[2].equals("wal-only");
        boolean includeNative = args.length > 2 && args[2].equals("all");
        boolean optimized = args.length > 2 && args[2].equals("optimized");
        boolean bounded = args.length > 2 && args[2].equals("bounded");
        if (seconds < 1 || seconds > 300) {
            throw new IllegalArgumentException("invalid duration");
        }
        if (CLIENTS < 1 || CLIENTS > 50 || LOW_RESOURCE && CLIENTS > 3) {
            throw new IllegalArgumentException("invalid client budget");
        }
        if (PAYLOAD_BYTES < 0 || PAYLOAD_BYTES > 4096) {
            throw new IllegalArgumentException("invalid payload size");
        }
        try (var serverA = container(); var serverB = container()) {
            serverA.start();
            serverB.start();
            Files.writeString(output.resolve("owned-containers.txt"), serverA.getContainerId() + "\n" + serverB.getContainerId() + "\n");
            var created = serverA.execInContainer("mysql", "-uroot", "-pwal_probe", "-e",
                    "create database wal_control; grant all on wal_control.* to 'wal_probe'@'%'");
            if (created.getExitCode() != 0) {
                throw new IllegalStateException("create isolated control database failed");
            }
            try (
                    var a = pool(serverA); var b = pool(serverB);
                    var control = pool(serverA, serverA.getJdbcUrl().replace("/wal_comparison", "/wal_control"))) {
                var lines = new ArrayList<String>();
                lines.add("mode\tclients\telapsed_seconds\tcommitted\ttps\tp50_ms\tp99_ms\tgroup_forces\twal_bytes\tmax_lag_at_end\tdrain_seconds\tversions_a\tversions_b");
                if (bounded) {
                    boundedComparison(control, a, b, seconds, output, lines);
                    return;
                }
                for (int round = 1; round <= (walOnly ? 1 : 2); round++) {
                    if (optimized) {
                        if (round == 1) {
                            lines.add(optimizedNativeMode(a, b, seconds, round, true));
                            Files.write(output.resolve("benchmark.tsv"), lines);
                            lines.add(optimizedNativeMode(a, b, seconds, round, false));
                            Files.write(output.resolve("benchmark.tsv"), lines);
                            lines.add(walMode(a, b, seconds, round, output));
                        } else {
                            lines.add(walMode(a, b, seconds, round, output));
                            Files.write(output.resolve("benchmark.tsv"), lines);
                            lines.add(optimizedNativeMode(a, b, seconds, round, false));
                            Files.write(output.resolve("benchmark.tsv"), lines);
                            lines.add(optimizedNativeMode(a, b, seconds, round, true));
                        }
                        Files.write(output.resolve("benchmark.tsv"), lines);
                        continue;
                    }
                    if (includeNative && round == 1) {
                        lines.add(nativeMode(a, b, seconds, round, true));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                        lines.add(nativeMode(a, b, seconds, round, false));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                    }
                    if (includeNative && round == 2) {
                        lines.add(walMode(a, b, seconds, round, output));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                    }
                    if (!walOnly) {
                        lines.add(oldMode(control, a, b, seconds, round));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                    }
                    if (!includeNative || round != 2) {
                        lines.add(walMode(a, b, seconds, round, output));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                    }
                    if (includeNative && round == 2) {
                        lines.add(nativeMode(a, b, seconds, round, false));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                        lines.add(nativeMode(a, b, seconds, round, true));
                        Files.write(output.resolve("benchmark.tsv"), lines);
                    }
                }
                Files.writeString(output.resolve("environment.txt"), "java=" + System.getProperty("java.version")
                        + "\nmysql=" + version(a) + "\nclients=" + CLIENTS + "\nworkload=RR, two point reads, two point updates, no key conflicts"
                        + "\nGC=one bounded round per second in both modes\nreplication=none"
                        + "\nold_control=separate database colocated with shard a\nmysql_durability=innodb_flush_log_at_trx_commit=1,sync_binlog=1"
                        + "\nwal_durability=local FileChannel.force(true), no replication\n");
                Files.writeString(output.resolve("payload.txt"), "payload_bytes=" + PAYLOAD_BYTES
                        + "\n0 means variable-size UTF-8 integer\n");
                Files.writeString(output.resolve("jdbc-security.txt"), "ssl_cipher=" + tlsCipher(a) + "\n");
                if (optimized) {
                    Files.writeString(output.resolve("optimized-contracts.txt"),
                            "native: each worker holds its own connections and four prepared statements throughout measurement\n"
                                    + "autocommit=false is set once; each business transaction still has its own commit\n"
                                    + "native-single: one local commit; native-two: two sequential local commits, no global RR or atomic commit\n"
                                    + "SQL, values, isolation and transaction boundaries match the two-read/two-update workload\n"
                                    + "statement preparation and connection acquisition occur before the start barrier\n"
                                    + "order: optimized-native-single,optimized-native-two,wal,wal,optimized-native-two,optimized-native-single\n");
                }
                if (includeNative) {
                    Files.writeString(output.resolve("native-contracts.txt"),
                            "native-single: one InnoDB RR transaction, two reads, two updates, one synchronous commit\n"
                                    + "native-two: two independent InnoDB RR transactions, total two reads and two updates, sequential synchronous commits\n"
                                    + "native-two has neither atomic cross-shard commit nor a global RR snapshot\n"
                                    + "native reads and writes use PreparedStatement; pools reset autocommit on return\n"
                                    + "all clients use distinct keys and small UTF-8 integer payloads\n"
                                    + "order: native-single,native-two,old,wal,wal,old,native-two,native-single\n");
                }
                lines.forEach(System.out::println);
            }
        }
    }

    /**
     * Original JDBC contrast: Single libraries have local atomic properties, two libraries only await each commit and no global protocol.
     *
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @param singleDatabase single database
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String nativeMode(final HikariDataSource a, final HikariDataSource b, final int seconds, final int round,
                                     final boolean singleDatabase) throws Exception {
        String table = singleDatabase ? "wal_native_single" : "wal_native_two";
        seedNative(a, table, singleDatabase ? List.of("L", "R") : List.of("L"));
        if (!singleDatabase) {
            seedNative(b, table, List.of("R"));
        }
        Result result = run((singleDatabase ? "native-single-" : "native-two-") + round, seconds, (worker, ordinal) -> {
            try (var left = a.getConnection(); var right = singleDatabase ? null : b.getConnection()) {
                left.setAutoCommit(false);
                if (right != null) {
                    right.setAutoCommit(false);
                }
                var second = right == null ? left : right;
                try {
                    checkEqual(nativeRead(left, table, "L" + worker), nativeRead(second, table, "R" + worker));
                    nativeUpdate(left, table, "L" + worker, ordinal);
                    nativeUpdate(second, table, "R" + worker, ordinal);
                    left.commit();
                    if (right != null) {
                        right.commit();
                    }
                } catch (final java.sql.SQLException | RuntimeException failure) {
                    try {
                        left.rollback();
                    } catch (final java.sql.SQLException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    if (right != null) {
                        try {
                            right.rollback();
                        } catch (final java.sql.SQLException cleanup) {
                            failure.addSuppressed(cleanup);
                        }
                    }
                    throw failure;
                }
            } catch (final java.sql.SQLException failure) {
                throw new IllegalStateException("native transaction failed", failure);
            }
        }, () -> {
        });
        try (var left = a.getConnection(); var right = singleDatabase ? null : b.getConnection()) {
            for (int worker = 0; worker < CLIENTS; worker++) {
                checkEqual(bytes(result.perWorker[worker]), nativeRead(left, table, "L" + worker));
                checkEqual(bytes(result.perWorker[worker]), nativeRead(right == null ? left : right, table, "R" + worker));
            }
        }
        return result.line((singleDatabase ? "native-single-" : "native-two-") + round,
                0, 0, 0, 0, count(a, table), singleDatabase ? 0 : count(b, table));
    }

    /**
     * Reuses the connection and statement of each client to maintain a original optimization baseline for each transaction independent commit.
     *
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @param singleDatabase single database
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String optimizedNativeMode(final HikariDataSource a, final HikariDataSource b, final int seconds, final int round,
                                              final boolean singleDatabase) throws Exception {
        String table = singleDatabase ? "wal_native_single" : "wal_native_two";
        seedNative(a, table, singleDatabase ? List.of("L", "R") : List.of("L"));
        if (!singleDatabase) {
            seedNative(b, table, List.of("R"));
        }
        String label = (singleDatabase ? "optimized-native-single-" : "optimized-native-two-") + round;
        warmConnections(a);
        if (!singleDatabase) {
            warmConnections(b);
        }
        Result result = runWorkers(label, seconds, worker -> new NativeWorker(a, b, table, worker, singleDatabase), () -> {
        });
        try (var left = a.getConnection(); var right = singleDatabase ? null : b.getConnection()) {
            for (int worker = 0; worker < CLIENTS; worker++) {
                checkEqual(bytes(result.perWorker[worker]), nativeRead(left, table, "L" + worker));
                checkEqual(bytes(result.perWorker[worker]), nativeRead(right == null ? left : right, table, "R" + worker));
            }
        }
        return result.line(label, 0, 0, 0, 0, count(a, table), singleDatabase ? 0 : count(b, table));
    }

    /**
     * Preheating connection pool in the order of time before counting, avoiding a five-second consecutive climb limit for concurrent clients holding connection.
     *
     * @param source database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void warmConnections(final HikariDataSource source) throws Exception {
        var held = new ArrayList<java.sql.Connection>();
        try {
            for (int i = 0; i < CLIENTS; i++) {
                held.add(source.getConnection());
            }
        } finally {
            Exception first = null;
            for (var connection : held) {
                try {
                    connection.close();
                } catch (final Exception failure) {
                    if (first == null) {
                        first = failure;
                    } else {
                        first.addSuppressed(failure);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }

    /**
     * Initializes the probe exclusive current value table and resets the current round client key to zero.
     *
     * @param source database connection source
     * @param table table
     * @param prefixes prefixes
     * @throws Exception if the operation cannot complete
     */
    private static void seedNative(final HikariDataSource source, final String table, final List<String> prefixes) throws Exception {
        try (var connection = source.getConnection(); var ddl = connection.createStatement()) {
            ddl.executeUpdate("create table if not exists " + table + " (user_key varbinary(512) primary key, payload longblob not null) engine=InnoDB");
            connection.setAutoCommit(false);
            try (
                    var seed = connection.prepareStatement("insert into " + table + " (user_key,payload) values (?,?) "
                            + "on duplicate key update payload=values(payload)")) {
                for (String prefix : prefixes) {
                    for (int worker = 0; worker < CLIENTS; worker++) {
                        seed.setBytes(1, (prefix + worker).getBytes(StandardCharsets.UTF_8));
                        seed.setBytes(2, bytes(0));
                        seed.addBatch();
                    }
                }
                seed.executeBatch();
                connection.commit();
            }
        }
    }

    /**
     * Reads a primary business value in the connection local transaction view.
     *
     * @param connection connection
     * @param table table
     * @param key business key
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws java.sql.SQLException if the operation cannot complete
     */
    private static byte[] nativeRead(final java.sql.Connection connection, final String table, final String key) throws java.sql.SQLException {
        try (var query = connection.prepareStatement("select payload from " + table + " where user_key=?")) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            try (var row = query.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("native benchmark key missing");
                }
                return row.getBytes(1);
            }
        }
    }

    /**
     * Synchronizes to update a line, and an anomaly in the number of lines leads to a failure of whole-wheel pressure measurement.
     *
     * @param connection connection
     * @param table table
     * @param key business key
     * @param ordinal ordinal
     * @throws IllegalStateException if the operation cannot complete
     * @throws java.sql.SQLException if the operation cannot complete
     */
    private static void nativeUpdate(final java.sql.Connection connection, final String table, final String key, final int ordinal) throws java.sql.SQLException {
        try (var update = connection.prepareStatement("update " + table + " set payload=? where user_key=?")) {
            update.setBytes(1, bytes(ordinal));
            update.setBytes(2, key.getBytes(StandardCharsets.UTF_8));
            if (update.executeUpdate() != 1) {
                throw new IllegalStateException("native update did not affect one row");
            }
        }
    }

    /**
     * Creates a strong durable sex segregation example using the local already existing mirror.
     *
     * @return the operation result
     */
    private static MySQLContainer<?> container() {
        var database = new MySQLContainer<>("mysql:8.4").withDatabaseName("wal_comparison")
                .withUsername("wal_probe").withPassword("wal_probe")
                .withUrlParam("rewriteBatchedStatements", "true").withUrlParam("socketTimeout", "10000")
                .withCommand("--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1");
        if (LOW_RESOURCE) {
            database.withCommand("--innodb-buffer-pool-size=64M", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1",
                    "--performance-schema=OFF", "--max-connections=32");
            database.withCreateContainerCmdModifier(command -> command.getHostConfig()
                    .withMemory(512L * 1024 * 1024).withNanoCPUs(500_000_000L));
        }
        return database;
    }

    /**
     * Prepare foreground for materialization with the same capacity connection pool.
     *
     * @param server server
     * @return the operation result
     */
    private static HikariDataSource pool(final MySQLContainer<?> server) {
        return pool(server, server.getJdbcUrl());
    }

    /**
     * Specifies an independent JDBC address for the same physical example in the isolation control library.
     *
     * @param server server
     * @param url url
     * @return the operation result
     */
    private static HikariDataSource pool(final MySQLContainer<?> server, final String url) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(server.getUsername());
        config.setPassword(server.getPassword());
        config.setMaximumPoolSize(LOW_RESOURCE ? 4 : 96);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "256");
        return new HikariDataSource(config);
    }

    /**
     * Install the old default transaction protocol in the new key space. Only Open interface is called from probe.
     *
     * @param control control
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String oldMode(final HikariDataSource control, final HikariDataSource a, final HikariDataSource b, final int seconds, final int round) throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        metadata.initialize();
        var timestamps = new MysqlTimestampOracle(control);
        timestamps.initialize(100);
        var safePoint = new MysqlSafePointStore(control);
        safePoint.initialize(0);
        var left = new MysqlStorageAdapter("a", a, metadata);
        left.initialize();
        var right = new MysqlStorageAdapter("b", b, metadata);
        right.initialize();
        Map<String, StorageAdapter> adapters = Map.of("a", left, "b", right);
        var gc = new GcManager(metadata, timestamps, safePoint, adapters);
        String prefix = "old-" + round + "-";
        try (var manager = new TransactionManager(timestamps, metadata, safePoint, TransactionOptions.defaults(), adapters)) {
            var seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (int worker = 0; worker < CLIENTS; worker++) {
                manager.put(seed, "a", prefix + worker, bytes(0));
                manager.put(seed, "b", prefix + worker, bytes(0));
            }
            manager.commit(seed);
            Result result = run("old-" + round, seconds, (worker, ordinal) -> {
                var tx = manager.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    byte[] av = manager.get(tx, "a", prefix + worker).orElseThrow();
                    byte[] bv = manager.get(tx, "b", prefix + worker).orElseThrow();
                    checkEqual(av, bv);
                    manager.put(tx, "a", prefix + worker, bytes(ordinal));
                    manager.put(tx, "b", prefix + worker, bytes(ordinal));
                    manager.commit(tx);
                } catch (final RuntimeException ex) {
                    try {
                        manager.rollback(tx);
                    } catch (final RuntimeException ignored) {
                    }
                    throw ex;
                }
            }, () -> gc.gcOnce(128));
            var verify = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (int worker = 0; worker < CLIENTS; worker++) {
                checkEqual(bytes(result.perWorker[worker]), manager.get(verify, "a", prefix + worker).orElseThrow());
                checkEqual(bytes(result.perWorker[worker]), manager.get(verify, "b", prefix + worker).orElseThrow());
            }
            manager.commit(verify);
            return result.line("old-" + round, 0, 0, 0, 0, count(a, "mvcc_data"), count(b, "mvcc_data"));
        }
    }

    /**
     * Runs the new mode using a stand-alone log and an exclusive materialization table to check the dropout results after the time has elapsed.
     *
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @param output output
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String walMode(final HikariDataSource a, final HikariDataSource b, final int seconds, final int round, final Path output) throws Exception {
        return walMode(a, b, seconds, round, output, 8L * 1024 * 1024, false);
    }

    /**
     * Double-reading and line-by-line backend checks are maintained against hotspots/cool return sources and single/diplets.
     *
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @param output output
     * @param cacheBytes cache bytes
     * @param single single
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static String walMode(final HikariDataSource a, final HikariDataSource b, final int seconds, final int round, final Path output,
                                  final long cacheBytes, final boolean single) throws Exception {
        for (var pool : List.of(a, b)) {
            try (var connection = pool.getConnection(); var query = connection.createStatement()) {
                query.executeUpdate("drop table if exists azir_wal_version");
                query.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
        var o = WalOptions.defaults();
        var limits = new WalOptions(o.maxActiveTransactions(), o.maxWriteKeys(), o.maxWriteBytes(), o.queueCapacity(),
                o.commitBatchSize(), o.batchDelayMillis(), 1024L * 1024 * 1024, 512L * 1024 * 1024,
                o.transactionTimeoutMillis(), o.flushBatchSize(), o.flushIntervalMillis()).withHotCacheBytes(cacheBytes);
        var persistedLeft = new MysqlWalMaterializedStore(a);
        var persistedRight = single ? persistedLeft : new MysqlWalMaterializedStore(b);
        String rightShard = single ? "a" : "b";
        String rightPrefix = single ? "r" : "k";
        int slots = cacheBytes == 0 ? 128 : 1;
        String label = round < 10 ? "wal-" + round : "wal-" + (cacheBytes == 0 ? "cold" : "hot") + "-" + (single ? "single" : "two") + "-" + round;
        try (
                var runtime = WalMvccRuntime.open(output.resolve("wal-" + round),
                        single ? Map.of("a", persistedLeft) : Map.of("a", persistedLeft, "b", persistedRight), limits)) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int worker = 0; worker < CLIENTS; worker++) {
                for (int slot = 0; slot < slots; slot++) {
                    String suffix = worker + ":" + slot;
                    runtime.put(seed, "a", "k" + suffix, bytes(0));
                    runtime.put(seed, rightShard, rightPrefix + suffix, bytes(0));
                }
            }
            runtime.commit(seed);
            runtime.flush();
            long progressStart = System.nanoTime();
            Path progressFile = output.resolve("wal-" + round + "-progress.tsv");
            Files.writeString(progressFile, "seconds\tpublished\ta\tb\tretained_bytes\theap_used_bytes\n");
            Result result = run(label, seconds, (worker, ordinal) -> {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    String suffix = worker + ":" + (ordinal - 1) % slots;
                    checkEqual(runtime.get(tx, "a", "k" + suffix).orElseThrow(), runtime.get(tx, rightShard, rightPrefix + suffix).orElseThrow());
                    runtime.put(tx, "a", "k" + suffix, bytes(ordinal));
                    runtime.put(tx, rightShard, rightPrefix + suffix, bytes(ordinal));
                    runtime.commit(tx);
                } catch (final RuntimeException ex) {
                    try {
                        runtime.rollback(tx);
                    } catch (final RuntimeException ignored) {
                    }
                    throw ex;
                }
            }, () -> {
                var progress = runtime.stats();
                try {
                    Files.writeString(progressFile, String.format(Locale.ROOT, "%.3f\t%d\t%d\t%d\t%d\t%d\n",
                            (System.nanoTime() - progressStart) / 1e9, progress.publishedSequence(),
                            progress.materializedSequences().get("a"), progress.materializedSequences().getOrDefault("b", progress.materializedSequences().get("a")),
                            progress.retainedBytes(), java.lang.management.ManagementFactory.getMemoryMXBean()
                                    .getHeapMemoryUsage().getUsed()),
                            java.nio.file.StandardOpenOption.APPEND);
                } catch (final java.io.IOException failure) {
                    throw new IllegalStateException(failure);
                }
                runtime.collectGarbage(128);
            });
            var stats = runtime.stats();
            long lag = stats.materializedSequences().values().stream().mapToLong(sequence -> stats.publishedSequence() - sequence).max().orElse(0);
            long drainStart = System.nanoTime();
            while (runtime.stats().materializedSequences().values().stream().anyMatch(sequence -> sequence < stats.publishedSequence())) {
                if (System.nanoTime() - drainStart > TimeUnit.SECONDS.toNanos(60)) {
                    throw new IllegalStateException("materialization failed to drain");
                }
                runtime.flush();
            }
            double drainSeconds = (System.nanoTime() - drainStart) / 1e9;
            var verify = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int worker = 0; worker < CLIENTS; worker++) {
                for (int slot = 0; slot < slots; slot++) {
                    int total = result.perWorker[worker];
                    int last = total <= slot ? 0 : total - Math.floorMod(total - 1 - slot, slots);
                    String suffix = worker + ":" + slot;
                    checkEqual(bytes(last), runtime.get(verify, "a", "k" + suffix).orElseThrow());
                    checkEqual(bytes(last), runtime.get(verify, rightShard, rightPrefix + suffix).orElseThrow());
                    // Verify Real database payload, cannot allow memory caches to mask errors in the result of materialization.
                    checkEqual(bytes(last), persistedLeft.get("k" + suffix, stats.publishedSequence()).orElseThrow().value());
                    checkEqual(bytes(last), persistedRight.get(rightPrefix + suffix, stats.publishedSequence()).orElseThrow().value());
                }
            }
            runtime.commit(verify);
            if (stats.publishedSequence() != result.committed + 1) {
                throw new IllegalStateException("WAL receipt count mismatch");
            }
            Files.writeString(output.resolve("wal-" + round + "-stats.txt"), "atEnd=" + stats + "\nafterDrain=" + runtime.stats() + "\n");
            return result.line(label, stats.groupForces(), stats.walBytes(), lag, drainSeconds,
                    count(a, "azir_wal_version"), single ? 0 : count(b, "azir_wal_version"));
        }
    }

    /**
     * Low resource positive-reverse comparison point read-only for reading, writing and range. Data is a low concurrent delayed observation instead of an upper machine capacity.
     *
     * @param control control
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param output output
     * @param lines lines
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void boundedComparison(final HikariDataSource control, final HikariDataSource a, final HikariDataSource b,
                                          final int seconds, final Path output, final List<String> lines) throws Exception {
        if (!LOW_RESOURCE || CLIENTS > 3) {
            throw new IllegalArgumentException("bounded comparison requires low-resource mode and <=3 clients");
        }
        for (int round = 1; round <= 2; round++) {
            for (int position = 0; position < 10; position++) {
                int mode = round == 1 ? position : 9 - position;
                String line = switch (mode) {
                    case 0 -> nativeMode(a, b, seconds, round, true);
                    case 1 -> nativeMode(a, b, seconds, round, false);
                    case 2 -> oldMode(control, a, b, seconds, round);
                    case 7 -> nativeRange(a, b, seconds, round);
                    case 8 -> oldRange(control, a, b, seconds, round);
                    case 9 -> walRange(a, b, seconds, round, output);
                    default -> walMode(a, b, seconds, round * 10 + mode, output, mode < 5 ? 8192 : 0, mode == 3 || mode == 5);
                };
                lines.add(line);
                Files.write(output.resolve("benchmark.tsv"), lines);
            }
        }
        Files.writeString(output.resolve("environment.txt"), "java=" + System.getProperty("java.version") + "\nmysql=" + version(a)
                + "\nclients=" + CLIENTS + "\nmysql_cpu_cores_each=0.5\nmysql_memory_bytes_each=536870912"
                + "\nworkload=RR two point reads and two point writes, independent client keys"
                + "\nwal_hot_cache_bytes=8192\nwal_cold_cache_bytes=0\nwal_hot_keys_per_worker=1\nwal_cold_keys_per_worker=128"
                + "\ninnodb_flush_log_at_trx_commit=1\nsync_binlog=1\n");
        Files.writeString(output.resolve("contracts.txt"), "Low concurrency comparison, not capacity or production SLA.\n"
                + "Native single: one local synchronous commit. Native two: two local commits, no global atomicity.\n"
                + "Old mode: unchanged default Azir API, cross-shard commit. WAL: local force(true), asynchronous MySQL materialization, no replicas.\n"
                + "Cold profile disables clean-value cache, but unmaterialized values remain in memory; observed miss rates must be checked.\n"
                + "Cold profile rotates 128 keys per worker; hot, native and old profiles use one key per worker per side. Working sets differ.\n"
                + "Point modes perform two point reads and two writes. Range modes are read-only transactions scanning 128 rows per shard.\n"
                + "Range data is static; range TPS must not be compared directly with point read/write TPS. No SQL joins are measured.\n"
                + "No separate warmup; reverse order reduces but cannot eliminate JIT and shared-host bias.\n");
    }

    /**
     * Original 2-hub range read-only contrast, full check of 128 rows and payload per piece.
     *
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String nativeRange(final HikariDataSource a, final HikariDataSource b, final int seconds, final int round) throws Exception {
        String prefix = "range:";
        for (var pool : List.of(a, b)) {
            try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
                ddl.executeUpdate("drop table if exists wal_native_range");
                ddl.executeUpdate("create table wal_native_range (user_key varbinary(512) primary key,payload longblob not null) engine=InnoDB");
                try (var insert = connection.prepareStatement("insert into wal_native_range values (?,?)")) {
                    for (int i = 0; i < 128; i++) {
                        insert.setBytes(1, rangeKey(prefix, i).getBytes(StandardCharsets.UTF_8));
                        insert.setBytes(2, bytes(i));
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
            }
        }
        String label = "range-native-two-" + round;
        Result result = run(label, seconds, (worker, ordinal) -> {
            try (var left = a.getConnection(); var right = b.getConnection()) {
                for (var connection : new java.sql.Connection[]{left, right}) {
                    connection.setAutoCommit(false);
                    try (var query = connection.prepareStatement("select user_key,payload from wal_native_range where user_key>=? and user_key<? order by user_key")) {
                        query.setBytes(1, prefix.getBytes(StandardCharsets.UTF_8));
                        query.setBytes(2, (prefix + "~").getBytes(StandardCharsets.UTF_8));
                        var rows = new java.util.TreeMap<String, byte[]>();
                        try (var row = query.executeQuery()) {
                            while (row.next()) {
                                if (rows.put(new String(row.getBytes(1), StandardCharsets.UTF_8), row.getBytes(2)) != null) {
                                    throw new IllegalStateException("duplicate range key");
                                }
                            }
                        }
                        checkRange(rows, prefix);
                    }
                    connection.commit();
                }
            } catch (final java.sql.SQLException failure) {
                throw new IllegalStateException("native range failed", failure);
            }
        }, () -> {
        });
        return result.line(label, 0, 0, 0, 0, count(a, "wal_native_range"), count(b, "wal_native_range"));
    }

    /**
     * Reads the same static data from the original public domain interface and maintains the same pattern.
     *
     * @param control control
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String oldRange(final HikariDataSource control, final HikariDataSource a, final HikariDataSource b, final int seconds, final int round) throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        metadata.initialize();
        var timestamps = new MysqlTimestampOracle(control);
        timestamps.initialize(100);
        var safe = new MysqlSafePointStore(control);
        safe.initialize(0);
        var left = new MysqlStorageAdapter("a", a, metadata);
        left.initialize();
        var right = new MysqlStorageAdapter("b", b, metadata);
        right.initialize();
        var adapters = Map.<String, StorageAdapter>of("a", left, "b", right);
        var gc = new GcManager(metadata, timestamps, safe, adapters);
        String prefix = "range-old-" + round + ":";
        String label = "range-old-two-" + round;
        try (var manager = new TransactionManager(timestamps, metadata, safe, TransactionOptions.defaults(), adapters)) {
            var seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : adapters.keySet()) {
                for (int i = 0; i < 128; i++) {
                    manager.put(seed, shard, rangeKey(prefix, i), bytes(i));
                }
            }
            manager.commit(seed);
            Result result = run(label, seconds, (worker, ordinal) -> {
                var tx = manager.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    var snapshot = manager.beginReadStatement(tx);
                    for (String shard : adapters.keySet()) {
                        var rows = new java.util.TreeMap<String, byte[]>();
                        for (var row : manager.scan(tx, shard, prefix, prefix + "~", snapshot)) {
                            if (row.deleted() || rows.put(row.key(), row.value()) != null) {
                                throw new IllegalStateException("invalid old range row");
                            }
                        }
                        checkRange(rows, prefix);
                    }
                    manager.commit(tx);
                } catch (final RuntimeException failure) {
                    try {
                        manager.rollback(tx);
                    } catch (final RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    throw failure;
                }
            }, () -> gc.gcOnce(128));
            return result.line(label, 0, 0, 0, 0, count(a, "mvcc_data"), count(b, "mvcc_data"));
        }
    }

    /**
     * Zero heat cache cross-shard range read-only, with a fixed statement snapshot and line-by-line check.
     *
     * @param a a
     * @param b b
     * @param seconds seconds
     * @param round round
     * @param output output
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String walRange(final HikariDataSource a, final HikariDataSource b, final int seconds, final int round, final Path output) throws Exception {
        for (var pool : List.of(a, b)) {
            try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
                ddl.executeUpdate("drop table if exists azir_wal_version");
                ddl.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
        String prefix = "range:";
        String label = "range-wal-two-" + round;
        var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
        try (var runtime = WalMvccRuntime.open(output.resolve(label), stores, WalOptions.defaults().withHotCacheBytes(0))) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : stores.keySet()) {
                for (int i = 0; i < 128; i++) {
                    runtime.put(seed, shard, rangeKey(prefix, i), bytes(i));
                }
            }
            runtime.commit(seed);
            runtime.flush();
            runtime.checkpoint();
            Result result = run(label, seconds, (worker, ordinal) -> {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    var snapshot = runtime.beginReadStatement(tx);
                    for (String shard : stores.keySet()) {
                        checkRange(runtime.scan(tx, shard, prefix, prefix + "~", snapshot), prefix);
                    }
                    runtime.commit(tx);
                } catch (final RuntimeException failure) {
                    try {
                        runtime.rollback(tx);
                    } catch (final RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    throw failure;
                }
            }, () -> runtime.collectGarbage(128));
            Files.writeString(output.resolve(label + "-stats.txt"), runtime.stats().toString());
            return result.line(label, runtime.stats().groupForces(), runtime.stats().walBytes(), 0, 0, count(a, "azir_wal_version"), count(b, "azir_wal_version"));
        }
    }

    /**
     * Construct a range key that is consistent in all modes and stable by dictionaries.
     *
     * @param prefix prefix
     * @param ordinal ordinal
     * @return the operation result
     */
    private static String rangeKey(final String prefix, final int ordinal) {
        return prefix + String.format(Locale.ROOT, "%04d", ordinal);
    }

    /**
     * Except for line numbers, each verify key is fully payload and is not allowed to miss, repeat or mismatch version.
     *
     * @param rows rows
     * @param prefix prefix
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void checkRange(final Map<String, byte[]> rows, final String prefix) {
        if (rows.size() != 128) {
            throw new IllegalStateException("range row count mismatch");
        }
        for (int i = 0; i < 128; i++) {
            checkEqual(bytes(i), rows.get(rangeKey(prefix, i)));
        }
    }

    /**
     * Starts a fixed client with GC per second, counting including the end of the journey transaction, and any failure will invalidate the evidence.
     *
     * @param label label
     * @param seconds seconds
     * @param workload workload
     * @param gc gc
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static Result run(final String label, final int seconds, final Workload workload, final Runnable gc) throws Exception {
        return runWorkers(label, seconds, worker -> new Worker() {

            /**
             * Retains the terminologies of the original probe for each transaction.
             *
             * @param ordinal ordinal
             */
            @Override
            public void execute(final int ordinal) {
                workload.execute(worker, ordinal);
            }
        }, gc);
    }

    /**
     * Uniformly measure reusable implementers, and the sample window aligns with the background load record in a visible timestamp.
     *
     * @param label label
     * @param seconds seconds
     * @param factory factory
     * @param gc gc
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static Result runWorkers(final String label, final int seconds, final WorkerFactory factory, final Runnable gc) throws Exception {
        var start = new CountDownLatch(1);
        var ready = new CountDownLatch(CLIENTS);
        var running = new AtomicBoolean(true);
        var maintenanceFailure = new AtomicReference<Throwable>();
        var latencies = new ArrayList<List<Long>>();
        int[] counts = new int[CLIENTS];
        long[] deadline = new long[1];
        for (int i = 0; i < CLIENTS; i++) {
            latencies.add(new ArrayList<>());
        }
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < CLIENTS; i++) {
                int worker = i;
                futures.add(tasks.submit(() -> {
                    try (var execution = factory.create(worker)) {
                        ready.countDown();
                        if (!start.await(30, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("measurement barrier timed out");
                        }
                        do {
                            long before = System.nanoTime();
                            execution.execute(counts[worker] + 1);
                            latencies.get(worker).add(System.nanoTime() - before);
                            counts[worker]++;
                        } while (System.nanoTime() < deadline[0]);
                    } catch (final Exception failure) {
                        ready.countDown();
                        throw new IllegalStateException(failure);
                    }
                }));
            }
            Thread maintenance = Thread.ofVirtual().start(() -> {
                try {
                    while (running.get()) {
                        Thread.sleep(1000);
                        if (running.get()) {
                            gc.run();
                        }
                    }
                } catch (final InterruptedException stop) {
                    Thread.currentThread().interrupt();
                } catch (final Throwable failure) {
                    maintenanceFailure.set(failure);
                }
            });
            if (!ready.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("client setup timed out");
            }
            phase(label, "start");
            long before = System.nanoTime();
            deadline[0] = before + TimeUnit.SECONDS.toNanos(seconds);
            start.countDown();
            long finished;
            try {
                for (var future : futures) {
                    future.get(seconds + 60L, TimeUnit.SECONDS);
                }
                finished = System.nanoTime();
                phase(label, "end");
            } finally {
                // JDBC reading and writing is closed by interrupt Socket; only the next round of GCs is forbidden, pending normal completion of the operation in transit.
                running.set(false);
                maintenance.join(15_000);
            }
            double elapsed = (finished - before) / 1e9;
            if (maintenanceFailure.get() != null || maintenance.isAlive()) {
                throw new IllegalStateException("GC failed", maintenanceFailure.get());
            }
            long[] samples = latencies.stream().flatMap(List::stream).mapToLong(Long::longValue).sorted().toArray();
            if (samples.length == 0) {
                throw new IllegalStateException("no acknowledged transaction");
            }
            return new Result(samples.length, elapsed, samples[(samples.length - 1) / 2] / 1e6,
                    samples[(int) Math.ceil(samples.length * .99) - 1] / 1e6, counts);
        }
    }

    /**
     * Writes the command line for each round of real timers without reading or recording other tasks.
     *
     * @param label label
     * @param event event
     * @throws java.io.IOException if the operation cannot complete
     */
    private static void phase(final String label, final String event) throws java.io.IOException {
        if (PHASE_EVENTS != null) {
            Files.writeString(PHASE_EVENTS, label + "\t" + event + "\t" + System.currentTimeMillis() + "\n",
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        }
    }

    /**
     * Saves the actually negotiated JDBC encryption package, without speculating whether the driver default uses TLS.
     *
     * @param source database connection source
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String tlsCipher(final HikariDataSource source) throws Exception {
        try (
                var connection = source.getConnection(); var query = connection.createStatement();
                var row = query.executeQuery("show session status like 'Ssl_cipher'")) {
            return row.next() ? row.getString(2) : "unknown";
        }
    }

    /**
     * Check cross-shard results and final business values. Errors cannot be recorded in throughput.
     *
     * @param a a
     * @param b b
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void checkEqual(final byte[] a, final byte[] b) {
        if (!Arrays.equals(a, b)) {
            throw new IllegalStateException("business value mismatch");
        }
    }

    /**
     * Encodes the client ' s single-to-business value.
     *
     * @param value value to write
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private static byte[] bytes(final int value) {
        byte[] prefix = Integer.toString(value).getBytes(StandardCharsets.UTF_8);
        if (PAYLOAD_BYTES == 0) {
            return prefix;
        }
        if (prefix.length > PAYLOAD_BYTES) {
            throw new IllegalStateException("ordinal exceeds configured payload");
        }
        byte[] payload = new byte[PAYLOAD_BYTES];
        Arrays.fill(payload, (byte) 'x');
        System.arraycopy(prefix, 0, payload, 0, prefix.length);
        return payload;
    }

    /**
     * Checks physical lines, allowing only fixed operating tables and version table names for probe.
     *
     * @param source database connection source
     * @param table table
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static long count(final HikariDataSource source, final String table) throws Exception {
        if (!List.of("mvcc_data", "azir_wal_version", "wal_native_single", "wal_native_two", "wal_native_range").contains(table)) {
            throw new IllegalArgumentException("unknown table");
        }
        try (
                var connection = source.getConnection(); var query = connection.createStatement();
                var row = query.executeQuery("select count(*) from " + table)) {
            row.next();
            return row.getLong(1);
        }
    }

    /**
     * Saves the real database version to avoid recording only mirror tags.
     *
     * @param source database connection source
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String version(final HikariDataSource source) throws Exception {
        try (
                var connection = source.getConnection(); var query = connection.createStatement();
                var row = query.executeQuery("select version()")) {
            row.next();
            return row.getString(1);
        }
    }

    /**
     * Each thread is unique to JDBC status, connection and repeat cannot cross the line or merge business transaction.
     */
    private static final class NativeWorker implements Worker {

        private final java.sql.Connection left;

        private final java.sql.Connection right;

        private final java.sql.PreparedStatement readLeft;

        private final java.sql.PreparedStatement readRight;

        private final java.sql.PreparedStatement writeLeft;

        private final java.sql.PreparedStatement writeRight;

        /**
         * Acquire resources and bind the same business keys before timing.
         *
         * @param a a
         * @param b b
         * @param table table
         * @param worker worker
         * @param single single
         * @throws Exception if the operation cannot complete
         */
        NativeWorker(final HikariDataSource a, final HikariDataSource b, final String table, final int worker, final boolean single) throws Exception {
            left = a.getConnection();
            java.sql.Connection second = null;
            try {
                second = single ? left : b.getConnection();
                right = second;
                left.setAutoCommit(false);
                if (right != left) {
                    right.setAutoCommit(false);
                }
                readLeft = left.prepareStatement("select payload from " + table + " where user_key=?");
                readRight = right.prepareStatement("select payload from " + table + " where user_key=?");
                writeLeft = left.prepareStatement("update " + table + " set payload=? where user_key=?");
                writeRight = right.prepareStatement("update " + table + " set payload=? where user_key=?");
                readLeft.setBytes(1, ("L" + worker).getBytes(StandardCharsets.UTF_8));
                readRight.setBytes(1, ("R" + worker).getBytes(StandardCharsets.UTF_8));
                writeLeft.setBytes(2, ("L" + worker).getBytes(StandardCharsets.UTF_8));
                writeRight.setBytes(2, ("R" + worker).getBytes(StandardCharsets.UTF_8));
            } catch (final Throwable failure) {
                if (second != null && second != left) {
                    second.close();
                }
                left.close();
                throw failure;
            }
        }

        /**
         * Each pen still performs two readings, two updates and the corresponding local commit.
         *
         * @param ordinal ordinal
         * @throws Exception if the operation cannot complete
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void execute(final int ordinal) throws Exception {
            try {
                checkEqual(read(readLeft), read(readRight));
                writeLeft.setBytes(1, bytes(ordinal));
                writeRight.setBytes(1, bytes(ordinal));
                if (writeLeft.executeUpdate() != 1 || writeRight.executeUpdate() != 1) {
                    throw new IllegalStateException("native row count mismatch");
                }
                left.commit();
                if (right != left) {
                    right.commit();
                }
            } catch (final Throwable failure) {
                try {
                    left.rollback();
                } catch (final Exception rollback) {
                    failure.addSuppressed(rollback);
                }
                if (right != left) {
                    try {
                        right.rollback();
                    } catch (final Exception rollback) {
                        failure.addSuppressed(rollback);
                    }
                }
                throw failure;
            }
        }

        /**
         * Read the right business lines from pre-compiled queries.
         *
         * @param query query
         * @return the operation result
         * @throws Exception if the operation cannot complete
         * @throws IllegalStateException if the operation cannot complete
         */
        private static byte[] read(final java.sql.PreparedStatement query) throws Exception {
            try (var row = query.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("native key missing");
                }
                return row.getBytes(1);
            }
        }

        /**
         * Close connection while releasing the connection created statement, and keep any closure anomaly.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            try {
                left.close();
            } finally {
                if (right != left) {
                    right.close();
                }
            }
        }
    }

    /**
     * An adapter for the same business transaction, only for the probe comparison.
     */
    @FunctionalInterface
    private interface Workload {

        /**
         * Executes a double-reading transaction with a single increase in serial numbers within each client.
         *
         * @param worker worker
         * @param ordinal ordinal
         */
        void execute(int worker, int ordinal);
    }

    /**
     * Operators held by individual threads during the measurement period.
     */
    private interface Worker extends AutoCloseable {

        /**
         * Execute a complete business transaction.
         *
         * @param ordinal ordinal
         * @throws Exception if the operation cannot complete
         */
        void execute(int ordinal) throws Exception;

        /**
         * Release resources that are the sole preserve of this thread.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        default void close() throws Exception {
        }
    }

    /**
     * Create a line-only resource before the time starts.
     */
    @FunctionalInterface
    private interface WorkerFactory {

        /**
         * Creates an implementer by client number.
         *
         * @param worker worker
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        Worker create(int worker) throws Exception;
    }

    /**
     * Keeps an exact delayed split when the results are aggregated and the end value of each client.
     *
     * @param committed committed
     * @param seconds seconds
     * @param p50 p50
     * @param p99 p99
     * @param perWorker per worker
     */
    private record Result(long committed, double seconds, double p50, double p99, int[] perWorker) {

        /**
         * Output a tab that can be saved directly to separate evidence.
         *
         * @param mode mode
         * @param forces forces
         * @param walBytes wal bytes
         * @param lag lag
         * @param drain drain
         * @param rowsA rows a
         * @param rowsB rows b
         * @return the operation result
         */
        String line(final String mode, final long forces, final long walBytes, final long lag, final double drain, final long rowsA, final long rowsB) {
            return String.format(Locale.ROOT, "%s\t%d\t%.6f\t%d\t%.2f\t%.3f\t%.3f\t%d\t%d\t%d\t%.6f\t%d\t%d",
                    mode, CLIENTS, seconds, committed, committed / seconds, p50, p99, forces, walBytes, lag, drain, rowsA, rowsB);
        }
    }
}

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

/** 隔离容量探针，可追加原生单库及两库基线，不加入普通测试执行或修改旧模式代码。 */
public final class WalMvpComparisonProbe {
    private static final int CLIENTS = Integer.getInteger("azir.wal.clients", 50);
    private static final boolean LOW_RESOURCE = Boolean.getBoolean("azir.wal.lowResource");
    private static final int PAYLOAD_BYTES = Integer.getInteger("azir.wal.payloadBytes", 0);
    private static final Path PHASE_EVENTS = System.getProperty("azir.wal.phaseEvents") == null ? null
            : Path.of(System.getProperty("azir.wal.phaseEvents"));

    /** 禁止实例化探针入口。 */
    private WalMvpComparisonProbe() { }

    /** 启动隔离实例，按正反顺序比较相同两读两写负载的不同提交合同。 */
    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]); Files.createDirectories(output);
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        boolean walOnly = args.length > 2 && args[2].equals("wal-only");
        boolean includeNative = args.length > 2 && args[2].equals("all");
        boolean optimized = args.length > 2 && args[2].equals("optimized");
        boolean bounded = args.length > 2 && args[2].equals("bounded");
        if (seconds < 1 || seconds > 300) { throw new IllegalArgumentException("invalid duration"); }
        if (CLIENTS < 1 || CLIENTS > 50 || LOW_RESOURCE && CLIENTS > 3) { throw new IllegalArgumentException("invalid client budget"); }
        if (PAYLOAD_BYTES < 0 || PAYLOAD_BYTES > 4096) { throw new IllegalArgumentException("invalid payload size"); }
        try (var serverA = container(); var serverB = container()) {
            serverA.start(); serverB.start();
            Files.writeString(output.resolve("owned-containers.txt"), serverA.getContainerId() + "\n" + serverB.getContainerId() + "\n");
            var created = serverA.execInContainer("mysql", "-uroot", "-pwal_probe", "-e",
                    "create database wal_control; grant all on wal_control.* to 'wal_probe'@'%'");
            if (created.getExitCode() != 0) { throw new IllegalStateException("create isolated control database failed"); }
            try (var a = pool(serverA); var b = pool(serverB);
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

    /** 原生 JDBC 对照：单库有本地原子性，两库只等待各自提交且没有全局协议。 */
    private static String nativeMode(HikariDataSource a, HikariDataSource b, int seconds, int round,
                                      boolean singleDatabase) throws Exception {
        String table = singleDatabase ? "wal_native_single" : "wal_native_two";
        seedNative(a, table, singleDatabase ? List.of("L", "R") : List.of("L"));
        if (!singleDatabase) { seedNative(b, table, List.of("R")); }
        Result result = run((singleDatabase ? "native-single-" : "native-two-") + round, seconds, (worker, ordinal) -> {
            try (var left = a.getConnection(); var right = singleDatabase ? null : b.getConnection()) {
                left.setAutoCommit(false);
                if (right != null) { right.setAutoCommit(false); }
                var second = right == null ? left : right;
                try {
                    checkEqual(nativeRead(left, table, "L" + worker), nativeRead(second, table, "R" + worker));
                    nativeUpdate(left, table, "L" + worker, ordinal);
                    nativeUpdate(second, table, "R" + worker, ordinal);
                    left.commit();
                    if (right != null) { right.commit(); }
                } catch (java.sql.SQLException | RuntimeException failure) {
                    try { left.rollback(); } catch (java.sql.SQLException cleanup) { failure.addSuppressed(cleanup); }
                    if (right != null) {
                        try { right.rollback(); } catch (java.sql.SQLException cleanup) { failure.addSuppressed(cleanup); }
                    }
                    throw failure;
                }
            } catch (java.sql.SQLException failure) { throw new IllegalStateException("native transaction failed", failure); }
        }, () -> { });
        try (var left = a.getConnection(); var right = singleDatabase ? null : b.getConnection()) {
            for (int worker = 0; worker < CLIENTS; worker++) {
                checkEqual(bytes(result.perWorker[worker]), nativeRead(left, table, "L" + worker));
                checkEqual(bytes(result.perWorker[worker]), nativeRead(right == null ? left : right, table, "R" + worker));
            }
        }
        return result.line((singleDatabase ? "native-single-" : "native-two-") + round,
                0, 0, 0, 0, count(a, table), singleDatabase ? 0 : count(b, table));
    }

    /** 复用每个客户端的连接和语句，保留每笔事务独立提交的原生优化基线。 */
    private static String optimizedNativeMode(HikariDataSource a, HikariDataSource b, int seconds, int round,
                                               boolean singleDatabase) throws Exception {
        String table = singleDatabase ? "wal_native_single" : "wal_native_two";
        seedNative(a, table, singleDatabase ? List.of("L", "R") : List.of("L"));
        if (!singleDatabase) { seedNative(b, table, List.of("R")); }
        String label = (singleDatabase ? "optimized-native-single-" : "optimized-native-two-") + round;
        warmConnections(a);
        if (!singleDatabase) { warmConnections(b); }
        Result result = runWorkers(label, seconds, worker -> new NativeWorker(a, b, table, worker, singleDatabase), () -> { });
        try (var left = a.getConnection(); var right = singleDatabase ? null : b.getConnection()) {
            for (int worker = 0; worker < CLIENTS; worker++) {
                checkEqual(bytes(result.perWorker[worker]), nativeRead(left, table, "L" + worker));
                checkEqual(bytes(result.perWorker[worker]), nativeRead(right == null ? left : right, table, "R" + worker));
            }
        }
        return result.line(label, 0, 0, 0, 0, count(a, table), singleDatabase ? 0 : count(b, table));
    }

    /** 在计时前顺序预热连接池，避免持有连接的并发客户端受五秒建连爬升限制。 */
    private static void warmConnections(HikariDataSource source) throws Exception {
        var held = new ArrayList<java.sql.Connection>();
        try {
            for (int i = 0; i < CLIENTS; i++) { held.add(source.getConnection()); }
        } finally {
            Exception first = null;
            for (var connection : held) {
                try { connection.close(); }
                catch (Exception failure) { if (first == null) { first = failure; } else { first.addSuppressed(failure); } }
            }
            if (first != null) { throw first; }
        }
    }

    /** 每个线程独占 JDBC 状态，连接和语句复用不能跨线程或合并业务事务。 */
    private static final class NativeWorker implements Worker {
        private final java.sql.Connection left;
        private final java.sql.Connection right;
        private final java.sql.PreparedStatement readLeft;
        private final java.sql.PreparedStatement readRight;
        private final java.sql.PreparedStatement writeLeft;
        private final java.sql.PreparedStatement writeRight;

        /** 在计时前取得资源并绑定不变的业务键。 */
        NativeWorker(HikariDataSource a, HikariDataSource b, String table, int worker, boolean single) throws Exception {
            left = a.getConnection();
            java.sql.Connection second = null;
            try {
                second = single ? left : b.getConnection();
                right = second;
                left.setAutoCommit(false); if (right != left) { right.setAutoCommit(false); }
                readLeft = left.prepareStatement("select payload from " + table + " where user_key=?");
                readRight = right.prepareStatement("select payload from " + table + " where user_key=?");
                writeLeft = left.prepareStatement("update " + table + " set payload=? where user_key=?");
                writeRight = right.prepareStatement("update " + table + " set payload=? where user_key=?");
                readLeft.setBytes(1, ("L" + worker).getBytes(StandardCharsets.UTF_8));
                readRight.setBytes(1, ("R" + worker).getBytes(StandardCharsets.UTF_8));
                writeLeft.setBytes(2, ("L" + worker).getBytes(StandardCharsets.UTF_8));
                writeRight.setBytes(2, ("R" + worker).getBytes(StandardCharsets.UTF_8));
            } catch (Throwable failure) {
                if (second != null && second != left) { second.close(); }
                left.close(); throw failure;
            }
        }

        /** 每笔仍执行两次读取、两次更新和相应的本地提交。 */
        @Override public void execute(int ordinal) throws Exception {
            try {
                checkEqual(read(readLeft), read(readRight));
                writeLeft.setBytes(1, bytes(ordinal)); writeRight.setBytes(1, bytes(ordinal));
                if (writeLeft.executeUpdate() != 1 || writeRight.executeUpdate() != 1) {
                    throw new IllegalStateException("native row count mismatch");
                }
                left.commit(); if (right != left) { right.commit(); }
            } catch (Throwable failure) {
                try { left.rollback(); } catch (Exception rollback) { failure.addSuppressed(rollback); }
                if (right != left) { try { right.rollback(); } catch (Exception rollback) { failure.addSuppressed(rollback); } }
                throw failure;
            }
        }

        /** 从预编译查询读取恰好存在的业务行。 */
        private static byte[] read(java.sql.PreparedStatement query) throws Exception {
            try (var row = query.executeQuery()) {
                if (!row.next()) { throw new IllegalStateException("native key missing"); }
                return row.getBytes(1);
            }
        }

        /** 关闭连接同时释放该连接创建的语句，任何关闭异常都保留。 */
        @Override public void close() throws Exception {
            try { left.close(); } finally { if (right != left) { right.close(); } }
        }
    }

    /** 初始化探针专属当前值表并将本轮客户端键重置为零。 */
    private static void seedNative(HikariDataSource source, String table, List<String> prefixes) throws Exception {
        try (var connection = source.getConnection(); var ddl = connection.createStatement()) {
            ddl.executeUpdate("create table if not exists " + table + " (user_key varbinary(512) primary key, payload longblob not null) engine=InnoDB");
            connection.setAutoCommit(false);
            try (var seed = connection.prepareStatement("insert into " + table + " (user_key,payload) values (?,?) "
                    + "on duplicate key update payload=values(payload)")) {
                for (String prefix : prefixes) {
                    for (int worker = 0; worker < CLIENTS; worker++) {
                        seed.setBytes(1, (prefix + worker).getBytes(StandardCharsets.UTF_8));
                        seed.setBytes(2, bytes(0)); seed.addBatch();
                    }
                }
                seed.executeBatch(); connection.commit();
            }
        }
    }

    /** 在连接的本地事务视图中读取一个原生业务值。 */
    private static byte[] nativeRead(java.sql.Connection connection, String table, String key) throws java.sql.SQLException {
        try (var query = connection.prepareStatement("select payload from " + table + " where user_key=?")) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            try (var row = query.executeQuery()) {
                if (!row.next()) { throw new IllegalStateException("native benchmark key missing"); }
                return row.getBytes(1);
            }
        }
    }

    /** 同步更新一行，行数异常会使整轮压测失败。 */
    private static void nativeUpdate(java.sql.Connection connection, String table, String key, int ordinal)
            throws java.sql.SQLException {
        try (var update = connection.prepareStatement("update " + table + " set payload=? where user_key=?")) {
            update.setBytes(1, bytes(ordinal)); update.setBytes(2, key.getBytes(StandardCharsets.UTF_8));
            if (update.executeUpdate() != 1) { throw new IllegalStateException("native update did not affect one row"); }
        }
    }

    /** 使用本地已有镜像创建强持久性隔离实例。 */
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

    /** 为前台与物化准备相同容量连接池。 */
    private static HikariDataSource pool(MySQLContainer<?> server) {
        return pool(server, server.getJdbcUrl());
    }

    /** 为隔离控制库指定相同物理实例上的独立 JDBC 地址。 */
    private static HikariDataSource pool(MySQLContainer<?> server, String url) {
        var config = new HikariConfig(); config.setJdbcUrl(url);
        config.setUsername(server.getUsername()); config.setPassword(server.getPassword());
        config.setMaximumPoolSize(LOW_RESOURCE ? 4 : 96); config.setMinimumIdle(0); config.setConnectionTimeout(5000);
        config.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "256");
        return new HikariDataSource(config);
    }

    /** 在新键空间装配原有默认事务协议，仅从探针调用公开接口。 */
    private static String oldMode(HikariDataSource control, HikariDataSource a, HikariDataSource b, int seconds, int round) throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control); metadata.initialize();
        var timestamps = new MysqlTimestampOracle(control); timestamps.initialize(100);
        var safePoint = new MysqlSafePointStore(control); safePoint.initialize(0);
        var left = new MysqlStorageAdapter("a", a, metadata); left.initialize();
        var right = new MysqlStorageAdapter("b", b, metadata); right.initialize();
        Map<String, StorageAdapter> adapters = Map.of("a", left, "b", right);
        var gc = new GcManager(metadata, timestamps, safePoint, adapters);
        String prefix = "old-" + round + "-";
        try (var manager = new TransactionManager(timestamps, metadata, safePoint, TransactionOptions.defaults(), adapters)) {
            var seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (int worker = 0; worker < CLIENTS; worker++) {
                manager.put(seed, "a", prefix + worker, bytes(0)); manager.put(seed, "b", prefix + worker, bytes(0));
            }
            manager.commit(seed);
            Result result = run("old-" + round, seconds, (worker, ordinal) -> {
                var tx = manager.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    byte[] av = manager.get(tx, "a", prefix + worker).orElseThrow();
                    byte[] bv = manager.get(tx, "b", prefix + worker).orElseThrow();
                    checkEqual(av, bv);
                    manager.put(tx, "a", prefix + worker, bytes(ordinal)); manager.put(tx, "b", prefix + worker, bytes(ordinal));
                    manager.commit(tx);
                } catch (RuntimeException ex) { try { manager.rollback(tx); } catch (RuntimeException ignored) { } throw ex; }
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

    /** 使用独立日志和专属物化表运行新模式，计时结束后核对落盘结果。 */
    private static String walMode(HikariDataSource a, HikariDataSource b, int seconds, int round, Path output) throws Exception {
        return walMode(a, b, seconds, round, output, 8L * 1024 * 1024, false);
    }

    /** 对照热点/冷回源与单片/双片，仍保持每笔两读两写和逐行后端核对。 */
    private static String walMode(HikariDataSource a, HikariDataSource b, int seconds, int round, Path output,
                                  long cacheBytes, boolean single) throws Exception {
        for (var pool : List.of(a, b)) {
            try (var connection = pool.getConnection(); var query = connection.createStatement()) {
                query.executeUpdate("drop table if exists azir_wal_version"); query.executeUpdate("drop table if exists azir_wal_binding");
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
        try (var runtime = WalMvccRuntime.open(output.resolve("wal-" + round),
                single ? Map.of("a", persistedLeft) : Map.of("a", persistedLeft, "b", persistedRight), limits)) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int worker = 0; worker < CLIENTS; worker++) {
                for (int slot = 0; slot < slots; slot++) {
                    String suffix = worker + ":" + slot;
                    runtime.put(seed, "a", "k" + suffix, bytes(0)); runtime.put(seed, rightShard, rightPrefix + suffix, bytes(0));
                }
            }
            runtime.commit(seed); runtime.flush();
            long progressStart = System.nanoTime();
            Path progressFile = output.resolve("wal-" + round + "-progress.tsv");
            Files.writeString(progressFile, "seconds\tpublished\ta\tb\tretained_bytes\theap_used_bytes\n");
            Result result = run(label, seconds, (worker, ordinal) -> {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    String suffix = worker + ":" + (ordinal - 1) % slots;
                    checkEqual(runtime.get(tx, "a", "k" + suffix).orElseThrow(), runtime.get(tx, rightShard, rightPrefix + suffix).orElseThrow());
                    runtime.put(tx, "a", "k" + suffix, bytes(ordinal)); runtime.put(tx, rightShard, rightPrefix + suffix, bytes(ordinal));
                    runtime.commit(tx);
                } catch (RuntimeException ex) { try { runtime.rollback(tx); } catch (RuntimeException ignored) { } throw ex; }
            }, () -> {
                var progress = runtime.stats();
                try {
                    Files.writeString(progressFile, String.format(Locale.ROOT, "%.3f\t%d\t%d\t%d\t%d\t%d\n",
                            (System.nanoTime() - progressStart) / 1e9, progress.publishedSequence(),
                            progress.materializedSequences().get("a"), progress.materializedSequences().getOrDefault("b", progress.materializedSequences().get("a")),
                            progress.retainedBytes(), java.lang.management.ManagementFactory.getMemoryMXBean()
                                    .getHeapMemoryUsage().getUsed()), java.nio.file.StandardOpenOption.APPEND);
                } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                runtime.collectGarbage(128);
            });
            var stats = runtime.stats();
            long lag = stats.materializedSequences().values().stream().mapToLong(sequence -> stats.publishedSequence() - sequence).max().orElse(0);
            long drainStart = System.nanoTime();
            while (runtime.stats().materializedSequences().values().stream().anyMatch(sequence -> sequence < stats.publishedSequence())) {
                if (System.nanoTime() - drainStart > TimeUnit.SECONDS.toNanos(60)) { throw new IllegalStateException("materialization failed to drain"); }
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
                    // 验证真实数据库载荷，不能让内存头缓存掩盖物化结果的错误。
                    checkEqual(bytes(last), persistedLeft.get("k" + suffix, stats.publishedSequence()).orElseThrow().value());
                    checkEqual(bytes(last), persistedRight.get(rightPrefix + suffix, stats.publishedSequence()).orElseThrow().value());
                }
            }
            runtime.commit(verify);
            if (stats.publishedSequence() != result.committed + 1) { throw new IllegalStateException("WAL receipt count mismatch"); }
            Files.writeString(output.resolve("wal-" + round + "-stats.txt"), "atEnd=" + stats + "\nafterDrain=" + runtime.stats() + "\n");
            return result.line(label, stats.groupForces(), stats.walBytes(), lag, drainSeconds,
                    count(a, "azir_wal_version"), single ? 0 : count(b, "azir_wal_version"));
        }
    }

    /** 低资源正反顺序比较点读写与范围只读，数据是低并发延迟观察而不是机器容量上限。 */
    private static void boundedComparison(HikariDataSource control, HikariDataSource a, HikariDataSource b,
                                           int seconds, Path output, List<String> lines) throws Exception {
        if (!LOW_RESOURCE || CLIENTS > 3) { throw new IllegalArgumentException("bounded comparison requires low-resource mode and <=3 clients"); }
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
                lines.add(line); Files.write(output.resolve("benchmark.tsv"), lines);
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

    /** 原生两库范围只读对照，每片完整核对 128 行及载荷。 */
    private static String nativeRange(HikariDataSource a, HikariDataSource b, int seconds, int round) throws Exception {
        String prefix = "range:";
        for (var pool : List.of(a, b)) {
            try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
                ddl.executeUpdate("drop table if exists wal_native_range");
                ddl.executeUpdate("create table wal_native_range (user_key varbinary(512) primary key,payload longblob not null) engine=InnoDB");
                try (var insert = connection.prepareStatement("insert into wal_native_range values (?,?)")) {
                    for (int i = 0; i < 128; i++) { insert.setBytes(1, rangeKey(prefix, i).getBytes(StandardCharsets.UTF_8)); insert.setBytes(2, bytes(i)); insert.addBatch(); }
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
                        query.setBytes(1, prefix.getBytes(StandardCharsets.UTF_8)); query.setBytes(2, (prefix + "~").getBytes(StandardCharsets.UTF_8));
                        var rows = new java.util.TreeMap<String, byte[]>();
                        try (var row = query.executeQuery()) {
                            while (row.next()) {
                                if (rows.put(new String(row.getBytes(1), StandardCharsets.UTF_8), row.getBytes(2)) != null) { throw new IllegalStateException("duplicate range key"); }
                            }
                        }
                        checkRange(rows, prefix);
                    }
                    connection.commit();
                }
            } catch (java.sql.SQLException failure) { throw new IllegalStateException("native range failed", failure); }
        }, () -> { });
        return result.line(label, 0, 0, 0, 0, count(a, "wal_native_range"), count(b, "wal_native_range"));
    }

    /** 从原有公开范围接口读取相同静态数据，保持原模式实现不变。 */
    private static String oldRange(HikariDataSource control, HikariDataSource a, HikariDataSource b, int seconds, int round) throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control); metadata.initialize();
        var timestamps = new MysqlTimestampOracle(control); timestamps.initialize(100);
        var safe = new MysqlSafePointStore(control); safe.initialize(0);
        var left = new MysqlStorageAdapter("a", a, metadata); left.initialize();
        var right = new MysqlStorageAdapter("b", b, metadata); right.initialize();
        var adapters = Map.<String, StorageAdapter>of("a", left, "b", right);
        var gc = new GcManager(metadata, timestamps, safe, adapters);
        String prefix = "range-old-" + round + ":"; String label = "range-old-two-" + round;
        try (var manager = new TransactionManager(timestamps, metadata, safe, TransactionOptions.defaults(), adapters)) {
            var seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : adapters.keySet()) { for (int i = 0; i < 128; i++) { manager.put(seed, shard, rangeKey(prefix, i), bytes(i)); } }
            manager.commit(seed);
            Result result = run(label, seconds, (worker, ordinal) -> {
                var tx = manager.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    var snapshot = manager.beginReadStatement(tx);
                    for (String shard : adapters.keySet()) {
                        var rows = new java.util.TreeMap<String, byte[]>();
                        for (var row : manager.scan(tx, shard, prefix, prefix + "~", snapshot)) {
                            if (row.deleted() || rows.put(row.key(), row.value()) != null) { throw new IllegalStateException("invalid old range row"); }
                        }
                        checkRange(rows, prefix);
                    }
                    manager.commit(tx);
                } catch (RuntimeException failure) { try { manager.rollback(tx); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
            }, () -> gc.gcOnce(128));
            return result.line(label, 0, 0, 0, 0, count(a, "mvcc_data"), count(b, "mvcc_data"));
        }
    }

    /** 零热点缓存的跨片范围只读，使用固定语句快照并逐行核对。 */
    private static String walRange(HikariDataSource a, HikariDataSource b, int seconds, int round, Path output) throws Exception {
        for (var pool : List.of(a, b)) {
            try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
                ddl.executeUpdate("drop table if exists azir_wal_version"); ddl.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
        String prefix = "range:"; String label = "range-wal-two-" + round;
        var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
        try (var runtime = WalMvccRuntime.open(output.resolve(label), stores, WalOptions.defaults().withHotCacheBytes(0))) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : stores.keySet()) { for (int i = 0; i < 128; i++) { runtime.put(seed, shard, rangeKey(prefix, i), bytes(i)); } }
            runtime.commit(seed); runtime.flush(); runtime.checkpoint();
            Result result = run(label, seconds, (worker, ordinal) -> {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                try {
                    var snapshot = runtime.beginReadStatement(tx);
                    for (String shard : stores.keySet()) { checkRange(runtime.scan(tx, shard, prefix, prefix + "~", snapshot), prefix); }
                    runtime.commit(tx);
                } catch (RuntimeException failure) { try { runtime.rollback(tx); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
            }, () -> runtime.collectGarbage(128));
            Files.writeString(output.resolve(label + "-stats.txt"), runtime.stats().toString());
            return result.line(label, runtime.stats().groupForces(), runtime.stats().walBytes(), 0, 0, count(a, "azir_wal_version"), count(b, "azir_wal_version"));
        }
    }

    /** 构造各模式一致且按字典序稳定的范围键。 */
    private static String rangeKey(String prefix, int ordinal) { return prefix + String.format(Locale.ROOT, "%04d", ordinal); }

    /** 除行数外验证每个键与完整载荷，不允许漏行、重复或版本错配。 */
    private static void checkRange(Map<String, byte[]> rows, String prefix) {
        if (rows.size() != 128) { throw new IllegalStateException("range row count mismatch"); }
        for (int i = 0; i < 128; i++) { checkEqual(bytes(i), rows.get(rangeKey(prefix, i))); }
    }

    /** 相同业务事务的适配器，仅用于探针对照。 */
    @FunctionalInterface private interface Workload {
        /** 执行一笔两读两写事务，序号在每个客户端中单调增长。 */
        void execute(int worker, int ordinal);
    }

    /** 测量期间由单个线程持有的业务执行器。 */
    private interface Worker extends AutoCloseable {
        /** 执行一笔完整业务事务。 */
        void execute(int ordinal) throws Exception;
        /** 释放该线程独占的资源。 */
        @Override default void close() throws Exception { }
    }

    /** 在计时开始前创建线程独占资源。 */
    @FunctionalInterface private interface WorkerFactory {
        /** 按客户端编号创建执行器。 */
        Worker create(int worker) throws Exception;
    }

    /** 聚合计时结果和每个客户端最终值，保留精确延迟分位。 */
    private record Result(long committed, double seconds, double p50, double p99, int[] perWorker) {
        /** 输出可直接保存的制表符分隔证据。 */
        String line(String mode, long forces, long walBytes, long lag, double drain, long rowsA, long rowsB) {
            return String.format(Locale.ROOT, "%s\t%d\t%.6f\t%d\t%.2f\t%.3f\t%.3f\t%d\t%d\t%d\t%.6f\t%d\t%d",
                    mode, CLIENTS, seconds, committed, committed / seconds, p50, p99, forces, walBytes, lag, drain, rowsA, rowsB);
        }
    }

    /** 启动固定客户端与每秒 GC，计时包括在途事务结束，任何失败都使证据无效。 */
    private static Result run(String label, int seconds, Workload workload, Runnable gc) throws Exception {
        return runWorkers(label, seconds, worker -> new Worker() {
            /** 保留原探针每笔事务的调用语义。 */
            @Override public void execute(int ordinal) { workload.execute(worker, ordinal); }
        }, gc);
    }

    /** 统一测量可复用执行器，采样窗口与后台负载记录以显式时间戳对齐。 */
    private static Result runWorkers(String label, int seconds, WorkerFactory factory, Runnable gc) throws Exception {
        var start = new CountDownLatch(1);
        var ready = new CountDownLatch(CLIENTS);
        var running = new AtomicBoolean(true);
        var maintenanceFailure = new AtomicReference<Throwable>();
        var latencies = new ArrayList<List<Long>>();
        int[] counts = new int[CLIENTS];
        long[] deadline = new long[1];
        for (int i = 0; i < CLIENTS; i++) { latencies.add(new ArrayList<>()); }
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < CLIENTS; i++) {
                int worker = i;
                futures.add(tasks.submit(() -> {
                    try (var execution = factory.create(worker)) {
                        ready.countDown();
                        if (!start.await(30, TimeUnit.SECONDS)) { throw new IllegalStateException("measurement barrier timed out"); }
                        do {
                            long before = System.nanoTime();
                            execution.execute(counts[worker] + 1);
                            latencies.get(worker).add(System.nanoTime() - before); counts[worker]++;
                        } while (System.nanoTime() < deadline[0]);
                    } catch (Exception failure) { ready.countDown(); throw new IllegalStateException(failure); }
                }));
            }
            Thread maintenance = Thread.ofVirtual().start(() -> {
                try {
                    while (running.get()) { Thread.sleep(1000); if (running.get()) { gc.run(); } }
                } catch (InterruptedException stop) { Thread.currentThread().interrupt(); }
                catch (Throwable failure) { maintenanceFailure.set(failure); }
            });
            if (!ready.await(30, TimeUnit.SECONDS)) { throw new IllegalStateException("client setup timed out"); }
            phase(label, "start");
            long before = System.nanoTime(); deadline[0] = before + TimeUnit.SECONDS.toNanos(seconds); start.countDown();
            long finished;
            try {
                for (var future : futures) { future.get(seconds + 60L, TimeUnit.SECONDS); }
                finished = System.nanoTime(); phase(label, "end");
            } finally {
                // JDBC 读写被 interrupt 会关闭 Socket；只禁止下一轮 GC，等待在途操作正常结束。
                running.set(false); maintenance.join(15_000);
            }
            double elapsed = (finished - before) / 1e9;
            if (maintenanceFailure.get() != null || maintenance.isAlive()) {
                throw new IllegalStateException("GC failed", maintenanceFailure.get());
            }
            long[] samples = latencies.stream().flatMap(List::stream).mapToLong(Long::longValue).sorted().toArray();
            if (samples.length == 0) { throw new IllegalStateException("no acknowledged transaction"); }
            return new Result(samples.length, elapsed, samples[(samples.length - 1) / 2] / 1e6,
                    samples[(int) Math.ceil(samples.length * .99) - 1] / 1e6, counts);
        }
    }

    /** 写出每轮真实计时窗口，不读取或记录其他任务的命令行。 */
    private static void phase(String label, String event) throws java.io.IOException {
        if (PHASE_EVENTS != null) {
            Files.writeString(PHASE_EVENTS, label + "\t" + event + "\t" + System.currentTimeMillis() + "\n",
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        }
    }

    /** 保存实际协商的 JDBC 加密套件，不推测驱动默认是否使用了 TLS。 */
    private static String tlsCipher(HikariDataSource source) throws Exception {
        try (var connection = source.getConnection(); var query = connection.createStatement();
             var row = query.executeQuery("show session status like 'Ssl_cipher'")) {
            return row.next() ? row.getString(2) : "unknown";
        }
    }

    /** 核对跨片结果和最终业务值，错误不能计入吞吐。 */
    private static void checkEqual(byte[] a, byte[] b) {
        if (!Arrays.equals(a, b)) { throw new IllegalStateException("business value mismatch"); }
    }

    /** 编码客户端单调业务值。 */
    private static byte[] bytes(int value) {
        byte[] prefix = Integer.toString(value).getBytes(StandardCharsets.UTF_8);
        if (PAYLOAD_BYTES == 0) { return prefix; }
        if (prefix.length > PAYLOAD_BYTES) { throw new IllegalStateException("ordinal exceeds configured payload"); }
        byte[] payload = new byte[PAYLOAD_BYTES]; Arrays.fill(payload, (byte) 'x');
        System.arraycopy(prefix, 0, payload, 0, prefix.length); return payload;
    }

    /** 核对物理行数，仅允许探针使用的固定业务表和版本表名。 */
    private static long count(HikariDataSource source, String table) throws Exception {
        if (!List.of("mvcc_data", "azir_wal_version", "wal_native_single", "wal_native_two", "wal_native_range").contains(table)) {
            throw new IllegalArgumentException("unknown table");
        }
        try (var connection = source.getConnection(); var query = connection.createStatement();
             var row = query.executeQuery("select count(*) from " + table)) { row.next(); return row.getLong(1); }
    }

    /** 保存真实数据库版本，避免只记录镜像标签。 */
    private static String version(HikariDataSource source) throws Exception {
        try (var connection = source.getConnection(); var query = connection.createStatement();
             var row = query.executeQuery("select version()")) { row.next(); return row.getString(1); }
    }
}

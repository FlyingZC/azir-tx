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

package com.flyingzc.azir.e2e;

import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MySQL local transaction benchmark test entrance.
 */
public final class MysqlLocalTxnBenchmark {

    /**
     * Construct a MysqlLocalTxnBenchmark instance.
     */
    private MysqlLocalTxnBenchmark() {
    }

    /**
     * Run the application entry point.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource dataSource = MysqlDataSourceFactory.create(config.jdbcUrl(), config.username(), config.password());
        try {
            initialize(dataSource);
            if (config.resetTables()) {
                reset(dataSource);
            }
            if (config.conflictMode()) {
                seedHotRows(dataSource);
            }

            BenchmarkResult result = runBenchmark(dataSource, config);
            System.out.println(result.toLine(config));
        } finally {
            close(dataSource);
        }
    }

    /**
     * Initialize the required database tables or runtime environment.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void initialize(final DataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_single_a (
                      k varchar(128) not null primary key,
                      v varchar(128) not null
                    )
                    """);
            statement.execute("""
                    create table if not exists bench_single_b (
                      k varchar(128) not null primary key,
                      v varchar(128) not null
                    )
                    """);
        }
    }

    /**
     * Reset the benchmark tables.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void reset(final DataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("delete from bench_single_a");
            statement.execute("delete from bench_single_b");
        }
    }

    /**
     * Insert initial hot-row data.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void seedHotRows(final DataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement insertA = connection.prepareStatement("""
                        insert into bench_single_a(k, v) values (?, ?)
                        on duplicate key update v = values(v)
                        """);
                PreparedStatement insertB = connection.prepareStatement("""
                        insert into bench_single_b(k, v) values (?, ?)
                        on duplicate key update v = values(v)
                        """)) {
            insertA.setString(1, "hot");
            insertA.setString(2, "v");
            insertA.executeUpdate();
            insertB.setString(1, "hot");
            insertB.setString(2, "v");
            insertB.executeUpdate();
        }
    }

    /**
     * Run the benchmark and return its statistics.
     *
     * @param dataSource database connection source
     * @param config config
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static BenchmarkResult runBenchmark(final DataSource dataSource, final Config config) throws Exception {
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder conflicts = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicLong sequence = new AtomicLong();

        var executor = Executors.newFixedThreadPool(config.concurrency());
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(config.concurrency());
        long startedAt = System.nanoTime();
        long deadline = startedAt + TimeUnit.SECONDS.toNanos(config.durationSeconds());

        for (int i = 0; i < config.concurrency(); i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (System.nanoTime() < deadline) {
                        attempts.increment();
                        try (Connection connection = dataSource.getConnection()) {
                            connection.setAutoCommit(false);
                            try {
                                if (config.conflictMode()) {
                                    executeConflictTxn(connection);
                                } else {
                                    executeNoConflictTxn(connection, sequence.incrementAndGet());
                                }
                                connection.commit();
                                committed.increment();
                            } catch (final SQLException ex) {
                                connection.rollback();
                                if (isConflict(ex)) {
                                    conflicts.increment();
                                } else {
                                    failures.increment();
                                }
                            } finally {
                                connection.setAutoCommit(true);
                            }
                        } catch (final SQLException ex) {
                            failures.increment();
                        }
                    }
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        return new BenchmarkResult(elapsedSeconds, attempts.sum(), committed.sum(), conflicts.sum(), failures.sum());
    }

    /**
     * Execute conflict-free local transaction.
     *
     * @param connection connection
     * @param id id
     * @throws SQLException if the operation cannot complete
     */
    private static void executeNoConflictTxn(final Connection connection, final long id) throws SQLException {
        try (
                PreparedStatement writeA = connection.prepareStatement("""
                        insert into bench_single_a(k, v) values (?, ?)
                        on duplicate key update v = values(v)
                        """);
                PreparedStatement writeB = connection.prepareStatement("""
                        insert into bench_single_b(k, v) values (?, ?)
                        on duplicate key update v = values(v)
                        """)) {
            writeA.setString(1, "bench:a:" + id);
            writeA.setString(2, "v" + id);
            writeA.executeUpdate();

            writeB.setString(1, "bench:b:" + id);
            writeB.setString(2, "v" + id);
            writeB.executeUpdate();
        }
    }

    /**
     * Enforcement competes for local transaction lines.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private static void executeConflictTxn(final Connection connection) throws SQLException {
        try (
                PreparedStatement updateA = connection.prepareStatement("""
                        update bench_single_a set v = ? where k = 'hot'
                        """);
                PreparedStatement updateB = connection.prepareStatement("""
                        update bench_single_b set v = ? where k = 'hot'
                        """)) {
            String value = Long.toString(System.nanoTime());
            updateA.setString(1, value);
            updateA.executeUpdate();
            updateB.setString(1, value);
            updateB.executeUpdate();
        }
    }

    /**
     * Determine whether the SQL failure represents a transaction conflict.
     *
     * @param ex ex
     * @return the operation result
     */
    private static boolean isConflict(final SQLException ex) {
        return ex instanceof SQLTransactionRollbackException
                || "40001".equals(ex.getSQLState())
                || "41000".equals(ex.getSQLState());
    }

    /**
     * Close resources and background executors.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void close(final DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * Read a string-valued environment variable.
     *
     * @param key business key
     * @param defaultValue default value
     * @return the operation result
     */
    private static String env(final String key, final String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /**
     * Benchmark configuration.
     *
     * @param jdbcUrl jdbc url
     * @param username username
     * @param password password
     * @param resetTables reset tables
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param conflictMode conflict mode
     */
    private record Config(String jdbcUrl,
            String username,
            String password,
            boolean resetTables,
            int concurrency,
            int durationSeconds,
            boolean conflictMode) {

        /**
         * Read configuration from environment variables.
         *
         * @return the operation result
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_LOCAL_BENCH_JDBC", "jdbc:mysql://127.0.0.1:23306/single_tx"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Boolean.parseBoolean(env("AZIR_TX_BENCH_CONFLICT", "false")));
        }
    }

    /**
     * Benchmark results.
     *
     * @param elapsedSeconds elapsed seconds
     * @param attempts attempts
     * @param committed committed
     * @param conflicts conflicts
     * @param failures failures
     */
    private record BenchmarkResult(double elapsedSeconds,
            long attempts,
            long committed,
            long conflicts,
            long failures) {

        /**
         * Format benchmark results as a single line.
         *
         * @param config config
         * @return the operation result
         */
        private String toLine(final Config config) {
            double attemptTps = attempts / elapsedSeconds;
            double commitTps = committed / elapsedSeconds;
            double conflictTps = conflicts / elapsedSeconds;
            return "LOCAL_BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + (config.conflictMode() ? "conflict" : "no_conflict")
                    + " attempts=" + attempts
                    + " committed=" + committed
                    + " conflicts=" + conflicts
                    + " failures=" + failures
                    + " attempt_tps=" + String.format("%.2f", attemptTps)
                    + " commit_tps=" + String.format("%.2f", commitTps)
                    + " conflict_tps=" + String.format("%.2f", conflictTps);
        }
    }
}

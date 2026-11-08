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
 * MySQL directs two shard benchmark test portals to compare performance baselines without global transaction coordination.
 */
public final class MysqlDirectTwoShardTxnBenchmark {

    /**
     * Construct a MysqlDirectTwoShardTxnBenchmark instance.
     */
    private MysqlDirectTwoShardTxnBenchmark() {
    }

    /**
     * Run the application entry point.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());
        try {
            initialize(shard1DataSource);
            initialize(shard2DataSource);
            if (config.resetTables()) {
                reset(shard1DataSource);
                reset(shard2DataSource);
            }
            if (config.conflictMode()) {
                seedHotRow(shard1DataSource);
                seedHotRow(shard2DataSource);
            }

            BenchmarkResult result = runBenchmark(shard1DataSource, shard2DataSource, config);
            System.out.println(result.toLine(config));
        } finally {
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

    /**
     * Initializes the benchmark test table.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void initialize(final DataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_direct_tx (
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
            statement.execute("delete from bench_direct_tx");
        }
    }

    /**
     * Insert initial hot-row data.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void seedHotRow(final DataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement("""
                        insert into bench_direct_tx(k, v) values ('hot', 'v')
                        on duplicate key update v = values(v)
                        """)) {
            statement.executeUpdate();
        }
    }

    /**
     * Run the benchmark and return its statistics.
     *
     * @param shard1DataSource shard1 data source
     * @param shard2DataSource shard2 data source
     * @param config config
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static BenchmarkResult runBenchmark(final DataSource shard1DataSource,
                                                final DataSource shard2DataSource,
                                                final Config config) throws Exception {
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
                        try {
                            executeTxn(shard1DataSource, shard2DataSource, sequence.incrementAndGet(), config.conflictMode());
                            committed.increment();
                        } catch (final SQLException ex) {
                            if (isConflict(ex)) {
                                conflicts.increment();
                            } else {
                                failures.increment();
                            }
                        } catch (final RuntimeException ex) {
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
     * Execute two shard writings without global coordination once.
     *
     * @param shard1DataSource shard1 data source
     * @param shard2DataSource shard2 data source
     * @param id id
     * @param conflictMode conflict mode
     * @throws SQLException if the operation cannot complete
     */
    private static void executeTxn(final DataSource shard1DataSource,
                                   final DataSource shard2DataSource,
                                   final long id,
                                   final boolean conflictMode) throws SQLException {
        try (
                Connection shard1 = shard1DataSource.getConnection();
                Connection shard2 = shard2DataSource.getConnection()) {
            shard1.setAutoCommit(false);
            shard2.setAutoCommit(false);
            try {
                if (conflictMode) {
                    executeConflictWrite(shard1);
                    executeConflictWrite(shard2);
                } else {
                    executeNoConflictWrite(shard1, "bench:s1:" + id, "v" + id);
                    executeNoConflictWrite(shard2, "bench:s2:" + id, "v" + id);
                }
                // The straight-link baseline does not have an global commit protocol: after either shard commit, another shard failure leaves a partial commit risk.
                shard1.commit();
                shard2.commit();
            } catch (final SQLException ex) {
                rollbackQuietly(shard1);
                rollbackQuietly(shard2);
                throw ex;
            } finally {
                shard1.setAutoCommit(true);
                shard2.setAutoCommit(true);
            }
        }
    }

    /**
     * Implementation is not conflict-free.
     *
     * @param connection connection
     * @param key business key
     * @param value value to write
     * @throws SQLException if the operation cannot complete
     */
    private static void executeNoConflictWrite(final Connection connection, final String key, final String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into bench_direct_tx(k, v) values (?, ?)
                on duplicate key update v = values(v)
                """)) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }

    /**
     * Execute hot line writing.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private static void executeConflictWrite(final Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                update bench_direct_tx set v = ? where k = 'hot'
                """)) {
            statement.setString(1, Long.toString(System.nanoTime()));
            statement.executeUpdate();
        }
    }

    /**
     * Quiet rollback local transaction.
     *
     * @param connection connection
     */
    private static void rollbackQuietly(final Connection connection) {
        try {
            connection.rollback();
        } catch (final SQLException ignored) {
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
     * @param shard1JdbcUrl shard1 jdbc url
     * @param shard2JdbcUrl shard2 jdbc url
     * @param username username
     * @param password password
     * @param resetTables reset tables
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param conflictMode conflict mode
     */
    private record Config(String shard1JdbcUrl,
            String shard2JdbcUrl,
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
                    env("AZIR_TX_DIRECT_SHARD1_JDBC", env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1")),
                    env("AZIR_TX_DIRECT_SHARD2_JDBC", env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2")),
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
            return "DIRECT_TWO_SHARD_BENCH"
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

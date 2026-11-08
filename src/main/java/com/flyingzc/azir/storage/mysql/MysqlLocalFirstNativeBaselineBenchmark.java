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

package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * MySQL single key transaction capacity contrasts with the original MySQL load, which is the same as the local first commit protocol.
 */
public final class MysqlLocalFirstNativeBaselineBenchmark {

    /**
     * The benchmark entry point keeps no global mutable state.
     */
    private MysqlLocalFirstNativeBaselineBenchmark() {
    }

    /**
     * When the standalone sheet is reset, it is constructed with the same key and value by durable transaction.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("native baseline requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "20"));
        boolean explicitTransaction = Boolean.parseBoolean(env("AZIR_TX_NATIVE_EXPLICIT_TX", "true"));
        boolean reuseConnection = Boolean.parseBoolean(env("AZIR_TX_NATIVE_REUSE_CONNECTION", "true"));
        boolean shardWatermark = Boolean.parseBoolean(env("AZIR_TX_NATIVE_SHARD_WATERMARK", "false"));
        int watermarkStripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "1"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 600
                || watermarkStripes < 1 || watermarkStripes > 256
                || shardWatermark && !explicitTransaction) {
            throw new IllegalArgumentException("invalid native baseline limits");
        }
        String url = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (
                HikariDataSource dataSource = (HikariDataSource) MysqlDataSourceFactory.create(
                        url, username, password)) {
            initializeAndReset(dataSource, watermarkStripes);
            run(dataSource, concurrency, durationSeconds, explicitTransaction,
                    reuseConnection, shardWatermark, watermarkStripes);
        }
    }

    /**
     * Emptys only the benchmark POP and does not touch the control protocol with the shard MVCC table.
     *
     * @param dataSource database connection source
     * @param watermarkStripes watermark stripes
     * @throws Exception if the operation cannot complete
     */
    private static void initializeAndReset(final HikariDataSource dataSource, final int watermarkStripes) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_local_first_native (
                      user_key varbinary(256) not null primary key,
                      payload blob not null
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_local_first_native");
            statement.execute("""
                    create table if not exists bench_local_first_watermark_stripes (
                      stripe_id smallint not null primary key,
                      committed_seq bigint not null
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_local_first_watermark_stripes");
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_local_first_watermark_stripes(stripe_id, committed_seq)
                    values (?, 0)
                    """)) {
                for (int stripe = 0; stripe < watermarkStripes; stripe++) {
                    insert.setInt(1, stripe);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /**
     * Toggle the impact of the connection policy on primary contrasts by switching to line-by-line or transaction -by- connection.
     *
     * @param dataSource database connection source
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param explicitTransaction explicit transaction
     * @param reuseConnection reuse connection
     * @param shardWatermark shard watermark
     * @param watermarkStripes watermark stripes
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void run(final HikariDataSource dataSource, final int concurrency, final int durationSeconds,
                            final boolean explicitTransaction, final boolean reuseConnection,
                            final boolean shardWatermark, final int watermarkStripes) throws Exception {
        AtomicLong sequence = new AtomicLong(100);
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    try {
                        if (reuseConnection) {
                            try (
                                    Connection connection = dataSource.getConnection();
                                    PreparedStatement insert = insertStatement(connection);
                                    PreparedStatement watermark = shardWatermark
                                            ? watermarkStatement(connection)
                                            : null) {
                                connection.setAutoCommit(!explicitTransaction);
                                ready.countDown();
                                start.await();
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                                while (System.nanoTime() < deadline) {
                                    attempts.increment();
                                    long started = System.nanoTime();
                                    try {
                                        long timestamp = sequence.incrementAndGet();
                                        writeOne(insert, timestamp);
                                        if (watermark != null) {
                                            watermark.setInt(1, (int) (timestamp % watermarkStripes));
                                            if (watermark.executeUpdate() != 1) {
                                                throw new SQLException("native shard watermark changed no row");
                                            }
                                        }
                                        if (explicitTransaction) {
                                            connection.commit();
                                        }
                                        committed.increment();
                                        latencies.add(System.nanoTime() - started);
                                    } catch (final Exception ex) {
                                        failures.increment();
                                        firstFailure.compareAndSet(null,
                                                ex.getClass().getSimpleName() + ":" + ex.getMessage());
                                        if (explicitTransaction) {
                                            connection.rollback();
                                        }
                                    }
                                }
                            }
                        } else {
                            ready.countDown();
                            start.await();
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                            while (System.nanoTime() < deadline) {
                                attempts.increment();
                                long started = System.nanoTime();
                                try (Connection connection = dataSource.getConnection()) {
                                    connection.setAutoCommit(!explicitTransaction);
                                    try (
                                            PreparedStatement insert = insertStatement(connection);
                                            PreparedStatement watermark = shardWatermark
                                                    ? watermarkStatement(connection)
                                                    : null) {
                                        long timestamp = sequence.incrementAndGet();
                                        writeOne(insert, timestamp);
                                        if (watermark != null) {
                                            watermark.setInt(1, (int) (timestamp % watermarkStripes));
                                            if (watermark.executeUpdate() != 1) {
                                                throw new SQLException("native shard watermark changed no row");
                                            }
                                        }
                                        if (explicitTransaction) {
                                            connection.commit();
                                        }
                                    } catch (final Exception ex) {
                                        if (explicitTransaction) {
                                            connection.rollback();
                                        }
                                        throw ex;
                                    }
                                    committed.increment();
                                    latencies.add(System.nanoTime() - started);
                                } catch (final Exception ex) {
                                    failures.increment();
                                    firstFailure.compareAndSet(null,
                                            ex.getClass().getSimpleName() + ":" + ex.getMessage());
                                }
                            }
                        }
                    } catch (final Exception ex) {
                        firstFailure.compareAndSet(null,
                                ex.getClass().getSimpleName() + ":" + ex.getMessage());
                        failures.increment();
                        ready.countDown();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            start.countDown();
            done.await();
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
            double p99Millis = sorted.length == 0 ? Double.NaN
                    : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
            long rows = rowCount(dataSource);
            if (rows != committed.sum()) {
                throw new IllegalStateException("native committed count differs from durable business rows");
            }
            long watermark = watermarkValue(dataSource);
            if (shardWatermark && watermark != committed.sum()) {
                throw new IllegalStateException("native shard watermark differs from durable business rows");
            }
            System.out.printf("LOCAL_FIRST_NATIVE_BENCH concurrency=%d duration_s=%.2f "
                    + "explicit_tx=%s reuse_connection=%s shard_watermark=%s "
                    + "watermark_stripes=%d "
                    + "attempts=%d committed=%d failures=%d "
                    + "commit_tps=%.2f p99_ms=%.2f rows=%d watermark=%d first_failure=%s%n",
                    concurrency, seconds, explicitTransaction, reuseConnection,
                    shardWatermark, watermarkStripes, attempts.sum(), committed.sum(),
                    failures.sum(), committed.sum() / seconds, p99Millis, rows, watermark,
                    firstFailure.get() == null ? "-" : firstFailure.get());
        }
    }

    /**
     * Two connection policies write statements using the same pre-compiled unit key.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static PreparedStatement insertStatement(final Connection connection) throws SQLException {
        return connection.prepareStatement("""
                insert into bench_local_first_native(user_key, payload) values (?, ?)
                """);
    }

    /**
     * Subsection local serial number is the same as business line in InnoDB transaction.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static PreparedStatement watermarkStatement(final Connection connection) throws SQLException {
        return connection.prepareStatement("""
                update bench_local_first_watermark_stripes
                set committed_seq = committed_seq + 1 where stripe_id = ?
                """);
    }

    /**
     * Keeps the same key and value bytes as the protocol benchmark, and requires that you write exactly to a line.
     *
     * @param insert insert
     * @param timestamp timestamp
     * @throws SQLException if the operation cannot complete
     */
    private static void writeOne(final PreparedStatement insert, final long timestamp) throws SQLException {
        insert.setBytes(1, ("local-first:" + timestamp).getBytes(StandardCharsets.UTF_8));
        insert.setBytes(2, ("v" + timestamp).getBytes(StandardCharsets.UTF_8));
        if (insert.executeUpdate() != 1) {
            throw new SQLException("native single-key insert changed no row");
        }
    }

    /**
     * Actual physical business lines as durable of the commit volume for success.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static long rowCount(final HikariDataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection(); Statement query = connection.createStatement();
                ResultSet row = query.executeQuery("select count(*) from bench_local_first_native")) {
            if (!row.next()) {
                throw new IllegalStateException("native row count returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Summarize all durable subsections watermark to verify that each successful commit completed the same transaction advance.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static long watermarkValue(final HikariDataSource dataSource) throws Exception {
        try (
                Connection connection = dataSource.getConnection(); Statement query = connection.createStatement();
                ResultSet row = query.executeQuery("select sum(committed_seq) "
                        + "from bench_local_first_watermark_stripes")) {
            if (!row.next()) {
                throw new IllegalStateException("native shard watermark returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Use only this benchmark specified connection and capacity parameters.
     *
     * @param name name
     * @param fallback fallback
     * @return the operation result
     */
    private static String env(final String name, final String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

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

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * The two real MySQLs each write the same description baseline for fixed key local transaction.
 */
public final class MysqlTwoShardLocalNativeBenchmark {

    /**
     * Benchmark entrance does not keep process global database status.
     */
    private MysqlTwoShardLocalNativeBenchmark() {
    }

    /**
     * Only an exclusive list of visible resets can be used to compare with the environment transaction throughput.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("two-shard native baseline requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "30"));
        if (concurrency < 2 || concurrency > 512 || seconds < 1 || seconds > 600) {
            throw new IllegalArgumentException("invalid native local two-shard limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (
                HikariDataSource first = pool(env("AZIR_TX_SHARD1_JDBC",
                        "jdbc:mysql://127.0.0.1:23307/shard_1"), username, password);
                HikariDataSource second = pool(env("AZIR_TX_SHARD2_JDBC",
                        "jdbc:mysql://127.0.0.1:23308/shard_2"), username, password)) {
            initialize(first);
            initialize(second);
            run(first, second, concurrency, seconds);
        }
    }

    /**
     * Follow product foreground connection pool budget and driver configuration.
     *
     * @param url url
     * @param user user
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource pool(final String url, final String user, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, user, password);
    }

    /**
     * Only the benchmark exclusive table is empty, and no shard MVCC or control library tables are modified.
     *
     * @param source database connection source
     * @throws SQLException if the operation cannot complete
     */
    private static void initialize(final HikariDataSource source) throws SQLException {
        try (Connection connection = source.getConnection(); Statement sql = connection.createStatement()) {
            sql.execute("""
                    create table if not exists bench_native_local_fixed (
                      user_key varbinary(256) not null primary key,
                      payload blob not null
                    ) engine=InnoDB
                    """);
            sql.executeUpdate("delete from bench_native_local_fixed");
        }
    }

    /**
     * 50 stand-alone client by the odd path in two slices, each repeating UPSERT with the same key.
     *
     * @param first first
     * @param second second
     * @param concurrency concurrency
     * @param seconds seconds
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void run(final HikariDataSource first, final HikariDataSource second,
                            final int concurrency, final int seconds) throws Exception {
        AtomicLong ids = new AtomicLong(100);
        LongAdder[] committed = {new LongAdder(), new LongAdder()};
        LongAdder failures = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(concurrency);
        AtomicLong deadline = new AtomicLong();
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                int shard = index & 1;
                HikariDataSource source = shard == 0 ? first : second;
                byte[] key = ("local-first:worker:" + index).getBytes(StandardCharsets.UTF_8);
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        while (System.nanoTime() < deadline.get()) {
                            long began = System.nanoTime();
                            long id = ids.incrementAndGet();
                            try (Connection connection = source.getConnection()) {
                                connection.setAutoCommit(false);
                                try (PreparedStatement write = connection.prepareStatement("""
                                        insert into bench_native_local_fixed(user_key, payload)
                                        values (?, ?) on duplicate key update payload = values(payload)
                                        """)) {
                                    write.setBytes(1, key);
                                    write.setBytes(2, ("v" + id).getBytes(StandardCharsets.UTF_8));
                                    write.executeUpdate();
                                    connection.commit();
                                    committed[shard].increment();
                                    latencies.add(System.nanoTime() - began);
                                } catch (final SQLException ex) {
                                    connection.rollback();
                                    throw ex;
                                } finally {
                                    connection.setAutoCommit(true);
                                }
                            } catch (final Exception ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null,
                                        ex.getClass().getSimpleName() + ':' + ex.getMessage());
                            }
                        }
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.countDown();
                    }
                });
            }
            if (!ready.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("native local workers did not become ready");
            }
            long began = System.nanoTime();
            deadline.set(began + TimeUnit.SECONDS.toNanos(seconds));
            start.countDown();
            if (!finished.await(seconds + 60L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("native local workers did not finish");
            }
            double elapsed = (System.nanoTime() - began) / 1_000_000_000.0;
            latencies.sort(Comparator.naturalOrder());
            long total = committed[0].sum() + committed[1].sum();
            long p99 = latencies.isEmpty() ? 0
                    : latencies.get(Math.min(latencies.size() - 1,
                            (int) Math.ceil(latencies.size() * 0.99) - 1));
            long leftRows = rowCount(first);
            long rightRows = rowCount(second);
            System.out.printf("NATIVE_TWO_LOCAL_BENCH concurrency=%d duration_s=%.2f "
                    + "committed=%d tps=%.2f p99_ms=%.3f shard_tps=%.2f/%.2f "
                    + "rows=%d/%d failures=%d first_failure=%s%n",
                    concurrency, elapsed, total, total / elapsed, p99 / 1_000_000.0,
                    committed[0].sum() / elapsed, committed[1].sum() / elapsed,
                    leftRows, rightRows, failures.sum(),
                    firstFailure.get() == null ? "-" : firstFailure.get());
            if (failures.sum() != 0 || total == 0
                    || leftRows != (concurrency + 1) / 2 || rightRows != concurrency / 2) {
                throw new IllegalStateException("native local two-shard baseline is invalid");
            }
        }
    }

    /**
     * Each piece ends with only one business line of its own for each independent client.
     *
     * @param source database connection source
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long rowCount(final HikariDataSource source) throws SQLException {
        try (
                Connection connection = source.getConnection();
                PreparedStatement query = connection.prepareStatement("""
                        select count(*) from bench_native_local_fixed
                        """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("native local row count returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Test parameters are read only from a visible environment variable or an isolation default.
     *
     * @param name name
     * @param defaultValue default value
     * @return the operation result
     */
    private static String env(final String name, final String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

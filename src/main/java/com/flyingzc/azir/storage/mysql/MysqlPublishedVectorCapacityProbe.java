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

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Measures the delay of local writing ACK throughput to RC reading center after the durable vector group is released.
 */
public final class MysqlPublishedVectorCapacityProbe {

    /**
     * Isolating probe capacity does not save the global configuration.
     */
    private MysqlPublishedVectorCapacityProbe() {
    }

    /**
     * The repositioning of the three laboratories must be allowed in a visible manner to avoid the capacity program touching the product list.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("published vector probe requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        boolean volatileExperiment = Boolean.parseBoolean(env("AZIR_TX_VECTOR_VOLATILE_EXPERIMENT", "false"));
        boolean remoteExperiment = Boolean.parseBoolean(env("AZIR_TX_VECTOR_REMOTE_EXPERIMENT", "false"));
        boolean tcpExperiment = Boolean.parseBoolean(env("AZIR_TX_VECTOR_TCP_EXPERIMENT", "false"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 120
                || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid published vector capacity limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (
                HikariDataSource center = pool(env("AZIR_TX_CONTROL_JDBC",
                        "jdbc:mysql://127.0.0.1:23306/control"), username, password);
                HikariDataSource left = pool(env("AZIR_TX_SHARD1_JDBC",
                        "jdbc:mysql://127.0.0.1:23306/shard_1"), username, password);
                HikariDataSource right = pool(env("AZIR_TX_SHARD2_JDBC",
                        "jdbc:mysql://127.0.0.1:23306/shard_2"), username, password)) {
            MysqlStripedCrossVersionPrototype.initializeIsolated(left, stripes);
            MysqlStripedCrossVersionPrototype.initializeIsolated(right, stripes);
            Map<String, DataSource> routes = Map.of("s1", left, "s2", right);
            if (tcpExperiment) {
                int port = Integer.parseInt(env("AZIR_TX_VECTOR_TCP_PORT", "23309"));
                var leases = new MysqlVectorCutLeaderLease(center, routes, stripes);
                leases.initializeIsolated();
                try (
                        var runtime = new MysqlVectorCutLeaderRuntime(center, routes, stripes,
                                () -> 0, leases, "127.0.0.1:" + port, 5_000, 200);
                        var server = new MysqlVectorCutTcpServer(runtime, port)) {
                    if (!runtime.start()) {
                        throw new IllegalStateException("TCP vector cut benchmark found another leader");
                    }
                    server.start();
                    try (var remote = new MysqlVectorCutTcpClient(center, routes, stripes)) {
                        run(null, runtime, null, remote, left, right, concurrency, durationSeconds, stripes,
                                "LEASED_TCP");
                    }
                }
            } else if (remoteExperiment) {
                int port = Integer.parseInt(env("AZIR_TX_VECTOR_HTTP_PORT", "23308"));
                var leases = new MysqlVectorCutLeaderLease(center, routes, stripes);
                leases.initializeIsolated();
                try (
                        var runtime = new MysqlVectorCutLeaderRuntime(center, routes, stripes,
                                () -> 0, leases, "127.0.0.1:" + port, 5_000, 200);
                        var server = new MysqlVectorCutHttpServer(runtime, port)) {
                    if (!runtime.start()) {
                        throw new IllegalStateException("remote vector cut benchmark found another leader");
                    }
                    server.start();
                    var remote = new MysqlVectorCutRemoteClient(center, routes, stripes);
                    run(null, runtime, remote, null, left, right, concurrency, durationSeconds, stripes,
                            "LEASED_REMOTE");
                }
            } else {
                try (
                        var cut = volatileExperiment
                                ? MysqlPublishedVectorCut.experimentalVolatile(center, routes, stripes, () -> 0)
                                : new MysqlPublishedVectorCut(center, routes, stripes, () -> 0)) {
                    if (volatileExperiment) {
                        cut.initializeVolatile();
                    } else {
                        cut.initializeIsolated();
                    }
                    run(cut, null, null, null, left, right, concurrency, durationSeconds, stripes,
                            volatileExperiment ? "VOLATILE_EXPERIMENT" : "DURABLE_CENTER");
                }
            }
        }
    }

    /**
     * Volume probe only returns the connection pool configuration of the same code library.
     *
     * @param url url
     * @param username username
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource pool(final String url, final String username, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /**
     * Each of the two runs shard transaction and the publisher is updated to confirm multiple commit s at the same time.
     *
     * @param cut cut
     * @param runtime runtime
     * @param remote remote
     * @param tcpRemote tcp remote
     * @param left left
     * @param right right
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param stripes stripes
     * @param mode mode
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void run(final MysqlPublishedVectorCut cut, final MysqlVectorCutLeaderRuntime runtime,
                            final MysqlVectorCutRemoteClient remote, final MysqlVectorCutTcpClient tcpRemote,
                            final HikariDataSource left,
                            final HikariDataSource right, final int concurrency,
                            final int durationSeconds, final int stripes, final String mode) throws Exception {
        AtomicLong sequence = new AtomicLong(100);
        LongAdder attempts = new LongAdder();
        LongAdder physical = new LongAdder();
        LongAdder acked = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder cutReads = new LongAdder();
        LongAdder publishCycles = new LongAdder();
        LongAdder publishUpdates = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> ackLatencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> readLatencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> publishLatencies = new ConcurrentLinkedQueue<>();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (
                var background = Executors.newVirtualThreadPerTaskExecutor();
                var workers = Executors.newFixedThreadPool(concurrency)) {
            Future<?> publisher = null;
            if (cut != null) {
                publisher = background.submit(() -> {
                    try {
                        start.await();
                        var previous = cut.currentCut();
                        while (running.get()) {
                            long began = System.nanoTime();
                            var published = cut.publishOnce();
                            publishLatencies.add(System.nanoTime() - began);
                            publishCycles.increment();
                            if (!published.equals(previous)) {
                                publishUpdates.increment();
                                previous = published;
                            }
                            LockSupport.parkNanos(200_000);
                        }
                    } catch (final Exception ex) {
                        firstFailure.compareAndSet(null, "publisher:" + ex);
                        running.set(false);
                    }
                });
            }
            var reader = background.submit(() -> {
                try {
                    start.await();
                    while (running.get()) {
                        long began = System.nanoTime();
                        if (tcpRemote != null) {
                            tcpRemote.currentView();
                        } else if (remote == null) {
                            cut.currentCut();
                        } else {
                            remote.currentView();
                        }
                        readLatencies.add(System.nanoTime() - began);
                        cutReads.increment();
                        LockSupport.parkNanos(500_000);
                    }
                } catch (final Exception ex) {
                    firstFailure.compareAndSet(null, "reader:" + ex);
                    running.set(false);
                }
            });
            for (int index = 0; index < concurrency; index++) {
                final String shardId = index % 2 == 0 ? "s1" : "s2";
                final HikariDataSource dataSource = index % 2 == 0 ? left : right;
                workers.submit(() -> {
                    boolean readySignaled = false;
                    try (
                            Connection connection = dataSource.getConnection();
                            PreparedStatement call = connection.prepareStatement("""
                                    call bench_striped_mvcc_commit_checked_v2(?, ?, ?, ?)
                                    """)) {
                        ready.countDown();
                        readySignaled = true;
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                        while (System.nanoTime() < deadline && running.get()) {
                            attempts.increment();
                            long began = System.nanoTime();
                            long id = sequence.incrementAndGet();
                            int stripe = (int) (id % stripes);
                            try {
                                call.setBytes(1, ("local-first:" + id).getBytes(StandardCharsets.UTF_8));
                                call.setBytes(2, ("v" + id).getBytes(StandardCharsets.UTF_8));
                                call.setInt(3, stripe);
                                call.setLong(4, 0);
                                long localSeq;
                                try (ResultSet row = call.executeQuery()) {
                                    if (!row.next()) {
                                        throw new SQLException("checked local commit returned no sequence");
                                    }
                                    localSeq = row.getLong(1);
                                }
                                physical.increment();
                                if (runtime == null) {
                                    cut.awaitLocallyPublished(shardId, stripe, localSeq, 0, 5_000);
                                } else {
                                    runtime.awaitPublished(shardId, stripe, localSeq, 0, 5_000);
                                }
                                acked.increment();
                                ackLatencies.add(System.nanoTime() - began);
                            } catch (final Exception ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null, "writer:" + ex);
                                if (runtime != null && !runtime.isServing()) {
                                    running.set(false);
                                }
                            }
                        }
                    } catch (final Exception ex) {
                        failures.increment();
                        firstFailure.compareAndSet(null, "writer-setup:" + ex);
                    } finally {
                        if (!readySignaled) {
                            ready.countDown();
                        }
                        done.countDown();
                    }
                });
            }
            ready.await();
            long began = System.nanoTime();
            start.countDown();
            done.await();
            double seconds = (System.nanoTime() - began) / 1_000_000_000.0;
            running.set(false);
            if (publisher != null) {
                publisher.get();
            }
            reader.get();
            long[] persistedLeft = persistedCounts(left);
            long[] persistedRight = persistedCounts(right);
            long persisted = persistedLeft[0] + persistedRight[0];
            long watermarks = persistedLeft[1] + persistedRight[1];
            if (persisted != physical.sum() || watermarks != physical.sum()
                    || physical.sum() != acked.sum() || failures.sum() != 0
                    || firstFailure.get() != null) {
                throw new IllegalStateException("published vector probe lost a commit or failed: "
                        + firstFailure.get() + ", physical=" + physical.sum() + ", acked=" + acked.sum()
                        + ", persisted=" + persisted + ", watermarks=" + watermarks);
            }
            System.out.printf("PUBLISHED_VECTOR_CAPACITY mode=%s concurrency=%d duration_s=%.2f "
                    + "attempts=%d physical=%d acked=%d failures=%d ack_tps=%.2f "
                    + "ack_p99_ms=%.3f cut_reads=%d cut_avg_ms=%.3f cut_p99_ms=%.3f "
                    + "publish_cycles=%d publish_updates=%d publish_avg_ms=%.3f "
                    + "publish_p99_ms=%.3f sample_avg_ms=%.3f center_write_avg_ms=%.3f "
                    + "versions=%d watermark_total=%d%n",
                    mode,
                    concurrency, seconds, attempts.sum(), physical.sum(), acked.sum(), failures.sum(),
                    acked.sum() / seconds, p99(ackLatencies), cutReads.sum(), average(readLatencies),
                    p99(readLatencies), publishCycles.sum(), publishUpdates.sum(),
                    average(publishLatencies), p99(publishLatencies),
                    cut == null ? Double.NaN : cut.sampleAverageMillis(),
                    cut == null ? Double.NaN : cut.centerWriteAverageMillis(), persisted, watermarks);
        }
    }

    /**
     * Shard version and watermark increments must equal all physical success transaction.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long[] persistedCounts(final HikariDataSource dataSource) throws SQLException {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement("""
                        select (select count(*) from bench_striped_mvcc_data) as versions,
                               (select sum(committed_seq) from bench_striped_mvcc_state) as watermarks
                        """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("published vector persistence count returned no row");
            }
            return new long[]{row.getLong("versions"), row.getLong("watermarks")};
        }
    }

    /**
     * Calculates the average of milliseconds in a single capacity probe that actually completes the operation.
     *
     * @param values values
     * @return the operation result
     */
    private static double average(final ConcurrentLinkedQueue<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(Double.NaN) / 1_000_000.0;
    }

    /**
     * Based on actual sample P99, connection pool is not needed for waiting or external shutdown time.
     *
     * @param values values
     * @return the operation result
     */
    private static double p99(final ConcurrentLinkedQueue<Long> values) {
        long[] sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
        return sorted.length == 0 ? Double.NaN
                : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
    }

    /**
     * Read-only takes the probe visible parameter and does not use the product default environment.
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

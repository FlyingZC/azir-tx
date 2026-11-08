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
import com.flyingzc.azir.common.TxStatus;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Primary shard Single registration prewrite, secondary prewrite, main decision, cut face and bounded materialization quarantine capacity probe.
 */
public final class MysqlPrimaryDecisionCapacityProbe {

    /**
     * Volume probe does not save global running state.
     */
    private MysqlPrimaryDecisionCapacityProbe() {
    }

    /**
     * A full writing experiment with limited concurrent and limited duration after the visible reset-separation database.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("primary decision probe requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int backgroundWorkers = Integer.parseInt(env("AZIR_TX_PRIMARY_BACKGROUND_WORKERS", "8"));
        int backlogLimit = Integer.parseInt(env("AZIR_TX_PRIMARY_BACKLOG_LIMIT", "1000"));
        if (concurrency < 1 || concurrency > 200 || seconds < 1 || seconds > 600
                || stripes < 1 || stripes > 256 || backgroundWorkers < 1
                || backgroundWorkers > 64 || backlogLimit < 1 || backlogLimit > 100_000) {
            throw new IllegalArgumentException("invalid primary decision probe limits");
        }
        String user = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (
                HikariDataSource primary = pool(env("AZIR_TX_SHARD1_JDBC",
                        "jdbc:mysql://127.0.0.1:23307/shard_1"), user, password);
                HikariDataSource secondary = pool(env("AZIR_TX_SHARD2_JDBC",
                        "jdbc:mysql://127.0.0.1:23308/shard_2"), user, password)) {
            MysqlStripedCrossVersionPrototype.initializeIsolated(primary, stripes);
            MysqlStripedCrossVersionPrototype.initializeIsolated(secondary, stripes);
            var decisions = new MysqlPrimaryDecisionPrototype(primary, secondary, stripes);
            decisions.initializeIsolated();
            decisions.resetIsolated();
            try (
                    var reader = new MysqlPrimaryVectorReadPrototype(
                            "s1", primary, "s2", secondary, stripes, decisions)) {
                run(primary, secondary, decisions, reader, concurrency, seconds,
                        stripes, backgroundWorkers, backlogLimit);
            }
        }
    }

    /**
     * Same JDBC pool configuration as current product capacity probe.
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
     * All clients apply for transaction ID, background on a continuous materialization basis and line capacity to form a clear back pressure.
     *
     * @param primary original Percolator primary identity
     * @param secondary secondary
     * @param decisions decisions
     * @param reader reader
     * @param concurrency concurrency
     * @param seconds seconds
     * @param stripes stripes
     * @param backgroundWorkers background workers
     * @param backlogLimit backlog limit
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void run(final HikariDataSource primary, final HikariDataSource secondary,
                            final MysqlPrimaryDecisionPrototype decisions,
                            final MysqlPrimaryVectorReadPrototype reader,
                            final int concurrency, final int seconds, final int stripes,
                            final int backgroundWorkers, final int backlogLimit) throws Exception {
        ArrayBlockingQueue<Committed> backlog = new ArrayBlockingQueue<>(backlogLimit);
        AtomicLong nextId = new AtomicLong(1);
        LongAdder acknowledged = new LongAdder();
        LongAdder materialized = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder prewriteNanos = new LongAdder();
        LongAdder decisionNanos = new LongAdder();
        LongAdder cutNanos = new LongAdder();
        AtomicInteger highWater = new AtomicInteger();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicBoolean producersRunning = new AtomicBoolean(true);
        List<Long> ackLatencies = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch frontDone = new CountDownLatch(concurrency);
        try (
                var clients = Executors.newFixedThreadPool(concurrency);
                var recovery = Executors.newFixedThreadPool(backgroundWorkers)) {
            for (int worker = 0; worker < backgroundWorkers; worker++) {
                recovery.submit(() -> {
                    while (producersRunning.get() || !backlog.isEmpty()) {
                        try {
                            Committed committed = backlog.poll(100, TimeUnit.MILLISECONDS);
                            if (committed == null) {
                                continue;
                            }
                            MysqlStripedCrossVersionPrototype.materialize(primary,
                                    committed.primaryKey(), committed.txId(),
                                    committed.decisionSequence(), tx -> committedSequence(decisions, tx));
                            MysqlStripedCrossVersionPrototype.materialize(secondary,
                                    committed.secondaryKey(), committed.txId(),
                                    committed.decisionSequence(), tx -> committedSequence(decisions, tx));
                            materialized.increment();
                        } catch (final Throwable failure) {
                            failures.increment();
                            firstFailure.compareAndSet(null, failure);
                        }
                    }
                });
            }
            long scheduledEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            for (int worker = 0; worker < concurrency; worker++) {
                clients.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        while (System.nanoTime() < scheduledEnd && firstFailure.get() == null) {
                            long began = System.nanoTime();
                            long txId = nextId.getAndIncrement();
                            int primaryStripe = Math.floorMod(txId, stripes);
                            int secondaryStripe = (primaryStripe + 1) % stripes;
                            int decisionStripe = (primaryStripe + 2) % stripes;
                            String primaryKey = "primary:" + txId;
                            String secondaryKey = "secondary:" + txId;
                            byte[] value = Long.toString(txId).getBytes(StandardCharsets.UTF_8);
                            var registration = new MysqlPrimaryDecisionPrototype.Registration(
                                    txId, primaryKey, value, secondaryKey, value,
                                    primaryStripe, secondaryStripe, 0, 0,
                                    System.currentTimeMillis() + 60_000, decisionStripe);
                            long stage = System.nanoTime();
                            // The main durable identity must precede the secondary prewrite to avoid orphan intent after collapse.
                            long primarySequence = decisions.registerAndPrewritePrimary(registration);
                            long secondarySequence = MysqlStripedCrossVersionPrototype.prewrite(
                                    secondary, secondaryKey, value, secondaryStripe, txId, 0);
                            prewriteNanos.add(System.nanoTime() - stage);
                            stage = System.nanoTime();
                            var decision = decisions.commit(registration);
                            decisionNanos.add(System.nanoTime() - stage);
                            if (decision.primarySequence() != primarySequence
                                    || decision.secondarySequence() != secondarySequence) {
                                throw new IllegalStateException("primary decision changed participant proof");
                            }
                            stage = System.nanoTime();
                            var cut = reader.captureUnpublished();
                            cutNanos.add(System.nanoTime() - stage);
                            if (cut.decisionSequence(decisionStripe) < decision.decisionSequence()
                                    || cut.dataSequence("s1", primaryStripe) < primarySequence
                                    || cut.dataSequence("s2", secondaryStripe) < secondarySequence) {
                                throw new IllegalStateException("primary decision cut does not cover ACK");
                            }
                            if (!backlog.offer(new Committed(txId, primaryKey,
                                    secondaryKey, decision.decisionSequence()), 2, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("primary decision materialization backlog did not drain");
                            }
                            highWater.accumulateAndGet(backlog.size(), Math::max);
                            acknowledged.increment();
                            ackLatencies.add(System.nanoTime() - began);
                        }
                    } catch (final Throwable failure) {
                        failures.increment();
                        firstFailure.compareAndSet(null, failure);
                    } finally {
                        frontDone.countDown();
                    }
                });
            }
            if (!ready.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("primary decision clients did not become ready");
            }
            long measuredStart = System.nanoTime();
            start.countDown();
            if (!frontDone.await(seconds + 60L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("primary decision clients did not finish");
            }
            long measuredEnd = System.nanoTime();
            int pendingAtStop = backlog.size();
            producersRunning.set(false);
            recovery.shutdown();
            if (!recovery.awaitTermination(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("primary decision materialization did not drain");
            }
            double drainSeconds = (System.nanoTime() - measuredEnd) / 1_000_000_000.0;
            ackLatencies.sort(Comparator.naturalOrder());
            long count = acknowledged.sum();
            double elapsed = (measuredEnd - measuredStart) / 1_000_000_000.0;
            long p99 = ackLatencies.isEmpty() ? 0
                    : ackLatencies.get(Math.min(ackLatencies.size() - 1,
                            (int) Math.ceil(ackLatencies.size() * 0.99) - 1));
            long durableDecisions = count(primary,
                    "select count(*) from bench_primary_txn_v2 where status = 'COMMITTED'");
            long durableDecisionSequence = count(primary,
                    "select coalesce(sum(committed_seq), 0) from bench_primary_decision_state_v1");
            long primaryVersions = count(primary,
                    "select count(*) from bench_striped_mvcc_cross_data");
            long secondaryVersions = count(secondary,
                    "select count(*) from bench_striped_mvcc_cross_data");
            System.out.printf("primary decision: clients=%d stripes=%d bg_workers=%d seconds=%.3f ack=%d "
                    + "tps=%.1f p99_ms=%.3f materialized=%d backlog_max=%d "
                    + "backlog_at_stop=%d backlog_final=%d drain_seconds=%.3f "
                    + "durable_decisions=%d decision_seq_sum=%d versions=%d/%d "
                    + "failures=%d prewrite_avg_ms=%.3f "
                    + "decision_avg_ms=%.3f cut_avg_ms=%.3f%n",
                    concurrency, stripes, backgroundWorkers, elapsed, count,
                    count / elapsed, p99 / 1_000_000.0,
                    materialized.sum(), highWater.get(), pendingAtStop,
                    backlog.size(), drainSeconds, durableDecisions,
                    durableDecisionSequence, primaryVersions, secondaryVersions,
                    failures.sum(),
                    averageMillis(prewriteNanos.sum(), count),
                    averageMillis(decisionNanos.sum(), count),
                    averageMillis(cutNanos.sum(), count));
            if (firstFailure.get() != null || count != materialized.sum()
                    || count != durableDecisions || count != durableDecisionSequence
                    || count != primaryVersions || count != secondaryVersions
                    || !backlog.isEmpty()) {
                throw new IllegalStateException("primary decision capacity result is invalid", firstFailure.get());
            }
        }
    }

    /**
     * Materialization accepts the same decision serial number only for primary shard durable COMMITTED.
     *
     * @param decisions decisions
     * @param txId transaction identifier
     * @return the operation result
     */
    private static OptionalLong committedSequence(final MysqlPrimaryDecisionPrototype decisions, final long txId) {
        var decision = decisions.decision(txId).orElseThrow();
        return decision.status() == TxStatus.COMMITTED
                ? OptionalLong.of(decision.decisionSequence())
                : OptionalLong.empty();
    }

    /**
     * Reads the final line number from the physical table to prevent background task count from covering the actual durable storage gap.
     *
     * @param dataSource database connection source
     * @param sql sql
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long count(final HikariDataSource dataSource, final String sql) throws SQLException {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement(sql);
                ResultSet result = query.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("primary decision count query returned no row");
            }
            return result.getLong(1);
        }
    }

    /**
     * The cumulative period of the phase is converted to an average millisecond for each ACK number.
     *
     * @param totalNanos total nanos
     * @param count count
     * @return the operation result
     */
    private static double averageMillis(final long totalNanos, final long count) {
        return count == 0 ? 0 : totalNanos / 1_000_000.0 / count;
    }

    /**
     * Test parameters are derived from environmental variables and use the quarantine test default when missing.
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
     * Only transaction identity of already ACK is saved for each background job.
     *
     * @param txId transaction identifier
     * @param primaryKey primary key
     * @param secondaryKey secondary key
     * @param decisionSequence decision sequence
     */
    private record Committed(long txId, String primaryKey, String secondaryKey, long decisionSequence) {
    }
}

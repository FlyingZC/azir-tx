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
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.mysql.MysqlLocalFirstMetadataView;
import com.flyingzc.azir.publication.LocalFirstIntentRecoveryManager;
import com.flyingzc.azir.publication.LocalFirstPublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.LocalFirstPublicationRecoveryManager;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.IntentCommitRequest;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.IntentRegistrationRequest;
import com.flyingzc.azir.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Tri-Court Isolation Capacity Experiment, with cross-shard prewrite, Center batch decision, release confirmation and parallel background materialization.
 */
public final class MysqlLocalFirstCrossShardProtocolBenchmark {

    /**
     * The benchmark entry point keeps no global mutable state.
     */
    private MysqlLocalFirstCrossShardProtocolBenchmark() {
    }

    /**
     * Runs a real two shard transaction with only visible reset and reports an background materialization backlog.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("cross-shard protocol benchmark requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        boolean vectorMode = Boolean.parseBoolean(env("AZIR_TX_VECTOR_CROSS", "false"));
        boolean vectorJavaPrewrite = Boolean.parseBoolean(env("AZIR_TX_VECTOR_PREWRITE_JAVA_FALLBACK", "false"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int prewriteGroupSize = Integer.parseInt(env("AZIR_TX_VECTOR_PREWRITE_COALESCE_BATCH", "0"));
        int prewriteLanes = Integer.parseInt(env("AZIR_TX_VECTOR_PREWRITE_BATCH_LANES", "4"));
        long prewriteWaitMicros = Long.parseLong(env("AZIR_TX_VECTOR_PREWRITE_BATCH_WAIT_MICROS", "500"));
        long recoveryParkMicros = Long.parseLong(env("AZIR_TX_INTENT_RECOVERY_PARK_MICROS", "20000"));
        long maxBacklog = Long.parseLong(env("AZIR_TX_INTENT_MAX_BACKLOG", "0"));
        if (concurrency < 1 || concurrency > 200 || seconds < 1 || seconds > 120
                || maxBacklog < 0 || maxBacklog > 1_000_000
                || stripes < 1 || stripes > 256
                || prewriteGroupSize < 0 || prewriteGroupSize > 64
                || prewriteGroupSize > 0 && !vectorMode
                || prewriteLanes < 1 || prewriteLanes > 32
                || prewriteWaitMicros < 0 || prewriteWaitMicros > 10_000
                || recoveryParkMicros < 0 || recoveryParkMicros > 1_000_000) {
            throw new IllegalArgumentException("invalid cross-shard benchmark limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        String controlUrl = env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        String leftUrl = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String rightUrl = env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2");
        long retentionMillis = Long.parseLong(env("AZIR_TX_INTENT_RETENTION_MS", "5000"));
        try (
                HikariDataSource controlData = pool(controlUrl, username, password);
                HikariDataSource leftData = pool(leftUrl, username, password);
                HikariDataSource rightData = pool(rightUrl, username, password);
                HikariDataSource backgroundControlData = backgroundPool(controlUrl, username, password);
                HikariDataSource backgroundLeftData = backgroundPool(leftUrl, username, password);
                HikariDataSource backgroundRightData = backgroundPool(rightUrl, username, password)) {
            MysqlDatabaseAuthority.requireIndependentSameDatabase(controlData, backgroundControlData, "control");
            MysqlDatabaseAuthority.requireIndependentSameDatabase(leftData, backgroundLeftData, "s1");
            MysqlDatabaseAuthority.requireIndependentSameDatabase(rightData, backgroundRightData, "s2");
            reset(controlData, leftData, rightData);
            var ledger = new MysqlLocalFirstPublicationLedger(controlData);
            ledger.initializeIsolated(100);
            var view = new MysqlLocalFirstMetadataView(ledger);
            var left = new MysqlStorageAdapter("s1", leftData, view);
            var right = new MysqlStorageAdapter("s2", rightData, view);
            left.initialize();
            right.initialize();
            if (vectorMode) {
                left.initializeVectorProtocol(stripes);
                right.initializeVectorProtocol(stripes);
            }
            var leftFence = new MysqlShardSegmentFence("s1", leftData);
            var rightFence = new MysqlShardSegmentFence("s2", rightData);
            leftFence.initializeIsolated();
            rightFence.initializeIsolated();
            var backgroundLedger = new MysqlLocalFirstPublicationLedger(backgroundControlData);
            backgroundLedger.requireInitializedFastPath();
            var backgroundView = new MysqlLocalFirstMetadataView(backgroundLedger);
            var backgroundLeft = new MysqlStorageAdapter("s1", backgroundLeftData, backgroundView);
            var backgroundRight = new MysqlStorageAdapter("s2", backgroundRightData, backgroundView);
            backgroundLeft.requireInitializedFastPath();
            backgroundRight.requireInitializedFastPath();
            if (vectorMode) {
                backgroundLeft.initializeVectorProtocol(stripes);
                backgroundRight.initializeVectorProtocol(stripes);
            }
            var backgroundLeftFence = new MysqlShardSegmentFence("s1", backgroundLeftData);
            var backgroundRightFence = new MysqlShardSegmentFence("s2", backgroundRightData);
            backgroundLeftFence.requireInitializedFastPath();
            backgroundRightFence.requireInitializedFastPath();
            int recoveryConcurrency = Integer.parseInt(env("AZIR_TX_INTENT_RECOVERY_CONCURRENCY", "16"));
            var recovery = new LocalFirstIntentRecoveryManager(ledger,
                    Map.of("s1", left, "s2", right), recoveryConcurrency);
            var segmentRecovery = new LocalFirstPublicationRecoveryManager(backgroundLedger,
                    Map.of("s1", backgroundLeftFence, "s2", backgroundRightFence));
            Map<String, DataSource> routes = Map.of("s1", leftData, "s2", rightData);
            var vectorLeases = vectorMode
                    ? MysqlVectorCutLeaderLease.forProduct(controlData, routes, stripes)
                    : null;
            if (vectorLeases != null) {
                vectorLeases.initializeProduct();
            }
            try (
                    var fanout = Executors.newVirtualThreadPerTaskExecutor();
                    var vectorPublisher = vectorMode
                            ? MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, stripes,
                                    ledger::currentPublished, vectorLeases, "cross-benchmark-owner", 5_000, 200)
                            : null;
                    var snapshots = new LocalFirstCoordinatorSnapshots(ledger, 20_000, 60_000, 1_000);
                    var maintenance = new LocalFirstPublicationMaintenanceScheduler(backgroundLedger,
                            Map.of("s1", backgroundLeftFence, "s2", backgroundRightFence),
                            Map.of("s1", backgroundLeft, "s2", backgroundRight),
                            16, 16, 128, 100, 1_000, retentionMillis);
                    var owner = new LocalFirstIntentSegmentOwnerRegistry(ledger, backgroundLedger,
                            snapshots::floorForReservation, 512, 5_000, 1_000, 100);
                    var registrations = new RegistrationBatcher(ledger, 4, vectorMode);
                    var leftPrewrites = prewriteGroupSize > 0
                            ? new MysqlVectorCrossPrewriteBatcher(left, prewriteLanes,
                                    prewriteGroupSize, prewriteWaitMicros, 4096)
                            : null;
                    var rightPrewrites = prewriteGroupSize > 0
                            ? new MysqlVectorCrossPrewriteBatcher(right, prewriteLanes,
                                    prewriteGroupSize, prewriteWaitMicros, 4096)
                            : null;
                    var batcher = new IntentBatcher(ledger, 4, vectorMode)) {
                if (vectorPublisher != null && !vectorPublisher.start()) {
                    throw new IllegalStateException("vector cross benchmark has another active publisher");
                }
                run(ledger, left, right, recovery, segmentRecovery, owner, batcher,
                        registrations, leftPrewrites, rightPrewrites,
                        snapshots, maintenance, controlData, leftData, rightData, fanout,
                        vectorPublisher, vectorMode, vectorJavaPrewrite, stripes,
                        concurrency, seconds, maxBacklog, recoveryParkMicros);
            }
        }
    }

    /**
     * External database URLs can point to different examples, but have to use their own separate libraries in a visible manner.
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
     * A small independent connection pool allows GC not to take foreground and wait for budget.
     *
     * @param url url
     * @param username username
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource backgroundPool(final String url, final String username, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, username, password);
    }

    /**
     * Benchmark clears only the clearly selected quarantine control table and two shard MVCC data.
     *
     * @param control control
     * @param left left
     * @param right right
     * @throws Exception if the operation cannot complete
     */
    private static void reset(final HikariDataSource control, final HikariDataSource left, final HikariDataSource right) throws Exception {
        try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_first_slot_v1");
            statement.execute("drop table if exists mvcc_local_first_txn_v1");
            statement.execute("drop table if exists mvcc_local_first_segment_v1");
            statement.execute("drop table if exists mvcc_local_first_snapshot_v1");
            statement.execute("drop table if exists mvcc_local_first_coordinator_v1");
            statement.execute("drop table if exists mvcc_local_first_state_v1");
            statement.execute("drop table if exists mvcc_vector_leader_v1");
            statement.execute("drop table if exists mvcc_vector_published_cut_v1");
        }
        resetShard(left, "s1");
        resetShard(right, "s2");
    }

    /**
     * Create a standard shard table before removing the version and lock left over from the previous experiment.
     *
     * @param dataSource database connection source
     * @param shardId logical shard identifier
     * @throws Exception if the operation cannot complete
     */
    private static void resetShard(final HikariDataSource dataSource, final String shardId) throws Exception {
        new MysqlStorageAdapter(shardId, dataSource).initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_segment_candidate_v1");
            statement.execute("drop table if exists mvcc_local_segment_fence_v1");
            statement.execute("drop table if exists mvcc_local_segment_state_v1");
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
            statement.execute("drop table if exists mvcc_vector_local_data_v1");
            statement.execute("drop table if exists mvcc_vector_committed_v1");
            statement.execute("drop table if exists mvcc_vector_intent_reservation_v1");
            statement.execute("drop table if exists mvcc_vector_stripe_state_v1");
            statement.execute("drop table if exists mvcc_vector_layout_v1");
        }
    }

    /**
     * Concurrent Real Business Paths, with background plus materialization and respective time phases.
     *
     * @param ledger ledger
     * @param left left
     * @param right right
     * @param recovery recovery
     * @param segmentRecovery segment recovery
     * @param owner owner
     * @param batcher batcher
     * @param registrations registrations
     * @param leftPrewrites left prewrites
     * @param rightPrewrites right prewrites
     * @param snapshots snapshots
     * @param maintenance maintenance
     * @param controlData control data
     * @param leftData left data
     * @param rightData right data
     * @param fanout fanout
     * @param vectorPublisher vector publisher
     * @param vectorMode vector mode
     * @param vectorJavaPrewrite vector java prewrite
     * @param stripes stripes
     * @param concurrency concurrency
     * @param seconds seconds
     * @param maxBacklog max backlog
     * @param recoveryParkMicros recovery park micros
     * @throws Exception if the operation cannot complete
     */
    private static void run(final MysqlLocalFirstPublicationLedger ledger, final MysqlStorageAdapter left,
                            final MysqlStorageAdapter right, final LocalFirstIntentRecoveryManager recovery,
                            final LocalFirstPublicationRecoveryManager segmentRecovery,
                            final LocalFirstIntentSegmentOwnerRegistry owner, final IntentBatcher batcher,
                            final RegistrationBatcher registrations,
                            final MysqlVectorCrossPrewriteBatcher leftPrewrites,
                            final MysqlVectorCrossPrewriteBatcher rightPrewrites,
                            final LocalFirstCoordinatorSnapshots snapshots,
                            final LocalFirstPublicationMaintenanceScheduler maintenance,
                            final HikariDataSource controlData, final HikariDataSource leftData,
                            final HikariDataSource rightData, final ExecutorService fanout,
                            final MysqlVectorCutLeaderRuntime vectorPublisher,
                            final boolean vectorMode, final boolean vectorJavaPrewrite, final int stripes,
                            final int concurrency, final int seconds, final long maxBacklog,
                            final long recoveryParkMicros) throws Exception {
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder materialized = new LongAdder();
        LongAdder recoveryFailures = new LongAdder();
        LongAdder sealedSegments = new LongAdder();
        LongAdder segmentFailures = new LongAdder();
        LongAdder beginNanos = new LongAdder();
        LongAdder registerNanos = new LongAdder();
        LongAdder prewriteNanos = new LongAdder();
        LongAdder centerNanos = new LongAdder();
        LongAdder publishNanos = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        AtomicBoolean recovering = new AtomicBoolean(true);
        AtomicBoolean collecting = new AtomicBoolean(true);
        Thread recoveryThread = Thread.ofVirtual().name("mvcc-intent-recovery").start(() -> {
            while (recovering.get()) {
                try {
                    var result = recovery.recoverOnce(64);
                    materialized.add(result.finalized());
                    recoveryFailures.add(result.failures());
                } catch (final RuntimeException ex) {
                    firstFailure.compareAndSet(null, "recovery:" + ex.getMessage());
                }
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(recoveryParkMicros));
            }
        });
        Thread segmentThread = Thread.ofVirtual().name("mvcc-intent-segment-recovery").start(() -> {
            while (recovering.get()) {
                try {
                    var result = segmentRecovery.recoverOnce(16, 16);
                    sealedSegments.add(result.sealedSegments());
                    segmentFailures.add(result.failures());
                    if (result.failures() > 0) {
                        firstFailure.compareAndSet(null, "segment:" + segmentRecovery.lastFailure());
                    }
                } catch (final RuntimeException ex) {
                    segmentFailures.increment();
                    firstFailure.compareAndSet(null, "segment:" + ex.getMessage());
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        });
        Thread gcThread = Thread.ofVirtual().name("mvcc-intent-gc").start(() -> {
            while (collecting.get()) {
                try {
                    if (!vectorMode) {
                        maintenance.gcOnce();
                    }
                } catch (final RuntimeException ex) {
                    firstFailure.compareAndSet(null, "gc:" + ex.getMessage());
                }
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        });
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
                        while (System.nanoTime() < end) {
                            // When only the background sustainability rate is measured, coordinator suspends the new transaction before the backlog ceiling.
                            while (maxBacklog > 0 && committed.sum() - materialized.sum() >= maxBacklog
                                    && System.nanoTime() < end) {
                                LockSupport.parkNanos(100_000);
                            }
                            if (System.nanoTime() >= end) {
                                break;
                            }
                            BeginReservation tx = null;
                            LocalFirstIntentSegmentOwnerRegistry.Assignment owned = null;
                            try {
                                long stage = System.nanoTime();
                                tx = snapshots.begin(IsolationLevel.REPEATABLE_READ);
                                beginNanos.add(System.nanoTime() - stage);
                                stage = System.nanoTime();
                                registrations.register(new IntentRegistrationRequest(tx.txId(), tx.startTs(),
                                        tx.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                                        Set.of("s1", "s2")));
                                registerNanos.add(System.nanoTime() - stage);
                                var current = tx;
                                int stripe = Math.floorMod(current.txId(), stripes);
                                Mutation leftWrite = Mutation.put("cross:" + current.txId() + ":left",
                                        Long.toString(current.txId()).getBytes(StandardCharsets.UTF_8));
                                Mutation rightWrite = Mutation.put("cross:" + current.txId() + ":right",
                                        Long.toString(current.txId()).getBytes(StandardCharsets.UTF_8));
                                CompletableFuture<MysqlStorageAdapter.PrewriteProof> first;
                                CompletableFuture<MysqlStorageAdapter.PrewriteProof> second;
                                if (leftPrewrites != null) {
                                    long expiry = System.currentTimeMillis() + 60_000;
                                    first = leftPrewrites.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                                            current.txId(), current.startTs(), expiry,
                                            leftWrite, stripe, 0), 5_000);
                                    second = rightPrewrites.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                                            current.txId(), current.startTs(), expiry,
                                            rightWrite, stripe, 0), 5_000);
                                } else {
                                    first = CompletableFuture.supplyAsync(() -> {
                                        if (!vectorMode) {
                                            return left.prewriteWithProof(current.txId(), current.startTs(),
                                                    List.of(leftWrite));
                                        }
                                        return vectorJavaPrewrite
                                                ? left.prewriteWithVectorProofJavaForProbe(current.txId(),
                                                        current.startTs(), leftWrite, stripe, 0)
                                                : left.prewriteWithVectorProof(current.txId(),
                                                        current.startTs(), leftWrite, stripe, 0);
                                    }, fanout);
                                    second = CompletableFuture.supplyAsync(() -> {
                                        if (!vectorMode) {
                                            return right.prewriteWithProof(current.txId(), current.startTs(),
                                                    List.of(rightWrite));
                                        }
                                        return vectorJavaPrewrite
                                                ? right.prewriteWithVectorProofJavaForProbe(current.txId(),
                                                        current.startTs(), rightWrite, stripe, 0)
                                                : right.prewriteWithVectorProof(current.txId(),
                                                        current.startTs(), rightWrite, stripe, 0);
                                    }, fanout);
                                }
                                var firstProof = first.join();
                                var secondProof = second.join();
                                prewriteNanos.add(System.nanoTime() - stage);
                                owned = owner.allocate(tx.startTs());
                                var decision = new IntentCommitRequest(owned.reservation(), owned.commitTs(),
                                        tx.txId(), tx.startTs(), tx.deadlineMillis(),
                                        IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"),
                                        List.of(firstProof, secondProof));
                                stage = System.nanoTime();
                                batcher.complete(decision);
                                owner.complete(owned);
                                centerNanos.add(System.nanoTime() - stage);
                                stage = System.nanoTime();
                                if (vectorMode) {
                                    vectorPublisher.awaitPublished("s1", stripe,
                                            firstProof.vectorReservation().orElseThrow().reservedSequence(),
                                            owned.commitTs(), 20_000);
                                    vectorPublisher.awaitPublished("s2", stripe,
                                            secondProof.vectorReservation().orElseThrow().reservedSequence(),
                                            owned.commitTs(), 20_000);
                                } else {
                                    batcher.awaitPublished(owned.commitTs(), 20_000);
                                }
                                publishNanos.add(System.nanoTime() - stage);
                                committed.increment();
                            } catch (final RuntimeException ex) {
                                if (owned != null) {
                                    owner.unknown(owned);
                                }
                                failures.increment();
                                firstFailure.compareAndSet(null, ex.getClass().getSimpleName() + ":" + ex.getMessage());
                                if (tx != null) {
                                    try {
                                        ledger.abortIntentTransaction(tx.txId());
                                    } catch (final RuntimeException ignored) {
                                        // The center may have been COMMITTED, unified to durable recovery scan.
                                    }
                                }
                            } finally {
                                if (tx != null) {
                                    snapshots.release(tx.txId());
                                }
                            }
                        }
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            Thread reporter = Thread.ofVirtual().name("mvcc-intent-window").start(() -> {
                long previous = 0;
                for (int index = 1; index <= (seconds + 9) / 10; index++) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
                    if (Thread.interrupted()) {
                        return;
                    }
                    long count = committed.sum();
                    try {
                        System.out.printf("LOCAL_FIRST_CROSS_WINDOW index=%d tps=%.2f committed=%d "
                                + "materialized=%d backlog=%d safe_point=%d slots=%d txn=%d%n",
                                index, (count - previous) / 10.0, count, materialized.sum(),
                                ledger.countUnfinalizedIntentDecisions(), ledger.currentSafePoint(),
                                countRows(controlData, "mvcc_local_first_slot_v1"),
                                countRows(controlData, "mvcc_local_first_txn_v1"));
                    } catch (final SQLException ex) {
                        firstFailure.compareAndSet(null, "window:" + ex.getMessage());
                    }
                    previous = count;
                }
            });
            start.countDown();
            done.await();
            reporter.interrupt();
            reporter.join();
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            recovering.set(false);
            collecting.set(false);
            recoveryThread.join(10_000);
            segmentThread.join(10_000);
            gcThread.join(10_000);
            owner.close();
            long pendingAtStop = ledger.countUnfinalizedIntentDecisions();
            long drainStarted = System.nanoTime();
            while (ledger.countUnfinalizedIntentDecisions() > 0
                    && System.nanoTime() - drainStarted < TimeUnit.SECONDS.toNanos(30)) {
                var drained = recovery.recoverOnce(64);
                materialized.add(drained.finalized());
                recoveryFailures.add(drained.failures());
            }
            double drainSeconds = (System.nanoTime() - drainStarted) / 1_000_000_000.0;
            if (!vectorMode) {
                maintenance.gcOnce();
            }
            System.out.printf("LOCAL_FIRST_CROSS_BENCH mode=%s concurrency=%d duration_s=%.2f committed=%d "
                    + "failures=%d tps=%.2f first_failure=%s groups=%d avg_batch=%.2f "
                    + "published=%d pending_materialization=%d materialized=%d "
                    + "recovery_failures=%d recovery_failure=%s "
                    + "registration_groups=%d registration_avg_batch=%.2f "
                    + "backlog_at_stop=%d drain_seconds=%.2f max_backlog=%d recovery_park_us=%d "
                    + "owner_segments=%d owner_failures=%d owner_error=%s "
                    + "recovered_segments=%d segment_failures=%d segment_error=%s "
                    + "safe_point=%d slot_rows=%d txn_rows=%d segment_rows=%d "
                    + "gc_runs=%d gc_failures=%d gc_error=%s "
                    + "vector_left=%d vector_right=%d vector_pending_left=%d vector_pending_right=%d "
                    + "prewrite_groups_left=%d prewrite_groups_right=%d "
                    + "prewrite_avg_batch_left=%.2f prewrite_avg_batch_right=%.2f "
                    + "prewrite_fallback_left=%d prewrite_fallback_right=%d%n",
                    vectorMode ? "VECTOR" : "SCALAR",
                    concurrency, elapsed, committed.sum(), failures.sum(), committed.sum() / elapsed,
                    firstFailure.get(), batcher.groups.sum(),
                    batcher.groups.sum() == 0 ? 0 : (double) batcher.requests.sum() / batcher.groups.sum(),
                    ledger.currentPublished(), ledger.countUnfinalizedIntentDecisions(),
                    materialized.sum(), recoveryFailures.sum(), recovery.lastFailure(), registrations.groups.sum(),
                    registrations.groups.sum() == 0 ? 0
                            : (double) registrations.requests.sum() / registrations.groups.sum(),
                    pendingAtStop, drainSeconds, maxBacklog, recoveryParkMicros,
                    owner.reservationCount(),
                    owner.refreshStats().failureCount(), owner.lastFailure(),
                    sealedSegments.sum(), segmentFailures.sum(), segmentRecovery.lastFailure(),
                    ledger.currentSafePoint(),
                    countRows(controlData, "mvcc_local_first_slot_v1"),
                    countRows(controlData, "mvcc_local_first_txn_v1"),
                    countRows(controlData, "mvcc_local_first_segment_v1"),
                    maintenance.gcStats().runCount(), maintenance.gcStats().failureCount(),
                    maintenance.lastGcFailure(),
                    vectorMode ? countRows(leftData, "mvcc_vector_committed_v1") : 0,
                    vectorMode ? countRows(rightData, "mvcc_vector_committed_v1") : 0,
                    vectorMode ? countRows(leftData, "mvcc_vector_intent_reservation_v1") : 0,
                    vectorMode ? countRows(rightData, "mvcc_vector_intent_reservation_v1") : 0,
                    leftPrewrites == null ? 0 : leftPrewrites.batchCount(),
                    rightPrewrites == null ? 0 : rightPrewrites.batchCount(),
                    leftPrewrites == null || leftPrewrites.batchCount() == 0 ? 0
                            : (double) leftPrewrites.groupedCount() / leftPrewrites.batchCount(),
                    rightPrewrites == null || rightPrewrites.batchCount() == 0 ? 0
                            : (double) rightPrewrites.groupedCount() / rightPrewrites.batchCount(),
                    leftPrewrites == null ? 0 : leftPrewrites.fallbackCount(),
                    rightPrewrites == null ? 0 : rightPrewrites.fallbackCount());
            long count = Math.max(1, committed.sum());
            System.out.printf("LOCAL_FIRST_CROSS_STAGE begin_avg_ms=%.3f register_avg_ms=%.3f "
                    + "prewrite_avg_ms=%.3f center_avg_ms=%.3f publish_avg_ms=%.3f%n",
                    beginNanos.sum() / count / 1_000_000.0, registerNanos.sum() / count / 1_000_000.0,
                    prewriteNanos.sum() / count / 1_000_000.0, centerNanos.sum() / count / 1_000_000.0,
                    publishNanos.sum() / count / 1_000_000.0);
        } finally {
            recovering.set(false);
            collecting.set(false);
            recoveryThread.interrupt();
            segmentThread.interrupt();
            gcThread.interrupt();
        }
    }

    /**
     * Only the fixed table name of this program is accepted to confirm that there are no linear piles of long run historical lines.
     *
     * @param dataSource database connection source
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long countRows(final HikariDataSource dataSource, final String table) throws SQLException {
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("select count(*) from " + table)) {
            if (!row.next()) {
                throw new SQLException("cross-shard benchmark table count returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Read-only Takes the active configuration of this experiment and guesses other database without system configuration.
     *
     * @param name name
     * @param fallback fallback
     * @return the operation result
     */
    private static String env(final String name, final String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    /**
     * A new transaction merge durable ACTIVE identity at the center, confirming before allowing shard prewrite.
     */
    private static final class RegistrationBatcher implements AutoCloseable {

        private final MysqlLocalFirstPublicationLedger ledger;

        private final ArrayBlockingQueue<Request> queue = new ArrayBlockingQueue<>(4096);

        private final List<Thread> workers;

        private final AtomicBoolean closed = new AtomicBoolean();

        private final LongAdder groups = new LongAdder();

        private final LongAdder requests = new LongAdder();

        private final boolean vectorMode;

        /**
         * Bounded Workspace parallels non-interactive new transaction batch.
         *
         * @param ledger ledger
         * @param workerCount worker count
         * @param vectorMode vector mode
         */
        private RegistrationBatcher(final MysqlLocalFirstPublicationLedger ledger,
                                    final int workerCount, final boolean vectorMode) {
            this.ledger = ledger;
            this.vectorMode = vectorMode;
            List<Thread> running = new ArrayList<>();
            for (int index = 0; index < workerCount; index++) {
                running.add(Thread.ofVirtual().name("mvcc-intent-register-" + index).start(this::work));
            }
            workers = List.copyOf(running);
        }

        /**
         * Foreground Only create intent in any shard after receiving durable confirmation.
         *
         * @param registration registration
         * @throws IllegalStateException if the operation cannot complete
         */
        private void register(final IntentRegistrationRequest registration) {
            var result = new CompletableFuture<Void>();
            if (closed.get() || !queue.offer(new Request(registration, result))) {
                throw new IllegalStateException("cross-shard registration queue is unavailable");
            }
            try {
                result.join();
            } catch (final CompletionException ex) {
                if (ex.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw ex;
            }
        }

        /**
         * An batch fully inserted or all failed, without queuing as central ACTIVE.
         */
        private void work() {
            while (!closed.get() || !queue.isEmpty()) {
                try {
                    Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    List<Request> batch = new ArrayList<>(64);
                    batch.add(first);
                    queue.drainTo(batch, 63);
                    groups.increment();
                    requests.add(batch.size());
                    try {
                        List<IntentRegistrationRequest> identities = batch.stream()
                                .map(Request::registration).toList();
                        if (vectorMode) {
                            ledger.registerVectorIntentBatch(identities);
                        } else {
                            ledger.registerIntentBatch(identities);
                        }
                        batch.forEach(request -> request.result().complete(null));
                    } catch (final RuntimeException ex) {
                        batch.forEach(request -> request.result().completeExceptionally(ex));
                    }
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * The registration line will not be stopped until the request for entry has been completed.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            closed.set(true);
            for (Thread worker : workers) {
                worker.join(10_000);
            }
        }

        /**
         * Each registration awaits completion of transaction durable at its centre.
         *
         * @param registration registration
         * @param result result
         */
        private record Request(IntentRegistrationRequest registration, CompletableFuture<Void> result) {
        }
    }

    /**
     * Multiple transaction s of double shard prewrite completed merge centre decision and release in the same paragraph.
     */
    private static final class IntentBatcher implements AutoCloseable {

        private final MysqlLocalFirstPublicationLedger ledger;

        private final ArrayBlockingQueue<Request> queue = new ArrayBlockingQueue<>(4096);

        private final List<Thread> workers;

        private final ScheduledExecutorService publisher;

        private final AtomicLong published;

        private final AtomicBoolean closed = new AtomicBoolean();

        private final LongAdder groups = new LongAdder();

        private final LongAdder requests = new LongAdder();

        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

        private final boolean vectorMode;

        /**
         * Fixed virtual workspace and cache already released by center transaction durable.
         *
         * @param ledger ledger
         * @param workerCount worker count
         * @param vectorMode vector mode
         */
        private IntentBatcher(final MysqlLocalFirstPublicationLedger ledger,
                              final int workerCount, final boolean vectorMode) {
            this.ledger = ledger;
            this.vectorMode = vectorMode;
            this.published = new AtomicLong(ledger.currentPublished());
            List<Thread> running = new ArrayList<>();
            for (int index = 0; index < workerCount; index++) {
                running.add(Thread.ofVirtual().name("mvcc-intent-batch-" + index).start(this::work));
            }
            workers = List.copyOf(running);
            publisher = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-intent-publisher").unstarted(task));
            publisher.scheduleWithFixedDelay(this::publishSafely, 0, 100, TimeUnit.MILLISECONDS);
        }

        /**
         * Foreground blocked to its own decision batch completion and cannot be queued as durable commit.
         *
         * @param decision decision
         * @throws IllegalStateException if the operation cannot complete
         */
        private void complete(final IntentCommitRequest decision) {
            var result = new CompletableFuture<Void>();
            if (closed.get() || !queue.offer(new Request(decision, result))) {
                throw new IllegalStateException("cross-shard decision queue is unavailable");
            }
            try {
                result.join();
            } catch (final CompletionException ex) {
                if (ex.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw ex;
            }
        }

        /**
         * Foreground Waits for its own commit time to enter the same center durable distribution prefix.
         *
         * @param commitTs durable commit timestamp
         * @param deadlineMillis deadline millis
         * @throws IllegalStateException if the operation cannot complete
         */
        private void awaitPublished(final long commitTs, final long deadlineMillis) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineMillis);
            while (published.get() < commitTs) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("cross-shard publication confirmation timed out");
                }
                LockSupport.parkNanos(100_000);
            }
        }

        /**
         * Each paragraph does not exceed 64 to complete metadata, slots and continuous distribution with the same control transaction.
         */
        private void work() {
            while (!closed.get() || !queue.isEmpty()) {
                try {
                    Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    List<Request> batch = new ArrayList<>(64);
                    batch.add(first);
                    queue.drainTo(batch, 63);
                    Map<Long, List<Request>> bySegment = new HashMap<>();
                    for (Request request : batch) {
                        bySegment.computeIfAbsent(request.decision().segment().lease().firstTs(),
                                ignored -> new ArrayList<>()).add(request);
                    }
                    bySegment.values().forEach(this::completeGroup);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * Center decision failed by notifying only this group, durable ACTIVE to the due recovery.
         *
         * @param batch batch
         */
        private void completeGroup(final List<Request> batch) {
            groups.increment();
            requests.add(batch.size());
            try {
                List<IntentCommitRequest> decisions = batch.stream().map(Request::decision).toList();
                var result = vectorMode ? ledger.completeVectorIntentBatchAndPublish(decisions)
                        : ledger.completeIntentBatchAndPublish(decisions);
                published.accumulateAndGet(result.publishedTs(), Math::max);
                batch.forEach(request -> request.result().complete(null));
            } catch (final RuntimeException ex) {
                failure.set(ex);
                batch.forEach(request -> request.result().completeExceptionally(ex));
            }
        }

        /**
         * The homeer closes or loses connection recovery advance watermark and wakes up the waiting foreground.
         */
        private void publishSafely() {
            try {
                published.accumulateAndGet(ledger.advancePublishedFast(4096), Math::max);
            } catch (final RuntimeException ex) {
                failure.set(ex);
            }
        }

        /**
         * The new request is closed and the incoming request is completed or unusual reported.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            closed.set(true);
            publisher.shutdownNow();
            for (Thread worker : workers) {
                worker.join(10_000);
            }
        }

        /**
         * Each waiter only receives its own durable Center batch result.
         *
         * @param decision decision
         * @param result result
         */
        private record Request(IntentCommitRequest decision, CompletableFuture<Void> result) {
        }
    }
}

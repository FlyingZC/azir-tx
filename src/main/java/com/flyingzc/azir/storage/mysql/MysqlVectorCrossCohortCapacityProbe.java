package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.mysql.MysqlLocalFirstMetadataView;
import com.flyingzc.azir.publication.LocalFirstIntentRecoveryManager;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.IntentCommitRequest;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.IntentRegistrationRequest;
import com.flyingzc.azir.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/** 同一事务组贯穿中心登记、两片预写和中心决议的隔离容量上界。 */
public final class MysqlVectorCrossCohortCapacityProbe {
    /** 容量探针不保存全局连接或事务状态。 */
    private MysqlVectorCrossCohortCapacityProbe() {
    }

    /** 仅使用全新空数据库，显式参数限制物理并发与逻辑组大小。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("vector cohort probe requires AZIR_TX_RESET=true");
        }
        int workers = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "13"));
        int cohortSize = Integer.parseInt(env("AZIR_TX_VECTOR_COHORT_SIZE", "4"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "5"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        boolean asynchronousPublication = Boolean.parseBoolean(env("AZIR_TX_COHORT_ASYNC_PUBLISH", "false"));
        boolean steadyRecovery = Boolean.parseBoolean(env("AZIR_TX_COHORT_STEADY_RECOVERY", "false"));
        boolean steadyFull = Boolean.parseBoolean(env("AZIR_TX_COHORT_STEADY_FULL", "false"));
        int gcParallelism = Integer.parseInt(env("AZIR_TX_VECTOR_CROSS_GC_PARALLELISM", "8"));
        int gcRetireBatch = Integer.parseInt(env("AZIR_TX_VECTOR_CROSS_GC_RETIRE_BATCH", "8"));
        int gcCandidateLimit = Integer.parseInt(env("AZIR_TX_VECTOR_CROSS_GC_BATCH_LIMIT", "64"));
        int localEveryRounds = Integer.parseInt(env("AZIR_TX_VECTOR_LOCAL_GC_EVERY_ROUNDS", "8"));
        long gcIntervalMillis = Long.parseLong(env("AZIR_TX_VECTOR_GC_INTERVAL_MS", "20"));
        long publicationParkMicros = Long.parseLong(env("AZIR_TX_COHORT_PUBLISH_PARK_MICROS", "500"));
        if (workers < 1 || workers > 100 || cohortSize < 1 || cohortSize > 16
                || seconds < 1 || seconds > 30 || stripes < 1 || stripes > 256
                || publicationParkMicros < 0 || publicationParkMicros > 100_000
                || steadyFull && !steadyRecovery
                || gcParallelism < 1 || gcParallelism > 32
                || gcRetireBatch < 1 || gcRetireBatch > 32
                || gcCandidateLimit < 1 || gcCandidateLimit > 512
                || localEveryRounds < 1 || localEveryRounds > 1_000
                || gcIntervalMillis < 1 || gcIntervalMillis > 60_000) {
            throw new IllegalArgumentException("invalid vector cohort capacity limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        String controlUrl = env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        String leftUrl = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23307/shard_1");
        String rightUrl = env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23308/shard_2");
        try (HikariDataSource center = pool(controlUrl, username, password);
             HikariDataSource leftData = pool(leftUrl, username, password);
             HikariDataSource rightData = pool(rightUrl, username, password)) {
            requireEmpty(center);
            requireEmpty(leftData);
            requireEmpty(rightData);
            var ledger = new MysqlLocalFirstPublicationLedger(center);
            ledger.initializeIsolated(100);
            var metadata = new MysqlLocalFirstMetadataView(ledger);
            var left = new MysqlStorageAdapter("s1", leftData, metadata);
            var right = new MysqlStorageAdapter("s2", rightData, metadata);
            left.initialize();
            right.initialize();
            left.initializeVectorProtocol(stripes);
            right.initializeVectorProtocol(stripes);
            Map<String, DataSource> routes = Map.of("s1", leftData, "s2", rightData);
            var leases = MysqlVectorCutLeaderLease.forProduct(center, routes, stripes);
            leases.initializeProduct();
            try (var fanout = Executors.newVirtualThreadPerTaskExecutor();
                 var snapshots = new LocalFirstCoordinatorSnapshots(ledger, 20_000, 60_000, 1_000);
                 var owner = new LocalFirstIntentSegmentOwnerRegistry(ledger,
                         snapshots::floorForReservation, 512, 5_000, 1_000, 100);
                 var publisher = MysqlVectorCutLeaderRuntime.forProduct(center, routes, stripes,
                         ledger::currentPublished, leases, "cohort-probe-owner", 5_000, 200)) {
                if (!publisher.start()) {
                    throw new IllegalStateException("vector cohort probe found another publisher");
                }
                try (var steady = steadyFull ? new MysqlVectorCrossSteadyBackground(ledger,
                        left, right, center, leftData, rightData, publisher,
                        controlUrl, leftUrl, rightUrl, username, password, stripes,
                        gcParallelism, gcRetireBatch, gcCandidateLimit,
                        localEveryRounds, gcIntervalMillis) : null) {
                    if (steady != null) {
                        steady.start();
                    }
                    run(ledger, left, right, center, leftData, rightData, snapshots, owner,
                            publisher, fanout, workers, cohortSize, seconds, stripes,
                            asynchronousPublication, publicationParkMicros, steadyRecovery, steady);
                }
            }
        }
    }

    /** 探针复用产品连接池与驱动超时配置。 */
    private static HikariDataSource pool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /** 已有产品表可能存有业务数据时拒绝运行，而非依赖重置标志猜测可删除。 */
    private static void requireEmpty(HikariDataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select table_name from information_schema.tables
                where table_schema = database() and left(table_name, 5) = 'mvcc_'
                limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                throw new IllegalArgumentException("vector cohort probe requires empty isolated databases");
            }
        }
    }

    /** 物理线程每轮组成完整事务组，后台物化只在计时结束后排空以量化前台上界。 */
    private static void run(MysqlLocalFirstPublicationLedger ledger,
                            MysqlStorageAdapter left, MysqlStorageAdapter right,
                            HikariDataSource center, HikariDataSource leftData,
                            HikariDataSource rightData, LocalFirstCoordinatorSnapshots snapshots,
                            LocalFirstIntentSegmentOwnerRegistry owner,
                            MysqlVectorCutLeaderRuntime publisher,
                            java.util.concurrent.ExecutorService fanout,
                            int workerCount, int cohortSize, int seconds, int stripes,
                            boolean asynchronousPublication, long publicationParkMicros,
                            boolean steadyRecovery,
                            MysqlVectorCrossSteadyBackground steady) throws Exception {
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder groups = new LongAdder();
        LongAdder beginNanos = new LongAdder();
        LongAdder registerNanos = new LongAdder();
        LongAdder prewriteNanos = new LongAdder();
        LongAdder centerNanos = new LongAdder();
        LongAdder publishNanos = new LongAdder();
        LongAdder prewriteFallbacks = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        AtomicReference<String> firstBatchFailure = new AtomicReference<>();
        AtomicBoolean publishing = new AtomicBoolean(asynchronousPublication);
        AtomicBoolean recovering = new AtomicBoolean(steadyRecovery);
        LongAdder steadyFinalized = new LongAdder();
        LongAdder steadyRecoveryFailures = new LongAdder();
        AtomicReference<String> firstRecoveryFailure = new AtomicReference<>();
        var recovery = new LocalFirstIntentRecoveryManager(ledger,
                Map.of("s1", left, "s2", right), 32);
        Thread recoveryWorker = steadyRecovery
                ? Thread.ofVirtual().name("azir-cohort-steady-recovery").start(() -> {
                    while (true) {
                        try {
                            if (!recovering.get() && ledger.countUnfinalizedIntentDecisions() == 0) {
                                return;
                            }
                            var result = recovery.recoverOnce(64);
                            steadyFinalized.add(result.finalized());
                            steadyRecoveryFailures.add(result.failures());
                            if (result.failures() != 0) {
                                firstRecoveryFailure.compareAndSet(null,
                                        describeFailure(recovery.lastFailure()));
                            }
                            if (result.finalized() == 0 && result.aborted() == 0) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                            }
                        } catch (RuntimeException ex) {
                            firstRecoveryFailure.compareAndSet(null, describeFailure(ex));
                            steadyRecoveryFailures.increment();
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                        }
                    }
                }) : null;
        Thread centerPublisher = asynchronousPublication
                ? Thread.ofVirtual().name("azir-cohort-center-publisher").start(() -> {
                    while (publishing.get()) {
                        try {
                            ledger.advancePublishedFast(4096);
                        } catch (RuntimeException ex) {
                            firstFailure.compareAndSet(null, describeFailure(ex));
                        }
                        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(publicationParkMicros));
                    }
                }) : null;
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workerCount);
        try (var workers = Executors.newFixedThreadPool(workerCount)) {
            for (int index = 0; index < workerCount; index++) {
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
                        while (System.nanoTime() < deadline) {
                            if (steady != null) {
                                try {
                                    steady.requireHealthy();
                                } catch (RuntimeException ex) {
                                    firstFailure.compareAndSet(null, describeFailure(ex));
                                    break;
                                }
                            }
                            List<BeginReservation> begun = new ArrayList<>(cohortSize);
                            List<LocalFirstIntentSegmentOwnerRegistry.Assignment> assigned =
                                    new ArrayList<>(cohortSize);
                            try {
                                long stage = System.nanoTime();
                                for (int item = 0; item < cohortSize; item++) {
                                    begun.add(snapshots.begin(IsolationLevel.REPEATABLE_READ));
                                }
                                beginNanos.add(System.nanoTime() - stage);
                                Set<String> routes = Set.of("s1", "s2");
                                stage = System.nanoTime();
                                ledger.registerVectorIntentBatch(begun.stream().map(tx ->
                                        new IntentRegistrationRequest(tx.txId(), tx.startTs(),
                                                tx.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                                                routes)).toList());
                                registerNanos.add(System.nanoTime() - stage);
                                long expiry = System.currentTimeMillis() + 60_000;
                                List<MysqlStorageAdapter.VectorPrewriteRequest> leftWrites = new ArrayList<>();
                                List<MysqlStorageAdapter.VectorPrewriteRequest> rightWrites = new ArrayList<>();
                                for (BeginReservation tx : begun) {
                                    int stripe = Math.floorMod(tx.txId(), stripes);
                                    byte[] value = Long.toString(tx.txId()).getBytes(StandardCharsets.UTF_8);
                                    leftWrites.add(new MysqlStorageAdapter.VectorPrewriteRequest(
                                            tx.txId(), tx.startTs(), expiry,
                                            Mutation.put("cross:" + tx.txId() + ":left", value), stripe, 0));
                                    rightWrites.add(new MysqlStorageAdapter.VectorPrewriteRequest(
                                            tx.txId(), tx.startTs(), expiry,
                                            Mutation.put("cross:" + tx.txId() + ":right", value), stripe, 0));
                                }
                                stage = System.nanoTime();
                                var leftFuture = CompletableFuture.supplyAsync(
                                        () -> prewriteWithFallback(left, leftWrites,
                                                prewriteFallbacks, firstBatchFailure), fanout);
                                var rightFuture = CompletableFuture.supplyAsync(
                                        () -> prewriteWithFallback(right, rightWrites,
                                                prewriteFallbacks, firstBatchFailure), fanout);
                                List<MysqlStorageAdapter.PrewriteProof> leftProofs = leftFuture.join();
                                List<MysqlStorageAdapter.PrewriteProof> rightProofs = rightFuture.join();
                                prewriteNanos.add(System.nanoTime() - stage);
                                stage = System.nanoTime();
                                assigned.addAll(owner.allocateBatch(begun.stream()
                                        .map(BeginReservation::startTs).toList()));
                                Map<Long, List<IntentCommitRequest>> bySegment = new HashMap<>();
                                for (int item = 0; item < begun.size(); item++) {
                                    BeginReservation tx = begun.get(item);
                                    var assignment = assigned.get(item);
                                    var decision = new IntentCommitRequest(assignment.reservation(),
                                            assignment.commitTs(), tx.txId(), tx.startTs(),
                                            tx.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                                            routes, List.of(leftProofs.get(item), rightProofs.get(item)));
                                    bySegment.computeIfAbsent(assignment.reservation().lease().firstTs(),
                                            ignored -> new ArrayList<>()).add(decision);
                                }
                                for (List<IntentCommitRequest> decisions : bySegment.values()) {
                                    if (asynchronousPublication) {
                                        ledger.completeVectorIntentBatch(decisions);
                                    } else {
                                        ledger.completeVectorIntentBatchAndPublish(decisions);
                                    }
                                }
                                assigned.forEach(owner::complete);
                                centerNanos.add(System.nanoTime() - stage);
                                stage = System.nanoTime();
                                for (int item = 0; item < begun.size(); item++) {
                                    var assignment = assigned.get(item);
                                    var leftProof = leftProofs.get(item).vectorReservation().orElseThrow();
                                    var rightProof = rightProofs.get(item).vectorReservation().orElseThrow();
                                    publisher.awaitPublished("s1", leftProof.stripe(),
                                            leftProof.reservedSequence(), assignment.commitTs(), 20_000);
                                    publisher.awaitPublished("s2", rightProof.stripe(),
                                            rightProof.reservedSequence(), assignment.commitTs(), 20_000);
                                }
                                publishNanos.add(System.nanoTime() - stage);
                                if (steady != null) {
                                    for (int item = 0; item < begun.size(); item++) {
                                        steady.enqueue(begun.get(item).txId(),
                                                leftProofs.get(item), rightProofs.get(item));
                                    }
                                }
                                committed.add(begun.size());
                                groups.increment();
                            } catch (RuntimeException ex) {
                                failures.add(Math.max(1, begun.size()));
                                firstFailure.compareAndSet(null, describeFailure(ex));
                                assigned.forEach(owner::unknown);
                                for (BeginReservation tx : begun) {
                                    try {
                                        ledger.abortIntentTransaction(tx.txId());
                                    } catch (RuntimeException ignored) {
                                        // 中心可能已持久提交，恢复器仍须依中心终态处理。
                                    }
                                }
                            } finally {
                                begun.forEach(tx -> snapshots.release(tx.txId()));
                            }
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            AtomicBoolean reporting = new AtomicBoolean(steady != null);
            Thread reporter = steady == null ? null : Thread.ofVirtual()
                    .name("azir-cross-steady-window").start(() -> {
                        long windowStarted = System.nanoTime();
                        while (reporting.get()) {
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
                            if (!reporting.get()) {
                                return;
                            }
                            try {
                                var status = steady.snapshot();
                                System.out.printf("VECTOR_CROSS_STEADY_WINDOW elapsed_s=%.2f "
                                                + "ack=%d pending_recovery=%d "
                                                + "successors=%d/%d mapped=%d retired=%d "
                                                + "gc_runs=%d gc_failures=%d%n",
                                        (System.nanoTime() - windowStarted) / 1_000_000_000.0,
                                        committed.sum(), ledger.countUnfinalizedIntentDecisions(),
                                        status.completed(), status.submitted(),
                                        status.leftMappings() + status.rightMappings(),
                                        status.leftReceipts() + status.rightReceipts(),
                                        status.gcRuns(), status.gcFailures());
                            } catch (RuntimeException | SQLException ex) {
                                firstFailure.compareAndSet(null, describeFailure(ex));
                                return;
                            }
                        }
                    });
            long started = System.nanoTime();
            start.countDown();
            done.await();
            reporting.set(false);
            if (reporter != null) {
                reporter.interrupt();
                reporter.join(5_000);
            }
            publishing.set(false);
            if (centerPublisher != null) {
                centerPublisher.interrupt();
                centerPublisher.join(5_000);
            }
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            MysqlVectorCrossSteadyBackground.Snapshot steadyAtStop = steady == null
                    ? null : steady.snapshot();
            owner.close();
            long pendingAtStop = ledger.countUnfinalizedIntentDecisions();
            long drainStarted = System.nanoTime();
            long finalized;
            long recoveryFailures;
            if (steadyRecovery) {
                recovering.set(false);
                recoveryWorker.join(30_000);
                if (recoveryWorker.isAlive()) {
                    throw new IllegalStateException("steady cross recovery failed to drain within 30 seconds");
                }
                finalized = steadyFinalized.sum();
                recoveryFailures = steadyRecoveryFailures.sum();
            } else {
                finalized = 0;
                recoveryFailures = 0;
                while (ledger.countUnfinalizedIntentDecisions() > 0
                        && System.nanoTime() - drainStarted < TimeUnit.SECONDS.toNanos(30)) {
                    var result = recovery.recoverOnce(64);
                    finalized += result.finalized();
                    recoveryFailures += result.failures();
                }
            }
            double drainSeconds = (System.nanoTime() - drainStarted) / 1_000_000_000.0;
            if (ledger.countUnfinalizedIntentDecisions() != 0
                    || recoveryFailures != 0 || finalized != committed.sum()) {
                throw new IllegalStateException("cross cohort recovery did not finalize every ACK: "
                        + "ack=" + committed.sum() + " finalized=" + finalized
                        + " writer_failures=" + failures.sum()
                        + " writer_first=" + firstFailure.get()
                        + " recovery_first=" + firstRecoveryFailure.get()
                        + " background_first=" + (steady == null ? null : steady.snapshot().firstFailure()));
            }
            if (steady != null) {
                steady.awaitSuccessors(Duration.ofSeconds(45));
                long gcDrainStarted = System.nanoTime();
                var finalSteady = steady.awaitGcDrain(Duration.ofSeconds(60));
                double gcDrainSeconds = (System.nanoTime() - gcDrainStarted) / 1_000_000_000.0;
                if (finalSteady.firstFailure() != null
                        || finalSteady.completed() != committed.sum()
                        || finalSteady.leftMappings() + finalSteady.leftReceipts() != committed.sum()
                        || finalSteady.rightMappings() + finalSteady.rightReceipts() != committed.sum()
                        || countRows(leftData, "mvcc_data") != finalSteady.leftMappings()
                        || countRows(rightData, "mvcc_data") != finalSteady.rightMappings()) {
                    throw new IllegalStateException("steady cross backlog or receipt conservation failed: "
                            + finalSteady.firstFailure());
                }
                System.out.printf("VECTOR_CROSS_STEADY_RESULT ack=%d duration_s=%.2f "
                                + "ack_tps=%.2f stop_pending_recovery=%d "
                                + "stop_successors=%d/%d stop_mapped=%d stop_retired=%d "
                                + "final_successors=%d final_mapped=%d final_retired=%d "
                                + "gc_drain_s=%.2f gc_runs=%d gc_failures=%d%n",
                        committed.sum(), elapsed, committed.sum() / elapsed,
                        pendingAtStop, steadyAtStop.completed(), steadyAtStop.submitted(),
                        steadyAtStop.leftMappings() + steadyAtStop.rightMappings(),
                        steadyAtStop.leftReceipts() + steadyAtStop.rightReceipts(),
                        finalSteady.completed(), finalSteady.leftMappings() + finalSteady.rightMappings(),
                        finalSteady.leftReceipts() + finalSteady.rightReceipts(),
                        gcDrainSeconds, finalSteady.gcRuns(), finalSteady.gcFailures());
            }
            long count = Math.max(1, committed.sum());
            System.out.printf("VECTOR_CROSS_COHORT_CAPACITY async_publish=%s steady_recovery=%s "
                            + "publish_park_us=%d "
                            + "physical_workers=%d cohort=%d "
                            + "logical_inflight=%d duration_s=%.2f committed=%d failures=%d "
                            + "ack_tps=%.2f groups=%d avg_group=%.2f first_failure=%s "
                            + "prewrite_fallback_members=%d first_batch_failure=%s "
                            + "begin_avg_ms=%.3f register_avg_ms=%.3f prewrite_avg_ms=%.3f "
                            + "center_avg_ms=%.3f publish_avg_ms=%.3f "
                            + "pending_at_stop=%d drain_s=%.2f finalized=%d recovery_failures=%d "
                            + "first_recovery_failure=%s "
                            + "left_versions=%d right_versions=%d left_pending=%d right_pending=%d "
                            + "control_rows=%d%n",
                    asynchronousPublication, steadyRecovery, publicationParkMicros,
                    workerCount, cohortSize, workerCount * cohortSize, elapsed,
                    committed.sum(), failures.sum(), committed.sum() / elapsed,
                    groups.sum(), groups.sum() == 0 ? 0 : (double) committed.sum() / groups.sum(),
                    firstFailure.get(), prewriteFallbacks.sum(), firstBatchFailure.get(),
                    beginNanos.sum() / count / 1_000_000.0,
                    registerNanos.sum() / count / 1_000_000.0,
                    prewriteNanos.sum() / count / 1_000_000.0,
                    centerNanos.sum() / count / 1_000_000.0,
                    publishNanos.sum() / count / 1_000_000.0,
                    pendingAtStop, drainSeconds, finalized, recoveryFailures,
                    firstRecoveryFailure.get(),
                    countRows(leftData, "mvcc_vector_committed_v1"),
                    countRows(rightData, "mvcc_vector_committed_v1"),
                    countRows(leftData, "mvcc_vector_intent_reservation_v1"),
                    countRows(rightData, "mvcc_vector_intent_reservation_v1"),
                    countRows(center, "mvcc_local_first_txn_v1"));
        }
    }

    /** 整批响应不确定时按独立事务号重放，保留原数据库错误供归因。 */
    private static List<MysqlStorageAdapter.PrewriteProof> prewriteWithFallback(
            MysqlStorageAdapter adapter, List<MysqlStorageAdapter.VectorPrewriteRequest> requests,
            LongAdder fallbackMembers, AtomicReference<String> firstBatchFailure) {
        try {
            return adapter.prewriteVectorBatchWithProof(requests);
        } catch (RuntimeException ex) {
            firstBatchFailure.compareAndSet(null, describeFailure(ex));
            fallbackMembers.add(requests.size());
            List<MysqlStorageAdapter.PrewriteProof> proofs = new ArrayList<>(requests.size());
            for (var request : requests) {
                proofs.add(adapter.prewriteWithVectorProof(request.txId(), request.startTs(),
                        request.mutation(), request.stripe(), request.startSequence()));
            }
            return List.copyOf(proofs);
        }
    }

    /** 完整异常链和 SQL 错误码用于区分死锁、超时与协议拒绝。 */
    private static String describeFailure(Throwable failure) {
        StringBuilder result = new StringBuilder();
        for (Throwable current = failure; current != null && result.length() < 600;
             current = current.getCause()) {
            if (!result.isEmpty()) {
                result.append(" <- ");
            }
            result.append(current.getClass().getSimpleName()).append(':').append(current.getMessage());
            if (current instanceof SQLException sql) {
                result.append("[mysql=").append(sql.getErrorCode()).append(']');
            }
        }
        return result.toString();
    }

    /** 按固定表名核对组提交物化与中心事务数。 */
    private static long countRows(HikariDataSource dataSource, String table) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement(
                "select count(*) from " + table); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("vector cohort count returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 仅读取本探针显式连接和短时容量参数。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

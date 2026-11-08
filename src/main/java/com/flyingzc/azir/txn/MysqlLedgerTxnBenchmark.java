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

package com.flyingzc.azir.txn;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.gc.NoOpSafePointStore;
import com.flyingzc.azir.gc.mysql.MysqlSafePointStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.metrics.NoOpTransactionMetrics;
import com.flyingzc.azir.metrics.InMemoryTransactionMetrics;
import com.flyingzc.azir.metrics.TransactionMetricsFormatter;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;

import java.nio.charset.StandardCharsets;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Separates the new database book with two shard complete transaction pressure access.
 */
public final class MysqlLedgerTxnBenchmark {

    /**
     * Prevents exemplifying benchmark programs.
     */
    private MysqlLedgerTxnBenchmark() {
    }

    /**
     * Run timed concurrent writing transaction using a clear segregation reset switch.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("ledger benchmark requires AZIR_TX_RESET=true on an isolated database");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        int shardConcurrency = Integer.parseInt(env("AZIR_TX_SHARD_CONCURRENCY", "16"));
        int windowSeconds = Integer.parseInt(env("AZIR_TX_BENCH_WINDOW_SECONDS", "10"));
        int writeShards = Integer.parseInt(env("AZIR_TX_BENCH_SHARDS", "2"));
        boolean readOnly = Boolean.parseBoolean(env("AZIR_TX_READ_ONLY", "false"));
        boolean directSingleShard = Boolean.parseBoolean(env("AZIR_TX_DIRECT_SINGLE_SHARD", "false"));
        boolean batchBegin = Boolean.parseBoolean(env("AZIR_TX_BATCH_BEGIN", "false"));
        boolean coordinatorSnapshots = Boolean.parseBoolean(env("AZIR_TX_COORDINATOR_SNAPSHOTS", "false"));
        long coordinatorLeaseMillis = Long.parseLong(env("AZIR_TX_COORDINATOR_LEASE_MS", "120000"));
        long coordinatorRefreshMillis = Long.parseLong(env("AZIR_TX_COORDINATOR_REFRESH_MS", "1000"));
        int beginWorkers = Integer.parseInt(env("AZIR_TX_BEGIN_BATCH_WORKERS", "1"));
        boolean backgroundEnabled = Boolean.parseBoolean(env("AZIR_TX_BACKGROUND_ENABLED", "false"));
        boolean segmentedRuntime = Boolean.parseBoolean(env("AZIR_TX_SEGMENT_RUNTIME", "false"));
        if (coordinatorSnapshots && !backgroundEnabled) {
            throw new IllegalArgumentException("coordinator snapshots require independent background pools");
        }
        if (segmentedRuntime && (!backgroundEnabled || coordinatorSnapshots
                || directSingleShard || batchBegin)) {
            throw new IllegalArgumentException("segmented benchmark requires independent pools and intent mode");
        }
        if (concurrency < 1 || durationSeconds < 1 || shardConcurrency < 1 || windowSeconds < 1
                || writeShards < 1 || writeShards > 2) {
            throw new IllegalArgumentException("invalid benchmark concurrency or duration");
        }
        try (
                HikariDataSource control = pool("AZIR_TX_CONTROL_JDBC", "control");
                HikariDataSource firstData = pool("AZIR_TX_SHARD1_JDBC", "shard_1");
                HikariDataSource secondData = pool("AZIR_TX_SHARD2_JDBC", "shard_2")) {
            var tso = new MysqlTimestampOracle(control);
            var metadata = new MysqlTransactionMetadataStore(control);
            var oldSafePoint = new MysqlSafePointStore(control);
            var first = new MysqlStorageAdapter("shard-1", firstData, metadata);
            var second = new MysqlStorageAdapter("shard-2", secondData, metadata);
            tso.initialize(100);
            metadata.initialize();
            oldSafePoint.initialize(0);
            first.initialize();
            second.initialize();
            resetControl(control);
            resetShard(firstData);
            resetShard(secondData);
            tso.initialize(100);
            metadata.initialize();
            oldSafePoint.initialize(0);
            first.initialize();
            second.initialize();
            var ledger = new MysqlPublicationLedger(control);
            ledger.initialize();
            var limits = new TransactionLimits(5_000, 10_000, 16L * 1024 * 1024,
                    1024, shardConcurrency, 1024, 256, 5_000);
            boolean asyncCommit = Boolean.parseBoolean(env("AZIR_TX_ASYNC_COMMIT", "true"));
            var options = new TransactionOptions(LockWaitPolicy.mysqlLikeDefaults(),
                    TransactionRetryPolicy.mysqlLikeDefaults(), asyncCommit
                            ? CrossShardCommitPolicy.asyncParallel(16)
                            : CrossShardCommitPolicy.parallel(),
                    true, true, false, limits);
            InMemoryTransactionMetrics stageMetrics = Boolean.parseBoolean(
                    env("AZIR_TX_STAGE_METRICS", "false")) ? new InMemoryTransactionMetrics() : null;
            boolean batchControlCommit = Boolean.parseBoolean(env("AZIR_TX_BATCH_CONTROL_COMMIT", "false"));
            int batchWorkers = Integer.parseInt(env("AZIR_TX_BATCH_WORKERS", "2"));
            if (segmentedRuntime) {
                new MysqlSegmentPublicationLedger(control).activateFromLedger();
                var segmentOptions = new SegmentRuntimeOptions(
                        Integer.parseInt(env("AZIR_TX_SEGMENT_SIZE", "32")),
                        Long.parseLong(env("AZIR_TX_SEGMENT_LEASE_MS", "5000")),
                        Long.parseLong(env("AZIR_TX_SEGMENT_REFRESH_MS", "1000")),
                        Long.parseLong(env("AZIR_TX_SEGMENT_IDLE_SEAL_MS", "100")),
                        128, 256, 128, 100,
                        Long.parseLong(env("AZIR_TX_SEGMENT_PUBLISH_MS", "10")), 1_000,
                        Boolean.parseBoolean(env("AZIR_TX_SEGMENT_BATCH_DECISIONS", "false")),
                        Integer.parseInt(env("AZIR_TX_SEGMENT_BATCH_WORKERS", "2")),
                        Boolean.parseBoolean(env("AZIR_TX_SEGMENT_DIRECT_SINGLE_SHARD", "false")),
                        Boolean.parseBoolean(env("AZIR_TX_SEGMENT_EVENT_PUBLICATION", "false")),
                        Boolean.parseBoolean(env("AZIR_TX_SEGMENT_OVERLAP_MATERIALIZATION", "false")),
                        Boolean.parseBoolean(env("AZIR_TX_SEGMENT_FAST_MATERIALIZATION", "false")),
                        Boolean.parseBoolean(env("AZIR_TX_SEGMENT_FAST_COMMIT_PROC", "false")));
                try (
                        HikariDataSource backgroundControl = backgroundPool("AZIR_TX_CONTROL_JDBC", "control");
                        HikariDataSource backgroundFirst = backgroundPool("AZIR_TX_SHARD1_JDBC", "shard_1");
                        HikariDataSource backgroundSecond = backgroundPool("AZIR_TX_SHARD2_JDBC", "shard_2");
                        var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                                Map.of("shard-1", firstData, "shard-2", secondData),
                                Map.of("shard-1", backgroundFirst, "shard-2", backgroundSecond),
                                options, segmentOptions,
                                stageMetrics == null ? NoOpTransactionMetrics.INSTANCE : stageMetrics)) {
                    runAndReport(runtime.transactionManager(), stageMetrics, concurrency,
                            durationSeconds, shardConcurrency, asyncCommit, windowSeconds, writeShards, readOnly);
                    System.out.printf("SEGMENT_BACKGROUND recovery_runs=%d recovery_failures=%d "
                            + "publication_runs=%d publication_failures=%d gc_runs=%d gc_failures=%d "
                            + "owner_runs=%d owner_failures=%d owner_error=%s recovery_error=%s "
                            + "publication_error=%s gc_error=%s%n",
                            runtime.maintenanceScheduler().recoveryStats().runCount(),
                            runtime.maintenanceScheduler().recoveryStats().failureCount(),
                            runtime.maintenanceScheduler().publicationStats().runCount(),
                            runtime.maintenanceScheduler().publicationStats().failureCount(),
                            runtime.maintenanceScheduler().gcStats().runCount(),
                            runtime.maintenanceScheduler().gcStats().failureCount(),
                            runtime.ownerRefreshStats().runCount(), runtime.ownerRefreshStats().failureCount(),
                            runtime.lastOwnerRefreshFailure(),
                            runtime.maintenanceScheduler().lastRecoveryFailure(),
                            runtime.maintenanceScheduler().lastPublicationFailure(),
                            runtime.maintenanceScheduler().lastGcFailure());
                }
            } else if (backgroundEnabled) {
                try (
                        HikariDataSource backgroundControl = backgroundPool("AZIR_TX_CONTROL_JDBC", "control");
                        HikariDataSource backgroundFirst = backgroundPool("AZIR_TX_SHARD1_JDBC", "shard_1");
                        HikariDataSource backgroundSecond = backgroundPool("AZIR_TX_SHARD2_JDBC", "shard_2");
                        var runtime = MysqlLedgerTransactionRuntime.open(control, backgroundControl,
                                Map.of("shard-1", firstData, "shard-2", secondData),
                                Map.of("shard-1", backgroundFirst, "shard-2", backgroundSecond),
                                options, new LedgerRuntimeOptions(128, 128, 100, 1_000,
                                        batchControlCommit, batchWorkers, directSingleShard,
                                        batchBegin, beginWorkers, coordinatorSnapshots,
                                        coordinatorLeaseMillis, coordinatorRefreshMillis),
                                stageMetrics == null ? NoOpTransactionMetrics.INSTANCE : stageMetrics)) {
                    runAndReport(runtime.transactionManager(), stageMetrics, concurrency,
                            durationSeconds, shardConcurrency, asyncCommit, windowSeconds, writeShards, readOnly);
                    System.out.printf("LEDGER_BACKGROUND recovery_runs=%d recovery_failures=%d "
                            + "gc_runs=%d gc_failures=%d recovery_error=%s gc_error=%s%n",
                            runtime.maintenanceScheduler().recoveryStats().runCount(),
                            runtime.maintenanceScheduler().recoveryStats().failureCount(),
                            runtime.maintenanceScheduler().gcStats().runCount(),
                            runtime.maintenanceScheduler().gcStats().failureCount(),
                            runtime.maintenanceScheduler().lastRecoveryFailure(),
                            runtime.maintenanceScheduler().lastGcFailure());
                }
            } else {
                try (
                        var manager = new TransactionManager(tso, metadata, NoOpSafePointStore.INSTANCE,
                                options, Map.of("shard-1", first, "shard-2", second),
                                stageMetrics == null ? NoOpTransactionMetrics.INSTANCE : stageMetrics,
                                ledger, batchControlCommit, batchWorkers, directSingleShard,
                                batchBegin, beginWorkers)) {
                    runAndReport(manager, stageMetrics, concurrency,
                            durationSeconds, shardConcurrency, asyncCommit, windowSeconds, writeShards, readOnly);
                }
            }
        }
    }

    /**
     * Harmonizes the output of foreground throughput, batch form and optional stage indicators.
     *
     * @param manager manager
     * @param stageMetrics stage metrics
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param shardConcurrency shard concurrency
     * @param asyncCommit async commit
     * @param windowSeconds window seconds
     * @param writeShards write shards
     * @param readOnly read only
     * @throws Exception if the operation cannot complete
     */
    private static void runAndReport(final TransactionManager manager, final InMemoryTransactionMetrics stageMetrics, final int concurrency,
                                     final int durationSeconds, final int shardConcurrency,
                                     final boolean asyncCommit, final int windowSeconds, final int writeShards,
                                     final boolean readOnly) throws Exception {
        run(manager, concurrency, durationSeconds, shardConcurrency, asyncCommit, windowSeconds,
                writeShards, readOnly);
        var batchStats = manager.ledgerBatchStats();
        if (batchStats != null) {
            System.out.printf("LEDGER_BATCH groups=%d requests=%d singletons=%d fallbacks=%d avg_size=%.2f%n",
                    batchStats.groups(), batchStats.requests(), batchStats.singletons(),
                    batchStats.fallbacks(), batchStats.groups() == 0 ? 0.0
                            : (double) batchStats.requests() / batchStats.groups());
            var timing = manager.ledgerBatchTiming();
            if (timing.calls() > 0) {
                System.out.printf("LEDGER_BATCH_TIME calls=%d validation_avg_ms=%.3f state_wait_avg_ms=%.3f "
                        + "write_avg_ms=%.3f finish_avg_ms=%.3f%n",
                        timing.calls(), timing.validationNanos() / 1_000_000.0 / timing.calls(),
                        timing.stateWaitNanos() / 1_000_000.0 / timing.calls(),
                        timing.writeNanos() / 1_000_000.0 / timing.calls(),
                        timing.finishNanos() / 1_000_000.0 / timing.calls());
            }
        }
        var segmentBatch = manager.segmentBatchStats();
        if (segmentBatch != null) {
            System.out.printf("SEGMENT_BATCH groups=%d requests=%d singletons=%d unknown_groups=%d "
                    + "synchronous_fallbacks=%d avg_size=%.2f%n",
                    segmentBatch.groups(), segmentBatch.requests(), segmentBatch.singletons(),
                    segmentBatch.unknownGroups(), segmentBatch.synchronousFallbacks(),
                    segmentBatch.groups() == 0 ? 0.0
                            : (double) segmentBatch.requests() / segmentBatch.groups());
        }
        var beginStats = manager.ledgerBeginBatchStats();
        if (beginStats != null) {
            System.out.printf("LEDGER_BEGIN_BATCH groups=%d requests=%d fallbacks=%d avg_size=%.2f%n",
                    beginStats.groups(), beginStats.requests(), beginStats.fallbacks(),
                    beginStats.groups() == 0 ? 0.0 : (double) beginStats.requests() / beginStats.groups());
        }
        var localReserveStats = manager.ledgerLocalReserveStats();
        if (localReserveStats != null) {
            System.out.printf("LEDGER_LOCAL_RESERVE_BATCH groups=%d requests=%d fallbacks=%d avg_size=%.2f%n",
                    localReserveStats.groups(), localReserveStats.requests(), localReserveStats.fallbacks(),
                    localReserveStats.groups() == 0 ? 0.0
                            : (double) localReserveStats.requests() / localReserveStats.groups());
        }
        var localCompletionStats = manager.ledgerLocalCompletionStats();
        if (localCompletionStats != null) {
            System.out.printf("LEDGER_LOCAL_COMPLETION_BATCH groups=%d requests=%d fallbacks=%d avg_size=%.2f%n",
                    localCompletionStats.groups(), localCompletionStats.requests(), localCompletionStats.fallbacks(),
                    localCompletionStats.groups() == 0 ? 0.0
                            : (double) localCompletionStats.requests() / localCompletionStats.groups());
        }
        if (stageMetrics != null) {
            TransactionMetricsFormatter.formatNonEmpty(stageMetrics).forEach(System.out::println);
        }
    }

    /**
     * Create local benchmark connection pool with fixed receipt.
     *
     * @param key business key
     * @param database database
     * @return the operation result
     */
    private static HikariDataSource pool(final String key, final String database) {
        String url = env(key, "jdbc:mysql://127.0.0.1:23306/" + database);
        return (HikariDataSource) MysqlDataSourceFactory.create(url,
                env("AZIR_TX_USERNAME", "root"), env("AZIR_TX_PASSWORD", "root"));
    }

    /**
     * Same database but independent budget background connection pool is used for recovery and GC.
     *
     * @param key business key
     * @param database database
     * @return the operation result
     */
    private static HikariDataSource backgroundPool(final String key, final String database) {
        String url = env(key, "jdbc:mysql://127.0.0.1:23306/" + database);
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url,
                env("AZIR_TX_USERNAME", "root"), env("AZIR_TX_PASSWORD", "root"));
    }

    /**
     * Clear all old control records and segregated book structures and prohibit re-use online agreement status.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void resetControl(final HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_publication_segment_slot_v1");
            statement.execute("drop table if exists mvcc_publication_segment_v1");
            statement.execute("drop table if exists mvcc_segment_publication_state_v1");
            statement.execute("drop table if exists mvcc_publication_state_v1_legacy_archived");
            statement.execute("drop table if exists mvcc_snapshot_lease");
            statement.execute("drop table if exists mvcc_legacy_lease_gate_v1");
            statement.execute("drop table if exists mvcc_protocol_epoch_v1");
            statement.execute("drop table if exists mvcc_snapshot_lease_v2");
            statement.execute("drop table if exists mvcc_coordinator_snapshot_lease_v1");
            statement.execute("drop table if exists mvcc_publication_entry_v1");
            statement.execute("drop table if exists mvcc_publication_state_v1");
            statement.executeUpdate("delete from mvcc_txn_finalized");
            statement.executeUpdate("delete from mvcc_txn");
            statement.executeUpdate("update mvcc_tso set current_value = 100 where sequence_name = 'global'");
            statement.executeUpdate("update mvcc_safe_point set safe_point = 0 where store_id = 'global'");
        }
    }

    /**
     * Reset version, intent, terminal state and local safe point for an shard.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void resetShard(final HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("delete from mvcc_local_safe_point");
        }
    }

    /**
     * Statistics foreground confirms the backlog of throughput, transaction tail delay and walk materialization.
     *
     * @param manager manager
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param shardConcurrency shard concurrency
     * @param asyncCommit async commit
     * @param windowSeconds window seconds
     * @param writeShards write shards
     * @param readOnly read only
     * @throws Exception if the operation cannot complete
     */
    private static void run(final TransactionManager manager, final int concurrency, final int durationSeconds,
                            final int shardConcurrency, final boolean asyncCommit, final int windowSeconds,
                            final int writeShards, final boolean readOnly) throws Exception {
        if (readOnly) {
            manager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                manager.put(tx, "shard-1", "ledger:read:seed", bytes("seed"));
                return null;
            });
        }
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicLong sequence = new AtomicLong();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                        while (System.nanoTime() < deadline) {
                            attempts.increment();
                            long started = System.nanoTime();
                            try {
                                manager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                                    if (readOnly) {
                                        manager.get(tx, "shard-1", "ledger:read:seed").orElseThrow();
                                    } else {
                                        long key = sequence.incrementAndGet();
                                        manager.put(tx, "shard-1", "ledger:one:" + key, bytes("v" + key));
                                        if (writeShards == 2) {
                                            manager.put(tx, "shard-2", "ledger:two:" + key, bytes("v" + key));
                                        }
                                    }
                                    return null;
                                });
                                committed.increment();
                                latencies.add(System.nanoTime() - started);
                            } catch (final RuntimeException ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null, describeFailure(ex));
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
            start.countDown();
            Thread reporter = Thread.ofVirtual().name("mvcc-ledger-bench-window").start(() -> reportWindows(manager, committed, failures, windowSeconds));
            done.await();
            reporter.interrupt();
            reporter.join();
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
            double p99Millis = sorted.length == 0 ? Double.NaN
                    : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
            var stats = manager.runtimeStats();
            String mode = Boolean.parseBoolean(env("AZIR_TX_SEGMENT_RUNTIME", "false")) ? "SEGMENT" : "LEDGER";
            System.out.printf("%s_BENCH concurrency=%d duration_s=%.2f read_only=%s write_shards=%d shard_concurrency=%d async_commit=%s attempts=%d committed=%d failures=%d "
                    + "commit_tps=%.2f p99_ms=%.2f async_queued=%d background_failures=%d first_failure=%s%n",
                    mode, concurrency, seconds, readOnly, writeShards, shardConcurrency, asyncCommit,
                    attempts.sum(), committed.sum(), failures.sum(),
                    committed.sum() / seconds, p99Millis, stats.asyncQueued(), stats.backgroundFailures(),
                    firstFailure.get() == null ? "-" : firstFailure.get());
        }
    }

    /**
     * Each fixed window print actually completes the throughput and batch scales to recognize degradation during long run.
     *
     * @param manager manager
     * @param committed committed
     * @param failures failures
     * @param windowSeconds window seconds
     */
    private static void reportWindows(final TransactionManager manager, final LongAdder committed, final LongAdder failures,
                                      final int windowSeconds) {
        long previousCommitted = 0;
        long previousFailures = 0;
        long previousGroups = 0;
        long previousRequests = 0;
        long previousAt = System.nanoTime();
        int window = 0;
        while (true) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(windowSeconds));
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
            long now = System.nanoTime();
            long currentCommitted = committed.sum();
            long currentFailures = failures.sum();
            var batch = manager.ledgerBatchStats();
            var segmented = manager.segmentBatchStats();
            long totalGroups = batch != null ? batch.groups()
                    : segmented == null ? 0 : segmented.groups();
            long totalRequests = batch != null ? batch.requests()
                    : segmented == null ? 0 : segmented.requests();
            long groups = totalGroups - previousGroups;
            long requests = totalRequests - previousRequests;
            String mode = Boolean.parseBoolean(env("AZIR_TX_SEGMENT_RUNTIME", "false")) ? "SEGMENT" : "LEDGER";
            System.out.printf("%s_WINDOW index=%d elapsed_s=%.2f commit_tps=%.2f committed=%d failures=%d "
                    + "batch_groups=%d batch_avg_size=%.2f host_load=%.2f%n",
                    mode, ++window, (now - previousAt) / 1_000_000_000.0,
                    (currentCommitted - previousCommitted) / ((now - previousAt) / 1_000_000_000.0),
                    currentCommitted - previousCommitted, currentFailures - previousFailures,
                    groups, groups == 0 ? 0.0 : (double) requests / groups,
                    ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
            previousCommitted = currentCommitted;
            previousFailures = currentFailures;
            previousGroups = totalGroups;
            previousRequests = totalRequests;
            previousAt = now;
        }
    }

    /**
     * Reads a single environment parameter and uses the default value for short local only.
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
     * Encodes the writing value as a binary format for the storage adapter.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Keeps SQLSate and error codes with an abnormal chain, distinguishing the protocol rollback from connection unknown outcome.
     *
     * @param failure failure
     * @return the operation result
     */
    private static String describeFailure(final Throwable failure) {
        StringBuilder result = new StringBuilder();
        for (Throwable cause = failure; cause != null && result.length() < 800; cause = cause.getCause()) {
            if (!result.isEmpty()) {
                result.append(" <- ");
            }
            result.append(cause.getClass().getSimpleName()).append(':').append(cause.getMessage());
            if (cause instanceof SQLException sql) {
                result.append("[state=").append(sql.getSQLState())
                        .append(",code=").append(sql.getErrorCode()).append(']');
            }
        }
        return result.toString();
    }
}

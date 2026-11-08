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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.gc.SafePointStore;
import com.flyingzc.azir.gc.mysql.MysqlSafePointStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.metrics.InMemoryTransactionMetrics;
import com.flyingzc.azir.metrics.NoOpTransactionMetrics;
import com.flyingzc.azir.metrics.TransactionMetricsFormatter;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlSegmentTimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import com.flyingzc.azir.txn.ConflictMode;
import com.flyingzc.azir.txn.CrossShardCommitPolicy;
import com.flyingzc.azir.txn.LockWaitPolicy;
import com.flyingzc.azir.txn.TransactionManager;
import com.flyingzc.azir.txn.TransactionOptions;
import com.flyingzc.azir.txn.TransactionRetryPolicy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MySQL MVCC transaction benchmark test entrance.
 */
public final class MysqlTxnBenchmark {

    /**
     * Construct a MysqlTxnBenchmark instance.
     */
    private MysqlTxnBenchmark() {
    }

    /**
     * Run the application entry point.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());

        try {
            MysqlTimestampOracle mysqlTso = new MysqlTimestampOracle(controlDataSource);
            MysqlSegmentTimestampOracle segmentTso = new MysqlSegmentTimestampOracle(controlDataSource, config.tsoBatchSize());
            TimestampOracle selectedTso = config.segmentTso()
                    ? segmentTso
                    : mysqlTso;
            // Control mode only covers concurrent sheet capacity, still using the same durable storage TSO authority.
            TimestampOracle tso = config.legacyControl() ? selectedTso::nextTimestamp : selectedTso;
            TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
            SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
            MysqlStorageAdapter shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore);
            MysqlStorageAdapter shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore);

            shard1.initialize();
            shard2.initialize();
            mysqlTso.initialize(100);
            segmentTso.initialize(100);
            ((MysqlTransactionMetadataStore) metadataStore).initialize();
            ((MysqlSafePointStore) safePointStore).initialize(0L);

            if (config.resetTables()) {
                reset(controlDataSource, "mvcc_txn_finalized", "mvcc_snapshot_lease", "mvcc_txn", "mvcc_tso", "mvcc_safe_point");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard1.initialize();
                reset(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard2.initialize();
                mysqlTso.initialize(100);
                segmentTso.initialize(100);
                ((MysqlTransactionMetadataStore) metadataStore).initialize();
                ((MysqlSafePointStore) safePointStore).initialize(0L);
            }

            InMemoryTransactionMetrics metrics = config.stageMetrics()
                    ? new InMemoryTransactionMetrics()
                    : null;
            BenchmarkResult result;
            try (
                    TransactionManager transactionManager = new TransactionManager(
                            tso,
                            metadataStore,
                            safePointStore,
                            new TransactionOptions(
                                    new LockWaitPolicy(
                                            config.conflictModeSetting(),
                                            config.lockWaitTimeoutMs(),
                                            config.lockWaitBackoffInitialMs(),
                                            config.lockWaitBackoffMaxMs(),
                                            config.lockWaitBackoffMultiplier(),
                                            config.lockWaitBackoffJitter()),
                                    new TransactionRetryPolicy(
                                            config.maxAttempts(),
                                            config.retryBackoffInitialMs(),
                                            config.retryBackoffMaxMs(),
                                            config.retryBackoffMultiplier(),
                                            config.retryBackoffJitter()),
                                    new CrossShardCommitPolicy(
                                            config.parallelPrewrite(),
                                            config.parallelPrepare(),
                                            config.parallelCommit(),
                                            config.parallelRollback(),
                                            config.asyncCommit(),
                                            config.asyncCommitWorkers()),
                                    config.bufferWrites(),
                                    config.lazyBeginMetadata(),
                                    config.deferIntentCleanup()),
                            Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                            metrics == null ? NoOpTransactionMetrics.INSTANCE : metrics)) {
                result = runBenchmark(transactionManager, config);
            }
            System.out.println(result.toLine(config));
            if (metrics != null) {
                TransactionMetricsFormatter.formatNonEmpty(metrics).forEach(System.out::println);
            }
            // When a non-conflict or total commit process fails, the exit code must clearly mark the evidence as invalid.
            if (result.failures() != 0 || result.committed() == 0) {
                throw new IllegalStateException("Azir cross-shard benchmark is invalid: "
                        + (result.firstFailure() == null ? "no committed transaction" : result.firstFailure()));
            }
        } finally {
            close(controlDataSource);
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

    /**
     * Run the benchmark and return its statistics.
     *
     * @param transactionManager transaction manager
     * @param config config
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static BenchmarkResult runBenchmark(final TransactionManager transactionManager, final Config config) throws Exception {
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder conflicts = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicLong keySequence = new AtomicLong();
        AtomicReference<String> firstFailure = new AtomicReference<>();

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
                            transactionManager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                                if (config.conflictMode()) {
                                    transactionManager.put(tx, "shard-1", "bench:hot:1", bytes("v"));
                                    transactionManager.put(tx, "shard-2", "bench:hot:2", bytes("v"));
                                } else {
                                    long id = keySequence.incrementAndGet();
                                    transactionManager.put(tx, "shard-1", "bench:s1:" + id, bytes("v" + id));
                                    transactionManager.put(tx, "shard-2", "bench:s2:" + id, bytes("v" + id));
                                }
                                return null;
                            });
                            committed.increment();
                        } catch (final MvccException ex) {
                            if (ex.errorCode() == ErrorCode.LOCK_CONFLICT
                                    || ex.errorCode() == ErrorCode.LOCK_WAIT_TIMEOUT
                                    || ex.errorCode() == ErrorCode.WRITE_CONFLICT) {
                                conflicts.increment();
                            } else {
                                failures.increment();
                                firstFailure.compareAndSet(null, formatFailure(ex));
                            }
                        } catch (final RuntimeException ex) {
                            failures.increment();
                            firstFailure.compareAndSet(null, formatFailure(ex));
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

        long endedAt = System.nanoTime();
        double elapsedSeconds = (endedAt - startedAt) / 1_000_000_000.0;
        return new BenchmarkResult(
                elapsedSeconds,
                attempts.sum(),
                committed.sum(),
                conflicts.sum(),
                failures.sum(),
                firstFailure.get());
    }

    /**
     * Encode the string as a byte array.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Format benchmark test failed information.
     *
     * @param throwable throwable
     * @return the operation result
     */
    private static String formatFailure(final Throwable throwable) {
        StringBuilder builder = new StringBuilder();
        builder.append(throwable.getClass().getSimpleName());
        if (throwable instanceof MvccException ex) {
            builder.append(":").append(ex.errorCode());
        }
        if (throwable.getMessage() != null && !throwable.getMessage().isBlank()) {
            builder.append(":").append(throwable.getMessage());
        }
        Throwable cause = throwable.getCause();
        if (cause != null) {
            builder.append(":cause=").append(cause.getClass().getSimpleName());
            if (cause.getMessage() != null && !cause.getMessage().isBlank()) {
                builder.append(":").append(cause.getMessage());
            }
        }
        return builder.toString();
    }

    /**
     * Reset the benchmark tables.
     *
     * @param dataSource database connection source
     * @param tables tables
     * @throws Exception if the operation cannot complete
     */
    private static void reset(final DataSource dataSource, final String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
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
     * @param controlJdbcUrl control jdbc url
     * @param shard1JdbcUrl shard1 jdbc url
     * @param shard2JdbcUrl shard2 jdbc url
     * @param username username
     * @param password password
     * @param resetTables reset tables
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param conflictMode conflict mode
     * @param conflictModeSetting conflict mode setting
     * @param lockWaitTimeoutMs lock wait timeout ms
     * @param lockWaitBackoffInitialMs lock wait backoff initial ms
     * @param lockWaitBackoffMaxMs lock wait backoff max ms
     * @param lockWaitBackoffMultiplier lock wait backoff multiplier
     * @param lockWaitBackoffJitter lock wait backoff jitter
     * @param maxAttempts max attempts
     * @param retryBackoffInitialMs retry backoff initial ms
     * @param retryBackoffMaxMs retry backoff max ms
     * @param retryBackoffMultiplier retry backoff multiplier
     * @param retryBackoffJitter retry backoff jitter
     * @param segmentTso segment tso
     * @param tsoBatchSize tso batch size
     * @param bufferWrites buffer writes
     * @param lazyBeginMetadata lazy begin metadata
     * @param deferIntentCleanup defer intent cleanup
     * @param parallelPrewrite parallel prewrite
     * @param parallelPrepare parallel prepare
     * @param parallelCommit parallel commit
     * @param parallelRollback parallel rollback
     * @param asyncCommit async commit
     * @param asyncCommitWorkers async commit workers
     * @param stageMetrics stage metrics
     * @param legacyControl legacy control
     */
    private record Config(String controlJdbcUrl,
            String shard1JdbcUrl,
            String shard2JdbcUrl,
            String username,
            String password,
            boolean resetTables,
            int concurrency,
            int durationSeconds,
            boolean conflictMode,
            ConflictMode conflictModeSetting,
            long lockWaitTimeoutMs,
            long lockWaitBackoffInitialMs,
            long lockWaitBackoffMaxMs,
            double lockWaitBackoffMultiplier,
            boolean lockWaitBackoffJitter,
            int maxAttempts,
            long retryBackoffInitialMs,
            long retryBackoffMaxMs,
            double retryBackoffMultiplier,
            boolean retryBackoffJitter,
            boolean segmentTso,
            int tsoBatchSize,
            boolean bufferWrites,
            boolean lazyBeginMetadata,
            boolean deferIntentCleanup,
            boolean parallelPrewrite,
            boolean parallelPrepare,
            boolean parallelCommit,
            boolean parallelRollback,
            boolean asyncCommit,
            int asyncCommitWorkers,
            boolean stageMetrics,
            boolean legacyControl) {

        /**
         * Read configuration from environment variables.
         *
         * @return the operation result
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control"),
                    env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1"),
                    env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Boolean.parseBoolean(env("AZIR_TX_BENCH_CONFLICT", "false")),
                    ConflictMode.valueOf(env("AZIR_TX_CONFLICT_MODE", "WAIT")),
                    Long.parseLong(env("AZIR_TX_LOCK_WAIT_TIMEOUT_MS", "5000")),
                    Long.parseLong(env("AZIR_TX_LOCK_WAIT_BACKOFF_INITIAL_MS", "5")),
                    Long.parseLong(env("AZIR_TX_LOCK_WAIT_BACKOFF_MAX_MS", "100")),
                    Double.parseDouble(env("AZIR_TX_LOCK_WAIT_BACKOFF_MULTIPLIER", "2.0")),
                    Boolean.parseBoolean(env("AZIR_TX_LOCK_WAIT_BACKOFF_JITTER", "true")),
                    Integer.parseInt(env("AZIR_TX_RETRY_MAX_ATTEMPTS", "8")),
                    Long.parseLong(env("AZIR_TX_RETRY_BACKOFF_INITIAL_MS", "5")),
                    Long.parseLong(env("AZIR_TX_RETRY_BACKOFF_MAX_MS", "100")),
                    Double.parseDouble(env("AZIR_TX_RETRY_BACKOFF_MULTIPLIER", "2.0")),
                    Boolean.parseBoolean(env("AZIR_TX_RETRY_BACKOFF_JITTER", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_SEGMENT_TSO", "true")),
                    Integer.parseInt(env("AZIR_TX_TSO_BATCH_SIZE", "256")),
                    Boolean.parseBoolean(env("AZIR_TX_BUFFER_WRITES", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_LAZY_BEGIN_METADATA", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_DEFER_INTENT_CLEANUP", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_PREWRITE", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_PREPARE", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_COMMIT", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_ROLLBACK", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_ASYNC_COMMIT", "true")),
                    Integer.parseInt(env("AZIR_TX_ASYNC_COMMIT_WORKERS", "16")),
                    Boolean.parseBoolean(env("AZIR_TX_STAGE_METRICS", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_LEGACY_CONTROL", "false")));
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
     * @param firstFailure first failure
     */
    private record BenchmarkResult(double elapsedSeconds,
            long attempts,
            long committed,
            long conflicts,
            long failures,
            String firstFailure) {

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
            return "BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + (config.conflictMode() ? "conflict" : "no_conflict")
                    + " conflict_mode=" + config.conflictModeSetting()
                    + " retry_max_attempts=" + config.maxAttempts()
                    + " segment_tso=" + config.segmentTso()
                    + " tso_batch_size=" + config.tsoBatchSize()
                    + " legacy_control=" + config.legacyControl()
                    + " buffer_writes=" + config.bufferWrites()
                    + " lazy_begin_metadata=" + config.lazyBeginMetadata()
                    + " defer_intent_cleanup=" + config.deferIntentCleanup()
                    + " parallel_prewrite=" + config.parallelPrewrite()
                    + " parallel_prepare=" + config.parallelPrepare()
                    + " parallel_commit=" + config.parallelCommit()
                    + " parallel_rollback=" + config.parallelRollback()
                    + " async_commit=" + config.asyncCommit()
                    + " stage_metrics=" + config.stageMetrics()
                    + " attempts=" + attempts
                    + " committed=" + committed
                    + " conflicts=" + conflicts
                    + " failures=" + failures
                    + " first_failure=" + (firstFailure == null ? "-" : firstFailure.replace(' ', '_'))
                    + " attempt_tps=" + String.format("%.2f", attemptTps)
                    + " commit_tps=" + String.format("%.2f", commitTps)
                    + " conflict_tps=" + String.format("%.2f", conflictTps);
        }
    }
}

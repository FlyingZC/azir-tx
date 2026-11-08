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
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Step commit prototype benchmark test entrance.
 */
public final class AsyncCommitPrototypeBenchmark {

    /**
     * Construct a AsyncCommitPrototypeBenchmark instance.
     */
    private AsyncCommitPrototypeBenchmark() {
    }

    /**
     * Run the application entry point.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());

        try {
            MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
            TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
            MysqlStorageAdapter shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
            MysqlStorageAdapter shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);

            shard1.initialize();
            shard2.initialize();
            tso.initialize(100);
            ((MysqlTransactionMetadataStore) metadataStore).initialize();

            if (config.resetTables()) {
                reset(controlDataSource, "mvcc_txn_finalized", "mvcc_snapshot_lease", "mvcc_txn", "mvcc_tso");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard1.initialize();
                reset(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard2.initialize();
                tso.initialize(100);
                ((MysqlTransactionMetadataStore) metadataStore).initialize();
            }

            AsyncCommitCoordinator coordinator = new AsyncCommitCoordinator(
                    tso,
                    metadataStore,
                    Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                    config.secondaryFinalizeWorkers());

            try {
                BenchmarkResult result = runBenchmark(coordinator, config);
                System.out.println(result.toLine(config));
            } finally {
                coordinator.close();
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
     * @param coordinator coordinator
     * @param config config
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static BenchmarkResult runBenchmark(final AsyncCommitCoordinator coordinator, final Config config) throws Exception {
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder conflicts = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicLong keySequence = new AtomicLong();

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
                            long id = keySequence.incrementAndGet();
                            coordinator.execute(id, config.asyncCommit());
                            committed.increment();
                        } catch (final MvccException ex) {
                            if (ex.errorCode() == ErrorCode.LOCK_CONFLICT
                                    || ex.errorCode() == ErrorCode.LOCK_WAIT_TIMEOUT
                                    || ex.errorCode() == ErrorCode.WRITE_CONFLICT) {
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

        long frontendFinishedAt = System.nanoTime();
        coordinator.awaitBackgroundDrain();
        long drainedAt = System.nanoTime();
        double frontendSeconds = (frontendFinishedAt - startedAt) / 1_000_000_000.0;
        double totalSeconds = (drainedAt - startedAt) / 1_000_000_000.0;
        return new BenchmarkResult(
                frontendSeconds,
                totalSeconds,
                attempts.sum(),
                committed.sum(),
                conflicts.sum(),
                failures.sum(),
                coordinator.primaryCommits(),
                coordinator.secondaryCommits(),
                coordinator.maxPendingSecondary());
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
     * Encode the string as a byte array.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
     * @param asyncCommit async commit
     * @param secondaryFinalizeWorkers secondary finalize workers
     */
    private record Config(String controlJdbcUrl,
            String shard1JdbcUrl,
            String shard2JdbcUrl,
            String username,
            String password,
            boolean resetTables,
            int concurrency,
            int durationSeconds,
            boolean asyncCommit,
            int secondaryFinalizeWorkers) {

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
                    Boolean.parseBoolean(env("AZIR_TX_ASYNC_COMMIT", "false")),
                    Integer.parseInt(env("AZIR_TX_SECONDARY_FINALIZE_WORKERS", "8")));
        }
    }

    /**
     * Benchmark results.
     *
     * @param frontendSeconds frontend seconds
     * @param totalSeconds total seconds
     * @param attempts attempts
     * @param committed committed
     * @param conflicts conflicts
     * @param failures failures
     * @param primaryCommits primary commits
     * @param secondaryCommits secondary commits
     * @param maxPendingSecondary max pending secondary
     */
    private record BenchmarkResult(double frontendSeconds,
            double totalSeconds,
            long attempts,
            long committed,
            long conflicts,
            long failures,
            long primaryCommits,
            long secondaryCommits,
            int maxPendingSecondary) {

        /**
         * Format benchmark results as a single line.
         *
         * @param config config
         * @return the operation result
         */
        private String toLine(final Config config) {
            double frontendCommitTps = committed / frontendSeconds;
            double secondaryFinalizeTps = secondaryCommits / totalSeconds;
            return "ASYNC_COMMIT_BENCH"
                    + " concurrency=" + config.concurrency()
                    + " frontend_duration_s=" + String.format("%.2f", frontendSeconds)
                    + " total_duration_s=" + String.format("%.2f", totalSeconds)
                    + " mode=" + (config.asyncCommit() ? "async_primary" : "sync_all")
                    + " secondary_finalize_workers=" + config.secondaryFinalizeWorkers()
                    + " attempts=" + attempts
                    + " committed=" + committed
                    + " conflicts=" + conflicts
                    + " failures=" + failures
                    + " primary_commits=" + primaryCommits
                    + " secondary_commits=" + secondaryCommits
                    + " max_pending_secondary=" + maxPendingSecondary
                    + " frontend_commit_tps=" + String.format("%.2f", frontendCommitTps)
                    + " secondary_finalize_tps=" + String.format("%.2f", secondaryFinalizeTps);
        }
    }

    /**
     * Step commit prototype coordinator.
     */
    private static final class AsyncCommitCoordinator implements AutoCloseable {

        private final TimestampOracle timestampOracle;

        private final TransactionMetadataStore metadataStore;

        private final Map<String, StorageAdapter> adapters;

        private final ExecutorService participantExecutor;

        private final ExecutorService secondaryExecutor;

        private final LongAdder primaryCommits = new LongAdder();

        private final LongAdder secondaryCommits = new LongAdder();

        private final AtomicInteger pendingSecondary = new AtomicInteger();

        private final AtomicInteger maxPendingSecondary = new AtomicInteger();

        /**
         * Construct a AsyncCommitCoordinator instance.
         *
         * @param timestampOracle timestamp oracle
         * @param metadataStore metadata store
         * @param adapters adapters
         * @param secondaryFinalizeWorkers secondary finalize workers
         * @throws IllegalArgumentException if the operation cannot complete
         */
        private AsyncCommitCoordinator(final TimestampOracle timestampOracle,
                                       final TransactionMetadataStore metadataStore,
                                       final Map<String, StorageAdapter> adapters,
                                       final int secondaryFinalizeWorkers) {
            this.timestampOracle = timestampOracle;
            this.metadataStore = metadataStore;
            this.adapters = adapters;
            // The prototype bypasses TransactionManager and must explicitly pass the same decision authority.
            for (StorageAdapter adapter : adapters.values()) {
                adapter.configureMetadataStore(metadataStore);
                if (!adapter.supportsCommittedIntentResolution()) {
                    throw new IllegalArgumentException("async prototype requires committed intent resolution");
                }
            }
            this.participantExecutor = Executors.newFixedThreadPool(Math.max(2, adapters.size() * 2));
            this.secondaryExecutor = Executors.newFixedThreadPool(secondaryFinalizeWorkers);
        }

        /**
         * Execute one prototype transaction.
         *
         * @param sequence sequence
         * @param asyncCommit async commit
         */
        private void execute(final long sequence, final boolean asyncCommit) {
            PrototypeTransaction tx = begin();
            try {
                tx.buffer("shard-1", Mutation.put("bench:s1:" + sequence, bytes("v" + sequence)));
                tx.buffer("shard-2", Mutation.put("bench:s2:" + sequence, bytes("v" + sequence)));
                runAcrossParticipants(tx.participants(), shardId -> prewrite(tx, shardId), true);
                metadataStore.markPreparing(tx.txId(), tx.participants());
                runAcrossParticipants(tx.participants(), shardId -> adapters.get(shardId).prepare(tx.txId()), true);
                long commitTs = timestampOracle.nextTimestamp();
                metadataStore.markCommitted(tx.txId(), commitTs);
                commitPrimary(tx, commitTs);
                if (asyncCommit) {
                    enqueueSecondaryCommit(tx, commitTs);
                } else {
                    commitSecondaries(tx, commitTs);
                }
                tx.status(TxStatus.COMMITTED);
            } catch (final RuntimeException ex) {
                rollbackQuietly(tx);
                throw ex;
            }
        }

        /**
         * Begin a new transaction.
         *
         * @return the operation result
         */
        private PrototypeTransaction begin() {
            long txId = timestampOracle.nextTimestamp();
            long startTs = timestampOracle.nextTimestamp();
            metadataStore.create(txId, startTs, IsolationLevel.REPEATABLE_READ);
            return new PrototypeTransaction(txId, startTs);
        }

        /**
         * Prewrite the prototype transaction.
         *
         * @param tx transaction context
         * @param shardId logical shard identifier
         */
        private void prewrite(final PrototypeTransaction tx, final String shardId) {
            tx.markParticipant(shardId);
            metadataStore.registerParticipants(tx.txId(), Set.of(shardId));
            adapters.get(shardId).prewrite(tx.txId(), tx.startTs(), tx.mutations(shardId));
        }

        /**
         * Commit primary shard writes.
         *
         * @param tx transaction context
         * @param commitTs durable commit timestamp
         */
        private void commitPrimary(final PrototypeTransaction tx, final long commitTs) {
            adapters.get(tx.primaryShard()).commit(tx.txId(), commitTs);
            primaryCommits.increment();
        }

        /**
         * From shard commit job into background queue.
         *
         * @param tx transaction context
         * @param commitTs durable commit timestamp
         */
        private void enqueueSecondaryCommit(final PrototypeTransaction tx, final long commitTs) {
            for (String shardId : tx.secondaryShards()) {
                incrementPending();
                secondaryExecutor.submit(() -> {
                    try {
                        adapters.get(shardId).commit(tx.txId(), commitTs);
                        secondaryCommits.increment();
                    } finally {
                        pendingSecondary.decrementAndGet();
                    }
                });
            }
        }

        /**
         * Commit All written from shard.
         *
         * @param tx transaction context
         * @param commitTs durable commit timestamp
         */
        private void commitSecondaries(final PrototypeTransaction tx, final long commitTs) {
            for (String shardId : tx.secondaryShards()) {
                adapters.get(shardId).commit(tx.txId(), commitTs);
                secondaryCommits.increment();
            }
        }

        /**
         * Roll back the prototype transaction without masking the original failure.
         *
         * @param tx transaction context
         */
        private void rollbackQuietly(final PrototypeTransaction tx) {
            if (tx.status() != TxStatus.ACTIVE) {
                return;
            }
            try {
                metadataStore.markAborted(tx.txId());
            } catch (final RuntimeException ignored) {
                return;
            }
            try {
                runAcrossParticipants(tx.participants(), shardId -> adapters.get(shardId).abort(tx.txId()), true);
            } finally {
                tx.status(TxStatus.ABORTED);
            }
        }

        /**
         * Waiting for background Step commit task emptied.
         *
         * @throws InterruptedException if the operation cannot complete
         */
        private void awaitBackgroundDrain() throws InterruptedException {
            while (pendingSecondary.get() > 0) {
                Thread.sleep(10L);
            }
        }

        /**
         * Returns the primary shard commit count.
         *
         * @return the operation result
         */
        private long primaryCommits() {
            return primaryCommits.sum();
        }

        /**
         * Returns the count from shard commit.
         *
         * @return the operation result
         */
        private long secondaryCommits() {
            return secondaryCommits.sum();
        }

        /**
         * Returns the maximum number to be processed from shard commit.
         *
         * @return the operation result
         */
        private int maxPendingSecondary() {
            return maxPendingSecondary.get();
        }

        /**
         * Incremental to process from shard commit.
         */
        private void incrementPending() {
            int value = pendingSecondary.incrementAndGet();
            maxPendingSecondary.accumulateAndGet(value, Math::max);
        }

        /**
         * Close resources and background executors.
         *
         * @throws InterruptedException if the operation cannot complete
         */
        @Override
        public void close() throws InterruptedException {
            participantExecutor.shutdown();
            secondaryExecutor.shutdown();
            participantExecutor.awaitTermination(10, TimeUnit.SECONDS);
            secondaryExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }

        /**
         * Execute the operation across participating shards.
         *
         * @param participants participants
         * @param action action
         * @param parallel parallel
         * @throws MvccException if the operation cannot complete
         */
        private void runAcrossParticipants(final Set<String> participants,
                                           final Consumer<String> action,
                                           final boolean parallel) {
            if (!parallel || participants.size() <= 1) {
                for (String participant : participants) {
                    action.accept(participant);
                }
                return;
            }
            ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
            List<CompletableFuture<Void>> futures = participants.stream()
                    .map(participant -> CompletableFuture.runAsync(() -> {
                        try {
                            action.accept(participant);
                        } catch (final Throwable ex) {
                            failures.add(ex);
                            throw ex;
                        }
                    }, participantExecutor))
                    .toList();
            try {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            } catch (final CompletionException ignored) {
                Throwable failure = failures.peek();
                if (failure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new MvccException(
                        ErrorCode.STORAGE_UNAVAILABLE,
                        "parallel participant action failed",
                        true,
                        null,
                        failure);
            }
        }
    }

    /**
     * Prototype transaction context.
     */
    private static final class PrototypeTransaction {

        private final long txId;

        private final long startTs;

        private final Map<String, List<Mutation>> bufferedMutations = new LinkedHashMap<>();

        private TxStatus status = TxStatus.ACTIVE;

        /**
         * Construct a PrototypeTransaction instance.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         */
        private PrototypeTransaction(final long txId, final long startTs) {
            this.txId = txId;
            this.startTs = startTs;
        }

        /**
         * Return the transaction identifier.
         *
         * @return the operation result
         */
        private long txId() {
            return txId;
        }

        /**
         * Return the transaction start timestamp.
         *
         * @return the operation result
         */
        private long startTs() {
            return startTs;
        }

        /**
         * Buffer a shard mutation.
         *
         * @param shardId logical shard identifier
         * @param mutation mutation
         */
        private void buffer(final String shardId, final Mutation mutation) {
            bufferedMutations.computeIfAbsent(shardId, ignored -> new ArrayList<>()).add(mutation);
        }

        /**
         * Return buffered mutations for the shard.
         *
         * @param shardId logical shard identifier
         * @return the operation result
         */
        private List<Mutation> mutations(final String shardId) {
            return bufferedMutations.getOrDefault(shardId, List.of());
        }

        /**
         * Register a participating shard.
         *
         * @param shardId logical shard identifier
         */
        private void markParticipant(final String shardId) {
            bufferedMutations.computeIfAbsent(shardId, ignored -> new ArrayList<>());
        }

        /**
         * Return the participating shards.
         *
         * @return the operation result
         */
        private Set<String> participants() {
            return bufferedMutations.keySet();
        }

        /**
         * Returns prototype transaction primary shard.
         *
         * @return the operation result
         */
        private String primaryShard() {
            return "shard-1";
        }

        /**
         * Returns the prototype transaction from the shard list.
         *
         * @return the operation result
         */
        private List<String> secondaryShards() {
            return participants().stream().filter(shardId -> !shardId.equals(primaryShard())).toList();
        }

        /**
         * Return the transaction state.
         *
         * @return the operation result
         */
        private TxStatus status() {
            return status;
        }

        /**
         * Return the transaction state.
         *
         * @param status status
         */
        private void status(final TxStatus status) {
            this.status = status;
        }
    }
}

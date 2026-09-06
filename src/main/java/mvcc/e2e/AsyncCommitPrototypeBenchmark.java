package mvcc.e2e;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.storage.Mutation;
import mvcc.storage.StorageAdapter;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.TimestampOracle;
import mvcc.tso.mysql.MysqlTimestampOracle;

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
 * 异步提交原型基准测试入口。
 */
public final class AsyncCommitPrototypeBenchmark {
    /**
     * 构造 AsyncCommitPrototypeBenchmark 实例。
     */
    private AsyncCommitPrototypeBenchmark() {
    }

    /**
     * 程序入口方法。
     */
    public static void main(String[] args) throws Exception {
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
                reset(controlDataSource, "mvcc_txn", "mvcc_tso");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
                reset(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
                tso.initialize(100);
                ((MysqlTransactionMetadataStore) metadataStore).initialize();
            }

            AsyncCommitCoordinator coordinator = new AsyncCommitCoordinator(
                    tso,
                    metadataStore,
                    Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                    config.secondaryFinalizeWorkers()
            );

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
     * 执行基准测试并返回统计结果。
     */
    private static BenchmarkResult runBenchmark(AsyncCommitCoordinator coordinator, Config config) throws Exception {
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
                        } catch (MvccException ex) {
                            if (ex.errorCode() == ErrorCode.LOCK_CONFLICT
                                    || ex.errorCode() == ErrorCode.LOCK_WAIT_TIMEOUT
                                    || ex.errorCode() == ErrorCode.WRITE_CONFLICT) {
                                conflicts.increment();
                            } else {
                                failures.increment();
                            }
                        } catch (RuntimeException ex) {
                            failures.increment();
                        }
                    }
                } catch (InterruptedException ex) {
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
                coordinator.maxPendingSecondary()
        );
    }

    /**
     * 重置基准测试表数据。
     */
    private static void reset(DataSource dataSource, String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    /**
     * 关闭资源或后台执行器。
     */
    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * 将字符串转换为字节数组。
     */
    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 基准测试配置对象。
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
         * 从环境变量读取配置对象。
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
                    Integer.parseInt(env("AZIR_TX_SECONDARY_FINALIZE_WORKERS", "8"))
            );
        }
    }

    /**
     * 基准测试结果对象。
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
         * 将基准测试结果格式化为一行文本。
         */
        private String toLine(Config config) {
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
     * 异步提交原型协调器。
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
         * 构造 AsyncCommitCoordinator 实例。
         */
        private AsyncCommitCoordinator(TimestampOracle timestampOracle,
                                       TransactionMetadataStore metadataStore,
                                       Map<String, StorageAdapter> adapters,
                                       int secondaryFinalizeWorkers) {
            this.timestampOracle = timestampOracle;
            this.metadataStore = metadataStore;
            this.adapters = adapters;
            this.participantExecutor = Executors.newFixedThreadPool(Math.max(2, adapters.size() * 2));
            this.secondaryExecutor = Executors.newFixedThreadPool(secondaryFinalizeWorkers);
        }

        /**
         * 执行一次原型事务流程。
         */
        private void execute(long sequence, boolean asyncCommit) {
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
            } catch (RuntimeException ex) {
                rollbackQuietly(tx);
                throw ex;
            }
        }

        /**
         * 开启一个新事务。
         */
        private PrototypeTransaction begin() {
            long txId = timestampOracle.nextTimestamp();
            long startTs = timestampOracle.nextTimestamp();
            metadataStore.create(txId, startTs, IsolationLevel.REPEATABLE_READ);
            return new PrototypeTransaction(txId, startTs);
        }

        /**
         * 执行原型事务预写。
         */
        private void prewrite(PrototypeTransaction tx, String shardId) {
            adapters.get(shardId).prewrite(tx.txId(), tx.startTs(), tx.mutations(shardId));
            tx.markParticipant(shardId);
        }

        /**
         * 提交主分片写入。
         */
        private void commitPrimary(PrototypeTransaction tx, long commitTs) {
            adapters.get(tx.primaryShard()).commit(tx.txId(), commitTs);
            primaryCommits.increment();
        }

        /**
         * 将从分片提交任务放入后台队列。
         */
        private void enqueueSecondaryCommit(PrototypeTransaction tx, long commitTs) {
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
         * 提交全部从分片写入。
         */
        private void commitSecondaries(PrototypeTransaction tx, long commitTs) {
            for (String shardId : tx.secondaryShards()) {
                adapters.get(shardId).commit(tx.txId(), commitTs);
                secondaryCommits.increment();
            }
        }

        /**
         * 安静回滚原型事务。
         */
        private void rollbackQuietly(PrototypeTransaction tx) {
            if (tx.status() != TxStatus.ACTIVE) {
                return;
            }
            try {
                metadataStore.markAborted(tx.txId());
            } catch (RuntimeException ignored) {
                return;
            }
            try {
                runAcrossParticipants(tx.participants(), shardId -> adapters.get(shardId).abort(tx.txId()), true);
            } finally {
                tx.status(TxStatus.ABORTED);
            }
        }

        /**
         * 等待后台异步提交任务排空。
         */
        private void awaitBackgroundDrain() throws InterruptedException {
            while (pendingSecondary.get() > 0) {
                Thread.sleep(10L);
            }
        }

        /**
         * 返回主分片提交计数。
         */
        private long primaryCommits() {
            return primaryCommits.sum();
        }

        /**
         * 返回从分片提交计数。
         */
        private long secondaryCommits() {
            return secondaryCommits.sum();
        }

        /**
         * 返回最大待处理从分片提交数。
         */
        private int maxPendingSecondary() {
            return maxPendingSecondary.get();
        }

        /**
         * 递增待处理从分片提交计数。
         */
        private void incrementPending() {
            int value = pendingSecondary.incrementAndGet();
            maxPendingSecondary.accumulateAndGet(value, Math::max);
        }

        /**
         * 关闭资源或后台执行器。
         */
        @Override
        public void close() throws InterruptedException {
            participantExecutor.shutdown();
            secondaryExecutor.shutdown();
            participantExecutor.awaitTermination(10, TimeUnit.SECONDS);
            secondaryExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }

        /**
         * 在多个参与分片上执行操作。
         */
        private void runAcrossParticipants(Set<String> participants,
                                           Consumer<String> action,
                                           boolean parallel) {
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
                        } catch (Throwable ex) {
                            failures.add(ex);
                            throw ex;
                        }
                    }, participantExecutor))
                    .toList();
            try {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            } catch (CompletionException ignored) {
                Throwable failure = failures.peek();
                if (failure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new MvccException(
                        ErrorCode.STORAGE_UNAVAILABLE,
                        "parallel participant action failed",
                        true,
                        null,
                        failure
                );
            }
        }
    }

    /**
     * 原型事务上下文。
     */
    private static final class PrototypeTransaction {
        private final long txId;
        private final long startTs;
        private final Map<String, List<Mutation>> bufferedMutations = new LinkedHashMap<>();
        private TxStatus status = TxStatus.ACTIVE;

        /**
         * 构造 PrototypeTransaction 实例。
         */
        private PrototypeTransaction(long txId, long startTs) {
            this.txId = txId;
            this.startTs = startTs;
        }

        /**
         * 返回事务编号。
         */
        private long txId() {
            return txId;
        }

        /**
         * 返回事务开始时间戳。
         */
        private long startTs() {
            return startTs;
        }

        /**
         * 缓存一次分片写入变更。
         */
        private void buffer(String shardId, Mutation mutation) {
            bufferedMutations.computeIfAbsent(shardId, ignored -> new ArrayList<>()).add(mutation);
        }

        /**
         * 返回指定分片的写入变更列表。
         */
        private List<Mutation> mutations(String shardId) {
            return bufferedMutations.getOrDefault(shardId, List.of());
        }

        /**
         * 标记事务参与分片。
         */
        private void markParticipant(String shardId) {
            bufferedMutations.computeIfAbsent(shardId, ignored -> new ArrayList<>());
        }

        /**
         * 返回事务参与分片集合。
         */
        private Set<String> participants() {
            return bufferedMutations.keySet();
        }

        /**
         * 返回原型事务主分片。
         */
        private String primaryShard() {
            return "shard-1";
        }

        /**
         * 返回原型事务从分片列表。
         */
        private List<String> secondaryShards() {
            return participants().stream().filter(shardId -> !shardId.equals(primaryShard())).toList();
        }

        /**
         * 返回事务状态。
         */
        private TxStatus status() {
            return status;
        }

        /**
         * 返回事务状态。
         */
        private void status(TxStatus status) {
            this.status = status;
        }
    }

    /**
     * 读取字符串环境变量配置。
     */
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

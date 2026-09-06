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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * 跨分片写路径原型基准测试入口。
 */
public final class CrossShardWritePathPrototypeBenchmark {
    /**
     * 构造 CrossShardWritePathPrototypeBenchmark 实例。
     */
    private CrossShardWritePathPrototypeBenchmark() {
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

            PrototypeCoordinator coordinator = new PrototypeCoordinator(
                    tso,
                    metadataStore,
                    Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                    config.parallelPrewrite(),
                    config.parallelPrepare(),
                    config.parallelCommit(),
                    config.parallelRollback()
            );

            BenchmarkResult result = runBenchmark(coordinator, config);
            System.out.println(result.toLine(config));
        } finally {
            close(controlDataSource);
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

    /**
     * 执行基准测试并返回统计结果。
     */
    private static BenchmarkResult runBenchmark(PrototypeCoordinator coordinator, Config config) throws Exception {
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
                            coordinator.execute(config.bufferedMode(), id);
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

        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        return new BenchmarkResult(elapsedSeconds, attempts.sum(), committed.sum(), conflicts.sum(), failures.sum());
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
                          boolean bufferedMode,
                          boolean parallelPrewrite,
                          boolean parallelPrepare,
                          boolean parallelCommit,
                          boolean parallelRollback) {
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
                    Boolean.parseBoolean(env("AZIR_TX_BUFFERED_MODE", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_PREWRITE", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_PREPARE", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_COMMIT", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_ROLLBACK", "false"))
            );
        }
    }

    /**
     * 基准测试结果对象。
     */
    private record BenchmarkResult(double elapsedSeconds,
                                   long attempts,
                                   long committed,
                                   long conflicts,
                                   long failures) {
        /**
         * 将基准测试结果格式化为一行文本。
         */
        private String toLine(Config config) {
            double attemptTps = attempts / elapsedSeconds;
            double commitTps = committed / elapsedSeconds;
            double conflictTps = conflicts / elapsedSeconds;
            return "WRITE_PATH_BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + (config.bufferedMode() ? "buffered" : "eager")
                    + " parallel_prewrite=" + config.parallelPrewrite()
                    + " parallel_prepare=" + config.parallelPrepare()
                    + " parallel_commit=" + config.parallelCommit()
                    + " parallel_rollback=" + config.parallelRollback()
                    + " attempts=" + attempts
                    + " committed=" + committed
                    + " conflicts=" + conflicts
                    + " failures=" + failures
                    + " attempt_tps=" + String.format("%.2f", attemptTps)
                    + " commit_tps=" + String.format("%.2f", commitTps)
                    + " conflict_tps=" + String.format("%.2f", conflictTps);
        }
    }

    /**
     * 跨分片写路径原型协调器。
     */
    private static final class PrototypeCoordinator {
        private final TimestampOracle timestampOracle;
        private final TransactionMetadataStore metadataStore;
        private final Map<String, StorageAdapter> adapters;
        private final boolean parallelPrewrite;
        private final boolean parallelPrepare;
        private final boolean parallelCommit;
        private final boolean parallelRollback;

        /**
         * 构造 PrototypeCoordinator 实例。
         */
        private PrototypeCoordinator(TimestampOracle timestampOracle,
                                     TransactionMetadataStore metadataStore,
                                     Map<String, StorageAdapter> adapters,
                                     boolean parallelPrewrite,
                                     boolean parallelPrepare,
                                     boolean parallelCommit,
                                     boolean parallelRollback) {
            this.timestampOracle = timestampOracle;
            this.metadataStore = metadataStore;
            this.adapters = adapters;
            this.parallelPrewrite = parallelPrewrite;
            this.parallelPrepare = parallelPrepare;
            this.parallelCommit = parallelCommit;
            this.parallelRollback = parallelRollback;
        }

        /**
         * 执行一次原型事务流程。
         */
        private void execute(boolean bufferedMode, long sequence) {
            PrototypeTransaction tx = begin();
            try {
                Mutation shard1Mutation = Mutation.put("bench:s1:" + sequence, bytes("v" + sequence));
                Mutation shard2Mutation = Mutation.put("bench:s2:" + sequence, bytes("v" + sequence));
                if (bufferedMode) {
                    tx.buffer("shard-1", shard1Mutation);
                    tx.buffer("shard-2", shard2Mutation);
                } else {
                    prewrite(tx, "shard-1", List.of(shard1Mutation));
                    prewrite(tx, "shard-2", List.of(shard2Mutation));
                }
                commit(tx, bufferedMode);
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
         * 提交事务意向并生成已提交版本。
         */
        private void commit(PrototypeTransaction tx, boolean bufferedMode) {
            ensureActive(tx);
            if (bufferedMode) {
                runAcrossParticipants(
                        tx.participants(),
                        shardId -> prewrite(tx, shardId, tx.mutations(shardId)),
                        parallelPrewrite
                );
            }
            metadataStore.markPreparing(tx.txId(), tx.participants());
            runAcrossParticipants(
                    tx.participants(),
                    shardId -> adapters.get(shardId).prepare(tx.txId()),
                    parallelPrepare
            );
            long commitTs = timestampOracle.nextTimestamp();
            metadataStore.markCommitted(tx.txId(), commitTs);
            runAcrossParticipants(
                    tx.participants(),
                    shardId -> adapters.get(shardId).commit(tx.txId(), commitTs),
                    parallelCommit
            );
            tx.status(TxStatus.COMMITTED);
        }

        /**
         * 执行原型事务预写。
         */
        private void prewrite(PrototypeTransaction tx, String shardId, List<Mutation> mutations) {
            adapters.get(shardId).prewrite(tx.txId(), tx.startTs(), mutations);
            tx.markParticipant(shardId);
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
                runAcrossParticipants(
                        tx.participants(),
                        shardId -> adapters.get(shardId).abort(tx.txId()),
                        parallelRollback
                );
            } finally {
                tx.status(TxStatus.ABORTED);
            }
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
                    }))
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

        /**
         * 确认事务仍处于可操作状态。
         */
        private void ensureActive(PrototypeTransaction tx) {
            if (tx.status() != TxStatus.ACTIVE) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "transaction not active: " + tx.txId(), false, tx.txId());
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
         * 返回事务参与分片集合。
         */
        private Set<String> participants() {
            return bufferedMutations.keySet();
        }

        /**
         * 返回指定分片的写入变更列表。
         */
        private List<Mutation> mutations(String shardId) {
            return bufferedMutations.getOrDefault(shardId, List.of());
        }

        /**
         * 缓存一次分片写入变更。
         */
        private void buffer(String shardId, Mutation mutation) {
            bufferedMutations.computeIfAbsent(shardId, ignored -> new ArrayList<>()).add(mutation);
        }

        /**
         * 标记事务参与分片。
         */
        private void markParticipant(String shardId) {
            bufferedMutations.computeIfAbsent(shardId, ignored -> new ArrayList<>());
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

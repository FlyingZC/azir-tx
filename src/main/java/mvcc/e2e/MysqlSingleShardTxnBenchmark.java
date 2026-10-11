package mvcc.e2e;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.TimestampOracle;
import mvcc.tso.mysql.MysqlSegmentTimestampOracle;
import mvcc.tso.mysql.MysqlTimestampOracle;
import mvcc.txn.ConflictMode;
import mvcc.txn.CrossShardCommitPolicy;
import mvcc.txn.LockWaitPolicy;
import mvcc.txn.TransactionManager;
import mvcc.txn.TransactionOptions;
import mvcc.txn.TransactionRetryPolicy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MySQL 单分片事务基准测试入口。
 */
public final class MysqlSingleShardTxnBenchmark {
    /**
     * 构造 MysqlSingleShardTxnBenchmark 实例。
     */
    private MysqlSingleShardTxnBenchmark() {
    }

    /**
     * 程序入口方法。
     */
    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());

        try {
            MysqlTimestampOracle mysqlTso = new MysqlTimestampOracle(controlDataSource);
            MysqlSegmentTimestampOracle segmentTso = new MysqlSegmentTimestampOracle(controlDataSource, config.tsoBatchSize());
            TimestampOracle tso = config.segmentTso()
                    ? segmentTso
                    : mysqlTso;
            TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
            MysqlStorageAdapter shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);

            shard1.initialize();
            mysqlTso.initialize(100);
            segmentTso.initialize(100);
            ((MysqlTransactionMetadataStore) metadataStore).initialize();

            if (config.resetTables()) {
                reset(controlDataSource, "mvcc_txn_finalized", "mvcc_snapshot_lease", "mvcc_txn", "mvcc_tso");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard1.initialize();
                mysqlTso.initialize(100);
                segmentTso.initialize(100);
                ((MysqlTransactionMetadataStore) metadataStore).initialize();
            }

            TransactionManager transactionManager = new TransactionManager(
                    tso,
                    metadataStore,
                    new TransactionOptions(
                            new LockWaitPolicy(
                                    config.conflictModeSetting(),
                                    config.lockWaitTimeoutMs(),
                                    config.lockWaitBackoffInitialMs(),
                                    config.lockWaitBackoffMaxMs(),
                                    config.lockWaitBackoffMultiplier(),
                                    config.lockWaitBackoffJitter()
                            ),
                            new TransactionRetryPolicy(
                                    config.maxAttempts(),
                                    config.retryBackoffInitialMs(),
                                    config.retryBackoffMaxMs(),
                                    config.retryBackoffMultiplier(),
                                    config.retryBackoffJitter()
                            ),
                            CrossShardCommitPolicy.serial(),
                            config.bufferWrites(),
                            config.lazyBeginMetadata(),
                            false
                    ),
                    Map.of(shard1.shardId(), shard1)
            );

            BenchmarkResult result = runBenchmark(transactionManager, config);
            System.out.println(result.toLine(config));
        } finally {
            close(controlDataSource);
            close(shard1DataSource);
        }
    }

    /**
     * 执行基准测试并返回统计结果。
     */
    private static BenchmarkResult runBenchmark(TransactionManager transactionManager, Config config) throws Exception {
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
                            transactionManager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                                if (config.conflictMode()) {
                                    transactionManager.put(tx, "shard-1", "bench:single:hot:1", bytes("v"));
                                } else {
                                    long id = keySequence.incrementAndGet();
                                    transactionManager.put(tx, "shard-1", "bench:single:" + id, bytes("v" + id));
                                }
                                return null;
                            });
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
     * 将字符串转换为字节数组。
     */
    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
     * 基准测试配置对象。
     */
    private record Config(String controlJdbcUrl,
                          String shard1JdbcUrl,
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
                          boolean lazyBeginMetadata) {
        /**
         * 从环境变量读取配置对象。
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control"),
                    env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Boolean.parseBoolean(env("AZIR_TX_BENCH_CONFLICT", "false")),
                    ConflictMode.valueOf(env("AZIR_TX_CONFLICT_MODE", "FAIL_FAST")),
                    Long.parseLong(env("AZIR_TX_LOCK_WAIT_TIMEOUT_MS", "5000")),
                    Long.parseLong(env("AZIR_TX_LOCK_WAIT_BACKOFF_INITIAL_MS", "5")),
                    Long.parseLong(env("AZIR_TX_LOCK_WAIT_BACKOFF_MAX_MS", "100")),
                    Double.parseDouble(env("AZIR_TX_LOCK_WAIT_BACKOFF_MULTIPLIER", "2.0")),
                    Boolean.parseBoolean(env("AZIR_TX_LOCK_WAIT_BACKOFF_JITTER", "true")),
                    Integer.parseInt(env("AZIR_TX_RETRY_MAX_ATTEMPTS", "1")),
                    Long.parseLong(env("AZIR_TX_RETRY_BACKOFF_INITIAL_MS", "5")),
                    Long.parseLong(env("AZIR_TX_RETRY_BACKOFF_MAX_MS", "100")),
                    Double.parseDouble(env("AZIR_TX_RETRY_BACKOFF_MULTIPLIER", "2.0")),
                    Boolean.parseBoolean(env("AZIR_TX_RETRY_BACKOFF_JITTER", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_SEGMENT_TSO", "true")),
                    Integer.parseInt(env("AZIR_TX_TSO_BATCH_SIZE", "256")),
                    Boolean.parseBoolean(env("AZIR_TX_BUFFER_WRITES", "true")),
                    Boolean.parseBoolean(env("AZIR_TX_LAZY_BEGIN_METADATA", "true"))
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
            return "SINGLE_SHARD_BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + (config.conflictMode() ? "conflict" : "no_conflict")
                    + " conflict_mode=" + config.conflictModeSetting()
                    + " retry_max_attempts=" + config.maxAttempts()
                    + " segment_tso=" + config.segmentTso()
                    + " tso_batch_size=" + config.tsoBatchSize()
                    + " buffer_writes=" + config.bufferWrites()
                    + " lazy_begin_metadata=" + config.lazyBeginMetadata()
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
     * 读取字符串环境变量配置。
     */
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

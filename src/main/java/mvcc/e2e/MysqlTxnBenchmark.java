package mvcc.e2e;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.gc.SafePointStore;
import mvcc.gc.mysql.MysqlSafePointStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
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

public final class MysqlTxnBenchmark {
    private MysqlTxnBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());

        try {
            MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
            TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
            SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
            MysqlStorageAdapter shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
            MysqlStorageAdapter shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);

            shard1.initialize();
            shard2.initialize();
            tso.initialize(100);
            ((MysqlTransactionMetadataStore) metadataStore).initialize();
            ((MysqlSafePointStore) safePointStore).initialize(0L);

            if (config.resetTables()) {
                reset(controlDataSource, "mvcc_txn", "mvcc_tso", "mvcc_safe_point");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
                reset(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
                tso.initialize(100);
                ((MysqlTransactionMetadataStore) metadataStore).initialize();
                ((MysqlSafePointStore) safePointStore).initialize(0L);
            }

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
                                    config.lockWaitBackoffJitter()
                            ),
                            new TransactionRetryPolicy(
                                    config.maxAttempts(),
                                    config.retryBackoffInitialMs(),
                                    config.retryBackoffMaxMs(),
                                    config.retryBackoffMultiplier(),
                                    config.retryBackoffJitter()
                            ),
                            new CrossShardCommitPolicy(
                                    config.parallelPrepare(),
                                    config.parallelCommit(),
                                    config.parallelRollback()
                            )
                    ),
                    Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
            );

            BenchmarkResult result = runBenchmark(transactionManager, config);
            System.out.println(result.toLine(config));
        } finally {
            close(controlDataSource);
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

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

        long endedAt = System.nanoTime();
        double elapsedSeconds = (endedAt - startedAt) / 1_000_000_000.0;
        return new BenchmarkResult(
                elapsedSeconds,
                attempts.sum(),
                committed.sum(),
                conflicts.sum(),
                failures.sum()
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void reset(DataSource dataSource, String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

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
                          boolean parallelPrepare,
                          boolean parallelCommit,
                          boolean parallelRollback) {
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:3306/control"),
                    env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:3306/shard_1"),
                    env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:3306/shard_2"),
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
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_PREPARE", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_COMMIT", "false")),
                    Boolean.parseBoolean(env("AZIR_TX_PARALLEL_ROLLBACK", "false"))
            );
        }
    }

    private record BenchmarkResult(double elapsedSeconds,
                                   long attempts,
                                   long committed,
                                   long conflicts,
                                   long failures) {
        private String toLine(Config config) {
            double attemptTps = attempts / elapsedSeconds;
            double commitTps = committed / elapsedSeconds;
            double conflictTps = conflicts / elapsedSeconds;
            return "BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + (config.conflictMode() ? "conflict" : "no_conflict")
                    + " conflict_mode=" + config.conflictModeSetting()
                    + " retry_max_attempts=" + config.maxAttempts()
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

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

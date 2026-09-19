package mvcc.e2e;

import mvcc.storage.mysql.MysqlDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MySQL 直连两分片基准测试入口，用于对比没有全局事务协调时的性能基线。
 */
public final class MysqlDirectTwoShardTxnBenchmark {
    /**
     * 构造 MysqlDirectTwoShardTxnBenchmark 实例。
     */
    private MysqlDirectTwoShardTxnBenchmark() {
    }

    /**
     * 程序入口方法。
     */
    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());
        try {
            initialize(shard1DataSource);
            initialize(shard2DataSource);
            if (config.resetTables()) {
                reset(shard1DataSource);
                reset(shard2DataSource);
            }
            if (config.conflictMode()) {
                seedHotRow(shard1DataSource);
                seedHotRow(shard2DataSource);
            }

            BenchmarkResult result = runBenchmark(shard1DataSource, shard2DataSource, config);
            System.out.println(result.toLine(config));
        } finally {
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

    /**
     * 初始化基准测试表。
     */
    private static void initialize(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_direct_tx (
                      k varchar(128) not null primary key,
                      v varchar(128) not null
                    )
                    """);
        }
    }

    /**
     * 重置基准测试表数据。
     */
    private static void reset(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("delete from bench_direct_tx");
        }
    }

    /**
     * 写入热点行初始数据。
     */
    private static void seedHotRow(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     insert into bench_direct_tx(k, v) values ('hot', 'v')
                     on duplicate key update v = values(v)
                     """)) {
            statement.executeUpdate();
        }
    }

    /**
     * 执行基准测试并返回统计结果。
     */
    private static BenchmarkResult runBenchmark(DataSource shard1DataSource,
                                                DataSource shard2DataSource,
                                                Config config) throws Exception {
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder conflicts = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicLong sequence = new AtomicLong();

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
                            executeTxn(shard1DataSource, shard2DataSource, sequence.incrementAndGet(), config.conflictMode());
                            committed.increment();
                        } catch (SQLException ex) {
                            if (isConflict(ex)) {
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
     * 执行一次没有全局协调的两分片写入。
     */
    private static void executeTxn(DataSource shard1DataSource,
                                   DataSource shard2DataSource,
                                   long id,
                                   boolean conflictMode) throws SQLException {
        try (Connection shard1 = shard1DataSource.getConnection();
             Connection shard2 = shard2DataSource.getConnection()) {
            shard1.setAutoCommit(false);
            shard2.setAutoCommit(false);
            try {
                if (conflictMode) {
                    executeConflictWrite(shard1);
                    executeConflictWrite(shard2);
                } else {
                    executeNoConflictWrite(shard1, "bench:s1:" + id, "v" + id);
                    executeNoConflictWrite(shard2, "bench:s2:" + id, "v" + id);
                }
                // 直连基线没有全局提交协议：任一分片提交后，另一个分片失败会留下部分提交风险。
                shard1.commit();
                shard2.commit();
            } catch (SQLException ex) {
                rollbackQuietly(shard1);
                rollbackQuietly(shard2);
                throw ex;
            } finally {
                shard1.setAutoCommit(true);
                shard2.setAutoCommit(true);
            }
        }
    }

    /**
     * 执行无冲突写入。
     */
    private static void executeNoConflictWrite(Connection connection, String key, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into bench_direct_tx(k, v) values (?, ?)
                on duplicate key update v = values(v)
                """)) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }

    /**
     * 执行热点行写入。
     */
    private static void executeConflictWrite(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                update bench_direct_tx set v = ? where k = 'hot'
                """)) {
            statement.setString(1, Long.toString(System.nanoTime()));
            statement.executeUpdate();
        }
    }

    /**
     * 安静回滚本地事务。
     */
    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    /**
     * 判断 SQL 异常是否为事务冲突。
     */
    private static boolean isConflict(SQLException ex) {
        return ex instanceof SQLTransactionRollbackException
                || "40001".equals(ex.getSQLState())
                || "41000".equals(ex.getSQLState());
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
    private record Config(String shard1JdbcUrl,
                          String shard2JdbcUrl,
                          String username,
                          String password,
                          boolean resetTables,
                          int concurrency,
                          int durationSeconds,
                          boolean conflictMode) {
        /**
         * 从环境变量读取配置对象。
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_DIRECT_SHARD1_JDBC", env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1")),
                    env("AZIR_TX_DIRECT_SHARD2_JDBC", env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2")),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Boolean.parseBoolean(env("AZIR_TX_BENCH_CONFLICT", "false"))
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
            return "DIRECT_TWO_SHARD_BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + (config.conflictMode() ? "conflict" : "no_conflict")
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

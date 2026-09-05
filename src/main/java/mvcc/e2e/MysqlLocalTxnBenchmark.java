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

public final class MysqlLocalTxnBenchmark {
    private MysqlLocalTxnBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource dataSource = MysqlDataSourceFactory.create(config.jdbcUrl(), config.username(), config.password());
        try {
            initialize(dataSource);
            if (config.resetTables()) {
                reset(dataSource);
            }
            if (config.conflictMode()) {
                seedHotRows(dataSource);
            }

            BenchmarkResult result = runBenchmark(dataSource, config);
            System.out.println(result.toLine(config));
        } finally {
            close(dataSource);
        }
    }

    private static void initialize(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_single_a (
                      k varchar(128) not null primary key,
                      v varchar(128) not null
                    )
                    """);
            statement.execute("""
                    create table if not exists bench_single_b (
                      k varchar(128) not null primary key,
                      v varchar(128) not null
                    )
                    """);
        }
    }

    private static void reset(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("delete from bench_single_a");
            statement.execute("delete from bench_single_b");
        }
    }

    private static void seedHotRows(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement insertA = connection.prepareStatement("""
                     insert into bench_single_a(k, v) values (?, ?)
                     on duplicate key update v = values(v)
                     """);
             PreparedStatement insertB = connection.prepareStatement("""
                     insert into bench_single_b(k, v) values (?, ?)
                     on duplicate key update v = values(v)
                     """)) {
            insertA.setString(1, "hot");
            insertA.setString(2, "v");
            insertA.executeUpdate();
            insertB.setString(1, "hot");
            insertB.setString(2, "v");
            insertB.executeUpdate();
        }
    }

    private static BenchmarkResult runBenchmark(DataSource dataSource, Config config) throws Exception {
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
                        try (Connection connection = dataSource.getConnection()) {
                            connection.setAutoCommit(false);
                            try {
                                if (config.conflictMode()) {
                                    executeConflictTxn(connection);
                                } else {
                                    executeNoConflictTxn(connection, sequence.incrementAndGet());
                                }
                                connection.commit();
                                committed.increment();
                            } catch (SQLException ex) {
                                connection.rollback();
                                if (isConflict(ex)) {
                                    conflicts.increment();
                                } else {
                                    failures.increment();
                                }
                            } finally {
                                connection.setAutoCommit(true);
                            }
                        } catch (SQLException ex) {
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

    private static void executeNoConflictTxn(Connection connection, long id) throws SQLException {
        try (PreparedStatement writeA = connection.prepareStatement("""
                insert into bench_single_a(k, v) values (?, ?)
                on duplicate key update v = values(v)
                """);
             PreparedStatement writeB = connection.prepareStatement("""
                insert into bench_single_b(k, v) values (?, ?)
                on duplicate key update v = values(v)
                """)) {
            writeA.setString(1, "bench:a:" + id);
            writeA.setString(2, "v" + id);
            writeA.executeUpdate();

            writeB.setString(1, "bench:b:" + id);
            writeB.setString(2, "v" + id);
            writeB.executeUpdate();
        }
    }

    private static void executeConflictTxn(Connection connection) throws SQLException {
        try (PreparedStatement updateA = connection.prepareStatement("""
                update bench_single_a set v = ? where k = 'hot'
                """);
             PreparedStatement updateB = connection.prepareStatement("""
                update bench_single_b set v = ? where k = 'hot'
                """)) {
            String value = Long.toString(System.nanoTime());
            updateA.setString(1, value);
            updateA.executeUpdate();
            updateB.setString(1, value);
            updateB.executeUpdate();
        }
    }

    private static boolean isConflict(SQLException ex) {
        return ex instanceof SQLTransactionRollbackException
                || "40001".equals(ex.getSQLState())
                || "41000".equals(ex.getSQLState());
    }

    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private record Config(String jdbcUrl,
                          String username,
                          String password,
                          boolean resetTables,
                          int concurrency,
                          int durationSeconds,
                          boolean conflictMode) {
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_LOCAL_BENCH_JDBC", "jdbc:mysql://127.0.0.1:3306/single_tx"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Boolean.parseBoolean(env("AZIR_TX_BENCH_CONFLICT", "false"))
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
            return "LOCAL_BENCH"
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

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

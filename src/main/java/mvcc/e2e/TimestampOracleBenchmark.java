package mvcc.e2e;

import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.tso.BatchingTimestampOracle;
import mvcc.tso.CountingTimestampOracle;
import mvcc.tso.TimestampOracle;
import mvcc.tso.mysql.MysqlSegmentTimestampOracle;
import mvcc.tso.mysql.MysqlTimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public final class TimestampOracleBenchmark {
    private TimestampOracleBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        try {
            MysqlTimestampOracle mysqlTso = new MysqlTimestampOracle(controlDataSource);
            MysqlSegmentTimestampOracle segmentTso = new MysqlSegmentTimestampOracle(controlDataSource, config.batchSize());
            mysqlTso.initialize(100);
            segmentTso.initialize(100);
            if (config.resetTable()) {
                reset(controlDataSource, "mvcc_tso");
                mysqlTso.initialize(100);
                segmentTso.initialize(100);
            }
            TimestampOracle delegate = switch (config.mode()) {
                case DIRECT, LEGACY_BATCH -> mysqlTso;
                case SEGMENT -> segmentTso;
            };
            CountingTimestampOracle countingOracle = new CountingTimestampOracle(delegate);
            TimestampOracle oracle = switch (config.mode()) {
                case DIRECT -> countingOracle;
                case LEGACY_BATCH -> config.batchSize() > 1
                        ? new BatchingTimestampOracle(countingOracle, config.batchSize())
                        : countingOracle;
                case SEGMENT -> countingOracle;
            };
            BenchmarkResult result = runBenchmark(oracle, countingOracle, segmentTso, config);
            System.out.println(result.toLine(config));
        } finally {
            close(controlDataSource);
        }
    }

    private static BenchmarkResult runBenchmark(TimestampOracle oracle,
                                                CountingTimestampOracle countingOracle,
                                                MysqlSegmentTimestampOracle segmentTso,
                                                Config config) throws Exception {
        LongAdder allocated = new LongAdder();
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
                        oracle.nextTimestamp();
                        allocated.increment();
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
        long allocatedCount = allocated.sum();
        long remoteCalls = switch (config.mode()) {
            case DIRECT, LEGACY_BATCH -> countingOracle.invocationCount();
            case SEGMENT -> segmentTso.segmentAllocations();
        };
        return new BenchmarkResult(elapsedSeconds, allocatedCount, remoteCalls);
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
                          String username,
                          String password,
                          boolean resetTable,
                          int concurrency,
                          int durationSeconds,
                          Mode mode,
                          int batchSize) {
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Mode.valueOf(env("AZIR_TX_TSO_MODE", "DIRECT")),
                    Integer.parseInt(env("AZIR_TX_TSO_BATCH_SIZE", "1"))
            );
        }
    }

    private record BenchmarkResult(double elapsedSeconds,
                                   long allocated,
                                   long remoteCalls) {
        private String toLine(Config config) {
            double allocatedTps = allocated / elapsedSeconds;
            double remoteCallTps = remoteCalls / elapsedSeconds;
            double remoteCallsPerAllocation = allocated == 0L ? 0.0 : (double) remoteCalls / allocated;
            return "TSO_BENCH"
                    + " concurrency=" + config.concurrency()
                    + " duration_s=" + String.format("%.2f", elapsedSeconds)
                    + " mode=" + config.mode()
                    + " batch_size=" + config.batchSize()
                    + " allocated=" + allocated
                    + " remote_calls=" + remoteCalls
                    + " allocated_tps=" + String.format("%.2f", allocatedTps)
                    + " remote_call_tps=" + String.format("%.2f", remoteCallTps)
                    + " remote_calls_per_allocation=" + String.format("%.4f", remoteCallsPerAllocation);
        }
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private enum Mode {
        DIRECT,
        LEGACY_BATCH,
        SEGMENT
    }
}

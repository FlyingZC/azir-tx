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

import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.tso.BatchingTimestampOracle;
import com.flyingzc.azir.tso.CountingTimestampOracle;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlSegmentTimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Timestamp dispenser benchmark test entrance.
 */
public final class TimestampOracleBenchmark {

    /**
     * Construct a TimestampOracleBenchmark instance.
     */
    private TimestampOracleBenchmark() {
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

    /**
     * Run the benchmark and return its statistics.
     *
     * @param oracle oracle
     * @param countingOracle counting oracle
     * @param segmentTso segment tso
     * @param config config
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static BenchmarkResult runBenchmark(final TimestampOracle oracle,
                                                final CountingTimestampOracle countingOracle,
                                                final MysqlSegmentTimestampOracle segmentTso,
                                                final Config config) throws Exception {
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

        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        long allocatedCount = allocated.sum();
        long remoteCalls = switch (config.mode()) {
            case DIRECT, LEGACY_BATCH -> countingOracle.invocationCount();
            case SEGMENT -> segmentTso.segmentAllocations();
        };
        return new BenchmarkResult(elapsedSeconds, allocatedCount, remoteCalls);
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
     * @param username username
     * @param password password
     * @param resetTable reset table
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param mode mode
     * @param batchSize batch size
     */
    private record Config(String controlJdbcUrl,
            String username,
            String password,
            boolean resetTable,
            int concurrency,
            int durationSeconds,
            Mode mode,
            int batchSize) {

        /**
         * Read configuration from environment variables.
         *
         * @return the operation result
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")),
                    Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50")),
                    Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10")),
                    Mode.valueOf(env("AZIR_TX_TSO_MODE", "DIRECT")),
                    Integer.parseInt(env("AZIR_TX_TSO_BATCH_SIZE", "1")));
        }
    }

    /**
     * Benchmark results.
     *
     * @param elapsedSeconds elapsed seconds
     * @param allocated allocated
     * @param remoteCalls remote calls
     */
    private record BenchmarkResult(double elapsedSeconds,
            long allocated,
            long remoteCalls) {

        /**
         * Format benchmark results as a single line.
         *
         * @param config config
         * @return the operation result
         */
        private String toLine(final Config config) {
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

    /**
     * Benchmark Test run mode count.
     */
    private enum Mode {
        DIRECT,
        LEGACY_BATCH,
        SEGMENT
    }
}

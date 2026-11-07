package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/** 在另一进程持续写入时测量双采集快照能否得到稳定分条切面。 */
public final class MysqlStripedSnapshotCaptureProbe {
    /** 基准入口无需实例状态。 */
    private MysqlStripedSnapshotCaptureProbe() {
    }

    /** 仅读取已经由写入探针创建的隔离水位表，不执行重置。 */
    public static void main(String[] args) throws Exception {
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int seconds = Integer.parseInt(env("AZIR_TX_CAPTURE_DURATION_SECONDS", "10"));
        int retries = Integer.parseInt(env("AZIR_TX_CAPTURE_MAX_RETRIES", "64"));
        boolean parallelReads = Boolean.parseBoolean(env("AZIR_TX_CAPTURE_PARALLEL", "false"));
        if (stripes < 1 || stripes > 256 || seconds < 1 || seconds > 120
                || retries < 1 || retries > 4096) {
            throw new IllegalArgumentException("invalid striped capture probe limits");
        }
        String url = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String secondUrl = System.getenv("AZIR_TX_SHARD2_JDBC");
        try (HikariDataSource dataSource = (HikariDataSource) MysqlDataSourceFactory.create(url,
                env("AZIR_TX_USERNAME", "root"), env("AZIR_TX_PASSWORD", "root"));
             HikariDataSource second = secondUrl == null || secondUrl.isBlank() ? null
                     : (HikariDataSource) MysqlDataSourceFactory.create(secondUrl,
                     env("AZIR_TX_USERNAME", "root"), env("AZIR_TX_PASSWORD", "root"))) {
            Map<String, DataSource> routes = second == null
                    ? Map.<String, DataSource>of("s1", dataSource)
                    : Map.<String, DataSource>of("s1", dataSource, "s2", second);
            try (var oracle = new MysqlStripedSnapshotOracle(routes, stripes, retries, parallelReads)) {
            long attempts = 0;
            long successful = 0;
            long unstable = 0;
            long successNanos = 0;
            List<Long> successfulLatencies = new ArrayList<>();
            long started = System.nanoTime();
            long deadline = started + TimeUnit.SECONDS.toNanos(seconds);
            while (System.nanoTime() < deadline) {
                attempts++;
                long oneStarted = System.nanoTime();
                try {
                    oracle.capture(() -> 0);
                    successful++;
                    long latency = System.nanoTime() - oneStarted;
                    successNanos += latency;
                    successfulLatencies.add(latency);
                } catch (MvccException ex) {
                    if (ex.errorCode() != ErrorCode.TRANSACTION_STATUS_UNKNOWN) {
                        throw ex;
                    }
                    unstable++;
                }
                LockSupport.parkNanos(100_000);
            }
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            long[] sorted = successfulLatencies.stream().mapToLong(Long::longValue).sorted().toArray();
            double p99Millis = sorted.length == 0 ? Double.NaN
                    : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
            System.out.printf("STRIPED_SNAPSHOT_CAPTURE duration_s=%.2f shards=%d "
                            + "stripes=%d max_retries=%d parallel=%s "
                            + "attempts=%d successful=%d unstable=%d success_rate=%.3f "
                            + "avg_rounds=%.2f success_avg_ms=%.3f success_p99_ms=%.3f%n",
                    elapsed, routes.size(), stripes, retries, parallelReads,
                    attempts, successful, unstable,
                    attempts == 0 ? 0 : (double) successful / attempts,
                    attempts == 0 ? 0 : (double) oracle.collectionAttempts() / attempts,
                    successful == 0 ? Double.NaN : successNanos / (double) successful / 1_000_000.0,
                    p99Millis);
            }
        }
    }

    /** 只读取隔离探针的分片地址与捕获预算。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

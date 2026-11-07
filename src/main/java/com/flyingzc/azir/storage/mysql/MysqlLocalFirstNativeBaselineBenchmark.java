package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** 与本地先提交协议相同键值负载的原生 MySQL 单键事务容量对照。 */
public final class MysqlLocalFirstNativeBaselineBenchmark {
    /** 基准入口不保存全局可变状态。 */
    private MysqlLocalFirstNativeBaselineBenchmark() {
    }

    /** 显式重置独立业务表后，以相同键和值构造逐笔持久事务。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("native baseline requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "20"));
        boolean explicitTransaction = Boolean.parseBoolean(env("AZIR_TX_NATIVE_EXPLICIT_TX", "true"));
        boolean reuseConnection = Boolean.parseBoolean(env("AZIR_TX_NATIVE_REUSE_CONNECTION", "true"));
        boolean shardWatermark = Boolean.parseBoolean(env("AZIR_TX_NATIVE_SHARD_WATERMARK", "false"));
        int watermarkStripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "1"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 600
                || watermarkStripes < 1 || watermarkStripes > 256
                || shardWatermark && !explicitTransaction) {
            throw new IllegalArgumentException("invalid native baseline limits");
        }
        String url = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (HikariDataSource dataSource = (HikariDataSource) MysqlDataSourceFactory.create(
                url, username, password)) {
            initializeAndReset(dataSource, watermarkStripes);
            run(dataSource, concurrency, durationSeconds, explicitTransaction,
                    reuseConnection, shardWatermark, watermarkStripes);
        }
    }

    /** 仅清空本基准专用业务表，不碰控制协议与分片 MVCC 表。 */
    private static void initializeAndReset(HikariDataSource dataSource, int watermarkStripes) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_local_first_native (
                      user_key varbinary(256) not null primary key,
                      payload blob not null
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_local_first_native");
            statement.execute("""
                    create table if not exists bench_local_first_watermark_stripes (
                      stripe_id smallint not null primary key,
                      committed_seq bigint not null
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_local_first_watermark_stripes");
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_local_first_watermark_stripes(stripe_id, committed_seq)
                    values (?, 0)
                    """)) {
                for (int stripe = 0; stripe < watermarkStripes; stripe++) {
                    insert.setInt(1, stripe);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** 可切换逐线程复用或逐事务借连接，量化连接策略对原生对照的影响。 */
    private static void run(HikariDataSource dataSource, int concurrency, int durationSeconds,
                            boolean explicitTransaction, boolean reuseConnection,
                            boolean shardWatermark, int watermarkStripes) throws Exception {
        AtomicLong sequence = new AtomicLong(100);
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    try {
                        if (reuseConnection) {
                            try (Connection connection = dataSource.getConnection();
                                 PreparedStatement insert = insertStatement(connection);
                                 PreparedStatement watermark = shardWatermark
                                         ? watermarkStatement(connection) : null) {
                                connection.setAutoCommit(!explicitTransaction);
                                ready.countDown();
                                start.await();
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                                while (System.nanoTime() < deadline) {
                                    attempts.increment();
                                    long started = System.nanoTime();
                                    try {
                                        long timestamp = sequence.incrementAndGet();
                                        writeOne(insert, timestamp);
                                        if (watermark != null) {
                                            watermark.setInt(1, (int) (timestamp % watermarkStripes));
                                            if (watermark.executeUpdate() != 1) {
                                                throw new SQLException("native shard watermark changed no row");
                                            }
                                        }
                                        if (explicitTransaction) {
                                            connection.commit();
                                        }
                                        committed.increment();
                                        latencies.add(System.nanoTime() - started);
                                    } catch (Exception ex) {
                                        failures.increment();
                                        firstFailure.compareAndSet(null,
                                                ex.getClass().getSimpleName() + ":" + ex.getMessage());
                                        if (explicitTransaction) {
                                            connection.rollback();
                                        }
                                    }
                                }
                            }
                        } else {
                            ready.countDown();
                            start.await();
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                            while (System.nanoTime() < deadline) {
                                attempts.increment();
                                long started = System.nanoTime();
                                try (Connection connection = dataSource.getConnection()) {
                                    connection.setAutoCommit(!explicitTransaction);
                                    try (PreparedStatement insert = insertStatement(connection);
                                         PreparedStatement watermark = shardWatermark
                                                 ? watermarkStatement(connection) : null) {
                                        long timestamp = sequence.incrementAndGet();
                                        writeOne(insert, timestamp);
                                        if (watermark != null) {
                                            watermark.setInt(1, (int) (timestamp % watermarkStripes));
                                            if (watermark.executeUpdate() != 1) {
                                                throw new SQLException("native shard watermark changed no row");
                                            }
                                        }
                                        if (explicitTransaction) {
                                            connection.commit();
                                        }
                                    } catch (Exception ex) {
                                        if (explicitTransaction) {
                                            connection.rollback();
                                        }
                                        throw ex;
                                    }
                                    committed.increment();
                                    latencies.add(System.nanoTime() - started);
                                } catch (Exception ex) {
                                    failures.increment();
                                    firstFailure.compareAndSet(null,
                                            ex.getClass().getSimpleName() + ":" + ex.getMessage());
                                }
                            }
                        }
                    } catch (Exception ex) {
                        firstFailure.compareAndSet(null,
                                ex.getClass().getSimpleName() + ":" + ex.getMessage());
                        failures.increment();
                        ready.countDown();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            start.countDown();
            done.await();
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
            double p99Millis = sorted.length == 0 ? Double.NaN
                    : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
            long rows = rowCount(dataSource);
            if (rows != committed.sum()) {
                throw new IllegalStateException("native committed count differs from durable business rows");
            }
            long watermark = watermarkValue(dataSource);
            if (shardWatermark && watermark != committed.sum()) {
                throw new IllegalStateException("native shard watermark differs from durable business rows");
            }
            System.out.printf("LOCAL_FIRST_NATIVE_BENCH concurrency=%d duration_s=%.2f "
                            + "explicit_tx=%s reuse_connection=%s shard_watermark=%s "
                            + "watermark_stripes=%d "
                            + "attempts=%d committed=%d failures=%d "
                            + "commit_tps=%.2f p99_ms=%.2f rows=%d watermark=%d first_failure=%s%n",
                    concurrency, seconds, explicitTransaction, reuseConnection,
                    shardWatermark, watermarkStripes, attempts.sum(), committed.sum(),
                    failures.sum(), committed.sum() / seconds, p99Millis, rows, watermark,
                    firstFailure.get() == null ? "-" : firstFailure.get());
        }
    }

    /** 两种连接策略使用同一条预编译单键写入语句。 */
    private static PreparedStatement insertStatement(Connection connection) throws SQLException {
        return connection.prepareStatement("""
                insert into bench_local_first_native(user_key, payload) values (?, ?)
                """);
    }

    /** 分条本地序号与业务行在同一 InnoDB 事务中持久化。 */
    private static PreparedStatement watermarkStatement(Connection connection) throws SQLException {
        return connection.prepareStatement("""
                update bench_local_first_watermark_stripes
                set committed_seq = committed_seq + 1 where stripe_id = ?
                """);
    }

    /** 与协议基准保持相同的键和值字节，并要求恰好写入一行。 */
    private static void writeOne(PreparedStatement insert, long timestamp) throws SQLException {
        insert.setBytes(1, ("local-first:" + timestamp).getBytes(StandardCharsets.UTF_8));
        insert.setBytes(2, ("v" + timestamp).getBytes(StandardCharsets.UTF_8));
        if (insert.executeUpdate() != 1) {
            throw new SQLException("native single-key insert changed no row");
        }
    }

    /** 实际物理业务行数作为成功提交量的持久证明。 */
    private static long rowCount(HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement query = connection.createStatement();
             ResultSet row = query.executeQuery("select count(*) from bench_local_first_native")) {
            if (!row.next()) {
                throw new IllegalStateException("native row count returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 汇总所有持久分条水位以核对每笔成功提交都完成了同事务推进。 */
    private static long watermarkValue(HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement query = connection.createStatement();
             ResultSet row = query.executeQuery("select sum(committed_seq) "
                     + "from bench_local_first_watermark_stripes")) {
            if (!row.next()) {
                throw new IllegalStateException("native shard watermark returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 只使用本基准明确指定的连接和容量参数。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

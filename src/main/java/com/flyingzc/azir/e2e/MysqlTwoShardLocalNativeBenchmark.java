package com.flyingzc.azir.e2e;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** 两台真实 MySQL 上各自本地事务写固定业务键的同形容量基线。 */
public final class MysqlTwoShardLocalNativeBenchmark {
    /** 基准入口不保留进程全局数据库状态。 */
    private MysqlTwoShardLocalNativeBenchmark() {
    }

    /** 只有显式重置的专属业务表可用于同环境事务吞吐对照。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("two-shard native baseline requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "30"));
        if (concurrency < 2 || concurrency > 512 || seconds < 1 || seconds > 600) {
            throw new IllegalArgumentException("invalid native local two-shard limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (HikariDataSource first = pool(env("AZIR_TX_SHARD1_JDBC",
                "jdbc:mysql://127.0.0.1:23307/shard_1"), username, password);
             HikariDataSource second = pool(env("AZIR_TX_SHARD2_JDBC",
                     "jdbc:mysql://127.0.0.1:23308/shard_2"), username, password)) {
            initialize(first);
            initialize(second);
            run(first, second, concurrency, seconds);
        }
    }

    /** 沿用产品前台连接池预算和驱动配置。 */
    private static HikariDataSource pool(String url, String user, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, user, password);
    }

    /** 只清空本基准专属表，不修改分片 MVCC 或控制库表。 */
    private static void initialize(HikariDataSource source) throws SQLException {
        try (Connection connection = source.getConnection(); Statement sql = connection.createStatement()) {
            sql.execute("""
                    create table if not exists bench_native_local_fixed (
                      user_key varbinary(256) not null primary key,
                      payload blob not null
                    ) engine=InnoDB
                    """);
            sql.executeUpdate("delete from bench_native_local_fixed");
        }
    }

    /** 50 个独立客户端按奇偶路由两片，每人反复 UPSERT 同一个键。 */
    private static void run(HikariDataSource first, HikariDataSource second,
                            int concurrency, int seconds) throws Exception {
        AtomicLong ids = new AtomicLong(100);
        LongAdder[] committed = {new LongAdder(), new LongAdder()};
        LongAdder failures = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(concurrency);
        AtomicLong deadline = new AtomicLong();
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                int shard = index & 1;
                HikariDataSource source = shard == 0 ? first : second;
                byte[] key = ("local-first:worker:" + index).getBytes(StandardCharsets.UTF_8);
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        while (System.nanoTime() < deadline.get()) {
                            long began = System.nanoTime();
                            long id = ids.incrementAndGet();
                            try (Connection connection = source.getConnection()) {
                                connection.setAutoCommit(false);
                                try (PreparedStatement write = connection.prepareStatement("""
                                        insert into bench_native_local_fixed(user_key, payload)
                                        values (?, ?) on duplicate key update payload = values(payload)
                                        """)) {
                                    write.setBytes(1, key);
                                    write.setBytes(2, ("v" + id).getBytes(StandardCharsets.UTF_8));
                                    write.executeUpdate();
                                    connection.commit();
                                    committed[shard].increment();
                                    latencies.add(System.nanoTime() - began);
                                } catch (SQLException ex) {
                                    connection.rollback();
                                    throw ex;
                                } finally {
                                    connection.setAutoCommit(true);
                                }
                            } catch (Exception ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null,
                                        ex.getClass().getSimpleName() + ':' + ex.getMessage());
                            }
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.countDown();
                    }
                });
            }
            if (!ready.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("native local workers did not become ready");
            }
            long began = System.nanoTime();
            deadline.set(began + TimeUnit.SECONDS.toNanos(seconds));
            start.countDown();
            if (!finished.await(seconds + 60L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("native local workers did not finish");
            }
            double elapsed = (System.nanoTime() - began) / 1_000_000_000.0;
            latencies.sort(Comparator.naturalOrder());
            long total = committed[0].sum() + committed[1].sum();
            long p99 = latencies.isEmpty() ? 0 : latencies.get(Math.min(latencies.size() - 1,
                    (int) Math.ceil(latencies.size() * 0.99) - 1));
            long leftRows = rowCount(first);
            long rightRows = rowCount(second);
            System.out.printf("NATIVE_TWO_LOCAL_BENCH concurrency=%d duration_s=%.2f "
                            + "committed=%d tps=%.2f p99_ms=%.3f shard_tps=%.2f/%.2f "
                            + "rows=%d/%d failures=%d first_failure=%s%n",
                    concurrency, elapsed, total, total / elapsed, p99 / 1_000_000.0,
                    committed[0].sum() / elapsed, committed[1].sum() / elapsed,
                    leftRows, rightRows, failures.sum(),
                    firstFailure.get() == null ? "-" : firstFailure.get());
            if (failures.sum() != 0 || total == 0
                    || leftRows != (concurrency + 1) / 2 || rightRows != concurrency / 2) {
                throw new IllegalStateException("native local two-shard baseline is invalid");
            }
        }
    }

    /** 每片最终只有每个独立客户端自己的一个业务行。 */
    private static long rowCount(HikariDataSource source) throws SQLException {
        try (Connection connection = source.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select count(*) from bench_native_local_fixed
                     """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("native local row count returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 测试参数仅从显式环境变量或隔离默认值读取。 */
    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

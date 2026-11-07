package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.TxStatus;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** 主分片单次登记预写、次片预写、主决议、切面和有界物化的隔离容量探针。 */
public final class MysqlPrimaryDecisionCapacityProbe {
    /** 每个后台任务只保存已经 ACK 的事务身份。 */
    private record Committed(long txId, String primaryKey, String secondaryKey, long decisionSequence) {
    }

    /** 容量探针不保存全局运行状态。 */
    private MysqlPrimaryDecisionCapacityProbe() {
    }

    /** 显式重置隔离数据库后运行有限并发和有限持续时间的完整写入实验。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("primary decision probe requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int backgroundWorkers = Integer.parseInt(env("AZIR_TX_PRIMARY_BACKGROUND_WORKERS", "8"));
        int backlogLimit = Integer.parseInt(env("AZIR_TX_PRIMARY_BACKLOG_LIMIT", "1000"));
        if (concurrency < 1 || concurrency > 200 || seconds < 1 || seconds > 600
                || stripes < 1 || stripes > 256 || backgroundWorkers < 1
                || backgroundWorkers > 64 || backlogLimit < 1 || backlogLimit > 100_000) {
            throw new IllegalArgumentException("invalid primary decision probe limits");
        }
        String user = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (HikariDataSource primary = pool(env("AZIR_TX_SHARD1_JDBC",
                "jdbc:mysql://127.0.0.1:23307/shard_1"), user, password);
             HikariDataSource secondary = pool(env("AZIR_TX_SHARD2_JDBC",
                     "jdbc:mysql://127.0.0.1:23308/shard_2"), user, password)) {
            MysqlStripedCrossVersionPrototype.initializeIsolated(primary, stripes);
            MysqlStripedCrossVersionPrototype.initializeIsolated(secondary, stripes);
            var decisions = new MysqlPrimaryDecisionPrototype(primary, secondary, stripes);
            decisions.initializeIsolated();
            decisions.resetIsolated();
            try (var reader = new MysqlPrimaryVectorReadPrototype(
                    "s1", primary, "s2", secondary, stripes, decisions)) {
                run(primary, secondary, decisions, reader, concurrency, seconds,
                        stripes, backgroundWorkers, backlogLimit);
            }
        }
    }

    /** 与当前产品容量探针复用同一 JDBC 池配置。 */
    private static HikariDataSource pool(String url, String user, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, user, password);
    }

    /** 所有客户端独立申请事务号，后台持续物化且队列容量形成明确背压。 */
    private static void run(HikariDataSource primary, HikariDataSource secondary,
                            MysqlPrimaryDecisionPrototype decisions,
                            MysqlPrimaryVectorReadPrototype reader,
                            int concurrency, int seconds, int stripes,
                            int backgroundWorkers, int backlogLimit) throws Exception {
        ArrayBlockingQueue<Committed> backlog = new ArrayBlockingQueue<>(backlogLimit);
        AtomicLong nextId = new AtomicLong(1);
        LongAdder acknowledged = new LongAdder();
        LongAdder materialized = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder prewriteNanos = new LongAdder();
        LongAdder decisionNanos = new LongAdder();
        LongAdder cutNanos = new LongAdder();
        AtomicInteger highWater = new AtomicInteger();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicBoolean producersRunning = new AtomicBoolean(true);
        List<Long> ackLatencies = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch frontDone = new CountDownLatch(concurrency);
        try (var clients = Executors.newFixedThreadPool(concurrency);
             var recovery = Executors.newFixedThreadPool(backgroundWorkers)) {
            for (int worker = 0; worker < backgroundWorkers; worker++) {
                recovery.submit(() -> {
                    while (producersRunning.get() || !backlog.isEmpty()) {
                        try {
                            Committed committed = backlog.poll(100, TimeUnit.MILLISECONDS);
                            if (committed == null) {
                                continue;
                            }
                            MysqlStripedCrossVersionPrototype.materialize(primary,
                                    committed.primaryKey(), committed.txId(),
                                    committed.decisionSequence(), tx -> committedSequence(decisions, tx));
                            MysqlStripedCrossVersionPrototype.materialize(secondary,
                                    committed.secondaryKey(), committed.txId(),
                                    committed.decisionSequence(), tx -> committedSequence(decisions, tx));
                            materialized.increment();
                        } catch (Throwable failure) {
                            failures.increment();
                            firstFailure.compareAndSet(null, failure);
                        }
                    }
                });
            }
            long scheduledEnd = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            for (int worker = 0; worker < concurrency; worker++) {
                clients.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        while (System.nanoTime() < scheduledEnd && firstFailure.get() == null) {
                            long began = System.nanoTime();
                            long txId = nextId.getAndIncrement();
                            int primaryStripe = Math.floorMod(txId, stripes);
                            int secondaryStripe = (primaryStripe + 1) % stripes;
                            int decisionStripe = (primaryStripe + 2) % stripes;
                            String primaryKey = "primary:" + txId;
                            String secondaryKey = "secondary:" + txId;
                            byte[] value = Long.toString(txId).getBytes(StandardCharsets.UTF_8);
                            var registration = new MysqlPrimaryDecisionPrototype.Registration(
                                    txId, primaryKey, value, secondaryKey, value,
                                    primaryStripe, secondaryStripe, 0, 0,
                                    System.currentTimeMillis() + 60_000, decisionStripe);
                            long stage = System.nanoTime();
                            // 主持久身份必须先于次片预写，避免崩溃后出现无主决议的孤儿意向。
                            long primarySequence = decisions.registerAndPrewritePrimary(registration);
                            long secondarySequence = MysqlStripedCrossVersionPrototype.prewrite(
                                    secondary, secondaryKey, value, secondaryStripe, txId, 0);
                            prewriteNanos.add(System.nanoTime() - stage);
                            stage = System.nanoTime();
                            var decision = decisions.commit(registration);
                            decisionNanos.add(System.nanoTime() - stage);
                            if (decision.primarySequence() != primarySequence
                                    || decision.secondarySequence() != secondarySequence) {
                                throw new IllegalStateException("primary decision changed participant proof");
                            }
                            stage = System.nanoTime();
                            var cut = reader.captureUnpublished();
                            cutNanos.add(System.nanoTime() - stage);
                            if (cut.decisionSequence(decisionStripe) < decision.decisionSequence()
                                    || cut.dataSequence("s1", primaryStripe) < primarySequence
                                    || cut.dataSequence("s2", secondaryStripe) < secondarySequence) {
                                throw new IllegalStateException("primary decision cut does not cover ACK");
                            }
                            if (!backlog.offer(new Committed(txId, primaryKey,
                                    secondaryKey, decision.decisionSequence()), 2, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("primary decision materialization backlog did not drain");
                            }
                            highWater.accumulateAndGet(backlog.size(), Math::max);
                            acknowledged.increment();
                            ackLatencies.add(System.nanoTime() - began);
                        }
                    } catch (Throwable failure) {
                        failures.increment();
                        firstFailure.compareAndSet(null, failure);
                    } finally {
                        frontDone.countDown();
                    }
                });
            }
            if (!ready.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("primary decision clients did not become ready");
            }
            long measuredStart = System.nanoTime();
            start.countDown();
            if (!frontDone.await(seconds + 60L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("primary decision clients did not finish");
            }
            long measuredEnd = System.nanoTime();
            int pendingAtStop = backlog.size();
            producersRunning.set(false);
            recovery.shutdown();
            if (!recovery.awaitTermination(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("primary decision materialization did not drain");
            }
            double drainSeconds = (System.nanoTime() - measuredEnd) / 1_000_000_000.0;
            ackLatencies.sort(Comparator.naturalOrder());
            long count = acknowledged.sum();
            double elapsed = (measuredEnd - measuredStart) / 1_000_000_000.0;
            long p99 = ackLatencies.isEmpty() ? 0
                    : ackLatencies.get(Math.min(ackLatencies.size() - 1,
                    (int) Math.ceil(ackLatencies.size() * 0.99) - 1));
            long durableDecisions = count(primary,
                    "select count(*) from bench_primary_txn_v2 where status = 'COMMITTED'");
            long durableDecisionSequence = count(primary,
                    "select coalesce(sum(committed_seq), 0) from bench_primary_decision_state_v1");
            long primaryVersions = count(primary,
                    "select count(*) from bench_striped_mvcc_cross_data");
            long secondaryVersions = count(secondary,
                    "select count(*) from bench_striped_mvcc_cross_data");
            System.out.printf("primary decision: clients=%d stripes=%d bg_workers=%d seconds=%.3f ack=%d "
                            + "tps=%.1f p99_ms=%.3f materialized=%d backlog_max=%d "
                            + "backlog_at_stop=%d backlog_final=%d drain_seconds=%.3f "
                            + "durable_decisions=%d decision_seq_sum=%d versions=%d/%d "
                            + "failures=%d prewrite_avg_ms=%.3f "
                            + "decision_avg_ms=%.3f cut_avg_ms=%.3f%n",
                    concurrency, stripes, backgroundWorkers, elapsed, count,
                    count / elapsed, p99 / 1_000_000.0,
                    materialized.sum(), highWater.get(), pendingAtStop,
                    backlog.size(), drainSeconds, durableDecisions,
                    durableDecisionSequence, primaryVersions, secondaryVersions,
                    failures.sum(),
                    averageMillis(prewriteNanos.sum(), count),
                    averageMillis(decisionNanos.sum(), count),
                    averageMillis(cutNanos.sum(), count));
            if (firstFailure.get() != null || count != materialized.sum()
                    || count != durableDecisions || count != durableDecisionSequence
                    || count != primaryVersions || count != secondaryVersions
                    || !backlog.isEmpty()) {
                throw new IllegalStateException("primary decision capacity result is invalid", firstFailure.get());
            }
        }
    }

    /** 物化只接受主分片持久 COMMITTED 状态与同一决议序号。 */
    private static OptionalLong committedSequence(MysqlPrimaryDecisionPrototype decisions, long txId) {
        var decision = decisions.decision(txId).orElseThrow();
        return decision.status() == TxStatus.COMMITTED
                ? OptionalLong.of(decision.decisionSequence()) : OptionalLong.empty();
    }

    /** 从物理表读取最终行数，防止后台任务计数掩盖实际持久化缺口。 */
    private static long count(HikariDataSource dataSource, String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement(sql);
             ResultSet result = query.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("primary decision count query returned no row");
            }
            return result.getLong(1);
        }
    }

    /** 阶段累计时间按实际 ACK 数换算为每笔平均毫秒。 */
    private static double averageMillis(long totalNanos, long count) {
        return count == 0 ? 0 : totalNanos / 1_000_000.0 / count;
    }

    /** 测试参数来自环境变量，缺失时使用隔离实验默认值。 */
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

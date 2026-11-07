package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/** 测量持久向量组发布后本地写 ACK 吞吐与 RC 读取中心切面的延迟。 */
public final class MysqlPublishedVectorCapacityProbe {
    /** 隔离容量探针不保存全局配置。 */
    private MysqlPublishedVectorCapacityProbe() {
    }

    /** 必须显式允许重置三个实验库，避免容量程序触碰产品表。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("published vector probe requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        boolean volatileExperiment = Boolean.parseBoolean(env("AZIR_TX_VECTOR_VOLATILE_EXPERIMENT", "false"));
        boolean remoteExperiment = Boolean.parseBoolean(env("AZIR_TX_VECTOR_REMOTE_EXPERIMENT", "false"));
        boolean tcpExperiment = Boolean.parseBoolean(env("AZIR_TX_VECTOR_TCP_EXPERIMENT", "false"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 120
                || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid published vector capacity limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (HikariDataSource center = pool(env("AZIR_TX_CONTROL_JDBC",
                "jdbc:mysql://127.0.0.1:23306/control"), username, password);
             HikariDataSource left = pool(env("AZIR_TX_SHARD1_JDBC",
                     "jdbc:mysql://127.0.0.1:23306/shard_1"), username, password);
             HikariDataSource right = pool(env("AZIR_TX_SHARD2_JDBC",
                     "jdbc:mysql://127.0.0.1:23306/shard_2"), username, password)) {
            MysqlStripedCrossVersionPrototype.initializeIsolated(left, stripes);
            MysqlStripedCrossVersionPrototype.initializeIsolated(right, stripes);
            Map<String, DataSource> routes = Map.of("s1", left, "s2", right);
            if (tcpExperiment) {
                int port = Integer.parseInt(env("AZIR_TX_VECTOR_TCP_PORT", "23309"));
                var leases = new MysqlVectorCutLeaderLease(center, routes, stripes);
                leases.initializeIsolated();
                try (var runtime = new MysqlVectorCutLeaderRuntime(center, routes, stripes,
                        () -> 0, leases, "127.0.0.1:" + port, 5_000, 200);
                     var server = new MysqlVectorCutTcpServer(runtime, port)) {
                    if (!runtime.start()) {
                        throw new IllegalStateException("TCP vector cut benchmark found another leader");
                    }
                    server.start();
                    try (var remote = new MysqlVectorCutTcpClient(center, routes, stripes)) {
                        run(null, runtime, null, remote, left, right, concurrency, durationSeconds, stripes,
                                "LEASED_TCP");
                    }
                }
            } else if (remoteExperiment) {
                int port = Integer.parseInt(env("AZIR_TX_VECTOR_HTTP_PORT", "23308"));
                var leases = new MysqlVectorCutLeaderLease(center, routes, stripes);
                leases.initializeIsolated();
                try (var runtime = new MysqlVectorCutLeaderRuntime(center, routes, stripes,
                        () -> 0, leases, "127.0.0.1:" + port, 5_000, 200);
                     var server = new MysqlVectorCutHttpServer(runtime, port)) {
                    if (!runtime.start()) {
                        throw new IllegalStateException("remote vector cut benchmark found another leader");
                    }
                    server.start();
                    var remote = new MysqlVectorCutRemoteClient(center, routes, stripes);
                    run(null, runtime, remote, null, left, right, concurrency, durationSeconds, stripes,
                            "LEASED_REMOTE");
                }
            } else {
                try (var cut = volatileExperiment
                        ? MysqlPublishedVectorCut.experimentalVolatile(center, routes, stripes, () -> 0)
                        : new MysqlPublishedVectorCut(center, routes, stripes, () -> 0)) {
                    if (volatileExperiment) {
                        cut.initializeVolatile();
                    } else {
                        cut.initializeIsolated();
                    }
                    run(cut, null, null, null, left, right, concurrency, durationSeconds, stripes,
                            volatileExperiment ? "VOLATILE_EXPERIMENT" : "DURABLE_CENTER");
                }
            }
        }
    }

    /** 容量探针只复用同一代码库的连接池配置。 */
    private static HikariDataSource pool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /** 两片各运行单分片事务，发布器一次更新可同时确认多笔提交。 */
    private static void run(MysqlPublishedVectorCut cut, MysqlVectorCutLeaderRuntime runtime,
                            MysqlVectorCutRemoteClient remote, MysqlVectorCutTcpClient tcpRemote,
                            HikariDataSource left,
                            HikariDataSource right, int concurrency,
                            int durationSeconds, int stripes, String mode) throws Exception {
        AtomicLong sequence = new AtomicLong(100);
        LongAdder attempts = new LongAdder();
        LongAdder physical = new LongAdder();
        LongAdder acked = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder cutReads = new LongAdder();
        LongAdder publishCycles = new LongAdder();
        LongAdder publishUpdates = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> ackLatencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> readLatencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> publishLatencies = new ConcurrentLinkedQueue<>();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var background = Executors.newVirtualThreadPerTaskExecutor();
             var workers = Executors.newFixedThreadPool(concurrency)) {
            Future<?> publisher = null;
            if (cut != null) {
                publisher = background.submit(() -> {
                    try {
                        start.await();
                        var previous = cut.currentCut();
                        while (running.get()) {
                            long began = System.nanoTime();
                            var published = cut.publishOnce();
                            publishLatencies.add(System.nanoTime() - began);
                            publishCycles.increment();
                            if (!published.equals(previous)) {
                                publishUpdates.increment();
                                previous = published;
                            }
                            LockSupport.parkNanos(200_000);
                        }
                    } catch (Exception ex) {
                        firstFailure.compareAndSet(null, "publisher:" + ex);
                        running.set(false);
                    }
                });
            }
            var reader = background.submit(() -> {
                try {
                    start.await();
                    while (running.get()) {
                        long began = System.nanoTime();
                        if (tcpRemote != null) {
                            tcpRemote.currentView();
                        } else if (remote == null) {
                            cut.currentCut();
                        } else {
                            remote.currentView();
                        }
                        readLatencies.add(System.nanoTime() - began);
                        cutReads.increment();
                        LockSupport.parkNanos(500_000);
                    }
                } catch (Exception ex) {
                    firstFailure.compareAndSet(null, "reader:" + ex);
                    running.set(false);
                }
            });
            for (int index = 0; index < concurrency; index++) {
                final String shardId = index % 2 == 0 ? "s1" : "s2";
                final HikariDataSource dataSource = index % 2 == 0 ? left : right;
                workers.submit(() -> {
                    boolean readySignaled = false;
                    try (Connection connection = dataSource.getConnection();
                         PreparedStatement call = connection.prepareStatement("""
                                 call bench_striped_mvcc_commit_checked_v2(?, ?, ?, ?)
                                 """)) {
                        ready.countDown();
                        readySignaled = true;
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                        while (System.nanoTime() < deadline && running.get()) {
                            attempts.increment();
                            long began = System.nanoTime();
                            long id = sequence.incrementAndGet();
                            int stripe = (int) (id % stripes);
                            try {
                                call.setBytes(1, ("local-first:" + id).getBytes(StandardCharsets.UTF_8));
                                call.setBytes(2, ("v" + id).getBytes(StandardCharsets.UTF_8));
                                call.setInt(3, stripe);
                                call.setLong(4, 0);
                                long localSeq;
                                try (ResultSet row = call.executeQuery()) {
                                    if (!row.next()) {
                                        throw new SQLException("checked local commit returned no sequence");
                                    }
                                    localSeq = row.getLong(1);
                                }
                                physical.increment();
                                if (runtime == null) {
                                    cut.awaitLocallyPublished(shardId, stripe, localSeq, 0, 5_000);
                                } else {
                                    runtime.awaitPublished(shardId, stripe, localSeq, 0, 5_000);
                                }
                                acked.increment();
                                ackLatencies.add(System.nanoTime() - began);
                            } catch (Exception ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null, "writer:" + ex);
                                if (runtime != null && !runtime.isServing()) {
                                    running.set(false);
                                }
                            }
                        }
                    } catch (Exception ex) {
                        failures.increment();
                        firstFailure.compareAndSet(null, "writer-setup:" + ex);
                    } finally {
                        if (!readySignaled) {
                            ready.countDown();
                        }
                        done.countDown();
                    }
                });
            }
            ready.await();
            long began = System.nanoTime();
            start.countDown();
            done.await();
            double seconds = (System.nanoTime() - began) / 1_000_000_000.0;
            running.set(false);
            if (publisher != null) {
                publisher.get();
            }
            reader.get();
            long[] persistedLeft = persistedCounts(left);
            long[] persistedRight = persistedCounts(right);
            long persisted = persistedLeft[0] + persistedRight[0];
            long watermarks = persistedLeft[1] + persistedRight[1];
            if (persisted != physical.sum() || watermarks != physical.sum()
                    || physical.sum() != acked.sum() || failures.sum() != 0
                    || firstFailure.get() != null) {
                throw new IllegalStateException("published vector probe lost a commit or failed: "
                        + firstFailure.get() + ", physical=" + physical.sum() + ", acked=" + acked.sum()
                        + ", persisted=" + persisted + ", watermarks=" + watermarks);
            }
            System.out.printf("PUBLISHED_VECTOR_CAPACITY mode=%s concurrency=%d duration_s=%.2f "
                            + "attempts=%d physical=%d acked=%d failures=%d ack_tps=%.2f "
                            + "ack_p99_ms=%.3f cut_reads=%d cut_avg_ms=%.3f cut_p99_ms=%.3f "
                            + "publish_cycles=%d publish_updates=%d publish_avg_ms=%.3f "
                            + "publish_p99_ms=%.3f sample_avg_ms=%.3f center_write_avg_ms=%.3f "
                            + "versions=%d watermark_total=%d%n",
                    mode,
                    concurrency, seconds, attempts.sum(), physical.sum(), acked.sum(), failures.sum(),
                    acked.sum() / seconds, p99(ackLatencies), cutReads.sum(), average(readLatencies),
                    p99(readLatencies), publishCycles.sum(), publishUpdates.sum(),
                    average(publishLatencies), p99(publishLatencies),
                    cut == null ? Double.NaN : cut.sampleAverageMillis(),
                    cut == null ? Double.NaN : cut.centerWriteAverageMillis(), persisted, watermarks);
        }
    }

    /** 分片版本数与水位增量必须等于所有物理成功事务。 */
    private static long[] persistedCounts(HikariDataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select (select count(*) from bench_striped_mvcc_data) as versions,
                            (select sum(committed_seq) from bench_striped_mvcc_state) as watermarks
                     """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("published vector persistence count returned no row");
            }
            return new long[]{row.getLong("versions"), row.getLong("watermarks")};
        }
    }

    /** 计算一次容量探针中实际完成操作的均值毫秒数。 */
    private static double average(ConcurrentLinkedQueue<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(Double.NaN) / 1_000_000.0;
    }

    /** 按实际样本统计 P99，不用连接池等待或外部停机时间代替。 */
    private static double p99(ConcurrentLinkedQueue<Long> values) {
        long[] sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
        return sorted.length == 0 ? Double.NaN
                : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
    }

    /** 只读取探针显式参数，不使用产品默认环境。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

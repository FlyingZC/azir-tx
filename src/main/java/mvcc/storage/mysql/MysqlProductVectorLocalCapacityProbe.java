package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.metadata.mysql.MysqlLocalFirstMetadataView;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/** 测量正式单键向量 1PC、持租约发布与远端 TCP 读切面的同形容量。 */
public final class MysqlProductVectorLocalCapacityProbe {
    /** 容量入口不保存全局数据库连接。 */
    private MysqlProductVectorLocalCapacityProbe() {
    }

    /** 必须显式选择全新隔离库，已有 MVCC 表时拒绝覆盖。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("product vector local probe requires AZIR_TX_RESET=true and fresh databases");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int port = Integer.parseInt(env("AZIR_TX_VECTOR_TCP_PORT", "23309"));
        boolean javaFallback = Boolean.parseBoolean(env("AZIR_TX_VECTOR_JAVA_FALLBACK", "false"));
        boolean batchJavaFallback = Boolean.parseBoolean(env("AZIR_TX_VECTOR_BATCH_JAVA_FALLBACK", "false"));
        int logicalBatch = Integer.parseInt(env("AZIR_TX_VECTOR_LOGICAL_BATCH", "1"));
        int coalesceBatch = Integer.parseInt(env("AZIR_TX_VECTOR_COALESCE_BATCH", "0"));
        int batchLanes = Integer.parseInt(env("AZIR_TX_VECTOR_BATCH_LANES", "6"));
        long batchWaitMicros = Long.parseLong(env("AZIR_TX_VECTOR_BATCH_WAIT_MICROS", "500"));
        boolean gcEnabled = Boolean.parseBoolean(env("AZIR_TX_VECTOR_LOCAL_GC", "false"));
        boolean reuseKeys = Boolean.parseBoolean(env("AZIR_TX_VECTOR_REUSE_KEYS", "false"));
        int gcBatchLimit = Integer.parseInt(env("AZIR_TX_VECTOR_GC_BATCH_LIMIT", "64"));
        long gcIntervalMillis = Long.parseLong(env("AZIR_TX_VECTOR_GC_INTERVAL_MS", "200"));
        int windowSeconds = Integer.parseInt(env("AZIR_TX_BENCH_WINDOW_SECONDS", "0"));
        if (concurrency < 1 || concurrency > 512 || seconds < 1 || seconds > 120
                || stripes < 1 || stripes > 256 || port < 1 || port > 65535
                || logicalBatch < 1 || logicalBatch > 16 || javaFallback && logicalBatch > 1
                || coalesceBatch < 0 || coalesceBatch > 16 || coalesceBatch > 0 && logicalBatch != 1
                || batchLanes < 1 || batchLanes > 32 || batchWaitMicros < 0 || batchWaitMicros > 10_000
                || gcBatchLimit < 1 || gcBatchLimit > 4096
                || gcIntervalMillis < 1 || gcIntervalMillis > 60_000
                || windowSeconds < 0 || windowSeconds > 60) {
            throw new IllegalArgumentException("invalid product vector local probe limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        String controlUrl = env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        String firstUrl = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String secondUrl = env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2");
        try (HikariDataSource center = pool(controlUrl, username, password);
             HikariDataSource left = pool(firstUrl, username, password);
             HikariDataSource right = pool(secondUrl, username, password)) {
            requireEmptyMvccSchema(center);
            requireEmptyMvccSchema(left);
            requireEmptyMvccSchema(right);
            var ledger = new MysqlLocalFirstPublicationLedger(center);
            ledger.initializeIsolated(100);
            var metadata = new MysqlLocalFirstMetadataView(ledger);
            var first = new MysqlStorageAdapter("s1", left, metadata);
            var second = new MysqlStorageAdapter("s2", right, metadata);
            first.initialize();
            second.initialize();
            first.initializeVectorProtocol(stripes);
            second.initializeVectorProtocol(stripes);
            Map<String, DataSource> routes = Map.of("s1", left, "s2", right);
            var leases = MysqlVectorCutLeaderLease.forProduct(center, routes, stripes);
            leases.initializeProduct();
            try (var runtime = MysqlVectorCutLeaderRuntime.forProduct(center, routes, stripes,
                    ledger::currentPublished, leases, "127.0.0.1:" + port, 5_000, 200);
                 var server = new MysqlVectorCutTcpServer(runtime, port);
                 var remote = MysqlVectorCutTcpClient.forProduct(center, routes, stripes)) {
                if (!runtime.start()) {
                    throw new IllegalStateException("product vector local probe found another active leader");
                }
                server.start();
                if (gcEnabled) {
                    try (HikariDataSource backgroundCenter = backgroundPool(controlUrl, username, password);
                         HikariDataSource backgroundLeft = backgroundPool(firstUrl, username, password);
                         HikariDataSource backgroundRight = backgroundPool(secondUrl, username, password)) {
                        var foregroundLeases = new MysqlVectorSnapshotLeaseStore(center,
                                List.of("s1", "s2"), stripes);
                        var backgroundLeases = new MysqlVectorSnapshotLeaseStore(backgroundCenter,
                                List.of("s1", "s2"), stripes);
                        foregroundLeases.initializeProduct();
                        backgroundLeases.initializeProduct();
                        var firstGc = new MysqlVectorLocalGarbageCollector(backgroundLeft,
                                "s1", stripes, backgroundLeases);
                        var secondGc = new MysqlVectorLocalGarbageCollector(backgroundRight,
                                "s2", stripes, backgroundLeases);
                        firstGc.initializeProduct();
                        secondGc.initializeProduct();
                        try (var protection = new MysqlVectorReaderProtectionRuntime(
                                foregroundLeases, () -> runtime.currentView().snapshot(),
                                "vector-local-benchmark-reader", 5_000, 1_000, 10_000);
                             var gc = new MysqlVectorLocalGcScheduler(runtime, backgroundLeases,
                                     Map.of("s1", firstGc, "s2", secondGc),
                                     gcBatchLimit, gcIntervalMillis)) {
                            protection.start();
                            gc.start();
                            runConfigured(first, second, left, right, runtime, remote,
                                    concurrency, seconds, stripes, javaFallback, batchJavaFallback,
                                    logicalBatch, coalesceBatch, batchLanes, batchWaitMicros,
                                    reuseKeys, gc, windowSeconds);
                            System.out.printf("PRODUCT_VECTOR_LOCAL_GC runs=%d failures=%d "
                                            + "last_reclaimed=%d total_reclaimed=%d first_error=%s%n",
                                    gc.stats().runCount(), gc.stats().failureCount(),
                                    gc.stats().lastProcessedCount(), gc.totalReclaimed(),
                                    gc.lastFailure());
                            if (gc.stats().runCount() == 0 || gc.stats().failureCount() != 0) {
                                throw new IllegalStateException("product vector local background GC failed");
                            }
                        }
                    }
                } else {
                    runConfigured(first, second, left, right, runtime, remote,
                            concurrency, seconds, stripes, javaFallback, batchJavaFallback,
                            logicalBatch, coalesceBatch, batchLanes, batchWaitMicros,
                            reuseKeys, null, windowSeconds);
                }
            }
        }
    }

    /** 沿用当前产品连接池配置，避免把不同超时预算混入吞吐对照。 */
    private static HikariDataSource pool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /** GC 必须使用与前台物理同权威但独立预算的小连接池。 */
    private static HikariDataSource backgroundPool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, username, password);
    }

    /** 两种维护模式复用完全相同的单键业务写入与切面 ACK 负载。 */
    private static void runConfigured(MysqlStorageAdapter first, MysqlStorageAdapter second,
                                      HikariDataSource left, HikariDataSource right,
                                      MysqlVectorCutLeaderRuntime runtime,
                                      MysqlVectorCutTcpClient remote,
                                      int concurrency, int seconds, int stripes,
                                      boolean javaFallback, boolean batchJavaFallback,
                                      int logicalBatch, int coalesceBatch,
                                      int batchLanes, long batchWaitMicros,
                                      boolean reuseKeys,
                                      MysqlVectorLocalGcScheduler gc,
                                      int windowSeconds) throws Exception {
        if (coalesceBatch > 0) {
            try (var firstBatcher = new MysqlVectorLocalCommitBatcher(
                    first, batchLanes, coalesceBatch, batchWaitMicros, 4096);
                 var secondBatcher = new MysqlVectorLocalCommitBatcher(
                         second, batchLanes, coalesceBatch, batchWaitMicros, 4096)) {
                run(first, second, firstBatcher, secondBatcher, left, right, runtime, remote,
                        concurrency, seconds, stripes, javaFallback, batchJavaFallback,
                        logicalBatch, reuseKeys, gc, windowSeconds);
            }
        } else {
            run(first, second, null, null, left, right, runtime, remote,
                    concurrency, seconds, stripes, javaFallback, batchJavaFallback,
                    logicalBatch, reuseKeys, gc, windowSeconds);
        }
    }

    /** 真正跑容量前确认没有已有产品 MVCC 表，避免误把旧业务数据当作探针输入。 */
    private static void requireEmptyMvccSchema(HikariDataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select table_name from information_schema.tables
                where table_schema = database() and left(table_name, 5) = 'mvcc_'
                limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                throw new IllegalArgumentException("product vector local probe requires an empty isolated database");
            }
        }
    }

    /** 两片各执行单分片写，ACK 只在正式发布切面包含该序号后计数。 */
    private static void run(MysqlStorageAdapter first, MysqlStorageAdapter second,
                            MysqlVectorLocalCommitBatcher firstBatcher,
                            MysqlVectorLocalCommitBatcher secondBatcher,
                            HikariDataSource left, HikariDataSource right,
                            MysqlVectorCutLeaderRuntime runtime, MysqlVectorCutTcpClient remote,
                            int concurrency, int seconds, int stripes,
                            boolean javaFallback, boolean batchJavaFallback,
                            int logicalBatch, boolean reuseKeys,
                            MysqlVectorLocalGcScheduler gc,
                            int windowSeconds) throws Exception {
        AtomicLong sequence = new AtomicLong(100);
        LongAdder attempts = new LongAdder();
        LongAdder physical = new LongAdder();
        LongAdder acked = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder cutReads = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> ackLatencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> physicalLatencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> publicationWaits = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> readLatencies = new ConcurrentLinkedQueue<>();
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var background = Executors.newVirtualThreadPerTaskExecutor();
             var workers = Executors.newFixedThreadPool(concurrency)) {
            var reader = background.submit(() -> {
                try {
                    start.await();
                    while (running.get()) {
                        long began = System.nanoTime();
                        remote.currentView();
                        readLatencies.add(System.nanoTime() - began);
                        cutReads.increment();
                        LockSupport.parkNanos(500_000);
                    }
                } catch (Exception ex) {
                    firstFailure.compareAndSet(null, "reader:" + ex);
                    running.set(false);
                }
            });
            var reporter = windowSeconds == 0 ? null : background.submit(() -> {
                try {
                    start.await();
                    long previous = 0;
                    long previousAt = System.nanoTime();
                    int index = 0;
                    while (running.get()) {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(windowSeconds));
                        if (!running.get()) {
                            break;
                        }
                        long now = System.nanoTime();
                        long current = acked.sum();
                        long reclaimed = gc == null ? 0 : gc.totalReclaimed();
                        System.out.printf("PRODUCT_VECTOR_LOCAL_WINDOW index=%d elapsed_s=%.2f "
                                        + "ack_tps=%.2f acked=%d estimated_remaining=%d "
                                        + "gc_reclaimed=%d%n",
                                ++index, (now - previousAt) / 1_000_000_000.0,
                                (current - previous) / ((now - previousAt) / 1_000_000_000.0),
                                current, current - reclaimed, reclaimed);
                        previous = current;
                        previousAt = now;
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
            for (int index = 0; index < concurrency; index++) {
                String shardId = index % 2 == 0 ? "s1" : "s2";
                MysqlStorageAdapter adapter = index % 2 == 0 ? first : second;
                MysqlVectorLocalCommitBatcher batcher = index % 2 == 0 ? firstBatcher : secondBatcher;
                String fixedKey = "local-first:worker:" + index;
                int fixedStripe = index % stripes;
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
                        while (System.nanoTime() < deadline && running.get()) {
                            long began = System.nanoTime();
                            List<MysqlStorageAdapter.LocalVectorCommitRequest> batch =
                                    new ArrayList<>(logicalBatch);
                            for (int item = 0; item < logicalBatch; item++) {
                                long id = sequence.incrementAndGet();
                                int stripe = reuseKeys ? fixedStripe : (int) (id % stripes);
                                long startSequence = reuseKeys
                                        ? remote.currentView().snapshot().sequence(shardId, stripe) : 0;
                                Mutation mutation = Mutation.put(reuseKeys ? fixedKey : "local-first:" + id,
                                        ("v" + id).getBytes(StandardCharsets.UTF_8));
                                batch.add(new MysqlStorageAdapter.LocalVectorCommitRequest(
                                        -id, mutation, stripe, startSequence));
                            }
                            attempts.add(batch.size());
                            try {
                                List<Long> localSequences;
                                if (batcher != null) {
                                    var request = batch.getFirst();
                                    localSequences = List.of(batcher.submit(request, 5_000)
                                            .get(5, TimeUnit.SECONDS));
                                } else if (logicalBatch == 1) {
                                    var request = batch.getFirst();
                                    long localSeq = javaFallback
                                            ? adapter.directCommitVectorLocalJavaForProbe(request.txId(),
                                            request.mutation(), request.stripe(), request.startSequence())
                                            : adapter.directCommitVectorLocal(request.txId(),
                                            request.mutation(), request.stripe(), request.startSequence());
                                    localSequences = List.of(localSeq);
                                } else {
                                    localSequences = batchJavaFallback
                                            ? adapter.directCommitVectorLocalBatchJavaForProbe(batch)
                                            : adapter.directCommitVectorLocalBatch(batch);
                                }
                                long physicallyCommittedAt = System.nanoTime();
                                physical.add(batch.size());
                                for (int item = 0; item < batch.size(); item++) {
                                    physicalLatencies.add(physicallyCommittedAt - began);
                                    runtime.awaitPublished(shardId, batch.get(item).stripe(),
                                            localSequences.get(item), 0, 5_000);
                                }
                                long acknowledgedAt = System.nanoTime();
                                acked.add(batch.size());
                                for (int item = 0; item < batch.size(); item++) {
                                    publicationWaits.add(acknowledgedAt - physicallyCommittedAt);
                                    ackLatencies.add(acknowledgedAt - began);
                                }
                            } catch (Exception ex) {
                                failures.add(batch.size());
                                firstFailure.compareAndSet(null, "writer:" + ex);
                                if (!runtime.isServing()) {
                                    running.set(false);
                                }
                            }
                        }
                    } catch (Exception ex) {
                        failures.increment();
                        firstFailure.compareAndSet(null, "writer-setup:" + ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long began = System.nanoTime();
            start.countDown();
            done.await();
            double duration = (System.nanoTime() - began) / 1_000_000_000.0;
            running.set(false);
            reader.get();
            if (reporter != null) {
                reporter.cancel(true);
            }
            if (gc != null) {
                gc.close();
            }
            long[] leftCounts = persistedCounts(left);
            long[] rightCounts = persistedCounts(right);
            long versions = leftCounts[0] + rightCounts[0];
            long watermarks = leftCounts[1] + rightCounts[1];
            long reclaimed = gc == null ? 0 : gc.totalReclaimed();
            long batchCount = firstBatcher == null ? 0
                    : firstBatcher.batchCount() + secondBatcher.batchCount();
            long groupedCount = firstBatcher == null ? 0
                    : firstBatcher.groupedCount() + secondBatcher.groupedCount();
            long fallbackCount = firstBatcher == null ? 0
                    : firstBatcher.fallbackCount() + secondBatcher.fallbackCount();
            if (failures.sum() != 0 || firstFailure.get() != null
                    || versions + reclaimed != physical.sum()
                    || watermarks != physical.sum() || physical.sum() != acked.sum()) {
                throw new IllegalStateException("product vector local probe failed: " + firstFailure.get()
                        + ", versions=" + versions + ", watermarks=" + watermarks
                        + ", reclaimed=" + reclaimed + ", physical=" + physical.sum()
                        + ", acked=" + acked.sum());
            }
            System.out.printf("PRODUCT_VECTOR_LOCAL_CAPACITY path=%s concurrency=%d logical_batch=%d "
                            + "reused_keys=%s duration_s=%.2f "
                            + "attempts=%d physical=%d acked=%d failures=%d ack_tps=%.2f "
                            + "ack_p99_ms=%.3f physical_avg_ms=%.3f physical_p99_ms=%.3f "
                            + "wait_avg_ms=%.3f wait_p99_ms=%.3f "
                            + "cut_reads=%d cut_avg_ms=%.3f cut_p99_ms=%.3f "
                            + "group_batches=%d group_items=%d group_avg_size=%.2f group_fallbacks=%d "
                            + "versions=%d gc_reclaimed=%d watermark_total=%d%n",
                    firstBatcher != null ? "COALESCED" : logicalBatch > 1
                            ? batchJavaFallback ? "JAVA_GROUP" : "PROCEDURE_GROUP"
                            : javaFallback ? "JAVA" : "PROCEDURE",
                    concurrency, logicalBatch, reuseKeys, duration, attempts.sum(),
                    physical.sum(), acked.sum(), failures.sum(),
                    acked.sum() / duration, p99(ackLatencies), average(physicalLatencies),
                    p99(physicalLatencies), average(publicationWaits), p99(publicationWaits), cutReads.sum(),
                    average(readLatencies), p99(readLatencies), batchCount, groupedCount,
                    batchCount == 0 ? 0 : (double) groupedCount / batchCount,
                    fallbackCount, versions, reclaimed, watermarks);
        }
    }

    /** 已提交本地版本数必须等于各分片分条计数之和。 */
    private static long[] persistedCounts(HikariDataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select (select count(*) from mvcc_vector_local_data_v1) as versions,
                       (select sum(committed_seq) from mvcc_vector_stripe_state_v1) as watermarks
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("product vector local count returned no row");
            }
            return new long[]{row.getLong("versions"), row.getLong("watermarks")};
        }
    }

    /** 计算一次有界探针中实际完成读取的平均毫秒数。 */
    private static double average(ConcurrentLinkedQueue<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(Double.NaN) / 1_000_000.0;
    }

    /** 按真实样本排序得到 P99，不用并发度倒推尾延迟。 */
    private static double p99(ConcurrentLinkedQueue<Long> values) {
        long[] sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
        return sorted.length == 0 ? Double.NaN
                : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
    }

    /** 容量参数只来自本探针显式环境变量。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

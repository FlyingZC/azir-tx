package mvcc.e2e;

import mvcc.common.IsolationLevel;
import mvcc.percolator.MysqlPercolatorStore;
import mvcc.percolator.PercolatorRuntime;
import mvcc.percolator.PercolatorStore;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.tso.TimestampOracle;
import mvcc.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 等语义 Percolator 逐键与同片批量对照；每个事务写两片各两键并等待物化。 */
public final class MysqlPercolatorBenchmark {
    /** 禁止实例化基准入口。 */
    private MysqlPercolatorBenchmark() { }

    /** 单轮计时结果；延迟按每个完整事务计算，错误必须显式报告。 */
    public record Result(boolean batchByShard, int concurrency, long committed, long failures,
                         double seconds, double tps, double p50Millis, double p95Millis, double p99Millis) {
        /** 生成不含连接凭据的单行 JSON 原始结果。 */
        public String json() {
            return String.format(Locale.ROOT,
                    "{\"batchByShard\":%s,\"concurrency\":%d,\"keysPerTransaction\":4,\"committed\":%d,\"failures\":%d,\"seconds\":%.3f,\"tps\":%.2f,\"p50Millis\":%.3f,\"p95Millis\":%.3f,\"p99Millis\":%.3f}",
                    batchByShard, concurrency, committed, failures, seconds, tps, p50Millis, p95Millis, p99Millis);
        }
    }

    /** 显式读取专用测试数据库地址；不删除任何已有数据或重置时间戳。 */
    public static void main(String[] args) throws Exception {
        String user = required("AZIR_PERCOLATOR_USER");
        String password = required("AZIR_PERCOLATOR_PASSWORD");
        DataSource poolA = MysqlDataSourceFactory.create(required("AZIR_PERCOLATOR_SHARD_A_URL"), user, password);
        DataSource poolB = MysqlDataSourceFactory.create(required("AZIR_PERCOLATOR_SHARD_B_URL"), user, password);
        try {
            var a = new MysqlPercolatorStore("a", poolA);
            var b = new MysqlPercolatorStore("b", poolB);
            a.initialize();
            b.initialize();
            var tso = new MysqlTimestampOracle(poolA);
            tso.initialize(100);
            int concurrency = Integer.parseInt(System.getenv().getOrDefault("AZIR_PERCOLATOR_CONCURRENCY", "50"));
            int duration = Integer.parseInt(System.getenv().getOrDefault("AZIR_PERCOLATOR_SECONDS", "10"));
            for (boolean batch : List.of(false, true, true, false)) {
                run(tso, Map.of("a", a, "b", b), concurrency, 2, batch);
                System.out.println(run(tso, Map.of("a", a, "b", b), concurrency, duration, batch).json());
            }
        } finally {
            if (poolA instanceof AutoCloseable closeable) { closeable.close(); }
            if (poolB instanceof AutoCloseable closeable) { closeable.close(); }
        }
    }

    /** 多工作线程各自使用固定且互不冲突的四键，计时仅包括提交完成于窗口内的事务。 */
    public static Result run(TimestampOracle tso, Map<String, ? extends PercolatorStore> stores,
                             int concurrency, int durationSeconds, boolean batch) throws Exception {
        if (concurrency < 1 || concurrency > 4096 || durationSeconds < 1 || durationSeconds > 300) {
            throw new IllegalArgumentException("invalid benchmark concurrency or duration");
        }
        var options = new PercolatorRuntime.Options(60_000, 4096, 16L * 1024 * 1024, batch);
        var runtime = new PercolatorRuntime(tso, stores, options);
        var ready = new CountDownLatch(concurrency);
        var gate = new CountDownLatch(1);
        var end = new AtomicLong(Long.MAX_VALUE);
        var committed = new AtomicLong();
        var failures = new AtomicLong();
        var firstError = new AtomicReference<Throwable>();
        var latencies = new ConcurrentLinkedQueue<Long>();
        String prefix = "pc-bench:" + UUID.randomUUID() + ":";
        // 与仓库既有 JDBC 基准一致使用固定平台线程，避免虚拟线程调度干扰 A/B 容量对照。
        try (var executor = Executors.newFixedThreadPool(concurrency)) {
            List<Future<?>> tasks = new java.util.ArrayList<>();
            for (int worker = 0; worker < concurrency; worker++) {
                final String workerPrefix = prefix + worker + ":";
                tasks.add(executor.submit(() -> {
                    ready.countDown();
                    try {
                        gate.await();
                        long iteration = 0;
                        while (System.nanoTime() < end.get() && firstError.get() == null) {
                            long started = System.nanoTime();
                            try (var tx = runtime.begin(IsolationLevel.REPEATABLE_READ)) {
                                byte[] value = Long.toString(++iteration).getBytes(StandardCharsets.UTF_8);
                                for (String shard : List.of("a", "b")) {
                                    tx.put(shard, workerPrefix + "0", value);
                                    tx.put(shard, workerPrefix + "1", value);
                                }
                                var result = tx.commit();
                                if (!result.pendingSecondaries().isEmpty()) {
                                    throw new IllegalStateException("benchmark requires all secondaries materialized");
                                }
                                long finished = System.nanoTime();
                                if (finished <= end.get()) {
                                    committed.incrementAndGet();
                                    latencies.add(finished - started);
                                }
                            } catch (RuntimeException failure) {
                                failures.incrementAndGet();
                                firstError.compareAndSet(null, failure);
                            }
                        }
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        firstError.compareAndSet(null, failure);
                    }
                }));
            }
            if (!ready.await(30, TimeUnit.SECONDS)) {
                gate.countDown();
                end.set(0);
                throw new IllegalStateException("benchmark workers not ready");
            }
            long start = System.nanoTime();
            end.set(start + TimeUnit.SECONDS.toNanos(durationSeconds));
            gate.countDown();
            for (Future<?> task : tasks) { task.get(durationSeconds + 60L, TimeUnit.SECONDS); }
            if (firstError.get() != null) { throw new IllegalStateException("Percolator benchmark failed", firstError.get()); }
            long[] sorted = latencies.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(sorted);
            if (sorted.length == 0) { throw new IllegalStateException("no completed transactions in measurement window"); }
            return new Result(batch, concurrency, committed.get(), failures.get(), durationSeconds,
                    committed.get() / (double) durationSeconds, percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99));
        }
    }

    /** 按最近秩百分位计算完整事务延迟。 */
    private static double percentile(long[] sorted, double percentile) {
        int index = Math.max(0, (int) Math.ceil(sorted.length * percentile) - 1);
        return sorted[index] / 1_000_000.0;
    }

    /** 缺少显式配置时停止，避免默认连接业务库。 */
    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) { throw new IllegalArgumentException("missing environment variable " + name); }
        return value;
    }
}

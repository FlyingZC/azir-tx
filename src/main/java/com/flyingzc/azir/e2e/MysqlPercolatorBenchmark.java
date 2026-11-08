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

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.percolator.MysqlPercolatorStore;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.percolator.PercolatorStore;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
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

/**
 * The semantic Percolator type-by-key against the same batch; each transaction writes two keys and waits for materialization.
 */
public final class MysqlPercolatorBenchmark {

    /**
     * Prohibits the exemplification of benchmark entrance.
     */
    private MysqlPercolatorBenchmark() {
    }

    /**
     * Visible read specific database address; no existing data removed or timestamp reset deleted.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
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
            if (poolA instanceof AutoCloseable closeable) {
                closeable.close();
            }
            if (poolB instanceof AutoCloseable closeable) {
                closeable.close();
            }
        }
    }

    /**
     * Multi-workspaces each use a fixed and non-controversial four key, which only includes commit completed in the window.
     *
     * @param tso tso
     * @param stores stores
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param batch batch
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static Result run(final TimestampOracle tso, final Map<String, ? extends PercolatorStore> stores,
                             final int concurrency, final int durationSeconds, final boolean batch) throws Exception {
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
        // Use of fixed-platform threads in line with existing JDBC benchmark in the warehouse to avoid virtual linear scheduling interference with A/B capacity.
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
                            } catch (final RuntimeException failure) {
                                failures.incrementAndGet();
                                firstError.compareAndSet(null, failure);
                            }
                        }
                    } catch (final InterruptedException failure) {
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
            for (Future<?> task : tasks) {
                task.get(durationSeconds + 60L, TimeUnit.SECONDS);
            }
            if (firstError.get() != null) {
                throw new IllegalStateException("Percolator benchmark failed", firstError.get());
            }
            long[] sorted = latencies.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(sorted);
            if (sorted.length == 0) {
                throw new IllegalStateException("no completed transactions in measurement window");
            }
            return new Result(batch, concurrency, committed.get(), failures.get(), durationSeconds,
                    committed.get() / (double) durationSeconds, percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99));
        }
    }

    /**
     * Full transaction delay based on the latest percentile.
     *
     * @param sorted sorted
     * @param percentile percentile
     * @return the operation result
     */
    private static double percentile(final long[] sorted, final double percentile) {
        int index = Math.max(0, (int) Math.ceil(sorted.length * percentile) - 1);
        return sorted[index] / 1_000_000.0;
    }

    /**
     * Stops when visible configuration is missing, avoiding default connection business library.
     *
     * @param name name
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static String required(final String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing environment variable " + name);
        }
        return value;
    }

    /**
     * Single-wheeled timing results; delays are calculated on a full transaction basis, and errors must be visiblely reported.
     *
     * @param batchByShard batch by shard
     * @param concurrency concurrency
     * @param committed committed
     * @param failures failures
     * @param seconds seconds
     * @param tps tps
     * @param p50Millis p50 millis
     * @param p95Millis p95 millis
     * @param p99Millis p99 millis
     */
    public record Result(boolean batchByShard, int concurrency, long committed, long failures,
            double seconds, double tps, double p50Millis, double p95Millis, double p99Millis) {

        /**
         * Generates a single-line original JSON result that does not contain connection receipt.
         *
         * @return the operation result
         */
        public String json() {
            return String.format(Locale.ROOT,
                    "{\"batchByShard\":%s,\"concurrency\":%d,\"keysPerTransaction\":4,\"committed\":%d,\"failures\":%d,\"seconds\":%.3f,\"tps\":%.2f,\"p50Millis\":%.3f,\"p95Millis\":%.3f,\"p99Millis\":%.3f}",
                    batchByShard, concurrency, committed, failures, seconds, tps, p50Millis, p95Millis, p99Millis);
        }
    }
}

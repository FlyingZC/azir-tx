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

package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.storage.Mutation;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Only the upper capacity of local fence and version transaction is measured in the isolation chamber, which does not represent a complete global RC/RR protocol.
 */
public final class MysqlLocalFirstCapacityProbe {

    /**
     * The benchmark entry point requires no instance state.
     */
    private MysqlLocalFirstCapacityProbe() {
    }

    /**
     * Visible reset-separation shard after timeless single-key physics commit.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("local-first capacity probe requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "20"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 600) {
            throw new IllegalArgumentException("invalid local-first capacity probe limits");
        }
        String url = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        try (
                HikariDataSource dataSource = (HikariDataSource) MysqlDataSourceFactory.create(url,
                        env("AZIR_TX_USERNAME", "root"), env("AZIR_TX_PASSWORD", "root"))) {
            reset(dataSource);
            var shard = new MysqlStorageAdapter("shard-1", dataSource);
            shard.initialize();
            var fence = new MysqlShardSegmentFence("shard-1", dataSource);
            fence.initializeIsolated();
            if (Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_PROC_V3", "false"))) {
                fence.enableSingleKeyProcedureV3();
            } else if (Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_PROC", "false"))) {
                fence.enableSingleKeyProcedure();
            }
            var owner = new PublicationSegmentLease(101, 1_000_000, "capacity-probe", 1, Long.MAX_VALUE);
            fence.install(owner, 600_000);
            run(shard, fence, owner, concurrency, durationSeconds);
        }
    }

    /**
     * Only the shard version and local paragraphs of this benchmark library have been cleared to prove that the caller must use isolation database.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void reset(final HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_segment_candidate_v1");
            statement.execute("drop table if exists mvcc_local_segment_fence_v1");
            statement.execute("drop table if exists mvcc_local_segment_state_v1");
        }
        new MysqlStorageAdapter("shard-1", dataSource).initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
        }
    }

    /**
     * All work lines start with the same barrier and result both throughput, tail delay and failure.
     *
     * @param shard logical shard identifier
     * @param fence fence
     * @param owner owner
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @throws Exception if the operation cannot complete
     */
    private static void run(final MysqlStorageAdapter shard, final MysqlShardSegmentFence fence,
                            final PublicationSegmentLease owner, final int concurrency, final int durationSeconds) throws Exception {
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
                    ready.countDown();
                    try {
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                        while (System.nanoTime() < deadline) {
                            attempts.increment();
                            long commitTs = sequence.incrementAndGet();
                            long txId = commitTs + 1_000_000;
                            long started = System.nanoTime();
                            try {
                                shard.directCommitFenced(fence, owner, txId, 100, commitTs,
                                        System.currentTimeMillis() + 600_000,
                                        IsolationLevel.REPEATABLE_READ,
                                        List.of(Mutation.put("local-first:" + commitTs,
                                                ("v" + commitTs).getBytes(StandardCharsets.UTF_8))));
                                committed.increment();
                                latencies.add(System.nanoTime() - started);
                            } catch (final RuntimeException ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null, ex.getClass().getSimpleName() + ":" + ex.getMessage());
                            }
                        }
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
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
            System.out.printf("LOCAL_FIRST_SHARD_UPPER_BOUND concurrency=%d duration_s=%.2f "
                    + "procedure_v3=%s attempts=%d committed=%d failures=%d "
                    + "commit_tps=%.2f p99_ms=%.2f "
                    + "first_failure=%s%n",
                    concurrency, seconds,
                    Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_PROC_V3", "false")),
                    attempts.sum(), committed.sum(), failures.sum(),
                    committed.sum() / seconds, p99Millis,
                    firstFailure.get() == null ? "-" : firstFailure.get());
        }
    }

    /**
     * Reads non-empty configuration from a visible benchmark environment.
     *
     * @param key business key
     * @param fallback fallback
     * @return the operation result
     */
    private static String env(final String key, final String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}

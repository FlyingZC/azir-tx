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

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lease launcher-driven security vector propulsion and background running time for local, cross-shard old version bounded collection.
 */
final class MysqlVectorLocalGcScheduler implements AutoCloseable {

    private final MysqlVectorCutLeaderRuntime publisher;

    private final MysqlVectorSnapshotLeaseStore protection;

    private final Map<String, MysqlVectorLocalGarbageCollector> collectors;

    private final Map<String, MysqlVectorCrossGarbageCollector> crossCollectors;

    private final int batchLimit;

    private final int crossBatchLimit;

    private final int localEveryRounds;

    private final long intervalMillis;

    private final ScheduledExecutorService worker;

    private final BackgroundTaskStats stats = new BackgroundTaskStats();

    private final LongAdder totalReclaimed = new LongAdder();

    private final LongAdder crossReclaimed = new LongAdder();

    private final LongAdder localCollectNanos = new LongAdder();

    private final LongAdder crossCollectNanos = new LongAdder();

    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();

    private final AtomicBoolean started = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicLong rounds = new AtomicLong();

    /**
     * Background shard connection pool is provided independently by the caller and only bounded lines per round.
     *
     * @param publisher publisher
     * @param protection protection
     * @param collectors collectors
     * @param batchLimit batch limit
     * @param intervalMillis interval millis
     */
    MysqlVectorLocalGcScheduler(final MysqlVectorCutLeaderRuntime publisher,
                                final MysqlVectorSnapshotLeaseStore protection,
                                final Map<String, MysqlVectorLocalGarbageCollector> collectors,
                                final int batchLimit, final long intervalMillis) {
        this(publisher, protection, collectors, Map.of(), batchLimit, 0, intervalMillis);
    }

    /**
     * Optional cross-shard collection runs after advancing with the same credible security vector and has an independent candidate budget.
     *
     * @param publisher publisher
     * @param protection protection
     * @param collectors collectors
     * @param crossCollectors cross collectors
     * @param batchLimit batch limit
     * @param crossBatchLimit cross batch limit
     * @param intervalMillis interval millis
     */
    MysqlVectorLocalGcScheduler(final MysqlVectorCutLeaderRuntime publisher,
                                final MysqlVectorSnapshotLeaseStore protection,
                                final Map<String, MysqlVectorLocalGarbageCollector> collectors,
                                final Map<String, MysqlVectorCrossGarbageCollector> crossCollectors,
                                final int batchLimit, final int crossBatchLimit, final long intervalMillis) {
        this(publisher, protection, collectors, crossCollectors,
                batchLimit, crossBatchLimit, 1, intervalMillis);
    }

    /**
     * The cross-shard lead load reduces the non-receivable local scanning frequency, and the lower secure boundary is still per round.
     *
     * @param publisher publisher
     * @param protection protection
     * @param collectors collectors
     * @param crossCollectors cross collectors
     * @param batchLimit batch limit
     * @param crossBatchLimit cross batch limit
     * @param localEveryRounds local every rounds
     * @param intervalMillis interval millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorLocalGcScheduler(final MysqlVectorCutLeaderRuntime publisher,
                                final MysqlVectorSnapshotLeaseStore protection,
                                final Map<String, MysqlVectorLocalGarbageCollector> collectors,
                                final Map<String, MysqlVectorCrossGarbageCollector> crossCollectors,
                                final int batchLimit, final int crossBatchLimit,
                                final int localEveryRounds, final long intervalMillis) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.collectors = Map.copyOf(collectors);
        this.crossCollectors = Map.copyOf(crossCollectors);
        if (this.collectors.isEmpty() || this.collectors.keySet().stream().anyMatch(String::isBlank)
                || batchLimit < 1 || batchLimit > 4096
                || this.crossCollectors.isEmpty() && crossBatchLimit != 0
                || !this.crossCollectors.isEmpty() && (crossBatchLimit < 1 || crossBatchLimit > 512)
                || localEveryRounds < 1 || localEveryRounds > 1_000
                || intervalMillis < 1 || intervalMillis > 60_000) {
            throw new IllegalArgumentException("invalid vector local GC scheduler limits");
        }
        requireMatchingAuthorities();
        this.batchLimit = batchLimit;
        this.crossBatchLimit = crossBatchLimit;
        this.localEveryRounds = localEveryRounds;
        this.intervalMillis = intervalMillis;
        this.worker = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("azir-vector-local-gc").unstarted(task));
    }

    /**
     * Check control, shard, layout and independent background connection budget for real MySQL library identity before startup.
     *
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void requireMatchingAuthorities() {
        if (!publisher.productMode()
                || !publisher.layoutHash().equals(protection.layoutHash())
                || !collectors.keySet().equals(publisher.shardSources().keySet())
                || publisher.centerDataSource() == protection.centerDataSource()
                || !MysqlVectorDatabaseIdentity.read(publisher.centerDataSource())
                        .equals(MysqlVectorDatabaseIdentity.read(protection.centerDataSource()))) {
            throw new IllegalArgumentException("vector GC center authority or route layout differs from publisher");
        }
        for (String route : collectors.keySet()) {
            MysqlVectorLocalGarbageCollector collector = collectors.get(route);
            DataSource foreground = publisher.shardSources().get(route);
            if (!route.equals(collector.shardId())
                    || collector.stripeCount() != publisher.stripeCount()
                    || foreground == collector.dataSource()
                    || !MysqlVectorDatabaseIdentity.read(foreground)
                            .equals(MysqlVectorDatabaseIdentity.read(collector.dataSource()))) {
                throw new IllegalArgumentException("vector GC background shard authority differs: " + route);
            }
        }
        if (!crossCollectors.isEmpty() && !crossCollectors.keySet().equals(collectors.keySet())) {
            throw new IllegalArgumentException("vector cross GC routes differ from local GC routes");
        }
        for (String route : crossCollectors.keySet()) {
            MysqlVectorCrossGarbageCollector collector = crossCollectors.get(route);
            DataSource foreground = publisher.shardSources().get(route);
            if (!route.equals(collector.shardId())
                    || collector.stripeCount() != publisher.stripeCount()
                    || foreground == collector.dataSource()
                    || !MysqlVectorDatabaseIdentity.read(foreground)
                            .equals(MysqlVectorDatabaseIdentity.read(collector.dataSource()))) {
                throw new IllegalArgumentException("vector cross GC background shard authority differs: " + route);
            }
        }
    }

    /**
     * The cycle collection can only be started if the official publisher already has an lease service.
     *
     * @throws MvccException if the operation cannot complete
     */
    void start() {
        if (closed.get() || !publisher.isServing()) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "vector local GC requires an active publisher", true, null);
        }
        if (started.compareAndSet(false, true)) {
            worker.scheduleWithFixedDelay(this::runSafely, 0,
                    intervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A single round of a credible cut-off advancees the lower security boundary and is then removed by a fixed route by block bounded.
     *
     * @return the operation result
     */
    int runOnce() {
        requireMatchingAuthorities();
        MysqlVectorCutLeaderRuntime.PublishedView view = publisher.currentView();
        protection.advanceSafeFloor(view.snapshot());
        boolean scanLocal = rounds.getAndIncrement() % localEveryRounds == 0;
        try (var shards = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Integer>> tasks = new ArrayList<>(collectors.size());
            for (String route : collectors.keySet().stream().sorted().toList()) {
                tasks.add(CompletableFuture.supplyAsync(() -> collectShard(route, scanLocal), shards));
            }
            int reclaimed = 0;
            RuntimeException firstFailure = null;
            for (CompletableFuture<Integer> task : tasks) {
                try {
                    reclaimed += task.join();
                } catch (final CompletionException ex) {
                    if (firstFailure == null) {
                        firstFailure = ex.getCause() instanceof RuntimeException runtime
                                ? runtime
                                : new IllegalStateException("vector shard GC failed", ex.getCause());
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
            return reclaimed;
        }
    }

    /**
     * Each piece of independent connection pool parallel work; the next physical removal is stopped after any failure has ended the current round.
     *
     * @param route route
     * @param scanLocal scan local
     * @return the operation result
     */
    private int collectShard(final String route, final boolean scanLocal) {
        // When the publisher fails to connect, even if the lower durable border is secure, the new transaction will not be opened.
        publisher.currentView();
        int reclaimed = 0;
        if (scanLocal) {
            long localStarted = System.nanoTime();
            int local = collectors.get(route).collect(batchLimit);
            localCollectNanos.add(System.nanoTime() - localStarted);
            reclaimed += local;
            totalReclaimed.add(local);
        }
        if (!crossCollectors.isEmpty()) {
            publisher.currentView();
            long crossStarted = System.nanoTime();
            int cross = crossCollectors.get(route).collect(crossBatchLimit);
            crossCollectNanos.add(System.nanoTime() - crossStarted);
            reclaimed += cross;
            totalReclaimed.add(cross);
            crossReclaimed.add(cross);
        }
        return reclaimed;
    }

    /**
     * Any background anomaly ceases the subsequent physical removal and must be checked and re-established.
     */
    private void runSafely() {
        if (closed.get()) {
            return;
        }
        try {
            int reclaimed = runOnce();
            stats.recordSuccess(reclaimed);
        } catch (final RuntimeException ex) {
            stats.recordFailure();
            lastFailure.compareAndSet(null, ex);
            worker.shutdown();
        }
    }

    /**
     * Returns complete successful rotations and number of failures, without linear survival extrapolation GC normal.
     *
     * @return the operation result
     */
    BackgroundTaskStats stats() {
        return stats;
    }

    /**
     * Returns the number of local and cross-shard version confirmed deleted in this scheduler 's life cycle.
     *
     * @return the operation result
     */
    long totalReclaimed() {
        return totalReclaimed.sum();
    }

    /**
     * Separately exposes the confirmed quantity of cross-shard large payload version.
     *
     * @return the operation result
     */
    long crossReclaimed() {
        return crossReclaimed.sum();
    }

    /**
     * Returns the actual implementation time accumulated on two pieces of local GC.
     *
     * @return the operation result
     */
    long localCollectNanos() {
        return localCollectNanos.sum();
    }

    /**
     * Returns the actual implementation time accumulated on two pieces of cross-shard GC.
     *
     * @return the operation result
     */
    long crossCollectNanos() {
        return crossCollectNanos.sum();
    }

    /**
     * Failed to return the first background, saves the original abnormal chain of the publisher's failure or database error.
     *
     * @return the operation result
     */
    RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /**
     * Stops new rounds and waits for the bounded collection exit that has already started.
     *
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        worker.shutdown();
        try {
            if (!worker.awaitTermination(15, TimeUnit.SECONDS)) {
                worker.shutdownNow();
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "vector local GC did not stop within timeout", true, null);
            }
        } catch (final InterruptedException ex) {
            worker.shutdownNow();
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "interrupted while closing vector local GC", true, null, ex);
        }
    }
}

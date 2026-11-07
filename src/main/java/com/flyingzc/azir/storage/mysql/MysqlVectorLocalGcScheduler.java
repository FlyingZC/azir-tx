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

/** 持租约发布者驱动安全向量推进及本地、跨片旧版本有界回收的后台运行时。 */
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

    /** 后台分片连接池由调用方独立提供，每轮只处理有界行数。 */
    MysqlVectorLocalGcScheduler(MysqlVectorCutLeaderRuntime publisher,
                                MysqlVectorSnapshotLeaseStore protection,
                                Map<String, MysqlVectorLocalGarbageCollector> collectors,
                                int batchLimit, long intervalMillis) {
        this(publisher, protection, collectors, Map.of(), batchLimit, 0, intervalMillis);
    }

    /** 可选跨片回收在同一可信安全向量推进后运行，并拥有独立候选预算。 */
    MysqlVectorLocalGcScheduler(MysqlVectorCutLeaderRuntime publisher,
                                MysqlVectorSnapshotLeaseStore protection,
                                Map<String, MysqlVectorLocalGarbageCollector> collectors,
                                Map<String, MysqlVectorCrossGarbageCollector> crossCollectors,
                                int batchLimit, int crossBatchLimit, long intervalMillis) {
        this(publisher, protection, collectors, crossCollectors,
                batchLimit, crossBatchLimit, 1, intervalMillis);
    }

    /** 跨片主导负载可降低无收益的本地扫描频率，安全下界仍每轮推进。 */
    MysqlVectorLocalGcScheduler(MysqlVectorCutLeaderRuntime publisher,
                                MysqlVectorSnapshotLeaseStore protection,
                                Map<String, MysqlVectorLocalGarbageCollector> collectors,
                                Map<String, MysqlVectorCrossGarbageCollector> crossCollectors,
                                int batchLimit, int crossBatchLimit,
                                int localEveryRounds, long intervalMillis) {
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
        this.worker = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("azir-vector-local-gc").unstarted(task));
    }

    /** 启动前按真实 MySQL 库身份核对控制、分片、布局及独立后台连接预算。 */
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

    /** 只有正式发布者已经持租约服务时才能启动周期回收。 */
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

    /** 单轮先采可信切面推进安全下界，再按固定路由逐片有界删除。 */
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
                } catch (CompletionException ex) {
                    if (firstFailure == null) {
                        firstFailure = ex.getCause() instanceof RuntimeException runtime
                                ? runtime : new IllegalStateException("vector shard GC failed", ex.getCause());
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
            return reclaimed;
        }
    }

    /** 每片独立连接池并行工作；任一失败结束本轮后停止下轮物理删除。 */
    private int collectShard(String route, boolean scanLocal) {
        // 发布者失联时即使持久下界仍安全，也不继续开启新的删除事务。
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

    /** 任一后台异常都停止后续物理删除，运维须检查并重建该调度器。 */
    private void runSafely() {
        if (closed.get()) {
            return;
        }
        try {
            int reclaimed = runOnce();
            stats.recordSuccess(reclaimed);
        } catch (RuntimeException ex) {
            stats.recordFailure();
            lastFailure.compareAndSet(null, ex);
            worker.shutdown();
        }
    }

    /** 返回完整成功轮次及失败次数，不用线程存活推断 GC 正常。 */
    BackgroundTaskStats stats() {
        return stats;
    }

    /** 返回本调度器生命周期内已确认删除的本地及跨片版本数量。 */
    long totalReclaimed() {
        return totalReclaimed.sum();
    }

    /** 单独暴露已确认退役的跨片大载荷版本数量。 */
    long crossReclaimed() {
        return crossReclaimed.sum();
    }

    /** 返回本地 GC 在两片上累积的实际执行时间。 */
    long localCollectNanos() {
        return localCollectNanos.sum();
    }

    /** 返回跨片 GC 在两片上累积的实际执行时间。 */
    long crossCollectNanos() {
        return crossCollectNanos.sum();
    }

    /** 返回第一条后台失败，保留发布者失联或数据库错误的原始异常链。 */
    RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /** 停止新轮次并等待已经开始的有界回收退出。 */
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
        } catch (InterruptedException ex) {
            worker.shutdownNow();
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "interrupted while closing vector local GC", true, null, ex);
        }
    }
}

package com.flyingzc.azir.wal.ha;

import java.io.IOException;
import java.util.concurrent.*;

/** 周期任务只观察后台镜像与清理结果，不在调度线程等待完整数据库导出。 */
final class RatisSnapshotController implements AutoCloseable {
    private final RatisWalNode node; private final RatisSnapshotOptions options; private final ScheduledExecutorService worker;
    private final ExecutorService verification;
    private volatile CompletableFuture<Long> image, purge; private volatile long rounds, snapshots, purges, bytes, firstIndex = -1, lastSnapshot = -1, lastCompleted = System.nanoTime();
    private long purgeStart, requestedBoundary = -1, completedBoundary = -1;
    private volatile String state = "STARTING", error;
    /** 一节点一条观察线程，文件扫描和退休工作量另有固定预算。 */
    RatisSnapshotController(RatisWalNode node, RatisSnapshotOptions options) {
        this.node = node; this.options = options;
        worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-snapshot-lifecycle").factory());
        // 同一观察轮最多提交维护和镜像请求两项；工作线程尚未取走首项时也必须容纳完整一轮。
        verification = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2),
                Thread.ofPlatform().daemon().name("wal-snapshot-verification").factory(), new ThreadPoolExecutor.AbortPolicy());
    }
    /** 首次启用与重启使用同一有限调度。 */
    void start() { worker.scheduleWithFixedDelay(this::round, options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS); }
    /** 返回当前策略以便装配幂等核对。 */
    RatisSnapshotOptions options() { return options; }
    /** 仅读取缓存状态，不让监控执行目录扫描或数据库查询。 */
    RatisWalNode.SnapshotStats stats() { return new RatisWalNode.SnapshotStats(true, state, bytes, firstIndex, lastSnapshot, rounds, snapshots, purges, image != null && !image.isDone(), node.snapshotLogPressure(), error); }
    /** 超水位时背压新业务，镜像失败保持日志不动，下一轮继续重试。 */
    private void round() {
        try {
            if (!node.snapshotMaintenanceReady()) { state = "WAITING"; return; }
            rounds++;
            try { bytes = node.measureRaftLogBytes(); } catch (IOException unavailable) { node.snapshotLogPressure(true); throw unavailable; }
            firstIndex = node.firstLogIndexForTest(); node.snapshotLogPressure(bytes >= options.logHighWaterBytes());
            if (purge != null && purge.isDone()) { purge.get(); purge = null; completedBoundary = Math.max(completedBoundary, requestedBoundary); firstIndex = node.firstLogIndexForTest(); if (requestedBoundary >= 0 && firstIndex > purgeStart) { purges++; } }
            if (image != null && image.isDone()) { long created = image.get(); image = null; if (created >= 0) { lastSnapshot = created; snapshots++; lastCompleted = System.nanoTime(); } }
            if (image == null && purge == null) {
                long existing = node.snapshotIndex(), applied = node.appliedRaftIndex(); lastSnapshot = Math.max(lastSnapshot, existing);
                long tail = node.snapshotLogPressure() ? 0 : options.preserveEntries();
                // 高水位下仍保留所有未覆盖记录，已覆盖尾部只是重放优化，不能让背压无法解除。
                boolean reclaim = existing >= 0 && existing - tail >= firstIndex && existing - tail > completedBoundary;
                purgeStart = firstIndex; requestedBoundary = reclaim ? existing - tail : -1;
                // 目录维护、完整性核对和 force 都不占水位观察线程；已成功边界不重复同步或请求回收。
                purge = CompletableFuture.supplyAsync(() -> {
                    try { node.maintainSnapshotFiles(); return reclaim ? node.purgeCoveredSnapshot(existing, tail) : CompletableFuture.completedFuture(-1L); }
                    catch (IOException failure) { throw new CompletionException(failure); }
                }, verification).thenCompose(java.util.function.Function.identity());
                boolean due = applied > existing && (applied - existing >= options.entryGap() || System.nanoTime() - lastCompleted >= TimeUnit.MILLISECONDS.toNanos(options.maxIntervalMillis()) || node.snapshotLogPressure());
                if (due) {
                    // 请求镜像也可能等待维护锁，交给同一个有界后台队列，观察线程只保留结果句柄。
                    image = CompletableFuture.supplyAsync(() -> {
                        try { return node.requestSnapshotImage(); }
                        catch (IOException failure) { throw new CompletionException(failure); }
                    }, verification).thenCompose(java.util.function.Function.identity());
                    state = "EXPORTING";
                }
                else { state = reclaim ? "PURGING" : "MAINTAINING"; }
            } else { state = image != null ? "EXPORTING" : requestedBoundary >= 0 ? "PURGING" : "MAINTAINING"; }
            error = null;
        } catch (Exception failure) { if (image != null && image.isDone()) { image = null; } if (purge != null && purge.isDone()) { purge = null; } error = failure.getClass().getSimpleName(); state = "RETRYING"; }
    }
    /** 后台镜像由状态机拥有，停止观察不取消其共享 I/O，关闭状态机时再排空。 */
    @Override public void close() throws IOException {
        worker.shutdown();
        try {
            if (!worker.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("snapshot lifecycle did not drain"); }
            verification.shutdown();
            if (!verification.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("snapshot verification did not drain"); }
            var pending = purge; if (pending != null) { try { pending.get(30, TimeUnit.SECONDS); } catch (ExecutionException finishedWithError) { /* 回收已终止，完整快照仍保留。 */ } }
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("snapshot lifecycle close interrupted", interrupted); }
        catch (TimeoutException running) { throw new IOException("snapshot purge did not drain", running); }
    }
}

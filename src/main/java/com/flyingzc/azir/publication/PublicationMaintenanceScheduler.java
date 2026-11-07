package com.flyingzc.azir.publication;

import com.flyingzc.azir.common.BackgroundTaskStats;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** 周期驱动新账本的恢复和 GC，两个任务分别使用独立守护线程。 */
public final class PublicationMaintenanceScheduler implements AutoCloseable {
    private final PublicationRecoveryRunner recoveryRunner;
    private final PublicationGcManager gcManager;
    private final int recoveryBatchSize;
    private final int gcBatchSize;
    private final long recoveryIntervalMillis;
    private final long gcIntervalMillis;
    private final ScheduledExecutorService recoveryExecutor;
    private final ScheduledExecutorService gcExecutor;
    private final BackgroundTaskStats recoveryStats = new BackgroundTaskStats();
    private final BackgroundTaskStats gcStats = new BackgroundTaskStats();
    private final AtomicReference<RuntimeException> lastRecoveryFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> lastGcFailure = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 建立有界周期调度；调用者应为后台任务提供独立连接池预算。 */
    public PublicationMaintenanceScheduler(PublicationRecoveryRunner recoveryRunner,
                                           PublicationGcManager gcManager,
                                           int recoveryBatchSize, int gcBatchSize,
                                           long recoveryIntervalMillis, long gcIntervalMillis) {
        if (recoveryBatchSize < 1 || recoveryBatchSize > 4096 || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || gcIntervalMillis < 1) {
            throw new IllegalArgumentException("invalid publication maintenance limits");
        }
        this.recoveryRunner = Objects.requireNonNull(recoveryRunner, "recoveryRunner");
        this.gcManager = Objects.requireNonNull(gcManager, "gcManager");
        this.recoveryBatchSize = recoveryBatchSize;
        this.gcBatchSize = gcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.recoveryExecutor = Executors.newSingleThreadScheduledExecutor(daemonFactory("mvcc-ledger-recovery"));
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor(daemonFactory("mvcc-ledger-gc"));
    }

    /** 启动一次性周期注册，重复启动不会叠加扫描任务。 */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("publication maintenance scheduler is closed");
        }
        if (started.compareAndSet(false, true)) {
            recoveryExecutor.scheduleWithFixedDelay(this::runRecoverySafely,
                    0, recoveryIntervalMillis, TimeUnit.MILLISECONDS);
            gcExecutor.scheduleWithFixedDelay(this::runGcSafely,
                    0, gcIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    /** 返回周期恢复任务的执行和异常统计。 */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /** 返回周期 GC 任务的执行和异常统计。 */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /** 返回最近一次恢复轮次的顶层失败。 */
    public RuntimeException lastRecoveryFailure() {
        return lastRecoveryFailure.get();
    }

    /** 返回最近一次 GC 轮次的顶层失败。 */
    public RuntimeException lastGcFailure() {
        return lastGcFailure.get();
    }

    /** 停止未来轮次并等待当前数据库任务结束，超时后中断后台线程。 */
    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recoveryExecutor.shutdown();
        gcExecutor.shutdown();
        awaitStop(recoveryExecutor);
        awaitStop(gcExecutor);
    }

    /** 单轮恢复失败保留下一轮机会，并暴露周期级失败统计。 */
    private void runRecoverySafely() {
        try {
            recoveryStats.recordSuccess(recoveryRunner.recoverOnce(recoveryBatchSize));
        } catch (RuntimeException ex) {
            recoveryStats.recordFailure();
            lastRecoveryFailure.set(ex);
        }
    }

    /** 单轮 GC 失败保留持久游标与安全点，并暴露周期级失败统计。 */
    private void runGcSafely() {
        try {
            gcStats.recordSuccess(gcManager.gcOnce(gcBatchSize));
        } catch (RuntimeException ex) {
            gcStats.recordFailure();
            lastGcFailure.set(ex);
        }
    }

    /** 等待一条任务线程退出，合作式预算之外的单次 JDBC 调用仍依赖驱动超时。 */
    private void awaitStop(ScheduledExecutorService executor) {
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** 为每类周期任务创建独立的守护线程。 */
    private static ThreadFactory daemonFactory(String name) {
        return runnable -> {
            Thread thread = Thread.ofPlatform().daemon().name(name).unstarted(runnable);
            return thread;
        };
    }
}

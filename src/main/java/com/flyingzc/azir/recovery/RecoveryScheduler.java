package com.flyingzc.azir.recovery;

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.gc.GcManager;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 后台恢复与 GC 调度器。
 */
public final class RecoveryScheduler implements AutoCloseable {
    private final BackgroundRecoveryRunner recoveryRunner;
    private final GcManager gcManager;
    private final int recoveryBatchSize;
    private final int gcBatchSize;
    private final long recoveryIntervalMillis;
    private final long gcIntervalMillis;
    private final ScheduledExecutorService executorService;
    private final BackgroundTaskStats recoveryStats = new BackgroundTaskStats();
    private final BackgroundTaskStats gcStats = new BackgroundTaskStats();

    /**
     * 构造 RecoveryScheduler 实例。
     */
    public RecoveryScheduler(BackgroundRecoveryRunner recoveryRunner,
                             GcManager gcManager,
                             int recoveryBatchSize,
                             int gcBatchSize,
                             long recoveryIntervalMillis,
                             long gcIntervalMillis) {
        this.recoveryRunner = Objects.requireNonNull(recoveryRunner);
        this.gcManager = Objects.requireNonNull(gcManager);
        this.recoveryBatchSize = recoveryBatchSize;
        this.gcBatchSize = gcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.executorService = Executors.newScheduledThreadPool(2, daemonFactory());
    }

    /**
     * 启动后台调度任务。
     */
    public void start() {
        executorService.scheduleWithFixedDelay(this::runRecoverySafely, 0L, recoveryIntervalMillis, TimeUnit.MILLISECONDS);
        executorService.scheduleWithFixedDelay(this::runGcSafely, 0L, gcIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 返回恢复任务统计信息。
     */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * 返回 GC 任务统计信息。
     */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /**
     * 关闭资源或后台执行器。
     */
    @Override
    public void close() {
        executorService.shutdownNow();
    }

    /**
     * 安全执行一次恢复任务。
     */
    private void runRecoverySafely() {
        try {
            int recovered = recoveryRunner.recoverOnce(recoveryBatchSize);
            recoveryStats.recordSuccess(recovered);
        } catch (RuntimeException ex) {
            recoveryStats.recordFailure();
        }
    }

    /**
     * 安全执行一次 GC 任务。
     */
    private void runGcSafely() {
        try {
            int reclaimed = gcManager.gcOnce(gcBatchSize);
            gcStats.recordSuccess(reclaimed);
        } catch (RuntimeException ex) {
            gcStats.recordFailure();
        }
    }

    /**
     * 创建守护线程工厂。
     */
    private static ThreadFactory daemonFactory() {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("mvcc-background");
            thread.setDaemon(true);
            return thread;
        };
    }
}

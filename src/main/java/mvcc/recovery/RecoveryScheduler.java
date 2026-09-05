package mvcc.recovery;

import mvcc.common.BackgroundTaskStats;
import mvcc.gc.GcManager;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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

    public void start() {
        executorService.scheduleWithFixedDelay(this::runRecoverySafely, 0L, recoveryIntervalMillis, TimeUnit.MILLISECONDS);
        executorService.scheduleWithFixedDelay(this::runGcSafely, 0L, gcIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    private void runRecoverySafely() {
        try {
            int recovered = recoveryRunner.recoverOnce(recoveryBatchSize);
            recoveryStats.recordSuccess(recovered);
        } catch (RuntimeException ex) {
            recoveryStats.recordFailure();
        }
    }

    private void runGcSafely() {
        try {
            int reclaimed = gcManager.gcOnce(gcBatchSize);
            gcStats.recordSuccess(reclaimed);
        } catch (RuntimeException ex) {
            gcStats.recordFailure();
        }
    }

    private static ThreadFactory daemonFactory() {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("mvcc-background");
            thread.setDaemon(true);
            return thread;
        };
    }
}

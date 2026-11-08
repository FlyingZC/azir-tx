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

package com.flyingzc.azir.recovery;

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.gc.GcManager;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Background recovery with GC Scheduler.
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
     * Construct a RecoveryScheduler instance.
     *
     * @param recoveryRunner recovery runner
     * @param gcManager gc manager
     * @param recoveryBatchSize recovery batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param gcIntervalMillis gc interval millis
     */
    public RecoveryScheduler(final BackgroundRecoveryRunner recoveryRunner,
                             final GcManager gcManager,
                             final int recoveryBatchSize,
                             final int gcBatchSize,
                             final long recoveryIntervalMillis,
                             final long gcIntervalMillis) {
        this.recoveryRunner = Objects.requireNonNull(recoveryRunner);
        this.gcManager = Objects.requireNonNull(gcManager);
        this.recoveryBatchSize = recoveryBatchSize;
        this.gcBatchSize = gcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.executorService = Executors.newScheduledThreadPool(2, daemonFactory());
    }

    /**
     * Starts the background schedule task.
     */
    public void start() {
        executorService.scheduleWithFixedDelay(this::runRecoverySafely, 0L, recoveryIntervalMillis, TimeUnit.MILLISECONDS);
        executorService.scheduleWithFixedDelay(this::runGcSafely, 0L, gcIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Synchronising "%s".
     *
     * @return the operation result
     */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Synchronising "%s".
     *
     * @return the operation result
     */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /**
     * Close resources and background executors.
     */
    @Override
    public void close() {
        executorService.shutdownNow();
    }

    /**
     * Safely perform an recovery mission.
     */
    private void runRecoverySafely() {
        try {
            int recovered = recoveryRunner.recoverOnce(recoveryBatchSize);
            recoveryStats.recordSuccess(recovered);
        } catch (final RuntimeException ex) {
            recoveryStats.recordFailure();
        }
    }

    /**
     * A GC mission is carried out safely.
     */
    private void runGcSafely() {
        try {
            int reclaimed = gcManager.gcOnce(gcBatchSize);
            gcStats.recordSuccess(reclaimed);
        } catch (final RuntimeException ex) {
            gcStats.recordFailure();
        }
    }

    /**
     * Create a daemon factory.
     *
     * @return the operation result
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

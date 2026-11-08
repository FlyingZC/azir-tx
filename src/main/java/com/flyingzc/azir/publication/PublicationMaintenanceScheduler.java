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

package com.flyingzc.azir.publication;

import com.flyingzc.azir.common.BackgroundTaskStats;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Periodically drive recovery and GCs for new books, with independent daemons used for each of the two tasks.
 */
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

    /**
     * Create bounded cycle schedule; callers should provide an independent background task connection pool budget.
     *
     * @param recoveryRunner recovery runner
     * @param gcManager gc manager
     * @param recoveryBatchSize recovery batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param gcIntervalMillis gc interval millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationMaintenanceScheduler(final PublicationRecoveryRunner recoveryRunner,
                                           final PublicationGcManager gcManager,
                                           final int recoveryBatchSize, final int gcBatchSize,
                                           final long recoveryIntervalMillis, final long gcIntervalMillis) {
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

    /**
     * Starts a one-time cycle of registration, and repeated start does not superimpose scanning tasks.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
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

    /**
     * Returns the recovery -cycle performance of tasks and unusual statistics.
     *
     * @return the operation result
     */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns GC tasks performed and unusual statistics.
     *
     * @return the operation result
     */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /**
     * Could not close temporary folder: %s.
     *
     * @return the operation result
     */
    public RuntimeException lastRecoveryFailure() {
        return lastRecoveryFailure.get();
    }

    /**
     * Returns the top layer of the last GC rotation failed.
     *
     * @return the operation result
     */
    public RuntimeException lastGcFailure() {
        return lastGcFailure.get();
    }

    /**
     * Stops the next rotation and waits for the end of the current database task, aborting the background thread after timeout.
     */
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

    /**
     * Single-wheel recovery failure preserves the next round of opportunity and exposes periodic failure statistics.
     */
    private void runRecoverySafely() {
        try {
            recoveryStats.recordSuccess(recoveryRunner.recoverOnce(recoveryBatchSize));
        } catch (final RuntimeException ex) {
            recoveryStats.recordFailure();
            lastRecoveryFailure.set(ex);
        }
    }

    /**
     * Single-wheel GC failed to keep durable cursor and safe point and to expose periodic failure statistics.
     */
    private void runGcSafely() {
        try {
            gcStats.recordSuccess(gcManager.gcOnce(gcBatchSize));
        } catch (final RuntimeException ex) {
            gcStats.recordFailure();
            lastGcFailure.set(ex);
        }
    }

    /**
     * Waiting for a task thread to exit, single JDBC calls outside the cooperative budget still depend on driver timeout.
     *
     * @param executor executor
     */
    private void awaitStop(final ScheduledExecutorService executor) {
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Creates a separate daemon for each type of periodic task.
     *
     * @param name name
     * @return the operation result
     */
    private static ThreadFactory daemonFactory(final String name) {
        return runnable -> {
            Thread thread = Thread.ofPlatform().daemon().name(name).unstarted(runnable);
            return thread;
        };
    }
}

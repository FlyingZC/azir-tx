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
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Execute the recovery and consecutive watermark in the independent background connection budget cycle.
 */
public final class SegmentPublicationMaintenanceScheduler implements AutoCloseable {

    private final SegmentPublicationRecoveryRunner recoveryRunner;

    private final MysqlSegmentPublicationLedger ledger;

    private final SegmentPublicationGcManager gcManager;

    private final int recoveryBatchSize;

    private final int publicationBatchSize;

    private final int gcBatchSize;

    private final long recoveryIntervalMillis;

    private final long publicationIntervalMillis;

    private final long gcIntervalMillis;

    private final boolean eventDrivenPublication;

    private final ScheduledExecutorService recoveryExecutor;

    private final ScheduledExecutorService publicationExecutor;

    private final ScheduledExecutorService gcExecutor;

    private final BackgroundTaskStats recoveryStats = new BackgroundTaskStats();

    private final BackgroundTaskStats publicationStats = new BackgroundTaskStats();

    private final BackgroundTaskStats gcStats = new BackgroundTaskStats();

    private final AtomicLong lastObservedPublished;

    private final AtomicReference<RuntimeException> lastRecoveryFailure = new AtomicReference<>();

    private final AtomicReference<RuntimeException> lastPublicationFailure = new AtomicReference<>();

    private final AtomicReference<RuntimeException> lastGcFailure = new AtomicReference<>();

    private final AtomicBoolean started = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean publicationRequestQueued = new AtomicBoolean();

    private final AtomicLong publicationRequestVersion = new AtomicLong();

    /**
     * REQUESTS bounded batch and regular, with the background data source configured independently by the caller.
     *
     * @param recoveryRunner recovery runner
     * @param ledger ledger
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     */
    public SegmentPublicationMaintenanceScheduler(final SegmentPublicationRecoveryRunner recoveryRunner,
                                                  final MysqlSegmentPublicationLedger ledger,
                                                  final int recoveryBatchSize, final int publicationBatchSize,
                                                  final long recoveryIntervalMillis, final long publicationIntervalMillis) {
        this(recoveryRunner, ledger, null, recoveryBatchSize, publicationBatchSize, 1,
                recoveryIntervalMillis, publicationIntervalMillis, 1);
    }

    /**
     * Enable shard and Centre safe point cycle GCs when fully running.
     *
     * @param recoveryRunner recovery runner
     * @param ledger ledger
     * @param gcManager gc manager
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     */
    public SegmentPublicationMaintenanceScheduler(final SegmentPublicationRecoveryRunner recoveryRunner,
                                                  final MysqlSegmentPublicationLedger ledger,
                                                  final SegmentPublicationGcManager gcManager,
                                                  final int recoveryBatchSize, final int publicationBatchSize,
                                                  final int gcBatchSize, final long recoveryIntervalMillis,
                                                  final long publicationIntervalMillis, final long gcIntervalMillis) {
        this(recoveryRunner, ledger, gcManager, recoveryBatchSize, publicationBatchSize,
                gcBatchSize, recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis, false);
    }

    /**
     * Optionally, you can use the same release thread to wake up again, and the cycle turns down to process terminal state slots without foreground waiting.
     *
     * @param recoveryRunner recovery runner
     * @param ledger ledger
     * @param gcManager gc manager
     * @param recoveryBatchSize recovery batch size
     * @param publicationBatchSize publication batch size
     * @param gcBatchSize gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param publicationIntervalMillis publication interval millis
     * @param gcIntervalMillis gc interval millis
     * @param eventDrivenPublication event driven publication
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public SegmentPublicationMaintenanceScheduler(final SegmentPublicationRecoveryRunner recoveryRunner,
                                                  final MysqlSegmentPublicationLedger ledger,
                                                  final SegmentPublicationGcManager gcManager,
                                                  final int recoveryBatchSize, final int publicationBatchSize,
                                                  final int gcBatchSize, final long recoveryIntervalMillis,
                                                  final long publicationIntervalMillis, final long gcIntervalMillis,
                                                  final boolean eventDrivenPublication) {
        if (recoveryBatchSize < 1 || recoveryBatchSize > 4096
                || publicationBatchSize < 1 || publicationBatchSize > 4096
                || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || publicationIntervalMillis < 1 || gcIntervalMillis < 1) {
            throw new IllegalArgumentException("invalid segmented maintenance limits");
        }
        this.recoveryRunner = Objects.requireNonNull(recoveryRunner, "recoveryRunner");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.gcManager = gcManager;
        this.lastObservedPublished = new AtomicLong(ledger.currentPublished());
        this.recoveryBatchSize = recoveryBatchSize;
        this.publicationBatchSize = publicationBatchSize;
        this.gcBatchSize = gcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.publicationIntervalMillis = publicationIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.eventDrivenPublication = eventDrivenPublication;
        this.recoveryExecutor = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-segment-recovery").unstarted(task));
        this.publicationExecutor = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-segment-publication").unstarted(task));
        this.gcExecutor = gcManager == null ? null : Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-segment-gc").unstarted(task));
    }

    /**
     * Starts a one-time schedule and repeats the call without overlapping tasks.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("segment maintenance scheduler is closed");
        }
        if (started.compareAndSet(false, true)) {
            recoveryExecutor.scheduleWithFixedDelay(this::recoverSafely, 0,
                    recoveryIntervalMillis, TimeUnit.MILLISECONDS);
            publicationExecutor.scheduleWithFixedDelay(this::publishSafely, 0,
                    publicationIntervalMillis, TimeUnit.MILLISECONDS);
            if (gcExecutor != null) {
                gcExecutor.scheduleWithFixedDelay(this::gcSafely, 0,
                        gcIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Returns bounded recovery round success and failure statistics.
     *
     * @return the operation result
     */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns consecutive watermark rounds of success and failure statistics.
     *
     * @return the operation result
     */
    public BackgroundTaskStats publicationStats() {
        return publicationStats;
    }

    /**
     * Only returns the latest watermark release of database commit completed for foreground awaiting confirmation.
     *
     * @return the operation result
     */
    public long publishedWatermark() {
        return lastObservedPublished.get();
    }

    /**
     * Commit foreground has delivered only one combination awakening, not calling database release on the foreground thread.
     */
    public void requestPublication() {
        if (!eventDrivenPublication || !started.get() || closed.get()) {
            return;
        }
        publicationRequestVersion.incrementAndGet();
        scheduleRequestedPublication();
    }

    /**
     * A maximum of one to run and a new decision during execution will be added at the end.
     */
    private void scheduleRequestedPublication() {
        if (!publicationRequestQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            publicationExecutor.schedule(this::publishRequestedSafely, 1, TimeUnit.MILLISECONDS);
        } catch (final RejectedExecutionException ex) {
            publicationRequestQueued.set(false);
        }
    }

    /**
     * Keeps the cycle bottom while avoiding the loss of a new commit notification at the end of the previous scan.
     */
    private void publishRequestedSafely() {
        long observed = publicationRequestVersion.get();
        try {
            publishSafely();
        } finally {
            publicationRequestQueued.set(false);
            if (publicationRequestVersion.get() != observed && !closed.get()) {
                scheduleRequestedPublication();
            }
        }
    }

    /**
     * Returns the completion and failure statistics of the GC cycle.
     *
     * @return the operation result
     */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /**
     * Returns the latest recovery top level anomaly for diagnosis.
     *
     * @return the operation result
     */
    public RuntimeException lastRecoveryFailure() {
        return lastRecoveryFailure.get();
    }

    /**
     * Returns the most recent anomaly published for diagnosis.
     *
     * @return the operation result
     */
    public RuntimeException lastPublicationFailure() {
        return lastPublicationFailure.get();
    }

    /**
     * Returns the last GC anomaly for diagnosis.
     *
     * @return the operation result
     */
    public RuntimeException lastGcFailure() {
        return lastGcFailure.get();
    }

    /**
     * Stops the new rotation and waits for the end of the current database task.
     */
    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recoveryExecutor.shutdown();
        publicationExecutor.shutdown();
        if (gcExecutor != null) {
            gcExecutor.shutdown();
        }
        awaitStop(recoveryExecutor);
        awaitStop(publicationExecutor);
        if (gcExecutor != null) {
            awaitStop(gcExecutor);
        }
    }

    /**
     * Single-wheel recovery abnormally retains the next cycle opportunity and discloses the reasons.
     */
    private void recoverSafely() {
        try {
            recoveryStats.recordSuccess(recoveryRunner.recoverOnce(recoveryBatchSize));
        } catch (final RuntimeException ex) {
            lastRecoveryFailure.set(ex);
            recoveryStats.recordFailure();
        }
    }

    /**
     * The new slot terminal state continues to attempt to advance successive prefixes even after the foreground exits.
     */
    private void publishSafely() {
        try {
            long published = ledger.advancePublished(publicationBatchSize);
            publicationStats.recordSuccess(Math.max(0, published - lastObservedPublished.getAndSet(published)));
        } catch (final RuntimeException ex) {
            lastPublicationFailure.set(ex);
            publicationStats.recordFailure();
        }
    }

    /**
     * The GC round failed to preserve the original phase and target safe point, leaving it for the next cycle retry.
     */
    private void gcSafely() {
        try {
            gcStats.recordSuccess(gcManager.gcOnce(gcBatchSize));
        } catch (final RuntimeException ex) {
            lastGcFailure.set(ex);
            gcStats.recordFailure();
        }
    }

    /**
     * Bounded Waits for the background round exit, and the database block is still protected by connection pool and driver timeout.
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
}

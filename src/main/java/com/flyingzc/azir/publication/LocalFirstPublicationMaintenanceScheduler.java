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
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlShardSegmentFence;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Schedule local on independent background connection budget first commit, recovery, candidate and side safe point.
 */
public final class LocalFirstPublicationMaintenanceScheduler implements AutoCloseable {

    /**
     * A central slot with a capacity level of up to collection per round and a Finalized record.
     */
    private static final int CENTER_GC_BATCH_SIZE = 4096;

    private final MysqlLocalFirstPublicationLedger ledger;

    private final Map<String, MysqlShardSegmentFence> fences;

    private final Map<String, StorageAdapter> adapters;

    private final LocalFirstPublicationRecoveryManager recovery;

    private final LocalFirstIntentRecoveryManager intentRecovery;

    private final int segmentBatchSize;

    private final int candidateSegmentBatchSize;

    private final int shardGcBatchSize;

    private final long recoveryIntervalMillis;

    private final long gcIntervalMillis;

    private final long retentionMillis;

    private final ScheduledExecutorService recoveryExecutor;

    private final ScheduledExecutorService gcExecutor;

    private final BackgroundTaskStats recoveryStats = new BackgroundTaskStats();

    private final BackgroundTaskStats gcStats = new BackgroundTaskStats();

    private final AtomicLong sealedSegments = new AtomicLong();

    private final AtomicLong purgedCandidates = new AtomicLong();

    private final AtomicLong retiredFences = new AtomicLong();

    private final AtomicReference<RuntimeException> lastRecoveryFailure = new AtomicReference<>();

    private final AtomicReference<RuntimeException> lastGcFailure = new AtomicReference<>();

    private final AtomicBoolean started = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    private long fenceGcAfterSegmentStart;

    /**
     * Limit the implementation cycle for each round of take-over, full section clean-up, shard GC and background.
     *
     * @param ledger ledger
     * @param fences fences
     * @param adapters adapters
     * @param segmentBatchSize segment batch size
     * @param candidateSegmentBatchSize candidate segment batch size
     * @param shardGcBatchSize shard gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param gcIntervalMillis gc interval millis
     */
    public LocalFirstPublicationMaintenanceScheduler(final MysqlLocalFirstPublicationLedger ledger,
                                                     final Map<String, MysqlShardSegmentFence> fences,
                                                     final Map<String, StorageAdapter> adapters,
                                                     final int segmentBatchSize, final int candidateSegmentBatchSize,
                                                     final int shardGcBatchSize, final long recoveryIntervalMillis,
                                                     final long gcIntervalMillis) {
        this(ledger, fences, adapters, segmentBatchSize, candidateSegmentBatchSize,
                shardGcBatchSize, recoveryIntervalMillis, gcIntervalMillis, 60_000);
    }

    /**
     * Visible retention windows keep central slots and metadata collection compliant with operational audit deadlines.
     *
     * @param ledger ledger
     * @param fences fences
     * @param adapters adapters
     * @param segmentBatchSize segment batch size
     * @param candidateSegmentBatchSize candidate segment batch size
     * @param shardGcBatchSize shard gc batch size
     * @param recoveryIntervalMillis recovery interval millis
     * @param gcIntervalMillis gc interval millis
     * @param retentionMillis retention millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public LocalFirstPublicationMaintenanceScheduler(final MysqlLocalFirstPublicationLedger ledger,
                                                     final Map<String, MysqlShardSegmentFence> fences,
                                                     final Map<String, StorageAdapter> adapters,
                                                     final int segmentBatchSize, final int candidateSegmentBatchSize,
                                                     final int shardGcBatchSize, final long recoveryIntervalMillis,
                                                     final long gcIntervalMillis, final long retentionMillis) {
        if (segmentBatchSize < 1 || segmentBatchSize > 4096
                || candidateSegmentBatchSize < 1 || candidateSegmentBatchSize > 4096
                || shardGcBatchSize < 1 || shardGcBatchSize > 4096
                || recoveryIntervalMillis < 1 || gcIntervalMillis < 1
                || retentionMillis < 0 || retentionMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid local-first maintenance limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.fences = Map.copyOf(fences);
        this.adapters = Map.copyOf(adapters);
        if (this.fences.isEmpty() || !this.fences.keySet().equals(this.adapters.keySet())) {
            throw new IllegalArgumentException("local-first maintenance requires matching shard routes");
        }
        this.fences.forEach((shardId, fence) -> {
            if (!shardId.equals(fence.shardId()) || !shardId.equals(this.adapters.get(shardId).shardId())) {
                throw new IllegalArgumentException("local-first maintenance route differs from shard storage");
            }
        });
        this.recovery = new LocalFirstPublicationRecoveryManager(ledger, this.fences);
        this.intentRecovery = new LocalFirstIntentRecoveryManager(ledger, this.adapters);
        this.segmentBatchSize = segmentBatchSize;
        this.candidateSegmentBatchSize = candidateSegmentBatchSize;
        this.shardGcBatchSize = shardGcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.retentionMillis = retentionMillis;
        this.recoveryExecutor = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-local-first-recovery").unstarted(task));
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-local-first-gc").unstarted(task));
    }

    /**
     * Repetitive startup creates only a set of recovery and GC-cycle tasks.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("local-first maintenance scheduler is closed");
        }
        if (started.compareAndSet(false, true)) {
            recoveryExecutor.scheduleWithFixedDelay(this::recoverSafely, recoveryIntervalMillis,
                    recoveryIntervalMillis, TimeUnit.MILLISECONDS);
            gcExecutor.scheduleWithFixedDelay(this::gcSafely, gcIntervalMillis,
                    gcIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Single-wheel processing of physics fencing, and the release of consecutive prefixes and full paragraph candidates collection.
     *
     * @return the operation result
     */
    public LocalFirstPublicationRecoveryManager.RecoveryResult recoverOnce() {
        var result = recovery.recoverOnce(segmentBatchSize, candidateSegmentBatchSize);
        // Store missing links first, and then fill cross-shard materialization or due rollback by centre terminal state.
        var intentResult = intentRecovery.recoverOnce(candidateSegmentBatchSize);
        sealedSegments.addAndGet(result.sealedSegments());
        purgedCandidates.addAndGet(result.purgedCandidates());
        int failures = result.failures() + intentResult.failures();
        if (failures == 0) {
            recoveryStats.recordSuccess(result.sealedSegments() + result.purgedCandidates()
                    + intentResult.finalized() + intentResult.aborted());
        } else {
            for (int index = 0; index < failures; index++) {
                recoveryStats.recordFailure();
            }
            lastRecoveryFailure.set(intentResult.failures() == 0
                    ? recovery.lastFailure()
                    : intentRecovery.lastFailure());
        }
        return result;
    }

    /**
     * Control safe point is limited first by the lower bounds of section and coordinator, and then additionally by the remaining candidate.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    public int gcOnce() {
        ledger.expireSnapshots(candidateSegmentBatchSize);
        ledger.expireCoordinatorLeases(candidateSegmentBatchSize);
        long controlSafe = ledger.advanceSafePoint();
        int reclaimed = 0;
        for (String shardId : fences.keySet().stream().sorted().toList()) {
            long localSafe = fences.get(shardId).oldestCandidateStartTs()
                    .stream().map(candidate -> Math.min(controlSafe, candidate))
                    .findFirst().orElse(controlSafe);
            reclaimed += adapters.get(shardId).gc(localSafe, shardGcBatchSize);
        }
        long now = ledger.currentDatabaseTimeMillis();
        long cutoff = now < retentionMillis ? 0 : now - retentionMillis;
        reclaimed += ledger.purgeTerminalSlots(controlSafe, cutoff, CENTER_GC_BATCH_SIZE);
        reclaimed += ledger.purgeFinalizedMetadata(controlSafe, cutoff, CENTER_GC_BATCH_SIZE);
        var retirable = ledger.findRetirableFences(fenceGcAfterSegmentStart, candidateSegmentBatchSize);
        fenceGcAfterSegmentStart = retirable.size() < candidateSegmentBatchSize
                ? 0
                : retirable.getLast().lease().firstTs();
        for (var segment : retirable) {
            var proof = ledger.fenceGcProof(segment.lease().firstTs());
            if (proof.isEmpty()) {
                continue;
            }
            MysqlShardSegmentFence physical = fences.get(proof.get().shardId());
            if (physical == null) {
                throw new IllegalStateException("local-first fence GC lost shard route");
            }
            physical.purgeFence(proof.get());
            if (ledger.markFenceRetired(proof.get(), physical)) {
                retiredFences.incrementAndGet();
                reclaimed++;
            }
        }
        reclaimed += ledger.purgeSealedSegments(controlSafe, cutoff, CENTER_GC_BATCH_SIZE);
        gcStats.recordSuccess(reclaimed);
        return reclaimed;
    }

    /**
     * Releases a round of success and failure for transport without relying on foreground memory guesses.
     *
     * @return the operation result
     */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns the GC completed and failed rotations.
     *
     * @return the operation result
     */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /**
     * Returns the number of sections that have been stored by durable.
     *
     * @return the operation result
     */
    public long sealedSegments() {
        return sealedSegments.get();
    }

    /**
     * Returns the number of candidates that have been marked from shard collection at centre.
     *
     * @return the operation result
     */
    public long purgedCandidates() {
        return purgedCandidates.get();
    }

    /**
     * Returns the number of physical fence lines that have been compressed safely with durable decommissioned watermark.
     *
     * @return the operation result
     */
    public long retiredFences() {
        return retiredFences.get();
    }

    /**
     * Returns the last recovery failed.
     *
     * @return the operation result
     */
    public RuntimeException lastRecoveryFailure() {
        return lastRecoveryFailure.get();
    }

    /**
     * Returns the last GC failed.
     *
     * @return the operation result
     */
    public RuntimeException lastGcFailure() {
        return lastGcFailure.get();
    }

    /**
     * The periodic task retains the abnormal evidence and does not allow the dispatcher to swallow the next round of retry opportunities.
     */
    private void recoverSafely() {
        try {
            recoverOnce();
        } catch (final RuntimeException ex) {
            lastRecoveryFailure.set(ex);
            recoveryStats.recordFailure();
        }
    }

    /**
     * A single GC thread does not miss all cycles because recovery database is waiting.
     */
    private void gcSafely() {
        try {
            gcOnce();
        } catch (final RuntimeException ex) {
            lastGcFailure.set(ex);
            gcStats.recordFailure();
        }
    }

    /**
     * Stop the new wheel and roll back within the bounded period to clear the end-end candidate after normal stoppage.
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (int round = 0; round < 64 && System.nanoTime() < deadline; round++) {
            try {
                if (ledger.findRecoverableSegments(0, 1).isEmpty()
                        && ledger.findPurgeableSegments(0, 1).isEmpty()) {
                    break;
                }
                var result = recoverOnce();
                if (result.failures() > 0) {
                    break;
                }
            } catch (final RuntimeException ex) {
                lastRecoveryFailure.set(ex);
                recoveryStats.recordFailure();
                break;
            }
        }
        try {
            if (!ledger.findRecoverableSegments(0, 1).isEmpty()
                    || !ledger.findPurgeableSegments(0, 1).isEmpty()) {
                lastRecoveryFailure.set(new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local-first shutdown left durable recovery work for restart", false, null));
                recoveryStats.recordFailure();
            }
        } catch (final RuntimeException ex) {
            lastRecoveryFailure.set(ex);
            recoveryStats.recordFailure();
        }
        // The latest published prefix is first reflected in safe point, so as to avoid missing the end of the paragraph that has just met the retirement condition.
        gcSafely();
        long gcDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (int round = 0; round < 64 && System.nanoTime() < gcDeadline; round++) {
            try {
                if (ledger.findRetirableFences(0, 1).isEmpty()) {
                    break;
                }
                gcOnce();
            } catch (final RuntimeException ex) {
                lastGcFailure.set(ex);
                gcStats.recordFailure();
                break;
            }
        }
        try {
            if (!ledger.findRetirableFences(0, 1).isEmpty()) {
                lastGcFailure.set(new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local-first shutdown left eligible physical fences for restart", false, null));
                gcStats.recordFailure();
            }
        } catch (final RuntimeException ex) {
            lastGcFailure.set(ex);
            gcStats.recordFailure();
        }
        gcSafely();
    }

    /**
     * Background connection timeout is responsible for data sources and drivers, with a maximum of five seconds to close the cooperation.
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

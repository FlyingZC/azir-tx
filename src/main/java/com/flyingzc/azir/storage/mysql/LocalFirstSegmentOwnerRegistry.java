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
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.SegmentReservation;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * Assign local to a target local first commit slot and cycle maintenance center and shard side owner lease.
 */
final class LocalFirstSegmentOwnerRegistry implements AutoCloseable {

    private final MysqlLocalFirstPublicationLedger control;

    private final MysqlShardSegmentFence physical;

    private final MysqlLocalFirstPublicationLedger backgroundControl;

    private final MysqlShardSegmentFence backgroundPhysical;

    private final LongSupplier snapshotFloor;

    private final String ownerId = UUID.randomUUID().toString();

    private final int segmentSize;

    private final long leaseMillis;

    private final long idleSealNanos;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final ConcurrentHashMap<Long, SegmentState> segments = new ConcurrentHashMap<>();

    private final ScheduledExecutorService refresher;

    private final BackgroundTaskStats refreshStats = new BackgroundTaskStats();

    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicLong reservationSequence = new AtomicLong();

    private volatile SegmentState current;

    private String pendingRequestKey;

    private long pendingSnapshotFloor;

    /**
     * Bind single target shard and bounded lease budget, background only with visible database authority.
     *
     * @param control control
     * @param physical physical
     * @param snapshotFloor snapshot floor
     * @param segmentSize segment size
     * @param leaseMillis lease millis
     * @param refreshMillis refresh millis
     * @param idleSealMillis idle seal millis
     */
    LocalFirstSegmentOwnerRegistry(final MysqlLocalFirstPublicationLedger control,
                                   final MysqlShardSegmentFence physical, final LongSupplier snapshotFloor,
                                   final int segmentSize, final long leaseMillis,
                                   final long refreshMillis, final long idleSealMillis) {
        this(control, control, physical, physical, snapshotFloor,
                segmentSize, leaseMillis, refreshMillis, idleSealMillis);
    }

    /**
     * Former background may use a stand-alone connection pool. Route identity must be consistent and verify called by the same physical authority.
     *
     * @param control control
     * @param backgroundControl background control
     * @param physical physical
     * @param backgroundPhysical background physical
     * @param snapshotFloor snapshot floor
     * @param segmentSize segment size
     * @param leaseMillis lease millis
     * @param refreshMillis refresh millis
     * @param idleSealMillis idle seal millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    LocalFirstSegmentOwnerRegistry(final MysqlLocalFirstPublicationLedger control,
                                   final MysqlLocalFirstPublicationLedger backgroundControl,
                                   final MysqlShardSegmentFence physical,
                                   final MysqlShardSegmentFence backgroundPhysical,
                                   final LongSupplier snapshotFloor, final int segmentSize, final long leaseMillis,
                                   final long refreshMillis, final long idleSealMillis) {
        if (segmentSize < 1 || segmentSize > 4096 || leaseMillis < 3
                || leaseMillis > 86_400_000L || refreshMillis < 1
                || refreshMillis >= leaseMillis / 2 || idleSealMillis < 1) {
            throw new IllegalArgumentException("invalid local-first owner resource limits");
        }
        this.control = Objects.requireNonNull(control, "control");
        this.physical = Objects.requireNonNull(physical, "physical");
        this.backgroundControl = Objects.requireNonNull(backgroundControl, "backgroundControl");
        this.backgroundPhysical = Objects.requireNonNull(backgroundPhysical, "backgroundPhysical");
        if (!physical.shardId().equals(backgroundPhysical.shardId())) {
            throw new IllegalArgumentException("local-first owner background shard route differs");
        }
        this.snapshotFloor = Objects.requireNonNull(snapshotFloor, "snapshotFloor");
        this.segmentSize = segmentSize;
        this.leaseMillis = leaseMillis;
        this.idleSealNanos = TimeUnit.MILLISECONDS.toNanos(idleSealMillis);
        this.refresher = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-local-first-owner-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshMillis,
                refreshMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * The normal path is only local CAS; the current segment is retired and set aside when the old snapshot is below the lower bounds.
     *
     * @param startTs transaction start timestamp
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    Assignment allocate(final long startTs) {
        if (startTs < 0) {
            throw new IllegalArgumentException("invalid local-first start timestamp");
        }
        lock.readLock().lock();
        try {
            requireOpen();
            Assignment fast = tryAllocate(current, startTs);
            if (fast != null) {
                return fast;
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            requireOpen();
            while (true) {
                Assignment replay = tryAllocate(current, startTs);
                if (replay != null) {
                    return replay;
                }
                if (current != null) {
                    current.retiring = true;
                    current = null;
                }
                reserveNextSegment(startTs);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Foreground Undo the local pending count after confirming its centre terminal state and allow subsequent safe closure.
     *
     * @param assignment assignment
     */
    void complete(final Assignment assignment) {
        resolve(assignment, false);
    }

    /**
     * Shard candidate durable complete to collect active physics segment upon completion, and batch supplement centre terminal state.
     *
     * @param assignment assignment
     */
    void completePhysicalAndSeal(final Assignment assignment) {
        resolve(assignment, false);
        SegmentState state = assignment.state;
        if (!state.unsafe && state.unresolved.get() == 0
                && (state.retiring || state.nextTs.get() > state.reservation.lease().lastTs())) {
            sealKnown(state);
        }
    }

    /**
     * Local or Centre unknown outcome ceases the renewal of this paragraph and the new allocation and is handed over to the lost recovery.
     *
     * @param assignment assignment
     */
    void unknown(final Assignment assignment) {
        resolve(assignment, true);
    }

    /**
     * Visible rotations can be used to test the double-side renewal lease, which is also called by the background time line.
     */
    void refreshOnce() {
        for (SegmentState state : segments.values()) {
            if (state.unsafe) {
                if (backgroundControl.segment(state.reservation.lease().firstTs())
                        .filter(segment -> segment.status() == MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED).isPresent()) {
                    segments.remove(state.reservation.lease().firstTs(), state);
                }
                continue;
            }
            boolean seal = state.unresolved.get() == 0
                    && (state.retiring || state.nextTs.get() > state.reservation.lease().lastTs()
                            || System.nanoTime() - state.lastAssignedNanos >= idleSealNanos);
            if (seal) {
                sealKnown(state);
                continue;
            }
            lock.readLock().lock();
            try {
                if (state.unsafe || state.retiring && state.unresolved.get() == 0) {
                    continue;
                }
                backgroundPhysical.renew(state.reservation.lease(), leaseMillis);
                state.reservation = backgroundControl.renewSegment(state.reservation);
                refreshStats.recordSuccess(1);
            } catch (final RuntimeException ex) {
                state.unsafe = true;
                lastFailure.set(ex);
                refreshStats.recordFailure();
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Returnes the current round of PHRC renewals, which will not change silently to a valid lease.
     *
     * @return the operation result
     */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /**
     * Returns the reason for the failure of the last two-sided renewal or block.
     *
     * @return the operation result
     */
    RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /**
     * Returns the number of times this coordinator has been set aside in a batch initiated by durable.
     *
     * @return the operation result
     */
    long reservationCount() {
        return reservationSequence.get();
    }

    /**
     * CAS failed with only the current retry segment, which cannot be miscalculated as depleting the entire section and creating a PENDING gap.
     *
     * @param state state
     * @param startTs transaction start timestamp
     * @return the operation result
     */
    private Assignment tryAllocate(final SegmentState state, final long startTs) {
        if (state == null || state.unsafe || state.retiring
                || startTs < state.reservation.snapshotFloor()) {
            return null;
        }
        while (true) {
            long next = state.nextTs.get();
            if (next > state.reservation.lease().lastTs() || next <= startTs) {
                return null;
            }
            if (state.nextTs.compareAndSet(next, next + 1)) {
                state.unresolved.incrementAndGet();
                state.lastAssignedNanos = System.nanoTime();
                return new Assignment(state, state.reservation, next);
            }
        }
    }

    /**
     * The same requested key is only the original replay section after it has been set, installed or activated.
     *
     * @param startTs transaction start timestamp
     */
    private void reserveNextSegment(final long startTs) {
        if (pendingRequestKey == null) {
            pendingRequestKey = ownerId + ":" + reservationSequence.incrementAndGet();
            pendingSnapshotFloor = Math.min(startTs, snapshotFloor.getAsLong());
        }
        try {
            SegmentReservation reserved = control.reserveSegment(physical.shardId(), ownerId,
                    pendingRequestKey, segmentSize, leaseMillis, pendingSnapshotFloor);
            physical.install(reserved.lease(), leaseMillis);
            SegmentReservation active = control.activateSegment(reserved, physical);
            SegmentState state = new SegmentState(active);
            segments.put(active.lease().firstTs(), state);
            current = state;
            pendingRequestKey = null;
        } catch (final MvccException ex) {
            if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                pendingRequestKey = null;
            }
            throw ex;
        }
    }

    /**
     * Each distribution is released only once; the unknown result remains unsafe and cannot be stored in local count.
     *
     * @param assignment assignment
     * @param unknown unknown
     */
    private void resolve(final Assignment assignment, final boolean unknown) {
        Objects.requireNonNull(assignment, "assignment");
        if (assignment.resolved.compareAndSet(false, true)) {
            if (unknown) {
                assignment.state.unsafe = true;
            }
            assignment.state.unresolved.decrementAndGet();
        }
    }

    /**
     * The local distribution is first banned and then completed in the order of control FENCING, shard fence, candidate alignment and closure.
     *
     * @param state state
     */
    private void sealKnown(final SegmentState state) {
        lock.writeLock().lock();
        try {
            if (state.unsafe || state.unresolved.get() != 0
                    || segments.get(state.reservation.lease().firstTs()) != state) {
                return;
            }
            state.retiring = true;
            if (current == state) {
                current = null;
            }
        } finally {
            lock.writeLock().unlock();
        }
        try {
            SegmentReservation latest = backgroundControl.segment(state.reservation.lease().firstTs())
                    .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local-first active owner lost durable segment", false, null));
            var request = backgroundControl.beginFencing(latest, true);
            backgroundPhysical.fence(request.physicalLease());
            backgroundControl.sealFencedSegment(latest, backgroundPhysical);
            backgroundControl.advancePublishedFast(4096);
            segments.remove(latest.lease().firstTs(), state);
            refreshStats.recordSuccess(1);
        } catch (final RuntimeException ex) {
            state.unsafe = true;
            lastFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /**
     * The lease renewal failed to leave only durable expired slots for independent recovery rounds.
     */
    private void refreshSafely() {
        try {
            refreshOnce();
        } catch (final RuntimeException ex) {
            lastFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /**
     * No new allocations will be accepted; zero pending is actively sealed in the same fencing order.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        refresher.shutdownNow();
        try {
            refresher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        for (SegmentState state : segments.values()) {
            if (!state.unsafe && state.unresolved.get() == 0) {
                sealKnown(state);
            }
        }
    }

    /**
     * Disables continued acceptance of transaction with old paragraph when closed or lost owner.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local-first owner registry is closed", false, null);
        }
    }

    /**
     * Durable, the serial number within the unlocked segment and the number of transaction s for Centre terminal state that have not yet completed.
     */
    private static final class SegmentState {

        private volatile SegmentReservation reservation;

        private final AtomicLong nextTs;

        private final AtomicInteger unresolved = new AtomicInteger();

        private volatile long lastAssignedNanos = System.nanoTime();

        private volatile boolean retiring;

        private volatile boolean unsafe;

        /**
         * Starting with the first durable slot in the paragraph, take a number for local.
         *
         * @param reservation reservation
         */
        private SegmentState(final SegmentReservation reservation) {
            this.reservation = reservation;
            this.nextTs = new AtomicLong(reservation.lease().firstTs());
        }
    }

    /**
     * Foreground Retains the owner receipt at the time of receipt, and renews the lease only to change the deadline without changing the algebra.
     */
    static final class Assignment {

        private final SegmentState state;

        private final SegmentReservation reservation;

        private final long commitTs;

        private final AtomicBoolean resolved = new AtomicBoolean();

        /**
         * Saves the only slot and current durable owner identity.
         *
         * @param state state
         * @param reservation reservation
         * @param commitTs durable commit timestamp
         */
        private Assignment(final SegmentState state, final SegmentReservation reservation, final long commitTs) {
            this.state = state;
            this.reservation = reservation;
            this.commitTs = commitTs;
        }

        /**
         * Returns original batch completed and shared with shard Physics commit. identity.
         *
         * @return the operation result
         */
        SegmentReservation reservation() {
            return reservation;
        }

        /**
         * Return this transaction's unique commit timestamp.
         *
         * @return the operation result
         */
        long commitTs() {
            return commitTs;
        }
    }
}

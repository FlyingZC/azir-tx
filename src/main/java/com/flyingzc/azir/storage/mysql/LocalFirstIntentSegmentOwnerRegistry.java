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
import java.util.ArrayList;
import java.util.List;
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
 * Allocates a central time slot for cross-shard intent transaction and maintains the lower bounds and safe endings of owner lease.
 */
final class LocalFirstIntentSegmentOwnerRegistry implements AutoCloseable {

    private final MysqlLocalFirstPublicationLedger control;

    private final MysqlLocalFirstPublicationLedger backgroundControl;

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
     * Ex- background reusable control example; production connection pool identity should be checked first by the outer entrance.
     *
     * @param control control
     * @param snapshotFloor snapshot floor
     * @param segmentSize segment size
     * @param leaseMillis lease millis
     * @param refreshMillis refresh millis
     * @param idleSealMillis idle seal millis
     */
    LocalFirstIntentSegmentOwnerRegistry(final MysqlLocalFirstPublicationLedger control,
                                         final LongSupplier snapshotFloor, final int segmentSize,
                                         final long leaseMillis, final long refreshMillis, final long idleSealMillis) {
        this(control, control, snapshotFloor, segmentSize, leaseMillis, refreshMillis, idleSealMillis);
    }

    /**
     * Limit the number of segments, database lease and background renewal periods to avoid a long-term nailing to safe point.
     *
     * @param control control
     * @param backgroundControl background control
     * @param snapshotFloor snapshot floor
     * @param segmentSize segment size
     * @param leaseMillis lease millis
     * @param refreshMillis refresh millis
     * @param idleSealMillis idle seal millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    LocalFirstIntentSegmentOwnerRegistry(final MysqlLocalFirstPublicationLedger control,
                                         final MysqlLocalFirstPublicationLedger backgroundControl,
                                         final LongSupplier snapshotFloor, final int segmentSize,
                                         final long leaseMillis, final long refreshMillis, final long idleSealMillis) {
        if (segmentSize < 1 || segmentSize > 4096 || leaseMillis < 3
                || leaseMillis > 86_400_000L || refreshMillis < 1
                || refreshMillis >= leaseMillis / 2 || idleSealMillis < 1) {
            throw new IllegalArgumentException("invalid cross-shard intent owner resource limits");
        }
        this.control = Objects.requireNonNull(control, "control");
        this.backgroundControl = Objects.requireNonNull(backgroundControl, "backgroundControl");
        this.snapshotFloor = Objects.requireNonNull(snapshotFloor, "snapshotFloor");
        this.segmentSize = segmentSize;
        this.leaseMillis = leaseMillis;
        this.idleSealNanos = TimeUnit.MILLISECONDS.toNanos(idleSealMillis);
        this.refresher = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-intent-owner-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshMillis,
                refreshMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * The current paragraph is available with no lock CAS numbers, and the old reading snapshot lower than the lower bounds of the segment is replaced by a new section.
     *
     * @param startTs transaction start timestamp
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    Assignment allocate(final long startTs) {
        if (startTs < 0) {
            throw new IllegalArgumentException("invalid cross-shard start timestamp");
        }
        lock.readLock().lock();
        try {
            requireOpen();
            Assignment assigned = tryAllocate(current, startTs);
            if (assigned != null) {
                return assigned;
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            requireOpen();
            while (true) {
                Assignment assigned = tryAllocate(current, startTs);
                if (assigned != null) {
                    return assigned;
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
     * The same transaction group receives a continuous time slot to avoid staggering the commit slots of the concurrent group.
     *
     * @param startTimestamps start timestamps
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    List<Assignment> allocateBatch(final List<Long> startTimestamps) {
        if (startTimestamps == null || startTimestamps.isEmpty()
                || startTimestamps.size() > 64 || startTimestamps.stream().anyMatch(
                        timestamp -> timestamp == null || timestamp < 0)) {
            throw new IllegalArgumentException("invalid cross-shard intent owner batch");
        }
        long minimumStart = startTimestamps.stream().mapToLong(Long::longValue).min().orElseThrow();
        long maximumStart = startTimestamps.stream().mapToLong(Long::longValue).max().orElseThrow();
        lock.writeLock().lock();
        try {
            requireOpen();
            while (true) {
                SegmentState state = current;
                if (state != null && !state.unsafe && !state.retiring
                        && minimumStart >= state.reservation.snapshotFloor()) {
                    long next = state.nextTs.get();
                    if (next > maximumStart && next <= Long.MAX_VALUE - startTimestamps.size()
                            && next + startTimestamps.size() - 1 <= state.reservation.lease().lastTs()) {
                        state.nextTs.set(next + startTimestamps.size());
                        state.unresolved.addAndGet(startTimestamps.size());
                        state.lastAssignedNanos = System.nanoTime();
                        List<Assignment> assigned = new ArrayList<>(startTimestamps.size());
                        for (int index = 0; index < startTimestamps.size(); index++) {
                            assigned.add(new Assignment(state, state.reservation, next + index));
                        }
                        return List.copyOf(assigned);
                    }
                }
                if (state != null) {
                    state.retiring = true;
                    current = null;
                }
                reserveNextSegment(minimumStart);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Centre decision has been released on board and pending count so that the empty segment can be safely sealed.
     *
     * @param assignment assignment
     */
    void complete(final Assignment assignment) {
        resolve(assignment, false);
    }

    /**
     * Stops the old owner 's new distribution when commit unknown outcome expires, judging slot terminal state by durable due recovery.
     *
     * @param assignment assignment
     */
    void unknown(final Assignment assignment) {
        resolve(assignment, true);
    }

    /**
     * Re-letting, closure and loss of connection logic for centres with the same visible rotations and timelines.
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
     * Returns background renewal and active segment count.
     *
     * @return the operation result
     */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /**
     * Failed to return the last lease or block renewal.
     *
     * @return the operation result
     */
    RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /**
     * Returns the number of segments that were previously reserved for durable for capacity and GC long run observations.
     *
     * @return the operation result
     */
    long reservationCount() {
        return reservationSequence.get();
    }

    /**
     * CAS failed in only retry current slot, without depleting the end of the paragraph that is still available.
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
     * The same requested key saves or activates only the original paragraph replay when the response is lost.
     *
     * @param startTs transaction start timestamp
     */
    private void reserveNextSegment(final long startTs) {
        if (pendingRequestKey == null) {
            pendingRequestKey = ownerId + ":" + reservationSequence.incrementAndGet();
            pendingSnapshotFloor = Math.min(startTs, snapshotFloor.getAsLong());
        }
        try {
            SegmentReservation reserved = control.reserveSegment(
                    MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE, ownerId,
                    pendingRequestKey, segmentSize, leaseMillis, pendingSnapshotFloor);
            SegmentReservation active = control.activateIntentSegment(reserved);
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
     * One distribution is released only once, and the unknown result leaves the original paragraph to expire recovery.
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
     * No new collections are allowed, then the centre FENCING, empty tail suspension and consecutive release of closed sections.
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
                            "cross-shard intent owner lost durable segment", false, null));
            backgroundControl.beginFencing(latest, true);
            backgroundControl.sealIntentSegment(latest);
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
     * Time failure retains evidence, and the next round of recovery cannot be based on an local guess to continue with the old algebra.
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
     * Normal stoppages only contain sections of commit that no longer exist at pending Center.
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
     * The old owner no longer allocates any slots after closing.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "cross-shard intent owner is closed", false, null);
        }
    }

    /**
     * A number of local receipts for durable lease and decision which have not completed the centre decision.
     */
    private static final class SegmentState {

        private volatile SegmentReservation reservation;

        private final AtomicLong nextTs;

        private final AtomicInteger unresolved = new AtomicInteger();

        private volatile long lastAssignedNanos = System.nanoTime();

        private volatile boolean retiring;

        private volatile boolean unsafe;

        /**
         * Distribution only starts with the first slot that has been set aside by durable.
         *
         * @param reservation reservation
         */
        private SegmentState(final SegmentReservation reservation) {
            this.reservation = reservation;
            this.nextTs = new AtomicLong(reservation.lease().firstTs());
        }
    }

    /**
     * Foreground preserves the original owner algebra at the time of receipt, and renewal does not change slot identity.
     */
    static final class Assignment {

        private final SegmentState state;

        private final SegmentReservation reservation;

        private final long commitTs;

        private final AtomicBoolean resolved = new AtomicBoolean();

        /**
         * The only time slot bound is one durable segment.
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
         * Returns the batch decision section identity which must be checked.
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

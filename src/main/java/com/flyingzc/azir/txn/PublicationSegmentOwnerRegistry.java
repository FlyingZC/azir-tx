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

package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.publication.PublicationSegmentCommit;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Local cross batch allocates the durable slot and coordinates decision and owner lease renewals and secure storage.
 */
final class PublicationSegmentOwnerRegistry implements AutoCloseable {

    private final MysqlSegmentPublicationLedger foregroundLedger;

    private final MysqlSegmentPublicationLedger backgroundLedger;

    private final String ownerId = UUID.randomUUID().toString();

    private final int segmentSize;

    private final long leaseMillis;

    private final long idleSealNanos;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final Map<Long, SegmentState> segments = new HashMap<>();

    private final ScheduledExecutorService refresher;

    private final BackgroundTaskStats refreshStats = new BackgroundTaskStats();

    private final AtomicReference<RuntimeException> lastRefreshFailure = new AtomicReference<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile SegmentState current;

    private String pendingReservationKey;

    private long reservationSequence;

    /**
     * Creates restricted segments, background lease duration and normal free storage cycles.
     *
     * @param foregroundLedger foreground ledger
     * @param backgroundLedger background ledger
     * @param segmentSize segment size
     * @param leaseMillis lease millis
     * @param refreshMillis refresh millis
     * @param idleSealMillis idle seal millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PublicationSegmentOwnerRegistry(final MysqlSegmentPublicationLedger foregroundLedger,
                                    final MysqlSegmentPublicationLedger backgroundLedger,
                                    final int segmentSize, final long leaseMillis,
                                    final long refreshMillis, final long idleSealMillis) {
        if (segmentSize < 1 || segmentSize > 4096 || leaseMillis < 3
                || leaseMillis > 86_400_000 || refreshMillis < 1
                || refreshMillis >= leaseMillis / 2 || idleSealMillis < 1) {
            throw new IllegalArgumentException("invalid segment owner resource limits");
        }
        this.foregroundLedger = Objects.requireNonNull(foregroundLedger, "foregroundLedger");
        this.backgroundLedger = Objects.requireNonNull(backgroundLedger, "backgroundLedger");
        this.segmentSize = segmentSize;
        this.leaseMillis = leaseMillis;
        this.idleSealNanos = TimeUnit.MILLISECONDS.toNanos(idleSealMillis);
        this.refresher = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-segment-owner-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshMillis,
                refreshMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Durable reserves a new range only when the local segment is exhausted, and the transaction identifier and the time slots remain independent of each other.
     *
     * @param startTs transaction start timestamp
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    Assignment allocate(final long startTs) {
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
                if (current == null || current.nextTs.get() > current.lease.lastTs() || current.unsafe) {
                    reserveNextSegment();
                }
                SegmentState segment = current;
                Assignment allocated = tryAllocate(segment, startTs);
                if (allocated != null) {
                    return allocated;
                }
                if (segment.nextTs.get() <= startTs) {
                    if (segment.unresolved.get() != 0) {
                        segment.unsafe = true;
                        throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                                "segment contains timestamps older than transaction snapshot", false, null);
                    }
                    backgroundLedger.sealOwnedSegment(segment.lease);
                    segments.remove(segment.lease.firstTs());
                    current = null;
                    continue;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Share fence to get empty slots in the sector with CAS, and renewing lease fence is still waiting for registration to be completed.
     *
     * @param segment segment
     * @param startTs transaction start timestamp
     * @return the operation result
     */
    private Assignment tryAllocate(final SegmentState segment, final long startTs) {
        if (segment == null || segment.unsafe) {
            return null;
        }
        while (true) {
            long next = segment.nextTs.get();
            if (next > segment.lease.lastTs() || next <= startTs) {
                return null;
            }
            if (segment.nextTs.compareAndSet(next, next + 1)) {
                segment.unresolved.incrementAndGet();
                segment.lastAssignedNanos = System.nanoTime();
                return new Assignment(segment.lease.firstTs(), next);
            }
        }
    }

    /**
     * Write durable Center decision with the current algebra and stop renewing this paragraph in case of an anomaly and submit it to recovery for confirmation.
     *
     * @param assignment assignment
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @return the operation result
     */
    long decide(final Assignment assignment, final long txId, final long deadlineMillis, final Set<String> participants) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                long commitTs = foregroundLedger.decideCommitted(segment.lease,
                        assignment.commitTs(), txId, deadlineMillis, participants);
                resolve(segment, assignment);
                return commitTs;
            } catch (final RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Center durable commit returns shard materialization receipt, the unknown result is still funcing local owner.
     *
     * @param assignment assignment
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @return the operation result
     */
    MysqlSegmentPublicationLedger.CommittedProof decideWithProof(final Assignment assignment, final long txId,
                                                                 final long startTs, final long deadlineMillis,
                                                                 final Set<String> participants) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                var proof = foregroundLedger.decideCommittedWithProof(segment.lease,
                        assignment.commitTs(), txId, startTs, deadlineMillis, participants);
                resolve(segment, assignment);
                return proof;
            } catch (final RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * A batch of decision s in the same segment holds local to read fence until the whole batch controls transaction drops.
     *
     * @param requests requests
     * @throws IllegalArgumentException if the operation cannot complete
     */
    void decideBatch(final List<CommitRequest> requests) {
        if (requests.isEmpty()) {
            return;
        }
        lock.readLock().lock();
        try {
            try {
                SegmentState segment = requireAssignment(requests.getFirst().assignment());
                List<PublicationSegmentCommit> durable = new java.util.ArrayList<>(requests.size());
                for (CommitRequest request : requests) {
                    if (requireAssignment(request.assignment()) != segment) {
                        throw new IllegalArgumentException("segmented decision batch mixed owner ranges");
                    }
                    durable.add(new PublicationSegmentCommit(segment.lease,
                            request.assignment().commitTs(), request.txId(),
                            request.deadlineMillis(), request.participants()));
                }
                foregroundLedger.decideCommittedBatch(durable);
                for (CommitRequest request : requests) {
                    resolve(segment, request.assignment());
                }
            } catch (final RuntimeException ex) {
                // Batch may have been commit or rollback, and the entire body lost local owner privileges and submitted to durable recovery for verification.
                for (CommitRequest request : requests) {
                    SegmentState segment = segments.get(request.assignment().segmentStart());
                    if (segment != null) {
                        segment.unsafe = true;
                        resolve(segment, request.assignment());
                    }
                }
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * The durable candidate for local before the first physical commit is in the current own algebra.
     *
     * @param assignment assignment
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participant participant
     * @return the operation result
     */
    long claimLocal(final Assignment assignment, final long txId, final long deadlineMillis, final String participant) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                return foregroundLedger.claimLocalPending(segment.lease,
                        assignment.commitTs(), txId, deadlineMillis, participant);
            } catch (final RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Local COMMITTED certifies that the same centre decision can be completed even after its owner has expired.
     *
     * @param assignment assignment
     * @param local local
     * @param participant participant
     * @return the operation result
     */
    boolean completeLocalCommit(final Assignment assignment, final ParticipantRecord local, final String participant) {
        return completeLocal(assignment, local, participant, true);
    }

    /**
     * Local ABORTED proves that the release of the candidate slot does not cover the possible commit.
     *
     * @param assignment assignment
     * @param local local
     * @param participant participant
     * @return the operation result
     */
    boolean completeLocalAbort(final Assignment assignment, final ParticipantRecord local, final String participant) {
        return completeLocal(assignment, local, participant, false);
    }

    /**
     * Local terminal state plus decision does not depend on the current homeer algebra, but only on the durable shard certificate.
     *
     * @param assignment assignment
     * @param local local
     * @param participant participant
     * @param committed committed
     * @return the operation result
     */
    private boolean completeLocal(final Assignment assignment, final ParticipantRecord local,
                                  final String participant, final boolean committed) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = segments.get(assignment.segmentStart());
            try {
                boolean changed = committed
                        ? foregroundLedger.completeLocalCommitAfterFencing(local,
                                assignment.commitTs(), participant)
                        : foregroundLedger.completeLocalAbortAfterFencing(local,
                                assignment.commitTs(), participant);
                if (segment != null) {
                    resolve(segment, assignment);
                }
                return changed;
            } catch (final RuntimeException ex) {
                if (segment != null) {
                    segment.unsafe = true;
                    resolve(segment, assignment);
                }
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Abort this slot after participant closing in the current algebra.
     *
     * @param assignment assignment
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @return the operation result
     */
    boolean abort(final Assignment assignment, final long txId, final long deadlineMillis) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                boolean aborted = foregroundLedger.abortReservedSlot(segment.lease,
                        assignment.commitTs(), txId, deadlineMillis);
                resolve(segment, assignment);
                return aborted;
            } catch (final RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Background is renewed to update the algebra in local row fence, reading side decision does not get a hybrid algebra.
     *
     * @return the operation result
     */
    int refresh() {
        lock.writeLock().lock();
        try {
            if (closed.get()) {
                return 0;
            }
            long published = backgroundLedger.currentPublished();
            long now = System.nanoTime();
            int progressed = 0;
            for (SegmentState segment : List.copyOf(segments.values())) {
                if (segment.unsafe) {
                    continue;
                }
                boolean unused = segment == current && segment.nextTs.get() <= segment.lease.lastTs();
                if (unused && segment.unresolved.get() == 0
                        && now - segment.lastAssignedNanos >= idleSealNanos) {
                    backgroundLedger.sealOwnedSegment(segment.lease);
                    segments.remove(segment.lease.firstTs());
                    current = null;
                    progressed++;
                } else if (segment.unresolved.get() == 0 && !unused) {
                    segments.remove(segment.lease.firstTs());
                } else if (segment.lease.lastTs() > published) {
                    renewOrFence(segment);
                    progressed++;
                }
            }
            if (pendingReservationKey != null) {
                String pending = pendingReservationKey;
                try {
                    PublicationSegmentLease recovered = backgroundLedger.reserveSegment(ownerId,
                            pending, segmentSize, leaseMillis);
                    pendingReservationKey = null;
                    SegmentState segment = new SegmentState(recovered);
                    segments.put(recovered.firstTs(), segment);
                    current = segment;
                    // The range not delivered to foreground is not in transit decision and should be immediately sealed instead of equal lease natural maturity.
                    backgroundLedger.sealOwnedSegment(recovered);
                    segments.remove(recovered.firstTs());
                    current = null;
                    progressed++;
                } catch (final MvccException ex) {
                    if (ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN
                            || ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                        pendingReservationKey = null;
                        lastRefreshFailure.set(ex);
                    } else {
                        throw ex;
                    }
                }
            }
            return progressed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the round count of background algebra updates and active storage.
     *
     * @return the operation result
     */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /**
     * Returns the latest background renewal or storage failed.
     *
     * @return the operation result
     */
    RuntimeException lastRefreshFailure() {
        return lastRefreshFailure.get();
    }

    /**
     * Stops the new distribution after closing; the tailings slot has been sealed in decision and unknown decision is reserved for durable due to take over.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        refresher.shutdownNow();
        lock.writeLock().lock();
        try {
            RuntimeException failure = null;
            for (SegmentState segment : List.copyOf(segments.values())) {
                if (segment.unsafe || segment.unresolved.get() != 0) {
                    continue;
                }
                try {
                    backgroundLedger.sealOwnedSegment(segment.lease);
                } catch (final RuntimeException ex) {
                    if (failure == null) {
                        failure = ex;
                    } else if (failure != ex) {
                        failure.addSuppressed(ex);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The same requested key will not be repeated after the retry key has been lost.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void reserveNextSegment() {
        if (pendingReservationKey == null) {
            if (reservationSequence == Long.MAX_VALUE) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "segment owner reservation sequence exhausted", false, null);
            }
            pendingReservationKey = ownerId + ":" + ++reservationSequence;
        }
        try {
            PublicationSegmentLease lease = foregroundLedger.reserveSegment(ownerId,
                    pendingReservationKey, segmentSize, leaseMillis);
            pendingReservationKey = null;
            SegmentState segment = new SegmentState(lease);
            segments.put(lease.firstTs(), segment);
            current = segment;
        } catch (final MvccException ex) {
            if (ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN
                    || ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                pendingReservationKey = null;
            }
            throw ex;
        }
    }

    /**
     * Any uncompleted segment stops the renewal of the lease when the owner responds to unknown information and is terminated by durable due take-over.
     *
     * @param segment segment
     */
    private void renewOrFence(final SegmentState segment) {
        PublicationSegmentLease previous = segment.lease;
        try {
            segment.lease = backgroundLedger.renewSegment(previous, leaseMillis);
        } catch (final RuntimeException ex) {
            try {
                PublicationSegmentLease durable = backgroundLedger.segmentLease(previous.firstTs()).orElse(null);
                if (durable != null && durable.ownerId().equals(ownerId)
                        && durable.lastTs() == previous.lastTs()
                        && durable.generation() >= previous.generation()) {
                    segment.lease = durable;
                } else {
                    segment.unsafe = true;
                }
            } catch (final RuntimeException inspectFailure) {
                segment.unsafe = true;
                ex.addSuppressed(inspectFailure);
            }
            throw ex;
        }
    }

    /**
     * Commit in the same sub-paragraph and suspension of confirmation of release of local places only once.
     *
     * @param segment segment
     * @param assignment assignment
     */
    private void resolve(final SegmentState segment, final Assignment assignment) {
        if (assignment.resolved.compareAndSet(false, true)) {
            segment.unresolved.decrementAndGet();
        }
    }

    /**
     * A slot that refuses to close, is missing or has been handed over to recovery for take-over.
     *
     * @param assignment assignment
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private SegmentState requireAssignment(final Assignment assignment) {
        requireOpen();
        SegmentState segment = segments.get(assignment.segmentStart());
        if (segment == null || segment.unsafe || assignment.resolved.get()) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segment assignment has no active local owner", false, null);
        }
        return segment;
    }

    /**
     * After-closing foreground requests no new subparagraph decision rights.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "segment owner registry is closed", false, null);
        }
    }

    /**
     * The cycle failed to keep the next retry opportunity open and abnormal.
     */
    private void refreshSafely() {
        try {
            refreshStats.recordSuccess(refresh());
        } catch (final RuntimeException ex) {
            lastRefreshFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /**
     * Saves a current durable algebra, the next local slot and an unverified number of terminal state.
     */
    private static final class SegmentState {

        private volatile PublicationSegmentLease lease;

        private final AtomicInteger unresolved = new AtomicInteger();

        private final AtomicLong nextTs;

        private volatile long lastAssignedNanos;

        private volatile boolean unsafe;

        /**
         * Distribution starts from the first slot set aside for durable.
         *
         * @param lease lease
         */
        private SegmentState(final PublicationSegmentLease lease) {
            this.lease = lease;
            this.nextTs = new AtomicLong(lease.firstTs());
        }
    }

    /**
     * A business commit has been received but not yet the terminal proof slot.
     */
    static final class Assignment {

        private final long segmentStart;

        private final long commitTs;

        private final AtomicBoolean resolved = new AtomicBoolean();

        /**
         * Save session identity with global commit timestamp.
         *
         * @param segmentStart segment start
         * @param commitTs durable commit timestamp
         */
        private Assignment(final long segmentStart, final long commitTs) {
            this.segmentStart = segmentStart;
            this.commitTs = commitTs;
        }

        /**
         * Returns the first timestamp of the durable segment.
         *
         * @return the operation result
         */
        long segmentStart() {
            return segmentStart;
        }

        /**
         * Returns this transaction only received commit timestamp.
         *
         * @return the operation result
         */
        long commitTs() {
            return commitTs;
        }
    }

    /**
     * An prewrite transaction and its local assigned to the decision slot.
     *
     * @param assignment assignment
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participants participants
     */
    record CommitRequest(Assignment assignment, long txId, long deadlineMillis, Set<String> participants) {

        /**
         * Fixed participant identity to avoid retweeting write set.
         *
         * @param assignment assignment
         * @param txId transaction identifier
         * @param deadlineMillis deadline millis
         * @param participants participants
         */
        CommitRequest {
            Objects.requireNonNull(assignment, "assignment");
            participants = Set.copyOf(participants);
        }
    }
}

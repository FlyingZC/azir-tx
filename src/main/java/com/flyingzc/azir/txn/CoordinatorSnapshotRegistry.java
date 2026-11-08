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

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.CoordinatorSnapshotLease;
import com.flyingzc.azir.publication.CoordinatorSnapshotView;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Protect this node snapshot with an durable coordinator lease active and write transaction lease as needed.
 */
final class CoordinatorSnapshotRegistry implements AutoCloseable {

    private static final long MAX_COORDINATOR_ID = (1L << 20) - 1;

    private static final long MAX_LOCAL_SEQUENCE = (1L << 43) - 1;

    private final MysqlPublicationLedger foregroundLedger;

    private final MysqlPublicationLedger backgroundLedger;

    private final long transactionTimeoutMillis;

    private final long leaseTimeoutMillis;

    private final ReentrantReadWriteLock membershipLock = new ReentrantReadWriteLock(true);

    private final ConcurrentHashMap<Long, Long> activeReadTs = new ConcurrentHashMap<>();

    private final AtomicLong localSequence = new AtomicLong();

    private final ScheduledExecutorService refresher;

    private final BackgroundTaskStats refreshStats = new BackgroundTaskStats();

    private final AtomicReference<RuntimeException> lastRefreshFailure = new AtomicReference<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile CoordinatorSnapshotLease lease;

    private volatile boolean fenced;

    /**
     * Creates the only durable coordinator number and starts the bounded cycle renewal.
     *
     * @param foregroundLedger foreground ledger
     * @param backgroundLedger background ledger
     * @param transactionTimeoutMillis transaction timeout millis
     * @param leaseTimeoutMillis lease timeout millis
     * @param refreshIntervalMillis refresh interval millis
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    CoordinatorSnapshotRegistry(final MysqlPublicationLedger foregroundLedger, final MysqlPublicationLedger backgroundLedger,
                                final long transactionTimeoutMillis, final long leaseTimeoutMillis,
                                final long refreshIntervalMillis) {
        if (transactionTimeoutMillis < 1 || leaseTimeoutMillis <= transactionTimeoutMillis
                || leaseTimeoutMillis > 86_400_000L || refreshIntervalMillis < 1
                || refreshIntervalMillis >= leaseTimeoutMillis - transactionTimeoutMillis) {
            throw new IllegalArgumentException("invalid coordinator snapshot lease limits");
        }
        this.foregroundLedger = Objects.requireNonNull(foregroundLedger, "foregroundLedger");
        this.backgroundLedger = Objects.requireNonNull(backgroundLedger, "backgroundLedger");
        this.transactionTimeoutMillis = transactionTimeoutMillis;
        this.leaseTimeoutMillis = leaseTimeoutMillis;
        this.lease = backgroundLedger.acquireCoordinatorLease(leaseTimeoutMillis);
        if (lease.coordinatorId() > MAX_COORDINATOR_ID) {
            backgroundLedger.releaseCoordinatorLease(lease.coordinatorId(), lease.generation());
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "coordinator id namespace exhausted", false, null);
        }
        this.refresher = Executors.newSingleThreadScheduledExecutor(runnable -> Thread.ofPlatform().daemon().name("mvcc-coordinator-snapshot-refresh").unstarted(runnable));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshIntervalMillis,
                refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Finish database snapshot reading and active registration in the local reading lock and return transaction IDs that do not collide with positive lease.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    BeginReservation begin(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        for (int attempt = 0; attempt < 2; attempt++) {
            boolean renewNeeded = false;
            membershipLock.readLock().lock();
            try {
                requireAvailable();
                CoordinatorSnapshotLease current = lease;
                CoordinatorSnapshotView view;
                try {
                    view = foregroundLedger.snapshotUnderCoordinatorLease(current.coordinatorId(),
                            current.generation(), transactionTimeoutMillis);
                } catch (final MvccException ex) {
                    if (ex.errorCode() == ErrorCode.TRANSACTION_TIMEOUT && attempt == 0) {
                        renewNeeded = true;
                        view = null;
                    } else {
                        if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                            fenced = true;
                        }
                        throw ex;
                    }
                }
                if (!renewNeeded) {
                    long txId = nextTransactionId(current.coordinatorId());
                    activeReadTs.put(txId, view.readTs());
                    return new BeginReservation(txId, view.readTs(), view.deadlineMillis(), false);
                }
            } finally {
                membershipLock.readLock().unlock();
            }
            refresh();
        }
        throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                "coordinator lease cannot cover a new transaction deadline", false, null);
    }

    /**
     * Atomic is independent of lease and write participant metadata in local reading fence.
     *
     * @param tx transaction context
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    TxRecord promoteAndRegister(final Transaction tx, final Set<String> participants) {
        membershipLock.readLock().lock();
        try {
            requireAvailable();
            Long protectedRead = activeReadTs.get(tx.txId());
            if (protectedRead == null || protectedRead != tx.startTs()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "transaction is not protected by this coordinator", false, tx.txId());
            }
            CoordinatorSnapshotLease current = lease;
            return foregroundLedger.promoteAndRegisterCoordinatorSnapshot(tx.txId(), tx.startTs(),
                    tx.deadlineMillis(), current.coordinatorId(), current.generation(),
                    tx.isolationLevel(), participants);
        } finally {
            membershipLock.readLock().unlock();
        }
    }

    /**
     * After the centre successfully writes metadata, independent durable lease takes over GC protection for this transaction.
     *
     * @param txId transaction identifier
     */
    void promotionPersisted(final long txId) {
        activeReadTs.remove(txId);
    }

    /**
     * Returns whether transaction is currently protected by coordinator lease.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    boolean protects(final long txId) {
        return activeReadTs.containsKey(txId);
    }

    /**
     * Review durable algebra and lease time limits before each storage operation to prevent missing coordinator from continuing to read and write.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    void assertProtected(final Transaction tx) {
        membershipLock.readLock().lock();
        try {
            requireAvailable();
            Long protectedRead = activeReadTs.get(tx.txId());
            if (protectedRead == null || protectedRead != tx.startTs()) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "transaction no longer has coordinator snapshot protection", false, tx.txId());
            }
            try {
                foregroundLedger.assertCoordinatorSnapshot(lease.coordinatorId(), lease.generation(),
                        tx.startTs(), tx.deadlineMillis(), tx.txId());
            } catch (final MvccException ex) {
                if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                    fenced = true;
                }
                throw ex;
            }
        } finally {
            membershipLock.readLock().unlock();
        }
    }

    /**
     * Remove local active snapshot and return if it is still protected by coordinator lease instead of independent lease.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    boolean release(final long txId) {
        return activeReadTs.remove(txId) != null;
    }

    /**
     * Raises the lower durable boundary to its oldest active snapshot within local fence and renews it.
     *
     * @throws MvccException if the operation cannot complete
     */
    void refresh() {
        membershipLock.writeLock().lock();
        try {
            requireAvailable();
            long oldest = activeReadTs.values().stream().mapToLong(Long::longValue).min()
                    .orElseGet(foregroundLedger::currentPublished);
            CoordinatorSnapshotLease current = lease;
            if (oldest < current.readTs()) {
                fenced = true;
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "coordinator snapshot lower bound would move backward", false, null);
            }
            try {
                lease = backgroundLedger.renewCoordinatorLease(current.coordinatorId(),
                        current.generation(), oldest, leaseTimeoutMillis);
            } catch (final MvccException ex) {
                if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                    fenced = true;
                }
                throw ex;
            }
        } finally {
            membershipLock.writeLock().unlock();
        }
    }

    /**
     * Returns the renewal of lease cycles for successful and unsuccessful statistics.
     *
     * @return the operation result
     */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /**
     * Returns an abnormal renewal of the lease during the last cycle.
     *
     * @return the operation result
     */
    RuntimeException lastRefreshFailure() {
        return lastRefreshFailure.get();
    }

    /**
     * Do not receive new snapshot after closing; if local transaction is still available, keep lease waiting for durable timeout.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        refresher.shutdownNow();
        membershipLock.writeLock().lock();
        try {
            if (activeReadTs.isEmpty() && !fenced) {
                backgroundLedger.releaseCoordinatorLease(lease.coordinatorId(), lease.generation());
            }
        } finally {
            membershipLock.writeLock().unlock();
        }
    }

    /**
     * A protocol with a combination of durable coordinator ID and a single-tone local count, using negative namespace.
     *
     * @param coordinatorId coordinator id
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private long nextTransactionId(final long coordinatorId) {
        long sequence = localSequence.incrementAndGet();
        if (sequence > MAX_LOCAL_SEQUENCE) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "coordinator local transaction id namespace exhausted", false, null);
        }
        return Long.MIN_VALUE | (coordinatorId << 43) | sequence;
    }

    /**
     * Coordinator Continued service is prohibited when the durable algebra has been closed or lost.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireAvailable() {
        if (closed.get() || fenced) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "coordinator snapshot lease is closed or fenced", false, null);
        }
    }

    /**
     * The lease renewal failed silently, and the database temporary failure is still next round retry.
     */
    private void refreshSafely() {
        try {
            refresh();
            refreshStats.recordSuccess(1);
        } catch (final RuntimeException ex) {
            lastRefreshFailure.set(ex);
            refreshStats.recordFailure();
        }
    }
}

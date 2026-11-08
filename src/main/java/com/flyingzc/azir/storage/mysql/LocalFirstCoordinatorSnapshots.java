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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.publication.CoordinatorSnapshotLease;
import com.flyingzc.azir.publication.CoordinatorSnapshotView;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Multiple RC/RR snapshot with an durable coordinator lease to protect the local first commit protocol.
 */
final class LocalFirstCoordinatorSnapshots implements AutoCloseable {

    private static final long MAX_COORDINATOR_ID = (1L << 20) - 1;

    private static final long MAX_LOCAL_SEQUENCE = (1L << 43) - 1;

    private final MysqlLocalFirstPublicationLedger control;

    private final long transactionTimeoutMillis;

    private final long leaseMillis;

    private final ReentrantReadWriteLock membership = new ReentrantReadWriteLock(true);

    private final ConcurrentHashMap<Long, Long> activeReadTs = new ConcurrentHashMap<>();

    private final AtomicLong sequence = new AtomicLong();

    private final ScheduledExecutorService refresher;

    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile CoordinatorSnapshotLease lease;

    private volatile boolean fenced;

    /**
     * Coordinator lease is required to overwrite the full transaction term and leave an background lease extension.
     *
     * @param control control
     * @param transactionTimeoutMillis transaction timeout millis
     * @param leaseMillis lease millis
     * @param refreshIntervalMillis refresh interval millis
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    LocalFirstCoordinatorSnapshots(final MysqlLocalFirstPublicationLedger control,
                                   final long transactionTimeoutMillis, final long leaseMillis,
                                   final long refreshIntervalMillis) {
        if (transactionTimeoutMillis < 1 || leaseMillis <= transactionTimeoutMillis
                || leaseMillis > 86_400_000L || refreshIntervalMillis < 1
                || refreshIntervalMillis >= leaseMillis - transactionTimeoutMillis) {
            throw new IllegalArgumentException("invalid local-first coordinator lease limits");
        }
        this.control = Objects.requireNonNull(control, "control");
        this.transactionTimeoutMillis = transactionTimeoutMillis;
        this.leaseMillis = leaseMillis;
        this.lease = control.acquireCoordinatorLease(leaseMillis);
        if (lease.coordinatorId() > MAX_COORDINATOR_ID) {
            control.releaseCoordinatorLease(lease.coordinatorId(), lease.generation());
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "local-first coordinator id namespace exhausted", false, null);
        }
        this.refresher = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-local-first-coordinator-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshIntervalMillis,
                refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Read fence Guarantee that snapshot registration precedes renewal of lease to increase the oldest active lower bounds.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    BeginReservation begin(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        for (int attempt = 0; attempt < 2; attempt++) {
            boolean renewNeeded = false;
            membership.readLock().lock();
            try {
                requireAvailable();
                CoordinatorSnapshotLease current = lease;
                CoordinatorSnapshotView view;
                try {
                    view = control.snapshotUnderCoordinatorLease(current.coordinatorId(),
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
                membership.readLock().unlock();
            }
            refresh();
        }
        throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                "local-first coordinator lease cannot cover a new transaction", false, null);
    }

    /**
     * Review shard control library durable algebra and database deadline when required.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    void assertProtected(final BeginReservation tx) {
        membership.readLock().lock();
        try {
            requireAvailable();
            Long readTs = activeReadTs.get(tx.txId());
            if (readTs == null || readTs != tx.startTs()) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "local-first transaction lost coordinator snapshot membership", false, tx.txId());
            }
            CoordinatorSnapshotLease current = lease;
            control.assertCoordinatorSnapshot(current.coordinatorId(), current.generation(),
                    tx.startTs(), tx.deadlineMillis(), tx.txId());
        } finally {
            membership.readLock().unlock();
        }
    }

    /**
     * When transaction is finished, only local members will be deleted and individual transaction will no longer write control snapshot tables.
     *
     * @param txId transaction identifier
     */
    void release(final long txId) {
        activeReadTs.remove(txId);
    }

    /**
     * The segment is scheduled to remain with the exclusionary member fence to read the oldest active snapshot and prevent omissions from being begin's old RR.
     *
     * @return the operation result
     */
    long floorForReservation() {
        membership.writeLock().lock();
        try {
            requireAvailable();
            return activeReadTs.values().stream().mapToLong(Long::longValue).min()
                    .orElseGet(control::currentPublished);
        } finally {
            membership.writeLock().unlock();
        }
    }

    /**
     * Discretion local fence moves the lower durable bounds to the oldest active reading watermark.
     *
     * @throws MvccException if the operation cannot complete
     */
    void refresh() {
        membership.writeLock().lock();
        try {
            requireAvailable();
            long oldest = activeReadTs.values().stream().mapToLong(Long::longValue).min()
                    .orElseGet(control::currentPublished);
            CoordinatorSnapshotLease current = lease;
            if (oldest < current.readTs()) {
                fenced = true;
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local-first coordinator snapshot floor would move backward", false, null);
            }
            try {
                lease = control.renewCoordinatorLease(current.coordinatorId(),
                        current.generation(), oldest, leaseMillis);
            } catch (final MvccException ex) {
                if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                    fenced = true;
                }
                throw ex;
            }
        } finally {
            membership.writeLock().unlock();
        }
    }

    /**
     * Returns the latest background renewal failed to isolate benchmark and stop falsely claiming success.
     *
     * @return the operation result
     */
    RuntimeException lastRefreshFailure() {
        return lastFailure.get();
    }

    /**
     * Normal close only removes coordinator lease when local is not active snapshot.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        refresher.shutdownNow();
        membership.writeLock().lock();
        try {
            if (activeReadTs.isEmpty() && !fenced) {
                control.releaseCoordinatorLease(lease.coordinatorId(), lease.generation());
            }
        } finally {
            membership.writeLock().unlock();
        }
    }

    /**
     * Coordinator durable number and local monotonous sequence make-up of negative transaction ID that does not collide.
     *
     * @param coordinatorId coordinator id
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private long nextTransactionId(final long coordinatorId) {
        long next = sequence.incrementAndGet();
        if (next > MAX_LOCAL_SEQUENCE) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "local-first coordinator transaction ids exhausted", false, null);
        }
        return Long.MIN_VALUE | (coordinatorId << 43) | next;
    }

    /**
     * The coordinator after losing the current durable algebra does not start again or verify transaction.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireAvailable() {
        if (closed.get() || fenced) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "local-first coordinator lease is closed or fenced", false, null);
        }
    }

    /**
     * Background failed to leave a certificate and renew the retry lease for the next round, without silent loss of protection.
     */
    private void refreshSafely() {
        try {
            refresh();
        } catch (final RuntimeException ex) {
            lastFailure.set(ex);
        }
    }
}

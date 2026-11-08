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
import com.flyingzc.azir.common.MvccException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * A coordination process shares an durable lease protection of all RR transaction and in transit RC statements.
 */
final class MysqlVectorReaderProtectionRuntime implements AutoCloseable {

    private final MysqlVectorSnapshotLeaseStore leases;

    private final Supplier<MysqlStripedSnapshotOracle.Snapshot> publishedCut;

    private final String ownerId;

    private final long leaseMillis;

    private final long refreshMillis;

    private final int maxReaders;

    private final Map<UUID, MysqlStripedSnapshotOracle.Snapshot> active = new HashMap<>();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private MysqlVectorSnapshotLeaseStore.Guard guard;

    private MysqlStripedSnapshotOracle.Snapshot protectedFloor;

    private Thread worker;

    private boolean serving;

    private boolean closed;

    private boolean releaseAfterReaders;

    /**
     * The publisher only provides confirmed cut-ups, reader lease aggregates by process rather than writing centres.
     *
     * @param leases leases
     * @param publishedCut published cut
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @param refreshMillis refresh millis
     * @param maxReaders max readers
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorReaderProtectionRuntime(final MysqlVectorSnapshotLeaseStore leases,
                                       final Supplier<MysqlStripedSnapshotOracle.Snapshot> publishedCut,
                                       final String ownerId, final long leaseMillis, final long refreshMillis,
                                       final int maxReaders) {
        this.leases = Objects.requireNonNull(leases, "leases");
        this.publishedCut = Objects.requireNonNull(publishedCut, "publishedCut");
        this.ownerId = Objects.requireNonNull(ownerId, "ownerId");
        if (leaseMillis < 200 || refreshMillis < 1 || refreshMillis >= leaseMillis / 2
                || maxReaders < 1 || maxReaders > 1_000_000) {
            throw new IllegalArgumentException("invalid vector reader runtime limits");
        }
        this.leaseMillis = leaseMillis;
        this.refreshMillis = refreshMillis;
        this.maxReaders = maxReaders;
    }

    /**
     * Durable first protects the current cut, then allows this process to start any RR or RC reading.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    synchronized void start() {
        if (closed || worker != null) {
            throw new IllegalStateException("vector reader runtime already started or closed");
        }
        MysqlStripedSnapshotOracle.Snapshot initial = publishedCut.get();
        guard = leases.acquire(ownerId, initial, leaseMillis);
        protectedFloor = initial;
        serving = true;
        try {
            worker = Thread.ofVirtual().name("azir-vector-reader-protection").start(this::runLoop);
        } catch (final RuntimeException | Error ex) {
            failClosed(ex);
            guard.close();
            throw ex;
        }
    }

    /**
     * RR holds this handle until the end of transaction, and RC holds it until the current statement is finished.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    synchronized ReadHandle beginRead() {
        requireServing();
        if (active.size() >= maxReaders) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "vector reader protection capacity exhausted", true, null);
        }
        try {
            MysqlStripedSnapshotOracle.Snapshot next = publishedCut.get();
            leases.requireAtLeast(next, protectedFloor);
            UUID id = UUID.randomUUID();
            active.put(id, next);
            try {
                requireServing();
                return new ReadHandle(id, next);
            } catch (final RuntimeException ex) {
                active.remove(id);
                throw ex;
            }
        } catch (final RuntimeException | Error ex) {
            failClosed(ex);
            throw ex;
        }
    }

    /**
     * The cycle renewal puts the oldest aspects of the process still held at the centre so that GC can move forward safely.
     */
    synchronized void refresh() {
        requireServing();
        try {
            MysqlStripedSnapshotOracle.Snapshot next = publishedCut.get();
            for (MysqlStripedSnapshotOracle.Snapshot held : active.values()) {
                next = leases.minimum(next, held);
            }
            leases.requireAtLeast(next, protectedFloor);
            guard.renew(next);
            protectedFloor = next;
        } catch (final RuntimeException | Error ex) {
            failClosed(ex);
            throw ex;
        }
    }

    /**
     * All reading handles are stopped for successful return as soon as the daemon is lost or lease renewal fails.
     */
    private void runLoop() {
        while (true) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(refreshMillis));
            synchronized (this) {
                if (!serving) {
                    return;
                }
                if (Thread.interrupted()) {
                    failClosed(new InterruptedException("vector reader renewer interrupted"));
                    return;
                }
            }
            try {
                refresh();
            } catch (final Throwable ex) {
                synchronized (this) {
                    if (serving) {
                        failClosed(ex);
                    }
                }
                return;
            }
        }
    }

    /**
     * The first background failure exposure does not equate linear survival to lease effectiveness.
     *
     * @return the operation result
     */
    synchronized Throwable failure() {
        return failure.get();
    }

    /**
     * Local services can only be delivered if they are valid at the same time as database lease.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireServing() {
        if (!serving || closed || guard == null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "vector reader protection is not serving", true, null);
        }
        guard.requireActive();
    }

    /**
     * The local eligibility is revoked after expiry and the centre line continues to protect on-line snapshot before its natural maturity.
     *
     * @param cause cause
     */
    private void failClosed(final Throwable cause) {
        failure.compareAndSet(null, cause);
        serving = false;
        if (guard != null) {
            guard.revokeLocally();
        }
    }

    /**
     * Do not open new readings after closing; do not release durable protection early until the handle does not exit.
     */
    @Override
    public void close() {
        Thread running;
        MysqlVectorSnapshotLeaseStore.Guard toRelease = null;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            serving = false;
            if (guard != null) {
                guard.revokeLocally();
                if (active.isEmpty()) {
                    toRelease = guard;
                } else {
                    releaseAfterReaders = true;
                }
            }
            running = worker;
        }
        if (running != null) {
            running.interrupt();
            try {
                running.join(5_000);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        if (toRelease != null) {
            toRelease.close();
        }
    }

    /**
     * Fixed read snapshot held by a caller needs to check the lease process before and after a successful return.
     */
    final class ReadHandle implements AutoCloseable {

        private final UUID id;

        private final MysqlStripedSnapshotOracle.Snapshot snapshot;

        private boolean released;

        /**
         * The handle will only be protected by the process to register successfully and return.
         *
         * @param id id
         * @param snapshot snapshot
         */
        private ReadHandle(final UUID id, final MysqlStripedSnapshotOracle.Snapshot snapshot) {
            this.id = id;
            this.snapshot = snapshot;
        }

        /**
         * Take the same RC statement or RR transaction vector before and after reading.
         *
         * @return the operation result
         */
        MysqlStripedSnapshotOracle.Snapshot snapshot() {
            requireActive();
            return snapshot;
        }

        /**
         * The leader or reader lease shall not return the old data as a successful result after it has lapsed.
         *
         * @throws MvccException if the operation cannot complete
         */
        void requireActive() {
            synchronized (MysqlVectorReaderProtectionRuntime.this) {
                if (released || !active.containsKey(id)) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "vector read handle was released", true, null);
                }
                requireServing();
            }
        }

        /**
         * The last old reader exit can release the closed process durable lease immediately.
         */
        @Override
        public void close() {
            MysqlVectorSnapshotLeaseStore.Guard toRelease = null;
            synchronized (MysqlVectorReaderProtectionRuntime.this) {
                if (released) {
                    return;
                }
                released = true;
                active.remove(id);
                if (closed && releaseAfterReaders && active.isEmpty()) {
                    releaseAfterReaders = false;
                    toRelease = guard;
                }
            }
            if (toRelease != null) {
                toRelease.close();
            }
        }
    }
}

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

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * Single leader background renewals and release experiments are immediately closed for reading and ACK at failure.
 */
final class MysqlVectorCutLeaderRuntime implements AutoCloseable {

    private final DataSource center;

    private final Map<String, DataSource> shards;

    private final int stripes;

    private final LongSupplier decisionEpoch;

    private final MysqlVectorCutLeaderLease leases;

    private final String ownerId;

    private final long leaseMillis;

    private final long publishIntervalNanos;

    private final boolean productState;

    private final AtomicBoolean active = new AtomicBoolean();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private volatile MysqlVectorCutLeaderLease.LeaseGuard guard;

    private volatile MysqlPublishedVectorCut cut;

    private volatile Thread worker;

    private volatile boolean closed;

    /**
     * Staples fixed shard scale, durable lease and background distribution cycles.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @param leases leases
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @param publishIntervalMicros publish interval micros
     */
    MysqlVectorCutLeaderRuntime(final DataSource center, final Map<String, DataSource> shards, final int stripes,
                                final LongSupplier decisionEpoch, final MysqlVectorCutLeaderLease leases,
                                final String ownerId, final long leaseMillis, final long publishIntervalMicros) {
        this(center, shards, stripes, decisionEpoch, leases,
                ownerId, leaseMillis, publishIntervalMicros, false);
    }

    /**
     * Formal participant Runtime read-only takes a true tabulation and must bind the official centre leader to lease.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @param leases leases
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @param publishIntervalMicros publish interval micros
     * @return the operation result
     */
    static MysqlVectorCutLeaderRuntime forProduct(final DataSource center,
                                                  final Map<String, DataSource> shards, final int stripes,
                                                  final LongSupplier decisionEpoch,
                                                  final MysqlVectorCutLeaderLease leases,
                                                  final String ownerId, final long leaseMillis,
                                                  final long publishIntervalMicros) {
        return new MysqlVectorCutLeaderRuntime(center, shards, stripes, decisionEpoch,
                leases, ownerId, leaseMillis, publishIntervalMicros, true);
    }

    /**
     * Two fixed watermark sources share the lease renewal, failure closure and client response logic.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @param leases leases
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @param publishIntervalMicros publish interval micros
     * @param productState product state
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private MysqlVectorCutLeaderRuntime(final DataSource center, final Map<String, DataSource> shards, final int stripes,
                                        final LongSupplier decisionEpoch, final MysqlVectorCutLeaderLease leases,
                                        final String ownerId, final long leaseMillis, final long publishIntervalMicros,
                                        final boolean productState) {
        this.center = Objects.requireNonNull(center, "center");
        this.shards = Map.copyOf(shards);
        this.stripes = stripes;
        this.decisionEpoch = Objects.requireNonNull(decisionEpoch, "decisionEpoch");
        this.leases = Objects.requireNonNull(leases, "leases");
        if (leases.productMode() != productState) {
            throw new IllegalArgumentException("vector publisher source and lease namespace differ");
        }
        this.ownerId = Objects.requireNonNull(ownerId, "ownerId");
        if (leaseMillis < 200 || leaseMillis > 60_000
                || publishIntervalMicros < 50 || publishIntervalMicros > 100_000) {
            throw new IllegalArgumentException("invalid vector publisher runtime limits");
        }
        this.leaseMillis = leaseMillis;
        this.publishIntervalNanos = TimeUnit.MICROSECONDS.toNanos(publishIntervalMicros);
        this.productState = productState;
    }

    /**
     * GC Scheduler read-only Actual foreground Control Library connection identity.
     *
     * @return the operation result
     */
    DataSource centerDataSource() {
        return center;
    }

    /**
     * Background shard must correspond to the current publisher 's fixed route.
     *
     * @return the operation result
     */
    Map<String, DataSource> shardSources() {
        return shards;
    }

    /**
     * Only official vector data sources can drive the product local version GC.
     *
     * @return the operation result
     */
    boolean productMode() {
        return productState;
    }

    /**
     * Reader lease and the publisher must use the same shard collection and splits.
     *
     * @return the operation result
     */
    String layoutHash() {
        return MysqlPublishedVectorCut.layoutFingerprint(
                shards.keySet().stream().sorted().toList(), stripes);
    }

    /**
     * The collection segment number must be consistent with the publisher 's durable layout.
     *
     * @return the operation result
     */
    int stripeCount() {
        return stripes;
    }

    /**
     * A central algebra is obtained before re-establishing the cut; returns false when you have a leader and does not start the thread.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    synchronized boolean start() {
        if (closed || worker != null) {
            throw new IllegalStateException("vector publisher runtime was already started or closed");
        }
        Optional<MysqlVectorCutLeaderLease.LeaseGuard> acquired = leases.acquire(ownerId, leaseMillis);
        if (acquired.isEmpty()) {
            return false;
        }
        guard = acquired.get();
        try {
            cut = productState
                    ? MysqlPublishedVectorCut.leasedProduct(center, shards, stripes, decisionEpoch, guard)
                    : MysqlPublishedVectorCut.leasedVolatile(center, shards, stripes, decisionEpoch, guard);
            cut.initializeVolatile();
            active.set(true);
            worker = Thread.ofVirtual().name("azir-vector-cut-leader-" + guard.generation())
                    .start(this::runLoop);
            return true;
        } catch (final RuntimeException | Error ex) {
            failClosed(ex);
            closed = true;
            if (cut != null) {
                cut.close();
            }
            throw ex;
        }
    }

    /**
     * The background lease is renewed at the local lease surplus, and any failure to issue or renew a lease cancels this algebra.
     *
     * @throws InterruptedException if the operation cannot complete
     */
    private void runLoop() {
        try {
            long renewalBoundary = TimeUnit.MILLISECONDS.toNanos(leaseMillis / 2);
            while (active.get()) {
                guard.requireActive();
                if (guard.remainingNanos() <= renewalBoundary) {
                    guard.renew();
                }
                cut.publishOnce();
                LockSupport.parkNanos(publishIntervalNanos);
                if (Thread.interrupted() && active.get()) {
                    throw new InterruptedException("vector publisher thread was interrupted");
                }
            }
        } catch (final Throwable ex) {
            if (active.get()) {
                failClosed(ex);
            }
        }
    }

    /**
     * Foreground verify is still available at the current running time for each RC cut.
     *
     * @return the operation result
     */
    PublishedView currentView() {
        requireServing();
        MysqlStripedSnapshotOracle.Snapshot snapshot = cut.currentCut();
        requireServing();
        return new PublishedView(guard.generation(), snapshot);
    }

    /**
     * Transaction of the completed physical commit can only be ACK after its own serial number has been published by the current algebra.
     *
     * @param shardId logical shard identifier
     * @param stripe stripe
     * @param sequence sequence
     * @param minimumDecisionEpoch minimum decision epoch
     * @param timeoutMillis timeout millis
     */
    void awaitPublished(final String shardId, final int stripe, final long sequence,
                        final long minimumDecisionEpoch, final long timeoutMillis) {
        requireServing();
        cut.awaitLocallyPublished(shardId, stripe, sequence, minimumDecisionEpoch, timeoutMillis);
        requireServing();
    }

    /**
     * Only returns to a proven state of operation and does not equate the survival of old threads with continued service eligibility.
     *
     * @return the operation result
     */
    boolean isServing() {
        if (!active.get() || guard == null) {
            return false;
        }
        try {
            guard.requireActive();
            return true;
        } catch (final MvccException ex) {
            return false;
        }
    }

    /**
     * Releases evidence of failure for testing and running surveillance reading.
     *
     * @return the operation result
     */
    Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    /**
     * Declines the posting of a cut-off network portal when it is disabled and prohibits only local ACK, while no new side can be read at a remote end.
     *
     * @param cause cause
     */
    void endpointUnavailable(final Throwable cause) {
        Objects.requireNonNull(cause, "cause");
        if (active.get()) {
            failClosed(cause);
        }
    }

    /**
     * Non-current algebra, background failure or visible closes all refuse to read and confirm successfully.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void requireServing() {
        if (!active.get() || guard == null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "vector publisher is not serving", true, null);
        }
        guard.requireActive();
    }

    /**
     * Errors are recorded only once and the local qualification is revoked before the waiting person is awakened.
     *
     * @param cause cause
     */
    private void failClosed(final Throwable cause) {
        failure.compareAndSet(null, cause);
        active.set(false);
        if (guard != null) {
            guard.revokeLocally();
        }
        if (cut != null) {
            cut.wakeWaiters();
        }
    }

    /**
     * Stop local reading with ACK, pending the release of threads; centre lease automatically expires before taking over.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        active.set(false);
        if (guard != null) {
            guard.revokeLocally();
        }
        if (cut != null) {
            cut.wakeWaiters();
        }
        Thread running = worker;
        if (running != null) {
            running.interrupt();
            try {
                running.join(5_000);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        if (cut != null) {
            cut.close();
        }
    }

    /**
     * Returns to reader tangents must carry the leader algebra for external routers to reject old algebra responses.
     *
     * @param generation generation
     * @param snapshot snapshot
     */
    record PublishedView(long generation, MysqlStripedSnapshotOracle.Snapshot snapshot) {

        /**
         * Successful responses to algebras or missing cut-off are not allowed.
         *
         * @param generation generation
         * @param snapshot snapshot
         * @throws IllegalArgumentException if the operation cannot complete
         */
        PublishedView {
            Objects.requireNonNull(snapshot, "snapshot");
            if (generation < 1) {
                throw new IllegalArgumentException("invalid vector leader generation");
            }
        }
    }
}

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

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.storage.Mutation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Cross-shard capacity probe parallel follow-up writing and security GC, real business ACK subject to bounded queue back pressure.
 */
final class MysqlVectorCrossSteadyBackground implements AutoCloseable {

    private final MysqlLocalFirstPublicationLedger ledger;

    private final MysqlStorageAdapter left;

    private final MysqlStorageAdapter right;

    private final HikariDataSource leftData;

    private final HikariDataSource rightData;

    private final HikariDataSource backgroundCenter;

    private final HikariDataSource backgroundLeft;

    private final HikariDataSource backgroundRight;

    private final MysqlVectorLocalGcScheduler gc;

    private final ArrayBlockingQueue<SuccessorWork> queue = new ArrayBlockingQueue<>(4_096);

    private final ExecutorService successors = Executors.newFixedThreadPool(16,
            task -> Thread.ofPlatform().daemon().name("azir-vector-successor").unstarted(task));

    private final AtomicBoolean accepting = new AtomicBoolean(true);

    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

    private final AtomicLong nextLocalId = new AtomicLong(-1);

    private final LongAdder submitted = new LongAdder();

    private final LongAdder completed = new LongAdder();

    /**
     * An independent background pool bound to the same control and two pieces of authority with parameters limiting the resources of materialization follow-up and collection budget.
     *
     * @param ledger ledger
     * @param left left
     * @param right right
     * @param center center
     * @param leftData left data
     * @param rightData right data
     * @param publisher publisher
     * @param controlUrl control url
     * @param leftUrl left url
     * @param rightUrl right url
     * @param user user
     * @param password password
     * @param stripes stripes
     * @param gcParallelism gc parallelism
     * @param gcRetireBatch gc retire batch
     * @param gcCandidateLimit gc candidate limit
     * @param localEveryRounds local every rounds
     * @param gcIntervalMillis gc interval millis
     * @throws IllegalStateException if the operation cannot complete
     */
    MysqlVectorCrossSteadyBackground(final MysqlLocalFirstPublicationLedger ledger,
                                     final MysqlStorageAdapter left, final MysqlStorageAdapter right,
                                     final HikariDataSource center, final HikariDataSource leftData,
                                     final HikariDataSource rightData,
                                     final MysqlVectorCutLeaderRuntime publisher,
                                     final String controlUrl, final String leftUrl, final String rightUrl,
                                     final String user, final String password, final int stripes,
                                     final int gcParallelism, final int gcRetireBatch,
                                     final int gcCandidateLimit, final int localEveryRounds,
                                     final long gcIntervalMillis) {
        this.ledger = ledger;
        this.left = left;
        this.right = right;
        this.leftData = leftData;
        this.rightData = rightData;
        backgroundCenter = pool(controlUrl, user, password);
        backgroundLeft = pool(leftUrl, user, password);
        backgroundRight = pool(rightUrl, user, password);
        var protection = new MysqlVectorSnapshotLeaseStore(backgroundCenter,
                List.of("s1", "s2"), stripes);
        protection.initializeProduct();
        var leftLocal = new MysqlVectorLocalGarbageCollector(backgroundLeft,
                "s1", stripes, protection);
        var rightLocal = new MysqlVectorLocalGarbageCollector(backgroundRight,
                "s2", stripes, protection);
        leftLocal.initializeProduct();
        rightLocal.initializeProduct();
        var leftCross = new MysqlVectorCrossGarbageCollector(left, backgroundLeft,
                "s1", stripes, protection, ledger, gcParallelism, gcRetireBatch);
        var rightCross = new MysqlVectorCrossGarbageCollector(right, backgroundRight,
                "s2", stripes, protection, ledger, gcParallelism, gcRetireBatch);
        leftCross.initializeProduct();
        rightCross.initializeProduct();
        gc = new MysqlVectorLocalGcScheduler(publisher, protection,
                Map.of("s1", leftLocal, "s2", rightLocal),
                Map.of("s1", leftCross, "s2", rightCross),
                Math.min(256, gcCandidateLimit), gcCandidateLimit,
                localEveryRounds, gcIntervalMillis);
        if (center == backgroundCenter || leftData == backgroundLeft || rightData == backgroundRight) {
            throw new IllegalStateException("steady cross background reused foreground pools");
        }
    }

    /**
     * Recovery Thread and Declan Launcher ready to start the bounded follow-up and GC jobs.
     */
    void start() {
        gc.start();
        for (int index = 0; index < 16; index++) {
            successors.submit(this::runSuccessors);
        }
    }

    /**
     * Business ACK places each subsequent peer update in the bounded queue, full of which gives the foreground natural backpressure.
     *
     * @param txId transaction identifier
     * @param leftProof left proof
     * @param rightProof right proof
     * @throws IllegalStateException if the operation cannot complete
     * @throws InterruptedException if the operation cannot complete
     */
    void enqueue(final long txId, final MysqlStorageAdapter.PrewriteProof leftProof,
                 final MysqlStorageAdapter.PrewriteProof rightProof) throws InterruptedException {
        requireHealthy();
        var leftReservation = leftProof.vectorReservation().orElseThrow();
        var rightReservation = rightProof.vectorReservation().orElseThrow();
        SuccessorWork work = new SuccessorWork(txId, nextLocalId.getAndDecrement(),
                leftReservation.key(), leftReservation.stripe(), leftReservation.reservedSequence(),
                rightReservation.key(), rightReservation.stripe(), rightReservation.reservedSequence());
        if (!queue.offer(work, 3, TimeUnit.SECONDS)) {
            throw new IllegalStateException("steady cross successor queue exhausted");
        }
        submitted.increment();
        requireHealthy();
    }

    /**
     * Each follow-up must wait for the centre to confirm the finalization of all participating films, and pending should not cover intent.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    private void runSuccessors() {
        try {
            while (accepting.get() || !queue.isEmpty()) {
                SuccessorWork work = queue.poll(100, TimeUnit.MILLISECONDS);
                if (work == null) {
                    continue;
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (ledger.finalizedVectorProof(work.txId()).isEmpty()) {
                    if (System.nanoTime() >= deadline) {
                        throw new IllegalStateException("steady cross successor waited too long for finalization");
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                }
                commitSuccessorWithRetry(left, work.localTxId(), work.leftKey(),
                        work.leftStripe(), work.leftSequence());
                commitSuccessorWithRetry(right, work.localTxId(), work.rightKey(),
                        work.rightStripe(), work.rightSequence());
                completed.increment();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            if (accepting.get()) {
                firstFailure.compareAndSet(null, ex);
            }
        } catch (final RuntimeException ex) {
            firstFailure.compareAndSet(null, ex);
            ex.printStackTrace(System.err);
        }
    }

    /**
     * The unknown response presses the same negative transaction ID accuracy replay, storage process will confirm an existing version or reset commit.
     *
     * @param adapter adapter
     * @param txId transaction identifier
     * @param key business key
     * @param stripe stripe
     * @param startSequence start sequence
     */
    private static void commitSuccessorWithRetry(final MysqlStorageAdapter adapter, final long txId,
                                                 final String key, final int stripe, final long startSequence) {
        Mutation mutation = Mutation.put(key, new byte[]{42});
        for (int attempt = 0;; attempt++) {
            try {
                adapter.directCommitVectorLocal(txId, mutation, stripe, startSequence);
                return;
            } catch (final MvccException ex) {
                if (attempt >= 7 || ex.errorCode() != ErrorCode.TRANSACTION_STATUS_UNKNOWN
                        && ex.errorCode() != ErrorCode.STORAGE_UNAVAILABLE
                        && ex.errorCode() != ErrorCode.LOCK_CONFLICT) {
                    throw ex;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(Math.min(32, 1L << attempt)));
            }
        }
    }

    /**
     * Confirmation of background without failure cannot be used as a zero backlog before regular reporting.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    Snapshot snapshot() throws SQLException {
        Throwable failure = firstFailure.get();
        if (failure == null && gc.stats().failureCount() != 0) {
            failure = gc.lastFailure();
        }
        return new Snapshot(count(leftData, "mvcc_vector_committed_v1"),
                count(rightData, "mvcc_vector_committed_v1"),
                count(leftData, "mvcc_vector_retired_v1"),
                count(rightData, "mvcc_vector_retired_v1"),
                submitted.sum(), completed.sum(), gc.stats().runCount(),
                gc.stats().failureCount(), gc.crossReclaimed(), failure);
    }

    /**
     * After the foreground waits for the bounded queue to be emptied, failure or timeout invalidates pressure measurement evidence.
     *
     * @param timeout timeout
     * @throws IllegalStateException if the operation cannot complete
     * @throws InterruptedException if the operation cannot complete
     */
    void awaitSuccessors(final Duration timeout) throws InterruptedException {
        accepting.set(false);
        successors.shutdown();
        if (!successors.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            successors.shutdownNow();
            throw new IllegalStateException("steady cross successors did not drain");
        }
        requireHealthy();
        if (completed.sum() != submitted.sum()) {
            throw new IllegalStateException("steady cross successor count differs from ACK queue");
        }
    }

    /**
     * Following completion of writing, the timeout returns to non-zero backlog for production judgement, pending clearance of old maps.
     *
     * @param timeout timeout
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    Snapshot awaitGcDrain(final Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Snapshot current;
        do {
            requireHealthy();
            current = snapshot();
            if (current.leftMappings() == 0 && current.rightMappings() == 0) {
                return current;
            }
            Thread.sleep(200);
        } while (System.nanoTime() < deadline);
        return current;
    }

    /**
     * The first follow-up or GC failure prevents operations from mistaking the unbounded backlog as a success ACK.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    void requireHealthy() {
        Throwable failure = firstFailure.get();
        if (failure == null && gc.stats().failureCount() != 0) {
            failure = gc.lastFailure();
        }
        if (failure != null) {
            throw new IllegalStateException("steady cross background failed", failure);
        }
    }

    /**
     * Releases the trunking, GC workspace and three stand-alone background pools.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public void close() {
        accepting.set(false);
        successors.shutdown();
        try {
            if (!successors.awaitTermination(5, TimeUnit.SECONDS)) {
                successors.shutdownNow();
            }
            gc.close();
        } catch (final InterruptedException ex) {
            successors.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing steady cross background", ex);
        } finally {
            backgroundCenter.close();
            backgroundLeft.close();
            backgroundRight.close();
        }
    }

    /**
     * GC background pool and connection budget of the operations pool are separate.
     *
     * @param url url
     * @param user user
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource pool(final String url, final String user, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, user, password);
    }

    /**
     * Table names only accept this probe fixed version and receipt tables.
     *
     * @param dataSource database connection source
     * @param table table
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static long count(final HikariDataSource dataSource, final String table) throws SQLException {
        if (!List.of("mvcc_vector_committed_v1", "mvcc_vector_retired_v1").contains(table)) {
            throw new IllegalArgumentException("unsupported steady cross count table");
        }
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement("select count(*) from " + table);
                ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("steady cross count returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Queue to save the original cross-shard subsection identity, followed by writing only after two pieces have been finalized.
     *
     * @param txId transaction identifier
     * @param localTxId local tx id
     * @param leftKey left key
     * @param leftStripe left stripe
     * @param leftSequence left sequence
     * @param rightKey right key
     * @param rightStripe right stripe
     * @param rightSequence right sequence
     */
    private record SuccessorWork(long txId, long localTxId,
            String leftKey, int leftStripe, long leftSequence,
            String rightKey, int rightStripe, long rightSequence) {
    }

    /**
     * The background status also records the remaining large version, durable receipt, the backlog and GC failure.
     *
     * @param leftMappings left mappings
     * @param rightMappings right mappings
     * @param leftReceipts left receipts
     * @param rightReceipts right receipts
     * @param submitted submitted
     * @param completed completed
     * @param gcRuns gc runs
     * @param gcFailures gc failures
     * @param gcRetired gc retired
     * @param firstFailure first failure
     */
    record Snapshot(long leftMappings, long rightMappings, long leftReceipts, long rightReceipts,
            long submitted, long completed, long gcRuns, long gcFailures,
            long gcRetired, Throwable firstFailure) {

        /**
         * Any negative count indicates that the observations are invalid.
         *
         * @param leftMappings left mappings
         * @param rightMappings right mappings
         * @param leftReceipts left receipts
         * @param rightReceipts right receipts
         * @param submitted submitted
         * @param completed completed
         * @param gcRuns gc runs
         * @param gcFailures gc failures
         * @param gcRetired gc retired
         * @param firstFailure first failure
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Snapshot {
            if (leftMappings < 0 || rightMappings < 0 || leftReceipts < 0 || rightReceipts < 0
                    || submitted < 0 || completed < 0 || gcRuns < 0 || gcFailures < 0
                    || gcRetired < 0) {
                throw new IllegalArgumentException("invalid steady cross probe snapshot");
            }
        }
    }
}

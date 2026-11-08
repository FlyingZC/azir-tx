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
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.publication.LocalFirstPublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.SegmentReservation;
import com.flyingzc.azir.storage.Mutation;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The local is measured in the isolation library first commit, the batch center terminal state and the complete shard writing path that is released consecutively.
 */
public final class MysqlLocalFirstProtocolBenchmark {

    /**
     * The benchmark entry does not save variable instance status.
     */
    private MysqlLocalFirstProtocolBenchmark() {
    }

    /**
     * Initializes two libraries only with a visible segregation reset, and exports confirmed throughput and tail delay.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("local-first protocol benchmark requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "20"));
        int segmentSize = Integer.parseInt(env("AZIR_TX_SEGMENT_SIZE", "4096"));
        int batchWorkers = Integer.parseInt(env("AZIR_TX_LOCAL_FIRST_BATCH_WORKERS", "4"));
        long transactionTimeoutMillis = Long.parseLong(env("AZIR_TX_LOCAL_FIRST_TX_TIMEOUT_MS", "60000"));
        long retentionMillis = Long.parseLong(env("AZIR_TX_LOCAL_FIRST_RETENTION_MS", "60000"));
        long gatherMicros = Long.parseLong(env("AZIR_TX_LOCAL_FIRST_GATHER_MICROS", "0"));
        boolean coordinatorSnapshots = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_COORDINATOR_SNAPSHOTS", "false"));
        boolean verifyEachOp = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_VERIFY_EACH_OP", "true"));
        boolean fastPublish = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_FAST_PUBLISH", "true"));
        boolean fusedPublish = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_FUSED_PUBLISH", "true"));
        boolean frontProof = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_FRONT_PROOF", "true"));
        boolean singleKeyProcedureV3 = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_PROC_V3", "false"));
        boolean ownedSegments = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_OWNER_REGISTRY", "false"));
        boolean segmentOnly = Boolean.parseBoolean(env("AZIR_TX_LOCAL_FIRST_SEGMENT_ONLY", "false"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 600
                || segmentSize < 1 || segmentSize > 4096 || batchWorkers < 1 || batchWorkers > 8
                || gatherMicros < 0 || gatherMicros > 5_000
                || transactionTimeoutMillis < 1 || transactionTimeoutMillis >= 120_000
                || retentionMillis < 0 || retentionMillis > 86_400_000L
                || segmentOnly && !ownedSegments) {
            throw new IllegalArgumentException("invalid local-first protocol benchmark limits");
        }
        String controlUrl = env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        String shardUrl = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        try (
                HikariDataSource controlData = (HikariDataSource) MysqlDataSourceFactory.create(
                        controlUrl,
                        username, password);
                HikariDataSource shardData = (HikariDataSource) MysqlDataSourceFactory.create(
                        shardUrl, username, password);
                HikariDataSource backgroundControlData = (HikariDataSource) MysqlDataSourceFactory.createBackground(
                        controlUrl, username, password);
                HikariDataSource backgroundShardData = (HikariDataSource) MysqlDataSourceFactory.createBackground(
                        shardUrl, username, password)) {
            MysqlDatabaseAuthority.requireIndependentSameDatabase(controlData, backgroundControlData, "control");
            MysqlDatabaseAuthority.requireIndependentSameDatabase(shardData, backgroundShardData, "shard-1");
            reset(controlData, shardData);
            var control = new MysqlLocalFirstPublicationLedger(controlData);
            control.initializeIsolated(100);
            var shard = new MysqlStorageAdapter("shard-1", shardData);
            shard.initialize();
            var physical = new MysqlShardSegmentFence("shard-1", shardData);
            physical.initializeIsolated();
            if (singleKeyProcedureV3) {
                physical.enableSingleKeyProcedureV3();
            } else {
                physical.enableSingleKeyProcedure();
            }
            var backgroundControl = new MysqlLocalFirstPublicationLedger(backgroundControlData);
            backgroundControl.requireInitializedFastPath();
            var backgroundPhysical = new MysqlShardSegmentFence("shard-1", backgroundShardData);
            backgroundPhysical.requireInitializedFastPath();
            var backgroundShard = new MysqlStorageAdapter("shard-1", backgroundShardData);
            backgroundShard.requireInitializedFastPath();
            var allocator = new SegmentAllocator(control, physical, segmentSize);
            try (
                    var coordinator = coordinatorSnapshots
                            ? new LocalFirstCoordinatorSnapshots(control,
                                    transactionTimeoutMillis, 120_000, 1_000)
                            : null;
                    var maintenance = ownedSegments
                            ? new LocalFirstPublicationMaintenanceScheduler(backgroundControl,
                                    Map.of("shard-1", backgroundPhysical), Map.of("shard-1", backgroundShard),
                                    16, 16, 128, 100, 1_000, retentionMillis)
                            : null;
                    var owner = ownedSegments
                            ? new LocalFirstSegmentOwnerRegistry(control, backgroundControl,
                                    physical, backgroundPhysical,
                                    coordinator == null ? () -> 0 : coordinator::floorForReservation,
                                    segmentSize, 5_000, segmentOnly ? 100 : 1_000,
                                    segmentOnly ? 1 : 100)
                            : null;
                    var batcher = new CompletionBatcher(control, physical, batchWorkers,
                            gatherMicros, fastPublish, fusedPublish,
                            segmentOnly ? 10 : 100)) {
                if (maintenance != null) {
                    maintenance.start();
                }
                run(control, shard, physical, allocator, owner, batcher, coordinator,
                        verifyEachOp, frontProof, segmentOnly, singleKeyProcedureV3,
                        concurrency, durationSeconds, transactionTimeoutMillis);
                if (maintenance != null) {
                    System.out.printf("LOCAL_FIRST_MAINTENANCE recovery_runs=%d recovery_failures=%d "
                            + "sealed_segments=%d purged_candidates=%d retired_fences=%d "
                            + "gc_runs=%d gc_failures=%d "
                            + "recovery_error=%s gc_error=%s%n",
                            maintenance.recoveryStats().runCount(),
                            maintenance.recoveryStats().failureCount(), maintenance.sealedSegments(),
                            maintenance.purgedCandidates(), maintenance.retiredFences(),
                            maintenance.gcStats().runCount(),
                            maintenance.gcStats().failureCount(), maintenance.lastRecoveryFailure(),
                            maintenance.lastGcFailure());
                }
            }
        }
    }

    /**
     * Only the benchmark dedicated independent control version table and shard data are cleared.
     *
     * @param control control
     * @param shard logical shard identifier
     * @throws Exception if the operation cannot complete
     */
    private static void reset(final HikariDataSource control, final HikariDataSource shard) throws Exception {
        try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_first_slot_v1");
            statement.execute("drop table if exists mvcc_local_first_txn_v1");
            statement.execute("drop table if exists mvcc_local_first_segment_v1");
            statement.execute("drop table if exists mvcc_local_first_snapshot_v1");
            statement.execute("drop table if exists mvcc_local_first_coordinator_v1");
            statement.execute("drop table if exists mvcc_local_first_state_v1");
        }
        try (Connection connection = shard.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_segment_candidate_v1");
            statement.execute("drop table if exists mvcc_local_segment_fence_v1");
            statement.execute("drop table if exists mvcc_local_segment_state_v1");
        }
        new MysqlStorageAdapter("shard-1", shard).initialize();
        try (Connection connection = shard.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
        }
    }

    /**
     * Statistics begin, local physics transaction, center batch, release waiting and lease release total throughput.
     *
     * @param control control
     * @param shard logical shard identifier
     * @param physical physical
     * @param allocator allocator
     * @param owner owner
     * @param batcher batcher
     * @param coordinator coordinator
     * @param verifyEachOp verify each op
     * @param frontProof front proof
     * @param segmentOnly segment only
     * @param singleKeyProcedureV3 single key procedure v3
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param transactionTimeoutMillis transaction timeout millis
     * @throws Exception if the operation cannot complete
     */
    private static void run(final MysqlLocalFirstPublicationLedger control, final MysqlStorageAdapter shard,
                            final MysqlShardSegmentFence physical, final SegmentAllocator allocator,
                            final LocalFirstSegmentOwnerRegistry owner, final CompletionBatcher batcher,
                            final LocalFirstCoordinatorSnapshots coordinator,
                            final boolean verifyEachOp, final boolean frontProof, final boolean segmentOnly,
                            final boolean singleKeyProcedureV3,
                            final int concurrency, final int durationSeconds,
                            final long transactionTimeoutMillis) throws Exception {
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder beginNanos = new LongAdder();
        LongAdder localNanos = new LongAdder();
        LongAdder centerNanos = new LongAdder();
        LongAdder awaitNanos = new LongAdder();
        LongAdder releaseNanos = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                        while (System.nanoTime() < deadline) {
                            attempts.increment();
                            long started = System.nanoTime();
                            BeginReservation tx = null;
                            LocalFirstSegmentOwnerRegistry.Assignment owned = null;
                            try {
                                long stageStarted = System.nanoTime();
                                tx = coordinator == null
                                        ? control.begin(IsolationLevel.REPEATABLE_READ, transactionTimeoutMillis)
                                        : coordinator.begin(IsolationLevel.REPEATABLE_READ);
                                beginNanos.add(System.nanoTime() - stageStarted);
                                if (owner != null) {
                                    owned = owner.allocate(tx.startTs());
                                }
                                Assignment assigned = owned == null ? allocator.allocate()
                                        : new Assignment(owned.reservation(), owned.commitTs());
                                long commitTs = assigned.commitTs();
                                stageStarted = System.nanoTime();
                                if (coordinator != null && verifyEachOp) {
                                    coordinator.assertProtected(tx);
                                }
                                var writes = List.of(Mutation.put("local-first:" + commitTs,
                                        ("v" + commitTs).getBytes(StandardCharsets.UTF_8)));
                                MysqlStorageAdapter.LocalCommitProof proof = null;
                                // The A/B switch only affects the acquisition of foreground evidence, and the missing recovery always returns to the durable shard candidate.
                                if (frontProof) {
                                    proof = shard.directCommitFencedWithProof(physical, assigned.segment().lease(),
                                            tx.txId(), tx.startTs(), commitTs, tx.deadlineMillis(),
                                            IsolationLevel.REPEATABLE_READ, writes);
                                } else {
                                    shard.directCommitFenced(physical, assigned.segment().lease(), tx.txId(),
                                            tx.startTs(), commitTs, tx.deadlineMillis(),
                                            IsolationLevel.REPEATABLE_READ, writes);
                                }
                                localNanos.add(System.nanoTime() - stageStarted);
                                stageStarted = System.nanoTime();
                                if (segmentOnly) {
                                    owner.completePhysicalAndSeal(owned);
                                } else {
                                    batcher.complete(assigned, tx.txId(), proof);
                                }
                                centerNanos.add(System.nanoTime() - stageStarted);
                                stageStarted = System.nanoTime();
                                batcher.awaitPublished(commitTs, Math.min(10_000, transactionTimeoutMillis));
                                awaitNanos.add(System.nanoTime() - stageStarted);
                                if (owned != null && !segmentOnly) {
                                    owner.complete(owned);
                                }
                                committed.increment();
                                latencies.add(System.nanoTime() - started);
                            } catch (final RuntimeException ex) {
                                if (owned != null) {
                                    owner.unknown(owned);
                                }
                                failures.increment();
                                firstFailure.compareAndSet(null,
                                        ex.getClass().getSimpleName() + ":" + ex.getMessage());
                            } finally {
                                if (tx != null) {
                                    long releaseStarted = System.nanoTime();
                                    if (coordinator == null) {
                                        control.releaseSnapshot(tx.txId());
                                    } else {
                                        coordinator.release(tx.txId());
                                    }
                                    releaseNanos.add(System.nanoTime() - releaseStarted);
                                }
                            }
                        }
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            start.countDown();
            Thread reporter = Thread.ofVirtual().name("mvcc-local-first-window").start(() -> reportWindows(control, committed, failures));
            done.await();
            reporter.interrupt();
            reporter.join();
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
            double p99Millis = sorted.length == 0 ? Double.NaN
                    : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
            System.out.printf("LOCAL_FIRST_PROTOCOL_BENCH concurrency=%d duration_s=%.2f "
                    + "coordinator_snapshots=%s verify_each_op=%s owner_registry=%s "
                    + "front_proof=%s segment_only=%s local_proc_v3=%s "
                    + "attempts=%d committed=%d failures=%d commit_tps=%.2f p99_ms=%.2f "
                    + "first_failure=%s refresh_failure=%s %s%n",
                    concurrency, seconds, coordinator != null, verifyEachOp, owner != null,
                    frontProof, segmentOnly, singleKeyProcedureV3,
                    attempts.sum(), committed.sum(), failures.sum(),
                    committed.sum() / seconds, p99Millis,
                    firstFailure.get() == null ? "-" : firstFailure.get(),
                    coordinator == null ? "-" : coordinator.lastRefreshFailure(), batcher.stats());
            long successes = Math.max(1, committed.sum());
            System.out.printf("LOCAL_FIRST_STAGE begin_avg_ms=%.3f local_avg_ms=%.3f "
                    + "center_queue_avg_ms=%.3f await_avg_ms=%.3f release_avg_ms=%.3f "
                    + "segment_reservations=%d reserve_total_ms=%.2f%n",
                    beginNanos.sum() / 1_000_000.0 / successes,
                    localNanos.sum() / 1_000_000.0 / successes,
                    centerNanos.sum() / 1_000_000.0 / successes,
                    awaitNanos.sum() / 1_000_000.0 / successes,
                    releaseNanos.sum() / 1_000_000.0 / successes,
                    owner == null ? allocator.reservationSequence.get() : owner.reservationCount(),
                    allocator.reserveNanos.sum() / 1_000_000.0);
            if (owner != null) {
                System.out.printf("LOCAL_FIRST_OWNER refresh_runs=%d refresh_failures=%d last_failure=%s%n",
                        owner.refreshStats().runCount(), owner.refreshStats().failureCount(),
                        owner.lastFailure());
            }
        }
    }

    /**
     * The 10-second window also displays confirmation of throughput and durable release/safe watermark, recognizing GC degradation after running.
     *
     * @param control control
     * @param committed committed
     * @param failures failures
     */
    private static void reportWindows(final MysqlLocalFirstPublicationLedger control,
                                      final LongAdder committed, final LongAdder failures) {
        long previousCommitted = 0;
        long previousAt = System.nanoTime();
        int index = 0;
        while (true) {
            try {
                Thread.sleep(10_000);
                long now = System.nanoTime();
                long count = committed.sum();
                double seconds = (now - previousAt) / 1_000_000_000.0;
                System.out.printf("LOCAL_FIRST_WINDOW index=%d commit_tps=%.2f committed=%d "
                        + "failures=%d published=%d safe_point=%d%n",
                        ++index, (count - previousCommitted) / seconds, count,
                        failures.sum(), control.currentPublished(), control.currentSafePoint());
                previousCommitted = count;
                previousAt = now;
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            } catch (final RuntimeException ex) {
                System.out.println("LOCAL_FIRST_WINDOW_ERROR " + ex);
                return;
            }
        }
    }

    /**
     * Reads a visible or default non-empty configuration from an isolated benchmark environment.
     *
     * @param key business key
     * @param fallback fallback
     * @return the operation result
     */
    private static String env(final String key, final String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    /**
     * The same owner only leaves a new paragraph after the current paragraph has been fully numbered, and the lower GC boundary is zero-conservative to protect this capacity experiment.
     */
    static final class SegmentAllocator {

        private final MysqlLocalFirstPublicationLedger control;

        private final MysqlShardSegmentFence physical;

        private final int segmentSize;

        private final String ownerId = UUID.randomUUID().toString();

        private final ReentrantLock reserveLock = new ReentrantLock();

        private final AtomicLong reservationSequence = new AtomicLong();

        private final LongAdder reserveNanos = new LongAdder();

        private volatile SegmentState current;

        /**
         * Saves two library authority and bounded slots per segment.
         *
         * @param control control
         * @param physical physical
         * @param segmentSize segment size
         */
        SegmentAllocator(final MysqlLocalFirstPublicationLedger control,
                         final MysqlShardSegmentFence physical, final int segmentSize) {
            this.control = control;
            this.physical = physical;
            this.segmentSize = segmentSize;
        }

        /**
         * Quick Path local CAS take sign; durable completes control reservation, shard installation and center activation at exhaustion.
         *
         * @return the operation result
         */
        Assignment allocate() {
            while (true) {
                SegmentState state = current;
                if (state != null) {
                    long next = state.nextTs().get();
                    if (next <= state.reservation().lease().lastTs()
                            && state.nextTs().compareAndSet(next, next + 1)) {
                        return new Assignment(state.reservation(), next);
                    }
                }
                reserveLock.lock();
                try {
                    // Only retry is required for a CAS competition failure; only the current paragraph does run out to allow an advance.
                    if (current == state && (state == null
                            || state.nextTs().get() > state.reservation().lease().lastTs())) {
                        long reserveStarted = System.nanoTime();
                        String key = "capacity-" + reservationSequence.incrementAndGet();
                        SegmentReservation reserved = control.reserveSegment(physical.shardId(), ownerId,
                                key, segmentSize, 600_000, 0);
                        physical.install(reserved.lease(), 600_000);
                        SegmentReservation active = control.activateSegment(reserved, physical);
                        current = new SegmentState(active, new AtomicLong(active.lease().firstTs()));
                        reserveNanos.add(System.nanoTime() - reserveStarted);
                    }
                } finally {
                    reserveLock.unlock();
                }
            }
        }

        /**
         * An durable owner and a monotonous CAS time slot within the process.
         *
         * @param reservation reservation
         * @param nextTs next ts
         */
        private record SegmentState(SegmentReservation reservation, AtomicLong nextTs) {
        }
    }

    /**
     * A full control section and timestamp of the local candidate.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     */
    record Assignment(SegmentReservation segment, long commitTs) {
    }

    /**
     * The durable local candidate by paragraph is selected and the centre will attempt to publish a continuous prefix as soon as it completes.
     */
    private static final class CompletionBatcher implements AutoCloseable {

        private final MysqlLocalFirstPublicationLedger control;

        private final MysqlShardSegmentFence physical;

        private final long gatherNanos;

        private final boolean fastPublish;

        private final boolean fusedPublish;

        private final ArrayBlockingQueue<Request> queue = new ArrayBlockingQueue<>(4096);

        private final List<Thread> workers;

        private final ScheduledExecutorService periodicPublisher;

        private final AtomicBoolean closed = new AtomicBoolean();

        private final AtomicBoolean publicationQueued = new AtomicBoolean();

        private final AtomicLong publicationSignalVersion = new AtomicLong();

        private final AtomicLong published;

        private final LongAdder groups = new LongAdder();

        private final LongAdder requests = new LongAdder();

        private final LongAdder failures = new LongAdder();

        private final LongAdder decisionNanos = new LongAdder();

        private final LongAdder fusedGroups = new LongAdder();

        private final LongAdder publicationNanos = new LongAdder();

        private final LongAdder publicationRuns = new LongAdder();

        private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();

        /**
         * Fixed bounded background workspaces and rounded an abnormal wheel with a periodic publisher.
         *
         * @param control control
         * @param physical physical
         * @param workerCount worker count
         * @param gatherMicros gather micros
         * @param fastPublish fast publish
         * @param fusedPublish fused publish
         * @param publicationIntervalMillis publication interval millis
         */
        private CompletionBatcher(final MysqlLocalFirstPublicationLedger control,
                                  final MysqlShardSegmentFence physical, final int workerCount,
                                  final long gatherMicros, final boolean fastPublish, final boolean fusedPublish,
                                  final long publicationIntervalMillis) {
            this.control = control;
            this.physical = physical;
            this.gatherNanos = TimeUnit.MICROSECONDS.toNanos(gatherMicros);
            this.fastPublish = fastPublish;
            this.fusedPublish = fusedPublish;
            this.published = new AtomicLong(control.currentPublished());
            List<Thread> started = new ArrayList<>(workerCount);
            for (int index = 0; index < workerCount; index++) {
                started.add(Thread.ofVirtual().name("mvcc-local-first-batch-" + index).start(this::run));
            }
            this.workers = List.copyOf(started);
            this.periodicPublisher = Executors.newSingleThreadScheduledExecutor(task -> Thread.ofPlatform().daemon().name("mvcc-local-first-publisher").unstarted(task));
            periodicPublisher.scheduleWithFixedDelay(this::publishSafely, 0,
                    publicationIntervalMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * Foreground is only queued after success of its own local transaction durable and waits for the center batch to confirm.
         *
         * @param assignment assignment
         * @param txId transaction identifier
         * @param proof proof
         * @throws IllegalStateException if the operation cannot complete
         */
        private void complete(final Assignment assignment, final long txId, final MysqlStorageAdapter.LocalCommitProof proof) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            if (closed.get() || !queue.offer(new Request(assignment, txId, proof, result))) {
                throw new IllegalStateException("local-first completion queue is closed or full");
            }
            try {
                result.join();
            } catch (final CompletionException ex) {
                if (ex.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw ex;
            }
        }

        /**
         * Only the centre watermark has been pushed durable as a successful foreground border.
         *
         * @param commitTs durable commit timestamp
         * @param timeoutMillis timeout millis
         * @throws IllegalStateException if the operation cannot complete
         */
        private void awaitPublished(final long commitTs, final long timeoutMillis) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (published.get() < commitTs) {
                if (System.nanoTime() >= deadline || Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException("local-first publication confirmation timed out");
                }
                LockSupport.parkNanos(100_000);
            }
        }

        /**
         * Returns the batch size of each transaction actually apportioned centre.
         *
         * @return the operation result
         */
        private String stats() {
            return "groups=" + groups.sum() + " requests=" + requests.sum()
                    + " avg_size=" + (groups.sum() == 0 ? 0.0 : (double) requests.sum() / groups.sum())
                    + " fused_groups=" + fusedGroups.sum()
                    + " decision_avg_ms=" + (groups.sum() == 0 ? 0.0
                            : decisionNanos.sum() / 1_000_000.0 / groups.sum())
                    + " publication_runs=" + publicationRuns.sum()
                    + " publication_avg_ms=" + (publicationRuns.sum() == 0 ? 0.0
                            : publicationNanos.sum() / 1_000_000.0 / publicationRuns.sum())
                    + " failures=" + failures.sum() + " last_failure=" + lastFailure.get();
        }

        /**
         * Safe exit after processing accepted requests; concurrent local commit is collected only for one millisecond at a time.
         */
        private void run() {
            while (!closed.get() || !queue.isEmpty()) {
                try {
                    Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    List<Request> batch = new ArrayList<>(64);
                    batch.add(first);
                    if (gatherNanos > 0) {
                        LockSupport.parkNanos(gatherNanos);
                    }
                    queue.drainTo(batch, 63);
                    if (batch.size() == 1 && gatherNanos == 0) {
                        Request second = queue.poll(1, TimeUnit.MILLISECONDS);
                        if (second != null) {
                            batch.add(second);
                            queue.drainTo(batch, 64 - batch.size());
                        }
                    }
                    Map<Long, List<Request>> bySegment = new LinkedHashMap<>();
                    for (Request request : batch) {
                        bySegment.computeIfAbsent(request.assignment().segment().lease().firstTs(),
                                ignored -> new ArrayList<>()).add(request);
                    }
                    bySegment.values().forEach(this::process);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * Batch atomic Center terminal state in a section, failed to keep the local certificate for recovery.
         *
         * @param batch batch
         */
        private void process(final List<Request> batch) {
            groups.increment();
            requests.add(batch.size());
            try {
                long decisionStarted = System.nanoTime();
                long earliest = batch.stream().mapToLong(request -> request.assignment().commitTs())
                        .min().orElseThrow();
                boolean useProofs = batch.stream().allMatch(request -> request.proof() != null);
                List<Long> txIds = batch.stream().map(Request::txId).toList();
                List<MysqlStorageAdapter.LocalCommitProof> proofs = useProofs
                        ? batch.stream().map(Request::proof).toList()
                        : List.of();
                if (fusedPublish && earliest <= published.get() + 1) {
                    var result = useProofs
                            ? control.completeProvedCandidatesAndPublish(
                                    batch.getFirst().assignment().segment(), physical, proofs)
                            : control.completeCandidatesAndPublish(
                                    batch.getFirst().assignment().segment(), physical, txIds);
                    fusedGroups.increment();
                    published.accumulateAndGet(result.publishedTs(), Math::max);
                    long latest = batch.stream().mapToLong(request -> request.assignment().commitTs())
                            .max().orElseThrow();
                    if (result.publishedTs() < latest) {
                        requestPublication();
                    }
                } else {
                    if (useProofs) {
                        control.completeProvedCandidates(
                                batch.getFirst().assignment().segment(), physical, proofs);
                    } else {
                        control.completeCandidates(batch.getFirst().assignment().segment(), physical, txIds);
                    }
                    requestPublication();
                }
                decisionNanos.add(System.nanoTime() - decisionStarted);
                batch.forEach(request -> request.result().complete(null));
            } catch (final RuntimeException ex) {
                failures.increment();
                lastFailure.set(ex);
                batch.forEach(request -> request.result().completeExceptionally(ex));
            }
        }

        /**
         * Multiple completed centres batch row only one release awakening, and foreground does not wait for the publisher to take up batch processing threads.
         */
        private void requestPublication() {
            publicationSignalVersion.incrementAndGet();
            if (publicationQueued.compareAndSet(false, true)) {
                periodicPublisher.schedule(this::publishRequested, 1, TimeUnit.MILLISECONDS);
            }
        }

        /**
         * When the new batch is completed during the release period, a consolidation wheel is next in the tail.
         */
        private void publishRequested() {
            long observed = publicationSignalVersion.get();
            try {
                publishSafely();
            } finally {
                publicationQueued.set(false);
                if (publicationSignalVersion.get() != observed && !closed.get()
                        && publicationQueued.compareAndSet(false, true)) {
                    periodicPublisher.schedule(this::publishRequested, 1, TimeUnit.MILLISECONDS);
                }
            }
        }

        /**
         * The cycle and event path are read-only database with durable release results, without extrapolating local continuity.
         */
        private void publishSafely() {
            try {
                long started = System.nanoTime();
                long durable = fastPublish ? control.advancePublishedFast(4096)
                        : control.advancePublished(4096);
                published.accumulateAndGet(durable, Math::max);
                publicationNanos.add(System.nanoTime() - started);
                publicationRuns.increment();
            } catch (final RuntimeException ex) {
                failures.increment();
                lastFailure.set(ex);
            }
        }

        /**
         * Waiting for the queue to accept the request, database durable candidate continues to be protected by the recovery protocol.
         */
        @Override
        public void close() {
            closed.set(true);
            try {
                for (Thread worker : workers) {
                    worker.join(5_000);
                }
                periodicPublisher.shutdown();
                periodicPublisher.awaitTermination(5, TimeUnit.SECONDS);
            } catch (final InterruptedException ex) {
                periodicPublisher.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        /**
         * An independent centre completes the result and does not disseminate guess decision to other batch s.
         *
         * @param assignment assignment
         * @param txId transaction identifier
         * @param proof proof
         * @param result result
         */
        private record Request(Assignment assignment, long txId, MysqlStorageAdapter.LocalCommitProof proof,
                CompletableFuture<Void> result) {
        }
    }
}

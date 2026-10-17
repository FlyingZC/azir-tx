package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import mvcc.control.MysqlDatabaseAuthority;
import mvcc.metadata.BeginReservation;
import mvcc.metadata.mysql.MysqlLocalFirstMetadataView;
import mvcc.publication.LocalFirstIntentRecoveryManager;
import mvcc.publication.LocalFirstPublicationMaintenanceScheduler;
import mvcc.publication.LocalFirstPublicationRecoveryManager;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger.IntentCommitRequest;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger.IntentRegistrationRequest;
import mvcc.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/** 三库隔离容量实验，计入跨分片预写、中心批量决议、发布确认和并行后台物化。 */
public final class MysqlLocalFirstCrossShardProtocolBenchmark {
    /** 基准入口不保存全局可变状态。 */
    private MysqlLocalFirstCrossShardProtocolBenchmark() {
    }

    /** 仅在显式重置下运行真实两分片事务并报告后台物化积压。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("cross-shard protocol benchmark requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "10"));
        boolean vectorMode = Boolean.parseBoolean(env("AZIR_TX_VECTOR_CROSS", "false"));
        boolean vectorJavaPrewrite = Boolean.parseBoolean(env("AZIR_TX_VECTOR_PREWRITE_JAVA_FALLBACK", "false"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int prewriteGroupSize = Integer.parseInt(env("AZIR_TX_VECTOR_PREWRITE_COALESCE_BATCH", "0"));
        int prewriteLanes = Integer.parseInt(env("AZIR_TX_VECTOR_PREWRITE_BATCH_LANES", "4"));
        long prewriteWaitMicros = Long.parseLong(env("AZIR_TX_VECTOR_PREWRITE_BATCH_WAIT_MICROS", "500"));
        long recoveryParkMicros = Long.parseLong(env("AZIR_TX_INTENT_RECOVERY_PARK_MICROS", "20000"));
        long maxBacklog = Long.parseLong(env("AZIR_TX_INTENT_MAX_BACKLOG", "0"));
        if (concurrency < 1 || concurrency > 200 || seconds < 1 || seconds > 120
                || maxBacklog < 0 || maxBacklog > 1_000_000
                || stripes < 1 || stripes > 256
                || prewriteGroupSize < 0 || prewriteGroupSize > 64
                || prewriteGroupSize > 0 && !vectorMode
                || prewriteLanes < 1 || prewriteLanes > 32
                || prewriteWaitMicros < 0 || prewriteWaitMicros > 10_000
                || recoveryParkMicros < 0 || recoveryParkMicros > 1_000_000) {
            throw new IllegalArgumentException("invalid cross-shard benchmark limits");
        }
        String username = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        String controlUrl = env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        String leftUrl = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        String rightUrl = env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2");
        long retentionMillis = Long.parseLong(env("AZIR_TX_INTENT_RETENTION_MS", "5000"));
        try (HikariDataSource controlData = pool(controlUrl, username, password);
             HikariDataSource leftData = pool(leftUrl, username, password);
             HikariDataSource rightData = pool(rightUrl, username, password);
             HikariDataSource backgroundControlData = backgroundPool(controlUrl, username, password);
             HikariDataSource backgroundLeftData = backgroundPool(leftUrl, username, password);
             HikariDataSource backgroundRightData = backgroundPool(rightUrl, username, password)) {
            MysqlDatabaseAuthority.requireIndependentSameDatabase(controlData, backgroundControlData, "control");
            MysqlDatabaseAuthority.requireIndependentSameDatabase(leftData, backgroundLeftData, "s1");
            MysqlDatabaseAuthority.requireIndependentSameDatabase(rightData, backgroundRightData, "s2");
            reset(controlData, leftData, rightData);
            var ledger = new MysqlLocalFirstPublicationLedger(controlData);
            ledger.initializeIsolated(100);
            var view = new MysqlLocalFirstMetadataView(ledger);
            var left = new MysqlStorageAdapter("s1", leftData, view);
            var right = new MysqlStorageAdapter("s2", rightData, view);
            left.initialize();
            right.initialize();
            if (vectorMode) {
                left.initializeVectorProtocol(stripes);
                right.initializeVectorProtocol(stripes);
            }
            var leftFence = new MysqlShardSegmentFence("s1", leftData);
            var rightFence = new MysqlShardSegmentFence("s2", rightData);
            leftFence.initializeIsolated();
            rightFence.initializeIsolated();
            var backgroundLedger = new MysqlLocalFirstPublicationLedger(backgroundControlData);
            backgroundLedger.requireInitializedFastPath();
            var backgroundView = new MysqlLocalFirstMetadataView(backgroundLedger);
            var backgroundLeft = new MysqlStorageAdapter("s1", backgroundLeftData, backgroundView);
            var backgroundRight = new MysqlStorageAdapter("s2", backgroundRightData, backgroundView);
            backgroundLeft.requireInitializedFastPath();
            backgroundRight.requireInitializedFastPath();
            if (vectorMode) {
                backgroundLeft.initializeVectorProtocol(stripes);
                backgroundRight.initializeVectorProtocol(stripes);
            }
            var backgroundLeftFence = new MysqlShardSegmentFence("s1", backgroundLeftData);
            var backgroundRightFence = new MysqlShardSegmentFence("s2", backgroundRightData);
            backgroundLeftFence.requireInitializedFastPath();
            backgroundRightFence.requireInitializedFastPath();
            int recoveryConcurrency = Integer.parseInt(env("AZIR_TX_INTENT_RECOVERY_CONCURRENCY", "16"));
            var recovery = new LocalFirstIntentRecoveryManager(ledger,
                    Map.of("s1", left, "s2", right), recoveryConcurrency);
            var segmentRecovery = new LocalFirstPublicationRecoveryManager(backgroundLedger,
                    Map.of("s1", backgroundLeftFence, "s2", backgroundRightFence));
            Map<String, DataSource> routes = Map.of("s1", leftData, "s2", rightData);
            var vectorLeases = vectorMode
                    ? MysqlVectorCutLeaderLease.forProduct(controlData, routes, stripes) : null;
            if (vectorLeases != null) {
                vectorLeases.initializeProduct();
            }
            try (var fanout = Executors.newVirtualThreadPerTaskExecutor();
                 var vectorPublisher = vectorMode
                         ? MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, stripes,
                         ledger::currentPublished, vectorLeases, "cross-benchmark-owner", 5_000, 200)
                         : null;
                 var snapshots = new LocalFirstCoordinatorSnapshots(ledger, 20_000, 60_000, 1_000);
                 var maintenance = new LocalFirstPublicationMaintenanceScheduler(backgroundLedger,
                         Map.of("s1", backgroundLeftFence, "s2", backgroundRightFence),
                         Map.of("s1", backgroundLeft, "s2", backgroundRight),
                         16, 16, 128, 100, 1_000, retentionMillis);
                 var owner = new LocalFirstIntentSegmentOwnerRegistry(ledger, backgroundLedger,
                         snapshots::floorForReservation, 512, 5_000, 1_000, 100);
                 var registrations = new RegistrationBatcher(ledger, 4, vectorMode);
                 var leftPrewrites = prewriteGroupSize > 0
                         ? new MysqlVectorCrossPrewriteBatcher(left, prewriteLanes,
                         prewriteGroupSize, prewriteWaitMicros, 4096) : null;
                 var rightPrewrites = prewriteGroupSize > 0
                         ? new MysqlVectorCrossPrewriteBatcher(right, prewriteLanes,
                         prewriteGroupSize, prewriteWaitMicros, 4096) : null;
                 var batcher = new IntentBatcher(ledger, 4, vectorMode)) {
                if (vectorPublisher != null && !vectorPublisher.start()) {
                    throw new IllegalStateException("vector cross benchmark has another active publisher");
                }
                run(ledger, left, right, recovery, segmentRecovery, owner, batcher,
                        registrations, leftPrewrites, rightPrewrites,
                        snapshots, maintenance, controlData, leftData, rightData, fanout,
                        vectorPublisher, vectorMode, vectorJavaPrewrite, stripes,
                        concurrency, seconds, maxBacklog, recoveryParkMicros);
            }
        }
    }

    /** 外部数据库 URL 可以指向不同实例，但必须显式使用各自独立库。 */
    private static HikariDataSource pool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /** 独立小连接池让 GC 不占用前台等待预算。 */
    private static HikariDataSource backgroundPool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, username, password);
    }

    /** 基准只清除明确选择的隔离控制表与两个分片 MVCC 数据。 */
    private static void reset(HikariDataSource control, HikariDataSource left, HikariDataSource right)
            throws Exception {
        try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_first_slot_v1");
            statement.execute("drop table if exists mvcc_local_first_txn_v1");
            statement.execute("drop table if exists mvcc_local_first_segment_v1");
            statement.execute("drop table if exists mvcc_local_first_snapshot_v1");
            statement.execute("drop table if exists mvcc_local_first_coordinator_v1");
            statement.execute("drop table if exists mvcc_local_first_state_v1");
            statement.execute("drop table if exists mvcc_vector_leader_v1");
            statement.execute("drop table if exists mvcc_vector_published_cut_v1");
        }
        resetShard(left, "s1");
        resetShard(right, "s2");
    }

    /** 先建分片标准表，再清除上次实验留下的版本与锁。 */
    private static void resetShard(HikariDataSource dataSource, String shardId) throws Exception {
        new MysqlStorageAdapter(shardId, dataSource).initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_segment_candidate_v1");
            statement.execute("drop table if exists mvcc_local_segment_fence_v1");
            statement.execute("drop table if exists mvcc_local_segment_state_v1");
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
            statement.execute("drop table if exists mvcc_vector_local_data_v1");
            statement.execute("drop table if exists mvcc_vector_committed_v1");
            statement.execute("drop table if exists mvcc_vector_intent_reservation_v1");
            statement.execute("drop table if exists mvcc_vector_stripe_state_v1");
            statement.execute("drop table if exists mvcc_vector_layout_v1");
        }
    }

    /** 全新事务先在中心合并持久 ACTIVE 身份，确认后才允许分片预写。 */
    private static final class RegistrationBatcher implements AutoCloseable {
        private final MysqlLocalFirstPublicationLedger ledger;
        private final ArrayBlockingQueue<Request> queue = new ArrayBlockingQueue<>(4096);
        private final List<Thread> workers;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final LongAdder groups = new LongAdder();
        private final LongAdder requests = new LongAdder();
        private final boolean vectorMode;

        /** 每笔注册等待自己所在中心事务持久完成。 */
        private record Request(IntentRegistrationRequest registration, CompletableFuture<Void> result) {
        }

        /** 有界工作线程并行处理互不相交的新事务批次。 */
        private RegistrationBatcher(MysqlLocalFirstPublicationLedger ledger,
                                    int workerCount, boolean vectorMode) {
            this.ledger = ledger;
            this.vectorMode = vectorMode;
            List<Thread> running = new ArrayList<>();
            for (int index = 0; index < workerCount; index++) {
                running.add(Thread.ofVirtual().name("mvcc-intent-register-" + index).start(this::work));
            }
            workers = List.copyOf(running);
        }

        /** 前台收到持久确认后才能在任一分片创建意向。 */
        private void register(IntentRegistrationRequest registration) {
            var result = new CompletableFuture<Void>();
            if (closed.get() || !queue.offer(new Request(registration, result))) {
                throw new IllegalStateException("cross-shard registration queue is unavailable");
            }
            try {
                result.join();
            } catch (CompletionException ex) {
                if (ex.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw ex;
            }
        }

        /** 一个批次全部插入或全部失败，不把排队误当作中心 ACTIVE。 */
        private void work() {
            while (!closed.get() || !queue.isEmpty()) {
                try {
                    Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    List<Request> batch = new ArrayList<>(64);
                    batch.add(first);
                    queue.drainTo(batch, 63);
                    groups.increment();
                    requests.add(batch.size());
                    try {
                        List<IntentRegistrationRequest> identities = batch.stream()
                                .map(Request::registration).toList();
                        if (vectorMode) {
                            ledger.registerVectorIntentBatch(identities);
                        } else {
                            ledger.registerIntentBatch(identities);
                        }
                        batch.forEach(request -> request.result().complete(null));
                    } catch (RuntimeException ex) {
                        batch.forEach(request -> request.result().completeExceptionally(ex));
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /** 已入队请求完成后再停止注册线程。 */
        @Override
        public void close() throws Exception {
            closed.set(true);
            for (Thread worker : workers) {
                worker.join(10_000);
            }
        }
    }

    /** 多个已完成双分片预写的事务在同一段内合并中心决议和发布。 */
    private static final class IntentBatcher implements AutoCloseable {
        private final MysqlLocalFirstPublicationLedger ledger;
        private final ArrayBlockingQueue<Request> queue = new ArrayBlockingQueue<>(4096);
        private final List<Thread> workers;
        private final ScheduledExecutorService publisher;
        private final AtomicLong published;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final LongAdder groups = new LongAdder();
        private final LongAdder requests = new LongAdder();
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        private final boolean vectorMode;

        /** 每个等待者只接收自己的持久中心批次结果。 */
        private record Request(IntentCommitRequest decision, CompletableFuture<Void> result) {
        }

        /** 固定虚拟工作线程并缓存已由中心事务持久发布的水位。 */
        private IntentBatcher(MysqlLocalFirstPublicationLedger ledger,
                              int workerCount, boolean vectorMode) {
            this.ledger = ledger;
            this.vectorMode = vectorMode;
            this.published = new AtomicLong(ledger.currentPublished());
            List<Thread> running = new ArrayList<>();
            for (int index = 0; index < workerCount; index++) {
                running.add(Thread.ofVirtual().name("mvcc-intent-batch-" + index).start(this::work));
            }
            workers = List.copyOf(running);
            publisher = Executors.newSingleThreadScheduledExecutor(task ->
                    Thread.ofPlatform().daemon().name("mvcc-intent-publisher").unstarted(task));
            publisher.scheduleWithFixedDelay(this::publishSafely, 0, 100, TimeUnit.MILLISECONDS);
        }

        /** 前台阻塞到自己的决议批次完成，不能把排队当作持久提交。 */
        private void complete(IntentCommitRequest decision) {
            var result = new CompletableFuture<Void>();
            if (closed.get() || !queue.offer(new Request(decision, result))) {
                throw new IllegalStateException("cross-shard decision queue is unavailable");
            }
            try {
                result.join();
            } catch (CompletionException ex) {
                if (ex.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw ex;
            }
        }

        /** 前台只等自身提交时间进入同一中心持久发布前缀。 */
        private void awaitPublished(long commitTs, long deadlineMillis) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineMillis);
            while (published.get() < commitTs) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("cross-shard publication confirmation timed out");
                }
                LockSupport.parkNanos(100_000);
            }
        }

        /** 每段不超过 64 笔，以同一控制事务完成元数据、槽与连续发布。 */
        private void work() {
            while (!closed.get() || !queue.isEmpty()) {
                try {
                    Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    List<Request> batch = new ArrayList<>(64);
                    batch.add(first);
                    queue.drainTo(batch, 63);
                    Map<Long, List<Request>> bySegment = new HashMap<>();
                    for (Request request : batch) {
                        bySegment.computeIfAbsent(request.decision().segment().lease().firstTs(),
                                ignored -> new ArrayList<>()).add(request);
                    }
                    bySegment.values().forEach(this::completeGroup);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /** 中心决议失败时仅通知本组，持久 ACTIVE 交给到期恢复。 */
        private void completeGroup(List<Request> batch) {
            groups.increment();
            requests.add(batch.size());
            try {
                List<IntentCommitRequest> decisions = batch.stream().map(Request::decision).toList();
                var result = vectorMode ? ledger.completeVectorIntentBatchAndPublish(decisions)
                        : ledger.completeIntentBatchAndPublish(decisions);
                published.accumulateAndGet(result.publishedTs(), Math::max);
                batch.forEach(request -> request.result().complete(null));
            } catch (RuntimeException ex) {
                failure.set(ex);
                batch.forEach(request -> request.result().completeExceptionally(ex));
            }
        }

        /** owner 封尾或失联恢复推进水位后也要唤醒等待中的前台。 */
        private void publishSafely() {
            try {
                published.accumulateAndGet(ledger.advancePublishedFast(4096), Math::max);
            } catch (RuntimeException ex) {
                failure.set(ex);
            }
        }

        /** 结束新请求并让已入队请求完成或报告异常。 */
        @Override
        public void close() throws Exception {
            closed.set(true);
            publisher.shutdownNow();
            for (Thread worker : workers) {
                worker.join(10_000);
            }
        }
    }

    /** 并发真实业务路径，同时后台补物化并分别计时各阶段。 */
    private static void run(MysqlLocalFirstPublicationLedger ledger, MysqlStorageAdapter left,
                            MysqlStorageAdapter right, LocalFirstIntentRecoveryManager recovery,
                            LocalFirstPublicationRecoveryManager segmentRecovery,
                            LocalFirstIntentSegmentOwnerRegistry owner, IntentBatcher batcher,
                            RegistrationBatcher registrations,
                            MysqlVectorCrossPrewriteBatcher leftPrewrites,
                            MysqlVectorCrossPrewriteBatcher rightPrewrites,
                            LocalFirstCoordinatorSnapshots snapshots,
                            LocalFirstPublicationMaintenanceScheduler maintenance,
                            HikariDataSource controlData, HikariDataSource leftData,
                            HikariDataSource rightData, ExecutorService fanout,
                            MysqlVectorCutLeaderRuntime vectorPublisher,
                            boolean vectorMode, boolean vectorJavaPrewrite, int stripes,
                            int concurrency, int seconds, long maxBacklog,
                            long recoveryParkMicros) throws Exception {
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        LongAdder materialized = new LongAdder();
        LongAdder recoveryFailures = new LongAdder();
        LongAdder sealedSegments = new LongAdder();
        LongAdder segmentFailures = new LongAdder();
        LongAdder beginNanos = new LongAdder();
        LongAdder registerNanos = new LongAdder();
        LongAdder prewriteNanos = new LongAdder();
        LongAdder centerNanos = new LongAdder();
        LongAdder publishNanos = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        AtomicBoolean recovering = new AtomicBoolean(true);
        AtomicBoolean collecting = new AtomicBoolean(true);
        Thread recoveryThread = Thread.ofVirtual().name("mvcc-intent-recovery").start(() -> {
            while (recovering.get()) {
                try {
                    var result = recovery.recoverOnce(64);
                    materialized.add(result.finalized());
                    recoveryFailures.add(result.failures());
                } catch (RuntimeException ex) {
                    firstFailure.compareAndSet(null, "recovery:" + ex.getMessage());
                }
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(recoveryParkMicros));
            }
        });
        Thread segmentThread = Thread.ofVirtual().name("mvcc-intent-segment-recovery").start(() -> {
            while (recovering.get()) {
                try {
                    var result = segmentRecovery.recoverOnce(16, 16);
                    sealedSegments.add(result.sealedSegments());
                    segmentFailures.add(result.failures());
                    if (result.failures() > 0) {
                        firstFailure.compareAndSet(null, "segment:" + segmentRecovery.lastFailure());
                    }
                } catch (RuntimeException ex) {
                    segmentFailures.increment();
                    firstFailure.compareAndSet(null, "segment:" + ex.getMessage());
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        });
        Thread gcThread = Thread.ofVirtual().name("mvcc-intent-gc").start(() -> {
            while (collecting.get()) {
                try {
                    if (!vectorMode) {
                        maintenance.gcOnce();
                    }
                } catch (RuntimeException ex) {
                    firstFailure.compareAndSet(null, "gc:" + ex.getMessage());
                }
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        });
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
                        while (System.nanoTime() < end) {
                            // 只测后台可持续速率时，协调器在积压上限前暂停新事务。
                            while (maxBacklog > 0 && committed.sum() - materialized.sum() >= maxBacklog
                                    && System.nanoTime() < end) {
                                LockSupport.parkNanos(100_000);
                            }
                            if (System.nanoTime() >= end) {
                                break;
                            }
                            BeginReservation tx = null;
                            LocalFirstIntentSegmentOwnerRegistry.Assignment owned = null;
                            try {
                                long stage = System.nanoTime();
                                tx = snapshots.begin(IsolationLevel.REPEATABLE_READ);
                                beginNanos.add(System.nanoTime() - stage);
                                stage = System.nanoTime();
                                registrations.register(new IntentRegistrationRequest(tx.txId(), tx.startTs(),
                                        tx.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                                        Set.of("s1", "s2")));
                                registerNanos.add(System.nanoTime() - stage);
                                var current = tx;
                                int stripe = Math.floorMod(current.txId(), stripes);
                                Mutation leftWrite = Mutation.put("cross:" + current.txId() + ":left",
                                        Long.toString(current.txId()).getBytes(StandardCharsets.UTF_8));
                                Mutation rightWrite = Mutation.put("cross:" + current.txId() + ":right",
                                        Long.toString(current.txId()).getBytes(StandardCharsets.UTF_8));
                                CompletableFuture<MysqlStorageAdapter.PrewriteProof> first;
                                CompletableFuture<MysqlStorageAdapter.PrewriteProof> second;
                                if (leftPrewrites != null) {
                                    long expiry = System.currentTimeMillis() + 60_000;
                                    first = leftPrewrites.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                                            current.txId(), current.startTs(), expiry,
                                            leftWrite, stripe, 0), 5_000);
                                    second = rightPrewrites.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                                            current.txId(), current.startTs(), expiry,
                                            rightWrite, stripe, 0), 5_000);
                                } else {
                                    first = CompletableFuture.supplyAsync(() -> {
                                        if (!vectorMode) {
                                            return left.prewriteWithProof(current.txId(), current.startTs(),
                                                    List.of(leftWrite));
                                        }
                                        return vectorJavaPrewrite
                                                ? left.prewriteWithVectorProofJavaForProbe(current.txId(),
                                                current.startTs(), leftWrite, stripe, 0)
                                                : left.prewriteWithVectorProof(current.txId(),
                                                current.startTs(), leftWrite, stripe, 0);
                                    }, fanout);
                                    second = CompletableFuture.supplyAsync(() -> {
                                        if (!vectorMode) {
                                            return right.prewriteWithProof(current.txId(), current.startTs(),
                                                    List.of(rightWrite));
                                        }
                                        return vectorJavaPrewrite
                                                ? right.prewriteWithVectorProofJavaForProbe(current.txId(),
                                                current.startTs(), rightWrite, stripe, 0)
                                                : right.prewriteWithVectorProof(current.txId(),
                                                current.startTs(), rightWrite, stripe, 0);
                                    }, fanout);
                                }
                                var firstProof = first.join();
                                var secondProof = second.join();
                                prewriteNanos.add(System.nanoTime() - stage);
                                owned = owner.allocate(tx.startTs());
                                var decision = new IntentCommitRequest(owned.reservation(), owned.commitTs(),
                                        tx.txId(), tx.startTs(), tx.deadlineMillis(),
                                        IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"),
                                        List.of(firstProof, secondProof));
                                stage = System.nanoTime();
                                batcher.complete(decision);
                                owner.complete(owned);
                                centerNanos.add(System.nanoTime() - stage);
                                stage = System.nanoTime();
                                if (vectorMode) {
                                    vectorPublisher.awaitPublished("s1", stripe,
                                            firstProof.vectorReservation().orElseThrow().reservedSequence(),
                                            owned.commitTs(), 20_000);
                                    vectorPublisher.awaitPublished("s2", stripe,
                                            secondProof.vectorReservation().orElseThrow().reservedSequence(),
                                            owned.commitTs(), 20_000);
                                } else {
                                    batcher.awaitPublished(owned.commitTs(), 20_000);
                                }
                                publishNanos.add(System.nanoTime() - stage);
                                committed.increment();
                            } catch (RuntimeException ex) {
                                if (owned != null) {
                                    owner.unknown(owned);
                                }
                                failures.increment();
                                firstFailure.compareAndSet(null, ex.getClass().getSimpleName() + ":" + ex.getMessage());
                                if (tx != null) {
                                    try {
                                        ledger.abortIntentTransaction(tx.txId());
                                    } catch (RuntimeException ignored) {
                                        // 中心可能已经 COMMITTED，统一交给持久恢复扫描。
                                    }
                                }
                            } finally {
                                if (tx != null) {
                                    snapshots.release(tx.txId());
                                }
                            }
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            Thread reporter = Thread.ofVirtual().name("mvcc-intent-window").start(() -> {
                long previous = 0;
                for (int index = 1; index <= (seconds + 9) / 10; index++) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
                    if (Thread.interrupted()) {
                        return;
                    }
                    long count = committed.sum();
                    try {
                        System.out.printf("LOCAL_FIRST_CROSS_WINDOW index=%d tps=%.2f committed=%d "
                                        + "materialized=%d backlog=%d safe_point=%d slots=%d txn=%d%n",
                                index, (count - previous) / 10.0, count, materialized.sum(),
                                ledger.countUnfinalizedIntentDecisions(), ledger.currentSafePoint(),
                                countRows(controlData, "mvcc_local_first_slot_v1"),
                                countRows(controlData, "mvcc_local_first_txn_v1"));
                    } catch (SQLException ex) {
                        firstFailure.compareAndSet(null, "window:" + ex.getMessage());
                    }
                    previous = count;
                }
            });
            start.countDown();
            done.await();
            reporter.interrupt();
            reporter.join();
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            recovering.set(false);
            collecting.set(false);
            recoveryThread.join(10_000);
            segmentThread.join(10_000);
            gcThread.join(10_000);
            owner.close();
            long pendingAtStop = ledger.countUnfinalizedIntentDecisions();
            long drainStarted = System.nanoTime();
            while (ledger.countUnfinalizedIntentDecisions() > 0
                    && System.nanoTime() - drainStarted < TimeUnit.SECONDS.toNanos(30)) {
                var drained = recovery.recoverOnce(64);
                materialized.add(drained.finalized());
                recoveryFailures.add(drained.failures());
            }
            double drainSeconds = (System.nanoTime() - drainStarted) / 1_000_000_000.0;
            if (!vectorMode) {
                maintenance.gcOnce();
            }
            System.out.printf("LOCAL_FIRST_CROSS_BENCH mode=%s concurrency=%d duration_s=%.2f committed=%d "
                            + "failures=%d tps=%.2f first_failure=%s groups=%d avg_batch=%.2f "
                            + "published=%d pending_materialization=%d materialized=%d "
                            + "recovery_failures=%d recovery_failure=%s "
                            + "registration_groups=%d registration_avg_batch=%.2f "
                            + "backlog_at_stop=%d drain_seconds=%.2f max_backlog=%d recovery_park_us=%d "
                            + "owner_segments=%d owner_failures=%d owner_error=%s "
                            + "recovered_segments=%d segment_failures=%d segment_error=%s "
                            + "safe_point=%d slot_rows=%d txn_rows=%d segment_rows=%d "
                            + "gc_runs=%d gc_failures=%d gc_error=%s "
                            + "vector_left=%d vector_right=%d vector_pending_left=%d vector_pending_right=%d "
                            + "prewrite_groups_left=%d prewrite_groups_right=%d "
                            + "prewrite_avg_batch_left=%.2f prewrite_avg_batch_right=%.2f "
                            + "prewrite_fallback_left=%d prewrite_fallback_right=%d%n",
                    vectorMode ? "VECTOR" : "SCALAR",
                    concurrency, elapsed, committed.sum(), failures.sum(), committed.sum() / elapsed,
                    firstFailure.get(), batcher.groups.sum(),
                    batcher.groups.sum() == 0 ? 0 : (double) batcher.requests.sum() / batcher.groups.sum(),
                    ledger.currentPublished(), ledger.countUnfinalizedIntentDecisions(),
                    materialized.sum(), recoveryFailures.sum(), recovery.lastFailure(), registrations.groups.sum(),
                    registrations.groups.sum() == 0 ? 0
                            : (double) registrations.requests.sum() / registrations.groups.sum(),
                    pendingAtStop, drainSeconds, maxBacklog, recoveryParkMicros,
                    owner.reservationCount(),
                    owner.refreshStats().failureCount(), owner.lastFailure(),
                    sealedSegments.sum(), segmentFailures.sum(), segmentRecovery.lastFailure(),
                    ledger.currentSafePoint(),
                    countRows(controlData, "mvcc_local_first_slot_v1"),
                    countRows(controlData, "mvcc_local_first_txn_v1"),
                    countRows(controlData, "mvcc_local_first_segment_v1"),
                    maintenance.gcStats().runCount(), maintenance.gcStats().failureCount(),
                    maintenance.lastGcFailure(),
                    vectorMode ? countRows(leftData, "mvcc_vector_committed_v1") : 0,
                    vectorMode ? countRows(rightData, "mvcc_vector_committed_v1") : 0,
                    vectorMode ? countRows(leftData, "mvcc_vector_intent_reservation_v1") : 0,
                    vectorMode ? countRows(rightData, "mvcc_vector_intent_reservation_v1") : 0,
                    leftPrewrites == null ? 0 : leftPrewrites.batchCount(),
                    rightPrewrites == null ? 0 : rightPrewrites.batchCount(),
                    leftPrewrites == null || leftPrewrites.batchCount() == 0 ? 0
                            : (double) leftPrewrites.groupedCount() / leftPrewrites.batchCount(),
                    rightPrewrites == null || rightPrewrites.batchCount() == 0 ? 0
                            : (double) rightPrewrites.groupedCount() / rightPrewrites.batchCount(),
                    leftPrewrites == null ? 0 : leftPrewrites.fallbackCount(),
                    rightPrewrites == null ? 0 : rightPrewrites.fallbackCount());
            long count = Math.max(1, committed.sum());
            System.out.printf("LOCAL_FIRST_CROSS_STAGE begin_avg_ms=%.3f register_avg_ms=%.3f "
                            + "prewrite_avg_ms=%.3f center_avg_ms=%.3f publish_avg_ms=%.3f%n",
                    beginNanos.sum() / count / 1_000_000.0, registerNanos.sum() / count / 1_000_000.0,
                    prewriteNanos.sum() / count / 1_000_000.0, centerNanos.sum() / count / 1_000_000.0,
                    publishNanos.sum() / count / 1_000_000.0);
        } finally {
            recovering.set(false);
            collecting.set(false);
            recoveryThread.interrupt();
            segmentThread.interrupt();
            gcThread.interrupt();
        }
    }

    /** 只接收本程序固定表名，用于确认长跑历史行数没有线性堆积。 */
    private static long countRows(HikariDataSource dataSource, String table) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("select count(*) from " + table)) {
            if (!row.next()) {
                throw new SQLException("cross-shard benchmark table count returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 只读取本次实验显式配置，不用系统配置猜测其他数据库。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.storage.Mutation;

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

/** 跨片容量探针的并行后继写入和安全 GC，真实业务 ACK 受有界队列背压约束。 */
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

    /** 队列保存原跨片分条身份，后继只在两片最终化后写入。 */
    private record SuccessorWork(long txId, long localTxId,
                                 String leftKey, int leftStripe, long leftSequence,
                                 String rightKey, int rightStripe, long rightSequence) {
    }

    /** 后台状态同时记录剩余大版本、持久回执、后继积压和 GC 失败。 */
    record Snapshot(long leftMappings, long rightMappings, long leftReceipts, long rightReceipts,
                    long submitted, long completed, long gcRuns, long gcFailures,
                    long gcRetired, Throwable firstFailure) {
        /** 任何计数为负都表示观测结果无效。 */
        Snapshot {
            if (leftMappings < 0 || rightMappings < 0 || leftReceipts < 0 || rightReceipts < 0
                    || submitted < 0 || completed < 0 || gcRuns < 0 || gcFailures < 0
                    || gcRetired < 0) {
                throw new IllegalArgumentException("invalid steady cross probe snapshot");
            }
        }
    }

    /** 独立后台池绑定同一控制与两片权威，参数限制物化后继和回收的资源预算。 */
    MysqlVectorCrossSteadyBackground(MysqlLocalFirstPublicationLedger ledger,
                                     MysqlStorageAdapter left, MysqlStorageAdapter right,
                                     HikariDataSource center, HikariDataSource leftData,
                                     HikariDataSource rightData,
                                     MysqlVectorCutLeaderRuntime publisher,
                                     String controlUrl, String leftUrl, String rightUrl,
                                     String user, String password, int stripes,
                                     int gcParallelism, int gcRetireBatch,
                                     int gcCandidateLimit, int localEveryRounds,
                                     long gcIntervalMillis) {
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

    /** 恢复线程和切面发布者就绪后才启动有界后继及 GC 工作。 */
    void start() {
        gc.start();
        for (int index = 0; index < 16; index++) {
            successors.submit(this::runSuccessors);
        }
    }

    /** 业务 ACK 前将每笔后续同键更新放入有界队列，满队列使前台自然背压。 */
    void enqueue(long txId, MysqlStorageAdapter.PrewriteProof leftProof,
                 MysqlStorageAdapter.PrewriteProof rightProof) throws InterruptedException {
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

    /** 每个后继必须等中心确认全部参与片最终化，未决时不得覆盖意向。 */
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
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            if (accepting.get()) {
                firstFailure.compareAndSet(null, ex);
            }
        } catch (RuntimeException ex) {
            firstFailure.compareAndSet(null, ex);
            ex.printStackTrace(System.err);
        }
    }

    /** 未知响应按同一负事务号精确重放，存储过程会确认已有版本或重新提交。 */
    private static void commitSuccessorWithRetry(MysqlStorageAdapter adapter, long txId,
                                                 String key, int stripe, long startSequence) {
        Mutation mutation = Mutation.put(key, new byte[]{42});
        for (int attempt = 0; ; attempt++) {
            try {
                adapter.directCommitVectorLocal(txId, mutation, stripe, startSequence);
                return;
            } catch (MvccException ex) {
                if (attempt >= 7 || ex.errorCode() != ErrorCode.TRANSACTION_STATUS_UNKNOWN
                        && ex.errorCode() != ErrorCode.STORAGE_UNAVAILABLE
                        && ex.errorCode() != ErrorCode.LOCK_CONFLICT) {
                    throw ex;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(Math.min(32, 1L << attempt)));
            }
        }
    }

    /** 定时报告前先确认后台无故障，不能把失活调度器当成 0 积压。 */
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

    /** 前台结束后等待有界队列排空，失败或超时均使压测证据无效。 */
    void awaitSuccessors(Duration timeout) throws InterruptedException {
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

    /** 后继写入完成后等待旧映射清零，超时返回非零积压供生产判断。 */
    Snapshot awaitGcDrain(Duration timeout) throws Exception {
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

    /** 第一条后继或 GC 故障阻止业务把无界积压误认为成功 ACK。 */
    void requireHealthy() {
        Throwable failure = firstFailure.get();
        if (failure == null && gc.stats().failureCount() != 0) {
            failure = gc.lastFailure();
        }
        if (failure != null) {
            throw new IllegalStateException("steady cross background failed", failure);
        }
    }

    /** 释放后继线程、GC 工作线程和三套独立后台池。 */
    @Override
    public void close() {
        accepting.set(false);
        successors.shutdown();
        try {
            if (!successors.awaitTermination(5, TimeUnit.SECONDS)) {
                successors.shutdownNow();
            }
            gc.close();
        } catch (InterruptedException ex) {
            successors.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing steady cross background", ex);
        } finally {
            backgroundCenter.close();
            backgroundLeft.close();
            backgroundRight.close();
        }
    }

    /** GC 后台池与业务池的连接预算各自独立。 */
    private static HikariDataSource pool(String url, String user, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, user, password);
    }

    /** 表名只接受本探针固定的版本和凭据表。 */
    private static long count(HikariDataSource dataSource, String table) throws SQLException {
        if (!List.of("mvcc_vector_committed_v1", "mvcc_vector_retired_v1").contains(table)) {
            throw new IllegalArgumentException("unsupported steady cross count table");
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("select count(*) from " + table);
             ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("steady cross count returned no row");
            }
            return row.getLong(1);
        }
    }
}

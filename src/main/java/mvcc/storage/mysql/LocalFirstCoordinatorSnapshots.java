package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.metadata.BeginReservation;
import mvcc.publication.CoordinatorSnapshotLease;
import mvcc.publication.CoordinatorSnapshotView;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** 用一条持久协调器租约保护本地先提交协议的多笔 RC/RR 快照。 */
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

    /** 要求协调器租约覆盖完整事务期限并留有后台续租余量。 */
    LocalFirstCoordinatorSnapshots(MysqlLocalFirstPublicationLedger control,
                                   long transactionTimeoutMillis, long leaseMillis,
                                   long refreshIntervalMillis) {
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
        this.refresher = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-local-first-coordinator-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshIntervalMillis,
                refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /** 读栅栏保证快照登记先于续租提高最老活跃下界。 */
    BeginReservation begin(IsolationLevel isolationLevel) {
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
                } catch (MvccException ex) {
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

    /** 需要时对分片操作前复核控制库持久代数和数据库截止时间。 */
    void assertProtected(BeginReservation tx) {
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

    /** 事务完成后只删本地成员，单个事务不再写控制快照表。 */
    void release(long txId) {
        activeReadTs.remove(txId);
    }

    /** 段预留在排他成员栅栏下读取最老活跃快照，防止遗漏正在 begin 的旧 RR。 */
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

    /** 排他本地栅栏内把持久下界推进至最老活跃读水位。 */
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
            } catch (MvccException ex) {
                if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                    fenced = true;
                }
                throw ex;
            }
        } finally {
            membership.writeLock().unlock();
        }
    }

    /** 返回最近一次后台续租失败以便隔离基准停止错误地宣称成功。 */
    RuntimeException lastRefreshFailure() {
        return lastFailure.get();
    }

    /** 正常关闭只在本地没有活跃快照时删除协调器租约。 */
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

    /** 协调器持久编号与本地单调序列组成不会碰撞的负事务号。 */
    private long nextTransactionId(long coordinatorId) {
        long next = sequence.incrementAndGet();
        if (next > MAX_LOCAL_SEQUENCE) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "local-first coordinator transaction ids exhausted", false, null);
        }
        return Long.MIN_VALUE | (coordinatorId << 43) | next;
    }

    /** 失去当前持久代数后本协调器不再开始或验证事务。 */
    private void requireAvailable() {
        if (closed.get() || fenced) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "local-first coordinator lease is closed or fenced", false, null);
        }
    }

    /** 后台失败留证并由下一轮续租重试，不能静默丢失保护。 */
    private void refreshSafely() {
        try {
            refresh();
        } catch (RuntimeException ex) {
            lastFailure.set(ex);
        }
    }
}

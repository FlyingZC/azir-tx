package mvcc.txn;

import mvcc.api.Transaction;
import mvcc.common.BackgroundTaskStats;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.metadata.BeginReservation;
import mvcc.metadata.TxRecord;
import mvcc.publication.CoordinatorSnapshotLease;
import mvcc.publication.CoordinatorSnapshotView;
import mvcc.publication.mysql.MysqlPublicationLedger;

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

/** 用一个持久协调器租约保护本节点活跃快照，并按需提升写事务租约。 */
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

    /** 建立唯一持久协调器编号并启动有界周期续租。 */
    CoordinatorSnapshotRegistry(MysqlPublicationLedger foregroundLedger, MysqlPublicationLedger backgroundLedger,
                                long transactionTimeoutMillis, long leaseTimeoutMillis,
                                long refreshIntervalMillis) {
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
        this.refresher = Executors.newSingleThreadScheduledExecutor(runnable ->
                Thread.ofPlatform().daemon().name("mvcc-coordinator-snapshot-refresh").unstarted(runnable));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshIntervalMillis,
                refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /** 在本地读锁内完成数据库快照读取与活跃登记，返回不与正数租约碰撞的事务 ID。 */
    BeginReservation begin(IsolationLevel isolationLevel) {
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
                membershipLock.readLock().unlock();
            }
            refresh();
        }
        throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                "coordinator lease cannot cover a new transaction deadline", false, null);
    }

    /** 在本地读栅栏内原子落独立租约和写参与者元数据。 */
    TxRecord promoteAndRegister(Transaction tx, Set<String> participants) {
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

    /** 中心写元数据成功后，独立持久租约接管该事务的 GC 保护。 */
    void promotionPersisted(long txId) {
        activeReadTs.remove(txId);
    }

    /** 返回事务当前是否仍由协调器租约保护。 */
    boolean protects(long txId) {
        return activeReadTs.containsKey(txId);
    }

    /** 每次存储操作前复核持久代数和租约时限，防止失联协调器继续读写。 */
    void assertProtected(Transaction tx) {
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
            } catch (MvccException ex) {
                if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                    fenced = true;
                }
                throw ex;
            }
        } finally {
            membershipLock.readLock().unlock();
        }
    }

    /** 移除本地活跃快照，返回它是否仍由协调器租约而非独立租约保护。 */
    boolean release(long txId) {
        return activeReadTs.remove(txId) != null;
    }

    /** 在排他本地栅栏内把持久下界提高到最老活跃快照并续期。 */
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
            } catch (MvccException ex) {
                if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                    fenced = true;
                }
                throw ex;
            }
        } finally {
            membershipLock.writeLock().unlock();
        }
    }

    /** 返回周期续租成功和失败统计。 */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /** 返回最近一次周期续租异常。 */
    RuntimeException lastRefreshFailure() {
        return lastRefreshFailure.get();
    }

    /** 关闭后不接收新快照；若仍有本地事务则保留租约等待持久超时。 */
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

    /** 一个协议内编号由持久协调器 ID 与单调本地计数拼接，使用负数命名空间。 */
    private long nextTransactionId(long coordinatorId) {
        long sequence = localSequence.incrementAndGet();
        if (sequence > MAX_LOCAL_SEQUENCE) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "coordinator local transaction id namespace exhausted", false, null);
        }
        return Long.MIN_VALUE | (coordinatorId << 43) | sequence;
    }

    /** 协调器已关闭或失去持久代数时禁止继续服务。 */
    private void requireAvailable() {
        if (closed.get() || fenced) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "coordinator snapshot lease is closed or fenced", false, null);
        }
    }

    /** 续租失败不静默丢失，数据库临时故障仍由下次轮次重试。 */
    private void refreshSafely() {
        try {
            refresh();
            refreshStats.recordSuccess(1);
        } catch (RuntimeException ex) {
            lastRefreshFailure.set(ex);
            refreshStats.recordFailure();
        }
    }
}

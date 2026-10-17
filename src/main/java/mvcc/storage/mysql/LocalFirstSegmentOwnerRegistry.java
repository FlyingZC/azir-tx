package mvcc.storage.mysql;

import mvcc.common.BackgroundTaskStats;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger.SegmentReservation;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/** 为一个目标分片分配本地先提交槽，并周期维护中心与分片双侧 owner 租约。 */
final class LocalFirstSegmentOwnerRegistry implements AutoCloseable {
    private final MysqlLocalFirstPublicationLedger control;
    private final MysqlShardSegmentFence physical;
    private final MysqlLocalFirstPublicationLedger backgroundControl;
    private final MysqlShardSegmentFence backgroundPhysical;
    private final LongSupplier snapshotFloor;
    private final String ownerId = UUID.randomUUID().toString();
    private final int segmentSize;
    private final long leaseMillis;
    private final long idleSealNanos;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final ConcurrentHashMap<Long, SegmentState> segments = new ConcurrentHashMap<>();
    private final ScheduledExecutorService refresher;
    private final BackgroundTaskStats refreshStats = new BackgroundTaskStats();
    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong reservationSequence = new AtomicLong();
    private volatile SegmentState current;
    private String pendingRequestKey;
    private long pendingSnapshotFloor;

    /** 持久段、无锁段内序号与尚未完成中心终态的事务数。 */
    private static final class SegmentState {
        private volatile SegmentReservation reservation;
        private final AtomicLong nextTs;
        private final AtomicInteger unresolved = new AtomicInteger();
        private volatile long lastAssignedNanos = System.nanoTime();
        private volatile boolean retiring;
        private volatile boolean unsafe;

        /** 从该段第一个持久占位槽开始本地取号。 */
        private SegmentState(SegmentReservation reservation) {
            this.reservation = reservation;
            this.nextTs = new AtomicLong(reservation.lease().firstTs());
        }
    }

    /** 前台保留领取时的 owner 凭据，续租只改变截止时间而不改变代数。 */
    static final class Assignment {
        private final SegmentState state;
        private final SegmentReservation reservation;
        private final long commitTs;
        private final AtomicBoolean resolved = new AtomicBoolean();

        /** 保存唯一段内槽和当前持久 owner 身份。 */
        private Assignment(SegmentState state, SegmentReservation reservation, long commitTs) {
            this.state = state;
            this.reservation = reservation;
            this.commitTs = commitTs;
        }

        /** 返回中心批量完成及分片物理提交共用的原段身份。 */
        SegmentReservation reservation() {
            return reservation;
        }

        /** 返回本事务唯一提交时间戳。 */
        long commitTs() {
            return commitTs;
        }
    }

    /** 绑定单个目标分片和有界租约预算，后台只使用显式提供的数据库权威。 */
    LocalFirstSegmentOwnerRegistry(MysqlLocalFirstPublicationLedger control,
                                   MysqlShardSegmentFence physical, LongSupplier snapshotFloor,
                                   int segmentSize, long leaseMillis,
                                   long refreshMillis, long idleSealMillis) {
        this(control, control, physical, physical, snapshotFloor,
                segmentSize, leaseMillis, refreshMillis, idleSealMillis);
    }

    /** 前后台可使用独立连接池，路由身份必须一致且由调用方验证同一物理权威。 */
    LocalFirstSegmentOwnerRegistry(MysqlLocalFirstPublicationLedger control,
                                   MysqlLocalFirstPublicationLedger backgroundControl,
                                   MysqlShardSegmentFence physical,
                                   MysqlShardSegmentFence backgroundPhysical,
                                   LongSupplier snapshotFloor, int segmentSize, long leaseMillis,
                                   long refreshMillis, long idleSealMillis) {
        if (segmentSize < 1 || segmentSize > 4096 || leaseMillis < 3
                || leaseMillis > 86_400_000L || refreshMillis < 1
                || refreshMillis >= leaseMillis / 2 || idleSealMillis < 1) {
            throw new IllegalArgumentException("invalid local-first owner resource limits");
        }
        this.control = Objects.requireNonNull(control, "control");
        this.physical = Objects.requireNonNull(physical, "physical");
        this.backgroundControl = Objects.requireNonNull(backgroundControl, "backgroundControl");
        this.backgroundPhysical = Objects.requireNonNull(backgroundPhysical, "backgroundPhysical");
        if (!physical.shardId().equals(backgroundPhysical.shardId())) {
            throw new IllegalArgumentException("local-first owner background shard route differs");
        }
        this.snapshotFloor = Objects.requireNonNull(snapshotFloor, "snapshotFloor");
        this.segmentSize = segmentSize;
        this.leaseMillis = leaseMillis;
        this.idleSealNanos = TimeUnit.MILLISECONDS.toNanos(idleSealMillis);
        this.refresher = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-local-first-owner-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshMillis,
                refreshMillis, TimeUnit.MILLISECONDS);
    }

    /** 正常路径只做本地 CAS；旧快照低于段下界时退役当前段并另行预留。 */
    Assignment allocate(long startTs) {
        if (startTs < 0) {
            throw new IllegalArgumentException("invalid local-first start timestamp");
        }
        lock.readLock().lock();
        try {
            requireOpen();
            Assignment fast = tryAllocate(current, startTs);
            if (fast != null) {
                return fast;
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            requireOpen();
            while (true) {
                Assignment replay = tryAllocate(current, startTs);
                if (replay != null) {
                    return replay;
                }
                if (current != null) {
                    current.retiring = true;
                    current = null;
                }
                reserveNextSegment(startTs);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 前台确认自己的中心终态后解除本地未决计数，允许后续安全封尾。 */
    void complete(Assignment assignment) {
        resolve(assignment, false);
    }

    /** 分片候选已持久且整段领取完成时主动物理封段，再批量补中心终态。 */
    void completePhysicalAndSeal(Assignment assignment) {
        resolve(assignment, false);
        SegmentState state = assignment.state;
        if (!state.unsafe && state.unresolved.get() == 0
                && (state.retiring || state.nextTs.get() > state.reservation.lease().lastTs())) {
            sealKnown(state);
        }
    }

    /** 本地或中心结果未知时停止该段续租与新分配，交由失联恢复。 */
    void unknown(Assignment assignment) {
        resolve(assignment, true);
    }

    /** 显式轮次可用于测试双侧续租，也由后台定时线程调用。 */
    void refreshOnce() {
        for (SegmentState state : segments.values()) {
            if (state.unsafe) {
                if (backgroundControl.segment(state.reservation.lease().firstTs())
                        .filter(segment -> segment.status()
                                == MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED).isPresent()) {
                    segments.remove(state.reservation.lease().firstTs(), state);
                }
                continue;
            }
            boolean seal = state.unresolved.get() == 0
                    && (state.retiring || state.nextTs.get() > state.reservation.lease().lastTs()
                    || System.nanoTime() - state.lastAssignedNanos >= idleSealNanos);
            if (seal) {
                sealKnown(state);
                continue;
            }
            lock.readLock().lock();
            try {
                if (state.unsafe || state.retiring && state.unresolved.get() == 0) {
                    continue;
                }
                backgroundPhysical.renew(state.reservation.lease(), leaseMillis);
                state.reservation = backgroundControl.renewSegment(state.reservation);
                refreshStats.recordSuccess(1);
            } catch (RuntimeException ex) {
                state.unsafe = true;
                lastFailure.set(ex);
                refreshStats.recordFailure();
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /** 返回本轮物理与中心续租结果，异常不会静默变成有效租约。 */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /** 返回最近一次双侧续租或封段失败原因。 */
    RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /** 返回本协调器曾持久发起的分段预留次数。 */
    long reservationCount() {
        return reservationSequence.get();
    }

    /** CAS 失败仅重试当前段，不能误判为整段耗尽并制造 PENDING 缺口。 */
    private Assignment tryAllocate(SegmentState state, long startTs) {
        if (state == null || state.unsafe || state.retiring
                || startTs < state.reservation.snapshotFloor()) {
            return null;
        }
        while (true) {
            long next = state.nextTs.get();
            if (next > state.reservation.lease().lastTs() || next <= startTs) {
                return null;
            }
            if (state.nextTs.compareAndSet(next, next + 1)) {
                state.unresolved.incrementAndGet();
                state.lastAssignedNanos = System.nanoTime();
                return new Assignment(state, state.reservation, next);
            }
        }
    }

    /** 同一请求键在预留、分片安装或中心激活响应丢失后仍只重放原段。 */
    private void reserveNextSegment(long startTs) {
        if (pendingRequestKey == null) {
            pendingRequestKey = ownerId + ":" + reservationSequence.incrementAndGet();
            pendingSnapshotFloor = Math.min(startTs, snapshotFloor.getAsLong());
        }
        try {
            SegmentReservation reserved = control.reserveSegment(physical.shardId(), ownerId,
                    pendingRequestKey, segmentSize, leaseMillis, pendingSnapshotFloor);
            physical.install(reserved.lease(), leaseMillis);
            SegmentReservation active = control.activateSegment(reserved, physical);
            SegmentState state = new SegmentState(active);
            segments.put(active.lease().firstTs(), state);
            current = state;
            pendingRequestKey = null;
        } catch (MvccException ex) {
            if (ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                pendingRequestKey = null;
            }
            throw ex;
        }
    }

    /** 每个分配只释放一次；未知结果保持 unsafe，不能靠本地计数封存。 */
    private void resolve(Assignment assignment, boolean unknown) {
        Objects.requireNonNull(assignment, "assignment");
        if (assignment.resolved.compareAndSet(false, true)) {
            if (unknown) {
                assignment.state.unsafe = true;
            }
            assignment.state.unresolved.decrementAndGet();
        }
    }

    /** 先禁止本地分配，再按控制 FENCING、分片 fence、候选补齐与封尾顺序完成。 */
    private void sealKnown(SegmentState state) {
        lock.writeLock().lock();
        try {
            if (state.unsafe || state.unresolved.get() != 0
                    || segments.get(state.reservation.lease().firstTs()) != state) {
                return;
            }
            state.retiring = true;
            if (current == state) {
                current = null;
            }
        } finally {
            lock.writeLock().unlock();
        }
        try {
            SegmentReservation latest = backgroundControl.segment(state.reservation.lease().firstTs())
                    .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local-first active owner lost durable segment", false, null));
            var request = backgroundControl.beginFencing(latest, true);
            backgroundPhysical.fence(request.physicalLease());
            backgroundControl.sealFencedSegment(latest, backgroundPhysical);
            backgroundControl.advancePublishedFast(4096);
            segments.remove(latest.lease().firstTs(), state);
            refreshStats.recordSuccess(1);
        } catch (RuntimeException ex) {
            state.unsafe = true;
            lastFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /** 续租失败只留下持久过期段供独立恢复轮次处理。 */
    private void refreshSafely() {
        try {
            refreshOnce();
        } catch (RuntimeException ex) {
            lastFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /** 不再接受新分配；零未决段按相同 fencing 顺序主动封存。 */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        refresher.shutdownNow();
        try {
            refresher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        for (SegmentState state : segments.values()) {
            if (!state.unsafe && state.unresolved.get() == 0) {
                sealKnown(state);
            }
        }
    }

    /** 已关闭或丢失 owner 时禁止用旧段继续接受事务。 */
    private void requireOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local-first owner registry is closed", false, null);
        }
    }
}

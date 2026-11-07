package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.SegmentReservation;

import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
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

/** 为跨分片意向事务分配中心时间槽，并维护 owner 租约、快照下界和安全封尾。 */
final class LocalFirstIntentSegmentOwnerRegistry implements AutoCloseable {
    private final MysqlLocalFirstPublicationLedger control;
    private final MysqlLocalFirstPublicationLedger backgroundControl;
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

    /** 一段持久租约及尚未完成中心决议的本地领取计数。 */
    private static final class SegmentState {
        private volatile SegmentReservation reservation;
        private final AtomicLong nextTs;
        private final AtomicInteger unresolved = new AtomicInteger();
        private volatile long lastAssignedNanos = System.nanoTime();
        private volatile boolean retiring;
        private volatile boolean unsafe;

        /** 分配只从已持久预留的第一个槽开始。 */
        private SegmentState(SegmentReservation reservation) {
            this.reservation = reservation;
            this.nextTs = new AtomicLong(reservation.lease().firstTs());
        }
    }

    /** 前台保留领取时的原 owner 代数，续租不会改变槽身份。 */
    static final class Assignment {
        private final SegmentState state;
        private final SegmentReservation reservation;
        private final long commitTs;
        private final AtomicBoolean resolved = new AtomicBoolean();

        /** 唯一时间槽只绑定一个持久段。 */
        private Assignment(SegmentState state, SegmentReservation reservation, long commitTs) {
            this.state = state;
            this.reservation = reservation;
            this.commitTs = commitTs;
        }

        /** 返回中心批量决议必须核对的段身份。 */
        SegmentReservation reservation() { return reservation; }

        /** 返回本事务唯一提交时间戳。 */
        long commitTs() { return commitTs; }
    }

    /** 前后台可复用控制实例；生产连接池身份应由外层入口先核对。 */
    LocalFirstIntentSegmentOwnerRegistry(MysqlLocalFirstPublicationLedger control,
                                         LongSupplier snapshotFloor, int segmentSize,
                                         long leaseMillis, long refreshMillis, long idleSealMillis) {
        this(control, control, snapshotFloor, segmentSize, leaseMillis, refreshMillis, idleSealMillis);
    }

    /** 限制段容量、数据库租约与后台续租周期，避免一段长期钉住安全点。 */
    LocalFirstIntentSegmentOwnerRegistry(MysqlLocalFirstPublicationLedger control,
                                         MysqlLocalFirstPublicationLedger backgroundControl,
                                         LongSupplier snapshotFloor, int segmentSize,
                                         long leaseMillis, long refreshMillis, long idleSealMillis) {
        if (segmentSize < 1 || segmentSize > 4096 || leaseMillis < 3
                || leaseMillis > 86_400_000L || refreshMillis < 1
                || refreshMillis >= leaseMillis / 2 || idleSealMillis < 1) {
            throw new IllegalArgumentException("invalid cross-shard intent owner resource limits");
        }
        this.control = Objects.requireNonNull(control, "control");
        this.backgroundControl = Objects.requireNonNull(backgroundControl, "backgroundControl");
        this.snapshotFloor = Objects.requireNonNull(snapshotFloor, "snapshotFloor");
        this.segmentSize = segmentSize;
        this.leaseMillis = leaseMillis;
        this.idleSealNanos = TimeUnit.MILLISECONDS.toNanos(idleSealMillis);
        this.refresher = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-intent-owner-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshMillis,
                refreshMillis, TimeUnit.MILLISECONDS);
    }

    /** 当前段可用时无锁 CAS 取号，旧读快照低于段下界则换新段。 */
    Assignment allocate(long startTs) {
        if (startTs < 0) {
            throw new IllegalArgumentException("invalid cross-shard start timestamp");
        }
        lock.readLock().lock();
        try {
            requireOpen();
            Assignment assigned = tryAllocate(current, startTs);
            if (assigned != null) {
                return assigned;
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            requireOpen();
            while (true) {
                Assignment assigned = tryAllocate(current, startTs);
                if (assigned != null) {
                    return assigned;
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

    /** 同一事务组连续领取时间槽，避免并发组的提交槽互相交错。 */
    List<Assignment> allocateBatch(List<Long> startTimestamps) {
        if (startTimestamps == null || startTimestamps.isEmpty()
                || startTimestamps.size() > 64 || startTimestamps.stream().anyMatch(
                timestamp -> timestamp == null || timestamp < 0)) {
            throw new IllegalArgumentException("invalid cross-shard intent owner batch");
        }
        long minimumStart = startTimestamps.stream().mapToLong(Long::longValue).min().orElseThrow();
        long maximumStart = startTimestamps.stream().mapToLong(Long::longValue).max().orElseThrow();
        lock.writeLock().lock();
        try {
            requireOpen();
            while (true) {
                SegmentState state = current;
                if (state != null && !state.unsafe && !state.retiring
                        && minimumStart >= state.reservation.snapshotFloor()) {
                    long next = state.nextTs.get();
                    if (next > maximumStart && next <= Long.MAX_VALUE - startTimestamps.size()
                            && next + startTimestamps.size() - 1 <= state.reservation.lease().lastTs()) {
                        state.nextTs.set(next + startTimestamps.size());
                        state.unresolved.addAndGet(startTimestamps.size());
                        state.lastAssignedNanos = System.nanoTime();
                        List<Assignment> assigned = new ArrayList<>(startTimestamps.size());
                        for (int index = 0; index < startTimestamps.size(); index++) {
                            assigned.add(new Assignment(state, state.reservation, next + index));
                        }
                        return List.copyOf(assigned);
                    }
                }
                if (state != null) {
                    state.retiring = true;
                    current = null;
                }
                reserveNextSegment(minimumStart);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 中心决议已落盘后释放未决计数，使空段可安全封尾。 */
    void complete(Assignment assignment) {
        resolve(assignment, false);
    }

    /** 提交结果未知时停止旧 owner 新分配，由持久到期恢复判断槽终态。 */
    void unknown(Assignment assignment) {
        resolve(assignment, true);
    }

    /** 显式轮次和定时线程共用相同的中心续租、封尾及失联逻辑。 */
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

    /** 返回后台续租和主动封段统计。 */
    BackgroundTaskStats refreshStats() { return refreshStats; }

    /** 返回最近一次续租或封段失败。 */
    RuntimeException lastFailure() { return lastFailure.get(); }

    /** 返回曾持久预留的段数供容量和 GC 长跑观测。 */
    long reservationCount() { return reservationSequence.get(); }

    /** CAS 失败只重试当前槽，不把仍可领取的尾段误判耗尽。 */
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

    /** 同一请求键在预留或激活响应丢失时只重放原段。 */
    private void reserveNextSegment(long startTs) {
        if (pendingRequestKey == null) {
            pendingRequestKey = ownerId + ":" + reservationSequence.incrementAndGet();
            pendingSnapshotFloor = Math.min(startTs, snapshotFloor.getAsLong());
        }
        try {
            SegmentReservation reserved = control.reserveSegment(
                    MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE, ownerId,
                    pendingRequestKey, segmentSize, leaseMillis, pendingSnapshotFloor);
            SegmentReservation active = control.activateIntentSegment(reserved);
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

    /** 一个分配只释放一次，未知结果把原段留给到期恢复。 */
    private void resolve(Assignment assignment, boolean unknown) {
        Objects.requireNonNull(assignment, "assignment");
        if (assignment.resolved.compareAndSet(false, true)) {
            if (unknown) {
                assignment.state.unsafe = true;
            }
            assignment.state.unresolved.decrementAndGet();
        }
    }

    /** 先禁止新领取，再以中心 FENCING、空尾中止和连续发布完成封段。 */
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
                            "cross-shard intent owner lost durable segment", false, null));
            backgroundControl.beginFencing(latest, true);
            backgroundControl.sealIntentSegment(latest);
            backgroundControl.advancePublishedFast(4096);
            segments.remove(latest.lease().firstTs(), state);
            refreshStats.recordSuccess(1);
        } catch (RuntimeException ex) {
            state.unsafe = true;
            lastFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /** 定时失败保留证据，下一轮恢复不得凭本地猜测续命旧代数。 */
    private void refreshSafely() {
        try {
            refreshOnce();
        } catch (RuntimeException ex) {
            lastFailure.set(ex);
            refreshStats.recordFailure();
        }
    }

    /** 正常停机只封存已无未决中心提交的段。 */
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

    /** 关闭后的旧 owner 不再分配任何时间槽。 */
    private void requireOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "cross-shard intent owner is closed", false, null);
        }
    }
}

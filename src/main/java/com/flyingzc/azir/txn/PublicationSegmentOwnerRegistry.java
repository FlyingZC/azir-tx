package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.publication.PublicationSegmentCommit;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** 本地跨批次分配持久时间槽，并协调决议、owner 续租与安全封存。 */
final class PublicationSegmentOwnerRegistry implements AutoCloseable {
    private final MysqlSegmentPublicationLedger foregroundLedger;
    private final MysqlSegmentPublicationLedger backgroundLedger;
    private final String ownerId = UUID.randomUUID().toString();
    private final int segmentSize;
    private final long leaseMillis;
    private final long idleSealNanos;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<Long, SegmentState> segments = new HashMap<>();
    private final ScheduledExecutorService refresher;
    private final BackgroundTaskStats refreshStats = new BackgroundTaskStats();
    private final AtomicReference<RuntimeException> lastRefreshFailure = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile SegmentState current;
    private String pendingReservationKey;
    private long reservationSequence;

    /** 保存一段当前持久代数、下一个本地槽与尚未证实终态的数量。 */
    private static final class SegmentState {
        private volatile PublicationSegmentLease lease;
        private final AtomicInteger unresolved = new AtomicInteger();
        private final AtomicLong nextTs;
        private volatile long lastAssignedNanos;
        private volatile boolean unsafe;

        /** 从持久预留的第一槽开始分配。 */
        private SegmentState(PublicationSegmentLease lease) {
            this.lease = lease;
            this.nextTs = new AtomicLong(lease.firstTs());
        }
    }

    /** 一笔业务提交已领取但还没有终态证明的时间槽。 */
    static final class Assignment {
        private final long segmentStart;
        private final long commitTs;
        private final AtomicBoolean resolved = new AtomicBoolean();

        /** 保存分段身份与全局提交时间戳。 */
        private Assignment(long segmentStart, long commitTs) {
            this.segmentStart = segmentStart;
            this.commitTs = commitTs;
        }

        /** 返回持久分段的第一时间戳。 */
        long segmentStart() {
            return segmentStart;
        }

        /** 返回此事务唯一领取的提交时间戳。 */
        long commitTs() {
            return commitTs;
        }
    }

    /** 一笔已预写事务及其本地分配的待决议时间槽。 */
    record CommitRequest(Assignment assignment, long txId, long deadlineMillis, Set<String> participants) {
        /** 固定参与者身份，避免入队后业务线程修改写集合。 */
        CommitRequest {
            Objects.requireNonNull(assignment, "assignment");
            participants = Set.copyOf(participants);
        }
    }

    /** 建立受限分段、后台租约期限和正常空闲封存周期。 */
    PublicationSegmentOwnerRegistry(MysqlSegmentPublicationLedger foregroundLedger,
                                    MysqlSegmentPublicationLedger backgroundLedger,
                                    int segmentSize, long leaseMillis,
                                    long refreshMillis, long idleSealMillis) {
        if (segmentSize < 1 || segmentSize > 4096 || leaseMillis < 3
                || leaseMillis > 86_400_000 || refreshMillis < 1
                || refreshMillis >= leaseMillis / 2 || idleSealMillis < 1) {
            throw new IllegalArgumentException("invalid segment owner resource limits");
        }
        this.foregroundLedger = Objects.requireNonNull(foregroundLedger, "foregroundLedger");
        this.backgroundLedger = Objects.requireNonNull(backgroundLedger, "backgroundLedger");
        this.segmentSize = segmentSize;
        this.leaseMillis = leaseMillis;
        this.idleSealNanos = TimeUnit.MILLISECONDS.toNanos(idleSealMillis);
        this.refresher = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-segment-owner-refresh").unstarted(task));
        refresher.scheduleWithFixedDelay(this::refreshSafely, refreshMillis,
                refreshMillis, TimeUnit.MILLISECONDS);
    }

    /** 仅在本地段耗尽时持久预留新范围，事务编号与时间槽仍彼此独立。 */
    Assignment allocate(long startTs) {
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
                if (current == null || current.nextTs.get() > current.lease.lastTs() || current.unsafe) {
                    reserveNextSegment();
                }
                SegmentState segment = current;
                Assignment allocated = tryAllocate(segment, startTs);
                if (allocated != null) {
                    return allocated;
                }
                if (segment.nextTs.get() <= startTs) {
                    if (segment.unresolved.get() != 0) {
                        segment.unsafe = true;
                        throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                                "segment contains timestamps older than transaction snapshot", false, null);
                    }
                    backgroundLedger.sealOwnedSegment(segment.lease);
                    segments.remove(segment.lease.firstTs());
                    current = null;
                    continue;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 共享栅栏下用 CAS 取得段内空槽，续租写栅栏仍等待登记完成。 */
    private Assignment tryAllocate(SegmentState segment, long startTs) {
        if (segment == null || segment.unsafe) {
            return null;
        }
        while (true) {
            long next = segment.nextTs.get();
            if (next > segment.lease.lastTs() || next <= startTs) {
                return null;
            }
            if (segment.nextTs.compareAndSet(next, next + 1)) {
                segment.unresolved.incrementAndGet();
                segment.lastAssignedNanos = System.nanoTime();
                return new Assignment(segment.lease.firstTs(), next);
            }
        }
    }

    /** 用当前代数写持久中心决议，异常时停止续租该段并交由恢复确认结果。 */
    long decide(Assignment assignment, long txId, long deadlineMillis, Set<String> participants) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                long commitTs = foregroundLedger.decideCommitted(segment.lease,
                        assignment.commitTs(), txId, deadlineMillis, participants);
                resolve(segment, assignment);
                return commitTs;
            } catch (RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 中心持久提交后返回分片物化凭据，未知结果仍 fencing 本地 owner。 */
    MysqlSegmentPublicationLedger.CommittedProof decideWithProof(Assignment assignment, long txId,
                                                                 long startTs, long deadlineMillis,
                                                                 Set<String> participants) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                var proof = foregroundLedger.decideCommittedWithProof(segment.lease,
                        assignment.commitTs(), txId, startTs, deadlineMillis, participants);
                resolve(segment, assignment);
                return proof;
            } catch (RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 同一分段的一批决议持本地读栅栏直到整批控制事务落盘。 */
    void decideBatch(List<CommitRequest> requests) {
        if (requests.isEmpty()) {
            return;
        }
        lock.readLock().lock();
        try {
            try {
                SegmentState segment = requireAssignment(requests.getFirst().assignment());
                List<PublicationSegmentCommit> durable = new java.util.ArrayList<>(requests.size());
                for (CommitRequest request : requests) {
                    if (requireAssignment(request.assignment()) != segment) {
                        throw new IllegalArgumentException("segmented decision batch mixed owner ranges");
                    }
                    durable.add(new PublicationSegmentCommit(segment.lease,
                            request.assignment().commitTs(), request.txId(),
                            request.deadlineMillis(), request.participants()));
                }
                foregroundLedger.decideCommittedBatch(durable);
                for (CommitRequest request : requests) {
                    resolve(segment, request.assignment());
                }
            } catch (RuntimeException ex) {
                // 批次可能已提交或已回滚，整体失去本地 owner 权限并交由持久恢复核对。
                for (CommitRequest request : requests) {
                    SegmentState segment = segments.get(request.assignment().segmentStart());
                    if (segment != null) {
                        segment.unsafe = true;
                        resolve(segment, request.assignment());
                    }
                }
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 首次物理提交前在当前 owner 代数内持久认领本地候选槽。 */
    long claimLocal(Assignment assignment, long txId, long deadlineMillis, String participant) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                return foregroundLedger.claimLocalPending(segment.lease,
                        assignment.commitTs(), txId, deadlineMillis, participant);
            } catch (RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 本地 COMMITTED 证明即使在 owner 过期后仍可补齐相同中心决议。 */
    boolean completeLocalCommit(Assignment assignment, ParticipantRecord local, String participant) {
        return completeLocal(assignment, local, participant, true);
    }

    /** 本地 ABORTED 证明释放候选时间槽而不覆盖可能存在的提交。 */
    boolean completeLocalAbort(Assignment assignment, ParticipantRecord local, String participant) {
        return completeLocal(assignment, local, participant, false);
    }

    /** 本地终态补决议不依赖当前 owner 代数，只依赖持久分片证明。 */
    private boolean completeLocal(Assignment assignment, ParticipantRecord local,
                                  String participant, boolean committed) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = segments.get(assignment.segmentStart());
            try {
                boolean changed = committed
                        ? foregroundLedger.completeLocalCommitAfterFencing(local,
                                assignment.commitTs(), participant)
                        : foregroundLedger.completeLocalAbortAfterFencing(local,
                                assignment.commitTs(), participant);
                if (segment != null) {
                    resolve(segment, assignment);
                }
                return changed;
            } catch (RuntimeException ex) {
                if (segment != null) {
                    segment.unsafe = true;
                    resolve(segment, assignment);
                }
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 已完成参与者 fencing 后在当前代数内原子中止此槽。 */
    boolean abort(Assignment assignment, long txId, long deadlineMillis) {
        Objects.requireNonNull(assignment, "assignment");
        lock.readLock().lock();
        try {
            SegmentState segment = requireAssignment(assignment);
            try {
                boolean aborted = foregroundLedger.abortReservedSlot(segment.lease,
                        assignment.commitTs(), txId, deadlineMillis);
                resolve(segment, assignment);
                return aborted;
            } catch (RuntimeException ex) {
                segment.unsafe = true;
                resolve(segment, assignment);
                throw ex;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 后台续租在本地排他栅栏内更新代数，读侧决议不会拿到混合代数。 */
    int refresh() {
        lock.writeLock().lock();
        try {
            if (closed.get()) {
                return 0;
            }
            long published = backgroundLedger.currentPublished();
            long now = System.nanoTime();
            int progressed = 0;
            for (SegmentState segment : List.copyOf(segments.values())) {
                if (segment.unsafe) {
                    continue;
                }
                boolean unused = segment == current && segment.nextTs.get() <= segment.lease.lastTs();
                if (unused && segment.unresolved.get() == 0
                        && now - segment.lastAssignedNanos >= idleSealNanos) {
                    backgroundLedger.sealOwnedSegment(segment.lease);
                    segments.remove(segment.lease.firstTs());
                    current = null;
                    progressed++;
                } else if (segment.unresolved.get() == 0 && !unused) {
                    segments.remove(segment.lease.firstTs());
                } else if (segment.lease.lastTs() > published) {
                    renewOrFence(segment);
                    progressed++;
                }
            }
            if (pendingReservationKey != null) {
                String pending = pendingReservationKey;
                try {
                    PublicationSegmentLease recovered = backgroundLedger.reserveSegment(ownerId,
                            pending, segmentSize, leaseMillis);
                    pendingReservationKey = null;
                    SegmentState segment = new SegmentState(recovered);
                    segments.put(recovered.firstTs(), segment);
                    current = segment;
                    // 未交付给前台的范围没有在途决议，应立即封存而非等租约自然到期。
                    backgroundLedger.sealOwnedSegment(recovered);
                    segments.remove(recovered.firstTs());
                    current = null;
                    progressed++;
                } catch (MvccException ex) {
                    if (ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN
                            || ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                        pendingReservationKey = null;
                        lastRefreshFailure.set(ex);
                    } else {
                        throw ex;
                    }
                }
            }
            return progressed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 返回后台代数更新和主动封存的轮次统计。 */
    BackgroundTaskStats refreshStats() {
        return refreshStats;
    }

    /** 返回最近一次后台续租或封存失败。 */
    RuntimeException lastRefreshFailure() {
        return lastRefreshFailure.get();
    }

    /** 关闭后停止新分配；已决议段封存尾槽，未知决议留待持久到期接管。 */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        refresher.shutdownNow();
        lock.writeLock().lock();
        try {
            RuntimeException failure = null;
            for (SegmentState segment : List.copyOf(segments.values())) {
                if (segment.unsafe || segment.unresolved.get() != 0) {
                    continue;
                }
                try {
                    backgroundLedger.sealOwnedSegment(segment.lease);
                } catch (RuntimeException ex) {
                    if (failure == null) {
                        failure = ex;
                    } else if (failure != ex) {
                        failure.addSuppressed(ex);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 同一个请求键在响应丢失后重试，不会重复预留时间范围。 */
    private void reserveNextSegment() {
        if (pendingReservationKey == null) {
            if (reservationSequence == Long.MAX_VALUE) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "segment owner reservation sequence exhausted", false, null);
            }
            pendingReservationKey = ownerId + ":" + ++reservationSequence;
        }
        try {
            PublicationSegmentLease lease = foregroundLedger.reserveSegment(ownerId,
                    pendingReservationKey, segmentSize, leaseMillis);
            pendingReservationKey = null;
            SegmentState segment = new SegmentState(lease);
            segments.put(lease.firstTs(), segment);
            current = segment;
        } catch (MvccException ex) {
            if (ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN
                    || ex.errorCode() == ErrorCode.TRANSACTION_ABORTED) {
                pendingReservationKey = null;
            }
            throw ex;
        }
    }

    /** 任何未完成分段在 owner 响应未知时停止续租，由持久到期接管终结。 */
    private void renewOrFence(SegmentState segment) {
        PublicationSegmentLease previous = segment.lease;
        try {
            segment.lease = backgroundLedger.renewSegment(previous, leaseMillis);
        } catch (RuntimeException ex) {
            try {
                PublicationSegmentLease durable = backgroundLedger.segmentLease(previous.firstTs()).orElse(null);
                if (durable != null && durable.ownerId().equals(ownerId)
                        && durable.lastTs() == previous.lastTs()
                        && durable.generation() >= previous.generation()) {
                    segment.lease = durable;
                } else {
                    segment.unsafe = true;
                }
            } catch (RuntimeException inspectFailure) {
                segment.unsafe = true;
                ex.addSuppressed(inspectFailure);
            }
            throw ex;
        }
    }

    /** 同一分段的提交和中止确认只能释放一次本地在途名额。 */
    private void resolve(SegmentState segment, Assignment assignment) {
        if (assignment.resolved.compareAndSet(false, true)) {
            segment.unresolved.decrementAndGet();
        }
    }

    /** 拒绝关闭、失联或已经交由恢复接管的时间槽。 */
    private SegmentState requireAssignment(Assignment assignment) {
        requireOpen();
        SegmentState segment = segments.get(assignment.segmentStart());
        if (segment == null || segment.unsafe || assignment.resolved.get()) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segment assignment has no active local owner", false, null);
        }
        return segment;
    }

    /** 关闭后的前台请求不再取得新的分段决议权。 */
    private void requireOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "segment owner registry is closed", false, null);
        }
    }

    /** 周期失败保留下一次重试机会并公开异常。 */
    private void refreshSafely() {
        try {
            refreshStats.recordSuccess(refresh());
        } catch (RuntimeException ex) {
            lastRefreshFailure.set(ex);
            refreshStats.recordFailure();
        }
    }
}

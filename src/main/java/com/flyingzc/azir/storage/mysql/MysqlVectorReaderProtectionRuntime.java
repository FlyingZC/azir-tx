package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/** 一个协调进程共用一条持久租约保护全部 RR 事务和在途 RC 语句。 */
final class MysqlVectorReaderProtectionRuntime implements AutoCloseable {
    private final MysqlVectorSnapshotLeaseStore leases;
    private final Supplier<MysqlStripedSnapshotOracle.Snapshot> publishedCut;
    private final String ownerId;
    private final long leaseMillis;
    private final long refreshMillis;
    private final int maxReaders;
    private final Map<UUID, MysqlStripedSnapshotOracle.Snapshot> active = new HashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private MysqlVectorSnapshotLeaseStore.Guard guard;
    private MysqlStripedSnapshotOracle.Snapshot protectedFloor;
    private Thread worker;
    private boolean serving;
    private boolean closed;
    private boolean releaseAfterReaders;

    /** 发布者只提供已确认切面，读者租约按进程聚合而非逐笔写中心。 */
    MysqlVectorReaderProtectionRuntime(MysqlVectorSnapshotLeaseStore leases,
                                       Supplier<MysqlStripedSnapshotOracle.Snapshot> publishedCut,
                                       String ownerId, long leaseMillis, long refreshMillis,
                                       int maxReaders) {
        this.leases = Objects.requireNonNull(leases, "leases");
        this.publishedCut = Objects.requireNonNull(publishedCut, "publishedCut");
        this.ownerId = Objects.requireNonNull(ownerId, "ownerId");
        if (leaseMillis < 200 || refreshMillis < 1 || refreshMillis >= leaseMillis / 2
                || maxReaders < 1 || maxReaders > 1_000_000) {
            throw new IllegalArgumentException("invalid vector reader runtime limits");
        }
        this.leaseMillis = leaseMillis;
        this.refreshMillis = refreshMillis;
        this.maxReaders = maxReaders;
    }

    /** 先持久保护当前切面，再允许本进程开始任何 RR 或 RC 读取。 */
    synchronized void start() {
        if (closed || worker != null) {
            throw new IllegalStateException("vector reader runtime already started or closed");
        }
        MysqlStripedSnapshotOracle.Snapshot initial = publishedCut.get();
        guard = leases.acquire(ownerId, initial, leaseMillis);
        protectedFloor = initial;
        serving = true;
        try {
            worker = Thread.ofVirtual().name("azir-vector-reader-protection").start(this::runLoop);
        } catch (RuntimeException | Error ex) {
            failClosed(ex);
            guard.close();
            throw ex;
        }
    }

    /** RR 持有该句柄直至事务结束，RC 持有它直至当前语句完成。 */
    synchronized ReadHandle beginRead() {
        requireServing();
        if (active.size() >= maxReaders) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "vector reader protection capacity exhausted", true, null);
        }
        try {
            MysqlStripedSnapshotOracle.Snapshot next = publishedCut.get();
            leases.requireAtLeast(next, protectedFloor);
            UUID id = UUID.randomUUID();
            active.put(id, next);
            try {
                requireServing();
                return new ReadHandle(id, next);
            } catch (RuntimeException ex) {
                active.remove(id);
                throw ex;
            }
        } catch (RuntimeException | Error ex) {
            failClosed(ex);
            throw ex;
        }
    }

    /** 周期续租把进程仍持有的最老切面写到中心，GC 才能安全推进。 */
    synchronized void refresh() {
        requireServing();
        try {
            MysqlStripedSnapshotOracle.Snapshot next = publishedCut.get();
            for (MysqlStripedSnapshotOracle.Snapshot held : active.values()) {
                next = leases.minimum(next, held);
            }
            leases.requireAtLeast(next, protectedFloor);
            guard.renew(next);
            protectedFloor = next;
        } catch (RuntimeException | Error ex) {
            failClosed(ex);
            throw ex;
        }
    }

    /** 守护线程失联或续租失败后立即使所有读句柄停止成功返回。 */
    private void runLoop() {
        while (true) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(refreshMillis));
            synchronized (this) {
                if (!serving) {
                    return;
                }
                if (Thread.interrupted()) {
                    failClosed(new InterruptedException("vector reader renewer interrupted"));
                    return;
                }
            }
            try {
                refresh();
            } catch (Throwable ex) {
                synchronized (this) {
                    if (serving) {
                        failClosed(ex);
                    }
                }
                return;
            }
        }
    }

    /** 暴露首个后台故障，不能把线程存活等同于租约有效。 */
    synchronized Throwable failure() {
        return failure.get();
    }

    /** 本地服务与数据库租约同时有效才可交付读结果。 */
    private void requireServing() {
        if (!serving || closed || guard == null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "vector reader protection is not serving", true, null);
        }
        guard.requireActive();
    }

    /** 失效先撤销本地资格，中心行自然到期前继续保护在途快照。 */
    private void failClosed(Throwable cause) {
        failure.compareAndSet(null, cause);
        serving = false;
        if (guard != null) {
            guard.revokeLocally();
        }
    }

    /** 关闭后不再开放新读；在途句柄未退出前不提前释放持久保护。 */
    @Override
    public void close() {
        Thread running;
        MysqlVectorSnapshotLeaseStore.Guard toRelease = null;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            serving = false;
            if (guard != null) {
                guard.revokeLocally();
                if (active.isEmpty()) {
                    toRelease = guard;
                } else {
                    releaseAfterReaders = true;
                }
            }
            running = worker;
        }
        if (running != null) {
            running.interrupt();
            try {
                running.join(5_000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        if (toRelease != null) {
            toRelease.close();
        }
    }

    /** 一个调用方持有的固定读切面需要在成功返回前后核对进程租约。 */
    final class ReadHandle implements AutoCloseable {
        private final UUID id;
        private final MysqlStripedSnapshotOracle.Snapshot snapshot;
        private boolean released;

        /** 句柄仅由进程保护运行时登记成功后返回。 */
        private ReadHandle(UUID id, MysqlStripedSnapshotOracle.Snapshot snapshot) {
            this.id = id;
            this.snapshot = snapshot;
        }

        /** 读取前后取用同一 RC 语句或 RR 事务向量。 */
        MysqlStripedSnapshotOracle.Snapshot snapshot() {
            requireActive();
            return snapshot;
        }

        /** 领导者或读者租约失效后不得把旧数据作为成功结果返回。 */
        void requireActive() {
            synchronized (MysqlVectorReaderProtectionRuntime.this) {
                if (released || !active.containsKey(id)) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "vector read handle was released", true, null);
                }
                requireServing();
            }
        }

        /** 最后一个旧读者退出后可立即释放已关闭进程的持久租约。 */
        @Override
        public void close() {
            MysqlVectorSnapshotLeaseStore.Guard toRelease = null;
            synchronized (MysqlVectorReaderProtectionRuntime.this) {
                if (released) {
                    return;
                }
                released = true;
                active.remove(id);
                if (closed && releaseAfterReaders && active.isEmpty()) {
                    releaseAfterReaders = false;
                    toRelease = guard;
                }
            }
            if (toRelease != null) {
                toRelease.close();
            }
        }
    }
}

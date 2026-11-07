package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/** 单领导者向量切面的后台续租与发布实验运行时，故障时立即关闭读和 ACK。 */
final class MysqlVectorCutLeaderRuntime implements AutoCloseable {
    private final DataSource center;
    private final Map<String, DataSource> shards;
    private final int stripes;
    private final LongSupplier decisionEpoch;
    private final MysqlVectorCutLeaderLease leases;
    private final String ownerId;
    private final long leaseMillis;
    private final long publishIntervalNanos;
    private final boolean productState;
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile MysqlVectorCutLeaderLease.LeaseGuard guard;
    private volatile MysqlPublishedVectorCut cut;
    private volatile Thread worker;
    private volatile boolean closed;

    /** 返回给读者的切面必须携带领导者代数，供外层路由拒绝旧代数响应。 */
    record PublishedView(long generation, MysqlStripedSnapshotOracle.Snapshot snapshot) {
        /** 不允许未初始化代数或缺失切面的成功响应。 */
        PublishedView {
            Objects.requireNonNull(snapshot, "snapshot");
            if (generation < 1) {
                throw new IllegalArgumentException("invalid vector leader generation");
            }
        }
    }

    /** 绑定固定分片拓扑、持久租约和后台发布周期。 */
    MysqlVectorCutLeaderRuntime(DataSource center, Map<String, DataSource> shards, int stripes,
                                LongSupplier decisionEpoch, MysqlVectorCutLeaderLease leases,
                                String ownerId, long leaseMillis, long publishIntervalMicros) {
        this(center, shards, stripes, decisionEpoch, leases,
                ownerId, leaseMillis, publishIntervalMicros, false);
    }

    /** 正式参与者运行时只读取真实分条表，并须绑定正式中心领导者租约。 */
    static MysqlVectorCutLeaderRuntime forProduct(DataSource center,
                                                   Map<String, DataSource> shards, int stripes,
                                                   LongSupplier decisionEpoch,
                                                   MysqlVectorCutLeaderLease leases,
                                                   String ownerId, long leaseMillis,
                                                   long publishIntervalMicros) {
        return new MysqlVectorCutLeaderRuntime(center, shards, stripes, decisionEpoch,
                leases, ownerId, leaseMillis, publishIntervalMicros, true);
    }

    /** 两种固定水位来源共用续租、故障关闭及客户端响应逻辑。 */
    private MysqlVectorCutLeaderRuntime(DataSource center, Map<String, DataSource> shards, int stripes,
                                        LongSupplier decisionEpoch, MysqlVectorCutLeaderLease leases,
                                        String ownerId, long leaseMillis, long publishIntervalMicros,
                                        boolean productState) {
        this.center = Objects.requireNonNull(center, "center");
        this.shards = Map.copyOf(shards);
        this.stripes = stripes;
        this.decisionEpoch = Objects.requireNonNull(decisionEpoch, "decisionEpoch");
        this.leases = Objects.requireNonNull(leases, "leases");
        if (leases.productMode() != productState) {
            throw new IllegalArgumentException("vector publisher source and lease namespace differ");
        }
        this.ownerId = Objects.requireNonNull(ownerId, "ownerId");
        if (leaseMillis < 200 || leaseMillis > 60_000
                || publishIntervalMicros < 50 || publishIntervalMicros > 100_000) {
            throw new IllegalArgumentException("invalid vector publisher runtime limits");
        }
        this.leaseMillis = leaseMillis;
        this.publishIntervalNanos = TimeUnit.MICROSECONDS.toNanos(publishIntervalMicros);
        this.productState = productState;
    }

    /** GC 调度器只读实际前台控制库连接身份。 */
    DataSource centerDataSource() {
        return center;
    }

    /** 后台分片必须与当前发布者的固定路由一一对应。 */
    Map<String, DataSource> shardSources() {
        return shards;
    }

    /** 只有正式向量数据源可以驱动产品本地版本 GC。 */
    boolean productMode() {
        return productState;
    }

    /** 读者租约与发布者必须使用相同分片集合及分条数。 */
    String layoutHash() {
        return MysqlPublishedVectorCut.layoutFingerprint(
                shards.keySet().stream().sorted().toList(), stripes);
    }

    /** 回收器分条数与发布者持久布局必须一致。 */
    int stripeCount() {
        return stripes;
    }

    /** 先取得中心代数再重建切面；已有领导者时返回 false 且不启动线程。 */
    synchronized boolean start() {
        if (closed || worker != null) {
            throw new IllegalStateException("vector publisher runtime was already started or closed");
        }
        Optional<MysqlVectorCutLeaderLease.LeaseGuard> acquired = leases.acquire(ownerId, leaseMillis);
        if (acquired.isEmpty()) {
            return false;
        }
        guard = acquired.get();
        try {
            cut = productState
                    ? MysqlPublishedVectorCut.leasedProduct(center, shards, stripes, decisionEpoch, guard)
                    : MysqlPublishedVectorCut.leasedVolatile(center, shards, stripes, decisionEpoch, guard);
            cut.initializeVolatile();
            active.set(true);
            worker = Thread.ofVirtual().name("azir-vector-cut-leader-" + guard.generation())
                    .start(this::runLoop);
            return true;
        } catch (RuntimeException | Error ex) {
            failClosed(ex);
            closed = true;
            if (cut != null) {
                cut.close();
            }
            throw ex;
        }
    }

    /** 后台按本地租约余量续租，任何发布或续租失败都撤销本代数。 */
    private void runLoop() {
        try {
            long renewalBoundary = TimeUnit.MILLISECONDS.toNanos(leaseMillis / 2);
            while (active.get()) {
                guard.requireActive();
                if (guard.remainingNanos() <= renewalBoundary) {
                    guard.renew();
                }
                cut.publishOnce();
                LockSupport.parkNanos(publishIntervalNanos);
                if (Thread.interrupted() && active.get()) {
                    throw new InterruptedException("vector publisher thread was interrupted");
                }
            }
        } catch (Throwable ex) {
            if (active.get()) {
                failClosed(ex);
            }
        }
    }

    /** 前台每次取 RC 切面都要验证当前运行时仍持有可用租约。 */
    PublishedView currentView() {
        requireServing();
        MysqlStripedSnapshotOracle.Snapshot snapshot = cut.currentCut();
        requireServing();
        return new PublishedView(guard.generation(), snapshot);
    }

    /** 已完成物理提交的事务在自己的序号被当前代数发布后才能 ACK。 */
    void awaitPublished(String shardId, int stripe, long sequence,
                        long minimumDecisionEpoch, long timeoutMillis) {
        requireServing();
        cut.awaitLocallyPublished(shardId, stripe, sequence, minimumDecisionEpoch, timeoutMillis);
        requireServing();
    }

    /** 只返回已证实运行的状态，不把旧线程存活等同于仍有服务资格。 */
    boolean isServing() {
        if (!active.get() || guard == null) {
            return false;
        }
        try {
            guard.requireActive();
            return true;
        } catch (MvccException ex) {
            return false;
        }
    }

    /** 发布失败证据供测试与运行监控读取。 */
    Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    /** 切面网络入口失效即撤销发布资格，禁止只有本地 ACK 而远端读不到新切面。 */
    void endpointUnavailable(Throwable cause) {
        Objects.requireNonNull(cause, "cause");
        if (active.get()) {
            failClosed(cause);
        }
    }

    /** 非当前代数、后台故障或显式关闭都拒绝成功读与确认。 */
    private void requireServing() {
        if (!active.get() || guard == null) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "vector publisher is not serving", true, null);
        }
        guard.requireActive();
    }

    /** 错误只记录一次，并先撤销本地资格再唤醒等待者。 */
    private void failClosed(Throwable cause) {
        failure.compareAndSet(null, cause);
        active.set(false);
        if (guard != null) {
            guard.revokeLocally();
        }
        if (cut != null) {
            cut.wakeWaiters();
        }
    }

    /** 停止本地读与 ACK，等待发布线程退出；中心租约自然到期后方可接任。 */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        active.set(false);
        if (guard != null) {
            guard.revokeLocally();
        }
        if (cut != null) {
            cut.wakeWaiters();
        }
        Thread running = worker;
        if (running != null) {
            running.interrupt();
            try {
                running.join(5_000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        if (cut != null) {
            cut.close();
        }
    }
}

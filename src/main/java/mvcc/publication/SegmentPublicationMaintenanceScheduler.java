package mvcc.publication;

import mvcc.common.BackgroundTaskStats;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 在独立后台连接预算上周期执行分段恢复与连续水位发布。 */
public final class SegmentPublicationMaintenanceScheduler implements AutoCloseable {
    private final SegmentPublicationRecoveryRunner recoveryRunner;
    private final MysqlSegmentPublicationLedger ledger;
    private final SegmentPublicationGcManager gcManager;
    private final int recoveryBatchSize;
    private final int publicationBatchSize;
    private final int gcBatchSize;
    private final long recoveryIntervalMillis;
    private final long publicationIntervalMillis;
    private final long gcIntervalMillis;
    private final boolean eventDrivenPublication;
    private final ScheduledExecutorService recoveryExecutor;
    private final ScheduledExecutorService publicationExecutor;
    private final ScheduledExecutorService gcExecutor;
    private final BackgroundTaskStats recoveryStats = new BackgroundTaskStats();
    private final BackgroundTaskStats publicationStats = new BackgroundTaskStats();
    private final BackgroundTaskStats gcStats = new BackgroundTaskStats();
    private final AtomicLong lastObservedPublished;
    private final AtomicReference<RuntimeException> lastRecoveryFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> lastPublicationFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> lastGcFailure = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean publicationRequestQueued = new AtomicBoolean();
    private final AtomicLong publicationRequestVersion = new AtomicLong();

    /** 要求有界批次和正周期，后台数据源由调用方独立配置。 */
    public SegmentPublicationMaintenanceScheduler(SegmentPublicationRecoveryRunner recoveryRunner,
                                                   MysqlSegmentPublicationLedger ledger,
                                                   int recoveryBatchSize, int publicationBatchSize,
                                                   long recoveryIntervalMillis, long publicationIntervalMillis) {
        this(recoveryRunner, ledger, null, recoveryBatchSize, publicationBatchSize, 1,
                recoveryIntervalMillis, publicationIntervalMillis, 1);
    }

    /** 完整运行时额外启用分片和中心共用安全点的周期 GC。 */
    public SegmentPublicationMaintenanceScheduler(SegmentPublicationRecoveryRunner recoveryRunner,
                                                   MysqlSegmentPublicationLedger ledger,
                                                   SegmentPublicationGcManager gcManager,
                                                   int recoveryBatchSize, int publicationBatchSize,
                                                   int gcBatchSize, long recoveryIntervalMillis,
                                                   long publicationIntervalMillis, long gcIntervalMillis) {
        this(recoveryRunner, ledger, gcManager, recoveryBatchSize, publicationBatchSize,
                gcBatchSize, recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis, false);
    }

    /** 可选唤醒复用同一发布线程，周期轮次仍兜底处理无前台等待的终态槽。 */
    public SegmentPublicationMaintenanceScheduler(SegmentPublicationRecoveryRunner recoveryRunner,
                                                   MysqlSegmentPublicationLedger ledger,
                                                   SegmentPublicationGcManager gcManager,
                                                   int recoveryBatchSize, int publicationBatchSize,
                                                   int gcBatchSize, long recoveryIntervalMillis,
                                                   long publicationIntervalMillis, long gcIntervalMillis,
                                                   boolean eventDrivenPublication) {
        if (recoveryBatchSize < 1 || recoveryBatchSize > 4096
                || publicationBatchSize < 1 || publicationBatchSize > 4096
                || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || publicationIntervalMillis < 1 || gcIntervalMillis < 1) {
            throw new IllegalArgumentException("invalid segmented maintenance limits");
        }
        this.recoveryRunner = Objects.requireNonNull(recoveryRunner, "recoveryRunner");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.gcManager = gcManager;
        this.lastObservedPublished = new AtomicLong(ledger.currentPublished());
        this.recoveryBatchSize = recoveryBatchSize;
        this.publicationBatchSize = publicationBatchSize;
        this.gcBatchSize = gcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.publicationIntervalMillis = publicationIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.eventDrivenPublication = eventDrivenPublication;
        this.recoveryExecutor = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-segment-recovery").unstarted(task));
        this.publicationExecutor = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-segment-publication").unstarted(task));
        this.gcExecutor = gcManager == null ? null : Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-segment-gc").unstarted(task));
    }

    /** 启动一次性调度，重复调用不叠加任务。 */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("segment maintenance scheduler is closed");
        }
        if (started.compareAndSet(false, true)) {
            recoveryExecutor.scheduleWithFixedDelay(this::recoverSafely, 0,
                    recoveryIntervalMillis, TimeUnit.MILLISECONDS);
            publicationExecutor.scheduleWithFixedDelay(this::publishSafely, 0,
                    publicationIntervalMillis, TimeUnit.MILLISECONDS);
            if (gcExecutor != null) {
                gcExecutor.scheduleWithFixedDelay(this::gcSafely, 0,
                        gcIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    /** 返回有界恢复轮次的成功和失败统计。 */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /** 返回连续水位发布轮次的成功和失败统计。 */
    public BackgroundTaskStats publicationStats() {
        return publicationStats;
    }

    /** 仅返回已经完成数据库提交的最近发布水位，供前台等待确认。 */
    public long publishedWatermark() {
        return lastObservedPublished.get();
    }

    /** 已提交前台只投递一个合并唤醒，不在前台线程调用数据库发布操作。 */
    public void requestPublication() {
        if (!eventDrivenPublication || !started.get() || closed.get()) {
            return;
        }
        publicationRequestVersion.incrementAndGet();
        scheduleRequestedPublication();
    }

    /** 最多留一个待运行请求，执行期间的新决议在尾部补一次唤醒。 */
    private void scheduleRequestedPublication() {
        if (!publicationRequestQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            publicationExecutor.schedule(this::publishRequestedSafely, 1, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            publicationRequestQueued.set(false);
        }
    }

    /** 保留周期兜底，同时避免在上次扫描尾部丢失新提交的通知。 */
    private void publishRequestedSafely() {
        long observed = publicationRequestVersion.get();
        try {
            publishSafely();
        } finally {
            publicationRequestQueued.set(false);
            if (publicationRequestVersion.get() != observed && !closed.get()) {
                scheduleRequestedPublication();
            }
        }
    }

    /** 返回分段 GC 周期的完成与失败统计。 */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /** 返回最近一次恢复顶层异常供诊断。 */
    public RuntimeException lastRecoveryFailure() {
        return lastRecoveryFailure.get();
    }

    /** 返回最近一次发布异常供诊断。 */
    public RuntimeException lastPublicationFailure() {
        return lastPublicationFailure.get();
    }

    /** 返回最近一次 GC 异常供诊断。 */
    public RuntimeException lastGcFailure() {
        return lastGcFailure.get();
    }

    /** 停止新轮次并等待当前数据库任务结束。 */
    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recoveryExecutor.shutdown();
        publicationExecutor.shutdown();
        if (gcExecutor != null) {
            gcExecutor.shutdown();
        }
        awaitStop(recoveryExecutor);
        awaitStop(publicationExecutor);
        if (gcExecutor != null) {
            awaitStop(gcExecutor);
        }
    }

    /** 单轮恢复异常保留下一次周期机会并公开原因。 */
    private void recoverSafely() {
        try {
            recoveryStats.recordSuccess(recoveryRunner.recoverOnce(recoveryBatchSize));
        } catch (RuntimeException ex) {
            lastRecoveryFailure.set(ex);
            recoveryStats.recordFailure();
        }
    }

    /** 新槽终态落盘后即使前台退出，也继续尝试推进连续已发布前缀。 */
    private void publishSafely() {
        try {
            long published = ledger.advancePublished(publicationBatchSize);
            publicationStats.recordSuccess(Math.max(0, published - lastObservedPublished.getAndSet(published)));
        } catch (RuntimeException ex) {
            lastPublicationFailure.set(ex);
            publicationStats.recordFailure();
        }
    }

    /** GC 一轮失败保留原阶段与目标安全点，留待下一周期重试。 */
    private void gcSafely() {
        try {
            gcStats.recordSuccess(gcManager.gcOnce(gcBatchSize));
        } catch (RuntimeException ex) {
            lastGcFailure.set(ex);
            gcStats.recordFailure();
        }
    }

    /** 有界等待后台轮次退出，数据库阻塞仍由连接池和驱动超时保护。 */
    private void awaitStop(ScheduledExecutorService executor) {
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

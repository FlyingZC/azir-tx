package mvcc.publication;

import mvcc.common.BackgroundTaskStats;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.storage.StorageAdapter;
import mvcc.storage.mysql.MysqlShardSegmentFence;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 在独立后台连接预算上调度本地先提交段恢复、候选清理和两侧安全点。 */
public final class LocalFirstPublicationMaintenanceScheduler implements AutoCloseable {
    /** 每轮最多回收一段容量级别的中心槽与 finalized 记录。 */
    private static final int CENTER_GC_BATCH_SIZE = 4096;
    private final MysqlLocalFirstPublicationLedger ledger;
    private final Map<String, MysqlShardSegmentFence> fences;
    private final Map<String, StorageAdapter> adapters;
    private final LocalFirstPublicationRecoveryManager recovery;
    private final LocalFirstIntentRecoveryManager intentRecovery;
    private final int segmentBatchSize;
    private final int candidateSegmentBatchSize;
    private final int shardGcBatchSize;
    private final long recoveryIntervalMillis;
    private final long gcIntervalMillis;
    private final long retentionMillis;
    private final ScheduledExecutorService recoveryExecutor;
    private final ScheduledExecutorService gcExecutor;
    private final BackgroundTaskStats recoveryStats = new BackgroundTaskStats();
    private final BackgroundTaskStats gcStats = new BackgroundTaskStats();
    private final AtomicLong sealedSegments = new AtomicLong();
    private final AtomicLong purgedCandidates = new AtomicLong();
    private final AtomicLong retiredFences = new AtomicLong();
    private final AtomicReference<RuntimeException> lastRecoveryFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> lastGcFailure = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private long fenceGcAfterSegmentStart;

    /** 限定每轮接管、整段候选清理、分片 GC 和后台执行周期。 */
    public LocalFirstPublicationMaintenanceScheduler(MysqlLocalFirstPublicationLedger ledger,
                                                     Map<String, MysqlShardSegmentFence> fences,
                                                     Map<String, StorageAdapter> adapters,
                                                     int segmentBatchSize, int candidateSegmentBatchSize,
                                                     int shardGcBatchSize, long recoveryIntervalMillis,
                                                     long gcIntervalMillis) {
        this(ledger, fences, adapters, segmentBatchSize, candidateSegmentBatchSize,
                shardGcBatchSize, recoveryIntervalMillis, gcIntervalMillis, 60_000);
    }

    /** 显式保留窗口使中心槽与元数据回收仍遵守业务审计期限。 */
    public LocalFirstPublicationMaintenanceScheduler(MysqlLocalFirstPublicationLedger ledger,
                                                     Map<String, MysqlShardSegmentFence> fences,
                                                     Map<String, StorageAdapter> adapters,
                                                     int segmentBatchSize, int candidateSegmentBatchSize,
                                                     int shardGcBatchSize, long recoveryIntervalMillis,
                                                     long gcIntervalMillis, long retentionMillis) {
        if (segmentBatchSize < 1 || segmentBatchSize > 4096
                || candidateSegmentBatchSize < 1 || candidateSegmentBatchSize > 4096
                || shardGcBatchSize < 1 || shardGcBatchSize > 4096
                || recoveryIntervalMillis < 1 || gcIntervalMillis < 1
                || retentionMillis < 0 || retentionMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid local-first maintenance limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.fences = Map.copyOf(fences);
        this.adapters = Map.copyOf(adapters);
        if (this.fences.isEmpty() || !this.fences.keySet().equals(this.adapters.keySet())) {
            throw new IllegalArgumentException("local-first maintenance requires matching shard routes");
        }
        this.fences.forEach((shardId, fence) -> {
            if (!shardId.equals(fence.shardId()) || !shardId.equals(this.adapters.get(shardId).shardId())) {
                throw new IllegalArgumentException("local-first maintenance route differs from shard storage");
            }
        });
        this.recovery = new LocalFirstPublicationRecoveryManager(ledger, this.fences);
        this.intentRecovery = new LocalFirstIntentRecoveryManager(ledger, this.adapters);
        this.segmentBatchSize = segmentBatchSize;
        this.candidateSegmentBatchSize = candidateSegmentBatchSize;
        this.shardGcBatchSize = shardGcBatchSize;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.gcIntervalMillis = gcIntervalMillis;
        this.retentionMillis = retentionMillis;
        this.recoveryExecutor = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-local-first-recovery").unstarted(task));
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().daemon().name("mvcc-local-first-gc").unstarted(task));
    }

    /** 可重复调用的启动只创建一组恢复和 GC 周期任务。 */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("local-first maintenance scheduler is closed");
        }
        if (started.compareAndSet(false, true)) {
            recoveryExecutor.scheduleWithFixedDelay(this::recoverSafely, recoveryIntervalMillis,
                    recoveryIntervalMillis, TimeUnit.MILLISECONDS);
            gcExecutor.scheduleWithFixedDelay(this::gcSafely, gcIntervalMillis,
                    gcIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    /** 单轮先处理物理 fencing，再发布连续前缀及整段候选回收。 */
    public LocalFirstPublicationRecoveryManager.RecoveryResult recoverOnce() {
        var result = recovery.recoverOnce(segmentBatchSize, candidateSegmentBatchSize);
        // 先封存失联段，再按中心终态补齐跨分片物化或到期回滚。
        var intentResult = intentRecovery.recoverOnce(candidateSegmentBatchSize);
        sealedSegments.addAndGet(result.sealedSegments());
        purgedCandidates.addAndGet(result.purgedCandidates());
        int failures = result.failures() + intentResult.failures();
        if (failures == 0) {
            recoveryStats.recordSuccess(result.sealedSegments() + result.purgedCandidates()
                    + intentResult.finalized() + intentResult.aborted());
        } else {
            for (int index = 0; index < failures; index++) {
                recoveryStats.recordFailure();
            }
            lastRecoveryFailure.set(intentResult.failures() == 0
                    ? recovery.lastFailure() : intentRecovery.lastFailure());
        }
        return result;
    }

    /** 控制安全点先受段与协调器下界限制，分片再受尚存候选额外限制。 */
    public int gcOnce() {
        ledger.expireSnapshots(candidateSegmentBatchSize);
        ledger.expireCoordinatorLeases(candidateSegmentBatchSize);
        long controlSafe = ledger.advanceSafePoint();
        int reclaimed = 0;
        for (String shardId : fences.keySet().stream().sorted().toList()) {
            long localSafe = fences.get(shardId).oldestCandidateStartTs()
                    .stream().map(candidate -> Math.min(controlSafe, candidate))
                    .findFirst().orElse(controlSafe);
            reclaimed += adapters.get(shardId).gc(localSafe, shardGcBatchSize);
        }
        long now = ledger.currentDatabaseTimeMillis();
        long cutoff = now < retentionMillis ? 0 : now - retentionMillis;
        reclaimed += ledger.purgeTerminalSlots(controlSafe, cutoff, CENTER_GC_BATCH_SIZE);
        reclaimed += ledger.purgeFinalizedMetadata(controlSafe, cutoff, CENTER_GC_BATCH_SIZE);
        var retirable = ledger.findRetirableFences(fenceGcAfterSegmentStart, candidateSegmentBatchSize);
        fenceGcAfterSegmentStart = retirable.size() < candidateSegmentBatchSize
                ? 0 : retirable.getLast().lease().firstTs();
        for (var segment : retirable) {
            var proof = ledger.fenceGcProof(segment.lease().firstTs());
            if (proof.isEmpty()) {
                continue;
            }
            MysqlShardSegmentFence physical = fences.get(proof.get().shardId());
            if (physical == null) {
                throw new IllegalStateException("local-first fence GC lost shard route");
            }
            physical.purgeFence(proof.get());
            if (ledger.markFenceRetired(proof.get(), physical)) {
                retiredFences.incrementAndGet();
                reclaimed++;
            }
        }
        reclaimed += ledger.purgeSealedSegments(controlSafe, cutoff, CENTER_GC_BATCH_SIZE);
        gcStats.recordSuccess(reclaimed);
        return reclaimed;
    }

    /** 发布给运维的成功和失败轮次不依赖前台内存猜测。 */
    public BackgroundTaskStats recoveryStats() {
        return recoveryStats;
    }

    /** 返回 GC 完成与失败轮次。 */
    public BackgroundTaskStats gcStats() {
        return gcStats;
    }

    /** 返回已持久封存的段数。 */
    public long sealedSegments() {
        return sealedSegments.get();
    }

    /** 返回已从分片回收并在中心标记的候选数量。 */
    public long purgedCandidates() {
        return purgedCandidates.get();
    }

    /** 返回已用持久退役水位安全压缩的物理 fence 行数。 */
    public long retiredFences() {
        return retiredFences.get();
    }

    /** 返回最近一次恢复失败。 */
    public RuntimeException lastRecoveryFailure() {
        return lastRecoveryFailure.get();
    }

    /** 返回最近一次 GC 失败。 */
    public RuntimeException lastGcFailure() {
        return lastGcFailure.get();
    }

    /** 周期任务保留异常证据，不让调度器吞掉下一轮重试机会。 */
    private void recoverSafely() {
        try {
            recoverOnce();
        } catch (RuntimeException ex) {
            lastRecoveryFailure.set(ex);
            recoveryStats.recordFailure();
        }
    }

    /** 单独的 GC 线程不会因为恢复数据库等待而错过所有周期。 */
    private void gcSafely() {
        try {
            gcOnce();
        } catch (RuntimeException ex) {
            lastGcFailure.set(ex);
            gcStats.recordFailure();
        }
    }

    /** 先停止新轮次，再在有界期限内回卷清理正常停机后的尾段候选。 */
    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recoveryExecutor.shutdown();
        gcExecutor.shutdown();
        awaitStop(recoveryExecutor);
        awaitStop(gcExecutor);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (int round = 0; round < 64 && System.nanoTime() < deadline; round++) {
            try {
                if (ledger.findRecoverableSegments(0, 1).isEmpty()
                        && ledger.findPurgeableSegments(0, 1).isEmpty()) {
                    break;
                }
                var result = recoverOnce();
                if (result.failures() > 0) {
                    break;
                }
            } catch (RuntimeException ex) {
                lastRecoveryFailure.set(ex);
                recoveryStats.recordFailure();
                break;
            }
        }
        try {
            if (!ledger.findRecoverableSegments(0, 1).isEmpty()
                    || !ledger.findPurgeableSegments(0, 1).isEmpty()) {
                lastRecoveryFailure.set(new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local-first shutdown left durable recovery work for restart", false, null));
                recoveryStats.recordFailure();
            }
        } catch (RuntimeException ex) {
            lastRecoveryFailure.set(ex);
            recoveryStats.recordFailure();
        }
        // 先把最新已发布前缀反映进安全点，避免漏掉刚满足退役条件的尾段。
        gcSafely();
        long gcDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (int round = 0; round < 64 && System.nanoTime() < gcDeadline; round++) {
            try {
                if (ledger.findRetirableFences(0, 1).isEmpty()) {
                    break;
                }
                gcOnce();
            } catch (RuntimeException ex) {
                lastGcFailure.set(ex);
                gcStats.recordFailure();
                break;
            }
        }
        try {
            if (!ledger.findRetirableFences(0, 1).isEmpty()) {
                lastGcFailure.set(new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local-first shutdown left eligible physical fences for restart", false, null));
                gcStats.recordFailure();
            }
        } catch (RuntimeException ex) {
            lastGcFailure.set(ex);
            gcStats.recordFailure();
        }
        gcSafely();
    }

    /** 后台连接超时由数据源和驱动负责，关闭最多合作等待五秒。 */
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

package com.flyingzc.azir.publication;

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** 有界轮转扫描分段到期 owner、未物化终态和无槽的过期事务。 */
public final class SegmentPublicationRecoveryRunner {
    private final MysqlSegmentPublicationLedger ledger;
    private final SegmentPublicationRecoveryManager recoveryManager;
    private final TransactionMetadataStore metadataStore;
    private final int maxTransactionsPerRun;
    private final long timeBudgetNanos;
    private final LongSupplier monotonicClock;
    private final BackgroundTaskStats transactionStats = new BackgroundTaskStats();
    private final BackgroundTaskStats scanStats = new BackgroundTaskStats();
    private final AtomicReference<RuntimeException> lastTransactionFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> lastScanFailure = new AtomicReference<>();
    private long terminalAfterCommitTs;
    private long localPendingAfterCommitTs;
    private long undecidedAfterTxId = Long.MIN_VALUE;
    private int scanCursor;

    /** 使用有界默认批次和一秒合作式轮次预算。 */
    public SegmentPublicationRecoveryRunner(MysqlSegmentPublicationLedger ledger,
                                            SegmentPublicationRecoveryManager recoveryManager,
                                            TransactionMetadataStore metadataStore) {
        this(ledger, recoveryManager, metadataStore, 256, 1_000, System::nanoTime);
    }

    /** 允许测试注入单轮数量、时间预算和单调时钟。 */
    public SegmentPublicationRecoveryRunner(MysqlSegmentPublicationLedger ledger,
                                            SegmentPublicationRecoveryManager recoveryManager,
                                            TransactionMetadataStore metadataStore,
                                            int maxTransactionsPerRun, long timeBudgetMillis,
                                            LongSupplier monotonicClock) {
        if (maxTransactionsPerRun < 1 || maxTransactionsPerRun > 4096 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid segmented recovery limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.recoveryManager = Objects.requireNonNull(recoveryManager, "recoveryManager");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.maxTransactionsPerRun = maxTransactionsPerRun;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /** 先 fencing 到期分段，再轮转恢复终态和过期未决事务并发布连续前缀。 */
    public synchronized int recoverOnce(int scanLimit) {
        if (scanLimit < 1 || scanLimit > 4096) {
            throw new IllegalArgumentException("invalid segmented recovery scan limit");
        }
        long started = monotonicClock.getAsLong();
        int completed = 0;
        try {
            completed += ledger.sealExpiredSegments(scanLimit);
            scanStats.recordSuccess(1);
        } catch (RuntimeException ex) {
            lastScanFailure.set(ex);
            scanStats.recordFailure();
        }
        long now = metadataStore.currentTimeMillis();
        LinkedHashSet<Long> candidates = new LinkedHashSet<>();
        for (int scanned = 0; scanned < 3 && candidates.size() < maxTransactionsPerRun
                && hasBudget(started); scanned++) {
            int category = scanCursor;
            scanCursor = (scanCursor + 1) % 3;
            int limit = Math.min(scanLimit, maxTransactionsPerRun - candidates.size());
            try {
                List<Long> found = switch (category) {
                    case 0 -> scanTerminal(limit);
                    case 1 -> scanExpiredUndecided(now, limit);
                    default -> scanLocalPending(now, limit);
                };
                candidates.addAll(found);
                scanStats.recordSuccess(found.size());
            } catch (RuntimeException ex) {
                lastScanFailure.set(ex);
                scanStats.recordFailure();
            }
        }
        for (long txId : candidates) {
            if (!hasBudget(started)) {
                break;
            }
            try {
                boolean recovered = recoveryManager.recover(txId);
                completed += recovered ? 1 : 0;
                transactionStats.recordSuccess(recovered ? 1 : 0);
            } catch (RuntimeException ex) {
                lastTransactionFailure.set(ex);
                transactionStats.recordFailure();
            }
        }
        // 未分配尾槽被封存后，即使没有事务编号也必须释放连续发布水位。
        ledger.advancePublished(256);
        return completed;
    }

    /** 已终态槽按提交时间戳分页，末尾回卷以重试暂时失败的参与者。 */
    private List<Long> scanTerminal(int limit) {
        List<PublicationEntry> entries = ledger.findUnfinalizedTerminal(terminalAfterCommitTs, limit);
        terminalAfterCommitTs = entries.size() < limit ? 0 : entries.getLast().commitTs();
        return entries.stream().map(PublicationEntry::txId).toList();
    }

    /** 无槽元数据按带符号事务编号分页，覆盖协调者分配的负事务号。 */
    private List<Long> scanExpiredUndecided(long nowMillis, int limit) {
        List<Long> found = ledger.findExpiredUndecided(nowMillis, undecidedAfterTxId, limit);
        undecidedAfterTxId = found.size() < limit ? Long.MIN_VALUE : found.getLast();
        return found;
    }

    /** 已认领本地候选按时间戳扫描，owner fencing 后即允许读取分片终态。 */
    private List<Long> scanLocalPending(long nowMillis, int limit) {
        List<PublicationEntry> entries = ledger.findRecoverableLocalPending(
                nowMillis, localPendingAfterCommitTs, limit);
        localPendingAfterCommitTs = entries.size() < limit ? 0 : entries.getLast().commitTs();
        return entries.stream().map(PublicationEntry::txId).toList();
    }

    /** 前后台数据库操作之间检查本轮合作式时间预算。 */
    private boolean hasBudget(long started) {
        return monotonicClock.getAsLong() - started < timeBudgetNanos;
    }

    /** 返回成功事务数与失败轮次统计。 */
    public BackgroundTaskStats transactionStats() {
        return transactionStats;
    }

    /** 返回扫描与 owner fencing 的成功失败统计。 */
    public BackgroundTaskStats scanStats() {
        return scanStats;
    }

    /** 返回最近一次事务恢复异常供运维定位。 */
    public RuntimeException lastTransactionFailure() {
        return lastTransactionFailure.get();
    }

    /** 返回最近一次扫描或 owner fencing 异常供运维定位。 */
    public RuntimeException lastScanFailure() {
        return lastScanFailure.get();
    }
}

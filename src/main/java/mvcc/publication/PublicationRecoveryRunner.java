package mvcc.publication;

import mvcc.common.BackgroundTaskStats;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.publication.mysql.MysqlPublicationLedger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.concurrent.atomic.AtomicReference;

/** 轮转发现新账本到期预留、未预留崩溃事务及未完成物化的终态。 */
public final class PublicationRecoveryRunner {
    private final MysqlPublicationLedger ledger;
    private final PublicationRecoveryManager recoveryManager;
    private final TransactionMetadataStore metadataStore;
    private final int maxTransactionsPerRun;
    private final long timeBudgetNanos;
    private final LongSupplier monotonicClock;
    private final BackgroundTaskStats transactionStats = new BackgroundTaskStats();
    private final BackgroundTaskStats scanStats = new BackgroundTaskStats();
    private final AtomicReference<RuntimeException> lastTransactionFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> lastScanFailure = new AtomicReference<>();
    private long pendingAfterCommitTs;
    private long unreservedAfterTxId = Long.MIN_VALUE;
    private long terminalAfterTxId = Long.MIN_VALUE;
    private int scanCursor;

    /** 使用有界默认批次和合作式时间预算构造恢复发现器。 */
    public PublicationRecoveryRunner(MysqlPublicationLedger ledger, PublicationRecoveryManager recoveryManager,
                                     TransactionMetadataStore metadataStore) {
        this(ledger, recoveryManager, metadataStore, 256, 1000, System::nanoTime);
    }

    /** 注入扫描上限和单轮预算，数据库调用仍由驱动超时保护。 */
    public PublicationRecoveryRunner(MysqlPublicationLedger ledger, PublicationRecoveryManager recoveryManager,
                                     TransactionMetadataStore metadataStore, int maxTransactionsPerRun,
                                     long timeBudgetMillis, LongSupplier monotonicClock) {
        if (maxTransactionsPerRun < 1 || maxTransactionsPerRun > 4096 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid publication recovery limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.recoveryManager = Objects.requireNonNull(recoveryManager, "recoveryManager");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.maxTransactionsPerRun = maxTransactionsPerRun;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /** 每轮从三个持久索引轮转取候选，单事务失败留待游标回卷后重试。 */
    public synchronized int recoverOnce(int perScanLimit) {
        if (perScanLimit < 1 || perScanLimit > 4096) {
            throw new IllegalArgumentException("publication recovery scan limit must be between 1 and 4096");
        }
        long started = monotonicClock.getAsLong();
        long now = metadataStore.currentTimeMillis();
        LinkedHashSet<Long> candidates = new LinkedHashSet<>();
        for (int scanned = 0; scanned < 3 && candidates.size() < maxTransactionsPerRun
                && hasBudget(started); scanned++) {
            int category = scanCursor;
            scanCursor = (scanCursor + 1) % 3;
            int limit = Math.min(perScanLimit, maxTransactionsPerRun - candidates.size());
            try {
                List<Long> found = switch (category) {
                    case 0 -> scanExpiredPending(now, limit);
                    case 1 -> scanExpiredUnreserved(now, limit);
                    default -> scanUnfinalizedTerminal(limit);
                };
                candidates.addAll(found);
                scanStats.recordSuccess(found.size());
            } catch (RuntimeException ex) {
                scanStats.recordFailure();
                lastScanFailure.set(ex);
            }
        }
        int recovered = 0;
        for (long txId : candidates) {
            if (!hasBudget(started)) {
                break;
            }
            try {
                boolean completed = recoveryManager.recover(txId);
                recovered += completed ? 1 : 0;
                transactionStats.recordSuccess(completed ? 1 : 0);
            } catch (RuntimeException ex) {
                transactionStats.recordFailure();
                lastTransactionFailure.set(ex);
            }
        }
        return recovered;
    }

    /** 按提交时间戳扫描到期 PENDING，游标到末尾后回卷。 */
    private List<Long> scanExpiredPending(long nowMillis, int limit) {
        List<PublicationEntry> entries = ledger.findExpiredPending(nowMillis, pendingAfterCommitTs, limit);
        pendingAfterCommitTs = entries.size() < limit ? 0 : entries.getLast().commitTs();
        return entries.stream().map(PublicationEntry::txId).toList();
    }

    /** 从元数据出发扫描无有效租约的未预留事务，覆盖 GC 先删租约的情况。 */
    private List<Long> scanExpiredUnreserved(long nowMillis, int limit) {
        List<Long> ids = ledger.findExpiredUnreserved(nowMillis, unreservedAfterTxId, limit);
        unreservedAfterTxId = ids.size() < limit ? Long.MIN_VALUE : ids.getLast();
        return ids;
    }

    /** 只扫描新账本未完成物化的终态，避免误接管迁移前旧协议记录。 */
    private List<Long> scanUnfinalizedTerminal(int limit) {
        List<Long> ids = ledger.findUnfinalizedTerminal(terminalAfterTxId, limit);
        terminalAfterTxId = ids.size() < limit ? Long.MIN_VALUE : ids.getLast();
        return ids;
    }

    /** 在相邻任务间检查单轮合作式预算。 */
    private boolean hasBudget(long started) {
        return monotonicClock.getAsLong() - started < timeBudgetNanos;
    }

    /** 返回逐事务恢复的成功量与失败量。 */
    public BackgroundTaskStats transactionStats() {
        return transactionStats;
    }

    /** 返回持久索引扫描的成功量与失败量。 */
    public BackgroundTaskStats scanStats() {
        return scanStats;
    }

    /** 返回最近一次逐事务恢复失败，供运维定位持久化阻塞。 */
    public RuntimeException lastTransactionFailure() {
        return lastTransactionFailure.get();
    }

    /** 返回最近一次候选扫描失败，供运维定位数据库或模式问题。 */
    public RuntimeException lastScanFailure() {
        return lastScanFailure.get();
    }
}

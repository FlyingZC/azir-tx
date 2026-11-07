package com.flyingzc.azir.txn;

/** 新账本显式运行时的恢复、GC 与批量决议资源预算。 */
public record LedgerRuntimeOptions(int recoveryBatchSize, int gcBatchSize,
                                   long recoveryIntervalMillis, long gcIntervalMillis,
                                   boolean batchControlCommit, int batchWorkers,
                                   boolean directSingleShard,
                                   boolean batchBegin, int beginWorkers,
                                   boolean coordinatorSnapshots, long coordinatorLeaseMillis,
                                   long coordinatorRefreshIntervalMillis) {
    /** 拒绝无界批次、过短或非正周期以及无效批量工作线程数。 */
    public LedgerRuntimeOptions {
        if (recoveryBatchSize < 1 || recoveryBatchSize > 4096 || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || gcIntervalMillis < 1
                || batchWorkers < 1 || batchWorkers > 8
                || beginWorkers < 1 || beginWorkers > 8
                || coordinatorLeaseMillis < 1 || coordinatorRefreshIntervalMillis < 1
                || (coordinatorSnapshots && batchBegin)) {
            throw new IllegalArgumentException("invalid ledger runtime resource limits");
        }
    }

    /** 兼容已有调用，协调者快照优化需要显式开启。 */
    public LedgerRuntimeOptions(int recoveryBatchSize, int gcBatchSize,
                                long recoveryIntervalMillis, long gcIntervalMillis,
                                boolean batchControlCommit, int batchWorkers,
                                boolean directSingleShard, boolean batchBegin, int beginWorkers) {
        this(recoveryBatchSize, gcBatchSize, recoveryIntervalMillis, gcIntervalMillis,
                batchControlCommit, batchWorkers, directSingleShard, batchBegin, beginWorkers,
                false, 120_000, 1_000);
    }

    /** 返回有界后台轮次与两个批量决议线程，单分片直接提交暂不默认启用。 */
    public static LedgerRuntimeOptions defaults() {
        return new LedgerRuntimeOptions(128, 128, 100, 1_000, true, 2, false, false, 1);
    }
}

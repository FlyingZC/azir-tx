package com.flyingzc.azir.txn;

/** 当前执行预算与后台积压的只读统计快照。 */
public record TransactionRuntimeStats(int activeTransactions, int asyncQueued, int cleanupQueued,
                                      long synchronousFallbacks, long backgroundFailures) {
    /** 构造不可变统计快照。 */
    public TransactionRuntimeStats {
    }
}

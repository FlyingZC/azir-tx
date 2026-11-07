package com.flyingzc.azir.api;

/**
 * 事务提交模式枚举。
 */
public enum TransactionCommitMode {
    SINGLE_SHARD_FAST_PATH,
    TWO_PHASE_COMMIT,
    ASYNC_PRIMARY_COMMIT
}

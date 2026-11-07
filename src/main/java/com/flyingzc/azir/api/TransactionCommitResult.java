package com.flyingzc.azir.api;

/**
 * 事务提交结果，记录提交时间戳与提交模式。
 */
public record TransactionCommitResult(long txId, long commitTs, TransactionCommitMode mode) {
}

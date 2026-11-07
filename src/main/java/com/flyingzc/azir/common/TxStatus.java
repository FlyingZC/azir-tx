package com.flyingzc.azir.common;

/**
 * 事务状态枚举。
 */
public enum TxStatus {
    ACTIVE,
    PREPARING,
    COMMITTED,
    ABORTED
}

package com.flyingzc.azir.common;

/**
 * MVCC 组件统一异常类型。
 */
public class MvccException extends RuntimeException {
    private final ErrorCode errorCode;
    private final boolean retryable;
    private final Long txId;

    /**
     * 构造 MvccException 实例。
     */
    public MvccException(ErrorCode errorCode, String message, boolean retryable, Long txId) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.txId = txId;
    }

    /**
     * 构造 MvccException 实例。
     */
    public MvccException(ErrorCode errorCode, String message, boolean retryable, Long txId, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.txId = txId;
    }

    /**
     * 返回 MVCC 错误码。
     */
    public ErrorCode errorCode() {
        return errorCode;
    }

    /**
     * 返回异常是否允许重试。
     */
    public boolean retryable() {
        return retryable;
    }

    /**
     * 返回事务编号。
     */
    public Long txId() {
        return txId;
    }
}

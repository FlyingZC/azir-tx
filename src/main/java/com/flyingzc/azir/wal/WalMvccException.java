package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import java.util.UUID;

/** 新模式独立的事务异常，不修改原有异常接口或错误码。 */
public final class WalMvccException extends RuntimeException {
    private final ErrorCode errorCode;
    private final UUID transactionId;

    /** 保存错误类别、稳定事务号以及原始失败原因。 */
    public WalMvccException(ErrorCode code, String message, UUID transactionId, Throwable cause) {
        super(message, cause);
        this.errorCode = code;
        this.transactionId = transactionId;
    }

    /** 返回可用于业务决策的错误码。 */
    public ErrorCode errorCode() { return errorCode; }

    /** 返回用于提交结果核对的事务号。 */
    public UUID transactionId() { return transactionId; }
}

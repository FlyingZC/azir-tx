package com.flyingzc.azir.percolator;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import java.util.Objects;

/** SQL 或 KV 提交结果未知时携带完整原事务身份，不能只凭 startTs 猜测 primary。 */
public final class PercolatorTransactionStatusUnknownException extends MvccException {
    private final PercolatorRuntime.Handle handle;

    /** 保留 primary 分片、键及 startTs，禁止自动换身份重试。 */
    public PercolatorTransactionStatusUnknownException(PercolatorRuntime.Handle handle, Throwable cause) {
        super(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "query the original Percolator handle before retrying",
                false, Objects.requireNonNull(handle, "handle").startTs(), cause);
        this.handle = handle;
    }

    /** 返回可供新运行时查询或恢复的原始身份。 */
    public PercolatorRuntime.Handle handle() { return handle; }
}

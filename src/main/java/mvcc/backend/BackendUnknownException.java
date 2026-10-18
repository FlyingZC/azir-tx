package mvcc.backend;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

/** 保留原后端和数据空间身份的未知结果异常，禁止自动换号重做。 */
public final class BackendUnknownException extends MvccException {
    private final BackendId id;

    /** 将原始存储异常与完整事务身份一起交给调用方。 */
    public BackendUnknownException(BackendId id, Throwable cause) {
        super(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "query the original backend transaction", false,
                java.util.Objects.requireNonNull(id, "id").startTs(), cause);
        this.id = id;
    }

    /** 返回可供新运行时查询的稳定原始身份。 */
    public BackendId id() { return id; }
}

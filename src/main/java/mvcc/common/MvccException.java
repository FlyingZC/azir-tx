package mvcc.common;

public class MvccException extends RuntimeException {
    private final ErrorCode errorCode;
    private final boolean retryable;
    private final Long txId;

    public MvccException(ErrorCode errorCode, String message, boolean retryable, Long txId) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.txId = txId;
    }

    public MvccException(ErrorCode errorCode, String message, boolean retryable, Long txId, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.txId = txId;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public boolean retryable() {
        return retryable;
    }

    public Long txId() {
        return txId;
    }
}

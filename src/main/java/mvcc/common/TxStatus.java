package mvcc.common;

public enum TxStatus {
    ACTIVE,
    PREPARING,
    COMMITTED,
    ABORTED
}

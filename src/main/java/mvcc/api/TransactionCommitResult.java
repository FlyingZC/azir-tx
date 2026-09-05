package mvcc.api;

public record TransactionCommitResult(long txId, long commitTs, TransactionCommitMode mode) {
}

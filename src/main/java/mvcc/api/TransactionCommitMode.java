package mvcc.api;

public enum TransactionCommitMode {
    SINGLE_SHARD_FAST_PATH,
    TWO_PHASE_COMMIT
}

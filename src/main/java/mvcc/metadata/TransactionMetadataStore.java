package mvcc.metadata;

import mvcc.common.IsolationLevel;

import java.util.Optional;
import java.util.Set;

public interface TransactionMetadataStore {
    TxRecord create(long txId, long startTs, IsolationLevel isolationLevel);

    TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants);

    TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs);

    Optional<TxRecord> get(long txId);

    TxRecord markPreparing(long txId, Set<String> participants);

    TxRecord markCommitted(long txId, long commitTs);

    TxRecord markCommitted(long txId, Set<String> participants, long commitTs);

    default void persistPreparing(long txId, Set<String> participants) {
        markPreparing(txId, participants);
    }

    default void persistCommitted(long txId, long commitTs) {
        markCommitted(txId, commitTs);
    }

    default void persistCommitted(long txId, Set<String> participants, long commitTs) {
        markCommitted(txId, participants, commitTs);
    }

    TxRecord markSingleShardCommitted(long txId, String participant, long commitTs);

    TxRecord markAborted(long txId);

    Optional<Long> oldestActiveStartTs();
}

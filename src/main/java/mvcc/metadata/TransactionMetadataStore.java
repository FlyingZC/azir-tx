package mvcc.metadata;

import mvcc.common.IsolationLevel;

import java.util.Optional;
import java.util.Set;

public interface TransactionMetadataStore {
    TxRecord create(long txId, long startTs, IsolationLevel isolationLevel);

    Optional<TxRecord> get(long txId);

    TxRecord markPreparing(long txId, Set<String> participants);

    TxRecord markCommitted(long txId, long commitTs);

    TxRecord markSingleShardCommitted(long txId, String participant, long commitTs);

    TxRecord markAborted(long txId);

    Optional<Long> oldestActiveStartTs();
}

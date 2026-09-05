package mvcc.storage;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface StorageAdapter {
    String shardId();

    void initialize();

    default boolean requiresExplicitPreparePhase() {
        return false;
    }

    Optional<VersionedValue> get(ReadContext context, String key);

    List<VersionedValue> scan(ReadContext context, KeyRange keyRange);

    void prewrite(long txId, long startTs, List<Mutation> mutations);

    void prepare(long txId);

    void commit(long txId, long commitTs);

    default void commit(long txId, long commitTs, List<Mutation> mutations) {
        commit(txId, commitTs);
    }

    void abort(long txId);

    Set<Long> findPendingTransactionIds(int limit);

    int gc(long safePoint, int limit);
}

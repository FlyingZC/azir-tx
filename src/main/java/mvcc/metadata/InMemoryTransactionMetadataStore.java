package mvcc.metadata;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryTransactionMetadataStore implements TransactionMetadataStore {
    private final Map<Long, TxRecord> records = new ConcurrentHashMap<>();

    @Override
    public TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    @Override
    public Optional<TxRecord> get(long txId) {
        return Optional.ofNullable(records.get(txId));
    }

    @Override
    public TxRecord markPreparing(long txId, Set<String> participants) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.COMMITTED || existing.status() == TxStatus.ABORTED) {
                return existing;
            }
            return new TxRecord(
                    existing.txId(),
                    existing.startTs(),
                    existing.isolationLevel(),
                    new LinkedHashSet<>(participants),
                    TxStatus.PREPARING,
                    existing.commitTs()
            );
        });
    }

    @Override
    public TxRecord markCommitted(long txId, long commitTs) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot commit aborted tx: " + id, false, id);
            }
            if (existing.status() == TxStatus.COMMITTED) {
                return existing;
            }
            return existing.withCommitted(commitTs);
        });
    }

    @Override
    public TxRecord markSingleShardCommitted(long txId, String participant, long commitTs) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot commit aborted tx: " + id, false, id);
            }
            if (existing.status() == TxStatus.COMMITTED) {
                return existing;
            }
            return existing.withParticipants(Set.of(participant)).withCommitted(commitTs);
        });
    }

    @Override
    public TxRecord markAborted(long txId) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.COMMITTED) {
                return existing;
            }
            if (existing.status() == TxStatus.ABORTED) {
                return existing;
            }
            return existing.withStatus(TxStatus.ABORTED);
        });
    }

    @Override
    public Optional<Long> oldestActiveStartTs() {
        return records.values().stream()
                .filter(record -> record.status() == TxStatus.ACTIVE || record.status() == TxStatus.PREPARING)
                .map(TxRecord::startTs)
                .min(Long::compareTo);
    }

    private static TxRecord requireRecord(long txId, TxRecord current) {
        if (current == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId);
        }
        return current;
    }
}

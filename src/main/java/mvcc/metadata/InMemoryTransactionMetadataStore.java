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

/**
 * 基于内存的事务元数据存储实现，主要用于本地测试和原型验证。
 */
public final class InMemoryTransactionMetadataStore implements TransactionMetadataStore {
    private final Map<Long, TxRecord> records = new ConcurrentHashMap<>();

    /**
     * 创建组件实例或事务记录。
     */
    @Override
    public TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    /**
     * 创建 PREPARING 状态的事务元数据记录。
     */
    @Override
    public TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, new LinkedHashSet<>(participants), TxStatus.PREPARING, null);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    /**
     * 创建 COMMITTED 状态的事务元数据记录。
     */
    @Override
    public TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, new LinkedHashSet<>(participants), TxStatus.COMMITTED, commitTs);
        TxRecord previous = records.putIfAbsent(txId, record);
        if (previous != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
        }
        return record;
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    @Override
    public Optional<TxRecord> get(long txId) {
        return Optional.ofNullable(records.get(txId));
    }

    /**
     * 将事务标记为 PREPARING 状态。
     */
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

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public TxRecord markCommitted(long txId, long commitTs) {
        return markCommitted(txId, null, commitTs);
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public TxRecord markCommitted(long txId, Set<String> participants, long commitTs) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot commit aborted tx: " + id, false, id);
            }
            if (existing.status() == TxStatus.COMMITTED) {
                return existing;
            }
            TxRecord next = existing.withCommitted(commitTs);
            if (participants != null) {
                next = next.withParticipants(new LinkedHashSet<>(participants));
            }
            return next;
        });
    }

    /**
     * 将单分片事务标记为已提交。
     */
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

    /**
     * 将事务标记为 ABORTED 状态。
     */
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

    /**
     * 查询当前最老的活跃事务开始时间戳。
     */
    @Override
    public Optional<Long> oldestActiveStartTs() {
        return records.values().stream()
                .filter(record -> record.status() == TxStatus.ACTIVE || record.status() == TxStatus.PREPARING)
                .map(TxRecord::startTs)
                .min(Long::compareTo);
    }

    /**
     * 校验并返回事务记录，不存在时抛出异常。
     */
    private static TxRecord requireRecord(long txId, TxRecord current) {
        if (current == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId);
        }
        return current;
    }
}

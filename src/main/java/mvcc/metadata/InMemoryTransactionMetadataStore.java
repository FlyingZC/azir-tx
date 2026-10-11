package mvcc.metadata;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static mvcc.metadata.TransactionMetadataValidation.validateCommitTs;
import static mvcc.metadata.TransactionMetadataValidation.validateParticipant;
import static mvcc.metadata.TransactionMetadataValidation.validateParticipants;

/**
 * 基于内存的事务元数据存储实现，主要用于本地测试和原型验证。
 */
public final class InMemoryTransactionMetadataStore implements TransactionMetadataStore {
    private final Map<Long, TxRecord> records = new HashMap<>();
    private final Map<Long, SnapshotLease> snapshots = new HashMap<>();
    private final Map<Long, Long> finalizedAt = new HashMap<>();

    /** 保存固定截止时间和始终不提高的快照保护下界。 */
    private record SnapshotLease(long readTs, long expiresAtMillis) {
    }

    /** 所有元数据操作共用同一实例监视器，允许屏障内部重入。 */
    @Override
    public synchronized <T> T withPublicationLock(Supplier<T> action) {
        return action.get();
    }

    /** 持久化唯一参与者的本地提交占位，重复调用必须使用相同时间戳。 */
    @Override
    public synchronized void markLocalCommitPending(long txId, long commitTs) {
        TxRecord record = requireRecord(txId, records.get(txId));
        validateCommitTs(txId, record.startTs(), commitTs);
        if (record.participants().size() != 1 || record.status() == TxStatus.ABORTED
                || record.status() == TxStatus.COMMITTED
                || record.commitTs() != null && record.commitTs() != commitTs) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "invalid local publication state", false, txId);
        }
        records.put(txId, new TxRecord(txId, record.startTs(), record.isolationLevel(), record.participants(),
                TxStatus.PREPARING, commitTs));
    }

    /** 本地提交未知时停止发出新快照，直到恢复封住迟到请求并写入真实终态。 */
    @Override
    public synchronized void assertNoPendingPublications() {
        records.values().stream().filter(record -> record.status() == TxStatus.PREPARING && record.commitTs() != null)
                .findFirst().ifPresent(record -> {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "local publication requires recovery", false, record.txId());
                });
    }

    /** 登记固定租约，重复登记只允许降低读时间戳。 */
    @Override
    public synchronized void registerSnapshot(long txId, long readTs, long expiresAtMillis) {
        SnapshotLease previous = snapshots.get(txId);
        if (previous != null && previous.expiresAtMillis() != expiresAtMillis) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot lease cannot be renewed", false, txId);
        }
        snapshots.put(txId, new SnapshotLease(previous == null ? readTs : Math.min(previous.readTs(), readTs), expiresAtMillis));
    }

    /** 返回租约固定截止时间。 */
    @Override
    public synchronized Optional<Long> snapshotExpiry(long txId) {
        return Optional.ofNullable(snapshots.get(txId)).map(SnapshotLease::expiresAtMillis);
    }

    /** 幂等释放快照租约。 */
    @Override
    public synchronized void releaseSnapshot(long txId) {
        snapshots.remove(txId);
    }

    /** 合并未决事务参与者，不修改其状态或覆盖终态。 */
    @Override
    public synchronized TxRecord registerParticipants(long txId, Set<String> participants) {
        validateParticipants(txId, participants);
        TxRecord current = requireRecord(txId, records.get(txId));
        if (current.status() == TxStatus.COMMITTED || current.status() == TxStatus.ABORTED) {
            return current;
        }
        if (current.commitTs() != null && !current.participants().containsAll(participants)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, txId);
        }
        TxRecord next = current.withParticipants(mergeParticipants(current, participants));
        records.put(txId, next);
        return next;
    }

    /** 用有序游标发现租约失效的未决事务和未完成物化的终态。 */
    @Override
    public synchronized List<TxRecord> findRecoveryCandidates(long nowMillis, long afterTxId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return records.values().stream().filter(record -> record.txId() > afterTxId)
                .filter(record -> !finalizedAt.containsKey(record.txId()))
                .filter(record -> record.status() == TxStatus.COMMITTED || record.status() == TxStatus.ABORTED
                        || snapshotExpiry(record.txId()).map(expiry -> expiry <= nowMillis).orElse(true))
                .sorted(Comparator.comparingLong(TxRecord::txId)).limit(limit).toList();
    }

    /** 保存首次完成物化的时间，重复通知不延长保留期限。 */
    @Override
    public synchronized void markFinalized(long txId) {
        TxRecord record = records.get(txId);
        if (record == null) {
            return;
        }
        if (record.status() != TxStatus.COMMITTED && record.status() != TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot finalize undecided tx", false, txId);
        }
        finalizedAt.putIfAbsent(txId, currentTimeMillis());
    }

    /** 仅删除已物化且严格早于安全点与保留时间的记录。 */
    @Override
    public synchronized int purgeFinalized(long safePoint, long finalizedBeforeMillis, int limit) {
        if (limit <= 0) {
            return 0;
        }
        List<Long> ids = finalizedAt.entrySet().stream()
                .filter(entry -> entry.getValue() < finalizedBeforeMillis)
                .filter(entry -> !snapshots.containsKey(entry.getKey()))
                .filter(entry -> {
                    TxRecord record = records.get(entry.getKey());
                    return record != null && record.startTs() < safePoint
                            && (record.status() == TxStatus.ABORTED
                            || record.status() == TxStatus.COMMITTED && record.commitTs() < safePoint);
                }).map(Map.Entry::getKey).sorted().limit(limit).toList();
        ids.forEach(id -> {
            records.remove(id);
            finalizedAt.remove(id);
        });
        return ids.size();
    }

    /** 分批清除已到期租约，未清理部分继续保守阻挡 GC。 */
    @Override
    public synchronized int expireSnapshots(long nowMillis, int limit) {
        if (limit <= 0) {
            return 0;
        }
        List<Long> ids = snapshots.entrySet().stream().filter(entry -> entry.getValue().expiresAtMillis() <= nowMillis)
                .map(Map.Entry::getKey).sorted().limit(limit).toList();
        ids.forEach(snapshots::remove);
        return ids.size();
    }

    /**
     * 创建组件实例或事务记录。
     */
    @Override
    public synchronized TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
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
    public synchronized TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants) {
        validateParticipants(txId, participants);
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
    public synchronized TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs) {
        validateCommitTs(txId, startTs, commitTs);
        validateParticipants(txId, participants);
        TxRecord existing = records.get(txId);
        if (existing != null && existing.status() == TxStatus.COMMITTED && existing.startTs() == startTs
                && existing.isolationLevel() == isolationLevel && existing.participants().equals(participants)) {
            return markCommitted(txId, commitTs);
        }
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
    public synchronized Optional<TxRecord> get(long txId) {
        return Optional.ofNullable(records.get(txId));
    }

    /**
     * 将事务标记为 PREPARING 状态。
     */
    @Override
    public synchronized TxRecord markPreparing(long txId, Set<String> participants) {
        validateParticipants(txId, participants);
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.COMMITTED || existing.status() == TxStatus.ABORTED) {
                return existing;
            }
            if (existing.commitTs() != null && !existing.participants().containsAll(participants)) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, id);
            }
            return new TxRecord(
                    existing.txId(),
                    existing.startTs(),
                    existing.isolationLevel(),
                    mergeParticipants(existing, participants),
                    TxStatus.PREPARING,
                    existing.commitTs()
            );
        });
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public synchronized TxRecord markCommitted(long txId, long commitTs) {
        return markCommitted(txId, null, commitTs);
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public synchronized TxRecord markCommitted(long txId, Set<String> participants, long commitTs) {
        if (participants != null) {
            validateParticipants(txId, participants);
        }
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            validateCommitTs(id, existing.startTs(), commitTs);
            if (existing.status() == TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot commit aborted tx: " + id, false, id);
            }
            if (existing.commitTs() != null && existing.commitTs() != commitTs) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp mismatch: " + id, false, id);
            }
            if (existing.status() == TxStatus.COMMITTED) {
                if (existing.commitTs() == null || existing.commitTs() != commitTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp mismatch: " + id, false, id);
                }
                return existing;
            }
            TxRecord next = existing.withCommitted(commitTs);
            if (participants != null) {
                if (existing.commitTs() != null && !existing.participants().containsAll(participants)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, id);
                }
                next = next.withParticipants(mergeParticipants(existing, participants));
            }
            return next;
        });
    }

    /**
     * 将单分片事务标记为已提交。
     */
    @Override
    public synchronized TxRecord markSingleShardCommitted(long txId, String participant, long commitTs) {
        validateParticipant(txId, participant);
        return markCommitted(txId, Set.of(participant), commitTs);
    }

    /**
     * 将事务标记为 ABORTED 状态。
     */
    @Override
    public synchronized TxRecord markAborted(long txId) {
        return records.compute(txId, (id, current) -> {
            TxRecord existing = requireRecord(id, current);
            if (existing.status() == TxStatus.COMMITTED) {
                return existing;
            }
            if (existing.status() == TxStatus.ABORTED) {
                return existing;
            }
            return new TxRecord(id, existing.startTs(), existing.isolationLevel(), existing.participants(), TxStatus.ABORTED, null);
        });
    }

    /**
     * 查询所有快照和未完成物化事务的最小保护下界，完成历史记录不再阻挡 GC。
     */
    @Override
    public synchronized Optional<Long> oldestActiveStartTs() {
        Optional<Long> active = records.values().stream()
                .filter(record -> !finalizedAt.containsKey(record.txId()))
                .map(TxRecord::startTs)
                .min(Long::compareTo);
        Optional<Long> snapshot = snapshots.values().stream().map(SnapshotLease::readTs).min(Long::compareTo);
        return java.util.stream.Stream.concat(active.stream(), snapshot.stream()).min(Long::compareTo);
    }

    /** 合并参与者，避免重复阶段调用丢失已登记分片。 */
    private static Set<String> mergeParticipants(TxRecord record, Set<String> participants) {
        Set<String> merged = new LinkedHashSet<>(record.participants());
        if (participants != null) {
            merged.addAll(participants);
        }
        return merged;
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

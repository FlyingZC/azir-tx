package mvcc.metadata;

import mvcc.common.IsolationLevel;

import java.util.Optional;
import java.util.Set;

/**
 * 事务元数据存储接口，定义事务生命周期状态的持久化操作。
 */
public interface TransactionMetadataStore {
    /**
     * 创建组件实例或事务记录。
     */
    TxRecord create(long txId, long startTs, IsolationLevel isolationLevel);

    /**
     * 创建 PREPARING 状态的事务元数据记录。
     */
    TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants);

    /**
     * 创建 COMMITTED 状态的事务元数据记录。
     */
    TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs);

    /**
     * 读取指定键或事务的当前可见数据。
     */
    Optional<TxRecord> get(long txId);

    /**
     * 将事务标记为 PREPARING 状态。
     */
    TxRecord markPreparing(long txId, Set<String> participants);

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    TxRecord markCommitted(long txId, long commitTs);

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    TxRecord markCommitted(long txId, Set<String> participants, long commitTs);

    /**
     * 快速持久化 PREPARING 状态。
     */
    default void persistPreparing(long txId, Set<String> participants) {
        markPreparing(txId, participants);
    }

    /**
     * 快速持久化 COMMITTED 状态。
     */
    default void persistCommitted(long txId, long commitTs) {
        markCommitted(txId, commitTs);
    }

    /**
     * 快速持久化 COMMITTED 状态。
     */
    default void persistCommitted(long txId, Set<String> participants, long commitTs) {
        markCommitted(txId, participants, commitTs);
    }

    /**
     * 将单分片事务标记为已提交。
     */
    TxRecord markSingleShardCommitted(long txId, String participant, long commitTs);

    /**
     * 将事务标记为 ABORTED 状态。
     */
    TxRecord markAborted(long txId);

    /**
     * 查询当前最老的活跃事务开始时间戳。
     */
    Optional<Long> oldestActiveStartTs();
}

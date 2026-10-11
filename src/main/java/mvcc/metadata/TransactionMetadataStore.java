package mvcc.metadata;

import mvcc.common.IsolationLevel;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 事务元数据存储接口，定义事务生命周期状态的持久化操作。
 */
public interface TransactionMetadataStore {
    /** 返回租约与恢复统一使用的墙钟；分布式实现应覆盖为控制面权威时钟。 */
    default long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /** 持久化单分片本地提交占位；已登记唯一参与者，且固定的 commitTs 必须严格大于 startTs。 */
    default void markLocalCommitPending(long txId, long commitTs) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "local publication fencing is required", false, txId);
    }

    /**
     * 新快照和 GC 必须确认没有本地提交未知槽；不支持时禁止继续发布。
     * 未知槽牺牲全库新读可用性，等待原租约到期和参与者终态 fencing 后才解除。
     */
    default void assertNoPendingPublications() {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "pending publication inspection is required", false, null);
    }

    /** 在可重入发布屏障内执行快照登记、提交决议或安全点推进；默认仅保证同实例互斥。 */
    default <T> T withPublicationLock(Supplier<T> action) {
        synchronized (this) {
            return action.get();
        }
    }

    /** 独立登记快照的固定截止租约；重复登记不得延长截止时间或提高保护下界。 */
    default void registerSnapshot(long txId, long readTs, long expiresAtMillis) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot leases are required", false, txId);
    }

    /** 查询租约原始截止时间，包括尚未清理的过期租约；不支持时禁止恢复接管。 */
    default Optional<Long> snapshotExpiry(long txId) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot leases are required", false, txId);
    }

    /** 释放事务快照保护，重复释放无副作用。 */
    default void releaseSnapshot(long txId) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "snapshot leases are required", false, txId);
    }

    /** 在预写前合并非空白且不含逗号的参与者标识；终态记录保持不变并返回真实状态。 */
    default TxRecord registerParticipants(long txId, Set<String> participants) {
        throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant registration is required", false, txId);
    }

    /** 按 txId 升序返回未完成物化的终态以及租约缺失或到期的未决事务。 */
    default List<TxRecord> findRecoveryCandidates(long nowMillis, long afterTxId, int limit) {
        return List.of();
    }

    /** 仅在全部参与者成功完成终态物化后标记；默认不授权删除历史决议。 */
    default void markFinalized(long txId) {
    }

    /** 有界删除早于安全点和保留截止时间且已物化的终态；默认保守保留。 */
    default int purgeFinalized(long safePoint, long finalizedBeforeMillis, int limit) {
        return 0;
    }

    /** 有界删除截止时间不晚于 nowMillis 的租约，不修改事务决议。 */
    default int expireSnapshots(long nowMillis, int limit) {
        return 0;
    }

    /**
     * 创建组件实例或事务记录。
     */
    TxRecord create(long txId, long startTs, IsolationLevel isolationLevel);

    /**
     * 创建 PREPARING 状态的事务元数据记录。
     */
    TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants);

    /**
     * 创建 COMMITTED 状态的事务元数据记录，commitTs 必须严格大于 startTs。
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
     * 将事务标记为 COMMITTED 状态，commitTs 必须严格大于已存储的 startTs。
     */
    TxRecord markCommitted(long txId, long commitTs);

    /**
     * 将事务标记为 COMMITTED 状态，commitTs 必须严格大于已存储的 startTs。
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
     * 查询最老的活跃快照或尚未完成物化事务下界，包含独立租约与未 finalized 的终态。
     */
    Optional<Long> oldestActiveStartTs();
}

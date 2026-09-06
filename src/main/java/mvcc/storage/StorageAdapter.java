package mvcc.storage;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 分片存储适配器接口，封装 MVCC 读写和清理操作。
 */
public interface StorageAdapter {
    /**
     * 返回当前存储适配器所属分片编号。
     */
    String shardId();

    /**
     * 初始化所需的数据库表结构或运行环境。
     */
    void initialize();

    /**
     * 判断参与分片是否需要显式 prepare 阶段。
     */
    default boolean requiresExplicitPreparePhase() {
        return false;
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    Optional<VersionedValue> get(ReadContext context, String key);

    /**
     * 按键范围扫描当前快照可见的数据。
     */
    List<VersionedValue> scan(ReadContext context, KeyRange keyRange);

    /**
     * 写入事务意向并检测写冲突。
     */
    void prewrite(long txId, long startTs, List<Mutation> mutations);

    /**
     * 检查事务意向是否存在并进入准备阶段。
     */
    void prepare(long txId);

    /**
     * 提交事务意向并生成已提交版本。
     */
    void commit(long txId, long commitTs);

    /**
     * 提交事务意向并生成已提交版本。
     */
    default void commit(long txId, long commitTs, List<Mutation> mutations) {
        commit(txId, commitTs);
    }

    /**
     * 提交事务意向并生成已提交版本。
     */
    default void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
        commit(txId, commitTs, mutations);
    }

    /**
     * 直接写入已提交版本，跳过显式 prepare 阶段。
     */
    default void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
        commit(txId, commitTs, mutations, false);
    }

    /**
     * 清理事务残留的写意向。
     */
    default void cleanupIntents(long txId, List<Mutation> mutations) {
    }

    /**
     * 清理事务残留的写意向。
     */
    default void cleanupIntents(long txId) {
        cleanupIntents(txId, List.of());
    }

    /**
     * 回滚事务并清理写意向。
     */
    void abort(long txId);

    /**
     * 查找仍有待处理意向的事务编号。
     */
    Set<Long> findPendingTransactionIds(int limit);

    /**
     * 清理安全点之前的历史版本。
     */
    int gc(long safePoint, int limit);
}

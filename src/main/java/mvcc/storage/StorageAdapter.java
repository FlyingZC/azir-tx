package mvcc.storage;

import mvcc.metadata.TransactionMetadataStore;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 分片存储适配器接口，封装 MVCC 读写和清理操作。
 */
public interface StorageAdapter {
    /**
     * 查询参与者的持久化本地决议；不支持此能力的适配器返回空值。
     */
    default Optional<ParticipantRecord> participantRecord(long txId) {
        return Optional.empty();
    }

    /**
     * 判断适配器是否能够原子保存单分片提交决议和版本。
     */
    default boolean supportsAtomicDirectCommit() {
        return false;
    }

    /**
     * 绑定事务中心权威，供读侧解析已提交意向及物化时核对决议。
     */
    default void configureMetadataStore(TransactionMetadataStore store) {
    }

    /**
     * 判断适配器是否能够解析其他事务的已提交意向。
     */
    default boolean supportsCommittedIntentResolution() {
        return false;
    }

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
     * 按 UTF-8 二进制键顺序读取一页；兼容实现会装载完整扫描结果，存储可覆盖为流式分页。
     */
    default ScanPage scanPage(ReadContext context, KeyRange keyRange, String afterKeyExclusive, int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("limit must be in 1..4096");
        }
        List<VersionedValue> values = scan(context, keyRange).stream()
                .filter(value -> afterKeyExclusive == null || Utf8KeyOrder.compare(value.key(), afterKeyExclusive) > 0)
                .sorted((left, right) -> Utf8KeyOrder.compare(left.key(), right.key()))
                .limit((long) limit + 1)
                .toList();
        boolean hasMore = values.size() > limit;
        List<VersionedValue> page = hasMore ? values.subList(0, limit) : values;
        return new ScanPage(page, hasMore ? page.getLast().key() : null);
    }

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

package com.flyingzc.azir.protocol;

import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.VersionedValue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 参与者存储门面，定义协议层访问分片存储所需的读写和清理能力。
 */
public interface ParticipantStore {
    /**
     * 返回参与者所属分片编号。
     *
     * @return 分片编号
     */
    String shardId();

    /**
     * 按读上下文读取指定业务键的可见版本。
     *
     * @param context 读上下文
     * @param key 业务键
     * @return 可见版本
     */
    Optional<VersionedValue> get(ReadContext context, String key);

    /**
     * 按读上下文扫描指定键范围的可见版本。
     *
     * @param context 读上下文
     * @param keyRange 键范围
     * @return 可见版本列表
     */
    List<VersionedValue> scan(ReadContext context, KeyRange keyRange);

    /**
     * 写入事务意向并检测写冲突。
     *
     * @param txId 事务编号
     * @param startTs 开始时间戳
     * @param mutations 写集合
     */
    void prewrite(long txId, long startTs, List<Mutation> mutations);

    /**
     * 将事务推进到准备阶段。
     *
     * @param txId 事务编号
     */
    void prepare(long txId);

    /**
     * 提交事务意向并生成已提交版本。
     *
     * @param txId 事务编号
     * @param commitTs 提交时间戳
     */
    void commit(long txId, long commitTs);

    /**
     * 基于已知写集合提交事务意向并可选择清理意向。
     *
     * @param txId 事务编号
     * @param commitTs 提交时间戳
     * @param mutations 写集合
     * @param cleanupIntents 是否清理意向
     */
    void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents);

    /**
     * 直接写入已提交版本，跳过显式准备阶段。
     *
     * @param txId 事务编号
     * @param startTs 开始时间戳
     * @param commitTs 提交时间戳
     * @param mutations 写集合
     */
    void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations);

    /**
     * 基于已知写集合清理事务残留意向。
     *
     * @param txId 事务编号
     * @param mutations 写集合
     */
    void cleanupIntents(long txId, List<Mutation> mutations);

    /**
     * 回滚事务并清理写意向。
     *
     * @param txId 事务编号
     */
    void abort(long txId);

    /**
     * 查找仍有待处理意向的事务编号。
     *
     * @param limit 查询数量上限
     * @return 待处理事务编号集合
     */
    Set<Long> findPendingTransactionIds(int limit);

    /**
     * 清理安全点之前的历史版本。
     *
     * @param safePoint 安全点
     * @param limit 清理数量上限
     * @return 清理数量
     */
    int gc(long safePoint, int limit);
}

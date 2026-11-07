package com.flyingzc.azir.protocol;

import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 基于 StorageAdapter 的参与者存储门面实现。
 */
public final class StorageAdapterParticipantStore implements ParticipantStore {
    private final StorageAdapter delegate;

    /**
     * 创建参与者存储门面。
     *
     * @param delegate 底层存储适配器
     */
    public StorageAdapterParticipantStore(final StorageAdapter delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * 返回参与者所属分片编号。
     *
     * @return 分片编号
     */
    @Override
    public String shardId() {
        return delegate.shardId();
    }

    /**
     * 按读上下文读取指定业务键的可见版本。
     *
     * @param context 读上下文
     * @param key 业务键
     * @return 可见版本
     */
    @Override
    public Optional<VersionedValue> get(final ReadContext context, final String key) {
        return delegate.get(context, key);
    }

    /**
     * 按读上下文扫描指定键范围的可见版本。
     *
     * @param context 读上下文
     * @param keyRange 键范围
     * @return 可见版本列表
     */
    @Override
    public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
        return delegate.scan(context, keyRange);
    }

    /**
     * 写入事务意向并检测写冲突。
     *
     * @param txId 事务编号
     * @param startTs 开始时间戳
     * @param mutations 写集合
     */
    @Override
    public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
        delegate.prewrite(txId, startTs, mutations);
    }

    /**
     * 将事务推进到准备阶段。
     *
     * @param txId 事务编号
     */
    @Override
    public void prepare(final long txId) {
        delegate.prepare(txId);
    }

    /**
     * 提交事务意向并生成已提交版本。
     *
     * @param txId 事务编号
     * @param commitTs 提交时间戳
     */
    @Override
    public void commit(final long txId, final long commitTs) {
        delegate.commit(txId, commitTs);
    }

    /**
     * 基于已知写集合提交事务意向并可选择清理意向。
     *
     * @param txId 事务编号
     * @param commitTs 提交时间戳
     * @param mutations 写集合
     * @param cleanupIntents 是否清理意向
     */
    @Override
    public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        delegate.commit(txId, commitTs, mutations, cleanupIntents);
    }

    /**
     * 直接写入已提交版本，跳过显式准备阶段。
     *
     * @param txId 事务编号
     * @param startTs 开始时间戳
     * @param commitTs 提交时间戳
     * @param mutations 写集合
     */
    @Override
    public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
        delegate.directCommit(txId, startTs, commitTs, mutations);
    }

    /**
     * 基于已知写集合清理事务残留意向。
     *
     * @param txId 事务编号
     * @param mutations 写集合
     */
    @Override
    public void cleanupIntents(final long txId, final List<Mutation> mutations) {
        delegate.cleanupIntents(txId, mutations);
    }

    /**
     * 回滚事务并清理写意向。
     *
     * @param txId 事务编号
     */
    @Override
    public void abort(final long txId) {
        delegate.abort(txId);
    }

    /**
     * 查找仍有待处理意向的事务编号。
     *
     * @param limit 查询数量上限
     * @return 待处理事务编号集合
     */
    @Override
    public Set<Long> findPendingTransactionIds(final int limit) {
        return delegate.findPendingTransactionIds(limit);
    }

    /**
     * 清理安全点之前的历史版本。
     *
     * @param safePoint 安全点
     * @param limit 清理数量上限
     * @return 清理数量
     */
    @Override
    public int gc(final long safePoint, final int limit) {
        return delegate.gc(safePoint, limit);
    }
}

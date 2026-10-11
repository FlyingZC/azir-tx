package mvcc.testing;

import mvcc.metadata.TransactionMetadataStore;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ParticipantRecord;
import mvcc.storage.ReadContext;
import mvcc.storage.ScanPage;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 完整转发真实存储协议，故障包装器只覆盖需要注入故障的调用。
 */
abstract class DelegatingStorageAdapter implements StorageAdapter {
    protected final StorageAdapter delegate;

    /** 构造绑定真实适配器的测试包装器。 */
    protected DelegatingStorageAdapter(StorageAdapter delegate) {
        this.delegate = delegate;
    }

    /** 转发事务中心绑定，保留真实适配器的禁止复绑约束。 */
    @Override
    public void configureMetadataStore(TransactionMetadataStore store) {
        delegate.configureMetadataStore(store);
    }

    /** 返回真实适配器的已提交意向解析能力。 */
    @Override
    public boolean supportsCommittedIntentResolution() {
        return delegate.supportsCommittedIntentResolution();
    }

    /** 返回真实适配器的原子直接提交能力。 */
    @Override
    public boolean supportsAtomicDirectCommit() {
        return delegate.supportsAtomicDirectCommit();
    }

    /** 查询真实参与者记录，不通过包装器伪造本地终态。 */
    @Override
    public Optional<ParticipantRecord> participantRecord(long txId) {
        return delegate.participantRecord(txId);
    }

    /** 返回真实分片编号。 */
    @Override
    public String shardId() {
        return delegate.shardId();
    }

    /** 初始化真实存储结构。 */
    @Override
    public void initialize() {
        delegate.initialize();
    }

    /** 返回真实适配器的显式准备要求。 */
    @Override
    public boolean requiresExplicitPreparePhase() {
        return delegate.requiresExplicitPreparePhase();
    }

    /** 转发快照点读。 */
    @Override
    public Optional<VersionedValue> get(ReadContext context, String key) {
        return delegate.get(context, key);
    }

    /** 转发完整范围扫描。 */
    @Override
    public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
        return delegate.scan(context, keyRange);
    }

    /** 转发真实分页实现，保留页边界和扫描资源限制。 */
    @Override
    public ScanPage scanPage(ReadContext context, KeyRange keyRange, String afterKeyExclusive, int limit) {
        return delegate.scanPage(context, keyRange, afterKeyExclusive, limit);
    }

    /** 转发意向预写。 */
    @Override
    public void prewrite(long txId, long startTs, List<Mutation> mutations) {
        delegate.prewrite(txId, startTs, mutations);
    }

    /** 转发显式准备。 */
    @Override
    public void prepare(long txId) {
        delegate.prepare(txId);
    }

    /** 按事务编号转发提交。 */
    @Override
    public void commit(long txId, long commitTs) {
        delegate.commit(txId, commitTs);
    }

    /** 按已知写集合转发提交。 */
    @Override
    public void commit(long txId, long commitTs, List<Mutation> mutations) {
        delegate.commit(txId, commitTs, mutations);
    }

    /** 转发提交及意向清理策略。 */
    @Override
    public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
        delegate.commit(txId, commitTs, mutations, cleanupIntents);
    }

    /** 转发版本与参与者记录的原子直接提交。 */
    @Override
    public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
        delegate.directCommit(txId, startTs, commitTs, mutations);
    }

    /** 按写集合转发意向清理。 */
    @Override
    public void cleanupIntents(long txId, List<Mutation> mutations) {
        delegate.cleanupIntents(txId, mutations);
    }

    /** 按事务编号转发意向清理。 */
    @Override
    public void cleanupIntents(long txId) {
        delegate.cleanupIntents(txId);
    }

    /** 转发真实回滚及终态防护。 */
    @Override
    public void abort(long txId) {
        delegate.abort(txId);
    }

    /** 转发待恢复事务扫描。 */
    @Override
    public Set<Long> findPendingTransactionIds(int limit) {
        return delegate.findPendingTransactionIds(limit);
    }

    /** 转发安全点垃圾回收。 */
    @Override
    public int gc(long safePoint, int limit) {
        return delegate.gc(safePoint, limit);
    }
}

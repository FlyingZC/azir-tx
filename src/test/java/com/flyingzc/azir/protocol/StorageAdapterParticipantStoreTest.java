package com.flyingzc.azir.protocol;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 存储适配器参与者门面测试，验证协议接口正确委托现有存储适配器。
 */
class StorageAdapterParticipantStoreTest {
    /**
     * 验证写入、提交和查询操作会委托到底层 StorageAdapter。
     */
    @Test
    void shouldDelegateOperationsToStorageAdapter() {
        RecordingStorageAdapter adapter = new RecordingStorageAdapter("shard-1");
        ParticipantStore store = new StorageAdapterParticipantStore(adapter);
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(Mutation.put("account:1", new byte[]{1}));

        store.prewrite(10L, 10L, mutations);
        store.prepare(10L);
        store.commit(10L, 11L, mutations, true);
        store.directCommit(12L, 12L, 13L, mutations);
        store.cleanupIntents(10L, mutations);
        store.abort(14L);
        store.findPendingTransactionIds(5);
        store.gc(20L, 100);

        assertEquals("shard-1", store.shardId());
        assertTrue(adapter.calls.contains("prewrite:10"));
        assertTrue(adapter.calls.contains("prepare:10"));
        assertTrue(adapter.calls.contains("commit-known:10:cleanup=true"));
        assertTrue(adapter.calls.contains("directCommit:12"));
        assertTrue(adapter.calls.contains("cleanup-known:10"));
        assertTrue(adapter.calls.contains("abort:14"));
        assertTrue(adapter.calls.contains("findPending:5"));
        assertTrue(adapter.calls.contains("gc:20:100"));
    }

    /**
     * 验证读路径操作会委托到底层 StorageAdapter。
     */
    @Test
    void shouldDelegateReadOperationsToStorageAdapter() {
        RecordingStorageAdapter adapter = new RecordingStorageAdapter("shard-1");
        ParticipantStore store = new StorageAdapterParticipantStore(adapter);
        ReadContext context = new ReadContext(1L, 1L, IsolationLevel.READ_COMMITTED);

        store.get(context, "account:1");
        store.scan(context, new KeyRange("account:0", "account:9"));

        assertTrue(adapter.calls.contains("get:account:1"));
        assertTrue(adapter.calls.contains("scan:account:0:account:9"));
    }

    /**
     * 记录调用的存储适配器测试桩。
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {
        private final String shardId;
        private final List<String> calls = new ArrayList<>();

        /**
         * 构造记录型存储适配器。
         *
         * @param shardId 分片编号
         */
        private RecordingStorageAdapter(String shardId) {
            this.shardId = shardId;
        }

        /**
         * 返回分片编号。
         *
         * @return 分片编号
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * 初始化测试桩。
         */
        @Override
        public void initialize() {
            calls.add("initialize");
        }

        /**
         * 记录单键读取调用。
         *
         * @param context 读上下文
         * @param key 业务键
         * @return 空结果
         */
        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) {
            calls.add("get:" + key);
            return Optional.empty();
        }

        /**
         * 记录范围扫描调用。
         *
         * @param context 读上下文
         * @param keyRange 键范围
         * @return 空列表
         */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            calls.add("scan:" + keyRange.startKeyInclusive() + ":" + keyRange.endKeyExclusive());
            return new ArrayList<>();
        }

        /**
         * 记录预写调用。
         *
         * @param txId 事务编号
         * @param startTs 开始时间戳
         * @param mutations 写集合
         */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
            calls.add("prewrite:" + txId);
        }

        /**
         * 记录 prepare 调用。
         *
         * @param txId 事务编号
         */
        @Override
        public void prepare(long txId) {
            calls.add("prepare:" + txId);
        }

        /**
         * 记录提交调用。
         *
         * @param txId 事务编号
         * @param commitTs 提交时间戳
         */
        @Override
        public void commit(long txId, long commitTs) {
            calls.add("commit:" + txId);
        }

        /**
         * 记录已知写集合提交调用。
         *
         * @param txId 事务编号
         * @param commitTs 提交时间戳
         * @param mutations 写集合
         * @param cleanupIntents 是否清理意向
         */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
            calls.add("commit-known:" + txId + ":cleanup=" + cleanupIntents);
        }

        /**
         * 记录直接提交调用。
         *
         * @param txId 事务编号
         * @param startTs 开始时间戳
         * @param commitTs 提交时间戳
         * @param mutations 写集合
         */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            calls.add("directCommit:" + txId);
        }

        /**
         * 记录清理已知意向调用。
         *
         * @param txId 事务编号
         * @param mutations 写集合
         */
        @Override
        public void cleanupIntents(long txId, List<Mutation> mutations) {
            calls.add("cleanup-known:" + txId);
        }

        /**
         * 记录回滚调用。
         *
         * @param txId 事务编号
         */
        @Override
        public void abort(long txId) {
            calls.add("abort:" + txId);
        }

        /**
         * 记录待恢复事务查询调用。
         *
         * @param limit 查询数量上限
         * @return 空集合
         */
        @Override
        public Set<Long> findPendingTransactionIds(int limit) {
            calls.add("findPending:" + limit);
            return new LinkedHashSet<>();
        }

        /**
         * 记录 GC 调用。
         *
         * @param safePoint 安全点
         * @param limit 清理数量上限
         * @return 清理数量
         */
        @Override
        public int gc(long safePoint, int limit) {
            calls.add("gc:" + safePoint + ":" + limit);
            return 0;
        }
    }
}

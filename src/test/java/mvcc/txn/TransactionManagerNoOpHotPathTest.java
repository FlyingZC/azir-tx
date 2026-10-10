package mvcc.txn;

import mvcc.api.Transaction;
import mvcc.common.IsolationLevel;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 事务管理器 no-op metrics 热路径测试。
 */
class TransactionManagerNoOpHotPathTest {
    /**
     * 验证事务管理器不再使用旧的 recordStage 包装热路径。
     */
    @Test
    void noOpHotPathShouldNotUseGenericRecordStageWrapper() throws Exception {
        String source = Files.readString(Path.of("src/main/java/mvcc/txn/TransactionManager.java"));

        assertFalse(source.contains("recordStage("));
    }

    /**
     * 验证默认 no-op metrics 仍能提交单分片事务。
     */
    @Test
    void defaultNoOpMetricsShouldStillCommitSingleShardTransaction() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard)
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.commit(tx);

        assertEquals(1, shard.directCommitCount());
    }

    /**
     * 验证默认 no-op metrics 仍能提交跨分片事务。
     */
    @Test
    void defaultNoOpMetricsShouldStillCommitCrossShardTransaction() {
        RecordingStorageAdapter shard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter shard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.put(tx, "shard-2", "k2", new byte[]{2});
        manager.commit(tx);

        assertEquals(1, shard1.commitCount());
        assertEquals(1, shard2.commitCount());
    }

    /**
     * 验证默认 no-op metrics 仍能回滚参与分片。
     */
    @Test
    void defaultNoOpMetricsShouldStillRollbackParticipants() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.failFastDefaults(),
                Map.of(shard.shardId(), shard)
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.rollback(tx);

        assertEquals(1, shard.abortCount());
    }

    /**
     * 记录调用次数的测试存储适配器。
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {
        private final String shardId;
        private int commitCount;
        private int directCommitCount;
        private int abortCount;

        /**
         * 构造测试存储适配器。
         */
        private RecordingStorageAdapter(String shardId) {
            this.shardId = shardId;
        }

        /**
         * 返回直接提交次数。
         */
        private int directCommitCount() {
            return directCommitCount;
        }

        /**
         * 返回普通提交次数。
         */
        private int commitCount() {
            return commitCount;
        }

        /**
         * 返回回滚次数。
         */
        private int abortCount() {
            return abortCount;
        }

        /**
         * 返回当前分片编号。
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * 初始化测试存储。
         */
        @Override
        public void initialize() {
        }

        /**
         * 读取测试值。
         */
        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) {
            return Optional.empty();
        }

        /**
         * 扫描测试值。
         */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            return List.of();
        }

        /**
         * 写入测试 intent。
         */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
        }

        /**
         * 准备测试事务。
         */
        @Override
        public void prepare(long txId) {
        }

        /**
         * 提交测试 intent。
         */
        @Override
        public void commit(long txId, long commitTs) {
            commitCount++;
        }

        /**
         * 提交测试写集合。
         */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
            commitCount++;
        }

        /**
         * 直接提交测试写集合。
         */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            directCommitCount++;
        }

        /**
         * 清理测试 intent。
         */
        @Override
        public void cleanupIntents(long txId, List<Mutation> mutations) {
        }

        /**
         * 回滚测试事务。
         */
        @Override
        public void abort(long txId) {
            abortCount++;
        }

        /**
         * 查找待恢复事务编号。
         */
        @Override
        public Set<Long> findPendingTransactionIds(int limit) {
            return Set.of();
        }

        /**
         * 清理测试历史版本。
         */
        @Override
        public int gc(long safePoint, int limit) {
            return 0;
        }
    }
}

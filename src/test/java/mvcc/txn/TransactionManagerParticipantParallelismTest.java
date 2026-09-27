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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 事务管理器参与者阶段并行执行测试。
 */
class TransactionManagerParticipantParallelismTest {
    /**
     * 验证开启并行预写时，两个参与分片也会并发执行。
     */
    @Test
    void twoParticipantPrewriteShouldRunConcurrentlyWhenParallelPrewriteEnabled() {
        CountDownLatch enteredPrewrite = new CountDownLatch(2);
        BlockingPrewriteStorageAdapter shard1 = new BlockingPrewriteStorageAdapter("shard-1", enteredPrewrite);
        BlockingPrewriteStorageAdapter shard2 = new BlockingPrewriteStorageAdapter("shard-2", enteredPrewrite);
        TransactionOptions options = new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2),
                true,
                true,
                false
        );
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "parallel:1", new byte[]{1});
        manager.put(tx, "shard-2", "parallel:2", new byte[]{2});

        assertDoesNotThrow(() -> manager.commit(tx));
        assertEquals(1, shard1.prewriteCalls());
        assertEquals(1, shard2.prewriteCalls());
    }

    /**
     * 会在预写阶段等待另一个分片同时进入的存储适配器。
     */
    private static final class BlockingPrewriteStorageAdapter implements StorageAdapter {
        private final String shardId;
        private final CountDownLatch enteredPrewrite;
        private final AtomicInteger prewriteCalls = new AtomicInteger();

        /**
         * 构造阻塞预写适配器。
         */
        private BlockingPrewriteStorageAdapter(String shardId, CountDownLatch enteredPrewrite) {
            this.shardId = shardId;
            this.enteredPrewrite = enteredPrewrite;
        }

        /**
         * 返回当前分片编号。
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * 初始化测试适配器。
         */
        @Override
        public void initialize() {
        }

        /**
         * 读取指定键。
         */
        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) {
            return Optional.empty();
        }

        /**
         * 扫描指定范围。
         */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            return List.of();
        }

        /**
         * 预写并等待另一个分片同时进入。
         */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
            prewriteCalls.incrementAndGet();
            enteredPrewrite.countDown();
            try {
                if (!enteredPrewrite.await(300, TimeUnit.MILLISECONDS)) {
                    throw new AssertionError("prewrite did not run concurrently");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for concurrent prewrite", ex);
            }
        }

        /**
         * 准备测试事务。
         */
        @Override
        public void prepare(long txId) {
        }

        /**
         * 提交测试事务。
         */
        @Override
        public void commit(long txId, long commitTs) {
        }

        /**
         * 提交已知测试写集合。
         */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
        }

        /**
         * 回滚测试事务。
         */
        @Override
        public void abort(long txId) {
        }

        /**
         * 查找待恢复事务编号。
         */
        @Override
        public Set<Long> findPendingTransactionIds(int limit) {
            return Set.of();
        }

        /**
         * 清理历史版本。
         */
        @Override
        public int gc(long safePoint, int limit) {
            return 0;
        }

        /**
         * 返回预写调用次数。
         */
        private int prewriteCalls() {
            return prewriteCalls.get();
        }
    }
}

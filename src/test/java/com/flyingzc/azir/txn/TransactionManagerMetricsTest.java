package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metrics.InMemoryTransactionMetrics;
import com.flyingzc.azir.metrics.TransactionMetrics;
import com.flyingzc.azir.metrics.TransactionStage;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 事务管理器指标记录测试。
 */
class TransactionManagerMetricsTest {
    /**
     * 验证不支持原子直接提交的单分片适配器记录预写和核心提交阶段。
     */
    @Test
    void singleShardBufferedCommitShouldRecordStages() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard),
                metrics
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.commit(tx);

        assertSuccess(metrics, TransactionStage.BEGIN_TSO, 1L);
        assertSuccess(metrics, TransactionStage.BEGIN_METADATA, 1L);
        assertSuccess(metrics, TransactionStage.BUFFERED_PREWRITE, 1L);
        assertEquals(1, shard.prewriteAttempts());
        assertSuccess(metrics, TransactionStage.COMMIT_TSO, 1L);
        assertSuccess(metrics, TransactionStage.SINGLE_SHARD_METADATA_COMMIT, 1L);
        assertSuccess(metrics, TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT, 1L);
    }

    /**
     * 验证跨分片提交会记录预写、元数据和主从分片提交阶段。
     */
    @Test
    void crossShardCommitShouldRecordStages() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter shard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                metrics
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.put(tx, "shard-2", "k2", new byte[]{2});
        manager.commit(tx);

        assertSuccess(metrics, TransactionStage.BUFFERED_PREWRITE, 1L);
        assertSuccess(metrics, TransactionStage.COMMIT_TSO, 1L);
        assertSuccess(metrics, TransactionStage.CROSS_SHARD_METADATA_COMMIT, 1L);
        assertSuccess(metrics, TransactionStage.PRIMARY_PARTICIPANT_COMMIT, 1L);
        assertSuccess(metrics, TransactionStage.SECONDARY_PARTICIPANT_COMMIT, 1L);
    }

    /**
     * 验证非缓冲写路径会记录 eager prewrite 阶段。
     */
    @Test
    void eagerPrewriteShouldRecordStageWhenWritesAreNotBuffered() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.failFastDefaults(),
                Map.of(shard.shardId(), shard),
                metrics
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        assertSuccess(metrics, TransactionStage.EAGER_PREWRITE, 1L);
    }

    /**
     * 验证 eager prewrite 阶段会统计包含冲突等待重试的完整阶段。
     */
    @Test
    void eagerPrewriteShouldRecordOneStageAcrossTransientRetry() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        shard.failPrewriteAttempts(1);
        TransactionOptions options = new TransactionOptions(
                new LockWaitPolicy(ConflictMode.WAIT, 100L, 0L, 0L, 1.0d, false),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(),
                false,
                false,
                false
        );
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard.shardId(), shard),
                metrics
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        assertEquals(2, shard.prewriteAttempts());
        assertEquals(1L, metrics.snapshot(TransactionStage.EAGER_PREWRITE).invocationCount());
        assertSuccess(metrics, TransactionStage.EAGER_PREWRITE, 1L);
    }

    /**
     * 验证指标记录成功异常不会改变事务提交结果。
     */
    @Test
    void metricsSuccessFailureShouldNotAffectTransactionFlow() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard),
                new ThrowingTransactionMetrics(true, false)
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        assertDoesNotThrow(() -> manager.commit(tx));
    }

    /**
     * 验证指标记录失败异常不会遮蔽原始业务异常。
     */
    @Test
    void metricsFailureFailureShouldNotMaskTransactionFailure() {
        RecordingStorageAdapter shard = new RecordingStorageAdapter("shard-1");
        shard.failCommit(true);
        TransactionManager manager = new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(shard.shardId(), shard),
                new ThrowingTransactionMetrics(false, true)
        );

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});

        MvccException ex = assertThrows(MvccException.class, () -> manager.commit(tx));
        assertEquals(com.flyingzc.azir.common.ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
        assertFalse(ex.retryable());
        MvccException cause = assertInstanceOf(MvccException.class, ex.getCause());
        assertEquals(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, cause.errorCode());
    }

    /**
     * 验证异步提交后台任务会记录成功和失败。
     */
    @Test
    void asyncCommitShouldRecordBackgroundSuccessAndFailure() throws Exception {
        InMemoryTransactionMetrics successMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter successShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter successShard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager successManager = manager(
                TransactionOptions.mysqlLikeDefaults(),
                successMetrics,
                successShard1,
                successShard2
        );

        Transaction successTx = successManager.begin(IsolationLevel.REPEATABLE_READ);
        successManager.put(successTx, "shard-1", "k1", new byte[]{1});
        successManager.put(successTx, "shard-2", "k2", new byte[]{2});
        successManager.commit(successTx);
        assertEventually(successMetrics, TransactionStage.ASYNC_PARTICIPANT_COMMIT, 2L, 0L);

        InMemoryTransactionMetrics failureMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter failingShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter failingShard2 = new RecordingStorageAdapter("shard-2");
        failingShard2.failCommit(true);
        TransactionManager failureManager = manager(
                TransactionOptions.mysqlLikeDefaults(),
                failureMetrics,
                failingShard1,
                failingShard2
        );

        Transaction failureTx = failureManager.begin(IsolationLevel.REPEATABLE_READ);
        failureManager.put(failureTx, "shard-1", "k3", new byte[]{3});
        failureManager.put(failureTx, "shard-2", "k4", new byte[]{4});
        failureManager.commit(failureTx);
        assertEventually(failureMetrics, TransactionStage.ASYNC_PARTICIPANT_COMMIT, 1L, 1L);
    }

    /**
     * 验证后台 intent cleanup 会记录成功和失败。
     */
    @Test
    void deferredIntentCleanupShouldRecordBackgroundSuccessAndFailure() throws Exception {
        InMemoryTransactionMetrics successMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter successShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter successShard2 = new RecordingStorageAdapter("shard-2");
        TransactionManager successManager = manager(
                asyncDeferCleanupOptions(),
                successMetrics,
                successShard1,
                successShard2
        );

        Transaction successTx = successManager.begin(IsolationLevel.REPEATABLE_READ);
        successManager.put(successTx, "shard-1", "k1", new byte[]{1});
        successManager.put(successTx, "shard-2", "k2", new byte[]{2});
        successManager.commit(successTx);
        assertEventually(successMetrics, TransactionStage.INTENT_CLEANUP, 2L, 0L);

        InMemoryTransactionMetrics failureMetrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter failingShard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter failingShard2 = new RecordingStorageAdapter("shard-2");
        failingShard2.failCleanup(true);
        TransactionManager failureManager = manager(
                asyncDeferCleanupOptions(),
                failureMetrics,
                failingShard1,
                failingShard2
        );

        Transaction failureTx = failureManager.begin(IsolationLevel.REPEATABLE_READ);
        failureManager.put(failureTx, "shard-1", "k3", new byte[]{3});
        failureManager.put(failureTx, "shard-2", "k4", new byte[]{4});
        failureManager.commit(failureTx);
        assertEventually(failureMetrics, TransactionStage.INTENT_CLEANUP, 1L, 1L);
    }

    /**
     * 验证关闭事务管理器会先等待 async commit，再等待 cleanup 指标可见。
     */
    @Test
    void closeShouldDrainAsyncCommitAndIntentCleanupMetrics() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        RecordingStorageAdapter shard1 = new RecordingStorageAdapter("shard-1");
        RecordingStorageAdapter shard2 = new RecordingStorageAdapter("shard-2");
        shard1.commitDelayMillis(50L);
        shard2.commitDelayMillis(50L);
        TransactionManager manager = manager(asyncDeferCleanupOptions(), metrics, shard1, shard2);

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "k1", new byte[]{1});
        manager.put(tx, "shard-2", "k2", new byte[]{2});
        manager.commit(tx);
        manager.close();

        assertSuccess(metrics, TransactionStage.ASYNC_PARTICIPANT_COMMIT, 2L);
        assertSuccess(metrics, TransactionStage.INTENT_CLEANUP, 2L);
    }

    /**
     * 创建带指标的事务管理器。
     */
    private TransactionManager manager(TransactionOptions options,
                                       InMemoryTransactionMetrics metrics,
                                       RecordingStorageAdapter shard1,
                                       RecordingStorageAdapter shard2) {
        return new TransactionManager(
                new AtomicTimestampOracle(100L),
                new InMemoryTransactionMetadataStore(),
                options,
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2),
                metrics
        );
    }

    /**
     * 创建异步提交并延迟清理 intent 的测试选项。
     */
    private TransactionOptions asyncDeferCleanupOptions() {
        return new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2),
                true,
                false,
                true
        );
    }

    /**
     * 断言指定阶段成功次数。
     */
    private void assertSuccess(InMemoryTransactionMetrics metrics, TransactionStage stage, long expectedSuccessCount) {
        assertEquals(expectedSuccessCount, metrics.snapshot(stage).successCount(), stage.name());
        assertEquals(0L, metrics.snapshot(stage).failureCount(), stage.name());
    }

    /**
     * 等待异步阶段统计达到预期。
     */
    private void assertEventually(InMemoryTransactionMetrics metrics,
                                  TransactionStage stage,
                                  long expectedSuccessCount,
                                  long expectedFailureCount) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadline) {
            if (metrics.snapshot(stage).successCount() == expectedSuccessCount
                    && metrics.snapshot(stage).failureCount() == expectedFailureCount) {
                return;
            }
            Thread.sleep(10L);
        }
        assertEquals(expectedSuccessCount, metrics.snapshot(stage).successCount(), stage.name());
        assertEquals(expectedFailureCount, metrics.snapshot(stage).failureCount(), stage.name());
    }

    /**
     * 记录调用并可按需失败的测试存储适配器。
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {
        private final String shardId;
        private boolean failCommit;
        private boolean failCleanup;
        private int failPrewriteAttempts;
        private int prewriteAttempts;
        private long commitDelayMillis;

        /**
         * 构造测试存储适配器。
         */
        private RecordingStorageAdapter(String shardId) {
            this.shardId = shardId;
        }

        /**
         * 仅模拟跨分片可见性能力，测试范围为指标记录及异常传播。
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * 设置提交是否失败。
         */
        private void failCommit(boolean failCommit) {
            this.failCommit = failCommit;
        }

        /**
         * 设置清理是否失败。
         */
        private void failCleanup(boolean failCleanup) {
            this.failCleanup = failCleanup;
        }

        /**
         * 设置预写临时失败次数。
         */
        private void failPrewriteAttempts(int failPrewriteAttempts) {
            this.failPrewriteAttempts = failPrewriteAttempts;
        }

        /**
         * 返回预写尝试次数。
         */
        private int prewriteAttempts() {
            return prewriteAttempts;
        }

        /**
         * 设置提交延迟毫秒数。
         */
        private void commitDelayMillis(long commitDelayMillis) {
            this.commitDelayMillis = commitDelayMillis;
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
            prewriteAttempts++;
            if (failPrewriteAttempts > 0) {
                failPrewriteAttempts--;
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.LOCK_CONFLICT, "prewrite conflict", true, txId);
            }
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
            commit(txId, commitTs, List.of(), true);
        }

        /**
         * 提交测试写集合。
         */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
            sleepQuietly(commitDelayMillis);
            if (failCommit) {
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "commit failed", true, txId);
            }
        }

        /**
         * 直接提交测试写集合。
         */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            throw new AssertionError("metrics adapter does not implement atomic direct commit");
        }

        /**
         * 清理测试 intent。
         */
        @Override
        public void cleanupIntents(long txId, List<Mutation> mutations) {
            if (failCleanup) {
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "cleanup failed", true, txId);
            }
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
         * 清理测试历史版本。
         */
        @Override
        public int gc(long safePoint, int limit) {
            return 0;
        }

        /**
         * 安静等待指定毫秒数。
         */
        private void sleepQuietly(long delayMillis) {
            if (delayMillis <= 0L) {
                return;
            }
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "interrupted", true, null, ex);
            }
        }
    }

    /**
     * 可模拟指标记录异常的测试指标实现。
     */
    private record ThrowingTransactionMetrics(boolean failSuccess, boolean failFailure) implements TransactionMetrics {
        /**
         * 记录成功指标。
         */
        @Override
        public void recordSuccess(TransactionStage stage, long elapsedNanos) {
            if (failSuccess) {
                throw new IllegalStateException("metrics success failed");
            }
        }

        /**
         * 记录失败指标。
         */
        @Override
        public void recordFailure(TransactionStage stage, long elapsedNanos) {
            if (failFailure) {
                throw new IllegalStateException("metrics failure failed");
            }
        }
    }
}

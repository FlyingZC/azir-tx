package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务管理器 no-op metrics 热路径测试。
 */
class TransactionManagerNoOpHotPathTest {
    /**
     * 验证事务管理器不再使用旧的 recordStage 包装热路径。
     */
    @Test
    void noOpHotPathShouldNotUseGenericRecordStageWrapper() throws Exception {
        String source = readTransactionManagerSource();

        assertFalse(source.contains("recordStage("));
    }

    /**
     * 验证事务入口不会直接进入计量包装方法。
     */
    @Test
    void publicHotPathShouldNotCallMeasuredStageDirectly() throws Exception {
        String source = readTransactionManagerSource();
        String publicAndBusinessSource = source.substring(0, source.indexOf("private boolean metricsEnabled()"));

        assertFalse(publicAndBusinessSource.contains("recordMeasuredStage("));
    }

    /**
     * 验证所有计量 helper 都先短路默认 no-op metrics。
     */
    @Test
    void measuredStageHelpersShouldGuardNoOpMetricsBeforeWrappingAction() throws Exception {
        String source = readTransactionManagerSource();

        assertMeasuredStageGuard(source, "nextTimestamp(TransactionStage stage)");
        assertMeasuredStageGuard(source, "acceleratedBeginStage(Supplier<Optional<BeginReservation>> action)");
        assertMeasuredStageGuard(source, "acceleratedCommitStage(Supplier<OptionalLong> action)");
        assertMeasuredStageGuard(source, "ledgerBeginStage(IsolationLevel isolationLevel)");
        assertMeasuredStageGuard(source, "ledgerReserveStage(Transaction tx)");
        assertMeasuredStageGuard(source, "ledgerDecisionStage(Transaction tx, long commitTs)");
        assertMeasuredStageGuard(source, "ledgerAtomicCommitStage(Transaction tx)");
        assertMeasuredStageGuard(source, "private long ledgerPublishStage()");
        assertMeasuredStageGuard(source, "ledgerAwaitStage(Transaction tx, long commitTs)");
        assertMeasuredStageGuard(source, "createBeginMetadata(long txId, long startTs, IsolationLevel isolationLevel)");
        assertMeasuredStageGuard(source, "markSingleShardCommittedStage(Transaction tx, String participant, long commitTs)");
        assertMeasuredStageGuard(source, "prewriteBufferedMutationsStage(Transaction tx)");
        assertMeasuredStageGuard(source, "prewriteEagerMutationStage(Transaction tx, String shardId, Mutation mutation, StorageAdapter adapter)");
        assertMeasuredStageGuard(source, "prepareParticipantsStage(Transaction tx)");
        assertMeasuredStageGuard(source, "persistCommittedMetadataStage(Transaction tx, long commitTs, boolean explicitPrepareRequired)");
        assertMeasuredStageGuard(source, "commitParticipantStage(TransactionStage stage, Transaction tx, String shardId, long commitTs)");
        assertMeasuredStageGuard(source, "directCommitSingleShardParticipantStage(Transaction tx, String shardId, long commitTs)");
        assertMeasuredStageGuard(source, "rollbackStage(Transaction tx)");
        assertMeasuredStageGuard(source, "cleanupIntentsStage(Transaction tx, String shardId)");
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

        assertEquals(0, shard.directCommitCount());
        assertEquals(1, shard.prewriteCount);
        assertEquals(1, shard.commitCount());
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
     * 读取事务管理器源码。
     */
    private static String readTransactionManagerSource() throws Exception {
        return Files.readString(Path.of("src/main/java/com/flyingzc/azir/txn/TransactionManager.java"));
    }

    /**
     * 断言计量 helper 在包装业务动作前先判断 no-op metrics。
     */
    private static void assertMeasuredStageGuard(String source, String signature) {
        String methodSource = extractMethodSource(source, signature);
        int guardIndex = methodSource.indexOf("if (!metricsEnabled())");
        int measuredIndex = methodSource.indexOf("recordMeasuredStage(");

        assertTrue(guardIndex >= 0, signature + " should guard no-op metrics");
        assertTrue(measuredIndex >= 0, signature + " should record measured metrics when enabled");
        assertTrue(guardIndex < measuredIndex, signature + " should guard before recordMeasuredStage");
    }

    /**
     * 从源码中截取指定方法体。
     */
    private static String extractMethodSource(String source, String signature) {
        int signatureIndex = source.indexOf(signature);
        assertTrue(signatureIndex >= 0, signature + " should exist");
        int bodyStart = source.indexOf('{', signatureIndex);
        assertTrue(bodyStart >= 0, signature + " should have method body");

        int depth = 0;
        for (int index = bodyStart; index < source.length(); index++) {
            char current = source.charAt(index);
            if (current == '{') {
                depth++;
            } else if (current == '}') {
                depth--;
                if (depth == 0) {
                    return source.substring(signatureIndex, index + 1);
                }
            }
        }
        throw new AssertionError(signature + " should have balanced braces");
    }

    /**
     * 记录调用次数的测试存储适配器。
     */
    private static final class RecordingStorageAdapter implements StorageAdapter {
        private final String shardId;
        private int prewriteCount;
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
         * 仅模拟跨分片可见性能力，使计数测试能够进入参与者调用路径。
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
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
            prewriteCount++;
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
            throw new AssertionError("counting adapter does not implement atomic direct commit");
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

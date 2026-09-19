package mvcc.txn;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务选项测试，验证默认配置和显式预设不会混淆。
 */
class TransactionOptionsTest {
    /**
     * 验证默认事务选项开启 ACID 安全的性能优化。
     */
    @Test
    void shouldUseSafePerformanceOptionsByDefault() {
        TransactionOptions actual = TransactionOptions.defaults();

        assertEquals(ConflictMode.WAIT, actual.lockWaitPolicy().conflictMode());
        assertEquals(8, actual.retryPolicy().maxAttempts());
        assertTrue(actual.crossShardCommitPolicy().parallelPrewrite());
        assertTrue(actual.crossShardCommitPolicy().parallelPrepare());
        assertTrue(actual.crossShardCommitPolicy().parallelCommit());
        assertTrue(actual.crossShardCommitPolicy().parallelRollback());
        assertFalse(actual.crossShardCommitPolicy().asyncCommit());
        assertTrue(actual.bufferWrites());
        assertFalse(actual.lazyBeginMetadata());
        assertFalse(actual.deferIntentCleanup());
    }

    /**
     * 验证失败优先预设保留低延迟失败和串行提交行为。
     */
    @Test
    void shouldKeepExplicitFailFastPreset() {
        TransactionOptions actual = TransactionOptionsPresets.failFast();

        assertEquals(ConflictMode.FAIL_FAST, actual.lockWaitPolicy().conflictMode());
        assertEquals(1, actual.retryPolicy().maxAttempts());
        assertFalse(actual.crossShardCommitPolicy().parallelPrewrite());
        assertFalse(actual.crossShardCommitPolicy().parallelPrepare());
        assertFalse(actual.crossShardCommitPolicy().parallelCommit());
        assertFalse(actual.crossShardCommitPolicy().parallelRollback());
        assertFalse(actual.crossShardCommitPolicy().asyncCommit());
        assertFalse(actual.bufferWrites());
        assertFalse(actual.lazyBeginMetadata());
        assertFalse(actual.deferIntentCleanup());
    }
}

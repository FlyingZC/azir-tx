package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.storage.Mutation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务分类器测试，验证不同事务形态会选择预期协议路径。
 */
class TransactionClassifierTest {
    private final TransactionClassifier classifier = new TransactionClassifier();

    /**
     * 验证无参与分片事务会被分类为只读快照路径。
     */
    @Test
    void shouldClassifyReadOnlyTransaction() {
        Transaction tx = new Transaction(1L, 1L, IsolationLevel.REPEATABLE_READ);

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.defaults());

        assertEquals(TransactionProtocolType.READ_ONLY_SNAPSHOT, actual.protocolType());
        assertTrue(actual.participants().isEmpty());
    }

    /**
     * 验证单分片写事务会被分类为单分片 1PC 路径。
     */
    @Test
    void shouldClassifySingleShardTransaction() {
        Transaction tx = new Transaction(2L, 2L, IsolationLevel.REPEATABLE_READ);
        tx.bufferMutation("shard-1", Mutation.put("account:1", new byte[]{1}));

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.mysqlLikeDefaults());

        assertEquals(TransactionProtocolType.SINGLE_SHARD_1PC, actual.protocolType());
        assertEquals(1, actual.participants().size());
        assertTrue(actual.participants().contains("shard-1"));
    }

    /**
     * 验证启用 async commit 的跨分片事务会被分类为异步提交路径。
     */
    @Test
    void shouldClassifyCrossShardAsyncTransaction() {
        Transaction tx = new Transaction(3L, 3L, IsolationLevel.REPEATABLE_READ);
        tx.bufferMutation("shard-1", Mutation.put("account:1", new byte[]{1}));
        tx.bufferMutation("shard-2", Mutation.put("account:2", new byte[]{2}));

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.mysqlLikeDefaults());

        assertEquals(TransactionProtocolType.CROSS_SHARD_ASYNC_COMMIT, actual.protocolType());
        assertEquals(2, actual.participants().size());
    }

    /**
     * 验证未启用 async commit 的跨分片事务会被分类为同步 2PC 路径。
     */
    @Test
    void shouldClassifyCrossShardSynchronousTransaction() {
        Transaction tx = new Transaction(4L, 4L, IsolationLevel.REPEATABLE_READ);
        tx.addParticipant("shard-1");
        tx.addParticipant("shard-2");

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.defaults());

        assertEquals(TransactionProtocolType.CROSS_SHARD_SYNC_2PC, actual.protocolType());
        assertEquals(2, actual.participants().size());
    }
}

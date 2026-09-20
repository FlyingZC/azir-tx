package mvcc.txn;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitResult;
import mvcc.common.IsolationLevel;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.tso.AtomicTimestampOracle;
import mvcc.tso.CountingTimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务管理器只读提交流程测试。
 */
class TransactionManagerReadOnlyCommitTest {
    /**
     * 验证 lazy metadata 的只读事务提交不额外分配提交时间戳。
     */
    @Test
    void lazyReadOnlyCommitShouldReuseStartTimestampWithoutMetadataWrite() {
        CountingTimestampOracle tso = new CountingTimestampOracle(new AtomicTimestampOracle(100L));
        InMemoryTransactionMetadataStore metadataStore = new InMemoryTransactionMetadataStore();
        TransactionOptions options = new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(),
                true,
                true,
                false
        );
        TransactionManager manager = new TransactionManager(tso, metadataStore, options, Map.of());

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        TransactionCommitResult result = manager.commit(tx);

        assertEquals(tx.startTs(), result.commitTs());
        assertEquals(1L, tso.invocationCount());
        assertTrue(metadataStore.get(tx.txId()).isEmpty());
    }

    /**
     * 验证非 lazy metadata 的只读事务仍分配提交时间戳并更新元数据。
     */
    @Test
    void persistedReadOnlyCommitShouldAllocateCommitTimestamp() {
        CountingTimestampOracle tso = new CountingTimestampOracle(new AtomicTimestampOracle(100L));
        InMemoryTransactionMetadataStore metadataStore = new InMemoryTransactionMetadataStore();
        TransactionManager manager = new TransactionManager(tso, metadataStore, TransactionOptions.failFastDefaults(), Map.of());

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        TransactionCommitResult result = manager.commit(tx);

        assertEquals(2L, tso.invocationCount());
        assertTrue(result.commitTs() > tx.startTs());
        assertTrue(metadataStore.get(tx.txId()).isPresent());
    }
}

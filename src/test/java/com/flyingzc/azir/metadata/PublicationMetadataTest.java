package com.flyingzc.azir.metadata;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** 验证固定租约、终态单调性和有界元数据清理。 */
class PublicationMetadataTest {
    /** 默认租约合同拒绝缺失、截止不匹配、过期和已释放记录，不续期或缓存有效性。 */
    @Test
    void leaseValidationRequiresOriginalLiveLease() {
        var store = new InMemoryTransactionMetadataStore();
        assertFalse(store.isSnapshotLeaseValid(1, Long.MAX_VALUE));
        store.registerSnapshot(1, 1, Long.MAX_VALUE);
        assertTrue(store.isSnapshotLeaseValid(1, Long.MAX_VALUE));
        assertFalse(store.isSnapshotLeaseValid(1, Long.MAX_VALUE - 1));
        store.registerSnapshot(2, 2, 1);
        assertFalse(store.isSnapshotLeaseValid(2, 1));
        assertEquals(1L, store.snapshotExpiry(2).orElseThrow());
        store.releaseSnapshot(1);
        assertFalse(store.isSnapshotLeaseValid(1, Long.MAX_VALUE));
    }

    /** 畸形持久记录不能构造成可供读侧静默忽略的元数据。 */
    @Test
    void recordRejectsMissingCommittedTimestampStatusAndIsolation() {
        assertThrows(MvccException.class,
                () -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), TxStatus.COMMITTED, null));
        assertThrows(MvccException.class,
                () -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), null, null));
        assertThrows(MvccException.class,
                () -> new TxRecord(1, 1, null, Set.of(), TxStatus.ACTIVE, null));
        assertDoesNotThrow(() -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), TxStatus.PREPARING, null));
        assertDoesNotThrow(() -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), TxStatus.COMMITTED, 2L));
    }

    /** 所有提交入口都拒绝不晚于开始时间的提交时间戳，失败不得改变原状态。 */
    @Test
    void commitTimestampMustBeStrictlyAfterStartTimestamp() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 10, IsolationLevel.REPEATABLE_READ);
        store.registerParticipants(1, Set.of("a"));
        for (long commitTs : new long[]{Long.MIN_VALUE, 9, 10}) {
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> store.markLocalCommitPending(1, commitTs)).errorCode());
            assertThrows(MvccException.class, () -> store.markCommitted(1, commitTs));
            assertThrows(MvccException.class, () -> store.persistCommitted(1, Set.of("a"), commitTs));
            assertThrows(MvccException.class, () -> store.markSingleShardCommitted(1, "a", commitTs));
            assertThrows(MvccException.class, () -> store.createCommitted(2, 10, IsolationLevel.REPEATABLE_READ, Set.of("a"), commitTs));
            assertEquals(TxStatus.ACTIVE, store.get(1).orElseThrow().status());
            assertTrue(store.get(2).isEmpty());
        }
        store.markLocalCommitPending(1, 11);
        assertEquals(11L, store.markCommitted(1, 11).commitTs());
    }

    /** CSV 路由标识在创建、登记、快速更新及终态重放入口都必须合法。 */
    @Test
    void participantIdentifiersCannotBeBlankOrContainCsvSeparators() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 1, IsolationLevel.REPEATABLE_READ);
        for (String participant : new String[]{"", " ", "\t", "a,b", ","}) {
            Set<String> participants = Set.of(participant);
            assertThrows(MvccException.class, () -> store.createPreparing(2, 2, IsolationLevel.REPEATABLE_READ, participants));
            assertThrows(MvccException.class, () -> store.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, participants, 3));
            assertThrows(MvccException.class, () -> store.registerParticipants(1, participants));
            assertThrows(MvccException.class, () -> store.persistPreparing(1, participants));
            assertThrows(MvccException.class, () -> store.persistCommitted(1, participants, 3));
            assertThrows(MvccException.class, () -> store.markSingleShardCommitted(1, participant, 3));
            assertTrue(store.get(2).isEmpty());
            assertEquals(TxStatus.ACTIVE, store.get(1).orElseThrow().status());
        }
        store.markCommitted(1, Set.of("valid-shard"), 3);
        assertThrows(MvccException.class, () -> store.registerParticipants(1, Set.of("a,b")));
        assertThrows(MvccException.class, () -> store.persistPreparing(1, Set.of(" ")));
        assertThrows(MvccException.class, () -> store.markCommitted(1, Set.of(","), 3));
        assertEquals(Set.of("valid-shard"), store.get(1).orElseThrow().participants());
    }

    /** 单分片未知提交占位阻止新快照，只有真实终态才能清除该发布障碍。 */
    @Test
    void pendingLocalCommitBlocksPublicationUntilTerminalDecision() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 1, IsolationLevel.REPEATABLE_READ);
        assertThrows(MvccException.class, () -> store.markLocalCommitPending(1, 10));
        store.registerParticipants(1, Set.of("a"));
        store.markLocalCommitPending(1, 10);
        store.markLocalCommitPending(1, 10);
        var error = assertThrows(MvccException.class, store::assertNoPendingPublications);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        assertThrows(MvccException.class, () -> store.markLocalCommitPending(1, 11));
        assertThrows(MvccException.class, () -> store.markCommitted(1, 11));
        assertThrows(MvccException.class, () -> store.registerParticipants(1, Set.of("b")));
        store.markAborted(1);
        assertNull(store.get(1).orElseThrow().commitTs());
        assertDoesNotThrow(store::assertNoPendingPublications);
    }

    /** 延迟创建元数据的快照也必须阻止安全点跨越。 */
    @Test
    void lazySnapshotPinsOldestTimestampAndCannotBeRenewed() {
        var store = new InMemoryTransactionMetadataStore();
        store.registerSnapshot(10, 10, 100);
        assertEquals(10L, store.oldestActiveStartTs().orElseThrow());
        store.registerSnapshot(10, 20, 100);
        assertEquals(10L, store.oldestActiveStartTs().orElseThrow());
        assertThrows(MvccException.class, () -> store.registerSnapshot(10, 10, 200));
        assertEquals(0, store.expireSnapshots(99, 1));
        assertEquals(1, store.expireSnapshots(100, 1));
        assertTrue(store.snapshotExpiry(10).isEmpty());
    }

    /** 重复提交必须保留时间戳，快速更新和回滚都不得撤销提交。 */
    @Test
    void terminalStatusAndCommitTimestampAreImmutable() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(10, 10, IsolationLevel.REPEATABLE_READ);
        store.registerParticipants(10, Set.of("a"));
        store.registerParticipants(10, Set.of("b"));
        assertEquals(Set.of("a", "b"), store.get(10).orElseThrow().participants());
        store.persistCommitted(10, 20);
        store.persistPreparing(10, Set.of("c"));
        assertEquals(TxStatus.COMMITTED, store.markAborted(10).status());
        assertEquals(Set.of("a", "b"), store.registerParticipants(10, Set.of("c")).participants());
        var error = assertThrows(MvccException.class, () -> store.persistCommitted(10, 21));
        assertEquals(ErrorCode.ILLEGAL_STATE, error.errorCode());
        assertEquals(20L, store.markCommitted(10, 20).commitTs());
    }

    /** 恢复候选按事务编号分页，并跳过仍持有租约的未决事务。 */
    @Test
    void recoveryCandidatesUseLeasesAndCursor() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 1, IsolationLevel.REPEATABLE_READ);
        store.registerSnapshot(1, 1, 100);
        store.create(2, 2, IsolationLevel.REPEATABLE_READ);
        store.createCommitted(3, 3, IsolationLevel.REPEATABLE_READ, Set.of(), 4);
        assertEquals(2L, store.findRecoveryCandidates(99, 0, 1).getFirst().txId());
        assertEquals(3L, store.findRecoveryCandidates(99, 2, 1).getFirst().txId());
        assertEquals(1L, store.findRecoveryCandidates(100, 0, 1).getFirst().txId());
    }

    /** 仅完成物化且早于安全点与保留期限的终态可以被清理。 */
    @Test
    void purgeRequiresFinalizationAndHonorsBatchLimit() {
        var store = new InMemoryTransactionMetadataStore();
        store.createCommitted(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), 5);
        store.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, Set.of(), 6);
        assertEquals(0, store.purgeFinalized(10, Long.MAX_VALUE, 10));
        store.markFinalized(1);
        store.markFinalized(2);
        assertEquals(0, store.purgeFinalized(5, Long.MAX_VALUE, 10));
        assertEquals(1, store.purgeFinalized(10, Long.MAX_VALUE, 1));
        assertEquals(1, store.purgeFinalized(10, Long.MAX_VALUE, 1));
    }

    /** 同实例屏障可重入且异常退出后仍能使用。 */
    @Test
    void publicationLockIsReentrantAndReleasedAfterFailure() {
        var store = new InMemoryTransactionMetadataStore();
        assertThrows(IllegalArgumentException.class, () -> store.withPublicationLock(() -> {
            store.withPublicationLock(() -> store.create(1, 1, IsolationLevel.REPEATABLE_READ));
            throw new IllegalArgumentException("failure");
        }));
        assertEquals(1L, store.withPublicationLock(() -> store.get(1).orElseThrow()).txId());
    }
}

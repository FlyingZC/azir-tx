package mvcc.recovery;

import mvcc.common.IsolationLevel;
import mvcc.common.TxStatus;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ParticipantRecord;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.storage.mysql.MysqlStorageAdapter;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** 验证租约接管、单分片本地提交补中心及恢复错误隔离。 */
class PublicationRecoveryTest {
    /** 独立恢复器必须为旧二参 MySQL 适配器绑定中心权威，不需要启动数据库。 */
    @Test
    void standaloneRecoveryBindsLegacyMysqlAdapter() {
        var metadata = new InMemoryTransactionMetadataStore();
        try (var unopenedPool = new HikariDataSource()) {
            var adapter = new MysqlStorageAdapter("a", unopenedPool);
            assertFalse(adapter.supportsCommittedIntentResolution());
            new RecoveryManager(metadata, Map.of("a", adapter));
            assertTrue(adapter.supportsCommittedIntentResolution());
            assertDoesNotThrow(() -> adapter.configureMetadataStore(metadata));
            assertThrows(IllegalStateException.class, () -> adapter.configureMetadataStore(new InMemoryTransactionMetadataStore()));
        }
    }

    /** 非法路由在绑定之前被拒绝，合法映射才绑定同一元数据实例。 */
    @Test
    void recoveryValidatesRoutesBeforeBindingAuthority() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        assertThrows(IllegalArgumentException.class, () -> new RecoveryManager(metadata, Map.of("other", adapter)));
        assertThrows(mvcc.common.MvccException.class, () -> new RecoveryManager(metadata, Map.of(" ", adapter)));
        assertThrows(mvcc.common.MvccException.class, () -> new RecoveryManager(metadata, Map.of("a,b", adapter)));
        assertNull(adapter.configuredMetadata);
        new RecoveryManager(metadata, Map.of("a", adapter));
        assertSame(metadata, adapter.configuredMetadata);
    }

    /** 原子提交参与者的空记录、未决记录及不匹配终态都不能解除中心未知槽。 */
    @Test
    void atomicFenceRequiresMatchingDurableTerminalRecord() {
        List<Optional<ParticipantRecord>> invalidRecords = List.of(Optional.empty(),
                Optional.of(new ParticipantRecord(1, 1, TxStatus.ACTIVE, null)),
                Optional.of(new ParticipantRecord(1, 1, TxStatus.PREPARING, null)),
                Optional.of(new ParticipantRecord(2, 1, TxStatus.ABORTED, null)),
                Optional.of(new ParticipantRecord(1, 0, TxStatus.ABORTED, null)),
                Optional.of(new ParticipantRecord(1, 2, TxStatus.COMMITTED, 10L)),
                Optional.of(new ParticipantRecord(1, 1, TxStatus.COMMITTED, 11L)));
        for (Optional<ParticipantRecord> invalid : invalidRecords) {
            var metadata = new InMemoryTransactionMetadataStore();
            var adapter = new RecordingAdapter(metadata);
            adapter.atomicDirect = true;
            adapter.preserveFenceRecord = true;
            adapter.local = invalid.orElse(null);
            metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
            metadata.registerParticipants(1, Set.of("a"));
            metadata.markLocalCommitPending(1, 10);
            var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
            assertThrows(mvcc.common.MvccException.class, () -> recovery.recover(1));
            assertEquals(TxStatus.PREPARING, metadata.get(1).orElseThrow().status());
            assertEquals(10L, metadata.get(1).orElseThrow().commitTs());
            assertThrows(mvcc.common.MvccException.class, metadata::assertNoPendingPublications);
            assertTrue(adapter.aborted.isEmpty());
        }
    }

    /** 没有独立提交权威的旧适配器使用中心先回滚、锁外清理路径。 */
    @Test
    void nonAtomicParticipantDoesNotNeedLocalFenceRecord() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        new RecoveryManager(metadata, Map.of("a", adapter)).recover(1);
        assertEquals(TxStatus.ABORTED, metadata.get(1).orElseThrow().status());
        assertTrue(adapter.fenced.isEmpty());
        assertEquals(List.of(1L), adapter.aborted);
    }

    /** 本地终态 fencing 失败时必须保留中心占位，不能先写 ABORTED 放行新快照。 */
    @Test
    void failedParticipantFenceKeepsPendingPublication() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        adapter.failFence = true;
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.markLocalCommitPending(1, 10);
        var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
        assertThrows(IllegalStateException.class, () -> recovery.recover(1));
        assertEquals(TxStatus.PREPARING, metadata.get(1).orElseThrow().status());
        assertEquals(10L, metadata.get(1).orElseThrow().commitTs());
        assertThrows(mvcc.common.MvccException.class, metadata::assertNoPendingPublications);
        assertTrue(adapter.committed.isEmpty());
        assertTrue(adapter.aborted.isEmpty());
    }

    /** 有效租约保护 ACTIVE；到期后接管，元数据缺失的 intent 不构成回滚授权。 */
    @Test
    void activeLeaseAndMissingMetadataPreventAbort() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.registerSnapshot(1, 1, Long.MAX_VALUE);
        assertFalse(recovery.recover(1, Set.of("a")));
        assertFalse(recovery.recover(2, Set.of("a")));
        assertEquals(TxStatus.ACTIVE, metadata.get(1).orElseThrow().status());
        assertTrue(adapter.aborted.isEmpty());
        metadata.releaseSnapshot(1);
        metadata.registerSnapshot(1, 1, 1);
        assertTrue(recovery.recover(1, Set.of("a")));
        assertEquals(List.of(1L), adapter.aborted);
    }

    /** 本地已提交的单分片只能按原提交时间戳补中心，不能回滚。 */
    @Test
    void localCommitCompletesCentralDecisionOutsideFanoutLock() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        adapter.local = new ParticipantRecord(1, 1, TxStatus.COMMITTED, 10L);
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.markLocalCommitPending(1, 10);
        new RecoveryManager(metadata, Map.of("a", adapter)).recover(1);
        assertEquals(TxStatus.COMMITTED, metadata.get(1).orElseThrow().status());
        assertEquals(10L, metadata.get(1).orElseThrow().commitTs());
        assertEquals(List.of(1L), adapter.committed);
        assertTrue(adapter.aborted.isEmpty());
        assertEquals(List.of(1L), adapter.fenced);
        assertDoesNotThrow(metadata::assertNoPendingPublications);
        assertTrue(metadata.findRecoveryCandidates(Long.MAX_VALUE, 0, 10).isEmpty());
    }

    /** 元数据候选不依赖 intent，坏事务失败后同批后续事务仍会完成。 */
    @Test
    void metadataCandidatesRecoverWithoutIntentsAndIsolateFailures() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        metadata.createCommitted(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a"), 10);
        metadata.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, Set.of("a"), 11);
        adapter.failCommitTxId = 1;
        var recovery = new RecoveryManager(metadata, Map.of("a", adapter));
        var runner = new BackgroundRecoveryRunner(recovery, Map.of("a", adapter), 2, 2, 10_000);
        assertEquals(1, runner.recoverOnce(2));
        assertEquals(1, runner.transactionStats().failureCount());
        assertEquals(List.of(2L), adapter.committed);
        assertEquals(1L, metadata.findRecoveryCandidates(Long.MAX_VALUE, 0, 10).getFirst().txId());
        assertEquals(1L, metadata.oldestActiveStartTs().orElseThrow());
    }

    /** 保留扫描实际发现的分片，ABORTED 即使元数据参与者为空也会清理该分片。 */
    @Test
    void discoveredShardIsRetainedForTerminalCleanup() {
        var metadata = new InMemoryTransactionMetadataStore();
        var adapter = new RecordingAdapter(metadata);
        adapter.pending = Set.of(1L);
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.markAborted(1);
        var runner = new BackgroundRecoveryRunner(new RecoveryManager(metadata, Map.of("a", adapter)), Map.of("a", adapter));
        assertEquals(1, runner.recoverOnce(10));
        assertEquals(List.of(1L), adapter.aborted);
    }

    /** 记录恢复副作用并断言提交和回滚物化发生在发布锁外。 */
    private static final class RecordingAdapter implements StorageAdapter {
        private final InMemoryTransactionMetadataStore metadata;
        private final List<Long> committed = new ArrayList<>();
        private final List<Long> aborted = new ArrayList<>();
        private final List<Long> fenced = new ArrayList<>();
        private ParticipantRecord local;
        private Set<Long> pending = Set.of();
        private long failCommitTxId = -1;
        private boolean failFence;
        private boolean atomicDirect;
        private boolean preserveFenceRecord;
        private TransactionMetadataStore configuredMetadata;

        /** 注入真实元数据实例以检查监视器持有状态。 */
        private RecordingAdapter(InMemoryTransactionMetadataStore metadata) {
            this.metadata = metadata;
        }

        /** 返回测试分片编号。 */
        @Override
        public String shardId() { return "a"; }

        /** 测试适配器无需初始化。 */
        @Override
        public void initialize() { }

        /** 记录恢复器绑定的权威，供独立构造测试验证。 */
        @Override
        public void configureMetadataStore(TransactionMetadataStore store) { configuredMetadata = store; }

        /** 按测试场景声明是否拥有原子本地提交权威。 */
        @Override
        public boolean supportsAtomicDirectCommit() { return atomicDirect; }

        /** 此测试不执行读取。 */
        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) { return Optional.empty(); }

        /** 此测试不执行范围扫描。 */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange range) { return List.of(); }

        /** 此测试不执行预写。 */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) { }

        /** 此测试不执行 prepare。 */
        @Override
        public void prepare(long txId) { }

        /** 返回模拟的持久化本地决议。 */
        @Override
        public Optional<ParticipantRecord> participantRecord(long txId) { return Optional.ofNullable(local); }

        /** 断言物化不持有发布锁，并按配置模拟单事务错误。 */
        @Override
        public void commit(long txId, long commitTs) {
            assertFalse(Thread.holdsLock(metadata));
            if (txId == failCommitTxId) {
                throw new IllegalStateException("participant unavailable");
            }
            committed.add(txId);
        }

        /** 屏障内封住迟到写，屏障外记录物化；本地已提交事务的回滚保持无副作用。 */
        @Override
        public void abort(long txId) {
            if (Thread.holdsLock(metadata)) {
                if (failFence) {
                    throw new IllegalStateException("participant fence unavailable");
                }
                fenced.add(txId);
            } else {
                aborted.add(txId);
            }
            if (!preserveFenceRecord && (local == null || local.status() != TxStatus.COMMITTED)) {
                local = new ParticipantRecord(txId, txId, TxStatus.ABORTED, null);
            }
        }

        /** 返回模拟 intent 扫描结果。 */
        @Override
        public Set<Long> findPendingTransactionIds(int limit) { return pending; }

        /** 此测试不回收存储数据。 */
        @Override
        public int gc(long safePoint, int limit) { return 0; }
    }
}

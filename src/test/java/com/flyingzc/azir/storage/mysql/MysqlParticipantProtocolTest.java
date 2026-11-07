package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.VersionedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 参与者协议的真实数据库回归用例，使用屏障确定并发起点，不依赖线程休眠。
 */
@Testcontainers
class MysqlParticipantProtocolTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withUsername("test").withPassword("test").withDatabaseName("participant_protocol");

    private DataSource dataSource;
    private MysqlStorageAdapter adapter;
    private InMemoryTransactionMetadataStore metadata;

    /**
     * 为每个用例清空所有协议表并绑定新的中心决议实例。
     */
    @BeforeEach
    void setUp() throws Exception {
        dataSource = MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
        metadata = new InMemoryTransactionMetadataStore();
        adapter = new MysqlStorageAdapter("s1", dataSource);
        adapter.initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : List.of("mvcc_data", "mvcc_intent", "mvcc_rollback", "mvcc_participant_txn", "mvcc_key_lock")) {
                statement.executeUpdate("delete from " + table);
            }
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null where singleton_id = 1");
        }
    }

    /**
     * 关闭用例使用的连接池。
     */
    @AfterEach
    void tearDown() throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * 验证不存在的空字符串键也只能由一个并发预写持有。
     */
    @Test
    void concurrentPrewritesMustConflictOnAbsentEmptyKey() throws Exception {
        List<ErrorCode> outcomes = race(
                () -> adapter.prewrite(10, 10, List.of(put("", "first"))),
                () -> adapter.prewrite(11, 11, List.of(put("", "second"))));
        assertEquals(1, outcomes.stream().filter(code -> code == null).count());
        assertEquals(1, outcomes.stream().filter(code -> code == ErrorCode.LOCK_CONFLICT).count());
    }

    /**
     * 验证直接提交和空键预写的并发执行只能产生一个成功写入者。
     */
    @Test
    void directCommitMustSerializeWithIntentCreation() throws Exception {
        List<ErrorCode> outcomes = race(
                () -> adapter.directCommit(10, 10, 30, List.of(put("key", "direct"))),
                () -> adapter.prewrite(11, 11, List.of(put("key", "intent"))));
        assertEquals(1, outcomes.stream().filter(code -> code == null).count());
        assertEquals(1, outcomes.stream().filter(code -> code == ErrorCode.LOCK_CONFLICT || code == ErrorCode.WRITE_CONFLICT).count());
    }

    /**
     * 验证没有意向的回滚仍阻止迟到预写和直接提交。
     */
    @Test
    void abortWithoutIntentMustFenceLateRequests() {
        adapter.abort(10);
        assertEquals(TxStatus.ABORTED, adapter.participantRecord(10).orElseThrow().status());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 30, List.of(put("key", "late"))));
    }

    /**
     * 验证等价提交可以重放，而改变提交时间戳和终态后的预写会被拒绝。
     */
    @Test
    void commitReplayMustPreserveTimestampAndTerminalState() {
        List<Mutation> writes = List.of(put("key", "value"));
        adapter.prewrite(10, 10, writes);
        commitDecision(10, 30);
        adapter.commit(10, 30, writes);
        adapter.commit(10, 30);
        adapter.commit(10, 30, writes);
        assertThrows(MvccException.class, () -> adapter.commit(10, 31));
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, writes));
        adapter.abort(10);
        assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        assertEquals(30L, adapter.participantRecord(10).orElseThrow().commitTs().longValue());
    }

    /** 未安装第二版例程的旧分片重启后仍使用第一版完成同一持久协议。 */
    @Test
    void oldSingleKeyCommitRoutineMustRemainAvailableAfterRestart() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop procedure if exists mvcc_commit_one_v2");
        }
        var restarted = new MysqlStorageAdapter("s1", dataSource, metadata);
        restarted.requireInitializedFastPath();
        restarted.prewrite(10, 10, List.of(put("legacy:routine", "value")));
        commitDecision(10, 20);
        restarted.commit(10, 20, List.of(put("legacy:routine", "value")), true);
        assertEquals(TxStatus.COMMITTED, restarted.participantRecord(10).orElseThrow().status());
        assertArrayEquals(bytes("value"), restarted.get(read(99, 30), "legacy:routine").orElseThrow().value());
    }

    /** 单键墓碑提交可暂留意向，重放清理时必须核对原值和终态。 */
    @Test
    void singleKeyDeleteCommitMustSupportDeferredCleanupAndVerifiedReplay() throws Exception {
        adapter.enableSingleKeyCommitV2();
        List<Mutation> writes = List.of(Mutation.delete("key"));
        adapter.prewrite(10, 10, writes);
        commitDecision(10, 30);
        adapter.commit(10, 30, writes, false);
        assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             var rows = statement.executeQuery("select count(*) from mvcc_intent where tx_id = 10")) {
            assertTrue(rows.next());
            assertEquals(1, rows.getInt(1));
        }
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, List.of(put("key", "changed")), true));
        adapter.commit(10, 30, writes, true);
        adapter.commit(10, 30, writes, true);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             var rows = statement.executeQuery("select count(*) from mvcc_intent where tx_id = 10")) {
            assertTrue(rows.next());
            assertEquals(0, rows.getInt(1));
        }
    }

    /**
     * 验证内存写集合不能改变已落盘意向，也不能在缺失意向时制造版本。
     */
    @Test
    void knownMutationsMustMatchAllDurableIntents() throws Exception {
        adapter.enableSingleKeyCommitV2();
        List<Mutation> writes = List.of(put("key", "original"));
        adapter.prewrite(10, 10, writes);
        commitDecision(10, 30);
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, List.of(put("key", "changed"))));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent where tx_id = 10");
        }
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, writes));
        assertTrue(adapter.get(read(99, 40), "key").isEmpty());
    }

    /**
     * 验证中心未提交时不能把已知写集合物化为已提交版本。
     */
    @Test
    void materializationRequiresCommittedDecision() {
        List<Mutation> writes = List.of(put("key", "value"));
        adapter.prewrite(10, 10, writes);
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, writes));
        assertTrue(adapter.get(read(99, 40), "key").isEmpty());
    }

    /**
     * 验证直接提交与本地决议原子保存，且不能重放为不同写集合。
     */
    @Test
    void directCommitReplayMustVerifyPersistedValues() {
        List<Mutation> writes = List.of(put("key", "value"));
        adapter.directCommit(10, 10, 30, writes);
        adapter.directCommit(10, 10, 30, writes);
        assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 31, writes));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 30, List.of(put("key", "changed"))));
    }

    /**
     * 验证主键冲突不会覆盖其他事务已经提交的版本。
     */
    @Test
    void duplicateVersionMustNeverOverwriteAnotherTransaction() throws Exception {
        adapter.prewrite(10, 10, List.of(put("key", "intent")));
        commitDecision(10, 30);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value) values ('key', 30, 20, 0, 'existing')");
        }
        assertThrows(MvccException.class, () -> adapter.commit(10, 30));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             var rows = statement.executeQuery("select tx_id, value from mvcc_data where user_key = 'key' and commit_ts = 30")) {
            assertTrue(rows.next());
            assertEquals(20, rows.getLong("tx_id"));
            assertArrayEquals(bytes("existing"), rows.getBytes("value"));
        }
    }

    /**
     * 验证分页游标来自原始键，自身删除可以产生空页且仍然继续扫描。
     */
    @Test
    void pagesMustOverlayOwnWritesAndAdvancePastDeletedKeys() {
        adapter.directCommit(10, 10, 20, List.of(put("a", "old"), put("b", "old")));
        adapter.prewrite(30, 30, List.of(Mutation.delete("a"), put("b", "own"), put("c", "new")));
        KeyRange range = new KeyRange("a", "z");
        ScanPage first = adapter.scanPage(read(30, 30), range, null, 1);
        assertTrue(first.values().isEmpty());
        assertEquals("a", first.nextKey());
        ScanPage second = adapter.scanPage(read(30, 30), range, first.nextKey(), 1);
        assertEquals(List.of("b"), second.values().stream().map(VersionedValue::key).toList());
        assertTrue(second.values().getFirst().ownIntent());
        assertArrayEquals(bytes("own"), second.values().getFirst().value());
        assertEquals(List.of("b", "c"), adapter.scan(read(30, 30), range).stream().map(VersionedValue::key).toList());
    }

    /**
     * 验证数据库二进制顺序与 Java UTF-16 顺序不同的键不会乱序或漏页。
     */
    @Test
    void pagesMustFollowUnsignedUtf8Order() {
        String bmp = "\uE000";
        String supplementary = "\uD800\uDC00";
        adapter.directCommit(10, 10, 20, List.of(put(supplementary, "s"), put(bmp, "b")));
        KeyRange range = new KeyRange("", "\uDBFF\uDFFF");
        ScanPage first = adapter.scanPage(read(99, 30), range, null, 1);
        assertEquals(bmp, first.values().getFirst().key());
        assertEquals(supplementary, adapter.scanPage(read(99, 30), range, first.nextKey(), 1).values().getFirst().key());
    }

    /**
     * 验证清理安全水位之后拒绝旧请求，同时保留没有中心决议的本地提交证据。
     */
    @Test
    void gcMustFenceOldRequestsAndRetainUnpublishedDecisions() {
        adapter.configureMetadataStore(metadata);
        metadata.createPreparing(10, 10, IsolationLevel.REPEATABLE_READ, Set.of("s1"));
        metadata.markLocalCommitPending(10, 20);
        adapter.directCommit(10, 10, 20, List.of(put("key", "value")));
        adapter.gc(40, 32);
        assertTrue(adapter.participantRecord(10).isPresent());
        assertThrows(MvccException.class, () -> adapter.prewrite(11, 11, List.of(put("new", "late"))));
        assertThrows(MvccException.class, () -> adapter.directCommit(12, 12, 50, List.of(put("new", "late"))));
        assertThrows(MvccException.class, () -> adapter.get(read(99, 39), "key"));
    }

    /**
     * 验证同一参与者不能被静默改绑到另一中心权威。
     */
    @Test
    void metadataAuthorityMustRemainStable() {
        adapter.configureMetadataStore(metadata);
        assertTrue(adapter.supportsAtomicDirectCommit());
        assertTrue(adapter.supportsCommittedIntentResolution());
        assertThrows(IllegalStateException.class, () -> adapter.configureMetadataStore(new InMemoryTransactionMetadataStore()));
    }

    /**
     * 验证真实 COMMIT 成功后响应丢失不会被底层锁冲突 SQLState 误判为可直接回滚。
     */
    @Test
    void lostCommitResponseMustRemainUnknownAndRecoverFromLocalRecord() {
        MysqlStorageAdapter unreliable = new MysqlStorageAdapter("s1", loseCommitResponse(dataSource));
        MvccException failure = assertThrows(MvccException.class,
                () -> unreliable.directCommit(10, 10, 20, List.of(put("key", "committed"))));
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE, failure.errorCode());
        assertFalse(failure.retryable());
        adapter.abort(10);
        assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        assertArrayEquals(bytes("committed"), adapter.get(read(99, 30), "key").orElseThrow().value());
    }

    /**
     * 验证本地终态不会在中心 finalized 前被清理，清理后水位阻止旧请求复活。
     */
    @Test
    void gcMustWaitForFinalizationAndFenceRequestsAfterRecordRemoval() throws Exception {
        adapter.prewrite(10, 10, List.of(put("key", "value")));
        commitDecision(10, 20);
        adapter.commit(10, 20);
        ageTerminalRecords();
        adapter.gc(40, 4);
        assertTrue(adapter.participantRecord(10).isPresent());
        metadata.markFinalized(10);
        adapter.gc(40, 4);
        assertTrue(adapter.participantRecord(10).isEmpty());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 20, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.commit(10, 20));
    }

    /**
     * 验证回滚标记和事务终态共用一个严格的 GC 删除行数预算。
     */
    @Test
    void gcMustBoundRollbackAndParticipantDeletion() throws Exception {
        adapter.prewrite(10, 10, List.of(put("a", "value"), put("b", "value"), put("c", "value")));
        adapter.configureMetadataStore(metadata);
        metadata.create(10, 10, IsolationLevel.REPEATABLE_READ);
        metadata.markAborted(10);
        adapter.abort(10);
        metadata.markFinalized(10);
        ageTerminalRecords();
        for (int index = 0; index < 16; index++) {
            assertTrue(adapter.gc(40, 1) <= 1);
        }
        assertTrue(adapter.participantRecord(10).isEmpty());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("a", "late"))));
    }

    /**
     * 验证墓碑链按旧到新逐行回收，清理中不复活旧值，并最终释放空键锁。
     */
    @Test
    void tombstoneMustBeDeletedLastWithinSharedGcBudget() throws Exception {
        adapter.directCommit(10, 10, 20, List.of(put("key", "first")));
        adapter.directCommit(30, 30, 40, List.of(put("key", "second")));
        adapter.directCommit(50, 50, 60, List.of(Mutation.delete("key")));
        for (int index = 0; index < 4; index++) {
            assertEquals(1, adapter.gc(80, 1));
            var value = adapter.get(read(99, 80), "key");
            assertTrue(value.isEmpty() || value.orElseThrow().deleted());
        }
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             var rows = statement.executeQuery("select count(*) from mvcc_key_lock where user_key = 'key'")) {
            assertTrue(rows.next());
            assertEquals(0, rows.getInt(1));
        }
        assertThrows(MvccException.class, () -> adapter.prewrite(15, 15, List.of(put("key", "late"))));
        adapter.prewrite(90, 90, List.of(put("key", "new")));
        assertArrayEquals(bytes("new"), adapter.get(read(90, 90), "key").orElseThrow().value());
    }

    /**
     * 验证不可回收的前置键也推进持久游标，新适配器从后续键继续清理。
     */
    @Test
    void gcCursorMustSurviveAdapterRestartAndSkipUnreclaimableKeys() throws Exception {
        adapter.directCommit(10, 10, 20, List.of(put("a", "keep"), put("b", "old")));
        adapter.directCommit(30, 30, 40, List.of(put("b", "latest")));
        assertEquals(0, adapter.gc(60, 1));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             var rows = statement.executeQuery("select gc_after_key from mvcc_local_safe_point where singleton_id = 1")) {
            assertTrue(rows.next());
            assertArrayEquals(bytes("a"), rows.getBytes(1));
        }
        MysqlStorageAdapter restarted = new MysqlStorageAdapter("s1", dataSource, metadata);
        assertEquals(1, restarted.gc(60, 1));
        assertArrayEquals(bytes("latest"), restarted.get(read(99, 60), "b").orElseThrow().value());
    }

    /**
     * 验证每页同一事务只解析一次，非终态不会污染下一次分页读取。
     */
    @Test
    void pageMustResolveEachForeignTransactionOnceWithoutCachingActiveState() {
        adapter.prewrite(10, 10, List.of(put("a", "one"), put("b", "two"), Mutation.delete("c")));
        metadata.create(10, 10, IsolationLevel.REPEATABLE_READ);
        AtomicInteger lookups = new AtomicInteger();
        TransactionMetadataStore tracked = (TransactionMetadataStore) Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(), new Class<?>[]{TransactionMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("get")) {
                        lookups.incrementAndGet();
                    }
                    try {
                        return method.invoke(metadata, arguments);
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", dataSource, tracked);
        KeyRange range = new KeyRange("a", "z");
        assertTrue(reader.scanPage(read(99, 30), range, null, 10).values().isEmpty());
        assertEquals(1, lookups.get());
        metadata.markCommitted(10, 20);
        assertEquals(2, reader.scanPage(read(99, 30), range, null, 10).values().size());
        assertEquals(2, lookups.get());
    }

    /**
     * 验证页大小边界在访问 JDBC 之前被拒绝。
     */
    @Test
    void pageSizeMustRemainBounded() {
        assertThrows(IllegalArgumentException.class, () -> adapter.scanPage(read(99, 30), new KeyRange("", "z"), null, 0));
        assertThrows(IllegalArgumentException.class, () -> adapter.scanPage(read(99, 30), new KeyRange("", "z"), null, 4097));
    }

    /**
     * 验证绑定中心权威的直接提交拒绝缺失决议和缺失候选时间，接受正确的发布预约。
     */
    @Test
    void boundDirectCommitMustRequireMatchingPublicationReservation() {
        adapter.configureMetadataStore(metadata);
        List<Mutation> writes = List.of(put("key", "value"));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 20, writes));
        metadata.createPreparing(10, 10, IsolationLevel.REPEATABLE_READ, Set.of("s1"));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 20, writes));
        metadata.markLocalCommitPending(10, 20);
        adapter.directCommit(10, 10, 20, writes);
        adapter.abort(10);
        assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
    }

    /**
     * 验证中心先回收已完成事务后，本地仍能依赖持久安全水位清理终态。
     */
    @Test
    void gcMustReclaimLocalTerminalAfterMetadataWasPurged() throws Exception {
        adapter.prewrite(10, 10, List.of(put("key", "value")));
        commitDecision(10, 20);
        adapter.commit(10, 20);
        metadata.markFinalized(10);
        assertEquals(1, metadata.purgeFinalized(40, Long.MAX_VALUE, 4));
        ageTerminalRecords();
        adapter.gc(40, 4);
        assertTrue(adapter.participantRecord(10).isEmpty());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.commit(10, 20));
    }

    /**
     * 验证等待键锁之前已经执行过普通查询时，写事务仍能发现随后提交的新版本。
     */
    @Test
    void writeTransactionMustReadCurrentVersionsAfterKeyLockAcquisition() {
        DataSource intercepted = interceptSql(dataSource, "insert into mvcc_key_lock", true,
                () -> adapter.directCommit(20, 20, 40, List.of(put("key", "winner"))));
        MysqlStorageAdapter contender = new MysqlStorageAdapter("s1", intercepted);
        MvccException conflict = assertThrows(MvccException.class,
                () -> contender.directCommit(10, 10, 50, List.of(put("key", "late"))));
        assertEquals(ErrorCode.WRITE_CONFLICT, conflict.errorCode());
        assertArrayEquals(bytes("winner"), adapter.get(read(99, 60), "key").orElseThrow().value());
    }

    /**
     * 验证在版本查询与意向查询之间完成物化不会造成漏读，读取必须共享同一 RR 快照。
     */
    @Test
    void readTransactionMustKeepOneSnapshotAcrossMaterializationSwitch() {
        adapter.prewrite(10, 10, List.of(put("key", "committed")));
        commitDecision(10, 20);
        DataSource intercepted = interceptSql(dataSource, "select d.user_key", false, () -> adapter.commit(10, 20));
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", intercepted, metadata);
        assertArrayEquals(bytes("committed"), reader.get(read(99, 30), "key").orElseThrow().value());
    }

    /**
     * 验证版本查询后发生物化和决议清理时，点读重建本地快照但保持原全局 readTs。
     */
    @Test
    void getMustRebuildSnapshotWhenMaterializedIntentDecisionWasPurged() {
        adapter.directCommit(1, 1, 5, List.of(put("key", "old")));
        adapter.prewrite(10, 10, List.of(put("key", "committed")));
        commitDecision(10, 20);
        DataSource intercepted = interceptSql(dataSource, "select d.user_key", false,
                () -> materializeAndPurgeBeforeSnapshot30("key"));
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", intercepted, metadata);

        VersionedValue value = reader.get(read(99, 30), "key").orElseThrow();
        assertArrayEquals(bytes("committed"), value.value());
        assertEquals(20, value.versionTs());
        assertFalse(value.ownIntent());
        assertTrue(metadata.get(10).isEmpty());
    }

    /**
     * 验证分页在决议被清理后重新收集原范围和游标下的键，保留删除语义及全局快照。
     */
    @Test
    void pageMustRebuildSnapshotWithoutChangingRangeCursorOrReadTimestamp() {
        adapter.directCommit(1, 1, 5, List.of(put("a", "before"), put("b", "old"), put("c", "old"), put("d", "old")));
        adapter.prewrite(10, 10, List.of(put("b", "committed"), Mutation.delete("c"), put("d", "next")));
        commitDecision(10, 20);
        DataSource intercepted = interceptSql(dataSource, "select d.user_key", false,
                () -> materializeAndPurgeBeforeSnapshot30("b"));
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", intercepted, metadata);

        ScanPage page = reader.scanPage(read(99, 30), new KeyRange("a", "z"), "a", 2);
        assertEquals(List.of("b", "d"), page.values().stream().map(VersionedValue::key).toList());
        assertArrayEquals(bytes("committed"), page.values().getFirst().value());
        assertEquals(20, page.values().getFirst().versionTs());
        assertNull(page.nextKey());
    }

    /**
     * 验证缺失决议持续存在时，点读与分页最多尝试两次且不能返回旧值。
     */
    @Test
    void missingDecisionMustFailClosedAfterExactlyOneSnapshotRebuild() {
        adapter.directCommit(1, 1, 5, List.of(put("key", "old")));
        adapter.prewrite(10, 10, List.of(put("key", "unknown")));
        AtomicInteger lookups = new AtomicInteger();
        MysqlStorageAdapter reader = readerCountingDecisions(lookups);

        MvccException getFailure = assertThrows(MvccException.class, () -> reader.get(read(99, 30), "key"));
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE, getFailure.errorCode());
        assertFalse(getFailure.retryable());
        assertEquals(2, lookups.get());
        lookups.set(0);
        MvccException pageFailure = assertThrows(MvccException.class,
                () -> reader.scanPage(read(99, 30), new KeyRange("a", "z"), null, 2));
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE, pageFailure.errorCode());
        assertFalse(pageFailure.retryable());
        assertEquals(2, lookups.get());

        // 未绑定 resolver 的底层适配器保留原行为，本事务自己的意向也无需查询中心。
        assertArrayEquals(bytes("old"), adapter.get(read(99, 30), "key").orElseThrow().value());
        lookups.set(0);
        assertArrayEquals(bytes("unknown"), reader.get(read(10, 30), "key").orElseThrow().value());
        assertEquals(0, lookups.get());
    }

    /**
     * 验证真实存在的 ACTIVE 和 PREPARING 意向仍被忽略，不触发本地快照重建。
     */
    @Test
    void undecidedExistingRecordsMustNotTriggerSnapshotRebuild() {
        adapter.directCommit(1, 1, 5, List.of(put("key", "old")));
        adapter.prewrite(10, 10, List.of(put("key", "pending")));
        metadata.create(10, 10, IsolationLevel.REPEATABLE_READ);
        AtomicInteger lookups = new AtomicInteger();
        MysqlStorageAdapter reader = readerCountingDecisions(lookups);
        assertArrayEquals(bytes("old"), reader.get(read(99, 30), "key").orElseThrow().value());
        assertEquals(1, lookups.get());
        metadata.markPreparing(10, Set.of("s1"));
        assertEquals(List.of("key"), reader.scanPage(read(99, 30), new KeyRange("a", "z"), null, 2)
                .values().stream().map(VersionedValue::key).toList());
        assertEquals(2, lookups.get());
    }

    /**
     * 在合法水位 readTs - 1 完成物化与决议清理，再加入全局快照不可见的新版本。
     */
    private void materializeAndPurgeBeforeSnapshot30(String futureKey) {
        adapter.commit(10, 20);
        metadata.markFinalized(10);
        assertEquals(1, metadata.purgeFinalized(29, Long.MAX_VALUE, 4));
        new MysqlStorageAdapter("s1", dataSource).directCommit(35, 35, 40, List.of(put(futureKey, "future")));
        adapter.gc(29, 16);
    }

    /**
     * 记录实际中心查询次数，不改变真实内存权威返回的事务状态。
     */
    private MysqlStorageAdapter readerCountingDecisions(AtomicInteger lookups) {
        TransactionMetadataStore tracked = (TransactionMetadataStore) Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(), new Class<?>[]{TransactionMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("get")) {
                        lookups.incrementAndGet();
                    }
                    return invoke(metadata, method, arguments);
                });
        return new MysqlStorageAdapter("s1", dataSource, tracked);
    }

    /**
     * 在真实 JDBC 语句前后插入一次确定性操作，只控制交错时序而不替代数据库结果。
     */
    private DataSource interceptSql(DataSource delegate, String sqlPrefix, boolean before, Runnable action) {
        AtomicBoolean pending = new AtomicBoolean(true);
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    Object result = invoke(delegate, method, arguments);
                    if (!(result instanceof Connection connection)) {
                        return result;
                    }
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (connectionProxy, connectionMethod, parameters) -> {
                                Object value = invoke(connection, connectionMethod, parameters);
                                if (!(value instanceof PreparedStatement statement) || parameters == null
                                        || !(parameters[0] instanceof String sql) || !sql.stripLeading().startsWith(sqlPrefix)) {
                                    return value;
                                }
                                return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                                        (statementProxy, statementMethod, binds) -> {
                                            boolean trigger = statementMethod.getName().startsWith("execute") && pending.compareAndSet(true, false);
                                            if (trigger && before) {
                                                action.run();
                                            }
                                            Object rows = invoke(statement, statementMethod, binds);
                                            if (trigger && !before) {
                                                action.run();
                                            }
                                            return rows;
                                        });
                            });
                });
    }

    /**
     * 透传真实 JDBC 异常，避免反射包装改变被验证的异常类型。
     */
    private Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException ex) {
            throw ex.getCause();
        }
    }

    /**
     * 直接移动测试记录的墙钟年龄，避免为保留期测试使用 sleep。
     */
    private void ageTerminalRecords() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_participant_txn set updated_at = 0");
            statement.executeUpdate("update mvcc_rollback set created_at = 0");
        }
    }

    /**
     * 代理真实连接并只丢失第一次成功提交的响应，不模拟数据库持久化行为。
     */
    private DataSource loseCommitResponse(DataSource delegate) {
        AtomicBoolean pending = new AtomicBoolean(true);
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (!(result instanceof Connection connection)) {
                            return result;
                        }
                        return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                                (connectionProxy, connectionMethod, parameters) -> {
                                    try {
                                        Object value = connectionMethod.invoke(connection, parameters);
                                        if (connectionMethod.getName().equals("commit") && pending.compareAndSet(true, false)) {
                                            throw new SQLException("lost successful COMMIT response", "40001", 1213);
                                        }
                                        return value;
                                    } catch (InvocationTargetException ex) {
                                        throw ex.getCause();
                                    }
                                });
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /**
     * 在同一屏障后启动两个操作并收集协议结果，异常以外的失败由 Future 传播。
     */
    private List<ErrorCode> race(Runnable first, Runnable second) throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var left = executor.submit(contender(first, ready, start));
            var right = executor.submit(contender(second, ready, start));
            try {
                assertTrue(ready.await(10, TimeUnit.SECONDS));
                start.countDown();
                return java.util.Arrays.asList(left.get(20, TimeUnit.SECONDS), right.get(20, TimeUnit.SECONDS));
            } finally {
                start.countDown();
                left.cancel(true);
                right.cancel(true);
            }
        }
    }

    /**
     * 将一个并发操作包装为有界等待且返回明确冲突码的任务。
     */
    private Callable<ErrorCode> contender(Runnable operation, CountDownLatch ready, CountDownLatch start) {
        return () -> {
            ready.countDown();
            assertTrue(start.await(10, TimeUnit.SECONDS));
            try {
                operation.run();
                return null;
            } catch (MvccException ex) {
                return ex.errorCode();
            }
        };
    }

    /**
     * 写入可供参与者核对的中心提交决议。
     */
    private void commitDecision(long txId, long commitTs) {
        adapter.configureMetadataStore(metadata);
        metadata.createCommitted(txId, txId, IsolationLevel.REPEATABLE_READ, Set.of("s1"), commitTs);
    }

    /**
     * 构造可读的字符串写入值。
     */
    private static Mutation put(String key, String value) {
        return Mutation.put(key, bytes(value));
    }

    /**
     * 构造指定事务和时间戳的读取上下文。
     */
    private static ReadContext read(long txId, long readTs) {
        return new ReadContext(txId, readTs, IsolationLevel.REPEATABLE_READ);
    }

    /**
     * 按 UTF-8 编码测试数据。
     */
    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

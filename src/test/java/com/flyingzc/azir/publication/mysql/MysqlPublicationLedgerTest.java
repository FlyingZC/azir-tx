package com.flyingzc.azir.publication.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.publication.PublicationEntryStatus;
import com.flyingzc.azir.publication.PublicationRecoveryManager;
import com.flyingzc.azir.publication.PublicationRecoveryRunner;
import com.flyingzc.azir.publication.SegmentPublicationRecoveryManager;
import com.flyingzc.azir.publication.SegmentPublicationRecoveryRunner;
import com.flyingzc.azir.publication.SegmentPublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.PublicationSegmentCommit;
import com.flyingzc.azir.publication.SegmentPublicationGcManager;
import com.flyingzc.azir.publication.PublicationGcManager;
import com.flyingzc.azir.publication.PublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.PublicationIntentCommit;
import com.flyingzc.azir.publication.PublicationLocalCommitRequest;
import com.flyingzc.azir.publication.PublicationLocalCommitProof;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 用真实 MySQL 验证隔离账本的乱序决议、旧协议排空和 GC 栅栏。 */
@Testcontainers
class MysqlPublicationLedgerTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("ledger").withUsername("test").withPassword("test");
    private static HikariDataSource dataSource;
    private MysqlPublicationLedger ledger;
    private MysqlTransactionMetadataStore metadata;

    /** 初始化旧控制面结构供新账本迁移检查使用。 */
    @BeforeAll
    static void initializeControl() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
        new MysqlTimestampOracle(dataSource).initialize(100);
        new MysqlTransactionMetadataStore(dataSource).initialize();
    }

    /** 关闭本测试独立连接池。 */
    @AfterAll
    static void closeControl() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /** 每个场景独立从已排空的旧 TSO 上界建立新账本。 */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_publication_segment_slot_v1");
            statement.execute("drop table if exists mvcc_publication_segment_v1");
            statement.execute("drop table if exists mvcc_segment_publication_state_v1");
            statement.execute("drop table if exists mvcc_publication_state_v1_legacy_archived");
            statement.execute("drop table if exists mvcc_snapshot_lease");
            statement.execute("drop table if exists mvcc_legacy_lease_gate_v1");
            statement.execute("drop table if exists mvcc_protocol_epoch_v1");
            statement.execute("drop table if exists mvcc_snapshot_lease_v2");
            statement.execute("drop table if exists mvcc_coordinator_snapshot_lease_v1");
            statement.execute("drop table if exists mvcc_publication_entry_v1");
            statement.execute("drop table if exists mvcc_publication_state_v1");
            statement.execute("drop table if exists mvcc_intent");
            statement.execute("drop table if exists mvcc_data");
            statement.execute("drop table if exists mvcc_rollback");
            statement.execute("drop table if exists mvcc_participant_txn");
            statement.execute("drop table if exists mvcc_key_lock");
            statement.execute("drop table if exists mvcc_local_safe_point");
            statement.executeUpdate("delete from mvcc_txn_finalized");
            statement.executeUpdate("delete from mvcc_txn");
            statement.executeUpdate("update mvcc_tso set current_value = 100 where sequence_name = 'global'");
        }
        metadata = new MysqlTransactionMetadataStore(dataSource);
        metadata.initialize();
        ledger = new MysqlPublicationLedger(dataSource);
        ledger.initialize();
    }

    /** 较大时间戳先决议仍不可见，较小预留恢复终态后才连续发布。 */
    @Test
    void outOfOrderDecisionsMustNotRetroactivelyChangeRepeatableRead() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, true);
        assertEquals(100, first.startTs());
        assertEquals(100, second.startTs());
        assertTrue(first.txId() > 100);
        assertTrue(second.txId() > first.txId());
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        long high = ledger.reserveCommit(second.txId(), second.deadlineMillis());
        assertEquals(low, ledger.reserveCommit(first.txId(), first.deadlineMillis()));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveCommit(first.txId(),
                        first.deadlineMillis() + 1)).errorCode());
        assertEquals(low + 1, high);
        ledger.decideCommitted(second.txId(), high, Set.of("b"));
        assertEquals(100, ledger.advancePublished(16));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> ledger.awaitPublished(second.txId(), high, 20))
                        .errorCode());
        var oldReader = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        assertEquals(100, oldReader.startTs());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        ledger.decideAbortedAfterFencing(first.txId(), low);
        assertEquals(high, ledger.advancePublished(16));
        ledger.awaitPublished(second.txId(), high, 1000);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> ledger.awaitPublished(first.txId(), low, 1000))
                        .errorCode());
        assertEquals(100, oldReader.startTs());
        assertEquals(high, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        assertEquals(TxStatus.COMMITTED, metadata.get(second.txId()).orElseThrow().status());
        assertEquals(high, metadata.get(second.txId()).orElseThrow().commitTs().longValue());
    }

    /** 决议参数不匹配必须同时保留元数据 ACTIVE 与账本 PENDING。 */
    @Test
    void mismatchedDecisionMustRollBackMetadataAndLedgerTogether() throws Exception {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a", "b"));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.decideCommitted(started.txId(), commitTs,
                        Set.of("a"))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(100, ledger.advancePublished(16));
        ledger.decideCommitted(started.txId(), commitTs, Set.of("a", "b"));
        var restartedLedger = new MysqlPublicationLedger(dataSource);
        restartedLedger.initialize();
        restartedLedger.decideCommitted(started.txId(), commitTs, Set.of("a", "b"));
        assertEquals(commitTs, ledger.advancePublished(16));
    }

    /** 元数据已更新但账本写入失败时，两处状态必须在同一控制事务中回滚。 */
    @Test
    void failedLedgerUpdateMustRollBackCommittedMetadata() throws Exception {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    alter table mvcc_publication_entry_v1
                    add constraint reject_committed_entry check (status <> 'COMMITTED')
                    """);
        }
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, () -> ledger.decideCommitted(started.txId(), commitTs,
                        Set.of("a"))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(100, ledger.advancePublished(16));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_entry_v1 drop check reject_committed_entry");
        }
        ledger.decideCommitted(started.txId(), commitTs, Set.of("a"));
        assertEquals(commitTs, ledger.advancePublished(16));
    }

    /** 意向事务的提交时间戳、中心决议和账本终态在一次控制事务中原子落盘。 */
    @Test
    void preparedIntentCommitMustAtomicallyPublishWithoutEarlierPendingSlot() {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a", "b"));
        var published = ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(), Set.of("a", "b"));
        long commitTs = published.commitTs();
        assertTrue(published.publishedImmediately());
        assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.COMMITTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        assertEquals(commitTs, ledger.currentPublished());
        assertEquals(commitTs, ledger.advancePublished(16));
        assertEquals(commitTs, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        assertEquals(commitTs, ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(),
                Set.of("a", "b")).commitTs());
    }

    /** 批量提交一次发布全部决议，调用顺序和时间戳顺序可以不同。 */
    @Test
    void batchIntentCommitMustPublishWholeAtomicGroup() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var third = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(first, second, third)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        var results = ledger.commitPreparedIntentBatch(List.of(
                new PublicationIntentCommit(third.txId(), third.deadlineMillis(), Set.of("a")),
                new PublicationIntentCommit(first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationIntentCommit(second.txId(), second.deadlineMillis(), Set.of("a"))));
        assertEquals(103, results.get(0).commitTs());
        assertEquals(101, results.get(1).commitTs());
        assertEquals(102, results.get(2).commitTs());
        assertTrue(results.stream().allMatch(result -> result.publishedImmediately()));
        assertEquals(103, ledger.currentPublished());
        assertEquals(103, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        for (var started : List.of(first, second, third)) {
            assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
        }
    }

    /** 早期 PENDING 槽存在时，批次全部持久化但水位必须停在该槽之前。 */
    @Test
    void batchIntentCommitMustWaitForEarlierPendingSlot() {
        var pending = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(pending, first, second)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        long low = ledger.reserveCommit(pending.txId(), pending.deadlineMillis());
        var results = ledger.commitPreparedIntentBatch(List.of(
                new PublicationIntentCommit(first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationIntentCommit(second.txId(), second.deadlineMillis(), Set.of("a"))));
        assertEquals(low + 1, results.get(0).commitTs());
        assertEquals(low + 2, results.get(1).commitTs());
        assertTrue(results.stream().noneMatch(result -> result.publishedImmediately()));
        assertEquals(100, ledger.currentPublished());
        ledger.decideAbortedAfterFencing(pending.txId(), low);
        assertEquals(low + 2, ledger.advancePublished(16));
    }

    /** 批次中一笔参与者信息错误时，其余事务和时间戳分配也必须整体回滚。 */
    @Test
    void invalidBatchIntentMustRollBackEveryDecisionAndTimestamp() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.commitPreparedIntentBatch(List.of(
                        new PublicationIntentCommit(first.txId(), first.deadlineMillis(), Set.of("a")),
                        new PublicationIntentCommit(second.txId(), second.deadlineMillis(), Set.of("wrong")))))
                        .errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        assertTrue(ledger.findByTxId(second.txId()).isEmpty());
        assertEquals(101, ledger.commitPreparedIntents(first.txId(), first.deadlineMillis(),
                Set.of("a")).commitTs());
    }

    /** 批量元数据更新被数据库拒绝时，时间槽与已插入的账本决议必须一起回滚。 */
    @Test
    void failedBatchMetadataUpdateMustRollBackLedgerAndTimestamp() throws Exception {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("a"));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_txn add constraint mvcc_test_batch_update_reject "
                    + "check (status <> 'COMMITTED')");
        }
        try {
            assertThrows(MvccException.class, () -> ledger.commitPreparedIntentBatch(List.of(
                    new PublicationIntentCommit(first.txId(), first.deadlineMillis(), Set.of("a")),
                    new PublicationIntentCommit(second.txId(), second.deadlineMillis(), Set.of("a")))));
            assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
            assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
            assertTrue(ledger.findByTxId(first.txId()).isEmpty());
            assertTrue(ledger.findByTxId(second.txId()).isEmpty());
            assertEquals(100, ledger.currentPublished());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn drop check mvcc_test_batch_update_reject");
            }
        }
        assertEquals(101, ledger.commitPreparedIntents(first.txId(), first.deadlineMillis(), Set.of("a")).commitTs());
    }

    /** 早期本地 PENDING 未决时，后来意向提交虽已持久化也不能跨槽发布。 */
    @Test
    void atomicIntentCommitMustWaitBehindEarlierPendingReservation() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        var later = ledger.commitPreparedIntents(second.txId(), second.deadlineMillis(), Set.of("b"));
        assertEquals(low + 1, later.commitTs());
        assertFalse(later.publishedImmediately());
        assertEquals(100, ledger.currentPublished());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        assertEquals(later.commitTs(), ledger.advancePublished(16));
        ledger.awaitPublished(second.txId(), later.commitTs(), 1000);
    }

    /** 本地已提交的较晚预留不能跨过较早 PENDING，补决议仍保持 RR 可见性顺序。 */
    @Test
    void localDirectCommitMustNotPublishAcrossEarlierPendingSlot() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        long high = ledger.reserveLocalCommit(second.txId(), second.deadlineMillis(), "b");
        var localProof = new ParticipantRecord(second.txId(), second.startTs(), TxStatus.COMMITTED, high);
        assertFalse(ledger.completeLocalCommitAfterFencing(localProof, high, "b"));
        assertEquals(100, ledger.currentPublished());
        metadata.markAborted(first.txId());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        assertEquals(high, ledger.advancePublished(16));
    }

    /** 账本插入失败后 TSO 上界与中心元数据必须一同回滚，不产生缺号。 */
    @Test
    void failedAtomicIntentCommitMustNotLeaveTimestampGap() throws Exception {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    alter table mvcc_publication_entry_v1
                    add constraint reject_atomic_commit check (status <> 'COMMITTED')
                    """);
        }
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, () -> ledger.commitPreparedIntents(started.txId(),
                        started.deadlineMillis(), Set.of("a"))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        assertTrue(ledger.findByTxId(started.txId()).isEmpty());
        assertEquals(100, ledger.currentPublished());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_entry_v1 drop check reject_atomic_commit");
        }
        assertEquals(101, ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(),
                Set.of("a")).commitTs());
    }

    /** 旧协议还有活动事务时不允许把新水位初始化到旧 TSO 上界。 */
    @Test
    void migrationMustRejectUndrainedLegacyTransaction() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        metadata.create(50, 50, IsolationLevel.REPEATABLE_READ);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
    }

    /** 旧协议终态未完成物化时拒绝迁移，完成标记后才允许建立新时间线。 */
    @Test
    void migrationMustWaitForLegacyTerminalFinalization() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        metadata.createCommitted(50, 50, IsolationLevel.REPEATABLE_READ, Set.of("a"), 60);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
        metadata.markFinalized(50);
        ledger.initialize();
        assertEquals(100, ledger.currentPublished());
    }

    /** 账本激活后数据库外键直接拒绝旧快照租约，重启初始化也不能重新放行。 */
    @Test
    void activatedEpochMustFenceLegacySnapshotInserts() throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement oldBegin = connection.prepareStatement("""
                insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at) values (999, 999, 9999999999999)
                """)) {
            SQLException blocked = assertThrows(SQLException.class, oldBegin::executeUpdate);
            assertEquals(1452, blocked.getErrorCode());
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement oldProcedure =
                     connection.prepareStatement("call mvcc_begin_v2('READ_COMMITTED', 60000, false, 1)")) {
            SQLException blocked = assertThrows(SQLException.class, oldProcedure::execute);
            assertEquals(1452, blocked.getErrorCode());
        }
        new MysqlPublicationLedger(dataSource).initialize();
        assertEquals(100, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
    }

    /** 模式停留在 ACTIVATING 时，新账本 begin 例程必须失败关闭。 */
    @Test
    void ledgerBeginMustRejectIncompleteProtocolActivation() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1 set protocol_mode = 'ACTIVATING' where singleton_id = 1
                    """);
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.begin(IsolationLevel.READ_COMMITTED,
                        60_000, false)).errorCode());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1 set protocol_mode = 'LEDGER' where singleton_id = 1
                    """);
        }
        assertEquals(100, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
    }

    /** 切换进程在 ACTIVATING 后崩溃时，新实例可完成状态建立并保持旧写者栅栏。 */
    @Test
    void interruptedActivationMustResumeWithoutReopeningLegacyLease() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1 set protocol_mode = 'ACTIVATING' where singleton_id = 1
                    """);
        }
        var restarted = new MysqlPublicationLedger(dataSource);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> restarted.begin(IsolationLevel.READ_COMMITTED,
                        60_000, false)).errorCode());
        restarted.initialize();
        assertEquals(100, restarted.currentPublished());
        try (Connection connection = dataSource.getConnection(); PreparedStatement oldBegin = connection.prepareStatement("""
                insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at) values (999, 999, 9999999999999)
                """)) {
            assertEquals(1452, assertThrows(SQLException.class, oldBegin::executeUpdate).getErrorCode());
        }
    }

    /** 在途旧租约的外键共享锁阻止切换越过它，排空后才能再次激活。 */
    @Test
    void activationMustWaitForInFlightLegacyLeaseAndRejectUndrainedState() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        try (Connection oldWriter = dataSource.getConnection();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            oldWriter.setAutoCommit(false);
            try (Statement insert = oldWriter.createStatement()) {
                insert.executeUpdate("""
                        insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at)
                        values (999, 999, 9999999999999)
                        """);
            }
            var activating = executor.submit(ledger::initialize);
            assertThrows(TimeoutException.class, () -> activating.get(150, TimeUnit.MILLISECONDS));
            oldWriter.commit();
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> activating.get(5, TimeUnit.SECONDS));
        }
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_snapshot_lease where tx_id = 999");
        }
        ledger.initialize();
        assertEquals(100, ledger.currentPublished());
    }

    /** 状态行丢失但仍有账本或租约时不能按旧 TSO 上界重建水位。 */
    @Test
    void missingPublicationStateWithExistingLeaseMustFailClosed() throws Exception {
        ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
    }

    /** 旧元数据超出已持久化 TSO 上界时不能把新水位初始化到错误位置。 */
    @Test
    void migrationMustRejectTimestampUpperBelowCommittedMetadata() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        metadata.createCommitted(101, 101, IsolationLevel.REPEATABLE_READ, Set.of("a"), 102);
        metadata.markFinalized(101);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet mode = statement.executeQuery("""
                     select protocol_mode from mvcc_protocol_epoch_v1 where singleton_id = 1
                     """)) {
            assertTrue(mode.next());
            assertEquals("LEGACY", mode.getString(1));
        }
    }

    /** 旧状态表归档后旧进程无法再分配时间槽，新分段从原发布上界连续开始。 */
    @Test
    void segmentedActivationMustPhysicallyFenceOldWriters() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        segments.assertActivated();
        assertEquals(100, segments.currentPublished());
        assertEquals(101, segments.reserveSegment("owner-a", "reservation-a", 4, 60_000).firstTs());
        assertThrows(SQLException.class, () -> {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.executeUpdate("update mvcc_publication_state_v1 set next_commit_ts = 999 where singleton_id = 1");
            }
        });
        assertThrows(MvccException.class, () -> ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false));
        assertThrows(MvccException.class, ledger::initialize);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("""
                     select count(*) from information_schema.tables
                     where table_schema = database() and table_name = 'mvcc_publication_state_v1'
                     """)) {
            assertTrue(rows.next());
            assertEquals(0, rows.getLong(1));
        }
        segments.assertActivated();
        segments.activateFromLedger();
    }

    /** 只有模式行而缺少旧表归档物理栅栏时，新业务入口必须失败关闭。 */
    @Test
    void segmentedModeRowAloneMustNotActivateBusinessEntry() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_protocol_epoch_v1 "
                    + "set protocol_mode = 'SEGMENT' where singleton_id = 1");
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> new MysqlSegmentPublicationLedger(dataSource).assertActivated()).errorCode());
    }

    /** 旧读者或未物化事务未排空时，切换在归档表之前失败并保留旧协议可用。 */
    @Test
    void segmentedActivationMustRejectLiveOldSnapshotBeforeFencing() {
        var snapshot = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, segments::activateFromLedger).errorCode());
        assertEquals(100, ledger.currentPublished());
        ledger.releaseSnapshot(snapshot.txId());
        segments.activateFromLedger();
        segments.assertActivated();
    }

    /** 旧写元数据尚未 finalized 时，即使快照租约已释放也不得归档旧时间线。 */
    @Test
    void segmentedActivationMustRejectUnfinalizedOldMetadata() {
        var writer = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        ledger.releaseSnapshot(writer.txId());
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, segments::activateFromLedger).errorCode());
        assertEquals(100, ledger.currentPublished());
        metadata.markAborted(writer.txId());
        metadata.markFinalized(writer.txId());
        segments.activateFromLedger();
        segments.assertActivated();
    }

    /** 物理归档后崩溃且旧初始化器重建空表时，新实例仍可清理残影并续建。 */
    @Test
    void segmentedActivationMustResumeAfterArchiveCrash() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("rename table mvcc_publication_state_v1 "
                    + "to mvcc_publication_state_v1_legacy_archived");
        }
        assertThrows(MvccException.class, () -> new MysqlPublicationLedger(dataSource).initialize());
        var restarted = new MysqlSegmentPublicationLedger(dataSource);
        restarted.activateFromLedger();
        restarted.assertActivated();
        assertEquals(100, restarted.currentPublished());
        assertEquals(101, restarted.reserveSegment("owner-b", "reservation-b", 2, 60_000).firstTs());
    }

    /** 已归档且进入 SEGMENT_PREPARE 后崩溃，重启只续建新表与最终模式。 */
    @Test
    void segmentedActivationMustResumeAfterPreparePhaseCrash() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("rename table mvcc_publication_state_v1 "
                    + "to mvcc_publication_state_v1_legacy_archived");
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1
                    set protocol_mode = 'SEGMENT_PREPARE', generation = generation + 1
                    where singleton_id = 1 and protocol_mode = 'LEDGER'
                    """);
        }
        var restarted = new MysqlSegmentPublicationLedger(dataSource);
        restarted.initializeIsolated(100);
        restarted.activateFromLedger();
        restarted.assertActivated();
        assertEquals(101, restarted.reserveSegment("owner-c", "reservation-c", 3, 60_000).firstTs());
    }

    /** 旧安全点必须随发布上界迁移，不能让新快照越过已经回收的版本边界。 */
    @Test
    void segmentedActivationMustCarryOldSafePoint() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_publication_state_v1 set safe_point = 80 where singleton_id = 1");
        }
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("select safe_point from mvcc_segment_publication_state_v1")) {
            assertTrue(row.next());
            assertEquals(80, row.getLong(1));
        }
    }

    /** 真实意向尚未物化时，新 RC 可解析已发布决议，旧 RR 仍固定原快照。 */
    @Test
    void segmentedDecisionMustPreserveRcAndRrBeforeMaterialization() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var repeatable = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:visible", new byte[]{7})));
        var first = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        var renewed = segments.renewSegment(first, 60_000);
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommitted(first, 101,
                        writer.txId(), writer.deadlineMillis(), Set.of("a"))).errorCode());
        assertEquals(101, segments.decideCommitted(renewed, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
        assertEquals(101, segments.advancePublished(10));
        var committed = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        assertEquals(101, committed.startTs());
        assertTrue(shard.get(new ReadContext(repeatable.txId(), repeatable.startTs(),
                IsolationLevel.REPEATABLE_READ), "segment:visible").isEmpty());
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(committed.txId(), committed.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:visible").orElseThrow().value());
        assertEquals(3, segments.sealOwnedSegment(renewed));
        assertEquals(104, segments.advancePublished(10));
        assertEquals(101, segments.decideCommitted(first, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
    }

    /** 参与者身份错误不能把槽或中心元数据单独改判为 COMMITTED。 */
    @Test
    void invalidSegmentedDecisionMustRollBackBothAuthorities() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                        writer.txId(), writer.deadlineMillis(), Set.of("wrong"))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("""
                     select status, tx_id from mvcc_publication_segment_slot_v1 where commit_ts = 101
                     """)) {
            assertTrue(row.next());
            assertEquals("PENDING", row.getString(1));
            assertEquals(null, row.getObject(2));
        }
        assertEquals(100, segments.currentPublished());
        assertEquals(101, segments.decideCommitted(owner, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
    }

    /** 中心更新 SQL 失败时槽与事务记录一起回滚，重复原请求仍可安全决议。 */
    @Test
    void segmentedDecisionSqlFailureMustRollBackSlotAndMetadata() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_txn add constraint mvcc_test_reject_segment_commit "
                    + "check (status <> 'COMMITTED')");
        }
        try {
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                            writer.txId(), writer.deadlineMillis(), Set.of("a"))).errorCode());
            assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                 ResultSet row = statement.executeQuery("""
                         select status, tx_id from mvcc_publication_segment_slot_v1 where commit_ts = 101
                         """)) {
                assertTrue(row.next());
                assertEquals("PENDING", row.getString(1));
                assertEquals(null, row.getObject(2));
            }
            assertEquals(100, segments.currentPublished());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn drop check mvcc_test_reject_segment_commit");
            }
        }
        assertEquals(101, segments.decideCommitted(owner, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
        assertEquals(101, segments.advancePublished(10));
    }

    /** 同一分段的乱序请求在一笔事务中决议，发布仅越过连续已决槽。 */
    @Test
    void segmentedBatchDecisionMustPublishContiguousAtomicGroup() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var first = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var third = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var tx : List.of(first, second, third)) {
            metadata.registerParticipants(tx.txId(), Set.of("a"));
        }
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        var batch = List.of(
                new PublicationSegmentCommit(owner, 103, third.txId(), third.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(owner, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(owner, 102, second.txId(), second.deadlineMillis(), Set.of("a")));
        segments.decideCommittedBatch(batch);
        assertEquals(103, segments.advancePublished(10));
        assertEquals(103, segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        for (var tx : List.of(first, second, third)) {
            assertEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
        }
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> segments.decideCommittedBatch(batch)).errorCode());
        assertEquals(101, segments.decideCommitted(owner, 101,
                first.txId(), first.deadlineMillis(), Set.of("a")));
    }

    /** 一个参与者信息错误时，批次内其他元数据和槽也不得落部分提交。 */
    @Test
    void invalidSegmentedBatchDecisionMustRollBackAllSlots() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var first = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> segments.decideCommittedBatch(List.of(
                        new PublicationSegmentCommit(owner, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                        new PublicationSegmentCommit(owner, 102, second.txId(), second.deadlineMillis(),
                                Set.of("wrong"))))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
        assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery("""
                     select count(*) from mvcc_publication_segment_slot_v1
                     where commit_ts in (101, 102) and status = 'PENDING' and tx_id is null
                     """)) {
            assertTrue(row.next());
            assertEquals(2, row.getLong(1));
        }
        segments.decideCommittedBatch(List.of(
                new PublicationSegmentCommit(owner, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(owner, 102, second.txId(), second.deadlineMillis(), Set.of("b"))));
        assertEquals(102, segments.advancePublished(10));
    }

    /** 批量槽更新被数据库拒绝时，之前的中心批量更新也必须整笔回滚。 */
    @Test
    void segmentedBatchSlotSqlFailureMustRollBackMetadata() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var first = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        var batch = List.of(
                new PublicationSegmentCommit(owner, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(owner, 102, second.txId(), second.deadlineMillis(), Set.of("a")));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_segment_slot_v1 "
                    + "add constraint mvcc_test_reject_segment_batch check (status <> 'COMMITTED')");
        }
        try {
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () -> segments.decideCommittedBatch(batch)).errorCode());
            assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
            assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
            assertEquals(100, segments.currentPublished());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_publication_segment_slot_v1 "
                        + "drop check mvcc_test_reject_segment_batch");
            }
        }
        var renewed = segments.renewSegment(owner, 60_000);
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommittedBatch(batch)).errorCode());
        segments.decideCommittedBatch(List.of(
                new PublicationSegmentCommit(renewed, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(renewed, 102, second.txId(), second.deadlineMillis(), Set.of("a"))));
        assertEquals(102, segments.advancePublished(10));
    }

    /** 物理单分片已提交但中心尚未补决议时保持不可见，补决议后 RC/RR 各守快照。 */
    @Test
    void segmentedLocalDirectCommitMustPublishOnlyAfterCenterCompletion() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var oldReader = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        assertEquals(101, segments.claimLocalPending(owner, 101,
                writer.txId(), writer.deadlineMillis(), "a"));
        assertEquals(TxStatus.PREPARING, metadata.get(writer.txId()).orElseThrow().status());
        shard.directCommit(writer.txId(), writer.startTs(), 101,
                List.of(Mutation.put("segment:direct", new byte[]{4})));
        var local = shard.participantRecord(writer.txId()).orElseThrow();
        assertEquals(TxStatus.COMMITTED, local.status());
        var before = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        assertEquals(100, before.startTs());
        assertTrue(shard.get(new ReadContext(before.txId(), before.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:direct").isEmpty());
        assertTrue(segments.completeLocalCommitAfterFencing(local, 101, "a"));
        assertEquals(101, segments.advancePublished(10));
        var after = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        assertArrayEquals(new byte[]{4}, shard.get(new ReadContext(after.txId(), after.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:direct").orElseThrow().value());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "segment:direct").isEmpty());
        segments.sealOwnedSegment(owner);
        assertEquals(101, segments.claimLocalPending(owner, 101,
                writer.txId(), writer.deadlineMillis(), "a"));
        assertFalse(segments.completeLocalCommitAfterFencing(local, 101, "a"));
    }

    /** 本地回滚证明释放已认领槽，中心 ABORTED 后迟到物理提交不能复活版本。 */
    @Test
    void segmentedLocalAbortMustReleasePendingSlotAfterFencing() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.claimLocalPending(owner, 101, writer.txId(), writer.deadlineMillis(), "a");
        shard.abort(writer.txId());
        var local = shard.participantRecord(writer.txId()).orElseThrow();
        assertEquals(TxStatus.ABORTED, local.status());
        assertTrue(segments.completeLocalAbortAfterFencing(local, 101, "a"));
        assertEquals(101, segments.advancePublished(10));
        assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertFalse(segments.completeLocalAbortAfterFencing(local, 101, "a"));
        assertThrows(MvccException.class, () -> shard.directCommit(writer.txId(), writer.startTs(), 101,
                List.of(Mutation.put("segment:late", new byte[]{1}))));
    }

    /** owner 失联后已提交本地终态优先于超时中止，恢复同一个分段时间戳。 */
    @Test
    void segmentedLocalCommittedPendingMustRecoverAfterOwnerFencing() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.claimLocalPending(owner, 101, writer.txId(), writer.deadlineMillis(), "a");
        shard.directCommit(writer.txId(), writer.startTs(), 101,
                List.of(Mutation.put("segment:local-recover", new byte[]{3})));
        try (Connection connection = dataSource.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            expire.setLong(1, owner.firstTs());
            expire.executeUpdate();
        }
        assertEquals(3, segments.sealExpiredSegment(owner.firstTs()));
        assertEquals(PublicationEntryStatus.PENDING,
                segments.findByTxId(writer.txId()).orElseThrow().status());
        assertEquals(100, segments.advancePublished(10));
        var restartedMetadata = new MysqlTransactionMetadataStore(dataSource);
        var restartedShard = new MysqlStorageAdapter("a", dataSource, restartedMetadata);
        var restartedLedger = new MysqlSegmentPublicationLedger(dataSource);
        var recovery = new SegmentPublicationRecoveryManager(restartedLedger, restartedMetadata,
                Map.of("a", restartedShard));
        var runner = new SegmentPublicationRecoveryRunner(restartedLedger, recovery, restartedMetadata);
        assertTrue(runner.recoverOnce(10) > 0);
        assertEquals(TxStatus.COMMITTED, restartedMetadata.get(writer.txId()).orElseThrow().status());
        assertEquals(104, restartedLedger.currentPublished());
        assertTrue(restartedLedger.snapshotExpiry(writer.txId()).isEmpty());
        assertArrayEquals(new byte[]{3}, restartedShard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "segment:local-recover").orElseThrow().value());
        assertEquals(0, runner.transactionStats().failureCount());
    }

    /** 本地 ABORTED 证明补齐中心中止，已认领槽不能被 owner 尾槽封存误判。 */
    @Test
    void segmentedLocalAbortedPendingMustRecoverAfterOwnerFencing() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.claimLocalPending(owner, 101, writer.txId(), writer.deadlineMillis(), "a");
        shard.abort(writer.txId());
        try (Connection connection = dataSource.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            expire.setLong(1, owner.firstTs());
            expire.executeUpdate();
        }
        assertEquals(3, segments.sealExpiredSegment(owner.firstTs()));
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        var runner = new SegmentPublicationRecoveryRunner(segments, recovery, metadata);
        assertTrue(runner.recoverOnce(10) > 0);
        assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.ABORTED,
                segments.findByTxId(writer.txId()).orElseThrow().status());
        assertEquals(104, segments.currentPublished());
    }

    /** owner 与事务租约都有效时，后台不得接管刚认领的本地候选。 */
    @Test
    void segmentedRecoveryMustLeaveLiveLocalCandidateAlone() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.claimLocalPending(owner, 101, writer.txId(), writer.deadlineMillis(), "a");
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        var runner = new SegmentPublicationRecoveryRunner(segments, recovery, metadata);
        assertEquals(0, runner.recoverOnce(10));
        assertEquals(TxStatus.PREPARING, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.PENDING,
                segments.findByTxId(writer.txId()).orElseThrow().status());
        assertEquals(100, segments.currentPublished());
    }

    /** 到期 owner 不能再认领 PENDING 槽，接管封存后仍保持中心事务未提交。 */
    @Test
    void expiredSegmentOwnerMustNotCommitUnclaimedSlot() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            expire.setLong(1, owner.firstTs());
            expire.executeUpdate();
        }
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                        writer.txId(), writer.deadlineMillis(), Set.of("a"))).errorCode());
        assertEquals(4, segments.sealExpiredSegment(owner.firstTs()));
        assertEquals(104, segments.advancePublished(10));
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
    }

    /** 前一个事务中止后，较晚已提交槽才可发布；RC/RR 仍按各自快照读取。 */
    @Test
    void segmentedAbortMustReleaseLaterCommittedPublication() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var repeatable = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var aborted = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var committed = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(aborted.txId(), Set.of("a"));
        metadata.registerParticipants(committed.txId(), Set.of("a"));
        shard.prewrite(aborted.txId(), aborted.startTs(),
                List.of(Mutation.put("segment:aborted", new byte[]{1})));
        shard.prewrite(committed.txId(), committed.startTs(),
                List.of(Mutation.put("segment:later", new byte[]{2})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.decideCommitted(owner, 102, committed.txId(), committed.deadlineMillis(), Set.of("a"));
        assertEquals(100, segments.advancePublished(10));
        shard.abort(aborted.txId());
        assertTrue(segments.abortReservedSlot(owner, 101, aborted.txId(), aborted.deadlineMillis()));
        assertEquals(102, segments.advancePublished(10));
        assertEquals(TxStatus.ABORTED, metadata.get(aborted.txId()).orElseThrow().status());
        var reader = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        assertArrayEquals(new byte[]{2}, shard.get(new ReadContext(reader.txId(), reader.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:later").orElseThrow().value());
        assertTrue(shard.get(new ReadContext(repeatable.txId(), repeatable.startTs(),
                IsolationLevel.REPEATABLE_READ), "segment:later").isEmpty());
        assertTrue(shard.get(new ReadContext(reader.txId(), reader.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:aborted").isEmpty());
        assertEquals(2, segments.sealOwnedSegment(owner));
        assertFalse(segments.abortReservedSlot(owner, 101, aborted.txId(), aborted.deadlineMillis()));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                        aborted.txId(), aborted.deadlineMillis(), Set.of("a"))).errorCode());
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> segments.abortReservedSlot(owner, 102,
                        committed.txId(), committed.deadlineMillis())).errorCode());
    }

    /** 中止 SQL 被数据库拒绝时，中心状态与分段槽均保持未决，重试仍可落终态。 */
    @Test
    void segmentedAbortSqlFailureMustNotLeavePartialTerminalState() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_txn add constraint mvcc_test_reject_segment_abort "
                    + "check (status <> 'ABORTED')");
        }
        try {
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () -> segments.abortReservedSlot(owner, 101,
                            writer.txId(), writer.deadlineMillis())).errorCode());
            assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
            assertEquals(100, segments.currentPublished());
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                 ResultSet row = statement.executeQuery("""
                         select status, tx_id from mvcc_publication_segment_slot_v1 where commit_ts = 101
                         """)) {
                assertTrue(row.next());
                assertEquals("PENDING", row.getString(1));
                assertEquals(null, row.getObject(2));
            }
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn drop check mvcc_test_reject_segment_abort");
            }
        }
        assertTrue(segments.abortReservedSlot(owner, 101, writer.txId(), writer.deadlineMillis()));
        assertEquals(101, segments.advancePublished(10));
    }

    /** 中心 COMMITTED 槽已持久化但分片未物化时，另一恢复实例补相同时间戳。 */
    @Test
    void committedSegmentedIntentMustMaterializeAfterRecoveryRestart() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:recovered", new byte[]{9})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.decideCommitted(owner, 101, writer.txId(), writer.deadlineMillis(), Set.of("a"));
        var restartedMetadata = new MysqlTransactionMetadataStore(dataSource);
        var restartedLedger = new MysqlSegmentPublicationLedger(dataSource);
        var restartedShard = new MysqlStorageAdapter("a", dataSource, restartedMetadata);
        var recovery = new SegmentPublicationRecoveryManager(restartedLedger, restartedMetadata,
                Map.of("a", restartedShard));
        assertTrue(recovery.recover(writer.txId()));
        assertEquals(101, segments.currentPublished());
        assertEquals(TxStatus.COMMITTED, restartedShard.participantRecord(writer.txId()).orElseThrow().status());
        assertArrayEquals(new byte[]{9}, restartedShard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "segment:recovered").orElseThrow().value());
        assertTrue(segments.snapshotExpiry(writer.txId()).isEmpty());
        assertTrue(recovery.recover(writer.txId()));
    }

    /** 持久中心凭据须绑定开始水位、参与者与控制权威，不能代替其他事务物化。 */
    @Test
    void segmentedCommittedProofMustFenceWrongIdentityAndAuthority() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var mutation = Mutation.put("segment:proof:owned", new byte[]{7});
        shard.prewrite(writer.txId(), writer.startTs(), List.of(mutation));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        var proof = segments.decideCommittedWithProof(owner, 101, writer.txId(),
                writer.startTs(), writer.deadlineMillis(), Set.of("a"));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> segments.decideCommittedWithProof(owner,
                        101, writer.txId(), writer.startTs() + 1, writer.deadlineMillis(), Set.of("a"))).errorCode());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> new MysqlStorageAdapter("b", dataSource, metadata)
                        .commitDecided(proof, List.of(mutation), true)).errorCode());
        try (HikariDataSource anotherPool = (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())) {
            var otherAuthority = new MysqlTransactionMetadataStore(anotherPool);
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> new MysqlStorageAdapter("a", dataSource,
                            otherAuthority).commitDecided(proof, List.of(mutation), true)).errorCode());
        }
        shard.commitDecided(proof, List.of(mutation), true);
        assertEquals(TxStatus.COMMITTED, shard.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(101, segments.advancePublished(10));
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "segment:proof:owned").orElseThrow().value());
    }

    /** owner 崩溃留下无事务编号的槽时，租约到期后仍能从中心元数据发现并清理意向。 */
    @Test
    void expiredUndecidedSegmentedIntentMustAbortAfterOwnerFencing() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:orphan", new byte[]{8})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 1 where tx_id = ?
                """)) {
            age.setLong(1, writer.txId());
            age.executeUpdate();
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            age.setLong(1, owner.firstTs());
            age.executeUpdate();
        }
        assertEquals(4, segments.sealExpiredSegment(owner.firstTs()));
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        assertTrue(recovery.recover(writer.txId()));
        assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, shard.participantRecord(writer.txId()).orElseThrow().status());
        assertTrue(segments.findByTxId(writer.txId()).isEmpty());
        assertEquals(104, segments.advancePublished(10));
    }

    /** 一轮后台扫描同时封存到期 owner、清理无槽意向并释放连续水位。 */
    @Test
    void segmentedRecoveryRunnerMustSealAndAbortExpiredOrphan() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:runner-orphan", new byte[]{6})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 1 where tx_id = ?
                """)) {
            expire.setLong(1, writer.txId());
            expire.executeUpdate();
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            expire.setLong(1, owner.firstTs());
            expire.executeUpdate();
        }
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        var runner = new SegmentPublicationRecoveryRunner(segments, recovery, metadata);
        assertTrue(runner.recoverOnce(10) > 0);
        assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, shard.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(104, segments.currentPublished());
        assertEquals(0, runner.transactionStats().failureCount());
        assertEquals(0, runner.scanStats().failureCount());
    }

    /** 中心已决议但未物化的槽由另一后台实例发现并提交同一个版本时间戳。 */
    @Test
    void segmentedRecoveryRunnerMustMaterializeUnfinalizedCommit() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:runner-commit", new byte[]{5})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.decideCommitted(owner, 101, writer.txId(), writer.deadlineMillis(), Set.of("a"));
        var restartedMetadata = new MysqlTransactionMetadataStore(dataSource);
        var restartedShard = new MysqlStorageAdapter("a", dataSource, restartedMetadata);
        var restartedLedger = new MysqlSegmentPublicationLedger(dataSource);
        var recovery = new SegmentPublicationRecoveryManager(restartedLedger, restartedMetadata,
                Map.of("a", restartedShard));
        var runner = new SegmentPublicationRecoveryRunner(restartedLedger, recovery, restartedMetadata);
        assertTrue(runner.recoverOnce(10) > 0);
        assertEquals(TxStatus.COMMITTED, restartedShard.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(101, restartedLedger.currentPublished());
        assertArrayEquals(new byte[]{5}, restartedShard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "segment:runner-commit").orElseThrow().value());
        assertEquals(0, runner.transactionStats().failureCount());
    }

    /** 有效事务租约仍由前台持有时，后台不得提前中止意向或封存 owner。 */
    @Test
    void segmentedRecoveryRunnerMustLeaveLiveWriterAlone() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:live", new byte[]{4})));
        segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        var runner = new SegmentPublicationRecoveryRunner(segments, recovery, metadata);
        assertEquals(0, runner.recoverOnce(10));
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ACTIVE, shard.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(100, segments.currentPublished());
    }

    /** 中心元数据称已提交但没有分段槽时，恢复只报告未知结果而不盲目中止。 */
    @Test
    void segmentedRecoveryMustReportCommittedMetadataWithoutSlot() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        try (Connection connection = dataSource.getConnection(); PreparedStatement corrupt = connection.prepareStatement("""
                update mvcc_txn set status = 'COMMITTED', commit_ts = 101 where tx_id = ?
                """)) {
            corrupt.setLong(1, writer.txId());
            corrupt.executeUpdate();
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 1 where tx_id = ?
                """)) {
            expire.setLong(1, writer.txId());
            expire.executeUpdate();
        }
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of());
        var runner = new SegmentPublicationRecoveryRunner(segments, recovery, metadata);
        assertEquals(0, runner.recoverOnce(10));
        assertEquals(1, runner.transactionStats().failureCount());
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                ((MvccException) runner.lastTransactionFailure()).errorCode());
        assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(100, segments.currentPublished());
    }

    /** 独立后台连接池在前台退出后仍周期补物化并推进持久水位。 */
    @Test
    void segmentedSchedulerMustRecoverCommittedIntentWithIndependentPools() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:scheduled", new byte[]{3})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.decideCommitted(owner, 101, writer.txId(), writer.deadlineMillis(), Set.of("a"));

        try (HikariDataSource backgroundControl = (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
             HikariDataSource backgroundShard = (HikariDataSource) MysqlDataSourceFactory.create(
                     MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())) {
            var backgroundMetadata = new MysqlTransactionMetadataStore(backgroundControl);
            var backgroundLedger = new MysqlSegmentPublicationLedger(backgroundControl);
            var participant = new MysqlStorageAdapter("a", backgroundShard, backgroundMetadata);
            participant.initialize();
            var recovery = new SegmentPublicationRecoveryManager(backgroundLedger, backgroundMetadata,
                    Map.of("a", participant));
            var runner = new SegmentPublicationRecoveryRunner(backgroundLedger, recovery, backgroundMetadata);
            var gc = new SegmentPublicationGcManager(backgroundLedger, backgroundMetadata,
                    Map.of("a", participant));
            try (var scheduler = new SegmentPublicationMaintenanceScheduler(runner, backgroundLedger,
                    gc, 10, 256, 10, 20, 10, 50)) {
                scheduler.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (System.nanoTime() < deadline
                        && (shard.participantRecord(writer.txId()).orElseThrow().status() != TxStatus.COMMITTED
                        || segments.currentPublished() < 101
                        || scheduler.recoveryStats().runCount() == 0
                        || scheduler.publicationStats().runCount() == 0
                        || scheduler.gcStats().runCount() == 0)) {
                    Thread.sleep(20);
                }
                assertEquals(TxStatus.COMMITTED, shard.participantRecord(writer.txId()).orElseThrow().status());
                assertEquals(101, segments.currentPublished());
                assertTrue(scheduler.recoveryStats().runCount() > 0);
                assertTrue(scheduler.publicationStats().runCount() > 0);
                assertTrue(scheduler.gcStats().runCount() > 0);
                assertEquals(0, scheduler.recoveryStats().failureCount());
                assertEquals(0, scheduler.publicationStats().failureCount());
                assertEquals(0, scheduler.gcStats().failureCount());
            }
        }
    }

    /** GC 等待旧 RR 读者并按槽、owner 顺序回收，不能越过持久安全点。 */
    @Test
    void segmentedGcMustRespectSnapshotAndTerminalPurgeOrder() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var reader = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var first = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        assertEquals(4, segments.sealOwnedSegment(first));
        assertEquals(104, segments.advancePublished(10));
        assertEquals(99, segments.advanceSafePoint());
        segments.releaseSnapshot(reader.txId());
        assertEquals(104, segments.advanceSafePoint());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> segments.purgeTerminalSlots(105, Long.MAX_VALUE, 10)).errorCode());
        assertEquals(3, segments.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        assertEquals(0, segments.purgeSealedSegments(104, Long.MAX_VALUE, 10));
        var second = segments.reserveSegment("owner-b", "reservation-b", 1, 60_000);
        assertEquals(1, segments.sealOwnedSegment(second));
        assertEquals(105, segments.advancePublished(10));
        assertEquals(105, segments.advanceSafePoint());
        assertEquals(1, segments.purgeTerminalSlots(105, Long.MAX_VALUE, 10));
        assertEquals(1, segments.purgeSealedSegments(105, Long.MAX_VALUE, 10));
    }

    /** 后台 GC 在同一目标安全点完成分片、中心、槽和 owner 全阶段后才刷新目标。 */
    @Test
    void segmentedGcManagerMustWaitForReaderAndPurgeInOrder() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var reader = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var first = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.sealOwnedSegment(first);
        segments.advancePublished(10);
        try (Connection connection = dataSource.getConnection(); Statement age = connection.createStatement()) {
            age.executeUpdate("update mvcc_publication_segment_slot_v1 set expires_at = 1");
            age.executeUpdate("update mvcc_publication_segment_v1 set expires_at = 1");
        }
        var gc = new SegmentPublicationGcManager(segments, metadata, Map.of("a", shard),
                10, 0, 1_000, System::nanoTime);
        gc.gcOnce(10);
        assertEquals(99, gc.currentSafePoint());
        try (Connection connection = dataSource.getConnection(); Statement count = connection.createStatement();
             ResultSet row = count.executeQuery("select count(*) from mvcc_publication_segment_slot_v1")) {
            assertTrue(row.next());
            assertEquals(4, row.getLong(1));
        }
        segments.releaseSnapshot(reader.txId());
        gc.gcOnce(10);
        assertEquals(104, gc.currentSafePoint());
        try (Connection connection = dataSource.getConnection(); Statement count = connection.createStatement();
             ResultSet row = count.executeQuery("select count(*) from mvcc_publication_segment_slot_v1")) {
            assertTrue(row.next());
            assertEquals(1, row.getLong(1));
        }
        var second = segments.reserveSegment("owner-b", "reservation-b", 1, 60_000);
        segments.sealOwnedSegment(second);
        segments.advancePublished(10);
        gc.gcOnce(10);
        assertEquals(105, gc.currentSafePoint());
        try (Connection connection = dataSource.getConnection(); Statement count = connection.createStatement();
             ResultSet row = count.executeQuery("select count(*) from mvcc_publication_segment_v1")) {
            assertTrue(row.next());
            assertEquals(1, row.getLong(1));
        }
    }

    /** 已物化事务的槽先于元数据回收，并等待 finalized 保留期，避免恢复扫描遇到孤槽。 */
    @Test
    void segmentedGcMustRetainTerminalSlotUntilFinalizationRetention() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        shard.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("segment:gc-terminal", new byte[]{1})));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        segments.decideCommitted(owner, 101, writer.txId(), writer.deadlineMillis(), Set.of("a"));
        shard.commit(writer.txId(), 101);
        metadata.markFinalized(writer.txId());
        segments.releaseSnapshot(writer.txId());
        segments.sealOwnedSegment(owner);
        segments.advancePublished(10);
        assertEquals(104, segments.advanceSafePoint());
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_publication_segment_slot_v1 set expires_at = 1 where commit_ts = 101
                """)) {
            age.executeUpdate();
        }
        long cutoff = metadata.currentTimeMillis() - 1_000;
        assertEquals(0, segments.purgeTerminalSlots(104, cutoff, 10));
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_txn_finalized set finalized_at = 1 where tx_id = ?
                """)) {
            age.setLong(1, writer.txId());
            age.executeUpdate();
        }
        assertEquals(1, segments.purgeTerminalSlots(104, cutoff, 10));
        assertEquals(1, segments.purgeFinalizedMetadata(104, cutoff, 10));
        assertTrue(metadata.get(writer.txId()).isEmpty());
        assertTrue(segments.findUnfinalizedTerminal(0, 10).isEmpty());
    }

    /** begin 已拿共享栅栏但租约未落盘时，GC 必须等待并保留该快照。 */
    @Test
    void gcMustWaitForSharedBeginFenceBeforeAdvancingSafePoint() throws Exception {
        try (Connection reader = dataSource.getConnection();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            reader.setAutoCommit(false);
            try (PreparedStatement lock = reader.prepareStatement("""
                    select published_ts from mvcc_publication_state_v1
                    where singleton_id = 1 for share
                    """);
                 ResultSet result = lock.executeQuery()) {
                assertTrue(result.next());
                assertEquals(100, result.getLong(1));
            }
            var advancing = executor.submit(ledger::advanceSafePoint);
            assertThrows(TimeoutException.class, () -> advancing.get(150, TimeUnit.MILLISECONDS));
            try (PreparedStatement lease = reader.prepareStatement("""
                    insert into mvcc_snapshot_lease_v2(read_ts, expires_at)
                    values (100, cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + 60000)
                    """)) {
                lease.executeUpdate();
            }
            reader.commit();
            assertEquals(99, advancing.get(5, TimeUnit.SECONDS));
            assertEquals(100, ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false).startTs());
        }
    }

    /** 只读事务释放租约后安全点才允许前进到当前已发布水位。 */
    @Test
    void releasedReadOnlySnapshotNoLongerPinsSafePoint() {
        var reader = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        assertEquals(reader.deadlineMillis(), ledger.snapshotExpiry(reader.txId()).orElseThrow());
        assertEquals(99, ledger.advanceSafePoint());
        ledger.releaseSnapshot(reader.txId());
        ledger.releaseSnapshot(reader.txId());
        assertTrue(ledger.snapshotExpiry(reader.txId()).isEmpty());
        assertEquals(100, ledger.advanceSafePoint());
    }

    /** 协调器租约保护最老快照，只有当前代数可提高下界并释放保护。 */
    @Test
    void coordinatorLeaseMustFenceStaleGenerationAndProtectGcBoundary() {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        assertEquals(100, coordinator.readTs());
        assertEquals(99, ledger.advanceSafePoint());
        var writer = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        long commitTs = ledger.commitPreparedIntents(writer.txId(), writer.deadlineMillis(), Set.of("a")).commitTs();
        metadata.markFinalized(writer.txId());
        ledger.releaseSnapshot(writer.txId());
        assertEquals(99, ledger.advanceSafePoint());

        var restarted = new MysqlPublicationLedger(dataSource);
        var renewed = restarted.renewCoordinatorLease(coordinator.coordinatorId(), coordinator.generation(),
                commitTs, 60_000);
        assertEquals(coordinator.generation() + 1, renewed.generation());
        assertEquals(commitTs, renewed.readTs());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewCoordinatorLease(
                        coordinator.coordinatorId(), coordinator.generation(), commitTs, 60_000)).errorCode());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.releaseCoordinatorLease(
                        coordinator.coordinatorId(), coordinator.generation())).errorCode());
        assertEquals(100, ledger.advanceSafePoint());
        assertTrue(ledger.releaseCoordinatorLease(renewed.coordinatorId(), renewed.generation()));
        assertFalse(ledger.releaseCoordinatorLease(renewed.coordinatorId(), renewed.generation()));
        assertEquals(commitTs, ledger.advanceSafePoint());
    }

    /** 到期协调器租约被 GC 删除后，旧实例不能续租或继续保护快照。 */
    @Test
    void expiredCoordinatorLeaseMustBeFencedAfterGc() throws Exception {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_coordinator_snapshot_lease_v1 set expires_at = 0 where coordinator_id = ?
                """)) {
            age.setLong(1, coordinator.coordinatorId());
            age.executeUpdate();
        }
        assertEquals(1, ledger.expireCoordinatorLeases(10));
        assertTrue(ledger.coordinatorLease(coordinator.coordinatorId()).isEmpty());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewCoordinatorLease(
                        coordinator.coordinatorId(), coordinator.generation(), 100, 60_000)).errorCode());
        assertEquals(100, ledger.advanceSafePoint());
    }

    /** 前台持有租约主键锁再删除时，后台到期清理不能先占过期索引造成反向死锁。 */
    @Test
    void expiryGcMustFollowForegroundPrimaryKeyLockOrder() throws Exception {
        var snapshot = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                """)) {
            age.setLong(1, snapshot.txId());
            age.executeUpdate();
        }
        try (Connection foreground = dataSource.getConnection()) {
            foreground.setAutoCommit(false);
            try (PreparedStatement lock = foreground.prepareStatement("""
                    select tx_id from mvcc_snapshot_lease_v2 where tx_id = ? for update
                    """)) {
                lock.setLong(1, snapshot.txId());
                try (ResultSet row = lock.executeQuery()) {
                    assertTrue(row.next());
                }
            }
            try (var worker = Executors.newVirtualThreadPerTaskExecutor()) {
                var gc = worker.submit(() -> ledger.expireSnapshots(10));
                try {
                    Thread.sleep(150);
                    assertFalse(gc.isDone());
                    try (PreparedStatement release = foreground.prepareStatement(
                            "delete from mvcc_snapshot_lease_v2 where tx_id = ?")) {
                        release.setLong(1, snapshot.txId());
                        assertEquals(1, release.executeUpdate());
                    }
                    foreground.commit();
                } finally {
                    foreground.rollback();
                }
                assertEquals(0, gc.get(5, TimeUnit.SECONDS));
            }
        }
    }

    /** 无单笔写入的快照由协调器租约保护，首次写入可幂等提升为独立租约。 */
    @Test
    void coordinatorSnapshotPromotionMustPreserveGcAndSeparateTransactionIds() {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        var view = ledger.snapshotUnderCoordinatorLease(coordinator.coordinatorId(),
                coordinator.generation(), 30_000);
        assertEquals(100, view.readTs());
        assertEquals(coordinator.readTs(), view.leaseReadTs());
        long localTxId = -1234;
        assertTrue(ledger.snapshotExpiry(localTxId).isEmpty());
        ledger.promoteCoordinatorSnapshot(localTxId, view.readTs(), view.deadlineMillis(),
                coordinator.coordinatorId(), coordinator.generation());
        ledger.promoteCoordinatorSnapshot(localTxId, view.readTs(), view.deadlineMillis(),
                coordinator.coordinatorId(), coordinator.generation());
        assertEquals(view.deadlineMillis(), ledger.snapshotExpiry(localTxId).orElseThrow());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.promoteCoordinatorSnapshot(localTxId,
                        view.readTs(), view.deadlineMillis() - 1000,
                        coordinator.coordinatorId(), coordinator.generation())).errorCode());
        assertTrue(ledger.releaseCoordinatorLease(coordinator.coordinatorId(), coordinator.generation()));
        assertEquals(99, ledger.advanceSafePoint());
        ledger.releaseSnapshot(localTxId);
        assertEquals(100, ledger.advanceSafePoint());
        assertTrue(ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).txId() > 0);
    }

    /** 首写租约和完整参与者必须同事务落盘，响应重试幂等且冲突不能留下半套状态。 */
    @Test
    void coordinatorPromotionAndMetadataRegistrationMustBeAtomic() {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        var view = ledger.snapshotUnderCoordinatorLease(coordinator.coordinatorId(),
                coordinator.generation(), 30_000);
        long txId = -1234;
        var record = ledger.promoteAndRegisterCoordinatorSnapshot(txId, view.readTs(),
                view.deadlineMillis(), coordinator.coordinatorId(), coordinator.generation(),
                IsolationLevel.REPEATABLE_READ, Set.of("a"));
        assertEquals(TxStatus.ACTIVE, record.status());
        assertEquals(Set.of("a"), metadata.get(txId).orElseThrow().participants());
        assertEquals(view.deadlineMillis(), ledger.snapshotExpiry(txId).orElseThrow());
        var replay = ledger.promoteAndRegisterCoordinatorSnapshot(txId, view.readTs(),
                view.deadlineMillis(), coordinator.coordinatorId(), coordinator.generation(),
                IsolationLevel.REPEATABLE_READ, Set.of("a"));
        assertEquals(record.txId(), replay.txId());
        assertEquals(record.startTs(), replay.startTs());
        assertEquals(record.participants(), replay.participants());
        assertEquals(record.status(), replay.status());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.promoteAndRegisterCoordinatorSnapshot(txId,
                        view.readTs(), view.deadlineMillis(), coordinator.coordinatorId(),
                        coordinator.generation(), IsolationLevel.REPEATABLE_READ, Set.of("b"))).errorCode());

        long conflictId = -1235;
        metadata.createWithParticipants(conflictId, view.readTs(), IsolationLevel.REPEATABLE_READ, Set.of("b"));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.promoteAndRegisterCoordinatorSnapshot(conflictId,
                        view.readTs(), view.deadlineMillis(), coordinator.coordinatorId(),
                        coordinator.generation(), IsolationLevel.REPEATABLE_READ, Set.of("a"))).errorCode());
        assertTrue(ledger.snapshotExpiry(conflictId).isEmpty());
    }

    /** 旧代数在续租后不能继续发放无单笔租约的快照。 */
    @Test
    void staleCoordinatorGenerationMustNotIssueSnapshots() {
        var first = ledger.acquireCoordinatorLease(60_000);
        var second = ledger.renewCoordinatorLease(first.coordinatorId(), first.generation(), 100, 60_000);
        var protectedView = ledger.snapshotUnderCoordinatorLease(second.coordinatorId(),
                second.generation(), 30_000);
        ledger.assertCoordinatorSnapshot(second.coordinatorId(), second.generation(),
                protectedView.readTs(), protectedView.deadlineMillis(), -1);
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.assertCoordinatorSnapshot(
                        first.coordinatorId(), first.generation(), protectedView.readTs(),
                        protectedView.deadlineMillis(), -1)).errorCode());
        assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                assertThrows(MvccException.class, () -> ledger.snapshotUnderCoordinatorLease(
                        first.coordinatorId(), first.generation(), 30_000)).errorCode());
        assertEquals(100, ledger.snapshotUnderCoordinatorLease(second.coordinatorId(),
                second.generation(), 30_000).readTs());
    }

    /** 批量 begin 的独立事务号共享一次已发布快照，全部租约提交后才允许 GC 前进。 */
    @Test
    void batchBeginMustPersistDistinctLeasesAtOnePublishedSnapshot() {
        var reservations = ledger.beginBatch(8, 60_000);
        assertEquals(8, reservations.size());
        assertEquals(8, reservations.stream().mapToLong(value -> value.txId()).distinct().count());
        long deadline = reservations.getFirst().deadlineMillis();
        for (var reservation : reservations) {
            assertEquals(100, reservation.startTs());
            assertEquals(deadline, reservation.deadlineMillis());
            assertFalse(reservation.metadataPersisted());
            assertEquals(deadline, ledger.snapshotExpiry(reservation.txId()).orElseThrow());
        }
        assertEquals(99, ledger.advanceSafePoint());
        reservations.forEach(value -> ledger.releaseSnapshot(value.txId()));
        assertEquals(100, ledger.advanceSafePoint());
    }

    /** 已确认提交先于批量 begin 时，批内所有新快照均须覆盖该提交。 */
    @Test
    void batchBeginAfterAcknowledgedCommitMustReadNewPublishedWatermark() {
        var writer = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        long commitTs = ledger.commitPreparedIntents(writer.txId(), writer.deadlineMillis(), Set.of("a")).commitTs();
        var reservations = ledger.beginBatch(4, 60_000);
        assertTrue(reservations.stream().allMatch(value -> value.startTs() == commitTs));
    }

    /** 多主编号步长不为一时，批量租约仍须返回真实生成的全部事务 ID。 */
    @Test
    void batchBeginMustRespectMysqlAutoIncrementStep() {
        try (HikariDataSource incremented = (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl() + "?sessionVariables=auto_increment_increment=2",
                MYSQL.getUsername(), MYSQL.getPassword())) {
            var alternate = new MysqlPublicationLedger(incremented);
            var reservations = alternate.beginBatch(4, 60_000);
            assertEquals(4, reservations.size());
            assertEquals(4, reservations.stream().mapToLong(value -> value.txId()).distinct().count());
            for (int index = 1; index < reservations.size(); index++) {
                assertEquals(2, reservations.get(index).txId() - reservations.get(index - 1).txId());
            }
            for (var reservation : reservations) {
                assertEquals(reservation.deadlineMillis(),
                        alternate.snapshotExpiry(reservation.txId()).orElseThrow());
            }
        }
    }

    /** 到期接管扫描只返回仍为 PENDING 的预留，并保留事务身份。 */
    @Test
    void recoveryScanMustFindExpiredPendingReservations() throws Exception {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_publication_entry_v1 set expires_at = 0 where commit_ts = ?
                """)) {
            age.setLong(1, commitTs);
            age.executeUpdate();
        }
        var candidates = ledger.findExpiredPending(System.currentTimeMillis(), 0, 10);
        assertEquals(1, candidates.size());
        assertEquals(started.txId(), candidates.getFirst().txId());
        assertEquals(commitTs, candidates.getFirst().commitTs());
        ledger.decideAbortedAfterFencing(started.txId(), commitTs);
        assertTrue(ledger.findExpiredPending(System.currentTimeMillis(), 0, 10).isEmpty());
    }

    /** 既有中心回滚可补账本，但单分片本地待确认时间戳不得被误判回滚。 */
    @Test
    void recoveryMayReplayAbortedMetadataButMustFenceLocalPendingCommit() {
        var aborted = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(aborted.txId(), Set.of("a"));
        long abortedTs = ledger.reserveCommit(aborted.txId(), aborted.deadlineMillis());
        metadata.markAborted(aborted.txId());
        ledger.decideAbortedAfterFencing(aborted.txId(), abortedTs);
        assertEquals(abortedTs, ledger.advancePublished(16));

        var localPending = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(localPending.txId(), Set.of("a"));
        long pendingTs = ledger.reserveCommit(localPending.txId(), localPending.deadlineMillis());
        metadata.markLocalCommitPending(localPending.txId(), pendingTs);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.decideAbortedAfterFencing(
                        localPending.txId(), pendingTs)).errorCode());
        assertEquals(abortedTs, ledger.advancePublished(16));
        var proof = new ParticipantRecord(localPending.txId(), localPending.startTs(), TxStatus.COMMITTED, pendingTs);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.completeLocalCommitAfterFencing(
                        proof, pendingTs + 1, "a")).errorCode());
        ledger.completeLocalCommitAfterFencing(proof, pendingTs, "a");
        ledger.completeLocalCommitAfterFencing(proof, pendingTs, "a");
        assertEquals(pendingTs, ledger.advancePublished(16));
    }

    /** 预留前必须有可恢复的完整参与者元数据，避免崩溃后无法定位 intent。 */
    @Test
    void reservationRequiresDurableParticipantRegistration() {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveCommit(started.txId(),
                        started.deadlineMillis())).errorCode());
        metadata.createWithParticipants(started.txId(), started.startTs(),
                IsolationLevel.REPEATABLE_READ, Set.of("a", "b"));
        assertTrue(ledger.reserveCommit(started.txId(), started.deadlineMillis()) > started.startTs());
    }

    /** 单分片候选时间与 PENDING 账本原子落盘，错误路由不能消耗提交编号。 */
    @Test
    void localCommitReservationMustAtomicallyPersistCandidateAndLedgerSlot() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveLocalCommit(
                        first.txId(), first.deadlineMillis(), "b")).errorCode());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        long commitTs = ledger.reserveLocalCommit(first.txId(), first.deadlineMillis(), "a");
        assertEquals(101, commitTs);
        assertEquals(commitTs, ledger.reserveLocalCommit(first.txId(), first.deadlineMillis(), "a"));
        var candidate = metadata.get(first.txId()).orElseThrow();
        assertEquals(TxStatus.PREPARING, candidate.status());
        assertEquals(commitTs, candidate.commitTs().longValue());
        assertEquals(PublicationEntryStatus.PENDING, ledger.findByTxId(first.txId()).orElseThrow().status());
        assertEquals(100, ledger.currentPublished());
    }

    /** 批量本地预留按事务身份分配独立时间戳，任一错误使整批无缺号回滚。 */
    @Test
    void localReservationBatchMustRemainAtomicAndReplayable() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var third = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(first, second, third)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveLocalCommitBatch(List.of(
                        new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                        new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "wrong"))))
                        .errorCode());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        List<Long> reserved = ledger.reserveLocalCommitBatch(List.of(
                new PublicationLocalCommitRequest(third.txId(), third.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "a")));
        assertEquals(List.of(103L, 101L, 102L), reserved);
        assertEquals(101, ledger.reserveLocalCommit(first.txId(), first.deadlineMillis(), "a"));
        assertEquals(100, ledger.currentPublished());
        for (var started : List.of(first, second, third)) {
            assertEquals(TxStatus.PREPARING, metadata.get(started.txId()).orElseThrow().status());
            assertEquals(PublicationEntryStatus.PENDING,
                    ledger.findByTxId(started.txId()).orElseThrow().status());
        }
    }

    /** 乱序交付的本地提交证明在同一控制事务中成为连续可见前缀。 */
    @Test
    void localCompletionBatchMustAtomicallyPublishContiguousProofs() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var third = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(first, second, third)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        List<Long> commits = ledger.reserveLocalCommitBatch(List.of(
                new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(third.txId(), third.deadlineMillis(), "a")));
        List<Boolean> published = ledger.completeLocalCommitBatch(List.of(
                new PublicationLocalCommitProof(
                        new ParticipantRecord(third.txId(), third.startTs(), TxStatus.COMMITTED, commits.get(2)),
                        commits.get(2), "a"),
                new PublicationLocalCommitProof(
                        new ParticipantRecord(first.txId(), first.startTs(), TxStatus.COMMITTED, commits.get(0)),
                        commits.get(0), "a"),
                new PublicationLocalCommitProof(
                        new ParticipantRecord(second.txId(), second.startTs(), TxStatus.COMMITTED, commits.get(1)),
                        commits.get(1), "a")));
        assertEquals(List.of(true, true, true), published);
        assertEquals(commits.getLast().longValue(), ledger.currentPublished());
        for (var started : List.of(first, second, third)) {
            assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
            assertEquals(PublicationEntryStatus.COMMITTED,
                    ledger.findByTxId(started.txId()).orElseThrow().status());
        }
    }

    /** 较早 PENDING 阻止批量本地终态发布，错误证明使整批决议原子回滚。 */
    @Test
    void localCompletionBatchMustRespectPendingAndRollBackMismatchedProof() {
        var pending = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(pending, first, second)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        long low = ledger.reserveCommit(pending.txId(), pending.deadlineMillis());
        List<Long> commits = ledger.reserveLocalCommitBatch(List.of(
                new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "a")));
        var firstProof = new PublicationLocalCommitProof(
                new ParticipantRecord(first.txId(), first.startTs(), TxStatus.COMMITTED, commits.get(0)),
                commits.get(0), "a");
        var secondProof = new PublicationLocalCommitProof(
                new ParticipantRecord(second.txId(), second.startTs(), TxStatus.COMMITTED, commits.get(1)),
                commits.get(1), "a");
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.completeLocalCommitBatch(List.of(
                        firstProof, new PublicationLocalCommitProof(secondProof.local(), secondProof.commitTs(),
                                "wrong")))).errorCode());
        assertEquals(TxStatus.PREPARING, metadata.get(first.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.PENDING, ledger.findByTxId(first.txId()).orElseThrow().status());
        assertEquals(List.of(false, false), ledger.completeLocalCommitBatch(List.of(firstProof, secondProof)));
        assertEquals(100, ledger.currentPublished());
        metadata.markAborted(pending.txId());
        ledger.decideAbortedAfterFencing(pending.txId(), low);
        assertEquals(commits.getLast().longValue(), ledger.advancePublished(16));
    }

    /** 最早槽最后由单笔补决议完成时，必须同时发布此前已提交的批量后缀。 */
    @Test
    void singletonLocalCompletionMustReleaseEarlierBatchedWaiters() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var third = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(first, second, third)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        List<Long> commits = ledger.reserveLocalCommitBatch(List.of(
                new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(third.txId(), third.deadlineMillis(), "a")));
        var later = List.of(
                new PublicationLocalCommitProof(
                        new ParticipantRecord(second.txId(), second.startTs(), TxStatus.COMMITTED, commits.get(1)),
                        commits.get(1), "a"),
                new PublicationLocalCommitProof(
                        new ParticipantRecord(third.txId(), third.startTs(), TxStatus.COMMITTED, commits.get(2)),
                        commits.get(2), "a"));
        assertEquals(List.of(false, false), ledger.completeLocalCommitBatch(later));
        assertEquals(100, ledger.currentPublished());
        var earliest = new ParticipantRecord(first.txId(), first.startTs(), TxStatus.COMMITTED, commits.get(0));
        assertTrue(ledger.completeLocalCommitAfterFencing(earliest, commits.get(0), "a"));
        assertEquals(commits.getLast().longValue(), ledger.currentPublished());
        ledger.awaitPublished(third.txId(), commits.getLast(), 1000);
    }

    /** 租约到期后迟到的提交决议必须失败，恢复才能先占终态并清理参与者。 */
    @Test
    void expiredLeaseMustRejectLateCommitDecision() throws Exception {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                """)) {
            age.setLong(1, started.txId());
            age.executeUpdate();
        }
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.decideCommitted(started.txId(), commitTs,
                        Set.of("a"))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(100, ledger.advancePublished(16));
        metadata.markAborted(started.txId());
        ledger.decideAbortedAfterFencing(started.txId(), commitTs);
        assertEquals(commitTs, ledger.advancePublished(16));
    }

    /** 到期意向经中心 ABORTED 决议和参与者 fencing 后才释放 PENDING 水位。 */
    @Test
    void expiredPendingIntentMustAbortThroughIndependentRecovery() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        shard.prewrite(started.txId(), started.startTs(), List.of(Mutation.put("recovery:abort", new byte[]{1})));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        agePending(started.txId(), commitTs);

        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.ABORTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status());
        assertEquals(commitTs, ledger.currentPublished());
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
    }

    /** 预写后未预留的租约即使先被 GC 删除，恢复仍须清理参与者且不造账本缺号。 */
    @Test
    void expiredUnreservedIntentMustAbortWithoutAdvancingWatermark() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        shard.prewrite(started.txId(), started.startTs(), List.of(Mutation.put("recovery:unreserved", new byte[]{3})));
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                """)) {
            age.setLong(1, started.txId());
            age.executeUpdate();
        }
        assertEquals(1, ledger.expireSnapshots(10));
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
        assertEquals(List.of(started.txId()), ledger.findExpiredUnreserved(System.currentTimeMillis(), 0, 10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status());
        assertTrue(ledger.findByTxId(started.txId()).isEmpty());
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
        assertEquals(100, ledger.currentPublished());
    }

    /** 协调者分配的负事务号在租约过期后仍能由恢复游标发现并中止。 */
    @Test
    void expiredCoordinatorTransactionMustBeFoundByRecoveryCursor() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        var view = ledger.snapshotUnderCoordinatorLease(coordinator.coordinatorId(),
                coordinator.generation(), 30_000);
        long txId = -1234;
        ledger.promoteCoordinatorSnapshot(txId, view.readTs(), view.deadlineMillis(),
                coordinator.coordinatorId(), coordinator.generation());
        metadata.createWithParticipants(txId, view.readTs(), IsolationLevel.REPEATABLE_READ, Set.of("a"));
        shard.prewrite(txId, view.readTs(), List.of(Mutation.put("recovery:negative", new byte[]{8})));
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                """)) {
            age.setLong(1, txId);
            age.executeUpdate();
        }
        assertEquals(List.of(txId), ledger.findExpiredUnreserved(System.currentTimeMillis(), Long.MIN_VALUE, 10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        assertEquals(TxStatus.ABORTED, shard.participantRecord(txId).orElseThrow().status());
    }

    /** 周期 GC 先移除过期租约时，后台扫描仍须自动恢复未预留的参与者。 */
    @Test
    void maintenanceSchedulerMustRecoverUnreservedIntentAfterLeaseGc() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        shard.prewrite(started.txId(), started.startTs(),
                List.of(Mutation.put("recovery:scheduled", new byte[]{5})));
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                """)) {
            age.setLong(1, started.txId());
            age.executeUpdate();
        }
        assertEquals(1, ledger.expireSnapshots(10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        var runner = new PublicationRecoveryRunner(ledger, recovery, metadata);
        var gc = new PublicationGcManager(ledger, metadata, java.util.Map.of("a", shard));
        try (var scheduler = new PublicationMaintenanceScheduler(runner, gc, 10, 10, 20, 20)) {
            scheduler.start();
            scheduler.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline
                    && shard.participantRecord(started.txId()).orElseThrow().status() != TxStatus.ABORTED) {
                Thread.sleep(20);
            }
            assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status(), () ->
                    "recovery ticks=" + scheduler.recoveryStats().runCount()
                            + ", tick failures=" + scheduler.recoveryStats().failureCount()
                            + ", scans=" + runner.scanStats().runCount()
                            + ", scan failures=" + runner.scanStats().failureCount()
                            + ", transactions=" + runner.transactionStats().runCount()
                            + ", transaction failures=" + runner.transactionStats().failureCount()
                            + ", gc ticks=" + scheduler.gcStats().runCount()
                            + ", gc failures=" + scheduler.gcStats().failureCount());
            assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status(), () ->
                    "transaction failures=" + runner.transactionStats().failureCount()
                            + ", last failure=" + runner.lastTransactionFailure());
            assertTrue(scheduler.recoveryStats().runCount() > 0);
            assertEquals(0, scheduler.recoveryStats().failureCount());
        }
    }

    /** 中心已中止但本地未回滚的事务在租约删除后仍须由周期发现器完成。 */
    @Test
    void unreservedAbortedMetadataMustStillFenceParticipantAfterLeaseGc() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        shard.prewrite(started.txId(), started.startTs(),
                List.of(Mutation.put("recovery:aborted-unreserved", new byte[]{6})));
        metadata.markAborted(started.txId());
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                """)) {
            age.setLong(1, started.txId());
            age.executeUpdate();
        }
        assertEquals(1, ledger.expireSnapshots(10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status());
        assertTrue(ledger.findExpiredUnreserved(System.currentTimeMillis(), 0, 10).isEmpty());
    }

    /** 已发布中心决议由独立恢复实例补物化同一个 commitTs。 */
    @Test
    void committedIntentMustMaterializeAfterRecoveryRestart() {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        shard.prewrite(started.txId(), started.startTs(), List.of(Mutation.put("recovery:commit", new byte[]{7})));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        ledger.decideCommitted(started.txId(), commitTs, Set.of("a"));
        var restartedStore = new MysqlTransactionMetadataStore(dataSource);
        var restartedShard = new MysqlStorageAdapter("a", dataSource, restartedStore);
        var recovery = new PublicationRecoveryManager(new MysqlPublicationLedger(dataSource), restartedStore,
                java.util.Map.of("a", restartedShard));
        assertEquals(1, new PublicationRecoveryRunner(new MysqlPublicationLedger(dataSource), recovery,
                restartedStore).recoverOnce(10));
        assertEquals(commitTs, ledger.currentPublished());
        assertArrayEquals(new byte[]{7}, restartedShard.get(new ReadContext(9999, commitTs,
                IsolationLevel.READ_COMMITTED), "recovery:commit").orElseThrow().value());
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
    }

    /** 终态恢复扫描只返回新账本决议，不误接管迁移前遗留的旧协议元数据。 */
    @Test
    void terminalRecoveryScanMustExcludeLegacyMetadataWithoutLedgerEntry() {
        metadata.createCommitted(50, 50, IsolationLevel.REPEATABLE_READ, Set.of("a"), 60);
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(), Set.of("a"));
        assertEquals(List.of(started.txId()), ledger.findUnfinalizedTerminal(0, 10));
        metadata.markFinalized(started.txId());
        assertTrue(ledger.findUnfinalizedTerminal(0, 10).isEmpty());
    }

    /** 已决议的负事务号也必须进入终态物化游标。 */
    @Test
    void committedCoordinatorTransactionMustBeFoundByRecoveryCursor() {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        var view = ledger.snapshotUnderCoordinatorLease(coordinator.coordinatorId(),
                coordinator.generation(), 30_000);
        long txId = -1234;
        ledger.promoteCoordinatorSnapshot(txId, view.readTs(), view.deadlineMillis(),
                coordinator.coordinatorId(), coordinator.generation());
        metadata.createWithParticipants(txId, view.readTs(), IsolationLevel.REPEATABLE_READ, Set.of("a"));
        shard.prewrite(txId, view.readTs(), List.of(Mutation.put("recovery:negative-committed", new byte[]{9})));
        long commitTs = ledger.commitPreparedIntents(txId, view.deadlineMillis(), Set.of("a")).commitTs();
        assertEquals(List.of(txId), ledger.findUnfinalizedTerminal(Long.MIN_VALUE, 10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        assertEquals(TxStatus.COMMITTED, shard.participantRecord(txId).orElseThrow().status());
        assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(9999, commitTs,
                IsolationLevel.READ_COMMITTED), "recovery:negative-committed").orElseThrow().value());
    }

    /** 本地物理提交后中心仍为 PREPARING 时，恢复必须采用参与者 COMMITTED 证明。 */
    @Test
    void localCommittedParticipantMustWinOverExpiredPendingMetadata() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        long commitTs = ledger.reserveLocalCommit(started.txId(), started.deadlineMillis(), "a");
        shard.directCommit(started.txId(), started.startTs(), commitTs,
                List.of(Mutation.put("recovery:local", new byte[]{9})));
        agePending(started.txId(), commitTs);

        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertTrue(recovery.recover(started.txId()));
        assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.COMMITTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        assertEquals(commitTs, ledger.currentPublished());
        assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(9999, commitTs,
                IsolationLevel.READ_COMMITTED), "recovery:local").orElseThrow().value());
    }

    /** 本地物理提交尚未发生时，恢复保留独立 txId 对应的真实开始快照并回滚。 */
    @Test
    void missingLocalCommitMustAbortWithOriginalSnapshotTimestamp() throws Exception {
        var shard = new MysqlStorageAdapter("a", dataSource, metadata);
        shard.initialize();
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        assertTrue(started.txId() != started.startTs());
        metadata.registerParticipants(started.txId(), Set.of("a"));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        metadata.markLocalCommitPending(started.txId(), commitTs);
        agePending(started.txId(), commitTs);

        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        assertTrue(recovery.recover(started.txId()));
        assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status());
        assertEquals(PublicationEntryStatus.ABORTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        var local = shard.participantRecord(started.txId()).orElseThrow();
        assertEquals(TxStatus.ABORTED, local.status());
        assertEquals(started.startTs(), local.startTs());
        assertEquals(commitTs, ledger.currentPublished());
    }

    /** 账本终态须等中心元数据回收并严格早于持久安全点后才可删除。 */
    @Test
    void terminalLedgerPurgeMustFollowMetadataAndSafePoint() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        long high = ledger.reserveCommit(second.txId(), second.deadlineMillis());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        ledger.decideAbortedAfterFencing(second.txId(), high);
        assertEquals(high, ledger.advancePublished(16));
        ledger.releaseSnapshot(first.txId());
        ledger.releaseSnapshot(second.txId());
        metadata.markFinalized(first.txId());
        metadata.markFinalized(second.txId());
        assertEquals(high, ledger.advanceSafePoint());
        assertEquals(0, ledger.purgeTerminal(high, Long.MAX_VALUE, 10));
        assertTrue(metadata.purgeFinalized(high, Long.MAX_VALUE, 10) >= 1);
        assertEquals(1, ledger.purgeTerminal(high, Long.MAX_VALUE, 10));
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        assertTrue(ledger.findByTxId(second.txId()).isPresent());
    }

    /** GC 编排须先回收已完成中心元数据，再处理早于安全点的终态账本。 */
    @Test
    void publicationGcManagerMustReclaimTerminalInDependencyOrder() throws Exception {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        long high = ledger.reserveCommit(second.txId(), second.deadlineMillis());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        ledger.decideAbortedAfterFencing(second.txId(), high);
        ledger.advancePublished(16);
        ledger.releaseSnapshot(first.txId());
        ledger.releaseSnapshot(second.txId());
        metadata.markFinalized(first.txId());
        metadata.markFinalized(second.txId());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_txn_finalized set finalized_at = 0");
            statement.executeUpdate("update mvcc_publication_entry_v1 set expires_at = 0");
        }
        var gc = new PublicationGcManager(ledger, metadata, java.util.Map.of(),
                16, 0, 1000, System::nanoTime);
        assertEquals(0, gc.gcOnce(16));
        assertEquals(high, gc.currentSafePoint());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        assertTrue(ledger.findByTxId(second.txId()).isPresent());
    }

    /** GC 分多轮执行时，中心回收不得使用尚未到达全部分片的更高安全点。 */
    @Test
    void publicationGcCycleMustHoldOneSafePointUntilEveryShardAdvances() {
        List<String> calls = new ArrayList<>();
        var clock = new AtomicLong();
        var gc = new PublicationGcManager(ledger, metadata,
                Map.of("a", recordingGcAdapter("a", calls), "b", recordingGcAdapter("b", calls)),
                16, 0, 1, () -> clock.getAndAdd(2_000_000L));
        assertEquals(0, gc.gcOnce(16));
        assertEquals(List.of("a:100"), calls);

        var later = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(later.txId(), Set.of("a"));
        long laterCommit = ledger.commitPreparedIntents(later.txId(), later.deadlineMillis(), Set.of("a")).commitTs();
        metadata.markFinalized(later.txId());
        ledger.releaseSnapshot(later.txId());
        assertEquals(laterCommit, ledger.advanceSafePoint());

        gc.gcOnce(16);
        assertEquals(List.of("a:100", "b:100"), calls);
        gc.gcOnce(16);
        gc.gcOnce(16);
        gc.gcOnce(16);
        assertEquals(List.of("a:100", "b:100", "a:" + laterCommit), calls);
    }

    /** 分片 GC 失败后必须重试同一水位和分片，不能跳到中心元数据回收。 */
    @Test
    void failedShardGcMustRetryBeforePurgingCentralMetadata() {
        var failFirst = new AtomicBoolean(true);
        List<String> calls = new ArrayList<>();
        StorageAdapter shard = (StorageAdapter) Proxy.newProxyInstance(StorageAdapter.class.getClassLoader(),
                new Class<?>[]{StorageAdapter.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "gc" -> {
                        calls.add("a:" + args[0]);
                        if (failFirst.getAndSet(false)) {
                            throw new IllegalStateException("simulated shard GC failure");
                        }
                        yield 0;
                    }
                    case "shardId" -> "a";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        var gc = new PublicationGcManager(ledger, metadata, Map.of("a", shard),
                16, 0, 1000, System::nanoTime);
        assertThrows(IllegalStateException.class, () -> gc.gcOnce(16));
        assertEquals(List.of("a:100"), calls);
        assertEquals(0, gc.gcOnce(16));
        assertEquals(List.of("a:100", "a:100"), calls);
    }

    /** 只记录 GC 接收到的水位，其他适配器操作在本测试中应视为错误调用。 */
    private static StorageAdapter recordingGcAdapter(String shardId, List<String> calls) {
        return (StorageAdapter) Proxy.newProxyInstance(StorageAdapter.class.getClassLoader(),
                new Class<?>[]{StorageAdapter.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "gc" -> {
                        calls.add(shardId + ":" + args[0]);
                        yield 0;
                    }
                    case "shardId" -> shardId;
                    case "toString" -> shardId;
                    default -> throw new UnsupportedOperationException("unexpected GC test adapter call: " + method.getName());
                });
    }

    /** 仅测试中缩短两种持久化租约，模拟独立进程在期限后接管。 */
    private void agePending(long txId, long commitTs) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement lease = connection.prepareStatement("""
                     update mvcc_snapshot_lease_v2 set expires_at = 0 where tx_id = ?
                     """);
             PreparedStatement entry = connection.prepareStatement("""
                     update mvcc_publication_entry_v1 set expires_at = 0 where commit_ts = ?
                     """)) {
            lease.setLong(1, txId);
            lease.executeUpdate();
            entry.setLong(1, commitTs);
            entry.executeUpdate();
        }
    }
}

package com.flyingzc.azir.publication.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlShardSegmentFence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 真实 MySQL 下验证本地先提交控制段的预留、物理安装和快照下界。 */
@Testcontainers
class MysqlLocalFirstPublicationLedgerTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("local_first_control").withUsername("test").withPassword("test");
    private static HikariDataSource dataSource;
    private MysqlLocalFirstPublicationLedger ledger;
    private MysqlShardSegmentFence physical;

    /** 建立控制账本与隔离物理栅栏共用的真实 MySQL 测试池。 */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭本类独占的容器连接池。 */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /** 每例建立从时间戳 100 开始的独立控制及分片栅栏时间线。 */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_segment_candidate_v1");
            statement.execute("drop table if exists mvcc_local_segment_fence_v1");
            statement.execute("drop table if exists mvcc_local_segment_state_v1");
            statement.execute("drop table if exists mvcc_local_first_slot_v1");
            statement.execute("drop table if exists mvcc_local_first_txn_v1");
            statement.execute("drop table if exists mvcc_local_first_segment_v1");
            statement.execute("drop table if exists mvcc_local_first_snapshot_v1");
            statement.execute("drop table if exists mvcc_local_first_coordinator_v1");
            statement.execute("drop table if exists mvcc_local_first_state_v1");
        }
        ledger = new MysqlLocalFirstPublicationLedger(dataSource);
        ledger.initializeIsolated(100);
        physical = new MysqlShardSegmentFence("s1", dataSource);
        physical.initializeIsolated();
    }

    /** 目标分片先安装物理栅栏，中心段才可激活；请求键重放不再取新号。 */
    @Test
    void reservationMustActivateOnlyAfterMatchingPhysicalInstall() {
        var begun = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        assertEquals(100, begun.startTs());
        var segment = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        assertEquals(101, segment.lease().firstTs());
        assertEquals(104, segment.lease().lastTs());
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.PREPARING, segment.status());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.activateSegment(segment, physical)).errorCode());
        physical.install(segment.lease(), 5_000);
        var active = ledger.activateSegment(segment, physical);
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.ACTIVE, active.status());
        assertEquals(active, ledger.activateSegment(segment, physical));
        assertEquals(active, ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment("s1", "owner-a",
                        "request-a", 4, 5_000, 99)).errorCode());
        assertEquals(4, queryLong("select count(*) from mvcc_local_first_slot_v1 where status = 'PENDING'"));
        assertEquals(104, queryLong("select next_commit_ts from mvcc_local_first_state_v1"));
        ledger.releaseSnapshot(begun.txId());
    }

    /** 控制段先进入 FENCING，再物理封存未安装的分片段并阻止迟到安装。 */
    @Test
    void preparingSegmentMustFenceMissingPhysicalInstallation() {
        var segment = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 80);
        var request = ledger.beginFencing(segment, true);
        assertEquals(1, request.physicalLease().generation());
        assertEquals("s1", request.shardId());
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.FENCING,
                ledger.segment(101).orElseThrow().status());
        assertEquals(2, ledger.segment(101).orElseThrow().lease().generation());
        assertTrue(physical.fence(request.physicalLease()));
        assertTrue(physical.scanFenced(request.physicalLease(), 100, 10).isEmpty());
        assertEquals(request, ledger.beginFencing(segment, true));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segment.lease(), 5_000)).errorCode());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> ledger.activateSegment(segment, physical)).errorCode());
    }

    /** 逐事务快照到期后，未封存段的保守下界仍固定全局安全点。 */
    @Test
    void segmentSnapshotFloorMustSurviveTransactionLeaseExpiry() throws Exception {
        var reader = ledger.begin(IsolationLevel.REPEATABLE_READ, 20);
        var segment = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 80);
        assertEquals(79, ledger.advanceSafePoint());
        Thread.sleep(50);
        assertEquals(1, ledger.expireSnapshots(10));
        assertEquals(79, ledger.advanceSafePoint());
        ledger.beginFencing(segment, true);
        assertEquals(79, ledger.advanceSafePoint());
        assertEquals(79, ledger.currentSafePoint());
        assertTrue(reader.txId() > 0);
    }

    /** 预留槽插入失败时全局下一个时间戳及段身份必须整笔回滚。 */
    @Test
    void failedSlotInsertionMustRollBackWholeReservation() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_first_slot_v1 "
                    + "add constraint mvcc_test_reject_local_slot check (commit_ts <> 101)");
        }
        try {
            assertThrows(MvccException.class, () -> ledger.reserveSegment("s1", "owner-a",
                    "request-a", 4, 5_000, 100));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_first_slot_v1 "
                        + "drop check mvcc_test_reject_local_slot");
            }
        }
        assertEquals(100, queryLong("select next_commit_ts from mvcc_local_first_state_v1"));
        assertEquals(0, queryLong("select count(*) from mvcc_local_first_segment_v1"));
        assertEquals(0, queryLong("select count(*) from mvcc_local_first_slot_v1"));
        assertEquals(101, ledger.reserveSegment("s1", "owner-a", "request-a",
                4, 5_000, 100).lease().firstTs());
    }

    /** 并发协调者只取得互不重叠的持久时间段。 */
    @Test
    void concurrentReservationsMustCoverUniqueTimeSlots() throws Exception {
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 8; index++) {
                final int owner = index;
                futures.add(workers.submit(() -> ledger.reserveSegment("s1", "owner-" + owner,
                        "request-" + owner, 4, 5_000, 100).lease().firstTs()));
            }
            var firstTimestamps = new HashSet<Long>();
            for (var future : futures) {
                assertTrue(firstTimestamps.add(future.get(10, TimeUnit.SECONDS)));
            }
            assertEquals(8, firstTimestamps.size());
            assertEquals(132, queryLong("select next_commit_ts from mvcc_local_first_state_v1"));
            assertEquals(32, queryLong("select count(*) from mvcc_local_first_slot_v1"));
        }
    }

    /** 协调器级租约在没有逐事务写入时仍固定快照下界，释放后旧代数不能继续开始。 */
    @Test
    void coordinatorLeaseMustProtectSnapshotWithoutPerTransactionRow() {
        var lease = ledger.acquireCoordinatorLease(120_000);
        var view = ledger.snapshotUnderCoordinatorLease(lease.coordinatorId(), lease.generation(), 60_000);
        assertEquals(100, view.readTs());
        assertEquals(0, queryLong("select count(*) from mvcc_local_first_snapshot_v1"));
        assertEquals(99, ledger.advanceSafePoint());
        assertTrue(ledger.releaseCoordinatorLease(lease.coordinatorId(), lease.generation()));
        assertEquals(100, ledger.advanceSafePoint());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.snapshotUnderCoordinatorLease(
                        lease.coordinatorId(), lease.generation(), 60_000)).errorCode());
    }

    /** 协调器续租只可提高保护下界，旧代数即使仍持本地事务也必须失效。 */
    @Test
    void coordinatorRenewalMustFenceOldGeneration() {
        var initial = ledger.acquireCoordinatorLease(120_000);
        var renewed = ledger.renewCoordinatorLease(initial.coordinatorId(),
                initial.generation(), 100, 120_000);
        assertEquals(2, renewed.generation());
        assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                assertThrows(MvccException.class, () -> ledger.snapshotUnderCoordinatorLease(
                        initial.coordinatorId(), initial.generation(), 60_000)).errorCode());
        assertEquals(100, ledger.snapshotUnderCoordinatorLease(
                renewed.coordinatorId(), renewed.generation(), 60_000).readTs());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.assertCoordinatorSnapshot(
                        initial.coordinatorId(), initial.generation(), 100,
                        initial.expiresAtMillis(), -1)).errorCode());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.renewCoordinatorLease(
                        renewed.coordinatorId(), renewed.generation(), 99, 120_000)).errorCode());
        assertFalse(ledger.releaseCoordinatorLease(initial.coordinatorId(), initial.generation()));
        assertTrue(ledger.releaseCoordinatorLease(renewed.coordinatorId(), renewed.generation()));
    }

    /** 发布例程遇到缺失主键槽时必须回滚，不能越过未来已终态槽。 */
    @Test
    void fastPublisherMustFailClosedOnMissingTimeSlot() throws Exception {
        ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_slot_v1 "
                    + "set status = 'ABORTED' where commit_ts = 101");
            statement.executeUpdate("delete from mvcc_local_first_slot_v1 where commit_ts = 102");
        }
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> ledger.advancePublishedFast(10)).errorCode());
        assertEquals(100, ledger.currentPublished());
    }

    /** 旧响应重试只能读取当前有效期限，FENCING 后不得把原 owner 续命。 */
    @Test
    void segmentRenewalMustRespectGenerationAndExpiry() throws Exception {
        var prepared = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(prepared.lease(), 5_000);
        var active = ledger.activateSegment(prepared, physical);
        Thread.sleep(20);
        var renewed = ledger.renewSegment(active);
        assertTrue(renewed.lease().expiresAtMillis() >= active.lease().expiresAtMillis());
        var replay = ledger.renewSegment(active);
        assertTrue(replay.lease().expiresAtMillis() >= renewed.lease().expiresAtMillis());
        ledger.beginFencing(active, true);
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(active)).errorCode());
        physical.fence(prepared.lease());
    }

    /** 控制数据库时间已超过持久截止后，续租不能复活同一代数。 */
    @Test
    void expiredControlOwnerMustFailRenewal() throws Exception {
        var prepared = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(prepared.lease(), 5_000);
        var active = ledger.activateSegment(prepared, physical);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                    + "where segment_start = 101");
        }
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(active)).errorCode());
        assertEquals(1, ledger.beginFencing(ledger.segment(101).orElseThrow(), false)
                .physicalLease().generation());
    }

    /** 用一个只读 SQL 返回控制库持久计数或水位。 */
    private long queryLong(String sql) {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery(sql)) {
            assertTrue(row.next());
            return row.getLong(1);
        } catch (Exception ex) {
            throw new IllegalStateException("read local-first test state failed", ex);
        }
    }
}

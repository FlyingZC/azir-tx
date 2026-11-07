package com.flyingzc.azir.publication.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
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
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 真实 MySQL 下验证分段时间线预留、owner fencing 和连续发布的持久不变量。 */
@Testcontainers
class MysqlSegmentPublicationLedgerTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("segment_ledger").withUsername("test").withPassword("test");
    private static HikariDataSource dataSource;
    private MysqlSegmentPublicationLedger ledger;

    /** 为独立版本表建立有界连接池。 */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 释放本类连接池。 */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /** 每例从已知时间上界初始化完全隔离的分段表。 */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_publication_segment_slot_v1");
            statement.execute("drop table if exists mvcc_publication_segment_v1");
            statement.execute("drop table if exists mvcc_segment_publication_state_v1");
        }
        ledger = new MysqlSegmentPublicationLedger(dataSource);
        ledger.initializeIsolated(100);
    }

    /** 不同协调者连续预留且重启初始化不回退上界或重复创建占位槽。 */
    @Test
    void ownersMustReserveContiguousDurableRanges() throws Exception {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        PublicationSegmentLease second = new MysqlSegmentPublicationLedger(dataSource)
                .reserveSegment("owner-b", "reservation-b", 3, 60_000);
        assertEquals(101, first.firstTs());
        assertEquals(104, first.lastTs());
        assertEquals(105, second.firstTs());
        assertEquals(107, second.lastTs());
        assertEquals(100, ledger.currentPublished());
        assertEquals(107, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
        assertEquals(7, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
        PublicationSegmentLease replay = new MysqlSegmentPublicationLedger(dataSource)
                .reserveSegment("owner-a", "reservation-a", 4, 60_000);
        assertEquals(first.firstTs(), replay.firstTs());
        assertEquals(107, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
        ledger.initializeIsolated(100);
        assertEquals(107, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.initializeIsolated(101)).errorCode());
    }

    /** 持久请求键必须区分大小写，且重放不能变更 owner、分段大小或租约预算。 */
    @Test
    void reservationReplayMustPreserveExactIdentityAndLimits() {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment(
                        "owner-b", "reservation-a", 4, 60_000)).errorCode());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment(
                        "owner-a", "reservation-a", 3, 60_000)).errorCode());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment(
                        "owner-a", "reservation-a", 4, 30_000)).errorCode());
        assertThrows(IllegalArgumentException.class,
                () -> ledger.reserveSegment("owner-a", "reservation-a ", 4, 60_000));
        PublicationSegmentLease distinct = ledger.reserveSegment("owner-a", "Reservation-a", 2, 60_000);
        assertEquals(first.lastTs() + 1, distinct.firstTs());
    }

    /** 独立连接池的两个协调者同时预留时，数据库行锁仍保证范围不重叠。 */
    @Test
    void concurrentOwnersWithIndependentPoolsMustNotOverlap() throws Exception {
        try (var secondPool = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var secondLedger = new MysqlSegmentPublicationLedger(secondPool);
            var start = new CountDownLatch(1);
            var first = workers.submit(() -> {
                start.await();
                return ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
            });
            var second = workers.submit(() -> {
                start.await();
                return secondLedger.reserveSegment("owner-b", "reservation-b", 4, 60_000);
            });
            start.countDown();
            PublicationSegmentLease left = first.get(10, TimeUnit.SECONDS);
            PublicationSegmentLease right = second.get(10, TimeUnit.SECONDS);
            assertEquals(101, Math.min(left.firstTs(), right.firstTs()));
            assertEquals(108, Math.max(left.lastTs(), right.lastTs()));
            assertTrue(left.lastTs() < right.firstTs() || right.lastTs() < left.firstTs());
            assertEquals(108, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
            assertEquals(8, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
        }
    }

    /** 状态行丢失但 owner 和槽仍在时，重启不能把时间线重新初始化到旧上界。 */
    @Test
    void missingStateWithDurableSegmentsMustFailClosed() throws Exception {
        ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_segment_publication_state_v1");
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> new MysqlSegmentPublicationLedger(dataSource)
                        .initializeIsolated(100)).errorCode());
        assertEquals(4, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
    }

    /** 已存在的实验表缺少唯一请求键时，重复初始化不能静默沿用旧结构。 */
    @Test
    void outdatedSegmentSchemaMustFailClosed() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_segment_v1 drop column reservation_key");
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.initializeIsolated(100)).errorCode());
    }

    /** 插入占位槽失败时上界、owner 与占位行必须在同一事务中全部回滚。 */
    @Test
    void failedSlotInsertionMustNotLeaveTimestampGap() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_segment_slot_v1 "
                    + "add constraint mvcc_test_reject_pending check (status <> 'PENDING')");
        }
        try {
            assertThrows(MvccException.class, () -> ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000));
            assertEquals(100, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
            assertEquals(0, queryLong("select count(*) from mvcc_publication_segment_v1"));
            assertEquals(0, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_publication_segment_slot_v1 drop check mvcc_test_reject_pending");
            }
        }
        assertEquals(101, ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000).firstTs());
    }

    /** 续租增加代数，旧代数不得再次续租或主动封存。 */
    @Test
    void renewalMustFenceOldOwnerGeneration() {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        PublicationSegmentLease renewed = ledger.renewSegment(first, 60_000);
        assertEquals(2, renewed.generation());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(first, 60_000)).errorCode());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.sealOwnedSegment(first)).errorCode());
        assertEquals(4, ledger.sealOwnedSegment(renewed));
        assertEquals(0, ledger.sealOwnedSegment(renewed));
        assertEquals(104, ledger.advancePublished(10));
    }

    /** 到期 owner 先被代数 fencing，再中止未决槽；重复接管不回退水位。 */
    @Test
    void expiredOwnerMustSealTailIdempotently() throws Exception {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            age.setLong(1, first.firstTs());
            age.executeUpdate();
        }
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(first, 60_000)).errorCode());
        assertEquals(4, ledger.sealExpiredSegments(1));
        assertEquals(0, ledger.sealExpiredSegment(first.firstTs()));
        assertEquals(104, ledger.advancePublished(10));
        assertEquals(4, queryLong("select count(*) from mvcc_publication_segment_slot_v1 where status='ABORTED'"));
    }

    /** 排他接管必须等待已持 owner 共享锁的决议，保留先提交的槽终态。 */
    @Test
    void expiryFencingMustWaitForInFlightSharedOwnerLock() throws Exception {
        PublicationSegmentLease segment = ledger.reserveSegment("owner-a", "reservation-a", 4, 1_000);
        try (Connection writer = dataSource.getConnection()) {
            writer.setAutoCommit(false);
            try (PreparedStatement lock = writer.prepareStatement("""
                    select generation from mvcc_publication_segment_v1
                    where segment_start = ? for share
                    """)) {
                lock.setLong(1, segment.firstTs());
                try (ResultSet row = lock.executeQuery()) {
                    assertTrue(row.next());
                }
            }
            // 决议先取得共享锁；租约随后过期，接管者仍须等待该决议事务结束。
            Thread.sleep(1_100);
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var fencing = workers.submit(() -> ledger.sealExpiredSegment(segment.firstTs()));
                try {
                    Thread.sleep(150);
                    assertFalse(fencing.isDone());
                    try (PreparedStatement decide = writer.prepareStatement("""
                            update mvcc_publication_segment_slot_v1
                            set tx_id = 900, expires_at = 1, status = 'COMMITTED' where commit_ts = 101
                            """)) {
                        assertEquals(1, decide.executeUpdate());
                    }
                    writer.commit();
                } finally {
                    writer.rollback();
                }
                assertEquals(3, fencing.get(5, TimeUnit.SECONDS));
            }
        }
        assertEquals(104, ledger.advancePublished(10));
        assertEquals(1, queryLong("select count(*) from mvcc_publication_segment_slot_v1 where status='COMMITTED'"));
    }

    /** 读取本测试控制库的一项持久计数或水位。 */
    private long queryLong(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet row = statement.executeQuery(sql)) {
            assertTrue(row.next());
            return row.getLong(1);
        }
    }
}

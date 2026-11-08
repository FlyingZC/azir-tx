/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.publication.mysql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

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

/**
 * Real MySQL verify timeline presets, owner fencing and consecutively issued durable not variable.
 */
@Testcontainers
class MysqlSegmentPublicationLedgerTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("segment_ledger").withUsername("test").withPassword("test");

    private static HikariDataSource dataSource;

    private MysqlSegmentPublicationLedger ledger;

    /**
     * Create bounded connection pool for stand-alone version tables.
     */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Release this class connection pool.
     */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /**
     * In each case, a fully segregated segment is initialized from the known time horizon.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * The different coordinators keep them on a continuous basis and restart their initialization without going back up or creating the slot again.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void ownersMustReserveContiguousDurableRanges() throws Exception {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        PublicationSegmentLease second = new MysqlSegmentPublicationLedger(dataSource)
                .reserveSegment("owner-b", "reservation-b", 3, 60_000);
        Assertions.assertEquals(101, first.firstTs());
        Assertions.assertEquals(104, first.lastTs());
        Assertions.assertEquals(105, second.firstTs());
        Assertions.assertEquals(107, second.lastTs());
        Assertions.assertEquals(100, ledger.currentPublished());
        Assertions.assertEquals(107, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
        Assertions.assertEquals(7, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
        PublicationSegmentLease replay = new MysqlSegmentPublicationLedger(dataSource)
                .reserveSegment("owner-a", "reservation-a", 4, 60_000);
        Assertions.assertEquals(first.firstTs(), replay.firstTs());
        Assertions.assertEquals(107, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
        ledger.initializeIsolated(100);
        Assertions.assertEquals(107, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.initializeIsolated(101)).errorCode());
    }

    /**
     * The durable request key must be case-sensitive, and the replay cannot change the owner, segment size or lease budget.
     */
    @Test
    void reservationReplayMustPreserveExactIdentityAndLimits() {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment(
                        "owner-b", "reservation-a", 4, 60_000)).errorCode());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment(
                        "owner-a", "reservation-a", 3, 60_000)).errorCode());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment(
                        "owner-a", "reservation-a", 4, 30_000)).errorCode());
        assertThrows(IllegalArgumentException.class,
                () -> ledger.reserveSegment("owner-a", "reservation-a ", 4, 60_000));
        PublicationSegmentLease distinct = ledger.reserveSegment("owner-a", "Reservation-a", 2, 60_000);
        Assertions.assertEquals(first.lastTs() + 1, distinct.firstTs());
    }

    /**
     * The database lock still guarantees no overlap when the two independent connection pool coordinators are set aside simultaneously.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentOwnersWithIndependentPoolsMustNotOverlap() throws Exception {
        try (
                var secondPool = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
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
            Assertions.assertEquals(101, Math.min(left.firstTs(), right.firstTs()));
            Assertions.assertEquals(108, Math.max(left.lastTs(), right.lastTs()));
            assertTrue(left.lastTs() < right.firstTs() || right.lastTs() < left.firstTs());
            Assertions.assertEquals(108, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
            Assertions.assertEquals(8, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
        }
    }

    /**
     * Reboot cannot re-initiate the time line to the old upper boundary while status lines are lost but owner and slots remain.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingStateWithDurableSegmentsMustFailClosed() throws Exception {
        ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_segment_publication_state_v1");
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> new MysqlSegmentPublicationLedger(dataSource)
                        .initializeIsolated(100)).errorCode());
        Assertions.assertEquals(4, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
    }

    /**
     * When the only requested key is missing from an already existing table, repetition of initialization does not quietly follow the old structure.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void outdatedSegmentSchemaMustFailClosed() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_segment_v1 drop column reservation_key");
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.initializeIsolated(100)).errorCode());
    }

    /**
     * All rollback s of the same transaction must be in the upper and lower lines when inserting a slot failed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failedSlotInsertionMustNotLeaveTimestampGap() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_segment_slot_v1 "
                    + "add constraint mvcc_test_reject_pending check (status <> 'PENDING')");
        }
        try {
            assertThrows(MvccException.class, () -> ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000));
            Assertions.assertEquals(100, queryLong("select next_commit_ts from mvcc_segment_publication_state_v1"));
            Assertions.assertEquals(0, queryLong("select count(*) from mvcc_publication_segment_v1"));
            Assertions.assertEquals(0, queryLong("select count(*) from mvcc_publication_segment_slot_v1"));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_publication_segment_slot_v1 drop check mvcc_test_reject_pending");
            }
        }
        Assertions.assertEquals(101, ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000).firstTs());
    }

    /**
     * The lease is extended by an additional algebra and the old algebra may not be renewed or actively sequestered.
     */
    @Test
    void renewalMustFenceOldOwnerGeneration() {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        PublicationSegmentLease renewed = ledger.renewSegment(first, 60_000);
        Assertions.assertEquals(2, renewed.generation());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(first, 60_000)).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.sealOwnedSegment(first)).errorCode());
        Assertions.assertEquals(4, ledger.sealOwnedSegment(renewed));
        Assertions.assertEquals(0, ledger.sealOwnedSegment(renewed));
        Assertions.assertEquals(104, ledger.advancePublished(10));
    }

    /**
     * Expiration owner first given algebra fencing and then aborted the pending slot; repeat take-over does not retreat watermark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredOwnerMustSealTailIdempotently() throws Exception {
        PublicationSegmentLease first = ledger.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                """)) {
            age.setLong(1, first.firstTs());
            age.executeUpdate();
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(first, 60_000)).errorCode());
        Assertions.assertEquals(4, ledger.sealExpiredSegments(1));
        Assertions.assertEquals(0, ledger.sealExpiredSegment(first.firstTs()));
        Assertions.assertEquals(104, ledger.advancePublished(10));
        Assertions.assertEquals(4, queryLong("select count(*) from mvcc_publication_segment_slot_v1 where status='ABORTED'"));
    }

    /**
     * The platoon taking over must wait for the decision with an owner share lock to keep the previous commit slot terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
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
            // Decision first obtains the shared lock; lease then expires and the receiver is still waiting for the decision transaction to be finished.
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
                        Assertions.assertEquals(1, decide.executeUpdate());
                    }
                    writer.commit();
                } finally {
                    writer.rollback();
                }
                Assertions.assertEquals(3, fencing.get(5, TimeUnit.SECONDS));
            }
        }
        Assertions.assertEquals(104, ledger.advancePublished(10));
        Assertions.assertEquals(1, queryLong("select count(*) from mvcc_publication_segment_slot_v1 where status='COMMITTED'"));
    }

    /**
     * Reads an durable count or watermark in this test control library.
     *
     * @param sql sql
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private long queryLong(final String sql) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery(sql)) {
            assertTrue(row.next());
            return row.getLong(1);
        }
    }
}

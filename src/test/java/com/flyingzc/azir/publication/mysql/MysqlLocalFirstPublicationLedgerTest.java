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

/**
 * The real MySQL verify local preset, physical installation and snapshot lower bounds of the commit control segment.
 */
@Testcontainers
class MysqlLocalFirstPublicationLedgerTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("local_first_control").withUsername("test").withPassword("test");

    private static HikariDataSource dataSource;

    private MysqlLocalFirstPublicationLedger ledger;

    private MysqlShardSegmentFence physical;

    /**
     * Creates a real MySQL test pool with control books shared with isolation physics fence.
     */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Closes the exclusive container of this class connection pool.
     */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /**
     * Initialize an independent control and shard-fence timeline starting at timestamp 100 for each test.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * Target shard first install physics fence to activate the center segment; requested key replay does not take a new number.
     */
    @Test
    void reservationMustActivateOnlyAfterMatchingPhysicalInstall() {
        var begun = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Assertions.assertEquals(100, begun.startTs());
        var segment = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        Assertions.assertEquals(101, segment.lease().firstTs());
        Assertions.assertEquals(104, segment.lease().lastTs());
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.PREPARING, segment.status());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.activateSegment(segment, physical)).errorCode());
        physical.install(segment.lease(), 5_000);
        var active = ledger.activateSegment(segment, physical);
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.ACTIVE, active.status());
        Assertions.assertEquals(active, ledger.activateSegment(segment, physical));
        Assertions.assertEquals(active, ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveSegment("s1", "owner-a",
                        "request-a", 4, 5_000, 99)).errorCode());
        Assertions.assertEquals(4, queryLong("select count(*) from mvcc_local_first_slot_v1 where status = 'PENDING'"));
        Assertions.assertEquals(104, queryLong("select next_commit_ts from mvcc_local_first_state_v1"));
        ledger.releaseSnapshot(begun.txId());
    }

    /**
     * The control section enters FENCING, then physically seals the uninstalled shard segment and prevents late installation.
     */
    @Test
    void preparingSegmentMustFenceMissingPhysicalInstallation() {
        var segment = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 80);
        var request = ledger.beginFencing(segment, true);
        Assertions.assertEquals(1, request.physicalLease().generation());
        Assertions.assertEquals("s1", request.shardId());
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.FENCING,
                ledger.segment(101).orElseThrow().status());
        Assertions.assertEquals(2, ledger.segment(101).orElseThrow().lease().generation());
        assertTrue(physical.fence(request.physicalLease()));
        assertTrue(physical.scanFenced(request.physicalLease(), 100, 10).isEmpty());
        Assertions.assertEquals(request, ledger.beginFencing(segment, true));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segment.lease(), 5_000)).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> ledger.activateSegment(segment, physical)).errorCode());
    }

    /**
     * When transaction snapshot expires, the conservative lower bounds of unsealed segments remain fixed to global safe point.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentSnapshotFloorMustSurviveTransactionLeaseExpiry() throws Exception {
        var reader = ledger.begin(IsolationLevel.REPEATABLE_READ, 20);
        var segment = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 80);
        Assertions.assertEquals(79, ledger.advanceSafePoint());
        Thread.sleep(50);
        Assertions.assertEquals(1, ledger.expireSnapshots(10));
        Assertions.assertEquals(79, ledger.advanceSafePoint());
        ledger.beginFencing(segment, true);
        Assertions.assertEquals(79, ledger.advanceSafePoint());
        Assertions.assertEquals(79, ledger.currentSafePoint());
        assertTrue(reader.txId() > 0);
    }

    /**
     * The next timestamp and paragraph identity must be a whole rollback when prep slot insertion failed.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(100, queryLong("select next_commit_ts from mvcc_local_first_state_v1"));
        Assertions.assertEquals(0, queryLong("select count(*) from mvcc_local_first_segment_v1"));
        Assertions.assertEquals(0, queryLong("select count(*) from mvcc_local_first_slot_v1"));
        Assertions.assertEquals(101, ledger.reserveSegment("s1", "owner-a", "request-a",
                4, 5_000, 100).lease().firstTs());
    }

    /**
     * Concurrent The coordinator will only obtain a non-overlapping durable period.
     *
     * @throws Exception if the operation cannot complete
     */
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
            Assertions.assertEquals(8, firstTimestamps.size());
            Assertions.assertEquals(132, queryLong("select next_commit_ts from mvcc_local_first_state_v1"));
            Assertions.assertEquals(32, queryLong("select count(*) from mvcc_local_first_slot_v1"));
        }
    }

    /**
     * Coordinator Class lease fixed the lower snapshot boundary without writing on a transaction basis and could not continue to start after release.
     */
    @Test
    void coordinatorLeaseMustProtectSnapshotWithoutPerTransactionRow() {
        var lease = ledger.acquireCoordinatorLease(120_000);
        var view = ledger.snapshotUnderCoordinatorLease(lease.coordinatorId(), lease.generation(), 60_000);
        Assertions.assertEquals(100, view.readTs());
        Assertions.assertEquals(0, queryLong("select count(*) from mvcc_local_first_snapshot_v1"));
        Assertions.assertEquals(99, ledger.advanceSafePoint());
        assertTrue(ledger.releaseCoordinatorLease(lease.coordinatorId(), lease.generation()));
        Assertions.assertEquals(100, ledger.advanceSafePoint());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.snapshotUnderCoordinatorLease(
                        lease.coordinatorId(), lease.generation(), 60_000)).errorCode());
    }

    /**
     * The renewal of the coordinator lease only increases the protected lower boundary, and the old algebra must be invalidated even if it holds local transaction.
     */
    @Test
    void coordinatorRenewalMustFenceOldGeneration() {
        var initial = ledger.acquireCoordinatorLease(120_000);
        var renewed = ledger.renewCoordinatorLease(initial.coordinatorId(),
                initial.generation(), 100, 120_000);
        Assertions.assertEquals(2, renewed.generation());
        Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                assertThrows(MvccException.class, () -> ledger.snapshotUnderCoordinatorLease(
                        initial.coordinatorId(), initial.generation(), 60_000)).errorCode());
        Assertions.assertEquals(100, ledger.snapshotUnderCoordinatorLease(
                renewed.coordinatorId(), renewed.generation(), 60_000).readTs());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.assertCoordinatorSnapshot(
                        initial.coordinatorId(), initial.generation(), 100,
                        initial.expiresAtMillis(), -1)).errorCode());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.renewCoordinatorLease(
                        renewed.coordinatorId(), renewed.generation(), 99, 120_000)).errorCode());
        assertFalse(ledger.releaseCoordinatorLease(initial.coordinatorId(), initial.generation()));
        assertTrue(ledger.releaseCoordinatorLease(renewed.coordinatorId(), renewed.generation()));
    }

    /**
     * When a routine is released, rollback must be used when the missing main key slot is encountered. The future terminal state slot cannot be crossed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void fastPublisherMustFailClosedOnMissingTimeSlot() throws Exception {
        ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_slot_v1 "
                    + "set status = 'ABORTED' where commit_ts = 101");
            statement.executeUpdate("delete from mvcc_local_first_slot_v1 where commit_ts = 102");
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> ledger.advancePublishedFast(10)).errorCode());
        Assertions.assertEquals(100, ledger.currentPublished());
    }

    /**
     * The old response retry can only read the current validity period and FENCING cannot continue after the original owner.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(active)).errorCode());
        physical.fence(prepared.lease());
    }

    /**
     * The renewal of the lease cannot revive the same generation after database control has exceeded the end of durable.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredControlOwnerMustFailRenewal() throws Exception {
        var prepared = ledger.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(prepared.lease(), 5_000);
        var active = ledger.activateSegment(prepared, physical);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                    + "where segment_start = 101");
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewSegment(active)).errorCode());
        Assertions.assertEquals(1, ledger.beginFencing(ledger.segment(101).orElseThrow(), false)
                .physicalLease().generation());
    }

    /**
     * Use an read-only SQL to return durable count or watermark.
     *
     * @param sql sql
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private long queryLong(final String sql) {
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery(sql)) {
            assertTrue(row.next());
            return row.getLong(1);
        } catch (final Exception ex) {
            throw new IllegalStateException("read local-first test state failed", ex);
        }
    }
}

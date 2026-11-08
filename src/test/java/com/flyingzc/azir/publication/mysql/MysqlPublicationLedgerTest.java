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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

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

/**
 * Unsettled decision with real MySQL verify books, emptying old protocols and GC fence.
 */
@Testcontainers
class MysqlPublicationLedgerTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("ledger").withUsername("test").withPassword("test");

    private static HikariDataSource dataSource;

    private MysqlPublicationLedger ledger;

    private MysqlTransactionMetadataStore metadata;

    /**
     * Initialization of the old control structure for new book migration inspection.
     */
    @BeforeAll
    static void initializeControl() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
        new MysqlTimestampOracle(dataSource).initialize(100);
        new MysqlTransactionMetadataStore(dataSource).initialize();
    }

    /**
     * Close this test standalone connection pool.
     */
    @AfterAll
    static void closeControl() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /**
     * Each scene creates a new book independently of the old TSO upper world that has been vacated.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * Large timestamp First decision is still invisible, little recovery terminal state is reserved for continuous release.
     */
    @Test
    void outOfOrderDecisionsMustNotRetroactivelyChangeRepeatableRead() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, true);
        Assertions.assertEquals(100, first.startTs());
        Assertions.assertEquals(100, second.startTs());
        assertTrue(first.txId() > 100);
        assertTrue(second.txId() > first.txId());
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        long high = ledger.reserveCommit(second.txId(), second.deadlineMillis());
        Assertions.assertEquals(low, ledger.reserveCommit(first.txId(), first.deadlineMillis()));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveCommit(first.txId(),
                        first.deadlineMillis() + 1)).errorCode());
        Assertions.assertEquals(low + 1, high);
        ledger.decideCommitted(second.txId(), high, Set.of("b"));
        Assertions.assertEquals(100, ledger.advancePublished(16));
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> ledger.awaitPublished(second.txId(), high, 20))
                        .errorCode());
        var oldReader = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        Assertions.assertEquals(100, oldReader.startTs());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        ledger.decideAbortedAfterFencing(first.txId(), low);
        Assertions.assertEquals(high, ledger.advancePublished(16));
        ledger.awaitPublished(second.txId(), high, 1000);
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> ledger.awaitPublished(first.txId(), low, 1000))
                        .errorCode());
        Assertions.assertEquals(100, oldReader.startTs());
        Assertions.assertEquals(high, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(second.txId()).orElseThrow().status());
        Assertions.assertEquals(high, metadata.get(second.txId()).orElseThrow().commitTs().longValue());
    }

    /**
     * The decision argument mismatch must be accompanied by the preservation of metadata ACTIVE with an account book PENDING.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void mismatchedDecisionMustRollBackMetadataAndLedgerTogether() throws Exception {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a", "b"));
        long commitTs = ledger.reserveCommit(started.txId(), started.deadlineMillis());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.decideCommitted(started.txId(), commitTs,
                        Set.of("a"))).errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(100, ledger.advancePublished(16));
        ledger.decideCommitted(started.txId(), commitTs, Set.of("a", "b"));
        var restartedLedger = new MysqlPublicationLedger(dataSource);
        restartedLedger.initialize();
        restartedLedger.decideCommitted(started.txId(), commitTs, Set.of("a", "b"));
        Assertions.assertEquals(commitTs, ledger.advancePublished(16));
    }

    /**
     * When metadata have been updated but account book writing failed, both states must be in the same control rollback.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, () -> ledger.decideCommitted(started.txId(), commitTs,
                        Set.of("a"))).errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(100, ledger.advancePublished(16));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_entry_v1 drop check reject_committed_entry");
        }
        ledger.decideCommitted(started.txId(), commitTs, Set.of("a"));
        Assertions.assertEquals(commitTs, ledger.advancePublished(16));
    }

    /**
     * Commit timestamp, centre decision and account book terminal state of intent transaction are in one control transaction for atomic.
     */
    @Test
    void preparedIntentCommitMustAtomicallyPublishWithoutEarlierPendingSlot() {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a", "b"));
        var published = ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(), Set.of("a", "b"));
        long commitTs = published.commitTs();
        assertTrue(published.publishedImmediately());
        Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.COMMITTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        Assertions.assertEquals(commitTs, ledger.currentPublished());
        Assertions.assertEquals(commitTs, ledger.advancePublished(16));
        Assertions.assertEquals(commitTs, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        Assertions.assertEquals(commitTs, ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(),
                Set.of("a", "b")).commitTs());
    }

    /**
     * Batch commit issues all decision at once. The order of call and the order of timestamp can be different.
     */
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
        Assertions.assertEquals(103, results.get(0).commitTs());
        Assertions.assertEquals(101, results.get(1).commitTs());
        Assertions.assertEquals(102, results.get(2).commitTs());
        assertTrue(results.stream().allMatch(result -> result.publishedImmediately()));
        Assertions.assertEquals(103, ledger.currentPublished());
        Assertions.assertEquals(103, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        for (var started : List.of(first, second, third)) {
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
        }
    }

    /**
     * When the early PENDING slot exists, batch all durable storage but watermark must stop before that slot.
     */
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
        Assertions.assertEquals(low + 1, results.get(0).commitTs());
        Assertions.assertEquals(low + 2, results.get(1).commitTs());
        assertTrue(results.stream().noneMatch(result -> result.publishedImmediately()));
        Assertions.assertEquals(100, ledger.currentPublished());
        ledger.decideAbortedAfterFencing(pending.txId(), low);
        Assertions.assertEquals(low + 2, ledger.advancePublished(16));
    }

    /**
     * The remaining transaction and timestamp distributions must also be the whole rollback when an participant message in batch is wrong.
     */
    @Test
    void invalidBatchIntentMustRollBackEveryDecisionAndTimestamp() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.commitPreparedIntentBatch(List.of(
                        new PublicationIntentCommit(first.txId(), first.deadlineMillis(), Set.of("a")),
                        new PublicationIntentCommit(second.txId(), second.deadlineMillis(), Set.of("wrong")))))
                        .errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        assertTrue(ledger.findByTxId(second.txId()).isEmpty());
        Assertions.assertEquals(101, ledger.commitPreparedIntents(first.txId(), first.deadlineMillis(),
                Set.of("a")).commitTs());
    }

    /**
     * When batch metadata update is rejected by database, the time slot and inserted book decision must be together with rollback.
     *
     * @throws Exception if the operation cannot complete
     */
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
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
            assertTrue(ledger.findByTxId(first.txId()).isEmpty());
            assertTrue(ledger.findByTxId(second.txId()).isEmpty());
            Assertions.assertEquals(100, ledger.currentPublished());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn drop check mvcc_test_batch_update_reject");
            }
        }
        Assertions.assertEquals(101, ledger.commitPreparedIntents(first.txId(), first.deadlineMillis(), Set.of("a")).commitTs());
    }

    /**
     * Early on local PENDING pending, then intent commit cannot be released across the slot even though it is durable storage.
     */
    @Test
    void atomicIntentCommitMustWaitBehindEarlierPendingReservation() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        long low = ledger.reserveCommit(first.txId(), first.deadlineMillis());
        var later = ledger.commitPreparedIntents(second.txId(), second.deadlineMillis(), Set.of("b"));
        Assertions.assertEquals(low + 1, later.commitTs());
        assertFalse(later.publishedImmediately());
        Assertions.assertEquals(100, ledger.currentPublished());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        Assertions.assertEquals(later.commitTs(), ledger.advancePublished(16));
        ledger.awaitPublished(second.txId(), later.commitTs(), 1000);
    }

    /**
     * Local The late reservation of commit cannot go beyond the earlier PENDING and decision is still in RR visibility order.
     */
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
        Assertions.assertEquals(100, ledger.currentPublished());
        metadata.markAborted(first.txId());
        ledger.decideAbortedAfterFencing(first.txId(), low);
        Assertions.assertEquals(high, ledger.advancePublished(16));
    }

    /**
     * After the account book has failed to be inserted, TSO upper bounds and central metadata must be together with rollback without missing numbers.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, () -> ledger.commitPreparedIntents(started.txId(),
                        started.deadlineMillis(), Set.of("a"))).errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        assertTrue(ledger.findByTxId(started.txId()).isEmpty());
        Assertions.assertEquals(100, ledger.currentPublished());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_publication_entry_v1 drop check reject_atomic_commit");
        }
        Assertions.assertEquals(101, ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(),
                Set.of("a")).commitTs());
    }

    /**
     * The old protocol also has activity transaction which does not allow the initialization of a new watermark to an old TSO upper boundary.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void migrationMustRejectUndrainedLegacyTransaction() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        metadata.create(50, 50, IsolationLevel.REPEATABLE_READ);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
    }

    /**
     * The old terminal state protocol refuses to move when the materialization is not completed, and does not allow the creation of a new time line until the marking has been completed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void migrationMustWaitForLegacyTerminalFinalization() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        metadata.createCommitted(50, 50, IsolationLevel.REPEATABLE_READ, Set.of("a"), 60);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
        metadata.markFinalized(50);
        ledger.initialize();
        Assertions.assertEquals(100, ledger.currentPublished());
    }

    /**
     * After the account book is activated, database external keys directly reject the old snapshot lease and restart initialization cannot be re-disable.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void activatedEpochMustFenceLegacySnapshotInserts() throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement oldBegin = connection.prepareStatement("""
                insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at) values (999, 999, 9999999999999)
                """)) {
            SQLException blocked = assertThrows(SQLException.class, oldBegin::executeUpdate);
            Assertions.assertEquals(1452, blocked.getErrorCode());
        }
        try (
                Connection connection = dataSource.getConnection(); PreparedStatement oldProcedure =
                        connection.prepareStatement("call mvcc_begin_v2('READ_COMMITTED', 60000, false, 1)")) {
            SQLException blocked = assertThrows(SQLException.class, oldProcedure::execute);
            Assertions.assertEquals(1452, blocked.getErrorCode());
        }
        new MysqlPublicationLedger(dataSource).initialize();
        Assertions.assertEquals(100, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
    }

    /**
     * The new account book must fail to close when mode stays on ACTIVATING.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void ledgerBeginMustRejectIncompleteProtocolActivation() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1 set protocol_mode = 'ACTIVATING' where singleton_id = 1
                    """);
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.begin(IsolationLevel.READ_COMMITTED,
                        60_000, false)).errorCode());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1 set protocol_mode = 'LEDGER' where singleton_id = 1
                    """);
        }
        Assertions.assertEquals(100, ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
    }

    /**
     * Toggle a new instance to create and keep an old writer fence when the process collapses after ACTIVATING.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void interruptedActivationMustResumeWithoutReopeningLegacyLease() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("""
                    update mvcc_protocol_epoch_v1 set protocol_mode = 'ACTIVATING' where singleton_id = 1
                    """);
        }
        var restarted = new MysqlPublicationLedger(dataSource);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> restarted.begin(IsolationLevel.READ_COMMITTED,
                        60_000, false)).errorCode());
        restarted.initialize();
        Assertions.assertEquals(100, restarted.currentPublished());
        try (Connection connection = dataSource.getConnection(); PreparedStatement oldBegin = connection.prepareStatement("""
                insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at) values (999, 999, 9999999999999)
                """)) {
            Assertions.assertEquals(1452, assertThrows(SQLException.class, oldBegin::executeUpdate).getErrorCode());
        }
    }

    /**
     * The old lease key-sharing lock prevents switching past it before you can activate again.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void activationMustWaitForInFlightLegacyLeaseAndRejectUndrainedState() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        try (
                Connection oldWriter = dataSource.getConnection();
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
        Assertions.assertEquals(100, ledger.currentPublished());
    }

    /**
     * The old TSO upper boundary could not be rebuilt at the time the status line was lost but there were books or lease.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingPublicationStateWithExistingLeaseMustFailClosed() throws Exception {
        ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
    }

    /**
     * The old metadata cannot start a new watermark in the wrong position when exceeding the already durable storage TSO upper bounds.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void migrationMustRejectTimestampUpperBelowCommittedMetadata() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table mvcc_publication_state_v1");
            statement.executeUpdate("update mvcc_legacy_lease_gate_v1 set mode_id = 0 where mode_id = 1");
            statement.executeUpdate("update mvcc_protocol_epoch_v1 set protocol_mode = 'LEGACY' where singleton_id = 1");
        }
        metadata.createCommitted(101, 101, IsolationLevel.REPEATABLE_READ, Set.of("a"), 102);
        metadata.markFinalized(101);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, ledger::initialize).errorCode());
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet mode = statement.executeQuery("""
                        select protocol_mode from mvcc_protocol_epoch_v1 where singleton_id = 1
                        """)) {
            assertTrue(mode.next());
            Assertions.assertEquals("LEGACY", mode.getString(1));
        }
    }

    /**
     * The old process cannot be redistributed after the old status table is archived, and new subparagraphs begin consecutively with the original release of the upper boundary.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedActivationMustPhysicallyFenceOldWriters() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        segments.assertActivated();
        Assertions.assertEquals(100, segments.currentPublished());
        Assertions.assertEquals(101, segments.reserveSegment("owner-a", "reservation-a", 4, 60_000).firstTs());
        assertThrows(SQLException.class, () -> {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.executeUpdate("update mvcc_publication_state_v1 set next_commit_ts = 999 where singleton_id = 1");
            }
        });
        assertThrows(MvccException.class, () -> ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false));
        assertThrows(MvccException.class, ledger::initialize);
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("""
                        select count(*) from information_schema.tables
                        where table_schema = database() and table_name = 'mvcc_publication_state_v1'
                        """)) {
            assertTrue(rows.next());
            Assertions.assertEquals(0, rows.getLong(1));
        }
        segments.assertActivated();
        segments.activateFromLedger();
    }

    /**
     * New business entrances must fail to close when only mode lines are missing the old table archive physics fence.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedModeRowAloneMustNotActivateBusinessEntry() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_protocol_epoch_v1 "
                    + "set protocol_mode = 'SEGMENT' where singleton_id = 1");
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> new MysqlSegmentPublicationLedger(dataSource).assertActivated()).errorCode());
    }

    /**
     * If the old reader or materialization transaction is not empty, switch to fail before an archive table and keep the old protocol available.
     */
    @Test
    void segmentedActivationMustRejectLiveOldSnapshotBeforeFencing() {
        var snapshot = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, segments::activateFromLedger).errorCode());
        Assertions.assertEquals(100, ledger.currentPublished());
        ledger.releaseSnapshot(snapshot.txId());
        segments.activateFromLedger();
        segments.assertActivated();
    }

    /**
     * The old time line cannot be archived even if snapshot lease has been released when the old metadata have not been applied.
     */
    @Test
    void segmentedActivationMustRejectUnfinalizedOldMetadata() {
        var writer = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        ledger.releaseSnapshot(writer.txId());
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, segments::activateFromLedger).errorCode());
        Assertions.assertEquals(100, ledger.currentPublished());
        metadata.markAborted(writer.txId());
        metadata.markFinalized(writer.txId());
        segments.activateFromLedger();
        segments.assertActivated();
    }

    /**
     * When a physical archive collapses and the old initializer recreates an empty table, new examples can still be cleaned up and built.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(100, restarted.currentPublished());
        Assertions.assertEquals(101, restarted.reserveSegment("owner-b", "reservation-b", 2, 60_000).firstTs());
    }

    /**
     * Archived and crashed after entering SEGMENT_PREPARE, restarting only the new watch and final mode.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(101, restarted.reserveSegment("owner-c", "reservation-c", 3, 60_000).firstTs());
    }

    /**
     * The old safe point must migrate with the posted top border and cannot allow new snapshot to cross the already collection version boundary.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedActivationMustCarryOldSafePoint() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_publication_state_v1 set safe_point = 80 where singleton_id = 1");
        }
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("select safe_point from mvcc_segment_publication_state_v1")) {
            assertTrue(row.next());
            Assertions.assertEquals(80, row.getLong(1));
        }
    }

    /**
     * When the real intent is not yet materialization, the new RC parses the released decision and the old RR remains fixed to snapshot.
     */
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
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommitted(first, 101,
                        writer.txId(), writer.deadlineMillis(), Set.of("a"))).errorCode());
        Assertions.assertEquals(101, segments.decideCommitted(renewed, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
        Assertions.assertEquals(101, segments.advancePublished(10));
        var committed = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        Assertions.assertEquals(101, committed.startTs());
        assertTrue(shard.get(new ReadContext(repeatable.txId(), repeatable.startTs(),
                IsolationLevel.REPEATABLE_READ), "segment:visible").isEmpty());
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(committed.txId(), committed.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:visible").orElseThrow().value());
        Assertions.assertEquals(3, segments.sealOwnedSegment(renewed));
        Assertions.assertEquals(104, segments.advancePublished(10));
        Assertions.assertEquals(101, segments.decideCommitted(first, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
    }

    /**
     * Participant identity error cannot separately convert a slot or central metadata to COMMITTED.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidSegmentedDecisionMustRollBackBothAuthorities() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                        writer.txId(), writer.deadlineMillis(), Set.of("wrong"))).errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("""
                        select status, tx_id from mvcc_publication_segment_slot_v1 where commit_ts = 101
                        """)) {
            assertTrue(row.next());
            Assertions.assertEquals("PENDING", row.getString(1));
            Assertions.assertEquals(null, row.getObject(2));
        }
        Assertions.assertEquals(100, segments.currentPublished());
        Assertions.assertEquals(101, segments.decideCommitted(owner, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
    }

    /**
     * Center updates rollback when SQL failed with transaction records. Recovering request is still safe decision.
     *
     * @throws Exception if the operation cannot complete
     */
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
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                            writer.txId(), writer.deadlineMillis(), Set.of("a"))).errorCode());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
            try (
                    Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                    ResultSet row = statement.executeQuery("""
                            select status, tx_id from mvcc_publication_segment_slot_v1 where commit_ts = 101
                            """)) {
                assertTrue(row.next());
                Assertions.assertEquals("PENDING", row.getString(1));
                Assertions.assertEquals(null, row.getObject(2));
            }
            Assertions.assertEquals(100, segments.currentPublished());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn drop check mvcc_test_reject_segment_commit");
            }
        }
        Assertions.assertEquals(101, segments.decideCommitted(owner, 101,
                writer.txId(), writer.deadlineMillis(), Set.of("a")));
        Assertions.assertEquals(101, segments.advancePublished(10));
    }

    /**
     * The unsequenced request for the same sub-paragraph is in decision of a single transaction and is published only over successive resolved slots.
     */
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
        Assertions.assertEquals(103, segments.advancePublished(10));
        Assertions.assertEquals(103, segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false).startTs());
        for (var tx : List.of(first, second, third)) {
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> segments.decideCommittedBatch(batch)).errorCode());
        Assertions.assertEquals(101, segments.decideCommitted(owner, 101,
                first.txId(), first.deadlineMillis(), Set.of("a")));
    }

    /**
     * When an participant message is wrong, no other metadata and slots in batch should be part of the commit.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidSegmentedBatchDecisionMustRollBackAllSlots() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var first = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        metadata.registerParticipants(second.txId(), Set.of("b"));
        var owner = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> segments.decideCommittedBatch(List.of(
                        new PublicationSegmentCommit(owner, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                        new PublicationSegmentCommit(owner, 102, second.txId(), second.deadlineMillis(),
                                Set.of("wrong")))))
                        .errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("""
                        select count(*) from mvcc_publication_segment_slot_v1
                        where commit_ts in (101, 102) and status = 'PENDING' and tx_id is null
                        """)) {
            assertTrue(row.next());
            Assertions.assertEquals(2, row.getLong(1));
        }
        segments.decideCommittedBatch(List.of(
                new PublicationSegmentCommit(owner, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(owner, 102, second.txId(), second.deadlineMillis(), Set.of("b"))));
        Assertions.assertEquals(102, segments.advancePublished(10));
    }

    /**
     * If batch slot update is rejected by database, the previous centre batch update must also have a whole rollback.
     *
     * @throws Exception if the operation cannot complete
     */
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
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () -> segments.decideCommittedBatch(batch)).errorCode());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
            Assertions.assertEquals(100, segments.currentPublished());
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_publication_segment_slot_v1 "
                        + "drop check mvcc_test_reject_segment_batch");
            }
        }
        var renewed = segments.renewSegment(owner, 60_000);
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommittedBatch(batch)).errorCode());
        segments.decideCommittedBatch(List.of(
                new PublicationSegmentCommit(renewed, 101, first.txId(), first.deadlineMillis(), Set.of("a")),
                new PublicationSegmentCommit(renewed, 102, second.txId(), second.deadlineMillis(), Set.of("a"))));
        Assertions.assertEquals(102, segments.advancePublished(10));
    }

    /**
     * The physical list shard has been commit but remains invisible when the center is not filled with decision, after decision and RC/RR each snapshot.
     */
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
        Assertions.assertEquals(101, segments.claimLocalPending(owner, 101,
                writer.txId(), writer.deadlineMillis(), "a"));
        Assertions.assertEquals(TxStatus.PREPARING, metadata.get(writer.txId()).orElseThrow().status());
        shard.directCommit(writer.txId(), writer.startTs(), 101,
                List.of(Mutation.put("segment:direct", new byte[]{4})));
        var local = shard.participantRecord(writer.txId()).orElseThrow();
        Assertions.assertEquals(TxStatus.COMMITTED, local.status());
        var before = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        Assertions.assertEquals(100, before.startTs());
        assertTrue(shard.get(new ReadContext(before.txId(), before.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:direct").isEmpty());
        assertTrue(segments.completeLocalCommitAfterFencing(local, 101, "a"));
        Assertions.assertEquals(101, segments.advancePublished(10));
        var after = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        assertArrayEquals(new byte[]{4}, shard.get(new ReadContext(after.txId(), after.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:direct").orElseThrow().value());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "segment:direct").isEmpty());
        segments.sealOwnedSegment(owner);
        Assertions.assertEquals(101, segments.claimLocalPending(owner, 101,
                writer.txId(), writer.deadlineMillis(), "a"));
        assertFalse(segments.completeLocalCommitAfterFencing(local, 101, "a"));
    }

    /**
     * Local rollback proves that the release of the recognized trunk was delayed after ABORTED physical commit could not be revived version.
     */
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
        Assertions.assertEquals(TxStatus.ABORTED, local.status());
        assertTrue(segments.completeLocalAbortAfterFencing(local, 101, "a"));
        Assertions.assertEquals(101, segments.advancePublished(10));
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertFalse(segments.completeLocalAbortAfterFencing(local, 101, "a"));
        assertThrows(MvccException.class, () -> shard.directCommit(writer.txId(), writer.startTs(), 101,
                List.of(Mutation.put("segment:late", new byte[]{1}))));
    }

    /**
     * Homeer lost commit local terminal state has priority over timeout aborted, recovery in the same row timestamp.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(3, segments.sealExpiredSegment(owner.firstTs()));
        Assertions.assertEquals(PublicationEntryStatus.PENDING,
                segments.findByTxId(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(100, segments.advancePublished(10));
        var restartedMetadata = new MysqlTransactionMetadataStore(dataSource);
        var restartedShard = new MysqlStorageAdapter("a", dataSource, restartedMetadata);
        var restartedLedger = new MysqlSegmentPublicationLedger(dataSource);
        var recovery = new SegmentPublicationRecoveryManager(restartedLedger, restartedMetadata,
                Map.of("a", restartedShard));
        var runner = new SegmentPublicationRecoveryRunner(restartedLedger, recovery, restartedMetadata);
        assertTrue(runner.recoverOnce(10) > 0);
        Assertions.assertEquals(TxStatus.COMMITTED, restartedMetadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(104, restartedLedger.currentPublished());
        assertTrue(restartedLedger.snapshotExpiry(writer.txId()).isEmpty());
        assertArrayEquals(new byte[]{3}, restartedShard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "segment:local-recover").orElseThrow().value());
        Assertions.assertEquals(0, runner.transactionStats().failureCount());
    }

    /**
     * Local ABORTED attests that the patching centre has been suspended and it is assumed that the lead slot cannot be misjudged by an downer tailing.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(3, segments.sealExpiredSegment(owner.firstTs()));
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        var runner = new SegmentPublicationRecoveryRunner(segments, recovery, metadata);
        assertTrue(runner.recoverOnce(10) > 0);
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.ABORTED,
                segments.findByTxId(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(104, segments.currentPublished());
    }

    /**
     * When the homeer and transaction lease are valid, the background cannot take over the newly accepted local candidate.
     */
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
        Assertions.assertEquals(0, runner.recoverOnce(10));
        Assertions.assertEquals(TxStatus.PREPARING, metadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.PENDING,
                segments.findByTxId(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(100, segments.currentPublished());
    }

    /**
     * The owner can no longer claim the PENDING slot and keep centre transaction un commit after taking over for storage.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                        writer.txId(), writer.deadlineMillis(), Set.of("a"))).errorCode());
        Assertions.assertEquals(4, segments.sealExpiredSegment(owner.firstTs()));
        Assertions.assertEquals(104, segments.advancePublished(10));
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
    }

    /**
     * After the previous transaction has been aborted, it can only be released later in the commit slot; RC/RR is still read according to their respective snapshot.
     */
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
        Assertions.assertEquals(100, segments.advancePublished(10));
        shard.abort(aborted.txId());
        assertTrue(segments.abortReservedSlot(owner, 101, aborted.txId(), aborted.deadlineMillis()));
        Assertions.assertEquals(102, segments.advancePublished(10));
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(aborted.txId()).orElseThrow().status());
        var reader = segments.begin(IsolationLevel.READ_COMMITTED, 60_000, false);
        assertArrayEquals(new byte[]{2}, shard.get(new ReadContext(reader.txId(), reader.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:later").orElseThrow().value());
        assertTrue(shard.get(new ReadContext(repeatable.txId(), repeatable.startTs(),
                IsolationLevel.REPEATABLE_READ), "segment:later").isEmpty());
        assertTrue(shard.get(new ReadContext(reader.txId(), reader.startTs(),
                IsolationLevel.READ_COMMITTED), "segment:aborted").isEmpty());
        Assertions.assertEquals(2, segments.sealOwnedSegment(owner));
        assertFalse(segments.abortReservedSlot(owner, 101, aborted.txId(), aborted.deadlineMillis()));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> segments.decideCommitted(owner, 101,
                        aborted.txId(), aborted.deadlineMillis(), Set.of("a"))).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> segments.abortReservedSlot(owner, 102,
                        committed.txId(), committed.deadlineMillis())).errorCode());
    }

    /**
     * If SQL is rejected by database, the center state and subtraction will be maintained at pending, and retry can still drop terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
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
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () -> segments.abortReservedSlot(owner, 101,
                            writer.txId(), writer.deadlineMillis())).errorCode());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
            Assertions.assertEquals(100, segments.currentPublished());
            try (
                    Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                    ResultSet row = statement.executeQuery("""
                            select status, tx_id from mvcc_publication_segment_slot_v1 where commit_ts = 101
                            """)) {
                assertTrue(row.next());
                Assertions.assertEquals("PENDING", row.getString(1));
                Assertions.assertEquals(null, row.getObject(2));
            }
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn drop check mvcc_test_reject_segment_abort");
            }
        }
        assertTrue(segments.abortReservedSlot(owner, 101, writer.txId(), writer.deadlineMillis()));
        Assertions.assertEquals(101, segments.advancePublished(10));
    }

    /**
     * Another recovery example is added to the same timestamp when the centre COMMITTED slot is durable storage but shard is not materialization.
     */
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
        Assertions.assertEquals(101, segments.currentPublished());
        Assertions.assertEquals(TxStatus.COMMITTED, restartedShard.participantRecord(writer.txId()).orElseThrow().status());
        assertArrayEquals(new byte[]{9}, restartedShard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "segment:recovered").orElseThrow().value());
        assertTrue(segments.snapshotExpiry(writer.txId()).isEmpty());
        assertTrue(recovery.recover(writer.txId()));
    }

    /**
     * Durable Center receipt shall be bound to start watermark, participant and control authority, which cannot replace the other transaction materialization.
     */
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
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> segments.decideCommittedWithProof(owner,
                        101, writer.txId(), writer.startTs() + 1, writer.deadlineMillis(), Set.of("a"))).errorCode());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> new MysqlStorageAdapter("b", dataSource, metadata)
                        .commitDecided(proof, List.of(mutation), true)).errorCode());
        try (
                HikariDataSource anotherPool = (HikariDataSource) MysqlDataSourceFactory.create(
                        MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())) {
            var otherAuthority = new MysqlTransactionMetadataStore(anotherPool);
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> new MysqlStorageAdapter("a", dataSource,
                            otherAuthority).commitDecided(proof, List.of(mutation), true)).errorCode());
        }
        shard.commitDecided(proof, List.of(mutation), true);
        Assertions.assertEquals(TxStatus.COMMITTED, shard.participantRecord(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(101, segments.advancePublished(10));
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "segment:proof:owned").orElseThrow().value());
    }

    /**
     * When an urbaner crash leaves a slot with no transaction identifier, the lease can still be found and cleaned from central metadata after its expiry.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(4, segments.sealExpiredSegment(owner.firstTs()));
        var recovery = new SegmentPublicationRecoveryManager(segments, metadata, Map.of("a", shard));
        assertTrue(recovery.recover(writer.txId()));
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(writer.txId()).orElseThrow().status());
        assertTrue(segments.findByTxId(writer.txId()).isEmpty());
        Assertions.assertEquals(104, segments.advancePublished(10));
    }

    /**
     * A round of background scans is also sealed to expire owner, clear untrucked intent and release consecutive watermark.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(104, segments.currentPublished());
        Assertions.assertEquals(0, runner.transactionStats().failureCount());
        Assertions.assertEquals(0, runner.scanStats().failureCount());
    }

    /**
     * Centre decision but not materialization slot found by another background instance and commit is the same version timestamp.
     */
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
        Assertions.assertEquals(TxStatus.COMMITTED, restartedShard.participantRecord(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(101, restartedLedger.currentPublished());
        assertArrayEquals(new byte[]{5}, restartedShard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "segment:runner-commit").orElseThrow().value());
        Assertions.assertEquals(0, runner.transactionStats().failureCount());
    }

    /**
     * When valid transaction lease is still held by foreground, background may not pre-empt intent or seal owner.
     */
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
        Assertions.assertEquals(0, runner.recoverOnce(10));
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ACTIVE, shard.participantRecord(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(100, segments.currentPublished());
    }

    /**
     * When central metadata states that commit is already available, and there are no slots, the recovery only reports unknown results without blind termination.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(0, runner.recoverOnce(10));
        Assertions.assertEquals(1, runner.transactionStats().failureCount());
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                ((MvccException) runner.lastTransactionFailure()).errorCode());
        Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        Assertions.assertEquals(100, segments.currentPublished());
    }

    /**
     * Independent background connection pool completes the materialization cycle after foreground exit and advances durable watermark.
     *
     * @throws Exception if the operation cannot complete
     */
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

        try (
                HikariDataSource backgroundControl = (HikariDataSource) MysqlDataSourceFactory.create(
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
            try (
                    var scheduler = new SegmentPublicationMaintenanceScheduler(runner, backgroundLedger,
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
                Assertions.assertEquals(TxStatus.COMMITTED, shard.participantRecord(writer.txId()).orElseThrow().status());
                Assertions.assertEquals(101, segments.currentPublished());
                assertTrue(scheduler.recoveryStats().runCount() > 0);
                assertTrue(scheduler.publicationStats().runCount() > 0);
                assertTrue(scheduler.gcStats().runCount() > 0);
                Assertions.assertEquals(0, scheduler.recoveryStats().failureCount());
                Assertions.assertEquals(0, scheduler.publicationStats().failureCount());
                Assertions.assertEquals(0, scheduler.gcStats().failureCount());
            }
        }
    }

    /**
     * GC Waiting for old RR reader and by slot, owner order collection, unable to cross durable safe point.
     */
    @Test
    void segmentedGcMustRespectSnapshotAndTerminalPurgeOrder() {
        var segments = new MysqlSegmentPublicationLedger(dataSource);
        segments.activateFromLedger();
        var reader = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        var first = segments.reserveSegment("owner-a", "reservation-a", 4, 60_000);
        Assertions.assertEquals(4, segments.sealOwnedSegment(first));
        Assertions.assertEquals(104, segments.advancePublished(10));
        Assertions.assertEquals(99, segments.advanceSafePoint());
        segments.releaseSnapshot(reader.txId());
        Assertions.assertEquals(104, segments.advanceSafePoint());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> segments.purgeTerminalSlots(105, Long.MAX_VALUE, 10)).errorCode());
        Assertions.assertEquals(3, segments.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        Assertions.assertEquals(0, segments.purgeSealedSegments(104, Long.MAX_VALUE, 10));
        var second = segments.reserveSegment("owner-b", "reservation-b", 1, 60_000);
        Assertions.assertEquals(1, segments.sealOwnedSegment(second));
        Assertions.assertEquals(105, segments.advancePublished(10));
        Assertions.assertEquals(105, segments.advanceSafePoint());
        Assertions.assertEquals(1, segments.purgeTerminalSlots(105, Long.MAX_VALUE, 10));
        Assertions.assertEquals(1, segments.purgeSealedSegments(105, Long.MAX_VALUE, 10));
    }

    /**
     * Background GC completes the entire phase of shard, centre, trough and owner before updating the target on the same goal safe point.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(99, gc.currentSafePoint());
        try (
                Connection connection = dataSource.getConnection(); Statement count = connection.createStatement();
                ResultSet row = count.executeQuery("select count(*) from mvcc_publication_segment_slot_v1")) {
            assertTrue(row.next());
            Assertions.assertEquals(4, row.getLong(1));
        }
        segments.releaseSnapshot(reader.txId());
        gc.gcOnce(10);
        Assertions.assertEquals(104, gc.currentSafePoint());
        try (
                Connection connection = dataSource.getConnection(); Statement count = connection.createStatement();
                ResultSet row = count.executeQuery("select count(*) from mvcc_publication_segment_slot_v1")) {
            assertTrue(row.next());
            Assertions.assertEquals(1, row.getLong(1));
        }
        var second = segments.reserveSegment("owner-b", "reservation-b", 1, 60_000);
        segments.sealOwnedSegment(second);
        segments.advancePublished(10);
        gc.gcOnce(10);
        Assertions.assertEquals(105, gc.currentSafePoint());
        try (
                Connection connection = dataSource.getConnection(); Statement count = connection.createStatement();
                ResultSet row = count.executeQuery("select count(*) from mvcc_publication_segment_v1")) {
            assertTrue(row.next());
            Assertions.assertEquals(1, row.getLong(1));
        }
    }

    /**
     * Materialization transaction slots are ahead of metadata collection and await the Finnishised retention period to avoid an recovery scan encountered by a cell.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(104, segments.advanceSafePoint());
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_publication_segment_slot_v1 set expires_at = 1 where commit_ts = 101
                """)) {
            age.executeUpdate();
        }
        long cutoff = metadata.currentTimeMillis() - 1_000;
        Assertions.assertEquals(0, segments.purgeTerminalSlots(104, cutoff, 10));
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_txn_finalized set finalized_at = 1 where tx_id = ?
                """)) {
            age.setLong(1, writer.txId());
            age.executeUpdate();
        }
        Assertions.assertEquals(1, segments.purgeTerminalSlots(104, cutoff, 10));
        Assertions.assertEquals(1, segments.purgeFinalizedMetadata(104, cutoff, 10));
        assertTrue(metadata.get(writer.txId()).isEmpty());
        assertTrue(segments.findUnfinalizedTerminal(0, 10).isEmpty());
    }

    /**
     * Begin has taken the share fence but GC must wait and keep the snapshot when lease is not set.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcMustWaitForSharedBeginFenceBeforeAdvancingSafePoint() throws Exception {
        try (
                Connection reader = dataSource.getConnection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            reader.setAutoCommit(false);
            try (
                    PreparedStatement lock = reader.prepareStatement("""
                            select published_ts from mvcc_publication_state_v1
                            where singleton_id = 1 for share
                            """);
                    ResultSet result = lock.executeQuery()) {
                assertTrue(result.next());
                Assertions.assertEquals(100, result.getLong(1));
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
            Assertions.assertEquals(99, advancing.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(100, ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false).startTs());
        }
    }

    /**
     * Read-only transaction released lease before safe point allowed to advance to the currently published watermark.
     */
    @Test
    void releasedReadOnlySnapshotNoLongerPinsSafePoint() {
        var reader = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        Assertions.assertEquals(reader.deadlineMillis(), ledger.snapshotExpiry(reader.txId()).orElseThrow());
        Assertions.assertEquals(99, ledger.advanceSafePoint());
        ledger.releaseSnapshot(reader.txId());
        ledger.releaseSnapshot(reader.txId());
        assertTrue(ledger.snapshotExpiry(reader.txId()).isEmpty());
        Assertions.assertEquals(100, ledger.advanceSafePoint());
    }

    /**
     * Coordinator lease protects the oldest snapshot and only current algebras can raise the lower boundary and release protection.
     */
    @Test
    void coordinatorLeaseMustFenceStaleGenerationAndProtectGcBoundary() {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        Assertions.assertEquals(100, coordinator.readTs());
        Assertions.assertEquals(99, ledger.advanceSafePoint());
        var writer = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        long commitTs = ledger.commitPreparedIntents(writer.txId(), writer.deadlineMillis(), Set.of("a")).commitTs();
        metadata.markFinalized(writer.txId());
        ledger.releaseSnapshot(writer.txId());
        Assertions.assertEquals(99, ledger.advanceSafePoint());

        var restarted = new MysqlPublicationLedger(dataSource);
        var renewed = restarted.renewCoordinatorLease(coordinator.coordinatorId(), coordinator.generation(),
                commitTs, 60_000);
        Assertions.assertEquals(coordinator.generation() + 1, renewed.generation());
        Assertions.assertEquals(commitTs, renewed.readTs());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewCoordinatorLease(
                        coordinator.coordinatorId(), coordinator.generation(), commitTs, 60_000)).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.releaseCoordinatorLease(
                        coordinator.coordinatorId(), coordinator.generation())).errorCode());
        Assertions.assertEquals(100, ledger.advanceSafePoint());
        assertTrue(ledger.releaseCoordinatorLease(renewed.coordinatorId(), renewed.generation()));
        assertFalse(ledger.releaseCoordinatorLease(renewed.coordinatorId(), renewed.generation()));
        Assertions.assertEquals(commitTs, ledger.advanceSafePoint());
    }

    /**
     * Expiry coordinator lease cannot renew or continue to protect snapshot after it has been removed from GC.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredCoordinatorLeaseMustBeFencedAfterGc() throws Exception {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        try (Connection connection = dataSource.getConnection(); PreparedStatement age = connection.prepareStatement("""
                update mvcc_coordinator_snapshot_lease_v1 set expires_at = 0 where coordinator_id = ?
                """)) {
            age.setLong(1, coordinator.coordinatorId());
            age.executeUpdate();
        }
        Assertions.assertEquals(1, ledger.expireCoordinatorLeases(10));
        assertTrue(ledger.coordinatorLease(coordinator.coordinatorId()).isEmpty());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.renewCoordinatorLease(
                        coordinator.coordinatorId(), coordinator.generation(), 100, 60_000)).errorCode());
        Assertions.assertEquals(100, ledger.advanceSafePoint());
    }

    /**
     * Foreground When holding the main lease key lock and then deleting it, the background due time cleanup cannot first take over the expired index to cause a reverse death lock.
     *
     * @throws Exception if the operation cannot complete
     */
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
                    try (
                            PreparedStatement release = foreground.prepareStatement(
                                    "delete from mvcc_snapshot_lease_v2 where tx_id = ?")) {
                        release.setLong(1, snapshot.txId());
                        Assertions.assertEquals(1, release.executeUpdate());
                    }
                    foreground.commit();
                } finally {
                    foreground.rollback();
                }
                Assertions.assertEquals(0, gc.get(5, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * Snapshot with no single pen is protected by coordinator lease and first written to raise idempotent to independent lease.
     */
    @Test
    void coordinatorSnapshotPromotionMustPreserveGcAndSeparateTransactionIds() {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        var view = ledger.snapshotUnderCoordinatorLease(coordinator.coordinatorId(),
                coordinator.generation(), 30_000);
        Assertions.assertEquals(100, view.readTs());
        Assertions.assertEquals(coordinator.readTs(), view.leaseReadTs());
        long localTxId = -1234;
        assertTrue(ledger.snapshotExpiry(localTxId).isEmpty());
        ledger.promoteCoordinatorSnapshot(localTxId, view.readTs(), view.deadlineMillis(),
                coordinator.coordinatorId(), coordinator.generation());
        ledger.promoteCoordinatorSnapshot(localTxId, view.readTs(), view.deadlineMillis(),
                coordinator.coordinatorId(), coordinator.generation());
        Assertions.assertEquals(view.deadlineMillis(), ledger.snapshotExpiry(localTxId).orElseThrow());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.promoteCoordinatorSnapshot(localTxId,
                        view.readTs(), view.deadlineMillis() - 1000,
                        coordinator.coordinatorId(), coordinator.generation())).errorCode());
        assertTrue(ledger.releaseCoordinatorLease(coordinator.coordinatorId(), coordinator.generation()));
        Assertions.assertEquals(99, ledger.advanceSafePoint());
        ledger.releaseSnapshot(localTxId);
        Assertions.assertEquals(100, ledger.advanceSafePoint());
        assertTrue(ledger.begin(IsolationLevel.READ_COMMITTED, 60_000, false).txId() > 0);
    }

    /**
     * The initial lease and complete participant must be dropped with the transaction disk, responding to retry idempotent and conflict cannot leave a half-state.
     */
    @Test
    void coordinatorPromotionAndMetadataRegistrationMustBeAtomic() {
        var coordinator = ledger.acquireCoordinatorLease(60_000);
        var view = ledger.snapshotUnderCoordinatorLease(coordinator.coordinatorId(),
                coordinator.generation(), 30_000);
        long txId = -1234;
        var record = ledger.promoteAndRegisterCoordinatorSnapshot(txId, view.readTs(),
                view.deadlineMillis(), coordinator.coordinatorId(), coordinator.generation(),
                IsolationLevel.REPEATABLE_READ, Set.of("a"));
        Assertions.assertEquals(TxStatus.ACTIVE, record.status());
        Assertions.assertEquals(Set.of("a"), metadata.get(txId).orElseThrow().participants());
        Assertions.assertEquals(view.deadlineMillis(), ledger.snapshotExpiry(txId).orElseThrow());
        var replay = ledger.promoteAndRegisterCoordinatorSnapshot(txId, view.readTs(),
                view.deadlineMillis(), coordinator.coordinatorId(), coordinator.generation(),
                IsolationLevel.REPEATABLE_READ, Set.of("a"));
        Assertions.assertEquals(record.txId(), replay.txId());
        Assertions.assertEquals(record.startTs(), replay.startTs());
        Assertions.assertEquals(record.participants(), replay.participants());
        Assertions.assertEquals(record.status(), replay.status());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.promoteAndRegisterCoordinatorSnapshot(txId,
                        view.readTs(), view.deadlineMillis(), coordinator.coordinatorId(),
                        coordinator.generation(), IsolationLevel.REPEATABLE_READ, Set.of("b"))).errorCode());

        long conflictId = -1235;
        metadata.createWithParticipants(conflictId, view.readTs(), IsolationLevel.REPEATABLE_READ, Set.of("b"));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.promoteAndRegisterCoordinatorSnapshot(conflictId,
                        view.readTs(), view.deadlineMillis(), coordinator.coordinatorId(),
                        coordinator.generation(), IsolationLevel.REPEATABLE_READ, Set.of("a"))).errorCode());
        assertTrue(ledger.snapshotExpiry(conflictId).isEmpty());
    }

    /**
     * The old algebra cannot continue to issue snapshot without a single lease after renewing the lease.
     */
    @Test
    void staleCoordinatorGenerationMustNotIssueSnapshots() {
        var first = ledger.acquireCoordinatorLease(60_000);
        var second = ledger.renewCoordinatorLease(first.coordinatorId(), first.generation(), 100, 60_000);
        var protectedView = ledger.snapshotUnderCoordinatorLease(second.coordinatorId(),
                second.generation(), 30_000);
        ledger.assertCoordinatorSnapshot(second.coordinatorId(), second.generation(),
                protectedView.readTs(), protectedView.deadlineMillis(), -1);
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.assertCoordinatorSnapshot(
                        first.coordinatorId(), first.generation(), protectedView.readTs(),
                        protectedView.deadlineMillis(), -1)).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                assertThrows(MvccException.class, () -> ledger.snapshotUnderCoordinatorLease(
                        first.coordinatorId(), first.generation(), 30_000)).errorCode());
        Assertions.assertEquals(100, ledger.snapshotUnderCoordinatorLease(second.coordinatorId(),
                second.generation(), 30_000).readTs());
    }

    /**
     * Batch begin shares an independent transaction ID share of snapshot, all lease commit before allowing GC to advance.
     */
    @Test
    void batchBeginMustPersistDistinctLeasesAtOnePublishedSnapshot() {
        var reservations = ledger.beginBatch(8, 60_000);
        Assertions.assertEquals(8, reservations.size());
        Assertions.assertEquals(8, reservations.stream().mapToLong(value -> value.txId()).distinct().count());
        long deadline = reservations.getFirst().deadlineMillis();
        for (var reservation : reservations) {
            Assertions.assertEquals(100, reservation.startTs());
            Assertions.assertEquals(deadline, reservation.deadlineMillis());
            assertFalse(reservation.metadataPersisted());
            Assertions.assertEquals(deadline, ledger.snapshotExpiry(reservation.txId()).orElseThrow());
        }
        Assertions.assertEquals(99, ledger.advanceSafePoint());
        reservations.forEach(value -> ledger.releaseSnapshot(value.txId()));
        Assertions.assertEquals(100, ledger.advanceSafePoint());
    }

    /**
     * All new snapshot s in the batch must overwrite the commit when commit is confirmed before batch begin.
     */
    @Test
    void batchBeginAfterAcknowledgedCommitMustReadNewPublishedWatermark() {
        var writer = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(writer.txId(), Set.of("a"));
        long commitTs = ledger.commitPreparedIntents(writer.txId(), writer.deadlineMillis(), Set.of("a")).commitTs();
        var reservations = ledger.beginBatch(4, 60_000);
        assertTrue(reservations.stream().allMatch(value -> value.startTs() == commitTs));
    }

    /**
     * Batch lease is still required to return all transaction IDs that are actually generated when a multi-main number step is not one.
     */
    @Test
    void batchBeginMustRespectMysqlAutoIncrementStep() {
        try (
                HikariDataSource incremented = (HikariDataSource) MysqlDataSourceFactory.create(
                        MYSQL.getJdbcUrl() + "?sessionVariables=auto_increment_increment=2",
                        MYSQL.getUsername(), MYSQL.getPassword())) {
            var alternate = new MysqlPublicationLedger(incremented);
            var reservations = alternate.beginBatch(4, 60_000);
            Assertions.assertEquals(4, reservations.size());
            Assertions.assertEquals(4, reservations.stream().mapToLong(value -> value.txId()).distinct().count());
            for (int index = 1; index < reservations.size(); index++) {
                Assertions.assertEquals(2, reservations.get(index).txId() - reservations.get(index - 1).txId());
            }
            for (var reservation : reservations) {
                Assertions.assertEquals(reservation.deadlineMillis(),
                        alternate.snapshotExpiry(reservation.txId()).orElseThrow());
            }
        }
    }

    /**
     * The take-over scan expires only returns a reservation that is still PENDING and retains transaction identity.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertEquals(started.txId(), candidates.getFirst().txId());
        Assertions.assertEquals(commitTs, candidates.getFirst().commitTs());
        ledger.decideAbortedAfterFencing(started.txId(), commitTs);
        assertTrue(ledger.findExpiredPending(System.currentTimeMillis(), 0, 10).isEmpty());
    }

    /**
     * An existing centre rollback refillable book, but single shard local to be confirmed timestamp may not be miscalculated rollback.
     */
    @Test
    void recoveryMayReplayAbortedMetadataButMustFenceLocalPendingCommit() {
        var aborted = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(aborted.txId(), Set.of("a"));
        long abortedTs = ledger.reserveCommit(aborted.txId(), aborted.deadlineMillis());
        metadata.markAborted(aborted.txId());
        ledger.decideAbortedAfterFencing(aborted.txId(), abortedTs);
        Assertions.assertEquals(abortedTs, ledger.advancePublished(16));

        var localPending = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(localPending.txId(), Set.of("a"));
        long pendingTs = ledger.reserveCommit(localPending.txId(), localPending.deadlineMillis());
        metadata.markLocalCommitPending(localPending.txId(), pendingTs);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.decideAbortedAfterFencing(
                        localPending.txId(), pendingTs)).errorCode());
        Assertions.assertEquals(abortedTs, ledger.advancePublished(16));
        var proof = new ParticipantRecord(localPending.txId(), localPending.startTs(), TxStatus.COMMITTED, pendingTs);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.completeLocalCommitAfterFencing(
                        proof, pendingTs + 1, "a")).errorCode());
        ledger.completeLocalCommitAfterFencing(proof, pendingTs, "a");
        ledger.completeLocalCommitAfterFencing(proof, pendingTs, "a");
        Assertions.assertEquals(pendingTs, ledger.advancePublished(16));
    }

    /**
     * A full participant metadata must be available before setting aside recovery to avoid failure to locate an indent.
     */
    @Test
    void reservationRequiresDurableParticipantRegistration() {
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, false);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveCommit(started.txId(),
                        started.deadlineMillis())).errorCode());
        metadata.createWithParticipants(started.txId(), started.startTs(),
                IsolationLevel.REPEATABLE_READ, Set.of("a", "b"));
        assertTrue(ledger.reserveCommit(started.txId(), started.deadlineMillis()) > started.startTs());
    }

    /**
     * Single shard candidate time and PENDING account atomic drop, error path cannot consume commit number.
     */
    @Test
    void localCommitReservationMustAtomicallyPersistCandidateAndLedgerSlot() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(first.txId(), Set.of("a"));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveLocalCommit(
                        first.txId(), first.deadlineMillis(), "b")).errorCode());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        long commitTs = ledger.reserveLocalCommit(first.txId(), first.deadlineMillis(), "a");
        Assertions.assertEquals(101, commitTs);
        Assertions.assertEquals(commitTs, ledger.reserveLocalCommit(first.txId(), first.deadlineMillis(), "a"));
        var candidate = metadata.get(first.txId()).orElseThrow();
        Assertions.assertEquals(TxStatus.PREPARING, candidate.status());
        Assertions.assertEquals(commitTs, candidate.commitTs().longValue());
        Assertions.assertEquals(PublicationEntryStatus.PENDING, ledger.findByTxId(first.txId()).orElseThrow().status());
        Assertions.assertEquals(100, ledger.currentPublished());
    }

    /**
     * Batch local presets the transaction identity allocation for an independent timestamp, with any error making the whole batch open to rollback.
     */
    @Test
    void localReservationBatchMustRemainAtomicAndReplayable() {
        var first = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var second = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        var third = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        for (var started : List.of(first, second, third)) {
            metadata.registerParticipants(started.txId(), Set.of("a"));
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.reserveLocalCommitBatch(List.of(
                        new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                        new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "wrong"))))
                        .errorCode());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        List<Long> reserved = ledger.reserveLocalCommitBatch(List.of(
                new PublicationLocalCommitRequest(third.txId(), third.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(first.txId(), first.deadlineMillis(), "a"),
                new PublicationLocalCommitRequest(second.txId(), second.deadlineMillis(), "a")));
        Assertions.assertEquals(List.of(103L, 101L, 102L), reserved);
        Assertions.assertEquals(101, ledger.reserveLocalCommit(first.txId(), first.deadlineMillis(), "a"));
        Assertions.assertEquals(100, ledger.currentPublished());
        for (var started : List.of(first, second, third)) {
            Assertions.assertEquals(TxStatus.PREPARING, metadata.get(started.txId()).orElseThrow().status());
            Assertions.assertEquals(PublicationEntryStatus.PENDING,
                    ledger.findByTxId(started.txId()).orElseThrow().status());
        }
    }

    /**
     * Local commit proof delivered in an unsophisticated order becomes a continuous visible prefix in the same control transaction.
     */
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
        Assertions.assertEquals(List.of(true, true, true), published);
        Assertions.assertEquals(commits.getLast().longValue(), ledger.currentPublished());
        for (var started : List.of(first, second, third)) {
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
            Assertions.assertEquals(PublicationEntryStatus.COMMITTED,
                    ledger.findByTxId(started.txId()).orElseThrow().status());
        }
    }

    /**
     * Earlier PENDING prevented batch local terminal state from being published and error proved that the whole batch decision atomic rollback was made.
     */
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
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> ledger.completeLocalCommitBatch(List.of(
                        firstProof, new PublicationLocalCommitProof(secondProof.local(), secondProof.commitTs(),
                                "wrong"))))
                        .errorCode());
        Assertions.assertEquals(TxStatus.PREPARING, metadata.get(first.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.PENDING, ledger.findByTxId(first.txId()).orElseThrow().status());
        Assertions.assertEquals(List.of(false, false), ledger.completeLocalCommitBatch(List.of(firstProof, secondProof)));
        Assertions.assertEquals(100, ledger.currentPublished());
        metadata.markAborted(pending.txId());
        ledger.decideAbortedAfterFencing(pending.txId(), low);
        Assertions.assertEquals(commits.getLast().longValue(), ledger.advancePublished(16));
    }

    /**
     * When the first slot is finally completed by a single pen to decision, the batch suffix that was previously commit must also be published.
     */
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
        Assertions.assertEquals(List.of(false, false), ledger.completeLocalCommitBatch(later));
        Assertions.assertEquals(100, ledger.currentPublished());
        var earliest = new ParticipantRecord(first.txId(), first.startTs(), TxStatus.COMMITTED, commits.get(0));
        assertTrue(ledger.completeLocalCommitAfterFencing(earliest, commits.get(0), "a"));
        Assertions.assertEquals(commits.getLast().longValue(), ledger.currentPublished());
        ledger.awaitPublished(third.txId(), commits.getLast(), 1000);
    }

    /**
     * Lease late commit decision after due has to fail before recovery can first take terminal state and clean participant.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> ledger.decideCommitted(started.txId(), commitTs,
                        Set.of("a"))).errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(100, ledger.advancePublished(16));
        metadata.markAborted(started.txId());
        ledger.decideAbortedAfterFencing(started.txId(), commitTs);
        Assertions.assertEquals(commitTs, ledger.advancePublished(16));
    }

    /**
     * Expiration intent Releases PENDING watermark only after ABORTED decision and participant Fencing.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.ABORTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status());
        Assertions.assertEquals(commitTs, ledger.currentPublished());
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
    }

    /**
     * Lease that has not been set aside since prewrite, even if it is first deleted by GC, the recovery must clean up participant and do not create a missing book number.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(1, ledger.expireSnapshots(10));
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
        Assertions.assertEquals(List.of(started.txId()), ledger.findExpiredUnreserved(System.currentTimeMillis(), 0, 10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        Assertions.assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status());
        assertTrue(ledger.findByTxId(started.txId()).isEmpty());
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
        Assertions.assertEquals(100, ledger.currentPublished());
    }

    /**
     * Negative transaction ID assigned by the coordinator can still be detected and aborted by recovery cursor after lease expires.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(List.of(txId), ledger.findExpiredUnreserved(System.currentTimeMillis(), Long.MIN_VALUE, 10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        Assertions.assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(txId).orElseThrow().status());
    }

    /**
     * Periodic GC removes expired lease first, the background scan still requires automatic recovery unset participant.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(1, ledger.expireSnapshots(10));
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
            Assertions.assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status(), () -> "recovery ticks=" + scheduler.recoveryStats().runCount()
                    + ", tick failures=" + scheduler.recoveryStats().failureCount()
                    + ", scans=" + runner.scanStats().runCount()
                    + ", scan failures=" + runner.scanStats().failureCount()
                    + ", transactions=" + runner.transactionStats().runCount()
                    + ", transaction failures=" + runner.transactionStats().failureCount()
                    + ", gc ticks=" + scheduler.gcStats().runCount()
                    + ", gc failures=" + scheduler.gcStats().failureCount());
            Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status(), () -> "transaction failures=" + runner.transactionStats().failureCount()
                    + ", last failure=" + runner.lastTransactionFailure());
            assertTrue(scheduler.recoveryStats().runCount() > 0);
            Assertions.assertEquals(0, scheduler.recoveryStats().failureCount());
        }
    }

    /**
     * Transaction, which has been suspended at the centre but is not rollback, must be completed by a periodic detector after the lease removal.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(1, ledger.expireSnapshots(10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        Assertions.assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        Assertions.assertEquals(TxStatus.ABORTED, shard.participantRecord(started.txId()).orElseThrow().status());
        assertTrue(ledger.findExpiredUnreserved(System.currentTimeMillis(), 0, 10).isEmpty());
    }

    /**
     * The decision centre has been released with a stand-alone recovery example materialization and the same committeeTs.
     */
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
        Assertions.assertEquals(1, new PublicationRecoveryRunner(new MysqlPublicationLedger(dataSource), recovery,
                restartedStore).recoverOnce(10));
        Assertions.assertEquals(commitTs, ledger.currentPublished());
        assertArrayEquals(new byte[]{7}, restartedShard.get(new ReadContext(9999, commitTs,
                IsolationLevel.READ_COMMITTED), "recovery:commit").orElseThrow().value());
        assertTrue(ledger.snapshotExpiry(started.txId()).isEmpty());
    }

    /**
     * The terminal state recovery scan returns only the new book decision and takes over the old protocol metadata that were left before the move.
     */
    @Test
    void terminalRecoveryScanMustExcludeLegacyMetadataWithoutLedgerEntry() {
        metadata.createCommitted(50, 50, IsolationLevel.REPEATABLE_READ, Set.of("a"), 60);
        var started = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(started.txId(), Set.of("a"));
        ledger.commitPreparedIntents(started.txId(), started.deadlineMillis(), Set.of("a"));
        Assertions.assertEquals(List.of(started.txId()), ledger.findUnfinalizedTerminal(0, 10));
        metadata.markFinalized(started.txId());
        assertTrue(ledger.findUnfinalizedTerminal(0, 10).isEmpty());
    }

    /**
     * Negative transaction ID of decision must also enter terminal state materialization cursor.
     */
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
        Assertions.assertEquals(List.of(txId), ledger.findUnfinalizedTerminal(Long.MIN_VALUE, 10));
        var recovery = new PublicationRecoveryManager(ledger, metadata, java.util.Map.of("a", shard));
        Assertions.assertEquals(1, new PublicationRecoveryRunner(ledger, recovery, metadata).recoverOnce(10));
        Assertions.assertEquals(TxStatus.COMMITTED, shard.participantRecord(txId).orElseThrow().status());
        assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(9999, commitTs,
                IsolationLevel.READ_COMMITTED), "recovery:negative-committed").orElseThrow().value());
    }

    /**
     * Local When the physical commit post-centre is PREPARING, recovery shall be certified using participant COMMITTED.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.COMMITTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        Assertions.assertEquals(commitTs, ledger.currentPublished());
        assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(9999, commitTs,
                IsolationLevel.READ_COMMITTED), "recovery:local").orElseThrow().value());
    }

    /**
     * Local Physical commit has not yet taken place, recovery retains an independent txId for the real start snapshot and rollback.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(TxStatus.ABORTED, metadata.get(started.txId()).orElseThrow().status());
        Assertions.assertEquals(PublicationEntryStatus.ABORTED, ledger.findByTxId(started.txId()).orElseThrow().status());
        var local = shard.participantRecord(started.txId()).orElseThrow();
        Assertions.assertEquals(TxStatus.ABORTED, local.status());
        Assertions.assertEquals(started.startTs(), local.startTs());
        Assertions.assertEquals(commitTs, ledger.currentPublished());
    }

    /**
     * The account book terminal state must wait for central metadata collection and strictly predate durable safe point before it can be deleted.
     */
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
        Assertions.assertEquals(high, ledger.advancePublished(16));
        ledger.releaseSnapshot(first.txId());
        ledger.releaseSnapshot(second.txId());
        metadata.markFinalized(first.txId());
        metadata.markFinalized(second.txId());
        Assertions.assertEquals(high, ledger.advanceSafePoint());
        Assertions.assertEquals(0, ledger.purgeTerminal(high, Long.MAX_VALUE, 10));
        assertTrue(metadata.purgeFinalized(high, Long.MAX_VALUE, 10) >= 1);
        Assertions.assertEquals(1, ledger.purgeTerminal(high, Long.MAX_VALUE, 10));
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        assertTrue(ledger.findByTxId(second.txId()).isPresent());
    }

    /**
     * GC The organization requires collection to have completed the central metadata before processing an terminal state account book earlier than safe point.
     *
     * @throws Exception if the operation cannot complete
     */
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
        Assertions.assertEquals(0, gc.gcOnce(16));
        Assertions.assertEquals(high, gc.currentSafePoint());
        assertTrue(ledger.findByTxId(first.txId()).isEmpty());
        assertTrue(ledger.findByTxId(second.txId()).isPresent());
    }

    /**
     * When GC is implemented in multiple rounds, the center collection shall not use higher safe point s that have not reached all shard.
     */
    @Test
    void publicationGcCycleMustHoldOneSafePointUntilEveryShardAdvances() {
        List<String> calls = new ArrayList<>();
        var clock = new AtomicLong();
        var gc = new PublicationGcManager(ledger, metadata,
                Map.of("a", recordingGcAdapter("a", calls), "b", recordingGcAdapter("b", calls)),
                16, 0, 1, () -> clock.getAndAdd(2_000_000L));
        Assertions.assertEquals(0, gc.gcOnce(16));
        Assertions.assertEquals(List.of("a:100"), calls);

        var later = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
        metadata.registerParticipants(later.txId(), Set.of("a"));
        long laterCommit = ledger.commitPreparedIntents(later.txId(), later.deadlineMillis(), Set.of("a")).commitTs();
        metadata.markFinalized(later.txId());
        ledger.releaseSnapshot(later.txId());
        Assertions.assertEquals(laterCommit, ledger.advanceSafePoint());

        gc.gcOnce(16);
        Assertions.assertEquals(List.of("a:100", "b:100"), calls);
        gc.gcOnce(16);
        gc.gcOnce(16);
        gc.gcOnce(16);
        Assertions.assertEquals(List.of("a:100", "b:100", "a:" + laterCommit), calls);
    }

    /**
     * Shard GC has to be retry the same watermark and shard after failure, not able to jump to central metadata collection.
     */
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
        Assertions.assertEquals(List.of("a:100"), calls);
        Assertions.assertEquals(0, gc.gcOnce(16));
        Assertions.assertEquals(List.of("a:100", "a:100"), calls);
    }

    /**
     * Only GC received watermark is recorded, and other adapter operations should be considered as calling errors in this test.
     *
     * @param shardId logical shard identifier
     * @param calls calls
     * @return the operation result
     */
    private static StorageAdapter recordingGcAdapter(final String shardId, final List<String> calls) {
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

    /**
     * Only two durable storage lease s have been shortened in the test, and the simulation independent process is taken over after the deadline.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @throws Exception if the operation cannot complete
     */
    private void agePending(final long txId, final long commitTs) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
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

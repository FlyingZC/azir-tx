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

package com.flyingzc.azir.control;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.gc.GcManager;
import com.flyingzc.azir.gc.mysql.MysqlSafePointStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.tso.mysql.MysqlSegmentTimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import com.flyingzc.azir.txn.TransactionManager;
import com.flyingzc.azir.txn.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * MySQL issues the integrated return of barrier, durable storage sequences and merge start path.
 */
@Testcontainers
class MysqlPublicationIntegrationTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("publication").withUsername("test").withPassword("test");

    private HikariDataSource firstPool;

    private HikariDataSource secondPool;

    private MysqlTransactionMetadataStore first;

    private MysqlTransactionMetadataStore second;

    private MysqlTimestampOracle tso;

    private MysqlSafePointStore safePoint;

    /**
     * Two standalone connection pool s bound to the same database and emptyed a book to test their control sheets.
     *
     * @throws SQLException if the operation cannot complete
     */
    @BeforeEach
    void initialize() throws SQLException {
        firstPool = pool();
        secondPool = pool();
        first = new MysqlTransactionMetadataStore(firstPool, 5);
        second = new MysqlTransactionMetadataStore(secondPool, 5);
        first.initialize();
        second.initialize();
        tso = new MysqlTimestampOracle(firstPool);
        tso.initialize(100);
        safePoint = new MysqlSafePointStore(firstPool);
        safePoint.initialize(0);
        try (Connection connection = firstPool.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_snapshot_lease");
            statement.executeUpdate("delete from mvcc_txn_finalized");
            statement.executeUpdate("delete from mvcc_txn");
            statement.executeUpdate("update mvcc_tso set current_value = 100");
            statement.executeUpdate("update mvcc_safe_point set safe_point = 0");
        }
    }

    /**
     * Each test releases its own connection pool.
     */
    @AfterEach
    void close() {
        if (firstPool != null) {
            firstPool.close();
        }
        if (secondPool != null) {
            secondPool.close();
        }
    }

    /**
     * The validate merge must use the database clock; the deadline is equal and the query cannot modify the original lease.
     */
    @Test
    void combinedLeaseValidationHonorsDatabaseClockAndOriginalDeadline() {
        long deadline = 2_000_000_000_000L;
        assertFalse(first.isSnapshotLeaseValid(1, deadline));
        first.registerSnapshot(1, 1, deadline);
        first.withPublicationLock(() -> {
            try (
                    Connection borrowed = MysqlConnectionContext.connection(firstPool);
                    var statement = borrowed.createStatement()) {
                try {
                    statement.execute("set timestamp = 1999999999");
                    assertTrue(first.isSnapshotLeaseValid(1, deadline));
                    assertFalse(first.isSnapshotLeaseValid(1, deadline + 1));
                    statement.execute("set timestamp = 2000000000");
                    assertFalse(first.isSnapshotLeaseValid(1, deadline));
                    Assertions.assertEquals(deadline, first.snapshotExpiry(1).orElseThrow());
                } finally {
                    // Return the recovery real database -hours before the lock, avoiding subsequent examples of contamination.
                    statement.execute("set timestamp = 0");
                }
            } catch (final SQLException ex) {
                throw new IllegalStateException(ex);
            }
            return null;
        });
        second.releaseSnapshot(1);
        assertFalse(first.isSnapshotLeaseValid(1, deadline));
    }

    /**
     * The released MySQL lease must be read and rejected at the transaction manager 's commit entrance, not only by an verify storage tool.
     */
    @Test
    void releasedMysqlLeaseRejectsReadAndCommit() {
        try (var manager = new TransactionManager(tso, first, TransactionOptions.defaults(), Map.of())) {
            var transaction = manager.begin(IsolationLevel.REPEATABLE_READ);
            second.releaseSnapshot(transaction.txId());
            Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                    assertThrows(MvccException.class, () -> manager.beginReadStatement(transaction)).errorCode());
            Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                    assertThrows(MvccException.class, () -> manager.commit(transaction)).errorCode());
            Assertions.assertEquals(TxStatus.ACTIVE, first.get(transaction.txId()).orElseThrow().status());
            manager.rollback(transaction);
        }
    }

    /**
     * Single shard condition decision covers new records and local slots and ensures cross-example replay use of original commit timestamp.
     */
    @Test
    void singleShardConditionalDecisionPreservesReplayAndPendingTimestamp() {
        first.createWithParticipants(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a"));
        first.markLocalCommitPending(1, 10);
        first.persistSingleShardCommitted(1, "a", 10);
        second.persistSingleShardCommitted(1, "a", 10);
        Assertions.assertEquals(TxStatus.COMMITTED, second.get(1).orElseThrow().status());
        Assertions.assertEquals(10L, second.get(1).orElseThrow().commitTs());
        assertDoesNotThrow(second::assertNoPendingPublications);
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "a", 11));
        Assertions.assertEquals(10L, first.get(1).orElseThrow().commitTs());

        first.createWithParticipants(2, 2, IsolationLevel.READ_COMMITTED, Set.of("a"));
        first.persistSingleShardCommitted(2, "a", 12);
        Assertions.assertEquals(TxStatus.COMMITTED, second.get(2).orElseThrow().status());
        Assertions.assertEquals(Set.of("a"), second.get(2).orElseThrow().participants());
    }

    /**
     * Error timestamp, error participant and aborted state cannot be covered by single shard condition updates.
     */
    @Test
    void singleShardConditionalDecisionRejectsMismatchedOrAbortedState() {
        first.createWithParticipants(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a"));
        first.markLocalCommitPending(1, 10);
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "a", 11));
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "b", 10));
        var pending = first.get(1).orElseThrow();
        Assertions.assertEquals(TxStatus.PREPARING, pending.status());
        Assertions.assertEquals(10L, pending.commitTs());
        Assertions.assertEquals(Set.of("a"), pending.participants());
        first.markAborted(1);
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "a", 10));
        Assertions.assertEquals(TxStatus.ABORTED, first.get(1).orElseThrow().status());
        assertNull(first.get(1).orElseThrow().commitTs());
    }

    /**
     * Database access failed to keep storage abnormal and cannot return valid lease.
     */
    @Test
    void combinedLeaseValidationFailsClosedOnStorageFailure() {
        firstPool.close();
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, () -> first.isSnapshotLeaseValid(1, Long.MAX_VALUE)).errorCode());
    }

    /**
     * Two metadata objects are in the same field barrier, TSO, server clock and safe point only one lock connection.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void sameDatabaseAndSingleConnectionPoolsReusePublicationConnection() throws SQLException {
        var otherTso = new MysqlTimestampOracle(secondPool);
        otherTso.initialize(100);
        try (Connection occupied = secondPool.getConnection()) {
            long result = first.withPublicationLock(() -> second.withPublicationLock(() -> {
                long txId = otherTso.nextTimestamp();
                first.registerSnapshot(txId, txId, first.currentTimeMillis() + 60_000);
                first.create(txId, txId, IsolationLevel.REPEATABLE_READ);
                Assertions.assertEquals(100, new GcManager(first, tso, safePoint, Map.of()).advanceSafePoint());
                Assertions.assertEquals(1, firstPool.getHikariPoolMXBean().getActiveConnections());
                return txId;
            }));
            Assertions.assertEquals(101, result);
            assertFalse(occupied.isClosed());
        }
    }

    /**
     * The back-to-back anomaly does not let the name lock leak with connection back to the pool.
     */
    @Test
    void callbackFailureReleasesDatabaseLock() {
        assertThrows(IllegalArgumentException.class, () -> first.withPublicationLock(() -> {
            first.create(1, 1, IsolationLevel.REPEATABLE_READ);
            throw new IllegalArgumentException("simulated failure");
        }));
        Assertions.assertEquals(1L, second.withPublicationLock(() -> second.get(1).orElseThrow()).txId());
    }

    /**
     * This release failed to close after the lock connection was lost, and the unknown slot durable storage still prevents another instance from reproducing snapshot.
     */
    @Test
    void disconnectedLockConnectionCannotResumePublicationOnAnotherConnection() {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        assertThrows(MvccException.class, () -> first.withPublicationLock(() -> {
            first.markLocalCommitPending(1, 10);
            try (Connection borrowed = MysqlConnectionContext.connection(firstPool)) {
                borrowed.abort(Runnable::run);
            } catch (final SQLException ex) {
                throw new IllegalStateException(ex);
            }
            assertThrows(MvccException.class, () -> first.markCommitted(1, 10));
            assertThrows(MvccException.class, tso::nextTimestamp);
            assertThrows(MvccException.class, () -> first.tryBegin(tso, IsolationLevel.READ_COMMITTED,
                    60_000, false, () -> {
                    }));
            assertThrows(MvccException.class, () -> first.tryCommit(tso, 1, Set.of("a"),
                    () -> {
                    }, ignored -> {
                    }));
            return null;
        }));
        Assertions.assertEquals(TxStatus.PREPARING, second.get(1).orElseThrow().status());
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, second::assertNoPendingPublications).errorCode());
    }

    /**
     * External session explicitly returns the GET_LOCK timeout error when holding an same database name lock.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void databaseLockTimeoutIsExplicit() throws SQLException {
        try (Connection external = secondPool.getConnection(); var statement = external.createStatement()) {
            String name;
            try (var result = statement.executeQuery("select concat('mvcc:pub:', substring(sha2(database(), 256), 1, 48))")) {
                assertTrue(result.next());
                name = result.getString(1);
            }
            try (
                    var lock = external.prepareStatement("select get_lock(?, 0)");
                    var release = external.prepareStatement("select release_lock(?)")) {
                lock.setString(1, name);
                release.setString(1, name);
                try (var result = lock.executeQuery()) {
                    assertTrue(result.next());
                    Assertions.assertEquals(1, result.getInt(1));
                }
                try {
                    var immediate = new MysqlTransactionMetadataStore(firstPool, 0);
                    var error = assertThrows(MvccException.class, () -> immediate.withPublicationLock(() -> 1));
                    Assertions.assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, error.errorCode());
                    assertTrue(error.getMessage().contains("GET_LOCK"));
                    var beginError = assertThrows(MvccException.class, () -> immediate.tryBegin(tso,
                            IsolationLevel.READ_COMMITTED, 60_000, false, () -> {
                            }));
                    Assertions.assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, beginError.errorCode());
                } finally {
                    release.execute();
                }
            }
        }
    }

    /**
     * The control connection must be visibly discarded if the server endroute is left with a session naming lock.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void serverSideProcedureMustDiscardConnectionThatRetainsLock() throws SQLException {
        assertThrows(SQLException.class, () -> MysqlConnectionContext.withPublicationProcedure(firstPool, 1,
                connection -> {
                    try (
                            var statement = connection.createStatement();
                            var result = statement.executeQuery("""
                                    select get_lock(concat('mvcc:pub:', substring(sha2(database(), 256), 1, 48)), 0)
                                    """)) {
                        assertTrue(result.next());
                        Assertions.assertEquals(1, result.getInt(1));
                    }
                    return 1;
                }));
        Assertions.assertEquals(1, second.withPublicationLock(() -> 1));
    }

    /**
     * If the routine leaves a visible database transaction, no connection pool reset autoCommit hidden commit should be used.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void serverSideProcedureMustDiscardConnectionWithOpenTransaction() throws SQLException {
        assertThrows(SQLException.class, () -> MysqlConnectionContext.withPublicationProcedure(firstPool, 1,
                connection -> {
                    try (var statement = connection.createStatement()) {
                        statement.execute("start transaction");
                    }
                    return 1;
                }));
        Assertions.assertEquals(1, second.withPublicationLock(() -> 1));
    }

    /**
     * Only v1 is called when barrier is held to avoid the server-end routine retrieving the same name lock.
     */
    @Test
    void nestedPublicationUsesExistingLockWithoutServerSideReentry() {
        long txId = first.withPublicationLock(() -> {
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationProcedure(
                            firstPool, 1, connection -> 1)).errorCode());
            return first.tryBegin(tso, IsolationLevel.REPEATABLE_READ, 60_000, false,
                    () -> {
                    }).orElseThrow().txId();
        });
        assertTrue(first.snapshotExpiry(txId).isPresent());
        Assertions.assertEquals(1, second.withPublicationLock(() -> 1));
    }

    /**
     * Materialization tags and lease releases of decision transaction should not wait for the release of barrier held by another transaction.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void terminalCleanupCanProceedWhilePublicationLockIsHeld() throws Exception {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerSnapshot(1, 1, first.currentTimeMillis() + 60_000);
        first.markCommitted(1, 2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var holder = executor.submit(() -> first.withPublicationLock(() -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("publication hold timed out");
                    }
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
                return null;
            }));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            try {
                var cleanup = executor.submit(() -> {
                    second.markFinalized(1);
                    second.releaseSnapshot(1);
                });
                cleanup.get(2, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                holder.get(5, TimeUnit.SECONDS);
            }
        }
        assertTrue(first.snapshotExpiry(1).isEmpty());
    }

    /**
     * Participant registration relies only on its own transaction line lock and cannot occupy other transaction releases.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void participantRegistrationCanProceedWhilePublicationLockIsHeld() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var holder = executor.submit(() -> first.withPublicationLock(() -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("publication hold timed out");
                    }
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
                return null;
            }));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            try {
                var registration = executor.submit(() -> {
                    second.createWithParticipants(2, 2, IsolationLevel.REPEATABLE_READ, Set.of("a"));
                    return second.registerParticipants(2, Set.of("b"));
                });
                Assertions.assertEquals(Set.of("a", "b"), registration.get(2, TimeUnit.SECONDS).participants());
            } finally {
                release.countDown();
                holder.get(5, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Independent timestamp range instance and regular TSO staggered numbers are still increasing in same database durable storage.
     */
    @Test
    void segmentCompatibilityNeverAllocatesIndependentClientRanges() {
        var left = new MysqlSegmentTimestampOracle(firstPool, 128);
        var right = new MysqlSegmentTimestampOracle(secondPool, 256);
        left.initialize(100);
        right.initialize(100);
        Assertions.assertEquals(101, left.nextTimestamp());
        Assertions.assertEquals(102, right.nextTimestamp());
        Assertions.assertEquals(103, left.nextTimestamp());
        Assertions.assertEquals(104, tso.nextTimestamp());
        Assertions.assertEquals(105, right.nextTimestamp());
        Assertions.assertEquals(2, left.segmentAllocations());
        Assertions.assertEquals(2, right.segmentAllocations());
    }

    /**
     * Durable storage lease and atomic registration activity transaction in a non-delayed mode are required before the merger starts to return.
     */
    @Test
    void serverSideBeginPersistsSnapshotAndOptionalMetadata() {
        var lazy = first.tryBegin(tso, IsolationLevel.REPEATABLE_READ, 60_000, false, () -> {
        })
                .orElseThrow();
        Assertions.assertEquals(lazy.txId(), lazy.startTs());
        Assertions.assertEquals(lazy.deadlineMillis(), first.snapshotExpiry(lazy.txId()).orElseThrow().longValue());
        assertTrue(first.get(lazy.txId()).isEmpty());
        var eager = first.tryBegin(tso, IsolationLevel.READ_COMMITTED, 60_000, true, () -> {
        })
                .orElseThrow();
        assertTrue(eager.txId() > lazy.txId());
        Assertions.assertEquals(TxStatus.ACTIVE, first.get(eager.txId()).orElseThrow().status());
        Assertions.assertEquals(eager.deadlineMillis(), first.snapshotExpiry(eager.txId()).orElseThrow().longValue());
        var restartedStore = new MysqlTransactionMetadataStore(firstPool);
        assertTrue(restartedStore.tryBegin(tso, IsolationLevel.READ_COMMITTED,
                60_000, false, () -> {
                }).isPresent());
        assertTrue(first.tryBegin(new MysqlTimestampOracle(secondPool), IsolationLevel.READ_COMMITTED,
                60_000, false, () -> {
                }).isEmpty());
    }

    /**
     * When single shard local commit is in place, consolidation cannot begin to allocate timestamp or release new snapshot.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void serverSideBeginRejectsPendingLocalPublication() throws SQLException {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        first.markLocalCommitPending(1, 10);
        long before;
        try (
                Connection connection = firstPool.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("select current_value from mvcc_tso where sequence_name = 'global'")) {
            assertTrue(result.next());
            before = result.getLong(1);
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> first.tryBegin(tso, IsolationLevel.READ_COMMITTED,
                        60_000, false, () -> {
                        })).errorCode());
        try (
                Connection connection = firstPool.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("select current_value from mvcc_tso where sequence_name = 'global'")) {
            assertTrue(result.next());
            Assertions.assertEquals(before, result.getLong(1));
        }
    }

    /**
     * Server commit accepts only registered participant and repeat responses must return the same durable storage decision.
     */
    @Test
    void serverSideCommitPreservesDecisionAcrossMetadataInstances() {
        var started = first.tryBegin(tso, IsolationLevel.REPEATABLE_READ, 60_000, true, () -> {
        })
                .orElseThrow();
        first.registerParticipants(started.txId(), Set.of("a", "b"));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> first.tryCommit(tso, started.txId(),
                        Set.of("a"), () -> {
                        }, ignored -> {
                        })).errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, second.get(started.txId()).orElseThrow().status());
        long committedTs = first.tryCommit(tso, started.txId(), Set.of("a", "b"), () -> {
        }, ignored -> {
        })
                .orElseThrow();
        assertTrue(committedTs > started.startTs());
        Assertions.assertEquals(committedTs, second.get(started.txId()).orElseThrow().commitTs().longValue());
        var restartedStore = new MysqlTransactionMetadataStore(secondPool);
        Assertions.assertEquals(committedTs, restartedStore.tryCommit(new MysqlTimestampOracle(secondPool), started.txId(),
                Set.of("a", "b"), () -> {
                }, ignored -> {
                }).orElseThrow());
    }

    /**
     * Reject exhausted global sequences instead of wrapping around or returning duplicate timestamps.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void timestampOracleMustRejectSequenceExhaustion() throws SQLException {
        try (Connection connection = firstPool.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_tso set current_value = 9223372036854775807 where sequence_name = 'global'");
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, tso::nextTimestamp).errorCode());
    }

    /**
     * Fixed lease, single shard position and irreversible terminal state to be consistent between independent metadata objects.
     */
    @Test
    void pendingPublicationAndFinalizationSurviveMetadataInstanceChanges() {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        long deadline = first.currentTimeMillis() + 60_000;
        first.registerSnapshot(1, 1, deadline);
        assertThrows(MvccException.class, () -> second.registerSnapshot(1, 1, deadline + 1));
        first.markLocalCommitPending(1, 10);
        var error = assertThrows(MvccException.class, second::assertNoPendingPublications);
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        second.markCommitted(1, 10);
        first.persistPreparing(1, Set.of("b"));
        Assertions.assertEquals(TxStatus.COMMITTED, first.markAborted(1).status());
        assertThrows(MvccException.class, () -> first.persistCommitted(1, 11));
        assertDoesNotThrow(second::assertNoPendingPublications);
        first.releaseSnapshot(1);
        Assertions.assertEquals(1L, second.oldestActiveStartTs().orElseThrow());
        second.markFinalized(1);
        assertTrue(first.oldestActiveStartTs().isEmpty());
        Assertions.assertEquals(0, first.purgeFinalized(10, Long.MAX_VALUE, 1));
        Assertions.assertEquals(1, first.purgeFinalized(11, Long.MAX_VALUE, 1));
    }

    /**
     * The competitor waits before borrowing a pool connection, and the lock-to-wire course can still use the only connection in the pool to advance control.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void publicationWaitersDoNotExhaustControlPool() throws Exception {
        var entered = new CountDownLatch(1);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var owner = executor.submit(() -> first.withPublicationLock(() -> {
                entered.countDown();
                await(release);
                return tso.nextTimestamp();
            }));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var waiter = executor.submit(() -> {
                    started.countDown();
                    return new MysqlTransactionMetadataStore(firstPool, 5).withPublicationLock(tso::nextTimestamp);
                });
                assertTrue(started.await(5, TimeUnit.SECONDS));
                Assertions.assertEquals(1, firstPool.getHikariPoolMXBean().getActiveConnections());
                release.countDown();
                assertTrue(owner.get(5, TimeUnit.SECONDS) < waiter.get(5, TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
        }
    }

    /**
     * MySQL commit entrance rejects the error timestamp and the unmistakable shard ID before durable storage.
     */
    @Test
    void metadataRejectsInvalidCommitTimestampsAndParticipantIdentifiers() {
        first.create(1, 10, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        for (long commitTs : new long[]{Long.MIN_VALUE, 9, 10}) {
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> first.markLocalCommitPending(1, commitTs)).errorCode());
            assertThrows(MvccException.class, () -> first.markCommitted(1, commitTs));
            assertThrows(MvccException.class, () -> first.persistCommitted(1, Set.of("a"), commitTs));
            assertThrows(MvccException.class, () -> first.markSingleShardCommitted(1, "a", commitTs));
            assertThrows(MvccException.class, () -> first.createCommitted(2, 10, IsolationLevel.REPEATABLE_READ, Set.of("a"), commitTs));
            Assertions.assertEquals(TxStatus.ACTIVE, second.get(1).orElseThrow().status());
            assertTrue(second.get(2).isEmpty());
        }
        for (String participant : new String[]{"", " ", "\t", "a,b", ","}) {
            Set<String> participants = Set.of(participant);
            assertThrows(MvccException.class, () -> first.createPreparing(2, 2, IsolationLevel.REPEATABLE_READ, participants));
            assertThrows(MvccException.class, () -> first.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, participants, 3));
            assertThrows(MvccException.class, () -> first.registerParticipants(1, participants));
            assertThrows(MvccException.class, () -> first.persistPreparing(1, participants));
            assertThrows(MvccException.class, () -> first.persistCommitted(1, participants, 11));
            assertThrows(MvccException.class, () -> first.markSingleShardCommitted(1, participant, 11));
            Assertions.assertEquals(Set.of("a"), second.get(1).orElseThrow().participants());
            Assertions.assertEquals(TxStatus.ACTIVE, second.get(1).orElseThrow().status());
            assertTrue(second.get(2).isEmpty());
        }
        first.markLocalCommitPending(1, 11);
        Assertions.assertEquals(11L, first.markCommitted(1, 11).commitTs());
        assertThrows(MvccException.class, () -> first.registerParticipants(1, Set.of("a,b")));
        assertThrows(MvccException.class, () -> first.persistPreparing(1, Set.of(" ")));
    }

    /**
     * The empty collection and legal route in the old CSV is still readable and the blank footage cannot be left silent and mislabeled recovery completed.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void legacyCsvRemainsCompatibleButMalformedSegmentsFailClosed() throws SQLException {
        first.createPreparing(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a", "b"));
        try (
                Connection connection = firstPool.getConnection();
                var update = connection.prepareStatement("update mvcc_txn set participants = ? where tx_id = 1")) {
            for (String encoded : new String[]{" ", "a,", ",a", "a,,b", "a, ,b"}) {
                update.setString(1, encoded);
                update.executeUpdate();
                Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                        assertThrows(MvccException.class, () -> second.get(1)).errorCode());
            }
            update.setString(1, "b,a");
            update.executeUpdate();
            Assertions.assertEquals(Set.of("a", "b"), second.get(1).orElseThrow().participants());
            update.setString(1, "");
            update.executeUpdate();
            assertTrue(second.get(1).orElseThrow().participants().isEmpty());
        }
    }

    /**
     * Creates a pool of connection to expose errors in barrier to repeat connection.
     *
     * @return the operation result
     */
    private static HikariDataSource pool() {
        var config = new HikariConfig();
        config.setJdbcUrl(MYSQL.getJdbcUrl());
        config.setUsername(MYSQL.getUsername());
        config.setPassword(MYSQL.getPassword());
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(1000);
        return new HikariDataSource(config);
    }

    /**
     * Use bounded to wait for the construction of competition, and recovery interrupts a bit when it fails.
     *
     * @param latch latch
     * @throws AssertionError if the operation cannot complete
     */
    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }
}

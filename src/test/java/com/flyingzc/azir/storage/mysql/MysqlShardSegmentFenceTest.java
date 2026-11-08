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

package com.flyingzc.azir.storage.mysql;

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
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Real MySQL verify shard owner installation, algebra fencing and en route local transaction serialization.
 */
@Testcontainers
class MysqlShardSegmentFenceTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("local_segment_fence").withUsername("test").withPassword("test");

    private static HikariDataSource dataSource;

    private MysqlShardSegmentFence fence;

    private MysqlStorageAdapter shard;

    /**
     * Creates an isolation connection pool for new protocol fence testing only.
     */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Close test held physical connection pool.
     */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /**
     * Recreate a fully independent version version of local fence for each example.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_segment_candidate_v1");
            statement.execute("drop table if exists mvcc_local_segment_fence_v1");
            statement.execute("drop table if exists mvcc_local_segment_state_v1");
        }
        fence = new MysqlShardSegmentFence("s1", dataSource);
        fence.initializeIsolated();
        shard = new MysqlStorageAdapter("s1", dataSource);
        shard.initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
        }
    }

    /**
     * Installation replay does not extend lease and overlaps with other owners cannot get the same slot.
     */
    @Test
    void installReplayMustPreserveIdentityAndRejectOverlap() {
        var owner = lease(101, 104, "owner-a");
        var installed = fence.install(owner, 60_000);
        var replay = fence.install(owner, 60_000);
        Assertions.assertEquals(installed.expiresAtMillis(), replay.expiresAtMillis());
        Assertions.assertEquals(1, replay.generation());
        assertTrue(replay.active());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> fence.install(lease(103, 106, "owner-b"),
                        60_000)).errorCode());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> fence.install(lease(101, 104, "owner-b"),
                        60_000)).errorCode());
        assertTrue(fence.renew(owner, 60_000).active());
    }

    /**
     * The platoon takes over the local commit waiting to hold shared fence, and cannot add commit after the old algebra.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void fencingMustWaitForInFlightLocalCommit() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        try (
                Connection writer = dataSource.getConnection();
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            writer.setAutoCommit(false);
            assertTrue(fence.lockForCommit(writer, owner, 101));
            var takeover = workers.submit(() -> fence.fence(owner));
            try {
                Thread.sleep(150);
                assertFalse(takeover.isDone());
                writer.commit();
            } finally {
                writer.rollback();
            }
            assertTrue(takeover.get(5, TimeUnit.SECONDS));
        }
        assertFalse(fence.snapshot(owner.firstTs()).active());
        Assertions.assertEquals(2, fence.snapshot(owner.firstTs()).generation());
        assertFalse(fence.fence(owner));
        try (Connection late = dataSource.getConnection()) {
            late.setAutoCommit(false);
            assertFalse(fence.lockForCommit(late, owner, 101));
            late.rollback();
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.renew(owner, 60_000)).errorCode());
    }

    /**
     * Shard lease cannot open a new local commit even if the centre has not been taken over by its expiry date.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredLocalLeaseMustRejectNewCommitAndRenewal() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 20);
        Thread.sleep(50);
        try (Connection late = dataSource.getConnection()) {
            late.setAutoCommit(false);
            Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                    assertThrows(MvccException.class, () -> fence.lockForCommit(late, owner, 101)).errorCode());
            late.rollback();
        }
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.renew(owner, 60_000)).errorCode());
        assertTrue(fence.fence(owner));
    }

    /**
     * The shard installation must be written to tombstone when it has not landed, preventing late installation and initial physical commit.
     */
    @Test
    void missingFenceMustPersistTombstoneBeforeCenterSealsSlots() {
        var owner = lease(101, 104, "owner-a");
        assertTrue(fence.fence(owner));
        assertFalse(fence.snapshot(101).active());
        Assertions.assertEquals(2, fence.snapshot(101).generation());
        assertFalse(fence.fence(owner));
        assertTrue(fence.scanFenced(owner, 100, 10).isEmpty());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.install(owner, 60_000)).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:tombstone", new byte[]{1})))).errorCode());
        assertTrue(fence.candidate(10).isEmpty());
        assertTrue(shard.participantRecord(10).isEmpty());
    }

    /**
     * Installation and absence of fencing will eventually leave no more than commit physical fence.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentInstallAndMissingFenceMustConverge() throws Exception {
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int index = 0; index < 8; index++) {
                long first = 101 + index * 4L;
                var owner = lease(first, first + 3, "owner-a");
                var start = new CountDownLatch(1);
                var install = workers.submit(() -> {
                    start.await();
                    try {
                        fence.install(owner, 60_000);
                        return true;
                    } catch (final MvccException ex) {
                        if (ex.errorCode() != ErrorCode.TRANSACTION_ABORTED) {
                            throw ex;
                        }
                        return false;
                    }
                });
                var takeover = workers.submit(() -> {
                    start.await();
                    return fence.fence(owner);
                });
                start.countDown();
                install.get(5, TimeUnit.SECONDS);
                assertTrue(takeover.get(5, TimeUnit.SECONDS));
                assertFalse(fence.snapshot(first).active());
                Assertions.assertEquals(2, fence.snapshot(first).generation());
                Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                        assertThrows(MvccException.class, () -> fence.install(owner, 60_000)).errorCode());
            }
        }
    }

    /**
     * Shard version, participant terminal state and local prove the same as transaction commit; physical fencing after collection remains stable.
     */
    @Test
    void localCommitCandidateMustRemainRecoverableAfterFencing() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        List<Mutation> writes = List.of(Mutation.put("local:first", new byte[]{7}));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        var proof = fence.candidate(10).orElseThrow();
        Assertions.assertEquals(101, proof.commitTs());
        Assertions.assertEquals(100, proof.startTs());
        Assertions.assertEquals(TxStatus.COMMITTED, shard.participantRecord(10).orElseThrow().status());
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:first").orElseThrow().value());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.scanFenced(owner, 100, 10)).errorCode());
        assertTrue(fence.fence(owner));
        Assertions.assertEquals(List.of(proof), fence.scanFenced(owner, 100, 10));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:first", new byte[]{8})))).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:late", new byte[]{1})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(shard.participantRecord(11).isEmpty());
    }

    /**
     * Write conflict shall not produce a candidate or leak local version and only the first commit transaction can be seen in the take-over scan.
     */
    @Test
    void conflictMustNotLeaveLocalCandidate() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:conflict", new byte[]{1})));
        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:conflict", new byte[]{2})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(shard.participantRecord(11).isEmpty());
        assertTrue(fence.fence(owner));
        Assertions.assertEquals(1, fence.scanFenced(owner, 100, 10).size());
    }

    /**
     * The candidate for insertion failed with rollback already written version and local COMMITTED terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void candidateWriteFailureMustRollBackLocalCommit() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_segment_candidate_v1 "
                    + "add constraint mvcc_test_reject_candidate check (commit_ts <> 101)");
        }
        try {
            assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                    10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:rollback", new byte[]{3}))));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_segment_candidate_v1 "
                        + "drop check mvcc_test_reject_candidate");
            }
        }
        assertTrue(fence.candidate(10).isEmpty());
        assertTrue(shard.participantRecord(10).isEmpty());
        assertTrue(shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:rollback").isEmpty());
    }

    /**
     * The local certificate for uncompleted centre decision forbids GC to cross its start snapshot.
     */
    @Test
    void localCandidateMustPinShardSafePoint() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:gc-protected", new byte[]{9})));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> shard.gc(101, 10)).errorCode());
        shard.gc(100, 10);
        assertTrue(fence.candidate(10).isPresent());
        Assertions.assertEquals(TxStatus.COMMITTED, shard.participantRecord(10).orElseThrow().status());
        assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:gc-protected").orElseThrow().value());
    }

    /**
     * The single-key server path maintains atomic proofs under physics fencing, replay and old snapshot write conflict.
     */
    @Test
    void singleKeyProcedureMustPreserveFencingReplayAndConflict() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedure();
        List<Mutation> writes = List.of(Mutation.put("local:procedure", new byte[]{6}));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        Assertions.assertEquals(101, fence.verifiedCandidate(10).orElseThrow().commitTs());
        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:procedure", new byte[]{7})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(fence.fence(owner));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        12, 100, 103, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:late-procedure", new byte[]{8})))).errorCode());
        Assertions.assertEquals(1, fence.scanFenced(owner, 100, 10).size());
    }

    /**
     * The third version of the direct terminal state routine still requires late requests for validate write conflict, old algebra replay and cover.
     */
    @Test
    void singleKeyProcedureV3MustPreserveFencingReplayAndConflict() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        List<Mutation> writes = List.of(Mutation.put("local:procedure-v3", new byte[]{6}));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        Assertions.assertEquals(101, fence.verifiedCandidate(10).orElseThrow().commitTs());
        Assertions.assertEquals(TxStatus.COMMITTED, shard.participantRecord(10).orElseThrow().status());
        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:procedure-v3", new byte[]{7})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(fence.fence(owner));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:procedure-v3", new byte[]{9})))).errorCode());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        12, 100, 103, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:late-procedure-v3", new byte[]{8})))).errorCode());
        Assertions.assertEquals(1, fence.scanFenced(owner, 100, 10).size());
    }

    /**
     * Concurrent and transaction ID can only have one irreversible version with a candidate. The local script that failed to be called must be rollback.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleKeyProcedureV3ConcurrentIdentityRaceMustHaveOneWinner() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        var start = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = workers.submit(() -> {
                start.await();
                try {
                    shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                            IsolationLevel.REPEATABLE_READ,
                            List.of(Mutation.put("local:v3-race:a", new byte[]{1})));
                    return true;
                } catch (final MvccException ex) {
                    return false;
                }
            });
            var second = workers.submit(() -> {
                start.await();
                try {
                    shard.directCommitFenced(fence, owner, 10, 100, 102, 60_000,
                            IsolationLevel.REPEATABLE_READ,
                            List.of(Mutation.put("local:v3-race:b", new byte[]{2})));
                    return true;
                } catch (final MvccException ex) {
                    return false;
                }
            });
            start.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS) ^ second.get(5, TimeUnit.SECONDS));
        }
        var proof = fence.verifiedCandidate(10).orElseThrow();
        Assertions.assertEquals(proof.commitTs(), shard.participantRecord(10).orElseThrow().commitTs());
        var reader = new ReadContext(999, 102, IsolationLevel.READ_COMMITTED);
        assertTrue(shard.get(reader, "local:v3-race:a").isPresent()
                ^ shard.get(reader, "local:v3-race:b").isPresent());
    }

    /**
     * Two old snapshot s are queuing in front of the key lock, after which the locker must see the version first.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleKeyProcedureV3MustRecheckConflictAfterKeyLockWait() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        try (Connection blocker = dataSource.getConnection(); Statement statement = blocker.createStatement()) {
            statement.execute("insert into mvcc_key_lock(user_key) values ('local:v3-stale-snapshot')");
            blocker.setAutoCommit(false);
            try (
                    var locked = statement.executeQuery("select user_key from mvcc_key_lock "
                            + "where user_key = 'local:v3-stale-snapshot' for update")) {
                assertTrue(locked.next());
            }
            var start = new CountDownLatch(1);
            try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var first = workers.submit(() -> {
                    start.await();
                    try {
                        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                                IsolationLevel.REPEATABLE_READ,
                                List.of(Mutation.put("local:v3-stale-snapshot", new byte[]{1})));
                        return true;
                    } catch (final MvccException ex) {
                        return false;
                    }
                });
                var second = workers.submit(() -> {
                    start.await();
                    try {
                        shard.directCommitFenced(fence, owner, 11, 100, 102, 60_000,
                                IsolationLevel.REPEATABLE_READ,
                                List.of(Mutation.put("local:v3-stale-snapshot", new byte[]{2})));
                        return true;
                    } catch (final MvccException ex) {
                        return false;
                    }
                });
                start.countDown();
                Thread.sleep(100);
                assertFalse(first.isDone());
                assertFalse(second.isDone());
                blocker.commit();
                assertTrue(first.get(5, TimeUnit.SECONDS) ^ second.get(5, TimeUnit.SECONDS));
            }
        }
        assertTrue(fence.candidate(10).isPresent() ^ fence.candidate(11).isPresent());
    }

    /**
     * Rollback direct with the third version of the commit competition can only leave COMMITTED candidate or one of ABORTED terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleKeyProcedureV3AbortRaceMustRemainAtomic() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        var start = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var commit = workers.submit(() -> {
                start.await();
                try {
                    shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                            IsolationLevel.REPEATABLE_READ,
                            List.of(Mutation.put("local:v3-abort-race", new byte[]{3})));
                    return true;
                } catch (final MvccException ex) {
                    return false;
                }
            });
            var abort = workers.submit(() -> {
                start.await();
                shard.abort(10);
                return true;
            });
            start.countDown();
            boolean committed = commit.get(5, TimeUnit.SECONDS);
            assertTrue(abort.get(5, TimeUnit.SECONDS));
            TxStatus status = shard.participantRecord(10).orElseThrow().status();
            if (committed) {
                Assertions.assertEquals(TxStatus.COMMITTED, status);
                assertTrue(fence.candidate(10).isPresent());
                assertArrayEquals(new byte[]{3}, shard.get(new ReadContext(999, 101,
                        IsolationLevel.READ_COMMITTED), "local:v3-abort-race").orElseThrow().value());
            } else {
                Assertions.assertEquals(TxStatus.ABORTED, status);
                assertTrue(fence.candidate(10).isEmpty());
                assertTrue(shard.get(new ReadContext(999, 101,
                        IsolationLevel.READ_COMMITTED), "local:v3-abort-race").isEmpty());
            }
        }
    }

    /**
     * Rollback local version and participant terminal state are also required for candidate insertion of original single key.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleKeyProcedureCandidateFailureMustRollBack() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedure();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_segment_candidate_v1 "
                    + "add constraint mvcc_test_reject_procedure check (commit_ts <> 101)");
        }
        try {
            assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                    10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:procedure-rollback", new byte[]{9}))));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_segment_candidate_v1 "
                        + "drop check mvcc_test_reject_procedure");
            }
        }
        assertTrue(fence.candidate(10).isEmpty());
        assertTrue(shard.participantRecord(10).isEmpty());
        assertTrue(shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:procedure-rollback").isEmpty());
    }

    /**
     * The third version of the candidate for insertion has to be both rollback data version and a directly written COMMITTED terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleKeyProcedureV3CandidateFailureMustRollBack() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_segment_candidate_v1 "
                    + "add constraint mvcc_test_reject_procedure_v3 check (commit_ts <> 101)");
        }
        try {
            assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                    10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:procedure-v3-rollback", new byte[]{9}))));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_segment_candidate_v1 "
                        + "drop check mvcc_test_reject_procedure_v3");
            }
        }
        assertTrue(fence.candidate(10).isEmpty());
        assertTrue(shard.participantRecord(10).isEmpty());
        assertTrue(shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:procedure-v3-rollback").isEmpty());
    }

    /**
     * Construct an durable control segment for local shell funcing only.
     *
     * @param first first
     * @param last last
     * @param ownerId owner id
     * @return the operation result
     */
    private PublicationSegmentLease lease(final long first, final long last, final String ownerId) {
        return new PublicationSegmentLease(first, last, ownerId, 1, Long.MAX_VALUE);
    }
}

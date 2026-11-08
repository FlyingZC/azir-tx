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
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.LocalFirstPublicationRecoveryManager;
import com.flyingzc.azir.publication.LocalFirstPublicationMaintenanceScheduler;
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
import java.util.Map;

/**
 * The real combination of verify shard local commit, batch Center terminal state and global for watermark under MySQL.
 */
@Testcontainers
class MysqlLocalFirstCompletionTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("local_first_complete").withUsername("test").withPassword("test");

    private static HikariDataSource dataSource;

    private MysqlLocalFirstPublicationLedger control;

    private MysqlShardSegmentFence physical;

    private MysqlStorageAdapter shard;

    /**
     * Creates a MySQL test for this class.
     */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Closes the real database connection pool.
     */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /**
     * Clears two separate version time lines and recovery is known shard safe point.
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
        control = new MysqlLocalFirstPublicationLedger(dataSource);
        control.initializeIsolated(100);
        physical = new MysqlShardSegmentFence("s1", dataSource);
        physical.initializeIsolated();
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
     * Shard is down, but both old RRs and new RCs are subject to watermark when the centre is not published. Only new RCs will see version after completion.
     */
    @Test
    void localCommitMustBecomeGloballyVisibleOnlyAfterCenterPublication() {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, writer.startTs());
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), writer.startTs(), 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:visible", new byte[]{7})));
        Assertions.assertEquals(100, control.currentPublished());
        assertFalse(control.decision(writer.txId()).isPresent());
        var earlyReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        Assertions.assertEquals(100, earlyReader.startTs());
        assertTrue(shard.get(new ReadContext(earlyReader.txId(), earlyReader.startTs(),
                IsolationLevel.READ_COMMITTED), "local:visible").isEmpty());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:visible").isEmpty());
        Assertions.assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
        Assertions.assertEquals(0, control.completeCandidates(segment, physical, List.of(writer.txId())));
        Assertions.assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        Assertions.assertEquals(101, control.advancePublished(10));
        var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        Assertions.assertEquals(101, newReader.startTs());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:visible").isEmpty());
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(newReader.txId(), newReader.startTs(),
                IsolationLevel.READ_COMMITTED), "local:visible").orElseThrow().value());
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(earlyReader.txId());
        control.releaseSnapshot(newReader.txId());
    }

    /**
     * The third version of the shard routine is also only visible to new RCs after the center durable release, and old RR fixed old snapshot.
     */
    @Test
    void localProcedureV3MustPreserveGlobalRcAndRrPublication() {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-v3", "request-v3", 1, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        physical.enableSingleKeyProcedureV3();
        var proof = shard.directCommitFencedWithProof(physical, segment.lease(),
                writer.txId(), writer.startTs(), 101, writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:v3-visible", new byte[]{4})));
        var earlyRc = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        Assertions.assertEquals(100, earlyRc.startTs());
        assertTrue(shard.get(new ReadContext(earlyRc.txId(), earlyRc.startTs(),
                IsolationLevel.READ_COMMITTED), "local:v3-visible").isEmpty());
        Assertions.assertEquals(101, control.completeProvedCandidatesAndPublish(segment, physical,
                List.of(proof)).publishedTs());
        var laterRc = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertArrayEquals(new byte[]{4}, shard.get(new ReadContext(laterRc.txId(), laterRc.startTs(),
                IsolationLevel.READ_COMMITTED), "local:v3-visible").orElseThrow().value());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:v3-visible").isEmpty());
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(earlyRc.txId());
        control.releaseSnapshot(laterRc.txId());
    }

    /**
     * The entire local candidate durable completes the center terminal state concurrent sheet with a physical cover at a time.
     */
    @Test
    void fullPhysicalSegmentMustPublishWithoutPerTransactionCenterCompletion() {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var owner = new LocalFirstSegmentOwnerRegistry(control, physical, () -> oldReader.startTs(),
                1, 5_000, 1_000, 100);
        var assigned = owner.allocate(writer.startTs());
        shard.directCommitFenced(physical, assigned.reservation().lease(), writer.txId(),
                writer.startTs(), assigned.commitTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:whole-segment", new byte[]{8})));
        Assertions.assertEquals(100, control.currentPublished());
        owner.completePhysicalAndSeal(assigned);
        Assertions.assertEquals(101, control.currentPublished());
        Assertions.assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
                control.segment(assigned.reservation().lease().firstTs()).orElseThrow().status());
        var currentReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:whole-segment").isEmpty());
        assertArrayEquals(new byte[]{8}, shard.get(new ReadContext(currentReader.txId(),
                currentReader.startTs(), IsolationLevel.READ_COMMITTED),
                "local:whole-segment").orElseThrow().value());
        owner.close();
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(currentReader.txId());
    }

    /**
     * The local successful foreground certificate can only be completed by the original shard instance, and Centre replay still checks durable identity.
     */
    @Test
    void localCommitProofMustCompleteWithoutShardLookupAndRejectAnotherFence() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-proof", "request-proof", 1, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        physical.enableSingleKeyProcedure();
        var proof = shard.directCommitFencedWithProof(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:proof", new byte[]{9})));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.completeProvedCandidates(segment,
                        new MysqlShardSegmentFence("s1", dataSource), List.of(proof))).errorCode());
        Assertions.assertEquals(1, control.completeProvedCandidatesAndPublish(segment, physical, List.of(proof)).completed());
        Assertions.assertEquals(0, control.completeProvedCandidatesAndPublish(segment, physical, List.of(proof)).completed());
        Assertions.assertEquals(101, control.currentPublished());
        Assertions.assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        control.releaseSnapshot(writer.txId());
    }

    /**
     * Rollback is also required for the first slot and two metadata when the second slot of a centre batch SQL fails.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void batchCompletionFailureMustRollBackAllCenterDecisions() throws Exception {
        var first = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var second = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), first.txId(), 100, 101,
                first.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:batch:first", new byte[]{1})));
        shard.directCommitFenced(physical, segment.lease(), second.txId(), 100, 102,
                second.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:batch:second", new byte[]{2})));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_first_slot_v1 "
                    + "add constraint mvcc_test_reject_local_completion check "
                    + "(commit_ts <> 102 or status <> 'COMMITTED')");
        }
        try {
            assertThrows(MvccException.class,
                    () -> control.completeCandidates(segment, physical, List.of(second.txId(), first.txId())));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_first_slot_v1 "
                        + "drop check mvcc_test_reject_local_completion");
            }
        }
        assertFalse(control.decision(first.txId()).isPresent());
        assertFalse(control.decision(second.txId()).isPresent());
        Assertions.assertEquals(100, control.advancePublished(10));
        Assertions.assertEquals(2, control.completeCandidates(segment, physical, List.of(second.txId(), first.txId())));
        Assertions.assertEquals(102, control.advancePublished(10));
    }

    /**
     * Controls have begun to take over and shard has been physically funcing, the durable candidate for the old algebra still needs to be replaced with COMMITTED.
     */
    @Test
    void fencedLocalCandidateMustStillCompleteOriginalCenterSlot() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:fenced", new byte[]{3})));
        var request = control.beginFencing(segment, true);
        assertTrue(physical.fence(request.physicalLease()));
        Assertions.assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
        Assertions.assertEquals(0, control.completeCandidates(control.segment(101).orElseThrow(),
                physical, List.of(writer.txId())));
        Assertions.assertEquals(101, control.advancePublished(10));
    }

    /**
     * Physical fencing only replaces the local candidate with commit, and the rest of the tailings are suspended and released for continuous release.
     */
    @Test
    void fencedRecoveryMustPublishCommittedCandidateAndAbortUnusedTail() {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:recovery", new byte[]{4})));
        var request = control.beginFencing(segment, true);
        assertTrue(physical.fence(request.physicalLease()));
        Assertions.assertEquals(3, control.sealFencedSegment(segment, physical));
        Assertions.assertEquals(0, control.sealFencedSegment(segment, physical));
        Assertions.assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        Assertions.assertEquals(104, control.advancePublished(10));
        var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        Assertions.assertEquals(104, newReader.startTs());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:recovery").isEmpty());
        assertArrayEquals(new byte[]{4}, shard.get(new ReadContext(newReader.txId(), newReader.startTs(),
                IsolationLevel.READ_COMMITTED), "local:recovery").orElseThrow().value());
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
                control.segment(101).orElseThrow().status());
    }

    /**
     * Shard has never been installed and writes before closing tombstone, then sealing the entire empty slot.
     */
    @Test
    void missingPhysicalInstallationMustAbortOnlyAfterTombstone() {
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        var request = control.beginFencing(segment, true);
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> control.sealFencedSegment(segment, physical)).errorCode());
        assertTrue(physical.fence(request.physicalLease()));
        Assertions.assertEquals(4, control.sealFencedSegment(segment, physical));
        Assertions.assertEquals(104, control.advancePublished(10));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segment.lease(), 5_000)).errorCode());
    }

    /**
     * SQL failed without pre-marking SEALED, retry is still completed by the same shard.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void sealFailureMustKeepFencingUntilAtomicRetry() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:seal-retry", new byte[]{5})));
        physical.fence(control.beginFencing(segment, true).physicalLease());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_first_slot_v1 "
                    + "add constraint mvcc_test_reject_local_seal check "
                    + "(commit_ts <> 102 or status <> 'ABORTED')");
        }
        try {
            assertThrows(MvccException.class, () -> control.sealFencedSegment(segment, physical));
        } finally {
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_first_slot_v1 drop check mvcc_test_reject_local_seal");
            }
        }
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.FENCING,
                control.segment(101).orElseThrow().status());
        Assertions.assertEquals(101, control.advancePublishedFast(10));
        Assertions.assertEquals(3, control.sealFencedSegment(segment, physical));
        Assertions.assertEquals(104, control.advancePublishedFast(10));
    }

    /**
     * The centre snapshot lease which is still in the center may not release local version if it is different from the start time of the local candidate.
     */
    @Test
    void liveSnapshotLeaseMismatchMustRejectCenterCompletion() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 80);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 99, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:wrong-snapshot", new byte[]{6})));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> control.completeCandidates(segment, physical, List.of(writer.txId()))).errorCode());
        assertFalse(control.decision(writer.txId()).isPresent());
        Assertions.assertEquals(100, control.advancePublished(10));
    }

    /**
     * An entire batch of PNDGs is maintained when one of the shard candidates lacks a match to terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptLocalProofMustRejectWholeCompletionBatch() throws Exception {
        var first = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var second = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), first.txId(), 100, 101,
                first.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:proof:first", new byte[]{1})));
        shard.directCommitFenced(physical, segment.lease(), second.txId(), 100, 102,
                second.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:proof:second", new byte[]{2})));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_participant_txn set commit_ts = 999 where tx_id = "
                    + second.txId());
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.completeCandidates(segment,
                        physical, List.of(first.txId(), second.txId()))).errorCode());
        assertFalse(control.decision(first.txId()).isPresent());
        assertFalse(control.decision(second.txId()).isPresent());
        Assertions.assertEquals(100, control.advancePublished(10));
    }

    /**
     * Concurrent local CAS failed to misjudge the entire section and leave an empty slot that could not be released.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentLocalAllocationMustNotSkipReservedTail() throws Exception {
        var allocator = new MysqlLocalFirstProtocolBenchmark.SegmentAllocator(control, physical, 128);
        try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = new java.util.concurrent.CountDownLatch(64);
            var start = new java.util.concurrent.CountDownLatch(1);
            var allocations = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 64; index++) {
                allocations.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return allocator.allocate().commitTs();
                }));
            }
            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            var unique = new java.util.HashSet<Long>();
            for (var allocated : allocations) {
                assertTrue(unique.add(allocated.get(10, java.util.concurrent.TimeUnit.SECONDS)));
            }
            Assertions.assertEquals(64, unique.size());
            Assertions.assertEquals(101, unique.stream().mapToLong(Long::longValue).min().orElseThrow());
            Assertions.assertEquals(164, unique.stream().mapToLong(Long::longValue).max().orElseThrow());
            assertTrue(control.segment(101).isPresent());
            assertTrue(control.segment(229).isEmpty());
        }
    }

    /**
     * Coordinator negative transaction ID maintains the global release view of old RR and new RCs without one-on-one transaction lease.
     */
    @Test
    void coordinatorSnapshotsMustPreservePublishedReadCommittedAndRepeatableRead() {
        try (var coordinator = new LocalFirstCoordinatorSnapshots(control, 60_000, 120_000, 1_000)) {
            var oldReader = coordinator.begin(IsolationLevel.REPEATABLE_READ);
            var writer = coordinator.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(oldReader.txId() < 0 && writer.txId() < 0 && oldReader.txId() != writer.txId());
            var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
            physical.install(segment.lease(), 5_000);
            control.activateSegment(segment, physical);
            coordinator.assertProtected(writer);
            shard.directCommitFenced(physical, segment.lease(), writer.txId(), writer.startTs(), 101,
                    writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:coordinator", new byte[]{8})));
            Assertions.assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
            Assertions.assertEquals(101, control.advancePublishedFast(10));
            var committedReader = coordinator.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(committedReader.startTs() >= 101);
            assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                    IsolationLevel.REPEATABLE_READ), "local:coordinator").isEmpty());
            assertArrayEquals(new byte[]{8}, shard.get(new ReadContext(committedReader.txId(),
                    committedReader.startTs(), IsolationLevel.READ_COMMITTED),
                    "local:coordinator").orElseThrow().value());
            coordinator.release(oldReader.txId());
            coordinator.release(writer.txId());
            coordinator.release(committedReader.txId());
        }
    }

    /**
     * After the coordinator lease lapse, unenclosed sections protect the local commit candidate until the centre fills decision.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentFloorMustProtectLocalCommitAfterCoordinatorLeaseExpires() throws Exception {
        try (var coordinator = new LocalFirstCoordinatorSnapshots(control, 60_000, 120_000, 1_000)) {
            var writer = coordinator.begin(IsolationLevel.REPEATABLE_READ);
            var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
            physical.install(segment.lease(), 5_000);
            control.activateSegment(segment, physical);
            shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                    writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:expired-coordinator", new byte[]{9})));
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.executeUpdate("update mvcc_local_first_coordinator_v1 set expires_at = 1");
            }
            Assertions.assertEquals(1, control.expireCoordinatorLeases(10));
            Assertions.assertEquals(99, control.advanceSafePoint());
            Assertions.assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
            Assertions.assertEquals(101, control.advancePublishedFast(10));
            assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(999, 101,
                    IsolationLevel.READ_COMMITTED), "local:expired-coordinator").orElseThrow().value());
            coordinator.release(writer.txId());
        }
    }

    /**
     * The same transaction release did not go past the earlier PENDING and the first consecutive release suffix after completion of the earlier candidate.
     */
    @Test
    void fusedCompletionMustPublishOnlyContiguousCommittedPrefix() {
        var first = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var second = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), second.txId(), 100, 102,
                second.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:fused:second", new byte[]{2})));
        var later = control.completeCandidatesAndPublish(segment, physical, List.of(second.txId()));
        Assertions.assertEquals(1, later.completed());
        Assertions.assertEquals(100, later.publishedTs());
        Assertions.assertEquals(100, control.currentPublished());
        shard.directCommitFenced(physical, segment.lease(), first.txId(), 100, 101,
                first.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:fused:first", new byte[]{1})));
        var earlier = control.completeCandidatesAndPublish(segment, physical, List.of(first.txId()));
        Assertions.assertEquals(1, earlier.completed());
        Assertions.assertEquals(102, earlier.publishedTs());
        Assertions.assertEquals(0, control.completeCandidatesAndPublish(segment,
                physical, List.of(first.txId())).completed());
        var reader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        Assertions.assertEquals(102, reader.startTs());
        assertArrayEquals(new byte[]{1}, shard.get(new ReadContext(reader.txId(), 102,
                IsolationLevel.READ_COMMITTED), "local:fused:first").orElseThrow().value());
        assertArrayEquals(new byte[]{2}, shard.get(new ReadContext(reader.txId(), 102,
                IsolationLevel.READ_COMMITTED), "local:fused:second").orElseThrow().value());
    }

    /**
     * Shard GC can only be deleted if the centre is closed and the local candidate is released.
     */
    @Test
    void finalizedCandidateGcMustWaitForSealAndPublication() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:gc-finalized", new byte[]{7})));
        assertTrue(control.candidateGcProof(writer.txId()).isEmpty());
        Assertions.assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
        assertTrue(control.candidateGcProof(writer.txId()).isEmpty());
        physical.fence(control.beginFencing(segment, true).physicalLease());
        Assertions.assertEquals(3, control.sealFencedSegment(segment, physical));
        assertTrue(control.candidateGcProof(writer.txId()).isEmpty());
        Assertions.assertEquals(104, control.advancePublishedFast(10));
        var proof = control.candidateGcProof(writer.txId()).orElseThrow();
        Assertions.assertEquals("s1", proof.shardId());
        Assertions.assertEquals(101, proof.candidate().commitTs());
        assertTrue(physical.purgeCandidate(proof));
        assertFalse(physical.purgeCandidate(proof));
        assertTrue(physical.candidate(writer.txId()).isEmpty());
        Assertions.assertEquals(List.of(writer.txId()), control.findPurgeableCandidateIds(Long.MIN_VALUE, 10));
        assertTrue(control.markCandidatePurged(writer.txId(), physical));
        assertFalse(control.markCandidatePurged(writer.txId(), physical));
        assertTrue(control.findPurgeableCandidateIds(Long.MIN_VALUE, 10).isEmpty());
        Assertions.assertEquals(0, control.sealFencedSegment(segment, physical));
        control.releaseSnapshot(writer.txId());
        Assertions.assertEquals(104, control.advanceSafePoint());
        shard.gc(104, 10);
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "local:gc-finalized").orElseThrow().value());
    }

    /**
     * Background is due to take over physical fence and replace local commit, seal, publish and remove candidates.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredOwnerRecoveryMustCompleteAndPurgeLocalCandidate() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:auto-recovered", new byte[]{8})));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                    + "where segment_start = 101");
        }
        var recovery = new LocalFirstPublicationRecoveryManager(control, Map.of("s1", physical));
        var result = recovery.recoverOnce(10, 10);
        Assertions.assertEquals(1, result.sealedSegments());
        Assertions.assertEquals(3, result.abortedSlots());
        Assertions.assertEquals(1, result.purgedCandidates());
        Assertions.assertEquals(0, result.failures());
        Assertions.assertEquals(104, result.publishedTs());
        assertTrue(physical.candidate(writer.txId()).isEmpty());
        assertArrayEquals(new byte[]{8}, shard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "local:auto-recovered").orElseThrow().value());
    }

    /**
     * Shard Installs missing and routers to keep the centre FENCING, recovery later write tombstone and close empty segments.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unavailableShardMustLeaveRecoverableFencingState() throws Exception {
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                    + "where segment_start = 101");
        }
        var unavailable = new LocalFirstPublicationRecoveryManager(control, Map.of());
        var failed = unavailable.recoverOnce(10, 10);
        Assertions.assertEquals(1, failed.failures());
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.FENCING,
                control.segment(101).orElseThrow().status());
        Assertions.assertEquals(100, control.currentPublished());
        var recovered = new LocalFirstPublicationRecoveryManager(control, Map.of("s1", physical))
                .recoverOnce(10, 10);
        Assertions.assertEquals(1, recovered.sealedSegments());
        Assertions.assertEquals(4, recovered.abortedSlots());
        Assertions.assertEquals(104, recovered.publishedTs());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segment.lease(), 5_000)).errorCode());
    }

    /**
     * Two candidates for the entire section have been released and were cleared at one time and recorded in GC at the Centre.
     */
    @Test
    void finalizedSegmentGcMustPurgeAllCandidatesAsOneBatch() {
        var first = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var second = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), first.txId(), 100, 101,
                first.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:gc-batch:first", new byte[]{1})));
        shard.directCommitFenced(physical, segment.lease(), second.txId(), 100, 102,
                second.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:gc-batch:second", new byte[]{2})));
        control.completeCandidates(segment, physical, List.of(first.txId(), second.txId()));
        physical.fence(control.beginFencing(segment, true).physicalLease());
        Assertions.assertEquals(2, control.sealFencedSegment(segment, physical));
        assertTrue(control.segmentGcProof(101).isEmpty());
        Assertions.assertEquals(104, control.advancePublishedFast(10));
        Assertions.assertEquals(1, control.findPurgeableSegments(0, 10).size());
        var proof = control.segmentGcProof(101).orElseThrow();
        Assertions.assertEquals(2, physical.countCandidates(101));
        Assertions.assertEquals(2, physical.purgeFencedSegmentCandidates(proof));
        Assertions.assertEquals(0, physical.purgeFencedSegmentCandidates(proof));
        Assertions.assertEquals(2, control.markSegmentCandidatesPurged(proof, physical));
        Assertions.assertEquals(0, control.markSegmentCandidatesPurged(proof, physical));
        assertTrue(control.findPurgeableSegments(0, 10).isEmpty());
        assertTrue(control.candidateGcProof(first.txId()).isEmpty());
        assertTrue(control.candidateGcProof(second.txId()).isEmpty());
        Assertions.assertEquals(0, control.sealFencedSegment(segment, physical));
    }

    /**
     * The new owner register concurrent does not skip the tailings slot, and the pending allocation closes a reserved section for recovery.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void ownerRegistryConcurrentAllocationMustStayWithinOneSegment() throws Exception {
        try (
                var owner = new LocalFirstSegmentOwnerRegistry(control, physical,
                        () -> 100, 128, 5_000, 1_000, 10_000);
                var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = new java.util.concurrent.CountDownLatch(64);
            var start = new java.util.concurrent.CountDownLatch(1);
            var assignments = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 64; index++) {
                assignments.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return owner.allocate(100).commitTs();
                }));
            }
            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            var unique = new java.util.HashSet<Long>();
            for (var assignment : assignments) {
                assertTrue(unique.add(assignment.get(10, java.util.concurrent.TimeUnit.SECONDS)));
            }
            Assertions.assertEquals(64, unique.size());
            Assertions.assertEquals(101, unique.stream().mapToLong(Long::longValue).min().orElseThrow());
            Assertions.assertEquals(164, unique.stream().mapToLong(Long::longValue).max().orElseThrow());
            assertTrue(control.segment(101).isPresent());
            assertTrue(control.segment(229).isEmpty());
        }
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.ACTIVE,
                control.segment(101).orElseThrow().status());
    }

    /**
     * Once the lease is renewed on both sides, the old distribution object will remain at shard and the patching centre, before the tailings can be safely sealed upon closure.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void ownerRegistryRenewalMustPreserveInFlightAssignment() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        try (
                var owner = new LocalFirstSegmentOwnerRegistry(control, physical,
                        () -> 100, 4, 600, 100, 10_000)) {
            var assignment = owner.allocate(writer.startTs());
            long originalDeadline = assignment.reservation().lease().expiresAtMillis();
            Thread.sleep(750);
            assertTrue(control.segment(101).orElseThrow().lease().expiresAtMillis() > originalDeadline);
            assertTrue(physical.snapshot(101).active());
            shard.directCommitFenced(physical, assignment.reservation().lease(), writer.txId(),
                    writer.startTs(), assignment.commitTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:renewed-owner", new byte[]{3})));
            Assertions.assertEquals(1, control.completeCandidates(assignment.reservation(),
                    physical, List.of(writer.txId())));
            owner.complete(assignment);
        }
        Assertions.assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
                control.segment(101).orElseThrow().status());
        Assertions.assertEquals(104, control.currentPublished());
        assertFalse(physical.snapshot(101).active());
    }

    /**
     * As a result, the paragraph was discontinued and the new allocation was replaced with a follow-up section and taken over to fill in the old candidate.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void ownerRegistryUnknownMustYieldSegmentToRecovery() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        try (
                var owner = new LocalFirstSegmentOwnerRegistry(control, physical,
                        () -> 100, 4, 5_000, 100, 10_000)) {
            var assignment = owner.allocate(writer.startTs());
            shard.directCommitFenced(physical, assignment.reservation().lease(), writer.txId(),
                    writer.startTs(), assignment.commitTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:unknown-owner", new byte[]{4})));
            owner.unknown(assignment);
            Assertions.assertEquals(105, owner.allocate(writer.startTs()).commitTs());
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                        + "where segment_start = 101");
            }
            var recovered = new LocalFirstPublicationRecoveryManager(control, Map.of("s1", physical))
                    .recoverOnce(10, 10);
            Assertions.assertEquals(1, recovered.sealedSegments());
            Assertions.assertEquals(3, recovered.abortedSlots());
            Assertions.assertEquals(104, recovered.publishedTs());
            Assertions.assertEquals(0, recovered.failures());
        }
    }

    /**
     * Background connection budget shall be independent and authentic to database identity.
     */
    @Test
    void backgroundAuthorityMustUseIndependentSameDatabasePool() {
        assertThrows(IllegalArgumentException.class,
                () -> MysqlDatabaseAuthority.requireIndependentSameDatabase(dataSource, dataSource, "control"));
        try (
                HikariDataSource background = (HikariDataSource) MysqlDataSourceFactory.create(
                        MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())) {
            MysqlDatabaseAuthority.requireIndependentSameDatabase(dataSource, background, "control");
        }
    }

    /**
     * Regular closing requires a rollback of bounded cursor until multiple closed candidates are cleared by durable.
     */
    @Test
    void maintenanceCloseMustDrainMoreThanOneCandidatePage() {
        for (int index = 0; index < 3; index++) {
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var segment = control.reserveSegment("s1", "owner-a", "request-" + index,
                    1, 5_000, 100);
            physical.install(segment.lease(), 5_000);
            control.activateSegment(segment, physical);
            shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100,
                    segment.lease().firstTs(), writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:drain:" + index, new byte[]{(byte) index})));
            control.completeCandidates(segment, physical, List.of(writer.txId()));
            physical.fence(control.beginFencing(segment, true).physicalLease());
            control.sealFencedSegment(segment, physical);
            control.releaseSnapshot(writer.txId());
        }
        Assertions.assertEquals(103, control.advancePublishedFast(10));
        try (
                var scheduler = new LocalFirstPublicationMaintenanceScheduler(control,
                        Map.of("s1", physical), Map.of("s1", shard),
                        1, 1, 10, 1_000, 1_000)) {
            Assertions.assertEquals(1, control.findPurgeableSegments(0, 1).size());
        }
        Assertions.assertEquals(0, physical.countCandidates(101));
        Assertions.assertEquals(0, physical.countCandidates(102));
        Assertions.assertEquals(0, physical.countCandidates(103));
        assertTrue(control.findPurgeableSegments(0, 10).isEmpty());
    }

    /**
     * The centre must first remove the finished slot and then delete the referenced Finalized metadata.
     */
    @Test
    void centerGcMustPurgeSlotsBeforeFinalizedMetadata() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:center-gc", new byte[]{5})));
        control.completeCandidates(segment, physical, List.of(writer.txId()));
        physical.fence(control.beginFencing(segment, true).physicalLease());
        control.sealFencedSegment(segment, physical);
        control.advancePublishedFast(10);
        control.releaseSnapshot(writer.txId());
        Assertions.assertEquals(104, control.advanceSafePoint());
        Assertions.assertEquals(0, control.purgeFinalizedMetadata(104, Long.MAX_VALUE, 10));
        Assertions.assertEquals(2, control.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        assertTrue(control.decision(writer.txId()).isPresent());
        var proof = control.segmentGcProof(101).orElseThrow();
        Assertions.assertEquals(1, physical.purgeFencedSegmentCandidates(proof));
        Assertions.assertEquals(1, control.markSegmentCandidatesPurged(proof, physical));
        Assertions.assertEquals(1, control.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        Assertions.assertEquals(1, control.purgeFinalizedMetadata(104, Long.MAX_VALUE, 10));
        assertTrue(control.decision(writer.txId()).isEmpty());
        Assertions.assertEquals(104, control.currentPublished());
        assertArrayEquals(new byte[]{5}, shard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "local:center-gc").orElseThrow().value());
    }

    /**
     * Cycle maintenance moves the same safe point in the order of shard, central slot, Finalized metadata.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void maintenanceGcMustReclaimAgedCenterHistoryAfterShardProof() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 100, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:aged-center", new byte[]{6})));
        control.completeCandidates(segment, physical, List.of(writer.txId()));
        physical.fence(control.beginFencing(segment, true).physicalLease());
        control.sealFencedSegment(segment, physical);
        control.advancePublishedFast(10);
        var proof = control.segmentGcProof(101).orElseThrow();
        physical.purgeFencedSegmentCandidates(proof);
        control.markSegmentCandidatesPurged(proof, physical);
        control.releaseSnapshot(writer.txId());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_slot_v1 set deadline_millis = 1");
            statement.executeUpdate("update mvcc_local_first_txn_v1 set finalized_at = 1");
        }
        try (
                var scheduler = new LocalFirstPublicationMaintenanceScheduler(control,
                        Map.of("s1", physical), Map.of("s1", shard),
                        1, 1, 10, 1_000, 1_000, 0)) {
            assertTrue(scheduler.gcOnce() >= 4);
            Assertions.assertEquals(1, scheduler.gcStats().runCount());
            Assertions.assertEquals(0, scheduler.gcStats().failureCount());
        }
        assertTrue(control.decision(writer.txId()).isEmpty());
        Assertions.assertEquals(104, control.currentPublished());
    }

    /**
     * Physical tombstone cannot be late for installation or writing after removing high watermark with transaction commit.
     */
    @Test
    void retiredFenceWaterlineMustPreventOldSegmentResurrection() {
        var first = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(first.lease(), 5_000);
        control.activateSegment(first, physical);
        physical.fence(control.beginFencing(first, true).physicalLease());
        Assertions.assertEquals(4, control.sealFencedSegment(first, physical));
        Assertions.assertEquals(104, control.advancePublishedFast(10));
        Assertions.assertEquals(104, control.advanceSafePoint());
        assertTrue(control.fenceGcProof(101).isEmpty());

        var second = control.reserveSegment("s1", "owner-a", "request-b", 4, 5_000, 104);
        physical.fence(control.beginFencing(second, true).physicalLease());
        Assertions.assertEquals(4, control.sealFencedSegment(second, physical));
        Assertions.assertEquals(108, control.advancePublishedFast(10));
        Assertions.assertEquals(108, control.advanceSafePoint());

        var proof = control.fenceGcProof(101).orElseThrow();
        Assertions.assertEquals(1, control.findRetirableFences(0, 10).size());
        assertTrue(physical.purgeFence(proof));
        assertFalse(physical.purgeFence(proof));
        assertTrue(physical.isFenceRetired(first.lease()));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> physical.install(first.lease(), 5_000)).errorCode());
        assertFalse(physical.fence(first.lease()));
        assertTrue(physical.scanFenced(first.lease(), 100, 10).isEmpty());
        Assertions.assertEquals(0, control.sealFencedSegment(first, physical));
        assertTrue(control.markFenceRetired(proof, physical));
        assertFalse(control.markFenceRetired(proof, physical));
        assertTrue(control.findRetirableFences(0, 10).isEmpty());
        assertTrue(control.purgeTerminalSlots(108, Long.MAX_VALUE, 10) >= 4);
        Assertions.assertEquals(1, control.purgeSealedSegments(108, Long.MAX_VALUE, 10));
        assertTrue(control.segment(101).isEmpty());
        assertTrue(physical.isFenceRetired(first.lease()));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> physical.install(first.lease(), 5_000)).errorCode());
        var restarted = new MysqlShardSegmentFence("s1", dataSource);
        restarted.initializeIsolated();
        assertTrue(restarted.isFenceRetired(first.lease()));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> restarted.install(first.lease(), 5_000)).errorCode());
    }

    /**
     * Close first to update safe point and then return multiple pages of physics fence, the latest paragraph remains conservative.
     */
    @Test
    void maintenanceCloseMustDrainEligiblePhysicalFences() {
        var segments = new java.util.ArrayList<MysqlLocalFirstPublicationLedger.SegmentReservation>();
        for (int index = 0; index < 3; index++) {
            var segment = control.reserveSegment("s1", "owner-a", "request-" + index,
                    1, 5_000, 100);
            physical.fence(control.beginFencing(segment, true).physicalLease());
            control.sealFencedSegment(segment, physical);
            segments.add(segment);
        }
        Assertions.assertEquals(103, control.advancePublishedFast(10));
        try (
                var scheduler = new LocalFirstPublicationMaintenanceScheduler(control,
                        Map.of("s1", physical), Map.of("s1", shard),
                        1, 1, 10, 1_000, 1_000, 0)) {
            Assertions.assertEquals(0, control.currentSafePoint());
        }
        Assertions.assertEquals(103, control.currentSafePoint());
        assertTrue(physical.isFenceRetired(segments.get(0).lease()));
        assertTrue(physical.isFenceRetired(segments.get(1).lease()));
        assertFalse(physical.isFenceRetired(segments.get(2).lease()));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segments.get(0).lease(), 5_000)).errorCode());
    }
}

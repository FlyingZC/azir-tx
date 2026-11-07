package com.flyingzc.azir.storage.mysql;

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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 真实 MySQL 下验证分片本地提交、批量中心终态与全局发布水位的组合。 */
@Testcontainers
class MysqlLocalFirstCompletionTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("local_first_complete").withUsername("test").withPassword("test");
    private static HikariDataSource dataSource;
    private MysqlLocalFirstPublicationLedger control;
    private MysqlShardSegmentFence physical;
    private MysqlStorageAdapter shard;

    /** 建立本类独占的 MySQL 测试连接池。 */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭真实数据库连接池。 */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /** 清除两条隔离版本时间线并恢复已知分片安全点。 */
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

    /** 已落分片但中心未发布时旧 RR 和新 RC 均受水位约束，完成后新 RC 才看见版本。 */
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
        assertEquals(100, control.currentPublished());
        assertFalse(control.decision(writer.txId()).isPresent());
        var earlyReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertEquals(100, earlyReader.startTs());
        assertTrue(shard.get(new ReadContext(earlyReader.txId(), earlyReader.startTs(),
                IsolationLevel.READ_COMMITTED), "local:visible").isEmpty());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:visible").isEmpty());
        assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
        assertEquals(0, control.completeCandidates(segment, physical, List.of(writer.txId())));
        assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        assertEquals(101, control.advancePublished(10));
        var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertEquals(101, newReader.startTs());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:visible").isEmpty());
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(newReader.txId(), newReader.startTs(),
                IsolationLevel.READ_COMMITTED), "local:visible").orElseThrow().value());
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(earlyReader.txId());
        control.releaseSnapshot(newReader.txId());
    }

    /** 第三版分片例程同样只在中心持久发布后对新 RC 可见，旧 RR 固定旧快照。 */
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
        assertEquals(100, earlyRc.startTs());
        assertTrue(shard.get(new ReadContext(earlyRc.txId(), earlyRc.startTs(),
                IsolationLevel.READ_COMMITTED), "local:v3-visible").isEmpty());
        assertEquals(101, control.completeProvedCandidatesAndPublish(segment, physical,
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

    /** 整段本地候选全部持久后，一次物理封段即可补齐中心终态并发布。 */
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
        assertEquals(100, control.currentPublished());
        owner.completePhysicalAndSeal(assigned);
        assertEquals(101, control.currentPublished());
        assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
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

    /** 本地成功后签发的前台证明只可由原分片实例完成，中心重放仍核对持久身份。 */
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
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.completeProvedCandidates(segment,
                        new MysqlShardSegmentFence("s1", dataSource), List.of(proof))).errorCode());
        assertEquals(1, control.completeProvedCandidatesAndPublish(segment, physical, List.of(proof)).completed());
        assertEquals(0, control.completeProvedCandidatesAndPublish(segment, physical, List.of(proof)).completed());
        assertEquals(101, control.currentPublished());
        assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        control.releaseSnapshot(writer.txId());
    }

    /** 一个中心批次中第二槽 SQL 失败时，第一槽及两笔元数据也须回滚。 */
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
        assertEquals(100, control.advancePublished(10));
        assertEquals(2, control.completeCandidates(segment, physical, List.of(second.txId(), first.txId())));
        assertEquals(102, control.advancePublished(10));
    }

    /** 控制已开始接管且分片已物理 fencing 后，旧代数的持久候选仍须补成 COMMITTED。 */
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
        assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
        assertEquals(0, control.completeCandidates(control.segment(101).orElseThrow(),
                physical, List.of(writer.txId())));
        assertEquals(101, control.advancePublished(10));
    }

    /** 物理 fencing 后只把本地候选补成提交，其余尾槽中止并释放连续发布。 */
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
        assertEquals(3, control.sealFencedSegment(segment, physical));
        assertEquals(0, control.sealFencedSegment(segment, physical));
        assertEquals(101, control.decision(writer.txId()).orElseThrow().commitTs());
        assertEquals(104, control.advancePublished(10));
        var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertEquals(104, newReader.startTs());
        assertTrue(shard.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "local:recovery").isEmpty());
        assertArrayEquals(new byte[]{4}, shard.get(new ReadContext(newReader.txId(), newReader.startTs(),
                IsolationLevel.READ_COMMITTED), "local:recovery").orElseThrow().value());
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
                control.segment(101).orElseThrow().status());
    }

    /** 分片从未安装也先写 fencing tombstone，再把整段空槽封存为中止。 */
    @Test
    void missingPhysicalInstallationMustAbortOnlyAfterTombstone() {
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        var request = control.beginFencing(segment, true);
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> control.sealFencedSegment(segment, physical)).errorCode());
        assertTrue(physical.fence(request.physicalLease()));
        assertEquals(4, control.sealFencedSegment(segment, physical));
        assertEquals(104, control.advancePublished(10));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segment.lease(), 5_000)).errorCode());
    }

    /** 尾槽 SQL 失败不得提前标记 SEALED，重试仍按同一分片证明完成。 */
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
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.FENCING,
                control.segment(101).orElseThrow().status());
        assertEquals(101, control.advancePublishedFast(10));
        assertEquals(3, control.sealFencedSegment(segment, physical));
        assertEquals(104, control.advancePublishedFast(10));
    }

    /** 仍在的中心快照租约若与本地候选开始时间不同，不得发布局部版本。 */
    @Test
    void liveSnapshotLeaseMismatchMustRejectCenterCompletion() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 80);
        physical.install(segment.lease(), 5_000);
        control.activateSegment(segment, physical);
        shard.directCommitFenced(physical, segment.lease(), writer.txId(), 99, 101,
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:wrong-snapshot", new byte[]{6})));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> control.completeCandidates(segment, physical, List.of(writer.txId()))).errorCode());
        assertFalse(control.decision(writer.txId()).isPresent());
        assertEquals(100, control.advancePublished(10));
    }

    /** 批次中一条分片候选缺少匹配终态时，整批中心槽保持 PENDING。 */
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
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.completeCandidates(segment,
                        physical, List.of(first.txId(), second.txId()))).errorCode());
        assertFalse(control.decision(first.txId()).isPresent());
        assertFalse(control.decision(second.txId()).isPresent());
        assertEquals(100, control.advancePublished(10));
    }

    /** 并发本地 CAS 失败不能误判整段耗尽并留下无法发布的空槽。 */
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
            assertEquals(64, unique.size());
            assertEquals(101, unique.stream().mapToLong(Long::longValue).min().orElseThrow());
            assertEquals(164, unique.stream().mapToLong(Long::longValue).max().orElseThrow());
            assertTrue(control.segment(101).isPresent());
            assertTrue(control.segment(229).isEmpty());
        }
    }

    /** 协调器负事务号在无逐事务租约下仍保持旧 RR 与新 RC 的全局发布视图。 */
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
            assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
            assertEquals(101, control.advancePublishedFast(10));
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

    /** 协调器租约失效后，未封存段仍保护已本地提交的候选直到中心补决议。 */
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
            assertEquals(1, control.expireCoordinatorLeases(10));
            assertEquals(99, control.advanceSafePoint());
            assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
            assertEquals(101, control.advancePublishedFast(10));
            assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(999, 101,
                    IsolationLevel.READ_COMMITTED), "local:expired-coordinator").orElseThrow().value());
            coordinator.release(writer.txId());
        }
    }

    /** 同事务发布遇到较早 PENDING 时不越过，较早候选完成后一次释放连续后缀。 */
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
        assertEquals(1, later.completed());
        assertEquals(100, later.publishedTs());
        assertEquals(100, control.currentPublished());
        shard.directCommitFenced(physical, segment.lease(), first.txId(), 100, 101,
                first.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:fused:first", new byte[]{1})));
        var earlier = control.completeCandidatesAndPublish(segment, physical, List.of(first.txId()));
        assertEquals(1, earlier.completed());
        assertEquals(102, earlier.publishedTs());
        assertEquals(0, control.completeCandidatesAndPublish(segment,
                physical, List.of(first.txId())).completed());
        var reader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertEquals(102, reader.startTs());
        assertArrayEquals(new byte[]{1}, shard.get(new ReadContext(reader.txId(), 102,
                IsolationLevel.READ_COMMITTED), "local:fused:first").orElseThrow().value());
        assertArrayEquals(new byte[]{2}, shard.get(new ReadContext(reader.txId(), 102,
                IsolationLevel.READ_COMMITTED), "local:fused:second").orElseThrow().value());
    }

    /** 只有中心已封段且发布的本地候选才可删除，之后分片 GC 才能继续推进。 */
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
        assertEquals(1, control.completeCandidates(segment, physical, List.of(writer.txId())));
        assertTrue(control.candidateGcProof(writer.txId()).isEmpty());
        physical.fence(control.beginFencing(segment, true).physicalLease());
        assertEquals(3, control.sealFencedSegment(segment, physical));
        assertTrue(control.candidateGcProof(writer.txId()).isEmpty());
        assertEquals(104, control.advancePublishedFast(10));
        var proof = control.candidateGcProof(writer.txId()).orElseThrow();
        assertEquals("s1", proof.shardId());
        assertEquals(101, proof.candidate().commitTs());
        assertTrue(physical.purgeCandidate(proof));
        assertFalse(physical.purgeCandidate(proof));
        assertTrue(physical.candidate(writer.txId()).isEmpty());
        assertEquals(List.of(writer.txId()), control.findPurgeableCandidateIds(Long.MIN_VALUE, 10));
        assertTrue(control.markCandidatePurged(writer.txId(), physical));
        assertFalse(control.markCandidatePurged(writer.txId(), physical));
        assertTrue(control.findPurgeableCandidateIds(Long.MIN_VALUE, 10).isEmpty());
        assertEquals(0, control.sealFencedSegment(segment, physical));
        control.releaseSnapshot(writer.txId());
        assertEquals(104, control.advanceSafePoint());
        shard.gc(104, 10);
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "local:gc-finalized").orElseThrow().value());
    }

    /** 后台到期接管先物理 fence，再补本地提交、封尾、发布和删除候选。 */
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
        assertEquals(1, result.sealedSegments());
        assertEquals(3, result.abortedSlots());
        assertEquals(1, result.purgedCandidates());
        assertEquals(0, result.failures());
        assertEquals(104, result.publishedTs());
        assertTrue(physical.candidate(writer.txId()).isEmpty());
        assertArrayEquals(new byte[]{8}, shard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "local:auto-recovered").orElseThrow().value());
    }

    /** 分片安装缺失与路由暂不可用时保留中心 FENCING，恢复后写 tombstone 并封空段。 */
    @Test
    void unavailableShardMustLeaveRecoverableFencingState() throws Exception {
        var segment = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                    + "where segment_start = 101");
        }
        var unavailable = new LocalFirstPublicationRecoveryManager(control, Map.of());
        var failed = unavailable.recoverOnce(10, 10);
        assertEquals(1, failed.failures());
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.FENCING,
                control.segment(101).orElseThrow().status());
        assertEquals(100, control.currentPublished());
        var recovered = new LocalFirstPublicationRecoveryManager(control, Map.of("s1", physical))
                .recoverOnce(10, 10);
        assertEquals(1, recovered.sealedSegments());
        assertEquals(4, recovered.abortedSlots());
        assertEquals(104, recovered.publishedTs());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segment.lease(), 5_000)).errorCode());
    }

    /** 已发布整段的两笔候选一次清理，并在中心同批记录 GC 完成。 */
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
        assertEquals(2, control.sealFencedSegment(segment, physical));
        assertTrue(control.segmentGcProof(101).isEmpty());
        assertEquals(104, control.advancePublishedFast(10));
        assertEquals(1, control.findPurgeableSegments(0, 10).size());
        var proof = control.segmentGcProof(101).orElseThrow();
        assertEquals(2, physical.countCandidates(101));
        assertEquals(2, physical.purgeFencedSegmentCandidates(proof));
        assertEquals(0, physical.purgeFencedSegmentCandidates(proof));
        assertEquals(2, control.markSegmentCandidatesPurged(proof, physical));
        assertEquals(0, control.markSegmentCandidatesPurged(proof, physical));
        assertTrue(control.findPurgeableSegments(0, 10).isEmpty());
        assertTrue(control.candidateGcProof(first.txId()).isEmpty());
        assertTrue(control.candidateGcProof(second.txId()).isEmpty());
        assertEquals(0, control.sealFencedSegment(segment, physical));
    }

    /** 新 owner 注册器并发取号不跳尾槽，未决分配使关闭保留段供恢复。 */
    @Test
    void ownerRegistryConcurrentAllocationMustStayWithinOneSegment() throws Exception {
        try (var owner = new LocalFirstSegmentOwnerRegistry(control, physical,
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
            assertEquals(64, unique.size());
            assertEquals(101, unique.stream().mapToLong(Long::longValue).min().orElseThrow());
            assertEquals(164, unique.stream().mapToLong(Long::longValue).max().orElseThrow());
            assertTrue(control.segment(101).isPresent());
            assertTrue(control.segment(229).isEmpty());
        }
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.ACTIVE,
                control.segment(101).orElseThrow().status());
    }

    /** 双侧续租后旧分配对象仍能落分片和补中心，关闭时再安全封存尾槽。 */
    @Test
    void ownerRegistryRenewalMustPreserveInFlightAssignment() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        try (var owner = new LocalFirstSegmentOwnerRegistry(control, physical,
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
            assertEquals(1, control.completeCandidates(assignment.reservation(),
                    physical, List.of(writer.txId())));
            owner.complete(assignment);
        }
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
                control.segment(101).orElseThrow().status());
        assertEquals(104, control.currentPublished());
        assertFalse(physical.snapshot(101).active());
    }

    /** owner 未决结果使该段停止续租，新分配改用后续段并由接管补旧候选。 */
    @Test
    void ownerRegistryUnknownMustYieldSegmentToRecovery() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        try (var owner = new LocalFirstSegmentOwnerRegistry(control, physical,
                () -> 100, 4, 5_000, 100, 10_000)) {
            var assignment = owner.allocate(writer.startTs());
            shard.directCommitFenced(physical, assignment.reservation().lease(), writer.txId(),
                    writer.startTs(), assignment.commitTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ,
                    List.of(Mutation.put("local:unknown-owner", new byte[]{4})));
            owner.unknown(assignment);
            assertEquals(105, owner.allocate(writer.startTs()).commitTs());
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.executeUpdate("update mvcc_local_first_segment_v1 set expires_at = 1 "
                        + "where segment_start = 101");
            }
            var recovered = new LocalFirstPublicationRecoveryManager(control, Map.of("s1", physical))
                    .recoverOnce(10, 10);
            assertEquals(1, recovered.sealedSegments());
            assertEquals(3, recovered.abortedSlots());
            assertEquals(104, recovered.publishedTs());
            assertEquals(0, recovered.failures());
        }
    }

    /** 后台连接预算须独立且真实服务器与数据库身份一致。 */
    @Test
    void backgroundAuthorityMustUseIndependentSameDatabasePool() {
        assertThrows(IllegalArgumentException.class,
                () -> MysqlDatabaseAuthority.requireIndependentSameDatabase(dataSource, dataSource, "control"));
        try (HikariDataSource background = (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())) {
            MysqlDatabaseAuthority.requireIndependentSameDatabase(dataSource, background, "control");
        }
    }

    /** 正常关闭须回卷有界游标直到多个已封段的候选都被持久清理。 */
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
        assertEquals(103, control.advancePublishedFast(10));
        try (var scheduler = new LocalFirstPublicationMaintenanceScheduler(control,
                Map.of("s1", physical), Map.of("s1", shard),
                1, 1, 10, 1_000, 1_000)) {
            assertEquals(1, control.findPurgeableSegments(0, 1).size());
        }
        assertEquals(0, physical.countCandidates(101));
        assertEquals(0, physical.countCandidates(102));
        assertEquals(0, physical.countCandidates(103));
        assertTrue(control.findPurgeableSegments(0, 10).isEmpty());
    }

    /** 中心必须先删已完成槽，再删失去槽引用的 finalized 元数据。 */
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
        assertEquals(104, control.advanceSafePoint());
        assertEquals(0, control.purgeFinalizedMetadata(104, Long.MAX_VALUE, 10));
        assertEquals(2, control.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        assertTrue(control.decision(writer.txId()).isPresent());
        var proof = control.segmentGcProof(101).orElseThrow();
        assertEquals(1, physical.purgeFencedSegmentCandidates(proof));
        assertEquals(1, control.markSegmentCandidatesPurged(proof, physical));
        assertEquals(1, control.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        assertEquals(1, control.purgeFinalizedMetadata(104, Long.MAX_VALUE, 10));
        assertTrue(control.decision(writer.txId()).isEmpty());
        assertEquals(104, control.currentPublished());
        assertArrayEquals(new byte[]{5}, shard.get(new ReadContext(999, 104,
                IsolationLevel.READ_COMMITTED), "local:center-gc").orElseThrow().value());
    }

    /** 周期维护按分片、中心槽、finalized 元数据顺序推进相同安全点。 */
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
        try (var scheduler = new LocalFirstPublicationMaintenanceScheduler(control,
                Map.of("s1", physical), Map.of("s1", shard),
                1, 1, 10, 1_000, 1_000, 0)) {
            assertTrue(scheduler.gcOnce() >= 4);
            assertEquals(1, scheduler.gcStats().runCount());
            assertEquals(0, scheduler.gcStats().failureCount());
        }
        assertTrue(control.decision(writer.txId()).isEmpty());
        assertEquals(104, control.currentPublished());
    }

    /** 物理墓碑删除与高水位同事务提交后，旧 owner 不能迟到安装或写入。 */
    @Test
    void retiredFenceWaterlineMustPreventOldSegmentResurrection() {
        var first = control.reserveSegment("s1", "owner-a", "request-a", 4, 5_000, 100);
        physical.install(first.lease(), 5_000);
        control.activateSegment(first, physical);
        physical.fence(control.beginFencing(first, true).physicalLease());
        assertEquals(4, control.sealFencedSegment(first, physical));
        assertEquals(104, control.advancePublishedFast(10));
        assertEquals(104, control.advanceSafePoint());
        assertTrue(control.fenceGcProof(101).isEmpty());

        var second = control.reserveSegment("s1", "owner-a", "request-b", 4, 5_000, 104);
        physical.fence(control.beginFencing(second, true).physicalLease());
        assertEquals(4, control.sealFencedSegment(second, physical));
        assertEquals(108, control.advancePublishedFast(10));
        assertEquals(108, control.advanceSafePoint());

        var proof = control.fenceGcProof(101).orElseThrow();
        assertEquals(1, control.findRetirableFences(0, 10).size());
        assertTrue(physical.purgeFence(proof));
        assertFalse(physical.purgeFence(proof));
        assertTrue(physical.isFenceRetired(first.lease()));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> physical.install(first.lease(), 5_000)).errorCode());
        assertFalse(physical.fence(first.lease()));
        assertTrue(physical.scanFenced(first.lease(), 100, 10).isEmpty());
        assertEquals(0, control.sealFencedSegment(first, physical));
        assertTrue(control.markFenceRetired(proof, physical));
        assertFalse(control.markFenceRetired(proof, physical));
        assertTrue(control.findRetirableFences(0, 10).isEmpty());
        assertTrue(control.purgeTerminalSlots(108, Long.MAX_VALUE, 10) >= 4);
        assertEquals(1, control.purgeSealedSegments(108, Long.MAX_VALUE, 10));
        assertTrue(control.segment(101).isEmpty());
        assertTrue(physical.isFenceRetired(first.lease()));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> physical.install(first.lease(), 5_000)).errorCode());
        var restarted = new MysqlShardSegmentFence("s1", dataSource);
        restarted.initializeIsolated();
        assertTrue(restarted.isFenceRetired(first.lease()));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> restarted.install(first.lease(), 5_000)).errorCode());
    }

    /** 关闭先更新安全点再回卷多页物理栅栏，最新末段仍保守保留。 */
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
        assertEquals(103, control.advancePublishedFast(10));
        try (var scheduler = new LocalFirstPublicationMaintenanceScheduler(control,
                Map.of("s1", physical), Map.of("s1", shard),
                1, 1, 10, 1_000, 1_000, 0)) {
            assertEquals(0, control.currentSafePoint());
        }
        assertEquals(103, control.currentSafePoint());
        assertTrue(physical.isFenceRetired(segments.get(0).lease()));
        assertTrue(physical.isFenceRetired(segments.get(1).lease()));
        assertFalse(physical.isFenceRetired(segments.get(2).lease()));
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class,
                        () -> physical.install(segments.get(0).lease(), 5_000)).errorCode());
    }
}

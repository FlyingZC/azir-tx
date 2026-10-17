package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.publication.PublicationSegmentLease;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 真实 MySQL 下验证分片 owner 安装、代数 fencing 和在途本地事务串行化。 */
@Testcontainers
class MysqlShardSegmentFenceTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("local_segment_fence").withUsername("test").withPassword("test");
    private static HikariDataSource dataSource;
    private MysqlShardSegmentFence fence;
    private MysqlStorageAdapter shard;

    /** 建立只供新协议栅栏测试使用的隔离连接池。 */
    @BeforeAll
    static void openDatabase() {
        dataSource = (HikariDataSource) MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭测试持有的物理连接池。 */
    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /** 每个用例重新创建完全独立的版本化本地栅栏。 */
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

    /** 安装重放不延长租约，范围重叠与其他 owner 都不能取得同一时间槽。 */
    @Test
    void installReplayMustPreserveIdentityAndRejectOverlap() {
        var owner = lease(101, 104, "owner-a");
        var installed = fence.install(owner, 60_000);
        var replay = fence.install(owner, 60_000);
        assertEquals(installed.expiresAtMillis(), replay.expiresAtMillis());
        assertEquals(1, replay.generation());
        assertTrue(replay.active());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> fence.install(lease(103, 106, "owner-b"),
                        60_000)).errorCode());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> fence.install(lease(101, 104, "owner-b"),
                        60_000)).errorCode());
        assertTrue(fence.renew(owner, 60_000).active());
    }

    /** 排他接管等待持共享栅栏的本地提交结束，旧代数之后不能新增提交。 */
    @Test
    void fencingMustWaitForInFlightLocalCommit() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        try (Connection writer = dataSource.getConnection();
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
        assertEquals(2, fence.snapshot(owner.firstTs()).generation());
        assertFalse(fence.fence(owner));
        try (Connection late = dataSource.getConnection()) {
            late.setAutoCommit(false);
            assertFalse(fence.lockForCommit(late, owner, 101));
            late.rollback();
        }
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.renew(owner, 60_000)).errorCode());
    }

    /** 分片租约到期后即使中心尚未接管，也不能再开启新的本地提交。 */
    @Test
    void expiredLocalLeaseMustRejectNewCommitAndRenewal() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 20);
        Thread.sleep(50);
        try (Connection late = dataSource.getConnection()) {
            late.setAutoCommit(false);
            assertEquals(ErrorCode.TRANSACTION_ABORTED,
                    assertThrows(MvccException.class, () -> fence.lockForCommit(late, owner, 101)).errorCode());
            late.rollback();
        }
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.renew(owner, 60_000)).errorCode());
        assertTrue(fence.fence(owner));
    }

    /** 分片安装尚未落盘时接管须写入墓碑，阻止迟到安装和首次物理提交。 */
    @Test
    void missingFenceMustPersistTombstoneBeforeCenterSealsSlots() {
        var owner = lease(101, 104, "owner-a");
        assertTrue(fence.fence(owner));
        assertFalse(fence.snapshot(101).active());
        assertEquals(2, fence.snapshot(101).generation());
        assertFalse(fence.fence(owner));
        assertTrue(fence.scanFenced(owner, 100, 10).isEmpty());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.install(owner, 60_000)).errorCode());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:tombstone", new byte[]{1})))).errorCode());
        assertTrue(fence.candidate(10).isEmpty());
        assertTrue(shard.participantRecord(10).isEmpty());
    }

    /** 安装与缺行 fencing 无论谁先执行，最终都只能留下不可再提交的物理栅栏。 */
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
                    } catch (MvccException ex) {
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
                assertEquals(2, fence.snapshot(first).generation());
                assertEquals(ErrorCode.TRANSACTION_ABORTED,
                        assertThrows(MvccException.class, () -> fence.install(owner, 60_000)).errorCode());
            }
        }
    }

    /** 分片版本、参与者终态和本地证明同事务提交；物理 fencing 后集合保持稳定。 */
    @Test
    void localCommitCandidateMustRemainRecoverableAfterFencing() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        List<Mutation> writes = List.of(Mutation.put("local:first", new byte[]{7}));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        var proof = fence.candidate(10).orElseThrow();
        assertEquals(101, proof.commitTs());
        assertEquals(100, proof.startTs());
        assertEquals(TxStatus.COMMITTED, shard.participantRecord(10).orElseThrow().status());
        assertArrayEquals(new byte[]{7}, shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:first").orElseThrow().value());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> fence.scanFenced(owner, 100, 10)).errorCode());
        assertTrue(fence.fence(owner));
        assertEquals(List.of(proof), fence.scanFenced(owner, 100, 10));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:first", new byte[]{8})))).errorCode());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:late", new byte[]{1})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(shard.participantRecord(11).isEmpty());
    }

    /** 写冲突不得产生候选证明或泄漏局部版本，接管扫描只看到先提交事务。 */
    @Test
    void conflictMustNotLeaveLocalCandidate() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:conflict", new byte[]{1})));
        assertEquals(ErrorCode.WRITE_CONFLICT,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:conflict", new byte[]{2})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(shard.participantRecord(11).isEmpty());
        assertTrue(fence.fence(owner));
        assertEquals(1, fence.scanFenced(owner, 100, 10).size());
    }

    /** 候选插入失败须回滚已经写入的版本和本地 COMMITTED 终态。 */
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

    /** 未完成中心决议的本地证明禁止 GC 越过其开始快照。 */
    @Test
    void localCandidateMustPinShardSafePoint() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ,
                List.of(Mutation.put("local:gc-protected", new byte[]{9})));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> shard.gc(101, 10)).errorCode());
        shard.gc(100, 10);
        assertTrue(fence.candidate(10).isPresent());
        assertEquals(TxStatus.COMMITTED, shard.participantRecord(10).orElseThrow().status());
        assertArrayEquals(new byte[]{9}, shard.get(new ReadContext(999, 101,
                IsolationLevel.READ_COMMITTED), "local:gc-protected").orElseThrow().value());
    }

    /** 单键服务器端例程在物理 fencing、重放和旧快照写冲突下保持原子证明。 */
    @Test
    void singleKeyProcedureMustPreserveFencingReplayAndConflict() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedure();
        List<Mutation> writes = List.of(Mutation.put("local:procedure", new byte[]{6}));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        assertEquals(101, fence.verifiedCandidate(10).orElseThrow().commitTs());
        assertEquals(ErrorCode.WRITE_CONFLICT,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:procedure", new byte[]{7})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(fence.fence(owner));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        12, 100, 103, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:late-procedure", new byte[]{8})))).errorCode());
        assertEquals(1, fence.scanFenced(owner, 100, 10).size());
    }

    /** 第三版直接终态例程仍须校验写冲突、旧代数重放及封段后迟到请求。 */
    @Test
    void singleKeyProcedureV3MustPreserveFencingReplayAndConflict() {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        List<Mutation> writes = List.of(Mutation.put("local:procedure-v3", new byte[]{6}));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        assertEquals(101, fence.verifiedCandidate(10).orElseThrow().commitTs());
        assertEquals(TxStatus.COMMITTED, shard.participantRecord(10).orElseThrow().status());
        assertEquals(ErrorCode.WRITE_CONFLICT,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        11, 100, 102, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:procedure-v3", new byte[]{7})))).errorCode());
        assertTrue(fence.candidate(11).isEmpty());
        assertTrue(fence.fence(owner));
        shard.directCommitFenced(fence, owner, 10, 100, 101, 60_000,
                IsolationLevel.REPEATABLE_READ, writes);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        10, 100, 101, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:procedure-v3", new byte[]{9})))).errorCode());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> shard.directCommitFenced(fence, owner,
                        12, 100, 103, 60_000, IsolationLevel.REPEATABLE_READ,
                        List.of(Mutation.put("local:late-procedure-v3", new byte[]{8})))).errorCode());
        assertEquals(1, fence.scanFenced(owner, 100, 10).size());
    }

    /** 并发同事务号只能有一份不可逆版本与候选，失败调用的本地写必须回滚。 */
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
                } catch (MvccException ex) {
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
                } catch (MvccException ex) {
                    return false;
                }
            });
            start.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS) ^ second.get(5, TimeUnit.SECONDS));
        }
        var proof = fence.verifiedCandidate(10).orElseThrow();
        assertEquals(proof.commitTs(), shard.participantRecord(10).orElseThrow().commitTs());
        var reader = new ReadContext(999, 102, IsolationLevel.READ_COMMITTED);
        assertTrue(shard.get(reader, "local:v3-race:a").isPresent()
                ^ shard.get(reader, "local:v3-race:b").isPresent());
    }

    /** 两笔旧快照写在业务键锁前排队，后取得锁者必须看见先提交的版本。 */
    @Test
    void singleKeyProcedureV3MustRecheckConflictAfterKeyLockWait() throws Exception {
        var owner = lease(101, 104, "owner-a");
        fence.install(owner, 60_000);
        fence.enableSingleKeyProcedureV3();
        try (Connection blocker = dataSource.getConnection(); Statement statement = blocker.createStatement()) {
            statement.execute("insert into mvcc_key_lock(user_key) values ('local:v3-stale-snapshot')");
            blocker.setAutoCommit(false);
            try (var locked = statement.executeQuery("select user_key from mvcc_key_lock "
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
                    } catch (MvccException ex) {
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
                    } catch (MvccException ex) {
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

    /** 回滚与第三版直接提交竞赛只能留下 COMMITTED 候选或 ABORTED 终态之一。 */
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
                } catch (MvccException ex) {
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
                assertEquals(TxStatus.COMMITTED, status);
                assertTrue(fence.candidate(10).isPresent());
                assertArrayEquals(new byte[]{3}, shard.get(new ReadContext(999, 101,
                        IsolationLevel.READ_COMMITTED), "local:v3-abort-race").orElseThrow().value());
            } else {
                assertEquals(TxStatus.ABORTED, status);
                assertTrue(fence.candidate(10).isEmpty());
                assertTrue(shard.get(new ReadContext(999, 101,
                        IsolationLevel.READ_COMMITTED), "local:v3-abort-race").isEmpty());
            }
        }
    }

    /** 原版单键调用的候选插入失败也须回滚本地版本和参与者终态。 */
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

    /** 第三版候选插入失败必须同时回滚数据版本和直接写入的 COMMITTED 终态。 */
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

    /** 构造一个只用于本地 shard fencing 的持久控制段身份。 */
    private PublicationSegmentLease lease(long first, long last, String ownerId) {
        return new PublicationSegmentLease(first, last, ownerId, 1, Long.MAX_VALUE);
    }
}

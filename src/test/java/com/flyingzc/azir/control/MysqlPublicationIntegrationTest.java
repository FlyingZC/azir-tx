package com.flyingzc.azir.control;

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

import static org.junit.jupiter.api.Assertions.*;

/** MySQL 发布屏障、持久化序列和合并开始路径的集成回归。 */
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

    /** 两个独立单连接池绑定同一数据库，并清空本测试自己的控制面表。 */
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

    /** 每个测试释放自己的连接池。 */
    @AfterEach
    void close() {
        if (firstPool != null) {
            firstPool.close();
        }
        if (secondPool != null) {
            secondPool.close();
        }
    }

    /** 合并校验必须使用数据库时钟；截止时间相等已到期，且查询不得修改原租约。 */
    @Test
    void combinedLeaseValidationHonorsDatabaseClockAndOriginalDeadline() {
        long deadline = 2_000_000_000_000L;
        assertFalse(first.isSnapshotLeaseValid(1, deadline));
        first.registerSnapshot(1, 1, deadline);
        first.withPublicationLock(() -> {
            try (Connection borrowed = MysqlConnectionContext.connection(firstPool);
                 var statement = borrowed.createStatement()) {
                try {
                    statement.execute("set timestamp = 1999999999");
                    assertTrue(first.isSnapshotLeaseValid(1, deadline));
                    assertFalse(first.isSnapshotLeaseValid(1, deadline + 1));
                    statement.execute("set timestamp = 2000000000");
                    assertFalse(first.isSnapshotLeaseValid(1, deadline));
                    assertEquals(deadline, first.snapshotExpiry(1).orElseThrow());
                } finally {
                    // 归还持锁连接前恢复真实数据库时钟，避免污染后续用例。
                    statement.execute("set timestamp = 0");
                }
            } catch (SQLException ex) {
                throw new IllegalStateException(ex);
            }
            return null;
        });
        second.releaseSnapshot(1);
        assertFalse(first.isSnapshotLeaseValid(1, deadline));
    }

    /** 已释放的 MySQL 租约必须在事务管理器读取和提交入口拒绝，不能只验证存储工具方法。 */
    @Test
    void releasedMysqlLeaseRejectsReadAndCommit() {
        try (var manager = new TransactionManager(tso, first, TransactionOptions.defaults(), Map.of())) {
            var transaction = manager.begin(IsolationLevel.REPEATABLE_READ);
            second.releaseSnapshot(transaction.txId());
            assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                    assertThrows(MvccException.class, () -> manager.beginReadStatement(transaction)).errorCode());
            assertEquals(ErrorCode.TRANSACTION_TIMEOUT,
                    assertThrows(MvccException.class, () -> manager.commit(transaction)).errorCode());
            assertEquals(TxStatus.ACTIVE, first.get(transaction.txId()).orElseThrow().status());
            manager.rollback(transaction);
        }
    }

    /** 单分片条件决议覆盖新记录和本地占位，并保证跨实例重放使用原提交时间戳。 */
    @Test
    void singleShardConditionalDecisionPreservesReplayAndPendingTimestamp() {
        first.createWithParticipants(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a"));
        first.markLocalCommitPending(1, 10);
        first.persistSingleShardCommitted(1, "a", 10);
        second.persistSingleShardCommitted(1, "a", 10);
        assertEquals(TxStatus.COMMITTED, second.get(1).orElseThrow().status());
        assertEquals(10L, second.get(1).orElseThrow().commitTs());
        assertDoesNotThrow(second::assertNoPendingPublications);
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "a", 11));
        assertEquals(10L, first.get(1).orElseThrow().commitTs());

        first.createWithParticipants(2, 2, IsolationLevel.READ_COMMITTED, Set.of("a"));
        first.persistSingleShardCommitted(2, "a", 12);
        assertEquals(TxStatus.COMMITTED, second.get(2).orElseThrow().status());
        assertEquals(Set.of("a"), second.get(2).orElseThrow().participants());
    }

    /** 错误时间戳、错误参与者及已中止状态不能被单分片条件更新覆盖。 */
    @Test
    void singleShardConditionalDecisionRejectsMismatchedOrAbortedState() {
        first.createWithParticipants(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a"));
        first.markLocalCommitPending(1, 10);
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "a", 11));
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "b", 10));
        var pending = first.get(1).orElseThrow();
        assertEquals(TxStatus.PREPARING, pending.status());
        assertEquals(10L, pending.commitTs());
        assertEquals(Set.of("a"), pending.participants());
        first.markAborted(1);
        assertThrows(MvccException.class, () -> second.persistSingleShardCommitted(1, "a", 10));
        assertEquals(TxStatus.ABORTED, first.get(1).orElseThrow().status());
        assertNull(first.get(1).orElseThrow().commitTs());
    }

    /** 数据库访问失败保留存储异常，不能返回有效租约。 */
    @Test
    void combinedLeaseValidationFailsClosedOnStorageFailure() {
        firstPool.close();
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, () -> first.isSnapshotLeaseValid(1, Long.MAX_VALUE)).errorCode());
    }

    /** 两个元数据对象重入同域屏障，TSO、服务器时钟与安全点只占用一条持锁连接。 */
    @Test
    void sameDatabaseAndSingleConnectionPoolsReusePublicationConnection() throws SQLException {
        var otherTso = new MysqlTimestampOracle(secondPool);
        otherTso.initialize(100);
        try (Connection occupied = secondPool.getConnection()) {
            long result = first.withPublicationLock(() -> second.withPublicationLock(() -> {
                long txId = otherTso.nextTimestamp();
                first.registerSnapshot(txId, txId, first.currentTimeMillis() + 60_000);
                first.create(txId, txId, IsolationLevel.REPEATABLE_READ);
                assertEquals(100, new GcManager(first, tso, safePoint, Map.of()).advanceSafePoint());
                assertEquals(1, firstPool.getHikariPoolMXBean().getActiveConnections());
                return txId;
            }));
            assertEquals(101, result);
            assertFalse(occupied.isClosed());
        }
    }

    /** 回调异常不会让命名锁随连接回池而泄漏。 */
    @Test
    void callbackFailureReleasesDatabaseLock() {
        assertThrows(IllegalArgumentException.class, () -> first.withPublicationLock(() -> {
            first.create(1, 1, IsolationLevel.REPEATABLE_READ);
            throw new IllegalArgumentException("simulated failure");
        }));
        assertEquals(1L, second.withPublicationLock(() -> second.get(1).orElseThrow()).txId());
    }

    /** 持锁连接丢失后本次发布失败关闭，持久化未知槽仍然阻止另一实例发新快照。 */
    @Test
    void disconnectedLockConnectionCannotResumePublicationOnAnotherConnection() {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        assertThrows(MvccException.class, () -> first.withPublicationLock(() -> {
            first.markLocalCommitPending(1, 10);
            try (Connection borrowed = MysqlConnectionContext.connection(firstPool)) {
                borrowed.abort(Runnable::run);
            } catch (SQLException ex) {
                throw new IllegalStateException(ex);
            }
            assertThrows(MvccException.class, () -> first.markCommitted(1, 10));
            assertThrows(MvccException.class, tso::nextTimestamp);
            assertThrows(MvccException.class, () -> first.tryBegin(tso, IsolationLevel.READ_COMMITTED,
                    60_000, false, () -> { }));
            assertThrows(MvccException.class, () -> first.tryCommit(tso, 1, Set.of("a"),
                    () -> { }, ignored -> { }));
            return null;
        }));
        assertEquals(TxStatus.PREPARING, second.get(1).orElseThrow().status());
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, second::assertNoPendingPublications).errorCode());
    }

    /** 外部会话持有同库命名锁时明确返回 GET_LOCK 超时错误。 */
    @Test
    void databaseLockTimeoutIsExplicit() throws SQLException {
        try (Connection external = secondPool.getConnection(); var statement = external.createStatement()) {
            String name;
            try (var result = statement.executeQuery("select concat('mvcc:pub:', substring(sha2(database(), 256), 1, 48))")) {
                assertTrue(result.next());
                name = result.getString(1);
            }
            try (var lock = external.prepareStatement("select get_lock(?, 0)");
                 var release = external.prepareStatement("select release_lock(?)")) {
                lock.setString(1, name);
                release.setString(1, name);
                try (var result = lock.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(1, result.getInt(1));
                }
                try {
                    var immediate = new MysqlTransactionMetadataStore(firstPool, 0);
                    var error = assertThrows(MvccException.class, () -> immediate.withPublicationLock(() -> 1));
                    assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, error.errorCode());
                    assertTrue(error.getMessage().contains("GET_LOCK"));
                    var beginError = assertThrows(MvccException.class, () -> immediate.tryBegin(tso,
                            IsolationLevel.READ_COMMITTED, 60_000, false, () -> { }));
                    assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, beginError.errorCode());
                } finally {
                    release.execute();
                }
            }
        }
    }

    /** 服务器端例程若遗留本会话命名锁，控制连接必须被显式弃用。 */
    @Test
    void serverSideProcedureMustDiscardConnectionThatRetainsLock() throws SQLException {
        assertThrows(SQLException.class, () -> MysqlConnectionContext.withPublicationProcedure(firstPool, 1,
                connection -> {
                    try (var statement = connection.createStatement();
                         var result = statement.executeQuery("""
                                 select get_lock(concat('mvcc:pub:', substring(sha2(database(), 256), 1, 48)), 0)
                                 """)) {
                        assertTrue(result.next());
                        assertEquals(1, result.getInt(1));
                    }
                    return 1;
                }));
        assertEquals(1, second.withPublicationLock(() -> 1));
    }

    /** 例程若留下显式数据库事务，不得靠连接池重置 autoCommit 隐式提交。 */
    @Test
    void serverSideProcedureMustDiscardConnectionWithOpenTransaction() throws SQLException {
        assertThrows(SQLException.class, () -> MysqlConnectionContext.withPublicationProcedure(firstPool, 1,
                connection -> {
                    try (var statement = connection.createStatement()) {
                        statement.execute("start transaction");
                    }
                    return 1;
                }));
        assertEquals(1, second.withPublicationLock(() -> 1));
    }

    /** 已持有发布屏障时只调用 v1，避免服务器端例程重复获取同一命名锁。 */
    @Test
    void nestedPublicationUsesExistingLockWithoutServerSideReentry() {
        long txId = first.withPublicationLock(() -> {
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationProcedure(
                            firstPool, 1, connection -> 1)).errorCode());
            return first.tryBegin(tso, IsolationLevel.REPEATABLE_READ, 60_000, false,
                    () -> { }).orElseThrow().txId();
        });
        assertTrue(first.snapshotExpiry(txId).isPresent());
        assertEquals(1, second.withPublicationLock(() -> 1));
    }

    /** 已决议事务的物化标记和租约释放不应等待另一事务持有的发布屏障。 */
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
                } catch (InterruptedException ex) {
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

    /** 参与者登记只依赖自身事务行锁，不能占用其他事务的发布屏障。 */
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
                } catch (InterruptedException ex) {
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
                assertEquals(Set.of("a", "b"), registration.get(2, TimeUnit.SECONDS).participants());
            } finally {
                release.countDown();
                holder.get(5, TimeUnit.SECONDS);
            }
        }
    }

    /** 独立兼容号段实例和普通 TSO 交错取号时仍按同库持久化顺序递增。 */
    @Test
    void segmentCompatibilityNeverAllocatesIndependentClientRanges() {
        var left = new MysqlSegmentTimestampOracle(firstPool, 128);
        var right = new MysqlSegmentTimestampOracle(secondPool, 256);
        left.initialize(100);
        right.initialize(100);
        assertEquals(101, left.nextTimestamp());
        assertEquals(102, right.nextTimestamp());
        assertEquals(103, left.nextTimestamp());
        assertEquals(104, tso.nextTimestamp());
        assertEquals(105, right.nextTimestamp());
        assertEquals(2, left.segmentAllocations());
        assertEquals(2, right.segmentAllocations());
    }

    /** 合并开始返回前必须持久化租约，非延迟模式还要原子登记活动事务。 */
    @Test
    void serverSideBeginPersistsSnapshotAndOptionalMetadata() {
        var lazy = first.tryBegin(tso, IsolationLevel.REPEATABLE_READ, 60_000, false, () -> { })
                .orElseThrow();
        assertEquals(lazy.txId(), lazy.startTs());
        assertEquals(lazy.deadlineMillis(), first.snapshotExpiry(lazy.txId()).orElseThrow().longValue());
        assertTrue(first.get(lazy.txId()).isEmpty());
        var eager = first.tryBegin(tso, IsolationLevel.READ_COMMITTED, 60_000, true, () -> { })
                .orElseThrow();
        assertTrue(eager.txId() > lazy.txId());
        assertEquals(TxStatus.ACTIVE, first.get(eager.txId()).orElseThrow().status());
        assertEquals(eager.deadlineMillis(), first.snapshotExpiry(eager.txId()).orElseThrow().longValue());
        var restartedStore = new MysqlTransactionMetadataStore(firstPool);
        assertTrue(restartedStore.tryBegin(tso, IsolationLevel.READ_COMMITTED,
                60_000, false, () -> { }).isPresent());
        assertTrue(first.tryBegin(new MysqlTimestampOracle(secondPool), IsolationLevel.READ_COMMITTED,
                60_000, false, () -> { }).isEmpty());
    }

    /** 单分片本地提交占位存在时，合并开始不得分配时间戳或发布新快照。 */
    @Test
    void serverSideBeginRejectsPendingLocalPublication() throws SQLException {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        first.markLocalCommitPending(1, 10);
        long before;
        try (Connection connection = firstPool.getConnection();
             var statement = connection.createStatement();
             var result = statement.executeQuery("select current_value from mvcc_tso where sequence_name = 'global'")) {
            assertTrue(result.next());
            before = result.getLong(1);
        }
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> first.tryBegin(tso, IsolationLevel.READ_COMMITTED,
                        60_000, false, () -> { })).errorCode());
        try (Connection connection = firstPool.getConnection();
             var statement = connection.createStatement();
             var result = statement.executeQuery("select current_value from mvcc_tso where sequence_name = 'global'")) {
            assertTrue(result.next());
            assertEquals(before, result.getLong(1));
        }
    }

    /** 服务器端提交只接受已登记参与者，重复响应必须返回同一持久化决议。 */
    @Test
    void serverSideCommitPreservesDecisionAcrossMetadataInstances() {
        var started = first.tryBegin(tso, IsolationLevel.REPEATABLE_READ, 60_000, true, () -> { })
                .orElseThrow();
        first.registerParticipants(started.txId(), Set.of("a", "b"));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> first.tryCommit(tso, started.txId(),
                        Set.of("a"), () -> { }, ignored -> { })).errorCode());
        assertEquals(TxStatus.ACTIVE, second.get(started.txId()).orElseThrow().status());
        long committedTs = first.tryCommit(tso, started.txId(), Set.of("a", "b"), () -> { }, ignored -> { })
                .orElseThrow();
        assertTrue(committedTs > started.startTs());
        assertEquals(committedTs, second.get(started.txId()).orElseThrow().commitTs().longValue());
        var restartedStore = new MysqlTransactionMetadataStore(secondPool);
        assertEquals(committedTs, restartedStore.tryCommit(new MysqlTimestampOracle(secondPool), started.txId(),
                Set.of("a", "b"), () -> { }, ignored -> { }).orElseThrow());
    }

    /** 全局序列耗尽时不能回绕或返回重复时间戳。 */
    @Test
    void timestampOracleMustRejectSequenceExhaustion() throws SQLException {
        try (Connection connection = firstPool.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_tso set current_value = 9223372036854775807 where sequence_name = 'global'");
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, tso::nextTimestamp).errorCode());
    }

    /** 固定租约、单分片占位和不可逆终态在独立元数据对象之间一致。 */
    @Test
    void pendingPublicationAndFinalizationSurviveMetadataInstanceChanges() {
        first.create(1, 1, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        long deadline = first.currentTimeMillis() + 60_000;
        first.registerSnapshot(1, 1, deadline);
        assertThrows(MvccException.class, () -> second.registerSnapshot(1, 1, deadline + 1));
        first.markLocalCommitPending(1, 10);
        var error = assertThrows(MvccException.class, second::assertNoPendingPublications);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        second.markCommitted(1, 10);
        first.persistPreparing(1, Set.of("b"));
        assertEquals(TxStatus.COMMITTED, first.markAborted(1).status());
        assertThrows(MvccException.class, () -> first.persistCommitted(1, 11));
        assertDoesNotThrow(second::assertNoPendingPublications);
        first.releaseSnapshot(1);
        assertEquals(1L, second.oldestActiveStartTs().orElseThrow());
        second.markFinalized(1);
        assertTrue(first.oldestActiveStartTs().isEmpty());
        assertEquals(0, first.purgeFinalized(10, Long.MAX_VALUE, 1));
        assertEquals(1, first.purgeFinalized(11, Long.MAX_VALUE, 1));
    }

    /** 同进程竞争者在借池连接前等待，持锁线程仍可使用池中唯一连接推进控制面。 */
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
                assertEquals(1, firstPool.getHikariPoolMXBean().getActiveConnections());
                release.countDown();
                assertTrue(owner.get(5, TimeUnit.SECONDS) < waiter.get(5, TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
        }
    }

    /** MySQL 提交入口在持久化前拒绝错误时间戳和无法无歧义编码的分片标识。 */
    @Test
    void metadataRejectsInvalidCommitTimestampsAndParticipantIdentifiers() {
        first.create(1, 10, IsolationLevel.REPEATABLE_READ);
        first.registerParticipants(1, Set.of("a"));
        for (long commitTs : new long[]{Long.MIN_VALUE, 9, 10}) {
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> first.markLocalCommitPending(1, commitTs)).errorCode());
            assertThrows(MvccException.class, () -> first.markCommitted(1, commitTs));
            assertThrows(MvccException.class, () -> first.persistCommitted(1, Set.of("a"), commitTs));
            assertThrows(MvccException.class, () -> first.markSingleShardCommitted(1, "a", commitTs));
            assertThrows(MvccException.class, () -> first.createCommitted(2, 10, IsolationLevel.REPEATABLE_READ, Set.of("a"), commitTs));
            assertEquals(TxStatus.ACTIVE, second.get(1).orElseThrow().status());
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
            assertEquals(Set.of("a"), second.get(1).orElseThrow().participants());
            assertEquals(TxStatus.ACTIVE, second.get(1).orElseThrow().status());
            assertTrue(second.get(2).isEmpty());
        }
        first.markLocalCommitPending(1, 11);
        assertEquals(11L, first.markCommitted(1, 11).commitTs());
        assertThrows(MvccException.class, () -> first.registerParticipants(1, Set.of("a,b")));
        assertThrows(MvccException.class, () -> first.persistPreparing(1, Set.of(" ")));
    }

    /** 旧 CSV 中的空集合和合法路由仍可读取，空白片段不能被静默丢弃而误标恢复完成。 */
    @Test
    void legacyCsvRemainsCompatibleButMalformedSegmentsFailClosed() throws SQLException {
        first.createPreparing(1, 1, IsolationLevel.REPEATABLE_READ, Set.of("a", "b"));
        try (Connection connection = firstPool.getConnection();
             var update = connection.prepareStatement("update mvcc_txn set participants = ? where tx_id = 1")) {
            for (String encoded : new String[]{" ", "a,", ",a", "a,,b", "a, ,b"}) {
                update.setString(1, encoded);
                update.executeUpdate();
                assertEquals(ErrorCode.ILLEGAL_STATE,
                        assertThrows(MvccException.class, () -> second.get(1)).errorCode());
            }
            update.setString(1, "b,a");
            update.executeUpdate();
            assertEquals(Set.of("a", "b"), second.get(1).orElseThrow().participants());
            update.setString(1, "");
            update.executeUpdate();
            assertTrue(second.get(1).orElseThrow().participants().isEmpty());
        }
    }

    /** 创建一条连接的池，以暴露屏障内错误重复借连接。 */
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

    /** 采用有界等待构造竞争，失败时恢复中断位。 */
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }
}

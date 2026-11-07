package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 两个真实 MySQL 分片验证分条水位的稳定切面、RC 重取和 RR 固定可见性。 */
@Testcontainers
class MysqlStripedSnapshotOracleTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");
    private static HikariDataSource left;
    private static HikariDataSource right;
    private MysqlStripedSnapshotOracle oracle;

    /** 在同一服务器建立两个独立数据库与连接池。 */
    @BeforeAll
    static void openDatabases() throws Exception {
        createDatabase("stripe_left");
        createDatabase("stripe_right");
        left = pool("stripe_left");
        right = pool("stripe_right");
    }

    /** 测试用户只获得目标隔离数据库权限。 */
    private static void createDatabase(String name) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + name + "`; "
                        + "grant all privileges on `" + name + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /** JDBC URL 只替换本测试的物理数据库名称。 */
    private static HikariDataSource pool(String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭两片测试连接池。 */
    @AfterAll
    static void closeDatabases() {
        if (left != null) left.close();
        if (right != null) right.close();
    }

    /** 每例重新创建四条空持久水位与空版本链。 */
    @BeforeEach
    void reset() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeIsolated(left, 4);
        MysqlStripedMvccCapacityProbe.initializeIsolated(right, 4);
        oracle = new MysqlStripedSnapshotOracle(Map.<String, DataSource>of("s1", left, "s2", right), 4, 8);
    }

    /** 旧 RR 始终不见后续两片提交，新 RC 重新捕获后看到两片版本。 */
    @Test
    void fixedRrAndFreshRcMustUseTheirOwnStableVector() throws Exception {
        var oldRr = oracle.capture(() -> 0);
        commit(left, "striped:left", new byte[]{1}, 1);
        commit(right, "striped:right", new byte[]{2}, 2);
        assertTrue(oracle.read(oldRr, "s1", "striped:left", 1).isEmpty());
        assertTrue(oracle.read(oldRr, "s2", "striped:right", 2).isEmpty());
        var newRc = oracle.capture(() -> 0);
        assertEquals(1, newRc.sequence("s1", 1));
        assertEquals(1, newRc.sequence("s2", 2));
        assertArrayEquals(new byte[]{1}, oracle.read(newRc, "s1", "striped:left", 1).orElseThrow());
        assertArrayEquals(new byte[]{2}, oracle.read(newRc, "s2", "striped:right", 2).orElseThrow());
    }

    /** 第一次采集后发生提交必须触发重试，不能返回提交前后混合的分片水位。 */
    @Test
    void commitBetweenCollectsMustRetryBeforeReturningSnapshot() throws Exception {
        AtomicInteger centerReads = new AtomicInteger();
        var snapshot = oracle.capture(() -> {
            if (centerReads.getAndIncrement() == 0) {
                try {
                    commit(left, "striped:between", new byte[]{3}, 1);
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return 7;
        });
        assertEquals(2, centerReads.get());
        assertEquals(7, snapshot.decisionEpoch());
        assertEquals(1, snapshot.sequence("s1", 1));
        assertArrayEquals(new byte[]{3}, oracle.read(snapshot, "s1", "striped:between", 1).orElseThrow());
    }

    /** 并行分片采集也必须在控制读水位前后发现中途提交并重新采集。 */
    @Test
    void parallelCollectorMustRetryOnMidpointCommit() throws Exception {
        try (var parallel = new MysqlStripedSnapshotOracle(
                Map.<String, DataSource>of("s1", left, "s2", right), 4, 8, true)) {
            AtomicInteger centerReads = new AtomicInteger();
            var snapshot = parallel.capture(() -> {
                if (centerReads.getAndIncrement() == 0) {
                    try {
                        commit(right, "striped:parallel", new byte[]{4}, 3);
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                return 9;
            });
            assertEquals(2, centerReads.get());
            assertEquals(2, parallel.collectionAttempts());
            assertEquals(9, snapshot.decisionEpoch());
            assertArrayEquals(new byte[]{4}, parallel.read(snapshot, "s2", "striped:parallel", 3)
                    .orElseThrow());
        }
    }

    /** 两片先后提交形成因果链时，快照不能只保留前一片而遗漏后一片。 */
    @Test
    void dependentCommitsBetweenCollectsMustBeObservedTogether() throws Exception {
        AtomicInteger centerReads = new AtomicInteger();
        var snapshot = oracle.capture(() -> {
            if (centerReads.getAndIncrement() == 0) {
                try {
                    commit(left, "striped:causal:left", new byte[]{5}, 1);
                    commit(right, "striped:causal:right", new byte[]{6}, 2);
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return 0;
        });
        assertEquals(2, centerReads.get());
        assertEquals(2, oracle.collectionAttempts());
        assertArrayEquals(new byte[]{5}, oracle.read(snapshot, "s1", "striped:causal:left", 1)
                .orElseThrow());
        assertArrayEquals(new byte[]{6}, oracle.read(snapshot, "s2", "striped:causal:right", 2)
                .orElseThrow());
    }

    /** 缺失任意分条时拒绝猜测快照边界。 */
    @Test
    void missingStripeMustFailClosed() throws Exception {
        try (Connection connection = right.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from bench_striped_mvcc_state where stripe_id = 2");
        }
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> oracle.capture(() -> 0)).errorCode());
    }

    /** 同键后续写入必须使用包含前一提交的起始水位，否则返回写冲突。 */
    @Test
    void checkedLocalCommitMustRejectNewerSameKeyVersion() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(left, 4);
        assertEquals(1, commitChecked(left, "striped:checked", new byte[]{1}, 1, 0));
        assertEquals(36003, assertThrows(SQLException.class,
                () -> commitChecked(left, "striped:checked", new byte[]{2}, 1, 0))
                .getErrorCode());
        assertEquals(2, commitChecked(left, "striped:checked", new byte[]{3}, 1, 1));
    }

    /** 持久跨分片意向存在时，本地单分片提交不能绕开同键锁。 */
    @Test
    void checkedLocalCommitMustRejectForeignIntent() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(left, 4);
        try (Connection connection = left.getConnection(); PreparedStatement guard = connection.prepareStatement("""
                insert into bench_striped_mvcc_key_lock(user_key) values (?)
                """); PreparedStatement intent = connection.prepareStatement("""
                insert into bench_striped_mvcc_intent(user_key, owner_tx_id) values (?, ?)
                """)) {
            connection.setAutoCommit(false);
            guard.setBytes(1, "striped:intent".getBytes(StandardCharsets.UTF_8));
            guard.executeUpdate();
            intent.setBytes(1, "striped:intent".getBytes(StandardCharsets.UTF_8));
            intent.setLong(2, 42);
            intent.executeUpdate();
            connection.commit();
        }
        assertEquals(36002, assertThrows(SQLException.class,
                () -> commitChecked(left, "striped:intent", new byte[]{4}, 2, 0))
                .getErrorCode());
    }

    /** 等待同键锁后必须读取前驱刚提交的版本，避免旧读视图漏判写冲突。 */
    @Test
    void checkedLocalCommitMustSeeVersionCommittedDuringKeyLockWait() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(left, 4);
        byte[] key = "striped:lock-wait".getBytes(StandardCharsets.UTF_8);
        try (Connection blocker = left.getConnection(); var worker = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try (PreparedStatement guard = blocker.prepareStatement("""
                    insert into bench_striped_mvcc_key_lock(user_key) values (?)
                    """); PreparedStatement state = blocker.prepareStatement("""
                    update bench_striped_mvcc_state set committed_seq = 1 where stripe_id = 1
                    """); PreparedStatement version = blocker.prepareStatement("""
                    insert into bench_striped_mvcc_data(user_key, local_seq, stripe_id, payload)
                    values (?, 1, 1, ?)
                    """)) {
                guard.setBytes(1, key);
                guard.executeUpdate();
                state.executeUpdate();
                version.setBytes(1, key);
                version.setBytes(2, new byte[]{7});
                version.executeUpdate();
            }
            CountDownLatch started = new CountDownLatch(1);
            var waiting = worker.submit(() -> {
                started.countDown();
                return commitChecked(left, "striped:lock-wait", new byte[]{8}, 1, 0);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
            blocker.commit();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> waiting.get(5, TimeUnit.SECONDS));
            assertEquals(36003, ((SQLException) failure.getCause()).getErrorCode());
        }
    }

    /** 单键版本和目标分条水位由同一分片存储过程原子提交。 */
    private static long commit(HikariDataSource dataSource, String key, byte[] value, int stripe)
            throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_commit_v1(?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setBytes(2, value);
            call.setInt(3, stripe);
            try (ResultSet row = call.executeQuery()) {
                assertTrue(row.next());
                return row.getLong(1);
            }
        }
    }

    /** 调用带键锁、意向和版本冲突检查的单键物理提交原型。 */
    private static long commitChecked(HikariDataSource dataSource, String key, byte[] value,
                                      int stripe, long startSeq) throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_commit_checked_v2(?, ?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setBytes(2, value);
            call.setInt(3, stripe);
            call.setLong(4, startSeq);
            try (ResultSet row = call.executeQuery()) {
                assertTrue(row.next());
                return row.getLong(1);
            }
        }
    }
}

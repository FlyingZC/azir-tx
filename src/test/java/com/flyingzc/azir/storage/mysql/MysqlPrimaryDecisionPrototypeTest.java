package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 两个真实 MySQL 分片核对主参与分片的持久决议与预写证明。 */
@Testcontainers
class MysqlPrimaryDecisionPrototypeTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");
    private static HikariDataSource primary;
    private static HikariDataSource secondary;
    private MysqlPrimaryDecisionPrototype decisions;

    /** 在同一个测试 MySQL 中隔离主次分片的持久数据库。 */
    @BeforeAll
    static void openDatabases() throws Exception {
        createDatabase("primary_decision_one");
        createDatabase("primary_decision_two");
        primary = pool("primary_decision_one");
        secondary = pool("primary_decision_two");
    }

    /** 测试账号只需拥有测试数据库的权限。 */
    private static void createDatabase(String name) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + name + "`; "
                        + "grant all privileges on `" + name + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /** JDBC URL 只替换测试的物理数据库名称。 */
    private static HikariDataSource pool(String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 测试结束后归还两片连接。 */
    @AfterAll
    static void closeDatabases() {
        if (primary != null) {
            primary.close();
        }
        if (secondary != null) {
            secondary.close();
        }
    }

    /** 每例从空的隔离意向和决议序号开始。 */
    @BeforeEach
    void reset() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(primary, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondary, 4);
        decisions = new MysqlPrimaryDecisionPrototype(primary, secondary, 4);
        decisions.initializeIsolated();
        decisions.resetIsolated();
    }

    /** 两片预写必须同时存在，提交重放不能再次推进主决议序号。 */
    @Test
    void commitRequiresBothDurableProofsAndReplayKeepsOneSequence() throws Exception {
        var registration = registration(101);
        assertEquals(TxStatus.ACTIVE, decisions.register(registration).status());
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, registration.txId(), 0);
        MvccException missing = assertThrows(MvccException.class, () -> decisions.commit(101));
        assertEquals(ErrorCode.ILLEGAL_STATE, missing.errorCode());
        assertEquals(0L, decisions.decisionVector().get(2));
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, registration.txId(), 0);
        var first = decisions.commit(101);
        assertEquals(TxStatus.COMMITTED, first.status());
        assertEquals(1L, first.decisionSequence());
        assertEquals(1L, first.primarySequence());
        assertEquals(1L, first.secondarySequence());
        assertEquals(first, decisions.commit(101));
        assertEquals(first, decisions.register(registration));
        assertEquals(first, decisions.decision(101).orElseThrow());
        assertEquals(1L, decisions.decisionVector().get(2));
        assertThrows(MvccException.class, () -> decisions.abort(101));
    }

    /** 主登记与主预写在一次物理事务内完成，重放沿用同一保留序号。 */
    @Test
    void combinedPrimaryPrewriteIsAtomicAndReplayKeepsItsSequence() throws Exception {
        var registration = registration(109);
        long first = decisions.registerAndPrewritePrimary(registration);
        assertEquals(1L, first);
        assertEquals(TxStatus.ACTIVE, decisions.decision(109).orElseThrow().status());
        assertEquals(first, decisions.registerAndPrewritePrimary(registration));
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, 109, 0);
        var committed = decisions.commit(registration);
        assertEquals(first, committed.primarySequence());
        assertEquals(first, decisions.registerAndPrewritePrimary(registration));
        MysqlStripedCrossVersionPrototype.materialize(primary, registration.primaryKey(),
                109, committed.decisionSequence(), this::committedSequence);
        assertEquals(first, decisions.registerAndPrewritePrimary(registration));
        MysqlStripedCrossVersionPrototype.materialize(secondary, registration.secondaryKey(),
                109, committed.decisionSequence(), this::committedSequence);
        assertEquals(committed, decisions.commit(registration));
    }

    /** 主键冲突使整次调用回滚，不能留下只有 ACTIVE 而没有主意向的事务。 */
    @Test
    void failedCombinedPrimaryPrewriteRollsBackRegistration() {
        var owner = registration(110);
        decisions.registerAndPrewritePrimary(owner);
        var conflicting = new MysqlPrimaryDecisionPrototype.Registration(111,
                owner.primaryKey(), bytes("other"), "secondary:111", bytes("right"),
                1, 3, 0, 0, deadline(), 2);
        MvccException rejected = assertThrows(MvccException.class,
                () -> decisions.registerAndPrewritePrimary(conflicting));
        assertEquals(ErrorCode.LOCK_CONFLICT, rejected.errorCode());
        assertTrue(decisions.decision(111).isEmpty());
        assertEquals(1L, decisions.registerAndPrewritePrimary(owner));
    }

    /** 主决议后新 RC 同时看见两片，旧 RR 在单侧及双侧物化后仍看旧值。 */
    @Test
    void oldRrAndNewRcRemainAtomicAcrossOneSidedMaterialization() throws Exception {
        MysqlStripedCrossVersionPrototype.localCommit(primary,
                "primary:106", bytes("old-left"), 1, 0);
        MysqlStripedCrossVersionPrototype.localCommit(secondary,
                "secondary:106", bytes("old-right"), 3, 0);
        var registration = new MysqlPrimaryDecisionPrototype.Registration(106,
                "primary:106", bytes("new-left"), "secondary:106", bytes("new-right"),
                1, 3, 1, 1, deadline(), 2);
        try (var reader = new MysqlPrimaryVectorReadPrototype(
                "s1", primary, "s2", secondary, 4, decisions)) {
            var oldRr = reader.captureUnpublished();
            decisions.register(registration);
            MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                    registration.primaryValue(), 1, 106, 1);
            MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                    registration.secondaryValue(), 3, 106, 1);
            var beforeDecision = reader.captureUnpublished();
            assertArrayEquals(bytes("old-left"),
                    reader.read(beforeDecision, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("old-right"),
                    reader.read(beforeDecision, "s2", registration.secondaryKey(), 3).orElseThrow());
            var committed = decisions.commit(106);
            var newRc = reader.captureUnpublished();
            assertArrayEquals(bytes("old-left"),
                    reader.read(oldRr, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("old-right"),
                    reader.read(oldRr, "s2", registration.secondaryKey(), 3).orElseThrow());
            assertArrayEquals(bytes("new-left"),
                    reader.read(newRc, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("new-right"),
                    reader.read(newRc, "s2", registration.secondaryKey(), 3).orElseThrow());
            MysqlStripedCrossVersionPrototype.materialize(primary, registration.primaryKey(),
                    106, committed.decisionSequence(), this::committedSequence);
            assertArrayEquals(bytes("old-left"),
                    reader.read(oldRr, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("new-left"),
                    reader.read(newRc, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("new-right"),
                    reader.read(newRc, "s2", registration.secondaryKey(), 3).orElseThrow());
            MysqlStripedCrossVersionPrototype.materialize(secondary, registration.secondaryKey(),
                    106, committed.decisionSequence(), this::committedSequence);
            assertArrayEquals(bytes("old-right"),
                    reader.read(oldRr, "s2", registration.secondaryKey(), 3).orElseThrow());
            assertArrayEquals(bytes("new-right"),
                    reader.read(newRc, "s2", registration.secondaryKey(), 3).orElseThrow());
        }
    }

    /** 相同事务号不得以不同载荷重新注册并继承既有主决议。 */
    @Test
    void registrationReplayRejectsChangedPayload() {
        decisions.register(registration(102));
        var changed = new MysqlPrimaryDecisionPrototype.Registration(102,
                "primary:102", bytes("different"), "secondary:102", bytes("right"),
                1, 3, 0, 0, deadline(), 2);
        MvccException rejected = assertThrows(MvccException.class, () -> decisions.register(changed));
        assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
    }

    /** 键值和事务号相同也不能把次片写到未注册的数据分条。 */
    @Test
    void commitRejectsPrewriteOnWrongParticipantStripe() throws Exception {
        var registration = registration(107);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, 107, 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 2, 107, 0);
        MvccException rejected = assertThrows(MvccException.class, () -> decisions.commit(107));
        assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
        assertEquals(TxStatus.ACTIVE, decisions.decision(107).orElseThrow().status());
        assertEquals(0L, decisions.decisionVector().get(2));
    }

    /** 主事务行锁让提交和中止只产生一个不可逆终态。 */
    @Test
    void commitAndAbortRaceHasExactlyOneDurableTerminalDecision() throws Exception {
        var registration = registration(103);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, registration.txId(), 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, registration.txId(), 0);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var commit = workers.submit(() -> {
                start.await();
                try {
                    return decisions.commit(103).status();
                } catch (MvccException rejected) {
                    if (rejected.errorCode() != ErrorCode.TRANSACTION_ABORTED) {
                        throw rejected;
                    }
                    return TxStatus.ABORTED;
                }
            });
            var abort = workers.submit(() -> {
                start.await();
                try {
                    return decisions.abort(103).status();
                } catch (MvccException rejected) {
                    if (rejected.errorCode() != ErrorCode.TRANSACTION_STATUS_UNKNOWN) {
                        throw rejected;
                    }
                    return TxStatus.COMMITTED;
                }
            });
            start.countDown();
            TxStatus committedSide = commit.get(30, TimeUnit.SECONDS);
            TxStatus abortedSide = abort.get(30, TimeUnit.SECONDS);
            assertEquals(committedSide, abortedSide);
            var durable = decisions.decision(103).orElseThrow();
            assertEquals(committedSide, durable.status());
            assertEquals(durable.status() == TxStatus.COMMITTED ? 1L : 0L,
                    decisions.decisionVector().get(2));
        }
    }

    /** 已持久中止证明允许两片留下墓碑，并拒绝迟到预写复活。 */
    @Test
    void abortProofRejectsLateParticipantPrewrite() throws Exception {
        var registration = registration(104);
        decisions.register(registration);
        assertEquals(TxStatus.ABORTED, decisions.abort(104).status());
        MysqlStripedCrossVersionPrototype.abort(primary, registration.primaryKey(), 104,
                txId -> decisions.decision(txId).orElseThrow().status());
        MysqlStripedCrossVersionPrototype.abort(secondary, registration.secondaryKey(), 104,
                txId -> decisions.decision(txId).orElseThrow().status());
        assertThrows(SQLException.class,
                () -> MysqlStripedCrossVersionPrototype.prewrite(secondary,
                        registration.secondaryKey(), registration.secondaryValue(), 3, 104, 0));
        assertTrue(decisions.decisionVector().stream().allMatch(sequence -> sequence == 0));
    }

    /** 已过期的主身份即便两片预写都存在也不得提交。 */
    @Test
    void expiredPrimaryIdentityCannotCommit() throws Exception {
        var registration = new MysqlPrimaryDecisionPrototype.Registration(105,
                "primary:105", bytes("left"), "secondary:105", bytes("right"),
                1, 3, 0, 0, System.currentTimeMillis() - 1000, 2);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, registration.txId(), 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, registration.txId(), 0);
        MvccException rejected = assertThrows(MvccException.class, () -> decisions.commit(105));
        assertEquals(ErrorCode.TRANSACTION_ABORTED, rejected.errorCode());
        assertEquals(TxStatus.ACTIVE, decisions.decision(105).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, decisions.abort(105).status());
    }

    /** 主决议库不可达时次片不能把未知意向当作可见或已中止。 */
    @Test
    void secondaryReadFailsClosedWhenPrimaryDecisionIsUnavailable() throws Exception {
        var registration = registration(108);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, 108, 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, 108, 0);
        decisions.commit(108);
        try (var available = new MysqlPrimaryVectorReadPrototype(
                "s1", primary, "s2", secondary, 4, decisions)) {
            var cut = available.captureUnpublished();
            try (HikariDataSource unavailable = pool("primary_decision_one")) {
                unavailable.close();
                var broken = new MysqlPrimaryDecisionPrototype(unavailable, secondary, 4);
                try (var reader = new MysqlPrimaryVectorReadPrototype(
                        "s1", primary, "s2", secondary, 4, broken)) {
                    MvccException failure = assertThrows(MvccException.class,
                            () -> reader.read(cut, "s2", registration.secondaryKey(), 3));
                    assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
                }
            }
        }
    }

    /** 为两片构造固定键、值、快照界限与主决议分条。 */
    private static MysqlPrimaryDecisionPrototype.Registration registration(long txId) {
        return new MysqlPrimaryDecisionPrototype.Registration(txId,
                "primary:" + txId, bytes("left"), "secondary:" + txId, bytes("right"),
                1, 3, 0, 0, deadline(), 2);
    }

    /** 测试期限足够覆盖数据库启动和并发交错。 */
    private static long deadline() {
        return System.currentTimeMillis() + 60_000;
    }

    /** 测试键值使用可重复的 UTF-8 字节。 */
    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** 原型物化只接受主分片持久 COMMITTED 序号。 */
    private OptionalLong committedSequence(long txId) {
        var decision = decisions.decision(txId).orElseThrow();
        return decision.status() == TxStatus.COMMITTED
                ? OptionalLong.of(decision.decisionSequence()) : OptionalLong.empty();
    }
}

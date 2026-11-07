package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.mysql.MysqlLocalFirstMetadataView;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.publication.LocalFirstIntentRecoveryManager;
import com.flyingzc.azir.publication.LocalFirstPublicationRecoveryManager;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.ReadContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 三个真实 MySQL 数据库下验证同一新时间线的跨分片 ACTIVE 意向不可见性。 */
@Testcontainers
class MysqlLocalFirstCrossShardTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");
    private static HikariDataSource controlData;
    private static HikariDataSource firstData;
    private static HikariDataSource secondData;
    private MysqlLocalFirstPublicationLedger control;
    private MysqlLocalFirstMetadataView metadata;
    private MysqlStorageAdapter first;
    private MysqlStorageAdapter second;

    /** 在同一容器内建立物理隔离的控制库和两个分片库。 */
    @BeforeAll
    static void openDatabases() throws Exception {
        createDatabase("control");
        createDatabase("shard_1");
        createDatabase("shard_2");
        controlData = pool("control");
        firstData = pool("shard_1");
        secondData = pool("shard_2");
    }

    /** 容器管理员只为测试用户开放三个隔离库。 */
    private static void createDatabase(String name) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + name + "`; "
                        + "grant all privileges on `" + name + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /** 连接池的 JDBC URL 只替换实际数据库名称。 */
    private static HikariDataSource pool(String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭本类使用的三个物理连接池。 */
    @AfterAll
    static void closeDatabases() {
        if (controlData != null) controlData.close();
        if (firstData != null) firstData.close();
        if (secondData != null) secondData.close();
    }

    /** 每例清除新协议控制表及两个分片的旧版本和意向。 */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_local_first_slot_v1");
            statement.execute("drop table if exists mvcc_local_first_txn_v1");
            statement.execute("drop table if exists mvcc_local_first_segment_v1");
            statement.execute("drop table if exists mvcc_local_first_snapshot_v1");
            statement.execute("drop table if exists mvcc_local_first_coordinator_v1");
            statement.execute("drop table if exists mvcc_local_first_state_v1");
            statement.execute("drop table if exists mvcc_vector_published_cut_v1");
            statement.execute("drop table if exists mvcc_vector_leader_v1");
            statement.execute("drop table if exists mvcc_vector_reader_lease_v1");
            statement.execute("drop table if exists mvcc_vector_gc_floor_v1");
        }
        control = new MysqlLocalFirstPublicationLedger(controlData);
        control.initializeIsolated(100);
        metadata = new MysqlLocalFirstMetadataView(control);
        first = new MysqlStorageAdapter("s1", firstData, metadata);
        second = new MysqlStorageAdapter("s2", secondData, metadata);
        resetShard(firstData, first);
        resetShard(secondData, second);
    }

    /** 保留旧协议分片水位并清空版本、意向和独立向量实验表。 */
    private void resetShard(HikariDataSource dataSource, MysqlStorageAdapter adapter) throws Exception {
        adapter.initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
            statement.execute("drop table if exists mvcc_vector_retire_block");
            statement.execute("drop table if exists mvcc_vector_committed_v1");
            statement.execute("drop table if exists mvcc_vector_retired_v1");
            statement.execute("drop table if exists mvcc_vector_cross_gc_cursor_v1");
            statement.execute("drop table if exists mvcc_vector_intent_reservation_v1");
            statement.execute("drop table if exists mvcc_vector_local_data_v1");
            statement.execute("drop table if exists mvcc_vector_stripe_state_v1");
            statement.execute("drop table if exists mvcc_vector_layout_v1");
            statement.execute("drop table if exists mvcc_vector_local_gc_cursor_v1");
        }
    }

    /** 中心 ACTIVE 记录使两个分片的外部意向均不可见，事务自身仍读取写集合。 */
    @Test
    void registeredCrossShardIntentMustRemainInvisibleUntilCommit() {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        var registered = control.registerIntentTransaction(writer.txId(), writer.startTs(),
                writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ, participants);
        assertEquals(TxStatus.ACTIVE, registered.status());
        assertEquals(participants, metadata.get(writer.txId()).orElseThrow().participants());
        first.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:active:left", new byte[]{1})));
        second.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:active:right", new byte[]{2})));
        var committedReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertFalse(first.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "cross:active:left").isPresent());
        assertFalse(second.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "cross:active:right").isPresent());
        assertFalse(first.get(new ReadContext(committedReader.txId(), committedReader.startTs(),
                IsolationLevel.READ_COMMITTED), "cross:active:left").isPresent());
        assertFalse(second.get(new ReadContext(committedReader.txId(), committedReader.startTs(),
                IsolationLevel.READ_COMMITTED), "cross:active:right").isPresent());
        assertArrayEquals(new byte[]{1}, first.get(new ReadContext(writer.txId(), writer.startTs(),
                IsolationLevel.REPEATABLE_READ), "cross:active:left").orElseThrow().value());
        assertArrayEquals(new byte[]{2}, second.get(new ReadContext(writer.txId(), writer.startTs(),
                IsolationLevel.REPEATABLE_READ), "cross:active:right").orElseThrow().value());
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(committedReader.txId());
    }

    /** 登记响应未知只接受同一开始时间、期限、隔离级别和完整参与分片集合。 */
    @Test
    void intentRegistrationReplayMustRejectChangedIdentity() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        assertEquals(TxStatus.ACTIVE, control.registerIntentTransaction(writer.txId(),
                writer.startTs(), writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                participants).status());
        assertEquals(TxStatus.ACTIVE, control.registerIntentTransaction(writer.txId(),
                writer.startTs(), writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                participants).status());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.registerIntentTransaction(writer.txId(),
                        writer.startTs(), writer.deadlineMillis() + 1,
                        IsolationLevel.REPEATABLE_READ, participants)).errorCode());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.registerIntentTransaction(writer.txId(),
                        writer.startTs(), writer.deadlineMillis(),
                        IsolationLevel.REPEATABLE_READ, Set.of("s1", "s3"))).errorCode());
    }

    /** 任一快照已失效时整批 ACTIVE 登记回滚，不让另一事务先在分片预写。 */
    @Test
    void intentRegistrationBatchMustBeAtomic() throws Exception {
        var firstWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var secondWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        var firstRequest = new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                firstWriter.txId(), firstWriter.startTs(), firstWriter.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var secondRequest = new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                secondWriter.txId(), secondWriter.startTs(), secondWriter.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_snapshot_v1 set expires_at = 1 "
                    + "where tx_id = " + secondWriter.txId());
        }
        assertThrows(MvccException.class, () ->
                control.registerIntentBatch(List.of(firstRequest, secondRequest)));
        assertTrue(metadata.get(firstWriter.txId()).isEmpty());
        assertTrue(metadata.get(secondWriter.txId()).isEmpty());
        assertEquals(TxStatus.ACTIVE, control.registerIntentTransaction(firstWriter.txId(),
                firstWriter.startTs(), firstWriter.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants).status());
        control.releaseSnapshot(firstWriter.txId());
        control.releaseSnapshot(secondWriter.txId());
    }

    /** 快照租约到期也不能让 GC 越过仍持久 ACTIVE 意向事务的开始水位。 */
    @Test
    void activeIntentMetadataMustPinSafePointAfterSnapshotExpiry() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_snapshot_v1 set expires_at = 1 "
                    + "where tx_id = " + writer.txId());
        }
        assertEquals(1, control.expireSnapshots(10));
        assertEquals(99, control.advanceSafePoint());
        assertEquals(100, metadata.oldestActiveStartTs().orElseThrow());
    }

    /** 两分片中心决议在同一水位发布后，即使本地尚未物化，新 RC 也看见完整写入。 */
    @Test
    void crossShardIntentCommitMustPreserveGlobalRcAndRrBeforeMaterialization() {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        List<Mutation> left = List.of(Mutation.put("cross:commit:left", new byte[]{3}));
        List<Mutation> right = List.of(Mutation.put("cross:commit:right", new byte[]{4}));
        var leftProof = first.prewriteWithProof(writer.txId(), writer.startTs(), left);
        var rightProof = second.prewriteWithProof(writer.txId(), writer.startTs(), right);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> first.abort(writer.txId())).errorCode());
        assertEquals(TxStatus.ACTIVE, first.participantRecord(writer.txId()).orElseThrow().status());
        var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-a", "intent-request-a", 4, 5_000, 100);
        var active = control.activateIntentSegment(reserved);
        var decision = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof));
        assertEquals(1, control.completeIntentBatch(List.of(decision)));
        assertEquals(0, control.completeIntentBatch(List.of(decision)));
        assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(100, control.currentPublished());
        assertEquals(101, control.advancePublishedFast(10));
        var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertEquals(101, newReader.startTs());
        assertTrue(first.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "cross:commit:left").isEmpty());
        assertTrue(second.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), "cross:commit:right").isEmpty());
        assertArrayEquals(new byte[]{3}, first.get(new ReadContext(newReader.txId(), 101,
                IsolationLevel.READ_COMMITTED), "cross:commit:left").orElseThrow().value());
        assertArrayEquals(new byte[]{4}, second.get(new ReadContext(newReader.txId(), 101,
                IsolationLevel.READ_COMMITTED), "cross:commit:right").orElseThrow().value());
        assertEquals(TxStatus.ACTIVE, first.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ACTIVE, second.participantRecord(writer.txId()).orElseThrow().status());
        control.beginFencing(active, true);
        assertEquals(3, control.sealIntentSegment(active));
        assertEquals(0, control.sealIntentSegment(active));
        assertEquals(104, control.advancePublishedFast(10));
        var afterSeal = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertArrayEquals(new byte[]{3}, first.get(new ReadContext(afterSeal.txId(), 104,
                IsolationLevel.READ_COMMITTED), "cross:commit:left").orElseThrow().value());
        assertArrayEquals(new byte[]{4}, second.get(new ReadContext(afterSeal.txId(), 104,
                IsolationLevel.READ_COMMITTED), "cross:commit:right").orElseThrow().value());
        first.commit(writer.txId(), 101, left, true);
        second.commit(writer.txId(), 101, right, true);
        assertArrayEquals(new byte[]{3}, first.get(new ReadContext(newReader.txId(), 101,
                IsolationLevel.READ_COMMITTED), "cross:commit:left").orElseThrow().value());
        assertArrayEquals(new byte[]{4}, second.get(new ReadContext(newReader.txId(), 101,
                IsolationLevel.READ_COMMITTED), "cross:commit:right").orElseThrow().value());
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(newReader.txId());
        control.releaseSnapshot(afterSeal.txId());
    }

    /** 分条序号预写后，跨片意向与物化版本须对同一个旧 RR 和新 RC 保持同值。 */
    @Test
    void stripedMixedSnapshotMustSurviveOneSidedCrossMaterialization() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        try (var oracle = new MysqlStripedSnapshotOracle(
                Map.<String, DataSource>of("s1", firstData, "s2", secondData), 4, 8, true)) {
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "mixed:left", new byte[]{11}, 1, 0));
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    secondData, "mixed:right", new byte[]{12}, 2, 0));
            var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var oldRr = oracle.capture(control::currentPublished);
            assertEquals(100, oldRr.decisionEpoch());

            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            List<Mutation> left = List.of(Mutation.put("mixed:left", new byte[]{21}));
            List<Mutation> right = List.of(Mutation.put("mixed:right", new byte[]{22}));
            var leftProof = first.prewriteWithProof(writer.txId(), writer.startTs(), left);
            var rightProof = second.prewriteWithProof(writer.txId(), writer.startTs(), right);
            assertEquals(2, MysqlStripedCrossVersionPrototype.prewrite(
                    firstData, "mixed:left", new byte[]{21}, 1, writer.txId(), 1));
            assertEquals(2, MysqlStripedCrossVersionPrototype.prewrite(
                    secondData, "mixed:right", new byte[]{22}, 2, writer.txId(), 1));
            assertEquals(2, MysqlStripedCrossVersionPrototype.prewrite(
                    firstData, "mixed:left", new byte[]{21}, 1, writer.txId(), 1));
            assertEquals(36005, assertThrows(SQLException.class, () ->
                    MysqlStripedCrossVersionPrototype.prewrite(
                            firstData, "mixed:left", new byte[]{99}, 1, writer.txId(), 1))
                    .getErrorCode());
            var activeRr = oracle.capture(control::currentPublished);
            assertArrayEquals(new byte[]{11}, mixedValue(oracle, activeRr, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{12}, mixedValue(oracle, activeRr, "s2", "mixed:right", 2));
            assertEquals(36002, assertThrows(SQLException.class, () ->
                    MysqlStripedCrossVersionPrototype.localCommit(
                            firstData, "mixed:left", new byte[]{30}, 1, 2)).getErrorCode());
            assertThrows(IllegalStateException.class, () ->
                    MysqlStripedCrossVersionPrototype.materialize(
                            firstData, "mixed:left", writer.txId(), 101,
                            this::committedEpochFromCenter));

            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "striped-owner", "striped-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            assertThrows(IllegalStateException.class, () ->
                    MysqlStripedCrossVersionPrototype.materialize(
                            firstData, "mixed:left", writer.txId(), 102,
                            this::committedEpochFromCenter));
            var newRc = oracle.capture(control::currentPublished);
            assertEquals(101, newRc.decisionEpoch());
            assertArrayEquals(new byte[]{21}, mixedValue(oracle, newRc, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{22}, mixedValue(oracle, newRc, "s2", "mixed:right", 2));
            assertArrayEquals(new byte[]{11}, mixedValue(oracle, oldRr, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{12}, mixedValue(oracle, oldRr, "s2", "mixed:right", 2));
            assertArrayEquals(new byte[]{11}, mixedValue(oracle, activeRr, "s1", "mixed:left", 1));

            MysqlStripedCrossVersionPrototype.materialize(firstData, "mixed:left", writer.txId(), 101,
                    this::committedEpochFromCenter);
            MysqlStripedCrossVersionPrototype.materialize(firstData, "mixed:left", writer.txId(), 101,
                    this::committedEpochFromCenter);
            assertEquals(2, MysqlStripedCrossVersionPrototype.prewrite(
                    firstData, "mixed:left", new byte[]{21}, 1, writer.txId(), 1));
            assertArrayEquals(new byte[]{21}, mixedValue(oracle, newRc, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{22}, mixedValue(oracle, newRc, "s2", "mixed:right", 2));
            MysqlStripedCrossVersionPrototype.materialize(secondData, "mixed:right", writer.txId(), 101,
                    this::committedEpochFromCenter);
            MysqlStripedCrossVersionPrototype.materialize(secondData, "mixed:right", writer.txId(), 101,
                    this::committedEpochFromCenter);
            assertArrayEquals(new byte[]{21}, mixedValue(oracle, newRc, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{22}, mixedValue(oracle, newRc, "s2", "mixed:right", 2));
            assertArrayEquals(new byte[]{11}, mixedValue(oracle, oldRr, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{12}, mixedValue(oracle, oldRr, "s2", "mixed:right", 2));
            assertEquals(36003, assertThrows(SQLException.class, () ->
                    MysqlStripedCrossVersionPrototype.localCommit(
                            firstData, "mixed:left", new byte[]{30}, 1, 1)).getErrorCode());
            assertEquals(3, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "mixed:left", new byte[]{31}, 1, 2));
            var newerRc = oracle.capture(control::currentPublished);
            assertArrayEquals(new byte[]{31}, mixedValue(oracle, newerRc, "s1", "mixed:left", 1));
            assertArrayEquals(new byte[]{21}, mixedValue(oracle, newRc, "s1", "mixed:left", 1));
            first.commit(writer.txId(), 101, left, true);
            second.commit(writer.txId(), 101, right, true);
            control.releaseSnapshot(oldReader.txId());
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 中心中止后意向清理与墓碑重放必须阻止迟到预写，同时释放同键本地写入。 */
    @Test
    void stripedCrossAbortMustRemainTerminalAcrossLostResponses() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        try (var oracle = new MysqlStripedSnapshotOracle(
                Map.<String, DataSource>of("s1", firstData, "s2", secondData), 4, 8)) {
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "mixed:abort", new byte[]{41}, 1, 0));
            var oldRr = oracle.capture(control::currentPublished);
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
            assertEquals(2, MysqlStripedCrossVersionPrototype.prewrite(
                    firstData, "mixed:abort", new byte[]{42}, 1, writer.txId(), 1));
            var activeRr = oracle.capture(control::currentPublished);
            assertArrayEquals(new byte[]{41}, mixedValue(oracle, activeRr, "s1", "mixed:abort", 1));
            assertThrows(IllegalStateException.class, () ->
                    MysqlStripedCrossVersionPrototype.abort(firstData, "mixed:abort",
                            writer.txId(), this::statusFromCenter));

            assertEquals(TxStatus.ABORTED, control.abortIntentTransaction(writer.txId()).status());
            MysqlStripedCrossVersionPrototype.abort(firstData, "mixed:abort",
                    writer.txId(), this::statusFromCenter);
            MysqlStripedCrossVersionPrototype.abort(firstData, "mixed:abort",
                    writer.txId(), this::statusFromCenter);
            // 第二片即使从未收到预写，也先留下墓碑抵挡迟到的 RPC。
            MysqlStripedCrossVersionPrototype.abort(secondData, "mixed:late",
                    writer.txId(), this::statusFromCenter);
            assertEquals(36006, assertThrows(SQLException.class, () ->
                    MysqlStripedCrossVersionPrototype.prewrite(
                            firstData, "mixed:abort", new byte[]{42}, 1, writer.txId(), 1))
                    .getErrorCode());
            assertEquals(36006, assertThrows(SQLException.class, () ->
                    MysqlStripedCrossVersionPrototype.prewrite(
                            secondData, "mixed:late", new byte[]{43}, 2, writer.txId(), 0))
                    .getErrorCode());
            assertArrayEquals(new byte[]{41}, mixedValue(oracle, oldRr, "s1", "mixed:abort", 1));
            assertEquals(3, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "mixed:abort", new byte[]{44}, 1, 1));
            var freshRc = oracle.capture(control::currentPublished);
            assertArrayEquals(new byte[]{44}, mixedValue(oracle, freshRc, "s1", "mixed:abort", 1));
            assertArrayEquals(new byte[]{41}, mixedValue(oracle, oldRr, "s1", "mixed:abort", 1));
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 正式意向与分条序号同事务落盘，中心拒绝缺失保留证明的跨片决议。 */
    @Test
    void productVectorPrewriteProofMustBindIntentAndCenterDecision() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        assertTrue(control.isVectorIntentTransaction(writer.txId()));
        assertEquals(TxStatus.ACTIVE, control.registerVectorIntentTransaction(writer.txId(),
                writer.startTs(), writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                participants).status());
        assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                control.registerIntentTransaction(writer.txId(), writer.startTs(),
                        writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                        participants)).errorCode());
        Mutation left = Mutation.put("vector:proof:left", new byte[]{31});
        Mutation right = Mutation.put("vector:proof:right", new byte[]{32});
        var legacyLeft = first.prewriteWithProof(writer.txId(), writer.startTs(), List.of(left));
        var vectorRight = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
        var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "vector-proof-owner", "vector-proof-request", 4, 5_000, 100);
        var active = control.activateIntentSegment(reserved);
        var incomplete = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(legacyLeft, vectorRight));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () ->
                        control.completeVectorIntentBatch(List.of(incomplete))).errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());

        var vectorLeft = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0);
        assertEquals(1, vectorLeft.vectorReservation().orElseThrow().reservedSequence());
        assertEquals(1, vectorRight.vectorReservation().orElseThrow().reservedSequence());
        assertEquals(vectorLeft.vectorReservation().orElseThrow(),
                first.vectorReservation(writer.txId(), left.key()).orElseThrow());
        assertEquals(vectorRight.vectorReservation().orElseThrow(),
                second.vectorReservation(writer.txId(), right.key()).orElseThrow());
        assertEquals(1, first.vectorSequence(1));
        assertEquals(1, second.vectorSequence(2));
        assertEquals(1, first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0)
                .vectorReservation().orElseThrow().reservedSequence());
        assertEquals(1, first.vectorSequence(1));
        var restartedFirst = new MysqlStorageAdapter("s1", firstData, metadata);
        restartedFirst.initialize();
        restartedFirst.initializeVectorProtocol(4);
        var replayedLeft = restartedFirst.prewriteWithVectorProof(
                writer.txId(), writer.startTs(), left, 1, 0);
        assertEquals(vectorLeft.vectorReservation().orElseThrow(),
                replayedLeft.vectorReservation().orElseThrow());
        assertEquals(1, restartedFirst.vectorSequence(1));
        assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                first.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                        Mutation.put(left.key(), new byte[]{99}), 1, 0)).errorCode());
        assertArrayEquals(new byte[]{31}, first.get(new ReadContext(writer.txId(), writer.startTs(),
                IsolationLevel.REPEATABLE_READ), left.key()).orElseThrow().value());
        var complete = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(replayedLeft, vectorRight));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () ->
                        control.completeIntentBatch(List.of(complete))).errorCode());
        assertEquals(1, control.completeVectorIntentBatch(List.of(complete)));
        assertEquals(101, control.advancePublishedFast(10));
        var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertTrue(first.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), left.key()).isEmpty());
        assertTrue(second.get(new ReadContext(oldReader.txId(), oldReader.startTs(),
                IsolationLevel.REPEATABLE_READ), right.key()).isEmpty());
        assertArrayEquals(new byte[]{31}, first.get(new ReadContext(newReader.txId(), 101,
                IsolationLevel.READ_COMMITTED), left.key()).orElseThrow().value());
        assertArrayEquals(new byte[]{32}, second.get(new ReadContext(newReader.txId(), 101,
                IsolationLevel.READ_COMMITTED), right.key()).orElseThrow().value());
        first.commitVectorIntentWithProof(writer.txId(), 101, left);
        second.commitVectorIntentWithProof(writer.txId(), 101, right);
        first.commitVectorIntentWithProof(writer.txId(), 101, left);
        assertTrue(first.vectorReservation(writer.txId(), left.key()).isEmpty());
        assertTrue(second.vectorReservation(writer.txId(), right.key()).isEmpty());
        assertEquals(vectorLeft.vectorReservation().orElseThrow(),
                first.committedVectorReservation(writer.txId(), left.key()).orElseThrow());
        assertEquals(vectorRight.vectorReservation().orElseThrow(),
                second.committedVectorReservation(writer.txId(), right.key()).orElseThrow());
        assertEquals(1, first.vectorSequence(1));
        assertEquals(1, second.vectorSequence(2));
        var successor = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerVectorIntentTransaction(successor.txId(), successor.startTs(), successor.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                first.prewriteWithVectorProof(successor.txId(), successor.startTs(),
                        Mutation.put(left.key(), new byte[]{35}), 2, 0)).errorCode());
        assertEquals(0, first.vectorSequence(2));
        var successorProof = first.prewriteWithVectorProof(successor.txId(), successor.startTs(),
                Mutation.put(left.key(), new byte[]{35}), 1, 1);
        assertEquals(2, successorProof.vectorReservation().orElseThrow().reservedSequence());
        control.abortIntentTransaction(successor.txId());
        first.abortIntentWithProof(successor.txId());
        control.releaseSnapshot(oldReader.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(newReader.txId());
        control.releaseSnapshot(successor.txId());
    }

    /** 缺失目标分条时服务器中途失败，真实意向、参与者记录和取号须整笔回滚。 */
    @Test
    void productVectorPrewriteFailureMustRollbackIntentAndSequence() throws Exception {
        first.initializeVectorProtocol(4);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        assertThrows(IllegalArgumentException.class, () ->
                first.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                        Mutation.put("vector:rollback", new byte[]{33}), 4, 0));
        try (Connection connection = firstData.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_vector_stripe_state_v1 where stripe_id = 1");
            try {
                assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                        first.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                                Mutation.put("vector:rollback", new byte[]{33}), 1, 0)).errorCode());
            } finally {
                statement.executeUpdate("""
                        insert into mvcc_vector_stripe_state_v1(stripe_id, committed_seq) values (1, 0)
                        """);
            }
        }
        assertTrue(first.participantRecord(writer.txId()).isEmpty());
        assertTrue(first.get(new ReadContext(writer.txId(), writer.startTs(),
                IsolationLevel.REPEATABLE_READ), "vector:rollback").isEmpty());
        assertEquals(0, first.vectorSequence(0));
        assertEquals(0, first.vectorSequence(1));
        assertTrue(first.vectorReservation(writer.txId(), "vector:rollback").isEmpty());
        control.releaseSnapshot(writer.txId());
    }

    /** 服务端跨片组预写按原请求返回独立序号，任一重放错误回滚整个物理批次。 */
    @Test
    void productVectorPrewriteBatchMustBeAtomicAndReplayable() {
        first.initializeVectorProtocol(4);
        var firstWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var secondWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerVectorIntentBatch(List.of(
                new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                        firstWriter.txId(), firstWriter.startTs(), firstWriter.deadlineMillis(),
                        IsolationLevel.REPEATABLE_READ, participants),
                new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                        secondWriter.txId(), secondWriter.startTs(), secondWriter.deadlineMillis(),
                        IsolationLevel.REPEATABLE_READ, participants)));
        long expiry = System.currentTimeMillis() + 60_000;
        var firstRequest = new MysqlStorageAdapter.VectorPrewriteRequest(
                firstWriter.txId(), firstWriter.startTs(), expiry,
                Mutation.put("vector:cohort:z", new byte[]{63}), 1, 0);
        var secondRequest = new MysqlStorageAdapter.VectorPrewriteRequest(
                secondWriter.txId(), secondWriter.startTs(), expiry,
                Mutation.put("vector:cohort:b", new byte[]{64}), 1, 0);
        List<MysqlStorageAdapter.PrewriteProof> proofs = first.prewriteVectorBatchWithProof(
                List.of(firstRequest, secondRequest));
        assertEquals(2, proofs.get(0).vectorReservation().orElseThrow().reservedSequence());
        assertEquals(1, proofs.get(1).vectorReservation().orElseThrow().reservedSequence());
        assertEquals(2, first.vectorSequence(1));
        assertEquals(0, first.vectorSequence(2));
        assertEquals(2, first.prewriteVectorBatchWithProof(List.of(firstRequest, secondRequest))
                .getFirst().vectorReservation().orElseThrow().reservedSequence());
        assertEquals(2, first.vectorSequence(1));
        assertEquals(0, first.vectorSequence(2));

        var thirdWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerVectorIntentTransaction(thirdWriter.txId(), thirdWriter.startTs(),
                thirdWriter.deadlineMillis(), IsolationLevel.REPEATABLE_READ, participants);
        var newRequest = new MysqlStorageAdapter.VectorPrewriteRequest(
                thirdWriter.txId(), thirdWriter.startTs(), expiry,
                Mutation.put("vector:cohort:new", new byte[]{65}), 0, 0);
        var changedReplay = new MysqlStorageAdapter.VectorPrewriteRequest(
                firstWriter.txId(), firstWriter.startTs(), expiry,
                Mutation.put("vector:cohort:z", new byte[]{66}), 1, 0);
        assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                first.prewriteVectorBatchWithProof(List.of(newRequest, changedReplay))).errorCode());
        assertEquals(0, first.vectorSequence(0));
        assertTrue(first.participantRecord(thirdWriter.txId()).isEmpty());
        assertTrue(first.vectorReservation(thirdWriter.txId(), "vector:cohort:new").isEmpty());
        assertArrayEquals(new byte[]{63}, first.get(new ReadContext(firstWriter.txId(),
                firstWriter.startTs(), IsolationLevel.REPEATABLE_READ),
                "vector:cohort:z").orElseThrow().value());
        control.abortIntentTransaction(firstWriter.txId());
        control.abortIntentTransaction(secondWriter.txId());
        control.abortIntentTransaction(thirdWriter.txId());
        first.abortIntentWithProof(firstWriter.txId());
        first.abortIntentWithProof(secondWriter.txId());
        control.releaseSnapshot(firstWriter.txId());
        control.releaseSnapshot(secondWriter.txId());
        control.releaseSnapshot(thirdWriter.txId());
    }

    /** 独立跨片客户端经有界预写队列仍逐笔获得证明，单笔冲突不污染同组成员。 */
    @Test
    void productVectorPrewriteBatcherMustReturnIndependentProofs() throws Exception {
        first.initializeVectorProtocol(4);
        List<com.flyingzc.azir.metadata.BeginReservation> writers = new java.util.ArrayList<>();
        List<MysqlLocalFirstPublicationLedger.IntentRegistrationRequest> registrations =
                new java.util.ArrayList<>();
        Set<String> participants = Set.of("s1", "s2");
        for (int index = 0; index < 8; index++) {
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            writers.add(writer);
            registrations.add(new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                    writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants));
        }
        control.registerVectorIntentBatch(registrations);
        long expiry = System.currentTimeMillis() + 60_000;
        try (var batcher = new MysqlVectorCrossPrewriteBatcher(first, 2, 4, 2_000, 64)) {
            List<CompletableFuture<MysqlStorageAdapter.PrewriteProof>> futures = new java.util.ArrayList<>();
            for (int index = 0; index < writers.size(); index++) {
                var writer = writers.get(index);
                futures.add(batcher.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                        writer.txId(), writer.startTs(), expiry,
                        Mutation.put("vector:queued:" + index, new byte[]{(byte) index}),
                        index % 4, 0), 5_000));
            }
            for (int index = 0; index < writers.size(); index++) {
                var writer = writers.get(index);
                var proof = futures.get(index).get(5, TimeUnit.SECONDS);
                assertEquals(writer.txId(), proof.txId());
                assertEquals(proof.vectorReservation().orElseThrow(),
                        first.vectorReservation(writer.txId(), "vector:queued:" + index).orElseThrow());
            }
            assertEquals(8, batcher.groupedCount());

            var conflictWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var independentWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            control.registerVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                            conflictWriter.txId(), conflictWriter.startTs(), conflictWriter.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants),
                    new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                            independentWriter.txId(), independentWriter.startTs(),
                            independentWriter.deadlineMillis(), IsolationLevel.REPEATABLE_READ,
                            participants)));
            var conflicted = batcher.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                    conflictWriter.txId(), conflictWriter.startTs(), expiry,
                    Mutation.put("vector:queued:0", new byte[]{42}), 0, 0), 5_000);
            var independent = batcher.submit(new MysqlStorageAdapter.VectorPrewriteRequest(
                    independentWriter.txId(), independentWriter.startTs(), expiry,
                    Mutation.put("vector:queued:fresh", new byte[]{43}), 0, 0), 5_000);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> conflicted.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.LOCK_CONFLICT,
                    ((MvccException) failure.getCause()).errorCode());
            assertEquals(independentWriter.txId(), independent.get(5, TimeUnit.SECONDS).txId());
            assertTrue(first.vectorReservation(conflictWriter.txId(), "vector:queued:0").isEmpty());
            control.abortIntentTransaction(conflictWriter.txId());
            control.abortIntentTransaction(independentWriter.txId());
            first.abortIntentWithProof(independentWriter.txId());
            control.releaseSnapshot(conflictWriter.txId());
            control.releaseSnapshot(independentWriter.txId());
        }
        for (var writer : writers) {
            control.abortIntentTransaction(writer.txId());
            first.abortIntentWithProof(writer.txId());
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 中心 ABORTED 后正式意向与序号保留同事务清除，旧事务不可复活而新事务可继续写。 */
    @Test
    void productVectorAbortMustReleaseReservationWithoutReusingOldSequence() {
        first.initializeVectorProtocol(4);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        Mutation mutation = Mutation.put("vector:abort", new byte[]{34});
        var proof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), mutation, 1, 0);
        assertEquals(1, proof.vectorReservation().orElseThrow().reservedSequence());
        assertEquals(TxStatus.ABORTED, control.abortIntentTransaction(writer.txId()).status());
        assertEquals(TxStatus.ABORTED, first.abortIntentWithProof(writer.txId()).record().status());
        assertTrue(first.vectorReservation(writer.txId(), mutation.key()).isEmpty());
        assertEquals(1, first.vectorSequence(1));
        assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(MvccException.class, () ->
                first.prewriteWithVectorProof(writer.txId(), writer.startTs(), mutation, 1, 0)).errorCode());

        var successor = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerVectorIntentTransaction(successor.txId(), successor.startTs(), successor.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        var nextProof = first.prewriteWithVectorProof(
                successor.txId(), successor.startTs(), mutation, 1, 1);
        assertEquals(2, nextProof.vectorReservation().orElseThrow().reservedSequence());
        assertEquals(2, first.vectorSequence(1));
        control.abortIntentTransaction(successor.txId());
        first.abortIntentWithProof(successor.txId());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(successor.txId());
    }

    /** 向量事务恢复缺少任一分片协议布局时保留中心锚点，修复后再补齐两片。 */
    @Test
    void productVectorRecoveryMustMaterializeEveryShardBeforeFinalization() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        Mutation left = Mutation.put("vector:recover:left", new byte[]{36});
        Mutation right = Mutation.put("vector:recover:right", new byte[]{37});
        var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0);
        var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
        var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "vector-recover-owner", "vector-recover-request", 1, 5_000, 100);
        var active = control.activateIntentSegment(reserved);
        assertEquals(1, control.completeVectorIntentBatch(List.of(
                new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                        writer.txId(), writer.startTs(), writer.deadlineMillis(),
                        IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
        assertEquals(101, control.advancePublishedFast(10));

        var restartedSecond = new MysqlStorageAdapter("s2", secondData, metadata);
        restartedSecond.initialize();
        var recovery = new LocalFirstIntentRecoveryManager(control,
                Map.of("s1", first, "s2", restartedSecond));
        assertEquals(1, recovery.recoverOnce(10).failures());
        assertEquals(List.of(writer.txId()),
                control.findUnfinalizedIntentDecisions(Long.MIN_VALUE, 10));
        restartedSecond.initializeVectorProtocol(4);
        var resumed = recovery.recoverOnce(10);
        assertEquals(1, resumed.finalized(), () -> "vector recovery failure: " + recovery.lastFailure());
        assertTrue(control.findUnfinalizedIntentDecisions(Long.MIN_VALUE, 10).isEmpty());
        assertTrue(first.vectorReservation(writer.txId(), left.key()).isEmpty());
        assertTrue(restartedSecond.vectorReservation(writer.txId(), right.key()).isEmpty());
        assertEquals(leftProof.vectorReservation().orElseThrow(),
                first.committedVectorReservation(writer.txId(), left.key()).orElseThrow());
        assertEquals(rightProof.vectorReservation().orElseThrow(),
                restartedSecond.committedVectorReservation(writer.txId(), right.key()).orElseThrow());
        var reader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
        assertArrayEquals(new byte[]{36}, first.get(new ReadContext(reader.txId(), 101,
                IsolationLevel.READ_COMMITTED), left.key()).orElseThrow().value());
        assertArrayEquals(new byte[]{37}, restartedSecond.get(new ReadContext(reader.txId(), 101,
                IsolationLevel.READ_COMMITTED), right.key()).orElseThrow().value());
        control.releaseSnapshot(writer.txId());
        control.releaseSnapshot(reader.txId());
        control.beginFencing(active, true);
        assertEquals(0, control.sealIntentSegment(active));
        var next = control.activateIntentSegment(control.reserveSegment(
                MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "vector-retention-next-owner", "vector-retention-next-request", 1, 5_000, 101));
        control.beginFencing(next, true);
        assertEquals(1, control.sealIntentSegment(next));
        assertEquals(102, control.advancePublishedFast(10));
        long safe = control.advanceSafePoint();
        assertTrue(safe > 101);
        control.purgeTerminalSlots(safe, Long.MAX_VALUE, 10);
        assertEquals(0, control.purgeFinalizedMetadata(safe, Long.MAX_VALUE, 10));
        assertTrue(control.decision(writer.txId()).isPresent());
        assertTrue(control.isVectorIntentTransaction(writer.txId()));
    }

    /** 旧 RR 退出且中心两片最终化后才退役跨片大载荷，重启重放只认紧凑凭据。 */
    @Test
    void vectorCrossGcMustPreserveOldRrAndDurableReplay() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            String leftKey = "vector:cross:retire:left";
            String rightKey = "vector:cross:retire:right";
            assertEquals(1, first.directCommitVectorLocal(-11_001,
                    Mutation.put(leftKey, new byte[]{1}), 1, 0));
            cut.publishOnce();
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerVectorIntentTransaction(writer.txId(), writer.startTs(),
                    writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ, participants);
            Mutation left = Mutation.put(leftKey, new byte[]{2});
            Mutation right = Mutation.put(rightKey, new byte[]{3});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 1);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "vector-retire-owner", "vector-retire-request", 1, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            first.commitVectorIntentWithProof(writer.txId(), 101, left);
            second.commitVectorIntentWithProof(writer.txId(), 101, right);
            assertTrue(control.finalizedVectorProof(writer.txId()).isEmpty());
            var recovery = new LocalFirstIntentRecoveryManager(control,
                    Map.of("s1", first, "s2", second));
            assertEquals(1, recovery.recoverOnce(10).finalized());
            assertTrue(control.finalizedVectorProof(writer.txId()).isPresent());
            control.releaseSnapshot(writer.txId());
            var crossCut = cut.publishOnce();
            AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published =
                    new AtomicReference<>(crossCut);
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    new MysqlVectorCrossGarbageCollector(first, secondData,
                            "s1", 4, leases, control).initializeProduct()).errorCode());
            var collector = new MysqlVectorCrossGarbageCollector(first, firstData,
                    "s1", 4, leases, control);
            collector.initializeProduct();
            try (var protection = new MysqlVectorReaderProtectionRuntime(
                    leases, published::get, "cross-retire-reader", 5_000, 1_000, 100)) {
                protection.start();
                var reader = new MysqlVectorProtectedReader(Map.of("s1", first, "s2", second),
                        protection);
                try (var oldRr = reader.beginRepeatableRead(11_001)) {
                    assertArrayEquals(new byte[]{2}, oldRr.get("s1", leftKey, 1)
                            .orElseThrow().value());
                    assertEquals(3, first.directCommitVectorLocal(-11_002,
                            Mutation.put(leftKey, new byte[]{4}), 1, 2));
                    published.set(cut.publishOnce());
                    protection.refresh();
                    leases.advanceSafeFloor(published.get());
                    assertEquals(0, collector.collect(10));
                    assertArrayEquals(new byte[]{2}, oldRr.get("s1", leftKey, 1)
                            .orElseThrow().value());
                }
                protection.refresh();
                leases.advanceSafeFloor(published.get());
                try (Connection connection = firstData.getConnection(); Statement statement = connection.createStatement()) {
                    statement.execute("""
                            create table mvcc_vector_retire_block (
                              user_key varbinary(256) not null,
                              reserved_seq bigint not null,
                              primary key (user_key, reserved_seq),
                              constraint fk_vector_retire_block foreign key (user_key, reserved_seq)
                                references mvcc_vector_committed_v1(user_key, reserved_seq)
                                on delete restrict
                            ) engine=InnoDB
                            """);
                    try (PreparedStatement block = connection.prepareStatement("""
                            insert into mvcc_vector_retire_block(user_key, reserved_seq) values (?, 2)
                            """)) {
                        block.setBytes(1, leftKey.getBytes(StandardCharsets.UTF_8));
                        block.executeUpdate();
                    }
                }
                try {
                    assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            assertThrows(MvccException.class, () -> collector.collect(10)).errorCode());
                    try (Connection connection = firstData.getConnection();
                         PreparedStatement query = connection.prepareStatement("""
                                 select (select count(*) from mvcc_vector_retired_v1 where tx_id = ?),
                                        (select count(*) from mvcc_data where tx_id = ?),
                                        (select count(*) from mvcc_vector_committed_v1 where tx_id = ?)
                                 """)) {
                        for (int index = 1; index <= 3; index++) {
                            query.setLong(index, writer.txId());
                        }
                        try (ResultSet row = query.executeQuery()) {
                            assertTrue(row.next());
                            assertEquals(0, row.getLong(1));
                            assertEquals(1, row.getLong(2));
                            assertEquals(1, row.getLong(3));
                        }
                    }
                } finally {
                    try (Connection connection = firstData.getConnection();
                         Statement statement = connection.createStatement()) {
                        statement.execute("drop table if exists mvcc_vector_retire_block");
                    }
                }
                var leaderLease = MysqlVectorCutLeaderLease.forProduct(controlData, routes, 4);
                leaderLease.initializeProduct();
                try (HikariDataSource backgroundControl = pool("control");
                     HikariDataSource backgroundFirst = pool("shard_1");
                     HikariDataSource backgroundSecond = pool("shard_2");
                     var leader = MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, 4,
                             control::currentPublished, leaderLease,
                             "cross-retire-scheduler-leader", 5_000, 200)) {
                    assertTrue(leader.start());
                    var backgroundLeases = new MysqlVectorSnapshotLeaseStore(backgroundControl,
                            List.of("s1", "s2"), 4);
                    backgroundLeases.initializeProduct();
                    var firstLocalGc = new MysqlVectorLocalGarbageCollector(backgroundFirst,
                            "s1", 4, backgroundLeases);
                    var secondLocalGc = new MysqlVectorLocalGarbageCollector(backgroundSecond,
                            "s2", 4, backgroundLeases);
                    firstLocalGc.initializeProduct();
                    secondLocalGc.initializeProduct();
                    var firstCrossGc = new MysqlVectorCrossGarbageCollector(first,
                            backgroundFirst, "s1", 4, backgroundLeases, control);
                    var secondCrossGc = new MysqlVectorCrossGarbageCollector(second,
                            backgroundSecond, "s2", 4, backgroundLeases, control);
                    firstCrossGc.initializeProduct();
                    secondCrossGc.initializeProduct();
                    try (var scheduler = new MysqlVectorLocalGcScheduler(leader, backgroundLeases,
                            Map.of("s1", firstLocalGc, "s2", secondLocalGc),
                            Map.of("s1", firstCrossGc, "s2", secondCrossGc), 100, 10, 50)) {
                        scheduler.runOnce();
                        assertEquals(1, scheduler.crossReclaimed());
                    }
                }
                assertEquals(0, collector.retireOne(writer.txId()));
                assertArrayEquals(new byte[]{4}, reader.getReadCommitted(11_002,
                        "s1", leftKey, 1).orElseThrow().value());
                assertArrayEquals(new byte[]{4}, reader.scanReadCommitted(11_003,
                        "s1", new KeyRange(leftKey, leftKey + "~")).getFirst().value());
                var restarted = new MysqlStorageAdapter("s1", firstData, metadata);
                restarted.initialize();
                restarted.initializeVectorProtocol(4);
                restarted.commitVectorIntentWithProof(control.committedIntentProof(writer.txId()));
                restarted.commitVectorIntentWithProof(writer.txId(), 101, left);
                assertEquals(ErrorCode.ILLEGAL_STATE,
                        assertThrows(MvccException.class, () -> restarted.commitVectorIntentWithProof(
                                writer.txId(), 101, Mutation.put(leftKey, new byte[]{99}))).errorCode());
                assertTrue(restarted.committedVectorReservation(writer.txId(), leftKey).isEmpty());
                try (Connection connection = firstData.getConnection();
                     PreparedStatement query = connection.prepareStatement("""
                             select (select count(*) from mvcc_vector_retired_v1 where tx_id = ?) as receipts,
                                    (select count(*) from mvcc_data where tx_id = ?) as data_rows
                             """)) {
                    query.setLong(1, writer.txId());
                    query.setLong(2, writer.txId());
                    try (ResultSet row = query.executeQuery()) {
                        assertTrue(row.next());
                        assertEquals(1, row.getLong("receipts"));
                        assertEquals(0, row.getLong("data_rows"));
                    }
                }
            }
        }
    }

    /** 两键批量退休中任一映射删除失败时，全批凭据与大载荷必须一起回滚。 */
    @Test
    void vectorCrossGcBatchMustRollBackAllKeysOnOneFailure() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            Set<String> participants = Set.of("s1", "s2");
            var firstWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var secondWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            List<BeginReservation> writers = List.of(firstWriter, secondWriter);
            List<Mutation> leftWrites = List.of(
                    Mutation.put("vector:retire:batch:a", new byte[]{1}),
                    Mutation.put("vector:retire:batch:b", new byte[]{2}));
            List<Mutation> rightWrites = List.of(
                    Mutation.put("vector:retire:batch:right:a", new byte[]{3}),
                    Mutation.put("vector:retire:batch:right:b", new byte[]{4}));
            List<MysqlStorageAdapter.PrewriteProof> leftProofs = new java.util.ArrayList<>();
            List<MysqlStorageAdapter.PrewriteProof> rightProofs = new java.util.ArrayList<>();
            for (int index = 0; index < writers.size(); index++) {
                var writer = writers.get(index);
                control.registerVectorIntentTransaction(writer.txId(), writer.startTs(),
                        writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ, participants);
                leftProofs.add(first.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                        leftWrites.get(index), 1, 0));
                rightProofs.add(second.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                        rightWrites.get(index), 2, 0));
            }
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "vector-retire-batch-owner", "vector-retire-batch-request", 2, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            List<MysqlLocalFirstPublicationLedger.IntentCommitRequest> decisions = new java.util.ArrayList<>();
            for (int index = 0; index < writers.size(); index++) {
                var writer = writers.get(index);
                decisions.add(new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active,
                        101 + index, writer.txId(), writer.startTs(), writer.deadlineMillis(),
                        IsolationLevel.REPEATABLE_READ, participants,
                        List.of(leftProofs.get(index), rightProofs.get(index))));
            }
            assertEquals(2, control.completeVectorIntentBatch(decisions));
            assertEquals(102, control.advancePublishedFast(10));
            var recovery = new LocalFirstIntentRecoveryManager(control,
                    Map.of("s1", first, "s2", second));
            assertEquals(2, recovery.recoverOnce(10).finalized());
            writers.forEach(writer -> control.releaseSnapshot(writer.txId()));
            for (int index = 0; index < writers.size(); index++) {
                long predecessor = leftProofs.get(index).vectorReservation()
                        .orElseThrow().reservedSequence();
                first.directCommitVectorLocal(-12_001 - index,
                        Mutation.put(leftWrites.get(index).key(), new byte[]{9}), 1, predecessor);
            }
            var latest = cut.publishOnce();
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            leases.advanceSafeFloor(latest);
            var collector = new MysqlVectorCrossGarbageCollector(first, firstData,
                    "s1", 4, leases, control, 1, 2);
            collector.initializeProduct();
            try (Connection connection = firstData.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("""
                        create table mvcc_vector_retire_block (
                          user_key varbinary(256) not null,
                          reserved_seq bigint not null,
                          primary key (user_key, reserved_seq),
                          constraint fk_vector_retire_block foreign key (user_key, reserved_seq)
                            references mvcc_vector_committed_v1(user_key, reserved_seq)
                            on delete restrict
                        ) engine=InnoDB
                        """);
                try (PreparedStatement block = connection.prepareStatement("""
                        insert into mvcc_vector_retire_block(user_key, reserved_seq) values (?, ?)
                        """)) {
                    block.setBytes(1, leftWrites.get(1).key().getBytes(StandardCharsets.UTF_8));
                    block.setLong(2, leftProofs.get(1).vectorReservation()
                            .orElseThrow().reservedSequence());
                    block.executeUpdate();
                }
            }
            try {
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> collector.collect(2)).errorCode());
                assertEquals(0, countRows(firstData, "mvcc_vector_retired_v1"));
                assertEquals(2, countRows(firstData, "mvcc_vector_committed_v1"));
                assertEquals(2, countRows(firstData, "mvcc_data"));
            } finally {
                try (Connection connection = firstData.getConnection();
                     Statement statement = connection.createStatement()) {
                    statement.execute("drop table if exists mvcc_vector_retire_block");
                }
            }
            int retired = 0;
            for (int attempt = 0; attempt < 3 && retired < 2; attempt++) {
                retired += collector.collect(2);
            }
            assertEquals(2, retired);
            assertEquals(2, countRows(firstData, "mvcc_vector_retired_v1"));
            assertEquals(0, countRows(firstData, "mvcc_vector_committed_v1"));
            assertEquals(0, countRows(firstData, "mvcc_data"));
            for (int index = 0; index < writers.size(); index++) {
                first.commitVectorIntentWithProof(control.committedIntentProof(writers.get(index).txId()));
                assertArrayEquals(new byte[]{9}, first.getVector(latest, 12_010 + index,
                        leftWrites.get(index).key(), 1).orElseThrow().value());
            }
        }
    }

    /** 向量 ACTIVE 批量登记与中心同事务决议发布必须保留完整分片证明。 */
    @Test
    void productVectorBatchRegistrationAndPublicationMustPreserveDecision() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldRr = cut.currentCut();
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            var request = new MysqlLocalFirstPublicationLedger.IntentRegistrationRequest(
                    writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            assertEquals(1, control.registerVectorIntentBatch(List.of(request)).size());
            assertTrue(control.isVectorIntentTransaction(writer.txId()));
            Mutation left = Mutation.put("vector:batch-center:left", new byte[]{59});
            Mutation right = Mutation.put("vector:batch-center:right", new byte[]{60});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "vector-batch-owner", "vector-batch-request", 1, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            var decision = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                    writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof));
            var completed = control.completeVectorIntentBatchAndPublish(List.of(decision));
            assertEquals(1, completed.completed());
            assertEquals(101, completed.publishedTs());
            assertEquals(100, cut.currentCut().decisionEpoch());
            var newRc = cut.publishOnce();
            assertEquals(101, newRc.decisionEpoch());
            assertArrayEquals(new byte[]{59}, first.getVector(newRc, 999, left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{60}, second.getVector(newRc, 999, right.key(), 2)
                    .orElseThrow().value());
            assertTrue(first.getVector(oldRr, 999, left.key(), 1).isEmpty());
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 正式向量读在中心决议、单侧物化与双侧物化后保持 RC/RR 同一可见切面。 */
    @Test
    void productVectorReadMustPreserveRcAndRrAcrossCrossShardMaterialization() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var oracle = MysqlStripedSnapshotOracle.forProduct(routes, 4, 8, true)) {
            var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var oldRr = oracle.capture(control::currentPublished);
            assertEquals(0, oldRr.sequence("s1", 1));
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            Mutation left = Mutation.put("vector:read:left", new byte[]{38});
            Mutation right = Mutation.put("vector:read:right", new byte[]{39});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
            var activeRr = oracle.capture(control::currentPublished);
            assertEquals(100, activeRr.decisionEpoch());
            assertTrue(first.getVector(activeRr, oldReader.txId(), left.key(), 1).isEmpty());
            assertTrue(second.getVector(activeRr, oldReader.txId(), right.key(), 2).isEmpty());
            assertArrayEquals(new byte[]{38}, first.getVector(oldRr, writer.txId(), left.key(), 1)
                    .orElseThrow().value());
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "vector-read-owner", "vector-read-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
            var newRc = oracle.capture(control::currentPublished);
            assertArrayEquals(new byte[]{38}, first.getVector(newRc, newReader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{39}, second.getVector(newRc, newReader.txId(), right.key(), 2)
                    .orElseThrow().value());
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    first.getVector(newRc, newReader.txId(), left.key(), 2)).errorCode());
            assertTrue(first.getVector(oldRr, oldReader.txId(), left.key(), 1).isEmpty());
            assertTrue(first.getVector(activeRr, oldReader.txId(), left.key(), 1).isEmpty());

            first.commitVectorIntentWithProof(writer.txId(), 101, left);
            assertArrayEquals(new byte[]{38}, first.getVector(newRc, newReader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{39}, second.getVector(newRc, newReader.txId(), right.key(), 2)
                    .orElseThrow().value());
            second.commitVectorIntentWithProof(writer.txId(), 101, right);
            assertArrayEquals(new byte[]{38}, first.getVector(newRc, newReader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{39}, second.getVector(newRc, newReader.txId(), right.key(), 2)
                    .orElseThrow().value());
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    first.getVector(newRc, newReader.txId(), left.key(), 2)).errorCode());
            assertTrue(first.getVector(oldRr, oldReader.txId(), left.key(), 1).isEmpty());
            assertTrue(second.getVector(oldRr, oldReader.txId(), right.key(), 2).isEmpty());
            control.releaseSnapshot(oldReader.txId());
            control.releaseSnapshot(writer.txId());
            control.releaseSnapshot(newReader.txId());
        }
    }

    /** 同键旧标量版本没有分条顺序证明时，正式向量读必须失败关闭。 */
    @Test
    void productVectorReadMustRejectUnmappedScalarVersion() throws Exception {
        first.initializeVectorProtocol(4);
        try (Connection connection = firstData.getConnection();
             java.sql.PreparedStatement insert = connection.prepareStatement("""
                     insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                     values (?, 101, 999, 0, ?)
                     """)) {
            insert.setBytes(1, "vector:legacy".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            insert.setBytes(2, new byte[]{40});
            insert.executeUpdate();
        }
        try (var oracle = MysqlStripedSnapshotOracle.forProduct(
                Map.<String, DataSource>of("s1", firstData), 4, 8, false)) {
            var snapshot = oracle.capture(control::currentPublished);
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () ->
                            first.getVector(snapshot, 123, "vector:legacy", 1)).errorCode());
        }
    }

    /** 正式持久切面先纳入两片保留序号，再纳入中心决议，新 RC 才能同时读到两片。 */
    @Test
    void productPublishedCutMustGateCrossShardRcAndSurviveReopen() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldRr = cut.currentCut();
            var reader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            Mutation left = Mutation.put("vector:cut:left", new byte[]{41});
            Mutation right = Mutation.put("vector:cut:right", new byte[]{42});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
            var activeCut = cut.publishOnce();
            assertEquals(100, activeCut.decisionEpoch());
            assertEquals(1, activeCut.sequence("s1", 1));
            assertEquals(1, activeCut.sequence("s2", 2));
            assertTrue(first.getVector(activeCut, reader.txId(), left.key(), 1).isEmpty());
            assertTrue(second.getVector(activeCut, reader.txId(), right.key(), 2).isEmpty());

            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "product-cut-owner", "product-cut-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            assertEquals(100, cut.currentCut().decisionEpoch());
            var newRc = cut.publishOnce();
            assertEquals(101, newRc.decisionEpoch());
            assertEquals(newRc, cut.currentCut());
            assertArrayEquals(new byte[]{41}, first.getVector(newRc, reader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{42}, second.getVector(newRc, reader.txId(), right.key(), 2)
                    .orElseThrow().value());
            assertTrue(first.getVector(oldRr, reader.txId(), left.key(), 1).isEmpty());
            first.commitVectorIntentWithProof(writer.txId(), 101, left);
            assertArrayEquals(new byte[]{41}, first.getVector(newRc, reader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{42}, second.getVector(newRc, reader.txId(), right.key(), 2)
                    .orElseThrow().value());
            second.commitVectorIntentWithProof(writer.txId(), 101, right);
            try (var reopened = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                    control::currentPublished)) {
                reopened.initializeProduct();
                assertEquals(newRc, reopened.currentCut());
            }
            control.releaseSnapshot(reader.txId());
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 正式表的持租约 TCP 切面跨决议、物化及领导者接任仍保持全局 RC/RR。 */
    @Test
    void productLeasedTcpCutMustPreserveCrossShardVisibilityAcrossHandoff() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = MysqlVectorCutLeaderLease.forProduct(controlData, routes, 4);
        leases.initializeProduct();
        var wrongLeaseNamespace = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        assertThrows(IllegalArgumentException.class, () ->
                MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, 4,
                        control::currentPublished, wrongLeaseNamespace,
                        "127.0.0.1:29999", 1_000, 200));
        int firstPort = unusedLoopbackPort();
        int secondPort = unusedLoopbackPort();
        while (secondPort == firstPort) {
            secondPort = unusedLoopbackPort();
        }
        try (var firstRuntime = MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, 4,
                control::currentPublished, leases, "127.0.0.1:" + firstPort, 1_000, 200);
             var nextRuntime = MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, 4,
                     control::currentPublished, leases, "127.0.0.1:" + secondPort, 1_000, 200);
             var firstServer = new MysqlVectorCutTcpServer(firstRuntime, firstPort);
             var nextServer = new MysqlVectorCutTcpServer(nextRuntime, secondPort);
             var remote = MysqlVectorCutTcpClient.forProduct(controlData, routes, 4)) {
            assertTrue(firstRuntime.start());
            firstServer.start();
            var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var oldRr = remote.currentView().snapshot();
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            Mutation left = Mutation.put("vector:tcp:left", new byte[]{43});
            Mutation right = Mutation.put("vector:tcp:right", new byte[]{44});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 0);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "product-tcp-owner", "product-tcp-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
            MysqlVectorCutLeaderRuntime.PublishedView acknowledged = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                var candidate = remote.currentView();
                if (candidate.snapshot().decisionEpoch() >= 101
                        && candidate.snapshot().sequence("s1", 1) >= 1
                        && candidate.snapshot().sequence("s2", 2) >= 1) {
                    acknowledged = candidate;
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(acknowledged != null);
            var newRc = acknowledged.snapshot();
            assertArrayEquals(new byte[]{43}, first.getVector(newRc, newReader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{44}, second.getVector(newRc, newReader.txId(), right.key(), 2)
                    .orElseThrow().value());
            assertTrue(first.getVector(oldRr, oldReader.txId(), left.key(), 1).isEmpty());
            first.commitVectorIntentWithProof(writer.txId(), 101, left);
            assertArrayEquals(new byte[]{43}, first.getVector(newRc, newReader.txId(), left.key(), 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{44}, second.getVector(newRc, newReader.txId(), right.key(), 2)
                    .orElseThrow().value());
            second.commitVectorIntentWithProof(writer.txId(), 101, right);

            firstRuntime.close();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());
            long takeoverDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean tookOver = false;
            while (!tookOver && System.nanoTime() < takeoverDeadline) {
                tookOver = nextRuntime.start();
                if (!tookOver) {
                    Thread.sleep(50);
                }
            }
            assertTrue(tookOver);
            nextServer.start();
            var recovered = remote.currentView();
            assertTrue(recovered.generation() > acknowledged.generation());
            assertTrue(recovered.snapshot().decisionEpoch() >= 101);
            assertArrayEquals(new byte[]{43}, first.getVector(recovered.snapshot(),
                    newReader.txId(), left.key(), 1).orElseThrow().value());
            assertArrayEquals(new byte[]{44}, second.getVector(recovered.snapshot(),
                    newReader.txId(), right.key(), 2).orElseThrow().value());
            assertTrue(first.getVector(oldRr, oldReader.txId(), left.key(), 1).isEmpty());
            control.releaseSnapshot(oldReader.txId());
            control.releaseSnapshot(writer.txId());
            control.releaseSnapshot(newReader.txId());
        }
    }

    /** 正式本地 1PC 由分条切面确认，RC 更新而旧 RR 不变，冲突、重放和墓碑均保留。 */
    @Test
    void productLocalVectorOnePhaseCommitMustPreserveRcAndRr() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var oldRr = cut.currentCut();
            Mutation firstValue = Mutation.put("vector:local:one", new byte[]{45});
            assertEquals(1, first.directCommitVectorLocal(-11, firstValue, 1, 0));
            assertEquals(0, cut.currentCut().sequence("s1", 1));
            assertTrue(first.getVector(oldRr, oldReader.txId(), firstValue.key(), 1).isEmpty());
            var firstRc = cut.publishOnce();
            var newReader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
            assertEquals(100, firstRc.decisionEpoch());
            assertEquals(1, firstRc.sequence("s1", 1));
            assertArrayEquals(new byte[]{45}, first.getVector(firstRc, newReader.txId(),
                    firstValue.key(), 1).orElseThrow().value());
            assertTrue(first.getVector(oldRr, oldReader.txId(), firstValue.key(), 1).isEmpty());
            assertEquals(1, first.directCommitVectorLocal(-11, firstValue, 1, 0));
            assertEquals(1, first.vectorSequence(1));
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    first.directCommitVectorLocal(-11,
                            Mutation.put(firstValue.key(), new byte[]{99}), 1, 0)).errorCode());
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    first.getVector(firstRc, newReader.txId(), firstValue.key(), 2)).errorCode());
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    first.directCommitVectorLocal(-12,
                            Mutation.put(firstValue.key(), new byte[]{46}), 2, 0)).errorCode());
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(MvccException.class, () ->
                    first.directCommitVectorLocal(-12,
                            Mutation.put(firstValue.key(), new byte[]{46}), 1, 0)).errorCode());
            assertEquals(2, first.directCommitVectorLocal(-12,
                    Mutation.put(firstValue.key(), new byte[]{46}), 1, 1));
            var secondRc = cut.publishOnce();
            assertArrayEquals(new byte[]{46}, first.getVector(secondRc, newReader.txId(),
                    firstValue.key(), 1).orElseThrow().value());
            assertArrayEquals(new byte[]{45}, first.getVector(firstRc, newReader.txId(),
                    firstValue.key(), 1).orElseThrow().value());
            assertEquals(3, first.directCommitVectorLocal(-13,
                    Mutation.delete(firstValue.key()), 1, 2));
            var deletedRc = cut.publishOnce();
            assertTrue(first.getVector(deletedRc, newReader.txId(), firstValue.key(), 1)
                    .orElseThrow().deleted());
            assertArrayEquals(new byte[]{46}, first.getVector(secondRc, newReader.txId(),
                    firstValue.key(), 1).orElseThrow().value());
            control.releaseSnapshot(oldReader.txId());
            control.releaseSnapshot(newReader.txId());
        }
    }

    /** 同键本地、跨片、本地先后提交按分条序号覆盖，固定旧切面不被物化改变。 */
    @Test
    void productVectorCutMustOrderLocalAndCrossShardWritesOnSameKey() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            var oldRr = cut.currentCut();
            String leftKey = "vector:mixed:left";
            String rightKey = "vector:mixed:right";
            assertEquals(1, first.directCommitVectorLocal(-21,
                    Mutation.put(leftKey, new byte[]{47}), 1, 0));
            var localRc = cut.publishOnce();
            var reader = control.begin(IsolationLevel.READ_COMMITTED, 60_000);
            assertArrayEquals(new byte[]{47}, first.getVector(localRc, reader.txId(), leftKey, 1)
                    .orElseThrow().value());

            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            Mutation left = Mutation.put(leftKey, new byte[]{48});
            Mutation right = Mutation.put(rightKey, new byte[]{49});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(), left, 1, 1);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(), right, 2, 0);
            assertEquals(2, leftProof.vectorReservation().orElseThrow().reservedSequence());
            var activeCut = cut.publishOnce();
            assertEquals(100, activeCut.decisionEpoch());
            assertArrayEquals(new byte[]{47}, first.getVector(activeCut, reader.txId(), leftKey, 1)
                    .orElseThrow().value());
            KeyRange leftRange = new KeyRange(leftKey, leftKey + "~");
            assertArrayEquals(new byte[]{48}, first.scanVectorPage(localRc, writer.txId(),
                    leftRange, null, 128).values().getFirst().value());
            assertArrayEquals(new byte[]{47}, first.scanVectorPage(activeCut, reader.txId(),
                    leftRange, null, 128).values().getFirst().value());
            assertTrue(second.getVector(activeCut, reader.txId(), rightKey, 2).isEmpty());
            assertEquals(ErrorCode.LOCK_CONFLICT, assertThrows(MvccException.class, () ->
                    first.directCommitVectorLocal(-22,
                            Mutation.put(leftKey, new byte[]{50}), 1, 1)).errorCode());

            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "mixed-order-owner", "mixed-order-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            var crossCut = cut.publishOnce();
            assertArrayEquals(new byte[]{48}, first.getVector(crossCut, reader.txId(), leftKey, 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{48}, first.scanVectorPage(crossCut, reader.txId(),
                    leftRange, null, 128).values().getFirst().value());
            assertArrayEquals(new byte[]{49}, second.getVector(crossCut, reader.txId(), rightKey, 2)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{47}, first.getVector(localRc, reader.txId(), leftKey, 1)
                    .orElseThrow().value());
            first.commitVectorIntentWithProof(writer.txId(), 101, left);
            assertArrayEquals(new byte[]{48}, first.getVector(crossCut, reader.txId(), leftKey, 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{48}, first.scanVectorPage(crossCut, reader.txId(),
                    leftRange, null, 128).values().getFirst().value());
            second.commitVectorIntentWithProof(writer.txId(), 101, right);
            assertEquals(3, first.directCommitVectorLocal(-23,
                    Mutation.put(leftKey, new byte[]{50}), 1, 2));
            var newestCut = cut.publishOnce();
            assertArrayEquals(new byte[]{50}, first.getVector(newestCut, reader.txId(), leftKey, 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{50}, first.scanVectorPage(newestCut, reader.txId(),
                    leftRange, null, 128).values().getFirst().value());
            assertArrayEquals(new byte[]{47}, first.scanVectorPage(localRc, reader.txId(),
                    leftRange, null, 128).values().getFirst().value());
            assertArrayEquals(new byte[]{48}, first.getVector(crossCut, reader.txId(), leftKey, 1)
                    .orElseThrow().value());
            assertTrue(first.getVector(oldRr, oldReader.txId(), leftKey, 1).isEmpty());
            control.releaseSnapshot(oldReader.txId());
            control.releaseSnapshot(reader.txId());
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 真实向量点读被一条进程租约保护，旧 RR 与新 RC 在 GC 下界推进时仍分离。 */
    @Test
    void productVectorReaderMustHoldGcFloorUntilOldRrCloses() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldCut = cut.currentCut();
            AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published =
                    new AtomicReference<>(oldCut);
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            try (var protection = new MysqlVectorReaderProtectionRuntime(
                    leases, published::get, "product-reader", 5_000, 1_000, 100)) {
                protection.start();
                var reader = new MysqlVectorProtectedReader(Map.of("s1", first, "s2", second),
                        protection);
                String key = "vector:protected:local";
                try (var oldRr = reader.beginRepeatableRead(7_001)) {
                    assertTrue(oldRr.get("s1", key, 1).isEmpty());
                    assertEquals(1, first.directCommitVectorLocal(-701,
                            Mutation.put(key, new byte[]{77}), 1, 0));
                    var newCut = cut.publishOnce();
                    published.set(newCut);
                    assertTrue(oldRr.get("s1", key, 1).isEmpty());
                    assertArrayEquals(new byte[]{77}, reader.getReadCommitted(7_002,
                            "s1", key, 1).orElseThrow().value());
                    assertEquals(oldCut, leases.advanceSafeFloor(newCut));
                }
                protection.refresh();
                assertEquals(published.get(), leases.advanceSafeFloor(published.get()));
            }
        }
    }

    /** 受保护范围读跨页保留 RR 切面，RC 新语句可见后续提交和墓碑。 */
    @Test
    void productVectorScanMustKeepOneCutAcrossPagesAndRejectScalarOrphans() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published =
                    new AtomicReference<>(cut.currentCut());
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            try (var protection = new MysqlVectorReaderProtectionRuntime(
                    leases, published::get, "vector-scan-reader", 5_000, 1_000, 100)) {
                protection.start();
                var reader = new MysqlVectorProtectedReader(Map.of("s1", first, "s2", second),
                        protection);
                KeyRange range = new KeyRange("vector:scan:a", "vector:scan:z");
                String a = "vector:scan:a";
                String b = "vector:scan:b";
                String c = "vector:scan:c";
                String d = "vector:scan:d";
                assertEquals(1, first.directCommitVectorLocal(-7_101,
                        Mutation.put(a, new byte[]{1}), 1, 0));
                assertEquals(2, first.directCommitVectorLocal(-7_102,
                        Mutation.delete(b), 1, 0));
                assertEquals(1, first.directCommitVectorLocal(-7_103,
                        Mutation.put(c, new byte[]{3}), 2, 0));
                published.set(cut.publishOnce());
                try (var rr = reader.beginRepeatableRead(7_101)) {
                    ScanPage firstPage = rr.scanPage("s1", range, null, 1);
                    assertEquals(List.of(a), firstPage.values().stream().map(v -> v.key()).toList());
                    assertEquals(a, firstPage.nextKey());
                    ScanPage deletedPage = rr.scanPage("s1", range, firstPage.nextKey(), 1);
                    assertTrue(deletedPage.values().isEmpty());
                    assertEquals(b, deletedPage.nextKey());
                    assertEquals(List.of(a, c), rr.scan("s1", range).stream()
                            .map(v -> v.key()).toList());
                    assertEquals(3, first.directCommitVectorLocal(-7_104,
                            Mutation.put(a, new byte[]{4}), 1, 2));
                    assertEquals(2, first.directCommitVectorLocal(-7_105,
                            Mutation.put(d, new byte[]{5}), 2, 1));
                    published.set(cut.publishOnce());
                    assertEquals(List.of(a, c), rr.scan("s1", range).stream()
                            .map(v -> v.key()).toList());
                    assertArrayEquals(new byte[]{1}, rr.scan("s1", range).getFirst().value());
                    assertEquals(List.of(a, c, d), reader.scanReadCommitted(7_102,
                            "s1", range).stream().map(v -> v.key()).toList());
                    assertArrayEquals(new byte[]{4}, reader.scanReadCommitted(7_103,
                            "s1", range).getFirst().value());
                }
                try (Connection connection = firstData.getConnection();
                     PreparedStatement insert = connection.prepareStatement("""
                             insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                             values (?, ?, ?, 0, ?)
                             """)) {
                    insert.setBytes(1, "vector:scan:e".getBytes(StandardCharsets.UTF_8));
                    insert.setLong(2, 99_001);
                    insert.setLong(3, 99_001);
                    insert.setBytes(4, new byte[]{9});
                    insert.executeUpdate();
                }
                assertEquals(ErrorCode.ILLEGAL_STATE,
                        assertThrows(MvccException.class, () -> reader.scanReadCommitted(
                                7_104, "s1", range)).errorCode());
            }
        }
    }

    /** 128 键页通过固定次数的分片查询返回，跨页不重复或漏掉末尾键。 */
    @Test
    void productVectorScanMustBatchOneHundredTwentyEightKeys() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            for (int index = 0; index < 130; index++) {
                String key = "vector:batchscan:%03d".formatted(index);
                assertEquals(index + 1, first.directCommitVectorLocal(-8_000 - index,
                        Mutation.put(key, new byte[]{(byte) index}), 1, 0));
            }
            var published = cut.publishOnce();
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            AtomicInteger queries = new AtomicInteger();
            var counted = new MysqlStorageAdapter("s1", countingDataSource(firstData, queries), metadata);
            counted.initializeVectorProtocol(4);
            try (var protection = new MysqlVectorReaderProtectionRuntime(
                    leases, () -> published, "batch-scan-reader", 5_000, 1_000, 100)) {
                protection.start();
                var reader = new MysqlVectorProtectedReader(Map.of("s1", counted, "s2", second),
                        protection);
                KeyRange range = new KeyRange("vector:batchscan:", "vector:batchscan:~");
                queries.set(0);
                ScanPage firstPage = reader.scanPageReadCommitted(8_001, "s1", range, null, 128);
                assertEquals(128, firstPage.values().size());
                assertEquals("vector:batchscan:127", firstPage.nextKey());
                assertTrue(queries.get() <= 8, "one vector page must use bounded shard queries");
                ScanPage lastPage = reader.scanPageReadCommitted(8_002, "s1", range,
                        firstPage.nextKey(), 128);
                assertEquals(List.of("vector:batchscan:128", "vector:batchscan:129"),
                        lastPage.values().stream().map(v -> v.key()).toList());
                assertEquals(null, lastPage.nextKey());
            }
        }
    }

    /** 只统计分片 JDBC 预编译查询，初始化和控制库不计入范围页成本。 */
    private static DataSource countingDataSource(DataSource delegate, AtomicInteger queries) {
        return (DataSource) Proxy.newProxyInstance(
                MysqlLocalFirstCrossShardTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (!(result instanceof Connection connection)) {
                            return result;
                        }
                        return Proxy.newProxyInstance(MysqlLocalFirstCrossShardTest.class.getClassLoader(),
                                new Class<?>[]{Connection.class}, (connectionProxy, connectionMethod, parameters) -> {
                                    if ("prepareStatement".equals(connectionMethod.getName())) {
                                        queries.incrementAndGet();
                                    }
                                    try {
                                        return connectionMethod.invoke(connection, parameters);
                                    } catch (InvocationTargetException ex) {
                                        throw ex.getCause();
                                    }
                                });
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /** 标量 GC 不能误删向量版本链，直至分条安全点与对应清理协议完成。 */
    @Test
    void scalarGcMustRejectInitializedVectorVersions() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldRr = cut.currentCut();
            String key = "vector:gc:fence";
            assertEquals(1, first.directCommitVectorLocal(-801,
                    Mutation.put(key, new byte[]{81}), 1, 0));
            var newRc = cut.publishOnce();
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> first.gc(101, 10)).errorCode());
            var restarted = new MysqlStorageAdapter("s1", firstData, metadata);
            restarted.initialize();
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> restarted.gc(101, 10)).errorCode());
            assertTrue(first.getVector(oldRr, 8_001, key, 1).isEmpty());
            assertArrayEquals(new byte[]{81}, first.getVector(newRc, 8_001, key, 1)
                    .orElseThrow().value());
        }
    }

    /** 物理 GC 只删安全前驱之前的本地版本，旧 RR 释放前保留其值。 */
    @Test
    void vectorLocalGcMustKeepOldReaderAndLatestVersion() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published =
                    new AtomicReference<>(cut.currentCut());
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            var collector = new MysqlVectorLocalGarbageCollector(firstData, "s1", 4, leases);
            collector.initializeProduct();
            try (var protection = new MysqlVectorReaderProtectionRuntime(
                    leases, published::get, "local-gc-reader", 5_000, 1_000, 100)) {
                protection.start();
                var reader = new MysqlVectorProtectedReader(Map.of("s1", first, "s2", second),
                        protection);
                String key = "vector:local:gc:history";
                assertEquals(1, first.directCommitVectorLocal(-901,
                        Mutation.put(key, new byte[]{1}), 1, 0));
                published.set(cut.publishOnce());
                try (var oldRr = reader.beginRepeatableRead(9_001)) {
                    assertEquals(2, first.directCommitVectorLocal(-902,
                            Mutation.put(key, new byte[]{2}), 1, 1));
                    assertEquals(3, first.directCommitVectorLocal(-903,
                            Mutation.put(key, new byte[]{3}), 1, 2));
                    published.set(cut.publishOnce());
                    protection.refresh();
                    assertEquals(1, leases.advanceSafeFloor(published.get()).sequence("s1", 1));
                    assertEquals(0, collector.collect(100));
                    assertEquals(3, countVectorLocalRows(firstData, key));
                    assertArrayEquals(new byte[]{1}, oldRr.get("s1", key, 1)
                            .orElseThrow().value());
                    assertArrayEquals(new byte[]{1}, oldRr.scan("s1",
                            new KeyRange(key, key + "~")).getFirst().value());
                    assertArrayEquals(new byte[]{3}, reader.getReadCommitted(9_002,
                            "s1", key, 1).orElseThrow().value());
                    assertArrayEquals(new byte[]{3}, reader.scanReadCommitted(9_002,
                            "s1", new KeyRange(key, key + "~")).getFirst().value());
                }
                protection.refresh();
                assertEquals(3, leases.advanceSafeFloor(published.get()).sequence("s1", 1));
                assertEquals(2, collector.collect(100));
                assertEquals(1, countVectorLocalRows(firstData, key));
                assertArrayEquals(new byte[]{3}, reader.getReadCommitted(9_003,
                        "s1", key, 1).orElseThrow().value());
                assertArrayEquals(new byte[]{3}, reader.scanReadCommitted(9_003,
                        "s1", new KeyRange(key, key + "~")).getFirst().value());
            }
        }
    }

    /** 墓碑作为安全切面的可见前驱保留，旧值可以被本地 GC 删除。 */
    @Test
    void vectorLocalGcMustRetainLatestTombstone() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            String key = "vector:local:gc:tombstone";
            assertEquals(1, first.directCommitVectorLocal(-911,
                    Mutation.put(key, new byte[]{11}), 1, 0));
            assertEquals(2, first.directCommitVectorLocal(-912,
                    Mutation.delete(key), 1, 1));
            var latest = cut.publishOnce();
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            var collector = new MysqlVectorLocalGarbageCollector(firstData, "s1", 4, leases);
            collector.initializeProduct();
            assertEquals(2, leases.advanceSafeFloor(latest).sequence("s1", 1));
            assertEquals(1, collector.collect(100));
            assertEquals(1, countVectorLocalRows(firstData, key));
            assertTrue(first.getVector(latest, 9_011, key, 1).orElseThrow().deleted());
        }
    }

    /** 跨片决议未进安全切面时保留本地前驱，决议进入后仅删除被遮盖本地行。 */
    @Test
    void vectorLocalGcMustRespectCrossDecisionBeforePruningLocalRows() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            String leftKey = "vector:gc:cross:left";
            String rightKey = "vector:gc:cross:right";
            assertEquals(1, first.directCommitVectorLocal(-921,
                    Mutation.put(leftKey, new byte[]{21}), 1, 0));
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerVectorIntentTransaction(writer.txId(), writer.startTs(),
                    writer.deadlineMillis(), IsolationLevel.REPEATABLE_READ, participants);
            Mutation leftWrite = Mutation.put(leftKey, new byte[]{22});
            Mutation rightWrite = Mutation.put(rightKey, new byte[]{23});
            var leftProof = first.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                    leftWrite, 1, 1);
            var rightProof = second.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                    rightWrite, 2, 0);
            var beforeDecision = cut.publishOnce();
            assertEquals(100, beforeDecision.decisionEpoch());
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            var collector = new MysqlVectorLocalGarbageCollector(firstData, "s1", 4, leases);
            collector.initializeProduct();
            assertEquals(2, leases.advanceSafeFloor(beforeDecision).sequence("s1", 1));
            assertEquals(0, collector.collect(100));
            assertEquals(1, countVectorLocalRows(firstData, leftKey));
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "gc-cross-owner", "gc-cross-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeVectorIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants,
                            List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            first.commitVectorIntentWithProof(writer.txId(), 101, leftWrite);
            second.commitVectorIntentWithProof(writer.txId(), 101, rightWrite);
            assertEquals(0, collector.collect(100));
            assertEquals(1, countVectorLocalRows(firstData, leftKey));
            assertArrayEquals(new byte[]{21}, first.getVector(beforeDecision,
                    9_021, leftKey, 1).orElseThrow().value());
            var crossCut = cut.publishOnce();
            assertEquals(101, crossCut.decisionEpoch());
            assertEquals(2, leases.advanceSafeFloor(crossCut).sequence("s1", 1));
            assertEquals(1, collector.collect(100));
            assertEquals(0, countVectorLocalRows(firstData, leftKey));
            assertEquals(1, countCrossMappedRows(firstData, leftKey));
            assertArrayEquals(new byte[]{22}, first.getVector(crossCut,
                    9_022, leftKey, 1).orElseThrow().value());
            assertEquals(3, first.directCommitVectorLocal(-922,
                    Mutation.put(leftKey, new byte[]{24}), 1, 2));
            var latest = cut.publishOnce();
            assertEquals(3, leases.advanceSafeFloor(latest).sequence("s1", 1));
            assertEquals(0, collector.collect(100));
            assertEquals(1, countVectorLocalRows(firstData, leftKey));
            assertEquals(1, countCrossMappedRows(firstData, leftKey));
            assertArrayEquals(new byte[]{24}, first.getVector(latest,
                    9_023, leftKey, 1).orElseThrow().value());
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 小删除预算使用持久键游标遍历不同键，不能反复只清理第一条链。 */
    @Test
    void vectorLocalGcCursorMustReachLaterKeys() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            String a = "vector:gc:budget:a";
            String b = "vector:gc:budget:b";
            assertEquals(1, first.directCommitVectorLocal(-931,
                    Mutation.put(a, new byte[]{31}), 1, 0));
            assertEquals(2, first.directCommitVectorLocal(-932,
                    Mutation.put(b, new byte[]{32}), 1, 0));
            assertEquals(3, first.directCommitVectorLocal(-933,
                    Mutation.put(a, new byte[]{33}), 1, 1));
            assertEquals(4, first.directCommitVectorLocal(-934,
                    Mutation.put(b, new byte[]{34}), 1, 2));
            var latest = cut.publishOnce();
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            var collector = new MysqlVectorLocalGarbageCollector(firstData, "s1", 4, leases);
            collector.initializeProduct();
            assertEquals(4, leases.advanceSafeFloor(latest).sequence("s1", 1));
            assertEquals(1, collector.collect(1));
            var restartedGc = new MysqlVectorLocalGarbageCollector(firstData, "s1", 4, leases);
            restartedGc.initializeProduct();
            assertEquals(1, restartedGc.collect(1));
            assertEquals(1, countVectorLocalRows(firstData, a));
            assertEquals(1, countVectorLocalRows(firstData, b));
            assertArrayEquals(new byte[]{33}, first.getVector(latest, 9_031, a, 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{34}, first.getVector(latest, 9_032, b, 1)
                    .orElseThrow().value());
        }
    }

    /** 跨片映射缺少真实版本时 GC 失败关闭，不能把伪前驱用于删除本地旧值。 */
    @Test
    void vectorLocalGcMustRejectCrossMappingWithoutVersion() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        String key = "vector:gc:broken-cross";
        assertEquals(1, first.directCommitVectorLocal(-941,
                Mutation.put(key, new byte[]{41}), 1, 0));
        try (Connection connection = firstData.getConnection();
             PreparedStatement insert = connection.prepareStatement("""
                     insert into mvcc_vector_committed_v1
                       (user_key, reserved_seq, tx_id, stripe_id, start_seq,
                        commit_ts, deleted, payload)
                     values (?, 2, 9401, 1, 1, 101, 0, ?)
                     """)) {
            insert.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            insert.setBytes(2, new byte[]{42});
            insert.executeUpdate();
        }
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var leases = new MysqlVectorSnapshotLeaseStore(controlData, List.of("s1", "s2"), 4);
            leases.initializeProduct();
            var collector = new MysqlVectorLocalGarbageCollector(firstData, "s1", 4, leases);
            collector.initializeProduct();
            leases.advanceSafeFloor(cut.currentCut());
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> collector.collect(100)).errorCode());
            assertEquals(1, countVectorLocalRows(firstData, key));
            assertEquals(1, countCrossMappedRows(firstData, key));
        }
    }

    /** 持租约发布、进程读保护与独立后台池共同维护本地安全 GC。 */
    @Test
    void productVectorGcSchedulerMustRespectOldRrAndReclaimAfterRelease() throws Exception {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leaderLease = MysqlVectorCutLeaderLease.forProduct(controlData, routes, 4);
        leaderLease.initializeProduct();
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_1");
             HikariDataSource backgroundSecond = pool("shard_2");
             var leader = MysqlVectorCutLeaderRuntime.forProduct(controlData, routes, 4,
                     control::currentPublished, leaderLease, "gc-scheduler-leader", 5_000, 200)) {
            assertTrue(leader.start());
            var frontLeases = new MysqlVectorSnapshotLeaseStore(controlData,
                    List.of("s1", "s2"), 4);
            var backgroundLeases = new MysqlVectorSnapshotLeaseStore(backgroundControl,
                    List.of("s1", "s2"), 4);
            frontLeases.initializeProduct();
            backgroundLeases.initializeProduct();
            var firstGc = new MysqlVectorLocalGarbageCollector(backgroundFirst, "s1", 4,
                    backgroundLeases);
            var secondGc = new MysqlVectorLocalGarbageCollector(backgroundSecond, "s2", 4,
                    backgroundLeases);
            firstGc.initializeProduct();
            secondGc.initializeProduct();
            var firstCrossGc = new MysqlVectorCrossGarbageCollector(first, backgroundFirst,
                    "s1", 4, backgroundLeases, control);
            var secondCrossGc = new MysqlVectorCrossGarbageCollector(second, backgroundSecond,
                    "s2", 4, backgroundLeases, control);
            firstCrossGc.initializeProduct();
            secondCrossGc.initializeProduct();
            var wrongShard = new MysqlVectorLocalGarbageCollector(backgroundSecond, "s1", 4,
                    backgroundLeases);
            assertThrows(IllegalArgumentException.class, () ->
                    new MysqlVectorLocalGcScheduler(leader, backgroundLeases,
                            Map.of("s1", wrongShard, "s2", secondGc), 100, 50));
            var wrongControl = new MysqlVectorSnapshotLeaseStore(backgroundFirst,
                    List.of("s1", "s2"), 4);
            assertThrows(IllegalArgumentException.class, () ->
                    new MysqlVectorLocalGcScheduler(leader, wrongControl,
                            Map.of("s1", firstGc, "s2", secondGc), 100, 50));
            AtomicReference<DataSource> currentControl = new AtomicReference<>(backgroundControl);
            var redirectedControl = new MysqlVectorSnapshotLeaseStore(
                    switchableDataSource(currentControl), List.of("s1", "s2"), 4);
            redirectedControl.initializeProduct();
            currentControl.set(backgroundFirst);
            assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, redirectedControl::currentSafeFloor).errorCode());
            AtomicReference<DataSource> currentFirst = new AtomicReference<>(backgroundFirst);
            var redirectedFirst = new MysqlVectorLocalGarbageCollector(
                    switchableDataSource(currentFirst), "s1", 4, backgroundLeases);
            redirectedFirst.initializeProduct();
            try (var redirectedScheduler = new MysqlVectorLocalGcScheduler(leader,
                    backgroundLeases, Map.of("s1", redirectedFirst, "s2", secondGc), 100, 50)) {
                currentFirst.set(backgroundSecond);
                assertThrows(IllegalArgumentException.class, redirectedScheduler::runOnce);
                assertEquals(ErrorCode.ILLEGAL_STATE,
                        assertThrows(MvccException.class, () -> redirectedFirst.collect(100)).errorCode());
            }
            try (var protection = new MysqlVectorReaderProtectionRuntime(frontLeases,
                    () -> leader.currentView().snapshot(), "gc-scheduler-reader", 5_000, 500, 100);
                 var scheduler = new MysqlVectorLocalGcScheduler(leader, backgroundLeases,
                         Map.of("s1", firstGc, "s2", secondGc),
                         Map.of("s1", firstCrossGc, "s2", secondCrossGc), 100, 10, 50)) {
                protection.start();
                var reader = new MysqlVectorProtectedReader(Map.of("s1", first, "s2", second),
                        protection);
                String key = "vector:gc:scheduler";
                assertEquals(1, first.directCommitVectorLocal(-951,
                        Mutation.put(key, new byte[]{51}), 1, 0));
                leader.awaitPublished("s1", 1, 1, 100, 5_000);
                try (var oldRr = reader.beginRepeatableRead(9_051)) {
                    assertEquals(2, first.directCommitVectorLocal(-952,
                            Mutation.put(key, new byte[]{52}), 1, 1));
                    assertEquals(3, first.directCommitVectorLocal(-953,
                            Mutation.put(key, new byte[]{53}), 1, 2));
                    leader.awaitPublished("s1", 1, 3, 100, 5_000);
                    protection.refresh();
                    assertEquals(0, scheduler.runOnce());
                    assertEquals(3, countVectorLocalRows(firstData, key));
                    assertArrayEquals(new byte[]{51}, oldRr.get("s1", key, 1)
                            .orElseThrow().value());
                }
                protection.refresh();
                assertEquals(2, scheduler.runOnce());
                assertEquals(0, scheduler.crossReclaimed());
                assertEquals(1, countVectorLocalRows(firstData, key));
                assertArrayEquals(new byte[]{53}, reader.getReadCommitted(9_052,
                        "s1", key, 1).orElseThrow().value());
                scheduler.start();
                long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (scheduler.stats().runCount() == 0 && System.nanoTime() < waitUntil) {
                    Thread.sleep(10);
                }
                assertTrue(scheduler.stats().runCount() > 0);
                assertEquals(0, scheduler.stats().failureCount());
                leader.endpointUnavailable(new IllegalStateException("cut endpoint lost"));
                long failureWait = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (scheduler.stats().failureCount() == 0 && System.nanoTime() < failureWait) {
                    Thread.sleep(10);
                }
                assertTrue(scheduler.stats().failureCount() > 0);
                assertTrue(scheduler.lastFailure() != null);
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> reader.getReadCommitted(
                                9_053, "s1", key, 1)).errorCode());
            }
        }
    }

    /** 物理行数只统计目标键的正式向量本地版本。 */
    private static long countVectorLocalRows(HikariDataSource dataSource, String key) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select count(*) from mvcc_vector_local_data_v1 where user_key = ?
                     """)) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            try (ResultSet row = query.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        }
    }

    /** 仅接受测试固定表名核对映射、真实版本与紧凑回执的行数守恒。 */
    private static long countRows(HikariDataSource dataSource, String table) throws Exception {
        if (!Set.of("mvcc_vector_retired_v1", "mvcc_vector_committed_v1", "mvcc_data").contains(table)) {
            throw new IllegalArgumentException("unsupported test row count table");
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("select count(*) from " + table);
             ResultSet row = query.executeQuery()) {
            assertTrue(row.next());
            return row.getLong(1);
        }
    }

    /** 跨片映射计数用于确认本地 GC 没有触碰恢复所需证明。 */
    private static long countCrossMappedRows(HikariDataSource dataSource, String key) throws Exception {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select count(*) from mvcc_vector_committed_v1 where user_key = ?
                     """)) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            try (ResultSet row = query.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        }
    }

    /** 模拟后台池重连后被错路由到另一分片，不修改真实连接池配置。 */
    private static DataSource switchableDataSource(AtomicReference<DataSource> current) {
        return (DataSource) Proxy.newProxyInstance(
                MysqlLocalFirstCrossShardTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    try {
                        return method.invoke(current.get(), arguments);
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /** 存储过程等待同键锁后必须看见前驱刚提交的本地版本。 */
    @Test
    void productLocalProcedureMustSeeVersionCommittedDuringKeyLockWait() throws Exception {
        first.initializeVectorProtocol(4);
        byte[] key = "vector:wait".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (Connection blocker = firstData.getConnection();
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try (java.sql.PreparedStatement guard = blocker.prepareStatement("""
                    insert into mvcc_key_lock(user_key) values (?)
                    """); java.sql.PreparedStatement state = blocker.prepareStatement("""
                    update mvcc_vector_stripe_state_v1 set committed_seq = 1 where stripe_id = 1
                    """); java.sql.PreparedStatement version = blocker.prepareStatement("""
                    insert into mvcc_vector_local_data_v1
                      (user_key, local_seq, tx_id, stripe_id, start_seq, deleted, payload)
                    values (?, 1, -101, 1, 0, 0, ?)
                    """)) {
                guard.setBytes(1, key);
                guard.executeUpdate();
                state.executeUpdate();
                version.setBytes(1, key);
                version.setBytes(2, new byte[]{51});
                version.executeUpdate();
            }
            CountDownLatch started = new CountDownLatch(1);
            var waiting = workers.submit(() -> {
                started.countDown();
                return first.directCommitVectorLocal(-102,
                        Mutation.put("vector:wait", new byte[]{52}), 1, 0);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
            blocker.commit();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> waiting.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.WRITE_CONFLICT,
                    ((MvccException) failure.getCause()).errorCode());
            assertEquals(1, first.vectorSequence(1));
        }
    }

    /** 向量预写例程等待同键锁后也必须看见前驱本地版本并整笔回滚。 */
    @Test
    void productVectorPrewriteProcedureMustSeeLocalCommitDuringKeyLockWait() throws Exception {
        first.initializeVectorProtocol(4);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerVectorIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        byte[] key = "vector:prewrite-wait".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (Connection blocker = firstData.getConnection();
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try (java.sql.PreparedStatement guard = blocker.prepareStatement("""
                    insert into mvcc_key_lock(user_key) values (?)
                    """); java.sql.PreparedStatement state = blocker.prepareStatement("""
                    update mvcc_vector_stripe_state_v1 set committed_seq = 1 where stripe_id = 1
                    """); java.sql.PreparedStatement version = blocker.prepareStatement("""
                    insert into mvcc_vector_local_data_v1
                      (user_key, local_seq, tx_id, stripe_id, start_seq, deleted, payload)
                    values (?, 1, -111, 1, 0, 0, ?)
                    """)) {
                guard.setBytes(1, key);
                guard.executeUpdate();
                state.executeUpdate();
                version.setBytes(1, key);
                version.setBytes(2, new byte[]{61});
                version.executeUpdate();
            }
            CountDownLatch started = new CountDownLatch(1);
            var waiting = workers.submit(() -> {
                started.countDown();
                return first.prewriteWithVectorProof(writer.txId(), writer.startTs(),
                        Mutation.put("vector:prewrite-wait", new byte[]{62}), 1, 0);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
            blocker.commit();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> waiting.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.WRITE_CONFLICT,
                    ((MvccException) failure.getCause()).errorCode());
            assertTrue(first.participantRecord(writer.txId()).isEmpty());
            assertTrue(first.vectorReservation(writer.txId(), "vector:prewrite-wait").isEmpty());
            assertEquals(1, first.vectorSequence(1));
        }
        control.releaseSnapshot(writer.txId());
    }

    /** 多笔独立本地事务共用物理提交，失败整批回滚且响应未知可逐笔核对。 */
    @Test
    void productLocalVectorBatchMustCommitAtomicallyAndReplayByTransaction() {
        first.initializeVectorProtocol(4);
        second.initializeVectorProtocol(4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var cut = MysqlPublishedVectorCut.productDurable(controlData, routes, 4,
                control::currentPublished)) {
            cut.initializeProduct();
            var oldRr = cut.currentCut();
            var firstRequest = new MysqlStorageAdapter.LocalVectorCommitRequest(-201,
                    Mutation.put("vector:batch:a", new byte[]{53}), 1, 0);
            var secondRequest = new MysqlStorageAdapter.LocalVectorCommitRequest(-202,
                    Mutation.put("vector:batch:b", new byte[]{54}), 2, 0);
            assertEquals(List.of(1L, 1L), first.directCommitVectorLocalBatch(
                    List.of(firstRequest, secondRequest)));
            assertEquals(1, first.verifyLocalVectorCommit(firstRequest).orElseThrow());
            assertEquals(1, first.verifyLocalVectorCommit(secondRequest).orElseThrow());
            assertEquals(1, first.vectorSequence(1));
            assertEquals(1, first.vectorSequence(2));
            assertEquals(List.of(1L, 1L), first.directCommitVectorLocalBatch(
                    List.of(firstRequest, secondRequest)));
            assertEquals(1, first.vectorSequence(1));
            assertEquals(1, first.vectorSequence(2));
            var published = cut.publishOnce();
            assertArrayEquals(new byte[]{53}, first.getVector(published, 900, "vector:batch:a", 1)
                    .orElseThrow().value());
            assertArrayEquals(new byte[]{54}, first.getVector(published, 900, "vector:batch:b", 2)
                    .orElseThrow().value());
            assertTrue(first.getVector(oldRr, 900, "vector:batch:a", 1).isEmpty());
            assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class, () ->
                    first.verifyLocalVectorCommit(new MysqlStorageAdapter.LocalVectorCommitRequest(
                            -201, Mutation.put("vector:batch:a", new byte[]{99}), 1, 0))).errorCode());

            var conflicting = new MysqlStorageAdapter.LocalVectorCommitRequest(-203,
                    Mutation.put("vector:batch:a", new byte[]{55}), 1, 0);
            var independent = new MysqlStorageAdapter.LocalVectorCommitRequest(-204,
                    Mutation.put("vector:batch:c", new byte[]{56}), 3, 0);
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(MvccException.class, () ->
                    first.directCommitVectorLocalBatch(List.of(conflicting, independent))).errorCode());
            assertTrue(first.verifyLocalVectorCommit(independent).isEmpty());
            assertEquals(0, first.vectorSequence(3));
            assertEquals(1, first.directCommitVectorLocal(independent.txId(),
                    independent.mutation(), independent.stripe(), independent.startSequence()));
            var nullValue = new MysqlStorageAdapter.LocalVectorCommitRequest(-205,
                    Mutation.put("vector:batch:null", null), 0, 0);
            var tombstone = new MysqlStorageAdapter.LocalVectorCommitRequest(-206,
                    Mutation.delete("vector:batch:delete"), 2, 0);
            assertEquals(2, first.directCommitVectorLocalBatch(List.of(nullValue, tombstone)).size());
            assertTrue(first.verifyLocalVectorCommit(nullValue).isPresent());
            assertTrue(first.verifyLocalVectorCommit(tombstone).isPresent());
            var reverseZ = new MysqlStorageAdapter.LocalVectorCommitRequest(-208,
                    Mutation.put("vector:batch:z", new byte[]{57}), 0, 0);
            var reverseA = new MysqlStorageAdapter.LocalVectorCommitRequest(-209,
                    Mutation.put("vector:batch:a0", new byte[]{58}), 0, 0);
            List<Long> originalOrder = first.directCommitVectorLocalBatch(List.of(reverseZ, reverseA));
            assertTrue(originalOrder.get(0) > originalOrder.get(1));
            assertEquals(originalOrder.get(0), first.verifyLocalVectorCommit(reverseZ).orElseThrow());
            assertEquals(originalOrder.get(1), first.verifyLocalVectorCommit(reverseA).orElseThrow());
            var tooLong = new MysqlStorageAdapter.LocalVectorCommitRequest(-207,
                    Mutation.put("x".repeat(257), new byte[]{1}), 0, 0);
            assertThrows(IllegalArgumentException.class, () ->
                    first.directCommitVectorLocalBatch(List.of(tooLong)));
            assertThrows(IllegalArgumentException.class, () ->
                    first.directCommitVectorLocalBatch(List.of(firstRequest, firstRequest)));
        }
    }

    /** 独立请求经有界队列组批仍逐笔拿到持久序号，冲突批次能逐笔核对。 */
    @Test
    void productLocalVectorBatcherMustPreserveIndependentResults() throws Exception {
        first.initializeVectorProtocol(4);
        try (var batcher = new MysqlVectorLocalCommitBatcher(first, 2, 4, 2_000, 64)) {
            List<MysqlStorageAdapter.LocalVectorCommitRequest> requests = new java.util.ArrayList<>();
            List<CompletableFuture<Long>> futures = new java.util.ArrayList<>();
            for (int index = 0; index < 12; index++) {
                var request = new MysqlStorageAdapter.LocalVectorCommitRequest(-300 - index,
                        Mutation.put("vector:coalesce:" + index, new byte[]{(byte) index}),
                        index % 4, 0);
                requests.add(request);
                futures.add(batcher.submit(request, 5_000));
            }
            for (int index = 0; index < requests.size(); index++) {
                long result = futures.get(index).get(5, TimeUnit.SECONDS);
                assertEquals(result, first.verifyLocalVectorCommit(requests.get(index)).orElseThrow());
            }
            assertEquals(12, batcher.groupedCount());
            assertTrue(batcher.batchCount() <= 12);

            var conflict = new MysqlStorageAdapter.LocalVectorCommitRequest(-400,
                    Mutation.put(requests.getFirst().mutation().key(), new byte[]{44}), 0, 0);
            var independent = new MysqlStorageAdapter.LocalVectorCommitRequest(-401,
                    Mutation.put("vector:coalesce:fresh", new byte[]{45}), 1, 0);
            var failed = batcher.submit(conflict, 5_000);
            var succeeded = batcher.submit(independent, 5_000);
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> failed.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.WRITE_CONFLICT,
                    ((MvccException) error.getCause()).errorCode());
            assertEquals(succeeded.get(5, TimeUnit.SECONDS),
                    first.verifyLocalVectorCommit(independent).orElseThrow());
            assertTrue(first.verifyLocalVectorCommit(conflict).isEmpty());
            byte[] mutable = new byte[]{77};
            var submitted = new MysqlStorageAdapter.LocalVectorCommitRequest(-402,
                    Mutation.put("vector:coalesce:immutable", mutable), 0, 0);
            var frozen = batcher.submit(submitted, 5_000);
            mutable[0] = 78;
            assertEquals(frozen.get(5, TimeUnit.SECONDS), first.verifyLocalVectorCommit(
                    new MysqlStorageAdapter.LocalVectorCommitRequest(-402,
                            Mutation.put("vector:coalesce:immutable", new byte[]{77}), 0, 0)).orElseThrow());
        }
    }

    /** 客户端确认须等待持久向量发布，随后新 RC 看到本地和跨片写，旧 RR 保持旧切面。 */
    @Test
    void publishedVectorMustGateAcknowledgementAndPreserveGlobalSnapshots() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        try (var publication = new MysqlPublishedVectorCut(controlData, routes, 4,
                control::currentPublished);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8)) {
            publication.initializeIsolated();
            var oldRr = publication.currentCut();
            assertEquals(100, oldRr.decisionEpoch());
            assertEquals(0, oldRr.sequence("s1", 1));

            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:local", new byte[]{51}, 1, 0));
            assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, assertThrows(MvccException.class, () ->
                    publication.awaitLocallyPublished("s1", 1, 1, 100, 5)).errorCode());
            assertTrue(reader.readMixed(oldRr, "s1", "cut:local", 1,
                    this::committedEpochFromCenter).isEmpty());
            publication.publishOnce();
            publication.awaitLocallyPublished("s1", 1, 1, 100, 100);
            var localRc = publication.currentCut();
            assertArrayEquals(new byte[]{51}, mixedValue(reader, localRc, "s1", "cut:local", 1));
            assertTrue(reader.readMixed(oldRr, "s1", "cut:local", 1,
                    this::committedEpochFromCenter).isEmpty());

            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            List<Mutation> left = List.of(Mutation.put("cut:cross:left", new byte[]{61}));
            List<Mutation> right = List.of(Mutation.put("cut:cross:right", new byte[]{62}));
            var leftProof = first.prewriteWithProof(writer.txId(), writer.startTs(), left);
            var rightProof = second.prewriteWithProof(writer.txId(), writer.startTs(), right);
            assertEquals(2, MysqlStripedCrossVersionPrototype.prewrite(
                    firstData, "cut:cross:left", new byte[]{61}, 1, writer.txId(), 1));
            assertEquals(1, MysqlStripedCrossVersionPrototype.prewrite(
                    secondData, "cut:cross:right", new byte[]{62}, 2, writer.txId(), 0));
            var beforeDecision = publication.publishOnce();
            assertEquals(100, beforeDecision.decisionEpoch());
            assertTrue(reader.readMixed(beforeDecision, "s1", "cut:cross:left", 1,
                    this::committedEpochFromCenter).isEmpty());

            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "cut-owner", "cut-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, assertThrows(MvccException.class, () ->
                    publication.awaitLocallyPublished("s1", 1, 2, 101, 5)).errorCode());
            var committedCut = publication.publishOnce();
            publication.awaitLocallyPublished("s1", 1, 2, 101, 100);
            publication.awaitLocallyPublished("s2", 2, 1, 101, 100);
            assertEquals(committedCut, publication.currentCut());
            assertArrayEquals(new byte[]{61}, mixedValue(reader, committedCut,
                    "s1", "cut:cross:left", 1));
            assertArrayEquals(new byte[]{62}, mixedValue(reader, committedCut,
                    "s2", "cut:cross:right", 2));
            assertTrue(reader.readMixed(localRc, "s1", "cut:cross:left", 1,
                    this::committedEpochFromCenter).isEmpty());
            MysqlStripedCrossVersionPrototype.materialize(firstData, "cut:cross:left",
                    writer.txId(), 101, this::committedEpochFromCenter);
            assertArrayEquals(new byte[]{61}, mixedValue(reader, committedCut,
                    "s1", "cut:cross:left", 1));
            assertArrayEquals(new byte[]{62}, mixedValue(reader, committedCut,
                    "s2", "cut:cross:right", 2));
            MysqlStripedCrossVersionPrototype.materialize(secondData, "cut:cross:right",
                    writer.txId(), 101, this::committedEpochFromCenter);
            try (var restarted = MysqlPublishedVectorCut.experimentalVolatile(
                    controlData, routes, 4, control::currentPublished)) {
                restarted.initializeVolatile();
                var recovered = restarted.currentCut();
                assertTrue(recovered.decisionEpoch() >= committedCut.decisionEpoch());
                assertTrue(recovered.sequence("s1", 1) >= committedCut.sequence("s1", 1));
                assertTrue(recovered.sequence("s2", 2) >= committedCut.sequence("s2", 2));
                assertArrayEquals(new byte[]{61}, mixedValue(reader, recovered,
                        "s1", "cut:cross:left", 1));
                assertArrayEquals(new byte[]{62}, mixedValue(reader, recovered,
                        "s2", "cut:cross:right", 2));
            }
            first.commit(writer.txId(), 101, left, true);
            second.commit(writer.txId(), 101, right, true);
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 非持久单领导者实验在重启后从分片状态重建不低于已确认的切面。 */
    @Test
    void volatileVectorExperimentMustReconstructAcknowledgedLocalCommit() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        MysqlStripedSnapshotOracle.Snapshot acknowledged;
        try (var firstLeader = MysqlPublishedVectorCut.experimentalVolatile(
                controlData, routes, 4, control::currentPublished)) {
            firstLeader.initializeVolatile();
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:restart", new byte[]{71}, 1, 0));
            firstLeader.publishOnce();
            firstLeader.awaitLocallyPublished("s1", 1, 1, 100, 100);
            acknowledged = firstLeader.currentCut();
        }
        try (var restartedLeader = MysqlPublishedVectorCut.experimentalVolatile(
                controlData, routes, 4, control::currentPublished);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8)) {
            restartedLeader.initializeVolatile();
            var reconstructed = restartedLeader.currentCut();
            assertTrue(reconstructed.sequence("s1", 1) >= acknowledged.sequence("s1", 1));
            assertTrue(reconstructed.decisionEpoch() >= acknowledged.decisionEpoch());
            assertArrayEquals(new byte[]{71}, mixedValue(reader, reconstructed,
                    "s1", "cut:restart", 1));
        }
    }

    /** 新领导者只能在数据库租约到期后接任，旧发布者须拒绝读、发布和 ACK。 */
    @Test
    void leasedVectorPublisherMustFenceExpiredOwnerBeforeTakeover() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        var changedRoutes = new MysqlVectorCutLeaderLease(controlData,
                Map.<String, DataSource>of("s1", firstData, "s3", secondData), 4);
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE, assertThrows(MvccException.class, () ->
                changedRoutes.acquire("wrong-layout", 300)).errorCode());
        var firstGuard = leases.acquire("leader-one", 300).orElseThrow();
        assertThrows(IllegalArgumentException.class, () ->
                MysqlPublishedVectorCut.leasedVolatile(controlData,
                        Map.<String, DataSource>of("s1", firstData, "s3", secondData),
                        4, control::currentPublished, firstGuard));
        assertTrue(leases.acquire("leader-two", 300).isEmpty());
        MysqlStripedSnapshotOracle.Snapshot acknowledged;
        try (var oldLeader = MysqlPublishedVectorCut.leasedVolatile(
                controlData, routes, 4, control::currentPublished, firstGuard)) {
            oldLeader.initializeVolatile();
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:leased", new byte[]{81}, 1, 0));
            oldLeader.publishOnce();
            oldLeader.awaitLocallyPublished("s1", 1, 1, 100, 100);
            acknowledged = oldLeader.currentCut();

            Thread.sleep(360);
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, oldLeader::currentCut).errorCode());
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, oldLeader::publishOnce).errorCode());
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () ->
                            oldLeader.awaitLocallyPublished("s1", 1, 1, 100, 100)).errorCode());

            var nextGuard = leases.acquire("leader-two", 300).orElseThrow();
            assertEquals(firstGuard.generation() + 1, nextGuard.generation());
            try (var newLeader = MysqlPublishedVectorCut.leasedVolatile(
                    controlData, routes, 4, control::currentPublished, nextGuard);
                 var reader = new MysqlStripedSnapshotOracle(routes, 4, 8)) {
                newLeader.initializeVolatile();
                var recovered = newLeader.currentCut();
                assertTrue(recovered.sequence("s1", 1) >= acknowledged.sequence("s1", 1));
                assertArrayEquals(new byte[]{81}, mixedValue(reader, recovered,
                        "s1", "cut:leased", 1));
            }
        }
    }

    /** 后台续租维持唯一服务者，关闭后新实例从持久状态重建并接手 ACK。 */
    @Test
    void vectorLeaderRuntimeMustRenewAndRecoverAfterHandoff() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        try (var firstRuntime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                control::currentPublished, leases, "runtime-one", 1_000, 200);
             var nextRuntime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                     control::currentPublished, leases, "runtime-two", 1_000, 200);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8)) {
            assertTrue(firstRuntime.start());
            assertFalse(nextRuntime.start());
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:runtime", new byte[]{91}, 1, 0));
            firstRuntime.awaitPublished("s1", 1, 1, 100, 5_000);
            var acknowledged = firstRuntime.currentView();
            assertArrayEquals(new byte[]{91}, mixedValue(reader, acknowledged.snapshot(),
                    "s1", "cut:runtime", 1));

            Thread.sleep(1_300);
            assertTrue(firstRuntime.isServing());
            assertTrue(firstRuntime.failure().isEmpty());
            assertFalse(nextRuntime.start());
            firstRuntime.close();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, firstRuntime::currentView).errorCode());

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean tookOver = false;
            while (!tookOver && System.nanoTime() < deadline) {
                tookOver = nextRuntime.start();
                if (!tookOver) {
                    Thread.sleep(50);
                }
            }
            assertTrue(tookOver);
            var recovered = nextRuntime.currentView();
            assertTrue(recovered.generation() > acknowledged.generation());
            assertTrue(recovered.snapshot().sequence("s1", 1)
                    >= acknowledged.snapshot().sequence("s1", 1));
            assertArrayEquals(new byte[]{91}, mixedValue(reader, recovered.snapshot(),
                    "s1", "cut:runtime", 1));
        }
    }

    /** 分片采样故障必须撤销本地领导者资格，不能继续返回旧切面或确认写入。 */
    @Test
    void vectorLeaderRuntimeMustFailClosedWhenShardSamplingFails() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        try (var runtime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                control::currentPublished, leases, "runtime-fault", 1_000, 200)) {
            assertTrue(runtime.start());
            assertEquals(100, runtime.currentView().snapshot().decisionEpoch());
            try (Connection connection = firstData.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("rename table bench_striped_mvcc_state to bench_striped_mvcc_state_unavailable");
                try {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (runtime.failure().isEmpty() && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertTrue(runtime.failure().isPresent());
                    assertFalse(runtime.isServing());
                    assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            assertThrows(MvccException.class, runtime::currentView).errorCode());
                    assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            assertThrows(MvccException.class, () ->
                                    runtime.awaitPublished("s1", 1, 1, 100, 100)).errorCode());
                } finally {
                    statement.execute("rename table bench_striped_mvcc_state_unavailable to bench_striped_mvcc_state");
                }
            }
        }
    }

    /** 远端 RC 读通过中心代数发现当前端点，旧端点关闭后只接受接任者的切面。 */
    @Test
    void remoteVectorCutReaderMustRediscoverLeaderAfterHandoff() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        int firstPort = unusedLoopbackPort();
        int secondPort = unusedLoopbackPort();
        while (secondPort == firstPort) {
            secondPort = unusedLoopbackPort();
        }
        try (var firstRuntime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                control::currentPublished, leases, "127.0.0.1:" + firstPort, 1_000, 200);
             var nextRuntime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                     control::currentPublished, leases, "127.0.0.1:" + secondPort, 1_000, 200);
             var firstServer = new MysqlVectorCutHttpServer(firstRuntime, firstPort);
             var nextServer = new MysqlVectorCutHttpServer(nextRuntime, secondPort);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8)) {
            assertTrue(firstRuntime.start());
            firstServer.start();
            var remote = new MysqlVectorCutRemoteClient(controlData, routes, 4);
            var before = remote.currentView();
            assertEquals(firstRuntime.currentView().generation(), before.generation());
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:remote", new byte[]{101}, 1, 0));
            firstRuntime.awaitPublished("s1", 1, 1, 100, 5_000);
            var acknowledged = remote.currentView();
            assertArrayEquals(new byte[]{101}, mixedValue(reader, acknowledged.snapshot(),
                    "s1", "cut:remote", 1));

            firstRuntime.close();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean tookOver = false;
            while (!tookOver && System.nanoTime() < deadline) {
                tookOver = nextRuntime.start();
                if (!tookOver) {
                    Thread.sleep(50);
                }
            }
            assertTrue(tookOver);
            nextServer.start();
            var recovered = remote.currentView();
            assertTrue(recovered.generation() > acknowledged.generation());
            assertTrue(recovered.snapshot().sequence("s1", 1)
                    >= acknowledged.snapshot().sequence("s1", 1));
            assertArrayEquals(new byte[]{101}, mixedValue(reader, recovered.snapshot(),
                    "s1", "cut:remote", 1));
        }
    }

    /** 二进制 TCP 客户端在旧代数失效后重新发现端点并保持已确认值可见。 */
    @Test
    void tcpVectorCutReaderMustRediscoverLeaderAfterHandoff() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        int firstPort = unusedLoopbackPort();
        int secondPort = unusedLoopbackPort();
        while (secondPort == firstPort) {
            secondPort = unusedLoopbackPort();
        }
        try (var firstRuntime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                control::currentPublished, leases, "127.0.0.1:" + firstPort, 1_000, 200);
             var nextRuntime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                     control::currentPublished, leases, "127.0.0.1:" + secondPort, 1_000, 200);
             var firstServer = new MysqlVectorCutTcpServer(firstRuntime, firstPort);
             var nextServer = new MysqlVectorCutTcpServer(nextRuntime, secondPort);
             var remote = new MysqlVectorCutTcpClient(controlData, routes, 4);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8)) {
            assertTrue(firstRuntime.start());
            firstServer.start();
            assertEquals(firstRuntime.currentView().generation(), remote.currentView().generation());
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:tcp", new byte[]{121}, 1, 0));
            firstRuntime.awaitPublished("s1", 1, 1, 100, 5_000);
            var acknowledged = remote.currentView();
            assertArrayEquals(new byte[]{121}, mixedValue(reader, acknowledged.snapshot(),
                    "s1", "cut:tcp", 1));
            firstRuntime.close();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean tookOver = false;
            while (!tookOver && System.nanoTime() < deadline) {
                tookOver = nextRuntime.start();
                if (!tookOver) {
                    Thread.sleep(50);
                }
            }
            assertTrue(tookOver);
            nextServer.start();
            var recovered = remote.currentView();
            assertTrue(recovered.generation() > acknowledged.generation());
            assertArrayEquals(new byte[]{121}, mixedValue(reader, recovered.snapshot(),
                    "s1", "cut:tcp", 1));
        }
    }

    /** 网络切面端点停止时发布运行时须同步停止，不能只让远端读失败而本地继续 ACK。 */
    @Test
    void vectorCutEndpointShutdownMustStopLocalAcknowledgements() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        int port = unusedLoopbackPort();
        try (var runtime = new MysqlVectorCutLeaderRuntime(controlData, routes, 4,
                control::currentPublished, leases, "127.0.0.1:" + port, 1_000, 200);
             var server = new MysqlVectorCutTcpServer(runtime, port);
             var remote = new MysqlVectorCutTcpClient(controlData, routes, 4)) {
            assertTrue(runtime.start());
            server.start();
            assertEquals(runtime.currentView().generation(), remote.currentView().generation());
            server.close();
            assertFalse(runtime.isServing());
            assertTrue(runtime.failure().isEmpty());
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, runtime::currentView).errorCode());
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, () ->
                            runtime.awaitPublished("s1", 1, 1, 100, 100)).errorCode());
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());
        }
    }

    /** 强制杀死独立发布 JVM 后，新进程必须重建已确认切面并提升代数。 */
    @Test
    void remoteVectorCutMustRecoverAfterPublisherProcessIsKilled() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        int firstPort = unusedLoopbackPort();
        int secondPort = unusedLoopbackPort();
        while (secondPort == firstPort) {
            secondPort = unusedLoopbackPort();
        }
        var remote = new MysqlVectorCutRemoteClient(controlData, routes, 4);
        try (var reader = new MysqlStripedSnapshotOracle(routes, 4, 8);
             var firstProcess = launchVectorServer(firstPort)) {
            firstProcess.awaitReady();
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:process", new byte[]{111}, 1, 0));
            MysqlVectorCutLeaderRuntime.PublishedView acknowledged = null;
            long ackDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < ackDeadline) {
                var candidate = remote.currentView();
                if (candidate.snapshot().sequence("s1", 1) >= 1) {
                    acknowledged = candidate;
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(acknowledged != null);
            assertArrayEquals(new byte[]{111}, mixedValue(reader, acknowledged.snapshot(),
                    "s1", "cut:process", 1));
            firstProcess.kill();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());

            long takeoverDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (leases.discover().isPresent() && System.nanoTime() < takeoverDeadline) {
                Thread.sleep(50);
            }
            assertTrue(leases.discover().isEmpty());
            try (var secondProcess = launchVectorServer(secondPort)) {
                secondProcess.awaitReady();
                var recovered = remote.currentView();
                assertTrue(recovered.generation() > acknowledged.generation());
                assertTrue(recovered.snapshot().sequence("s1", 1)
                        >= acknowledged.snapshot().sequence("s1", 1));
                assertArrayEquals(new byte[]{111}, mixedValue(reader, recovered.snapshot(),
                        "s1", "cut:process", 1));
            }
        }
    }

    /** 强杀独立 TCP 发布进程后，旧复用连接失效并由新代数恢复已确认版本。 */
    @Test
    void tcpVectorCutMustRecoverAfterPublisherProcessIsKilled() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        int firstPort = unusedLoopbackPort();
        int secondPort = unusedLoopbackPort();
        while (secondPort == firstPort) {
            secondPort = unusedLoopbackPort();
        }
        try (var remote = new MysqlVectorCutTcpClient(controlData, routes, 4);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8);
             var firstProcess = launchVectorServer(firstPort, "tcp")) {
            firstProcess.awaitReady();
            assertEquals(1, MysqlStripedCrossVersionPrototype.localCommit(
                    firstData, "cut:tcp-process", new byte[]{112}, 1, 0));
            MysqlVectorCutLeaderRuntime.PublishedView acknowledged = null;
            long ackDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < ackDeadline) {
                var candidate = remote.currentView();
                if (candidate.snapshot().sequence("s1", 1) >= 1) {
                    acknowledged = candidate;
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(acknowledged != null);
            assertArrayEquals(new byte[]{112}, mixedValue(reader, acknowledged.snapshot(),
                    "s1", "cut:tcp-process", 1));
            firstProcess.kill();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());
            long takeoverDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (leases.discover().isPresent() && System.nanoTime() < takeoverDeadline) {
                Thread.sleep(50);
            }
            assertTrue(leases.discover().isEmpty());
            try (var secondProcess = launchVectorServer(secondPort, "tcp")) {
                secondProcess.awaitReady();
                var recovered = remote.currentView();
                assertTrue(recovered.generation() > acknowledged.generation());
                assertTrue(recovered.snapshot().sequence("s1", 1)
                        >= acknowledged.snapshot().sequence("s1", 1));
                assertArrayEquals(new byte[]{112}, mixedValue(reader, recovered.snapshot(),
                        "s1", "cut:tcp-process", 1));
            }
        }
    }

    /** 跨片中心决议后强杀发布 JVM，新代数仍须同时解析两片未物化意向。 */
    @Test
    void tcpVectorCutMustRecoverCommittedCrossShardIntentAfterProcessKill() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(firstData, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondData, 4);
        Map<String, DataSource> routes = Map.of("s1", firstData, "s2", secondData);
        var leases = new MysqlVectorCutLeaderLease(controlData, routes, 4);
        leases.initializeIsolated();
        int firstPort = unusedLoopbackPort();
        int secondPort = unusedLoopbackPort();
        while (secondPort == firstPort) {
            secondPort = unusedLoopbackPort();
        }
        try (var remote = new MysqlVectorCutTcpClient(controlData, routes, 4);
             var reader = new MysqlStripedSnapshotOracle(routes, 4, 8);
             var firstProcess = launchVectorServer(firstPort, "tcp")) {
            firstProcess.awaitReady();
            var oldRr = remote.currentView().snapshot();
            var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
            Set<String> participants = Set.of("s1", "s2");
            control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                    IsolationLevel.REPEATABLE_READ, participants);
            List<Mutation> left = List.of(Mutation.put("cut:kill:left", new byte[]{122}));
            List<Mutation> right = List.of(Mutation.put("cut:kill:right", new byte[]{123}));
            var leftProof = first.prewriteWithProof(writer.txId(), writer.startTs(), left);
            var rightProof = second.prewriteWithProof(writer.txId(), writer.startTs(), right);
            assertEquals(1, MysqlStripedCrossVersionPrototype.prewrite(
                    firstData, "cut:kill:left", new byte[]{122}, 1, writer.txId(), 0));
            assertEquals(1, MysqlStripedCrossVersionPrototype.prewrite(
                    secondData, "cut:kill:right", new byte[]{123}, 2, writer.txId(), 0));
            var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                    "kill-owner", "kill-request", 4, 5_000, 100);
            var active = control.activateIntentSegment(reserved);
            assertEquals(1, control.completeIntentBatch(List.of(
                    new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                            writer.txId(), writer.startTs(), writer.deadlineMillis(),
                            IsolationLevel.REPEATABLE_READ, participants, List.of(leftProof, rightProof)))));
            assertEquals(101, control.advancePublishedFast(10));
            MysqlVectorCutLeaderRuntime.PublishedView acknowledged = null;
            long ackDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < ackDeadline) {
                var candidate = remote.currentView();
                if (candidate.snapshot().decisionEpoch() >= 101
                        && candidate.snapshot().sequence("s1", 1) >= 1
                        && candidate.snapshot().sequence("s2", 2) >= 1) {
                    acknowledged = candidate;
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(acknowledged != null);
            assertArrayEquals(new byte[]{122}, mixedValue(reader, acknowledged.snapshot(),
                    "s1", "cut:kill:left", 1));
            assertArrayEquals(new byte[]{123}, mixedValue(reader, acknowledged.snapshot(),
                    "s2", "cut:kill:right", 2));
            firstProcess.kill();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, remote::currentView).errorCode());

            long takeoverDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (leases.discover().isPresent() && System.nanoTime() < takeoverDeadline) {
                Thread.sleep(50);
            }
            assertTrue(leases.discover().isEmpty());
            try (var secondProcess = launchVectorServer(secondPort, "tcp")) {
                secondProcess.awaitReady();
                var recovered = remote.currentView();
                assertTrue(recovered.generation() > acknowledged.generation());
                assertTrue(recovered.snapshot().decisionEpoch() >= 101);
                assertTrue(recovered.snapshot().sequence("s1", 1) >= 1);
                assertTrue(recovered.snapshot().sequence("s2", 2) >= 1);
                assertArrayEquals(new byte[]{122}, mixedValue(reader, recovered.snapshot(),
                        "s1", "cut:kill:left", 1));
                assertArrayEquals(new byte[]{123}, mixedValue(reader, recovered.snapshot(),
                        "s2", "cut:kill:right", 2));
                assertTrue(reader.readMixed(oldRr, "s1", "cut:kill:left", 1,
                        this::committedEpochFromCenter).isEmpty());
                MysqlStripedCrossVersionPrototype.materialize(firstData, "cut:kill:left",
                        writer.txId(), 101, this::committedEpochFromCenter);
                assertArrayEquals(new byte[]{122}, mixedValue(reader, recovered.snapshot(),
                        "s1", "cut:kill:left", 1));
                assertArrayEquals(new byte[]{123}, mixedValue(reader, recovered.snapshot(),
                        "s2", "cut:kill:right", 2));
            }
            first.commit(writer.txId(), 101, left, true);
            second.commit(writer.txId(), 101, right, true);
            control.releaseSnapshot(writer.txId());
        }
    }

    /** 使用当前测试 JVM 类路径启动真正独立的发布服务进程。 */
    private static VectorServerProcess launchVectorServer(int port) throws Exception {
        return launchVectorServer(port, "http");
    }

    /** 传输方式只允许回环 HTTP 或二进制 TCP，启动过程保持相同租约语义。 */
    private static VectorServerProcess launchVectorServer(int port, String transport) throws Exception {
        String javaBinary = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classPath = System.getProperty("java.class.path");
        ProcessBuilder builder = new ProcessBuilder(javaBinary, "-cp", classPath,
                MysqlVectorCutServerProbe.class.getName(),
                MYSQL.getJdbcUrl().replace("/bootstrap", "/control"),
                MYSQL.getJdbcUrl().replace("/bootstrap", "/shard_1"),
                MYSQL.getJdbcUrl().replace("/bootstrap", "/shard_2"),
                MYSQL.getUsername(), MYSQL.getPassword(), Integer.toString(port), "1500", transport);
        builder.redirectErrorStream(true);
        builder.environment().put("AZIR_TX_VECTOR_TEST_SERVER", "true");
        Process process = builder.start();
        BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        Thread.ofVirtual().start(() -> {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = input.readLine()) != null) {
                    lines.offer(line);
                }
            } catch (Exception ignored) {
                // 子进程被 SIGKILL 后输出管道关闭属于预期故障注入路径。
            }
        });
        return new VectorServerProcess(process, lines);
    }

    /** 测试负责回收子 JVM，不能把失联进程留给后续用例。 */
    private record VectorServerProcess(Process process, BlockingQueue<String> lines) implements AutoCloseable {
        /** 等待服务明确声明已持租约并监听，不把进程仍存活当作就绪。 */
        private void awaitReady() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                String line = lines.poll(100, TimeUnit.MILLISECONDS);
                if (line != null && line.startsWith("VECTOR_SERVER_READY")) {
                    return;
                }
                if (!process.isAlive() && lines.isEmpty()) {
                    throw new IllegalStateException("vector server subprocess exited before readiness");
                }
            }
            throw new IllegalStateException("vector server subprocess did not become ready");
        }

        /** SIGKILL 模拟发布进程突然消失，而非调用正常关闭钩子。 */
        private void kill() throws Exception {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }

        /** 成功与失败路径都清理子进程。 */
        @Override
        public void close() throws Exception {
            kill();
        }
    }

    /** 回环 RPC 测试只临时保留系统分配端口，不使用固定服务端口。 */
    private static int unusedLoopbackPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** 从持久中心决议解析跨片意向，缺失事务元数据时拒绝猜测状态。 */
    private byte[] mixedValue(MysqlStripedSnapshotOracle oracle,
                              MysqlStripedSnapshotOracle.Snapshot snapshot,
                              String shardId, String key, int stripe) {
        return oracle.readMixed(snapshot, shardId, key, stripe,
                this::committedEpochFromCenter).orElseThrow();
    }

    /** 中心状态缺失时失败关闭，只有真实 COMMITTED 决议提供可见时间。 */
    private OptionalLong committedEpochFromCenter(long txId) {
        var record = metadata.get(txId).orElseThrow(() ->
                new IllegalStateException("cross intent has no durable center record"));
        return record.status() == TxStatus.COMMITTED
                ? OptionalLong.of(record.commitTs()) : OptionalLong.empty();
    }

    /** 中止恢复必须读取持久中心状态，不能依赖前台内存中的推测。 */
    private TxStatus statusFromCenter(long txId) {
        return metadata.get(txId).orElseThrow(() ->
                new IllegalStateException("cross intent has no durable center record")).status();
    }

    /** 中心决议响应丢失后，恢复器按持久决议补齐两个分片并释放 GC 锚点。 */
    @Test
    void committedIntentRecoveryMustMaterializeEveryShardBeforeFinalization() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:recover:left", new byte[]{5})));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:recover:right", new byte[]{6})));
        var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-recover", "intent-recover", 1, 5_000, 100);
        var active = control.activateIntentSegment(reserved);
        assertEquals(1, control.completeIntentBatch(List.of(new MysqlLocalFirstPublicationLedger.IntentCommitRequest(
                active, 101, writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right)))));
        assertEquals(101, control.advancePublishedFast(10));
        var missingRoute = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first));
        assertEquals(1, missingRoute.recoverOnce(10).failures());
        assertEquals(TxStatus.COMMITTED, first.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ACTIVE, second.participantRecord(writer.txId()).orElseThrow().status());
        var incomplete = first.commitIntentWithProof(writer.txId(), 101);
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () ->
                        control.markIntentFinalized(writer.txId(), List.of(incomplete))).errorCode());
        assertEquals(List.of(writer.txId()), control.findUnfinalizedIntentDecisions(Long.MIN_VALUE, 10));
        assertEquals(99, control.advanceSafePoint());
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(1, recovery.recoverOnce(10).finalized());
        assertEquals(0, recovery.recoverOnce(10).failures());
        assertTrue(control.findUnfinalizedIntentDecisions(Long.MIN_VALUE, 10).isEmpty());
        assertEquals(TxStatus.COMMITTED, second.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(101L, second.participantRecord(writer.txId()).orElseThrow().commitTs());
        control.releaseSnapshot(writer.txId());
    }

    /** ACTIVE 到期先落中心 ABORTED，缺失路由时保持未完成 GC 锚点。 */
    @Test
    void expiredIntentRecoveryMustAbortEveryShardBeforeCenterTerminal() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        first.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:expire:left", new byte[]{7})));
        second.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:expire:right", new byte[]{8})));
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_txn_v1 set deadline_millis = 1 where tx_id = "
                    + writer.txId());
        }
        var missingRoute = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first));
        assertEquals(1, missingRoute.recoverOnce(10).failures());
        assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(99, control.advanceSafePoint());
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(1, recovery.recoverOnce(10).aborted());
        assertEquals(0, recovery.recoverOnce(10).failures());
        assertEquals(TxStatus.ABORTED, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, first.participantRecord(writer.txId()).orElseThrow().status());
        assertEquals(TxStatus.ABORTED, second.participantRecord(writer.txId()).orElseThrow().status());
        assertTrue(control.findIntentAbortWorkIds(Long.MIN_VALUE, 10).isEmpty());
        control.releaseSnapshot(writer.txId());
    }

    /** 前台预写失败可立即作中心 ABORTED 决议，后台继续补齐未完成分片回滚。 */
    @Test
    void earlyIntentAbortMustRemainRecoverableUntilEveryParticipantIsAborted() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, Set.of("s1", "s2"));
        first.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:early-abort:left", new byte[]{1})));
        second.prewrite(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:early-abort:right", new byte[]{2})));
        assertEquals(TxStatus.ABORTED, control.abortIntentTransaction(writer.txId()).status());
        assertEquals(TxStatus.ABORTED, control.abortIntentTransaction(writer.txId()).status());
        first.abort(writer.txId());
        control.releaseSnapshot(writer.txId());
        assertEquals(99, control.advanceSafePoint());
        var incomplete = first.abortIntentWithProof(writer.txId());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () ->
                        control.markIntentAbortFinalized(writer.txId(), List.of(incomplete))).errorCode());
        assertEquals(List.of(writer.txId()), control.findIntentAbortWorkIds(Long.MIN_VALUE, 10));
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(1, recovery.recoverOnce(10).aborted());
        assertEquals(TxStatus.ABORTED, second.participantRecord(writer.txId()).orElseThrow().status());
        assertTrue(control.findIntentAbortWorkIds(Long.MIN_VALUE, 10).isEmpty());
        assertEquals(100, control.advanceSafePoint());
    }

    /** 单键物化例程遇到多键分片时回退通用事务，仍保持同一中心提交时间。 */
    @Test
    void multiKeyIntentRecoveryMustFallBackWithoutLosingVersions() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(), List.of(
                Mutation.put("cross:multi:a", new byte[]{1}),
                Mutation.put("cross:multi:b", new byte[]{2})));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(), List.of(
                Mutation.put("cross:multi:c", new byte[]{3})));
        var segment = control.activateIntentSegment(control.reserveSegment(
                MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-multi", "intent-multi", 1, 5_000, 100));
        var request = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(segment, 101,
                writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right));
        assertEquals(101, control.completeIntentBatchAndPublish(List.of(request)).publishedTs());
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(1, recovery.recoverOnce(10).finalized());
        var reader = new ReadContext(999, 101, IsolationLevel.READ_COMMITTED);
        assertArrayEquals(new byte[]{1}, first.get(reader, "cross:multi:a").orElseThrow().value());
        assertArrayEquals(new byte[]{2}, first.get(reader, "cross:multi:b").orElseThrow().value());
        assertArrayEquals(new byte[]{3}, second.get(reader, "cross:multi:c").orElseThrow().value());
        control.releaseSnapshot(writer.txId());
    }

    /** 一笔最终化证明错配时整批 GC 标记回滚，正确证明可重试并幂等重放。 */
    @Test
    void intentFinalizationBatchMustBeAtomicAndReplayable() {
        var firstWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var secondWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        var segment = control.activateIntentSegment(control.reserveSegment(
                MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-finalize", "intent-finalize", 2, 5_000, 100));
        var firstDecision = intentRequest(firstWriter, segment, participants, 101,
                "finalize-one", new byte[]{1});
        var secondDecision = intentRequest(secondWriter, segment, participants, 102,
                "finalize-two", new byte[]{2});
        assertEquals(102, control.completeIntentBatchAndPublish(List.of(firstDecision, secondDecision))
                .publishedTs());
        var firstCenter = control.committedIntentProof(firstWriter.txId());
        var secondCenter = control.committedIntentProof(secondWriter.txId());
        var firstLeft = first.commitIntentWithProof(firstCenter);
        var firstRight = second.commitIntentWithProof(firstCenter);
        var secondLeft = first.commitIntentWithProof(secondCenter);
        var secondRight = second.commitIntentWithProof(secondCenter);
        var firstFinalization = new MysqlLocalFirstPublicationLedger.IntentFinalizationRequest(
                firstWriter.txId(), List.of(firstLeft, firstRight));
        var badSecond = new MysqlLocalFirstPublicationLedger.IntentFinalizationRequest(
                secondWriter.txId(), List.of(firstLeft, secondRight));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () ->
                        control.markIntentFinalizedBatch(List.of(firstFinalization, badSecond))).errorCode());
        assertEquals(2, control.countUnfinalizedIntentDecisions());
        var secondFinalization = new MysqlLocalFirstPublicationLedger.IntentFinalizationRequest(
                secondWriter.txId(), List.of(secondLeft, secondRight));
        assertEquals(2, control.markIntentFinalizedBatch(List.of(secondFinalization, firstFinalization)));
        assertEquals(0, control.markIntentFinalizedBatch(List.of(firstFinalization, secondFinalization)));
        assertEquals(0, control.countUnfinalizedIntentDecisions());
        control.releaseSnapshot(firstWriter.txId());
        control.releaseSnapshot(secondWriter.txId());
    }

    /** 中心意向 owner 以活跃快照作段下界，完成决议后主动封存空尾并释放安全点。 */
    @Test
    void intentOwnerMustSealTailAndReleaseSnapshotFloor() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:owner:left", new byte[]{1})));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:owner:right", new byte[]{2})));
        var owner = new LocalFirstIntentSegmentOwnerRegistry(control, () -> writer.startTs(),
                4, 5_000, 1_000, 60_000);
        var assigned = owner.allocate(writer.startTs());
        assertEquals(writer.startTs(), assigned.reservation().snapshotFloor());
        var request = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(assigned.reservation(),
                assigned.commitTs(), writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right));
        assertEquals(101, control.completeIntentBatchAndPublish(List.of(request)).publishedTs());
        owner.complete(assigned);
        control.releaseSnapshot(writer.txId());
        assertEquals(99, control.advanceSafePoint());
        owner.close();
        assertEquals(MysqlLocalFirstPublicationLedger.SegmentStatus.SEALED,
                control.segment(assigned.reservation().lease().firstTs()).orElseThrow().status());
        assertEquals(104, control.currentPublished());
        assertEquals(99, control.advanceSafePoint());
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(1, recovery.recoverOnce(10).finalized());
        assertEquals(104, control.advanceSafePoint());
    }

    /** 同一事务组必须连续领取中心槽，避免其他并发组把发布前缀插入本组中间。 */
    @Test
    void intentOwnerBatchMustAllocateContiguousCommittedSlots() {
        List<BeginReservation> writers = new java.util.ArrayList<>();
        for (int index = 0; index < 4; index++) {
            writers.add(control.begin(IsolationLevel.REPEATABLE_READ, 60_000));
        }
        var owner = new LocalFirstIntentSegmentOwnerRegistry(control, () -> 100,
                8, 5_000, 1_000, 60_000);
        List<LocalFirstIntentSegmentOwnerRegistry.Assignment> assignments = owner.allocateBatch(
                writers.stream().map(BeginReservation::startTs).toList());
        assertEquals(List.of(101L, 102L, 103L, 104L),
                assignments.stream().map(LocalFirstIntentSegmentOwnerRegistry.Assignment::commitTs).toList());
        Set<String> participants = Set.of("s1", "s2");
        List<MysqlLocalFirstPublicationLedger.IntentCommitRequest> decisions = new java.util.ArrayList<>();
        for (int index = 0; index < writers.size(); index++) {
            decisions.add(intentRequest(writers.get(index), assignments.get(index).reservation(),
                    participants, assignments.get(index).commitTs(),
                    "owner-cohort-" + index, new byte[]{(byte) index}));
        }
        assertEquals(104, control.completeIntentBatchAndPublish(decisions).publishedTs());
        assignments.forEach(owner::complete);
        writers.forEach(writer -> control.releaseSnapshot(writer.txId()));
        owner.close();
        assertEquals(108, control.currentPublished());
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(4, recovery.recoverOnce(10).finalized());
    }

    /** 长 RR 持续阻止三库 GC 越过旧快照；释放后中心历史可按槽、元数据、段顺序清理。 */
    @Test
    void crossShardGcMustKeepOldRrAndReclaimFinalizedHistory() throws Exception {
        var oldReader = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:gc:left", new byte[]{1})));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:gc:right", new byte[]{2})));
        var owner = new LocalFirstIntentSegmentOwnerRegistry(control, () -> oldReader.startTs(),
                4, 5_000, 1_000, 60_000);
        var assigned = owner.allocate(writer.startTs());
        var decision = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(assigned.reservation(),
                assigned.commitTs(), writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right));
        assertEquals(101, control.completeIntentBatchAndPublish(List.of(decision)).publishedTs());
        owner.complete(assigned);
        owner.close();
        var recovery = new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second));
        assertEquals(1, recovery.recoverOnce(10).finalized());
        control.releaseSnapshot(writer.txId());
        assertEquals(99, control.advanceSafePoint());
        first.gc(99, 100);
        second.gc(99, 100);
        var old = new ReadContext(oldReader.txId(), oldReader.startTs(), IsolationLevel.REPEATABLE_READ);
        assertTrue(first.get(old, "cross:gc:left").isEmpty());
        assertTrue(second.get(old, "cross:gc:right").isEmpty());
        control.releaseSnapshot(oldReader.txId());
        assertEquals(104, control.advanceSafePoint());
        assertEquals(3, control.purgeTerminalSlots(104, Long.MAX_VALUE, 10));
        assertEquals(0, control.purgeSealedSegments(104, Long.MAX_VALUE, 10));
        var next = control.activateIntentSegment(control.reserveSegment(
                MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-gc-next", "intent-gc-next", 1, 5_000, 104));
        control.beginFencing(next, true);
        assertEquals(1, control.sealIntentSegment(next));
        assertEquals(105, control.advancePublishedFast(10));
        assertEquals(105, control.advanceSafePoint());
        first.gc(105, 100);
        second.gc(105, 100);
        assertEquals(1, control.purgeTerminalSlots(105, Long.MAX_VALUE, 10));
        assertEquals(1, control.purgeFinalizedMetadata(105, Long.MAX_VALUE, 10));
        assertEquals(1, control.purgeSealedSegments(105, Long.MAX_VALUE, 10));
        var current = new ReadContext(999, 105, IsolationLevel.READ_COMMITTED);
        assertArrayEquals(new byte[]{1}, first.get(current, "cross:gc:left").orElseThrow().value());
        assertArrayEquals(new byte[]{2}, second.get(current, "cross:gc:right").orElseThrow().value());
    }

    /** 第二槽违反数据库约束时整批中心决议回滚，移除故障后同一批可重试。 */
    @Test
    void intentDecisionBatchMustRollBackAtomically() throws Exception {
        var firstWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        var secondWriter = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-batch", "intent-batch", 2, 5_000, 100);
        var active = control.activateIntentSegment(reserved);
        var firstDecision = intentRequest(firstWriter, active, participants, 101,
                "batch-one", new byte[]{1});
        var secondDecision = intentRequest(secondWriter, active, participants, 102,
                "batch-two", new byte[]{2});
        List<MysqlLocalFirstPublicationLedger.IntentCommitRequest> batch = List.of(firstDecision, secondDecision);
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_first_slot_v1 add constraint mvcc_test_reject_intent_slot "
                    + "check (status <> 'COMMITTED' or commit_ts <> 102)");
        }
        try {
            assertThrows(MvccException.class, () -> control.completeIntentBatch(batch));
            assertEquals(TxStatus.ACTIVE, metadata.get(firstWriter.txId()).orElseThrow().status());
            assertEquals(TxStatus.ACTIVE, metadata.get(secondWriter.txId()).orElseThrow().status());
            assertEquals(100, control.currentPublished());
        } finally {
            try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_first_slot_v1 drop check mvcc_test_reject_intent_slot");
            }
        }
        var published = control.completeIntentBatchAndPublish(batch);
        assertEquals(2, published.completed());
        assertEquals(102, published.publishedTs());
        assertEquals(0, control.completeIntentBatchAndPublish(batch).completed());
        control.releaseSnapshot(firstWriter.txId());
        control.releaseSnapshot(secondWriter.txId());
    }

    /** 中心批次失败后失联 owner 到期，恢复器中止空槽并发布，迟到决议不能复活。 */
    @Test
    void expiredIntentSegmentRecoveryMustSealFailedDecisionSlot() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:recover-slot:left", new byte[]{1})));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:recover-slot:right", new byte[]{2})));
        var segment = control.activateIntentSegment(control.reserveSegment(
                MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-lost", "intent-lost", 1, 100, 100));
        var request = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(segment, 101,
                writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right));
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("alter table mvcc_local_first_slot_v1 add constraint mvcc_test_reject_lost_slot "
                    + "check (status <> 'COMMITTED' or commit_ts <> 101)");
        }
        try {
            assertThrows(MvccException.class, () -> control.completeIntentBatch(List.of(request)));
        } finally {
            try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_local_first_slot_v1 drop check mvcc_test_reject_lost_slot");
            }
        }
        Thread.sleep(200);
        var recovery = new LocalFirstPublicationRecoveryManager(control, Map.of());
        var result = recovery.recoverOnce(10, 10);
        assertEquals(1, result.sealedSegments());
        assertEquals(1, result.abortedSlots());
        assertEquals(101, result.publishedTs());
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(ErrorCode.TRANSACTION_ABORTED,
                assertThrows(MvccException.class, () -> control.completeIntentBatch(List.of(request))).errorCode());
        control.abortIntentTransaction(writer.txId());
        assertEquals(1, new LocalFirstIntentRecoveryManager(control, Map.of("s1", first, "s2", second))
                .recoverOnce(10).aborted());
        control.releaseSnapshot(writer.txId());
    }

    /** 仅一个分片持久预写证明时中心不得提交跨分片决议。 */
    @Test
    void intentDecisionMustRejectIncompletePrewriteProofs() {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:proof:left", new byte[]{1})));
        var reserved = control.reserveSegment(MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-proof", "intent-proof", 1, 5_000, 100);
        var active = control.activateIntentSegment(reserved);
        assertThrows(IllegalArgumentException.class, () ->
                new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                        writer.txId(), writer.startTs(), writer.deadlineMillis(),
                        IsolationLevel.REPEATABLE_READ, participants, List.of(left)));
        var duplicate = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(active, 101,
                writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, left));
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> control.completeIntentBatch(List.of(duplicate)))
                        .errorCode());
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(100, control.currentPublished());
        control.releaseSnapshot(writer.txId());
    }

    /** 决议等待事务行锁跨越数据库期限时，取锁后的重新读时钟必须拒绝提交。 */
    @Test
    void intentDecisionMustRecheckDeadlineAfterMetadataLock() throws Exception {
        var writer = control.begin(IsolationLevel.REPEATABLE_READ, 60_000);
        Set<String> participants = Set.of("s1", "s2");
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:deadline:left", new byte[]{1})));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:deadline:right", new byte[]{2})));
        var segment = control.activateIntentSegment(control.reserveSegment(
                MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE,
                "owner-deadline", "intent-deadline", 1, 5_000, 100));
        long deadline;
        try (Connection connection = controlData.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_local_first_txn_v1 set deadline_millis = "
                    + "cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + 500 "
                    + "where tx_id = " + writer.txId());
            try (ResultSet row = statement.executeQuery("select deadline_millis from mvcc_local_first_txn_v1 "
                    + "where tx_id = " + writer.txId())) {
                assertTrue(row.next());
                deadline = row.getLong(1);
            }
        }
        var request = new MysqlLocalFirstPublicationLedger.IntentCommitRequest(segment, 101,
                writer.txId(), writer.startTs(), deadline,
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right));
        try (Connection blocker = controlData.getConnection(); Statement statement = blocker.createStatement()) {
            blocker.setAutoCommit(false);
            try (ResultSet locked = statement.executeQuery("select tx_id from mvcc_local_first_txn_v1 "
                    + "where tx_id = " + writer.txId() + " for update")) {
                assertTrue(locked.next());
            }
            CompletableFuture<Void> decision = CompletableFuture.runAsync(() ->
                    control.completeIntentBatch(List.of(request)));
            Thread.sleep(800);
            assertFalse(decision.isDone());
            blocker.commit();
            assertEquals(ErrorCode.TRANSACTION_ABORTED,
                    assertThrows(CompletionException.class, decision::join)
                            .getCause() instanceof MvccException ex ? ex.errorCode() : null);
        }
        assertEquals(TxStatus.ACTIVE, metadata.get(writer.txId()).orElseThrow().status());
        assertEquals(100, control.currentPublished());
        control.releaseSnapshot(writer.txId());
    }

    /** 为两个独立写事务建立完整意向证明，待测试再绑定同一中心段。 */
    private MysqlLocalFirstPublicationLedger.IntentCommitRequest intentRequest(
            BeginReservation writer, MysqlLocalFirstPublicationLedger.SegmentReservation segment,
            Set<String> participants,
            long commitTs, String key, byte[] value) {
        control.registerIntentTransaction(writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants);
        var left = first.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:" + key + ":left", value)));
        var right = second.prewriteWithProof(writer.txId(), writer.startTs(),
                List.of(Mutation.put("cross:" + key + ":right", value)));
        return new MysqlLocalFirstPublicationLedger.IntentCommitRequest(
                segment,
                commitTs, writer.txId(), writer.startTs(), writer.deadlineMillis(),
                IsolationLevel.REPEATABLE_READ, participants, List.of(left, right));
    }
}

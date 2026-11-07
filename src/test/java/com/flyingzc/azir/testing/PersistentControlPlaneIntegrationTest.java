package com.flyingzc.azir.testing;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.gc.SafePointStore;
import com.flyingzc.azir.gc.mysql.MysqlSafePointStore;
import com.flyingzc.azir.gc.GcManager;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.FastTransactionBegin;
import com.flyingzc.azir.metadata.FastTransactionCommit;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.recovery.RecoveryScheduler;
import com.flyingzc.azir.recovery.RecoveryManager;
import com.flyingzc.azir.recovery.BackgroundRecoveryRunner;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import com.flyingzc.azir.txn.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证持久控制面、协调器重启、快照租约及垃圾回收。 */
@Testcontainers
class PersistentControlPlaneIntegrationTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withUsername("test")
            .withPassword("test")
            .withDatabaseName("bootstrap");

    private static DataSource controlDataSource;
    private static DataSource shard1DataSource;
    private static DataSource shard2DataSource;
    private MysqlStorageAdapter shard1;
    private MysqlStorageAdapter shard2;

    /** 创建隔离测试数据库并初始化协议表。 */
    @BeforeAll
    static void beforeAll() throws Exception {
        createDatabase("control");
        createDatabase("shard_1");
        createDatabase("shard_2");
        controlDataSource = MysqlDataSourceFactory.create(databaseUrl("control"), MYSQL.getUsername(), MYSQL.getPassword());
        shard1DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_1"), MYSQL.getUsername(), MYSQL.getPassword());
        shard2DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_2"), MYSQL.getUsername(), MYSQL.getPassword());
        new MysqlStorageAdapter("shard-1", shard1DataSource).initialize();
        new MysqlStorageAdapter("shard-2", shard2DataSource).initialize();
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        tso.initialize(100);
        MysqlTransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        metadataStore.initialize();
        MysqlSafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        safePointStore.initialize(0);
    }

    /** 关闭测试使用的数据源。 */
    @AfterAll
    static void afterAll() throws Exception {
        close(controlDataSource);
        close(shard1DataSource);
        close(shard2DataSource);
    }

    /** 清理协议状态并为当前用例创建独立适配器。 */
    @BeforeEach
    void setUp() throws Exception {
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);
        truncate(controlDataSource, "mvcc_txn_finalized", "mvcc_snapshot_lease", "mvcc_txn", "mvcc_tso", "mvcc_safe_point");
        truncate(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
        shard1.initialize();
        truncate(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
        shard2.initialize();

        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        tso.initialize(100);
        MysqlTransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        metadataStore.initialize();
        MysqlSafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        safePointStore.initialize(0);
    }

    /** 验证重启后使用新适配器读取持久中心决议并完成恢复。 */
    @Test
    void restartedCoordinatorShouldRecoverCommittedTransactionFromMysqlMetadata() {
        MysqlTimestampOracle tso1 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore1 = new MysqlTransactionMetadataStore(controlDataSource);
        TransactionManager manager1 = new TransactionManager(tso1, metadataStore1, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)
        ));

        Transaction tx = manager1.begin(IsolationLevel.REPEATABLE_READ);
        manager1.put(tx, "shard-1", "acct:1", bytes("100"));
        manager1.put(tx, "shard-2", "acct:2", bytes("200"));

        MvccException error = assertThrows(MvccException.class, () -> manager1.commit(tx));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
        // 模拟重启时重建适配器，不复绑旧协调器使用的实例。
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore2);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore2);
        TransactionManager manager2 = new TransactionManager(tso2, metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        recoveryManager.recover(tx.txId());

        Transaction reader = manager2.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("100", readString(manager2, reader, "shard-1", "acct:1"));
        assertEquals("200", readString(manager2, reader, "shard-2", "acct:2"));

        long nextTs = tso2.nextTimestamp();
        assertTrue(nextTs > tx.txId());
    }

    /** 提交例程已持久化决议但响应丢失时，独立协调器必须按原 commitTs 补物化。 */
    @Test
    void lostServerSideCommitResponseMustRecoverTheDurableDecision() {
        var backing = new MysqlTransactionMetadataStore(controlDataSource);
        backing.initialize();
        var loseResponse = new AtomicBoolean(true);
        TransactionMetadataStore intercepted = (TransactionMetadataStore) Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(),
                new Class<?>[]{TransactionMetadataStore.class, FastTransactionBegin.class, FastTransactionCommit.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(backing, arguments);
                        if (method.getName().equals("tryCommit") && loseResponse.getAndSet(false)) {
                            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                                    "simulated lost commit response", true, null);
                        }
                        return result;
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
        var first = new MysqlStorageAdapter("shard-1", shard1DataSource, intercepted);
        var second = new MysqlStorageAdapter("shard-2", shard2DataSource, intercepted);
        var coordinator = new TransactionManager(new MysqlTimestampOracle(controlDataSource), intercepted,
                Map.of(first.shardId(), first, second.shardId(), second));
        Transaction tx = coordinator.begin(IsolationLevel.REPEATABLE_READ);
        coordinator.put(tx, "shard-1", "lost:one", bytes("one"));
        coordinator.put(tx, "shard-2", "lost:two", bytes("two"));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> coordinator.commit(tx)).errorCode());
        var persisted = backing.get(tx.txId()).orElseThrow();
        assertEquals(TxStatus.COMMITTED, persisted.status());
        long commitTs = persisted.commitTs();

        var restartedStore = new MysqlTransactionMetadataStore(controlDataSource);
        var recoveredFirst = new MysqlStorageAdapter("shard-1", shard1DataSource, restartedStore);
        var recoveredSecond = new MysqlStorageAdapter("shard-2", shard2DataSource, restartedStore);
        new RecoveryManager(restartedStore, Map.of(recoveredFirst.shardId(), recoveredFirst,
                recoveredSecond.shardId(), recoveredSecond)).recover(tx.txId());
        assertEquals(commitTs, restartedStore.get(tx.txId()).orElseThrow().commitTs().longValue());
        try (var reader = new TransactionManager(new MysqlTimestampOracle(controlDataSource), restartedStore,
                Map.of(recoveredFirst.shardId(), recoveredFirst, recoveredSecond.shardId(), recoveredSecond))) {
            Transaction snapshot = reader.begin(IsolationLevel.READ_COMMITTED);
            assertEquals("one", readString(reader, snapshot, "shard-1", "lost:one"));
            assertEquals("two", readString(reader, snapshot, "shard-2", "lost:two"));
            reader.commit(snapshot);
        }
        coordinator.close();
    }

    /** 验证省略显式准备的事务仍能在协调器重启后恢复。 */
    @Test
    void restartedCoordinatorShouldRecoverCommittedTransactionWithoutPreparingMetadataStep() {
        MysqlTimestampOracle tso1 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore1 = new MysqlTransactionMetadataStore(controlDataSource);
        TransactionManager manager1 = new TransactionManager(
                tso1,
                metadataStore1,
                new com.flyingzc.azir.txn.TransactionOptions(
                        com.flyingzc.azir.txn.LockWaitPolicy.mysqlLikeDefaults(),
                        com.flyingzc.azir.txn.TransactionRetryPolicy.mysqlLikeDefaults(),
                        com.flyingzc.azir.txn.CrossShardCommitPolicy.parallel(),
                        true,
                        false,
                        false
                ),
                Map.of(
                        shard1.shardId(), shard1,
                        shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)
                )
        );

        Transaction tx = manager1.begin(IsolationLevel.REPEATABLE_READ);
        manager1.put(tx, "shard-1", "acct:no-prepare:1", bytes("100"));
        manager1.put(tx, "shard-2", "acct:no-prepare:2", bytes("200"));

        MvccException error = assertThrows(MvccException.class, () -> manager1.commit(tx));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
        // 模拟重启时重建适配器，不复绑旧协调器使用的实例。
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore2);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore2);
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        recoveryManager.recover(tx.txId());

        TransactionManager manager2 = new TransactionManager(tso2, metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        Transaction reader = manager2.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("100", readString(manager2, reader, "shard-1", "acct:no-prepare:1"));
        assertEquals("200", readString(manager2, reader, "shard-2", "acct:no-prepare:2"));
    }

    /** 验证后台扫描可发现残留意向并完成一次事务恢复。 */
    @Test
    void backgroundRecoveryRunnerShouldScanLingeringIntentsAndRecover() {
        MysqlTimestampOracle tso1 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore1 = new MysqlTransactionMetadataStore(controlDataSource);
        TransactionManager manager1 = new TransactionManager(tso1, metadataStore1, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)
        ));

        Transaction tx = manager1.begin(IsolationLevel.REPEATABLE_READ);
        manager1.put(tx, "shard-1", "order:1", bytes("created"));
        manager1.put(tx, "shard-2", "stock:1", bytes("reserved"));
        assertThrows(MvccException.class, () -> manager1.commit(tx));

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
        // 模拟重启时重建适配器，不复绑旧协调器使用的实例。
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore2);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore2);
        TransactionManager manager2 = new TransactionManager(tso2, metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        BackgroundRecoveryRunner runner = new BackgroundRecoveryRunner(recoveryManager, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        int recovered = runner.recoverOnce(100);

        assertEquals(1, recovered);
        Transaction reader = manager2.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("created", readString(manager2, reader, "shard-1", "order:1"));
        assertEquals("reserved", readString(manager2, reader, "shard-2", "stock:1"));
    }

    /** 验证活跃快照阻止水位推进，结束后能够回收旧版本。 */
    @Test
    void safePointShouldBeBlockedByOldestActiveTransactionAndGcShouldReclaimOldVersions() throws Exception {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        TransactionManager manager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        Transaction seed1 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed1, "shard-1", "gc:001", bytes("v1"));
        manager.commit(seed1);

        Transaction longTx = manager.begin(IsolationLevel.REPEATABLE_READ);
        assertEquals("v1", readString(manager, longTx, "shard-1", "gc:001"));

        Transaction seed2 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed2, "shard-1", "gc:001", bytes("v2"));
        manager.commit(seed2);

        long safePoint = gcManager.advanceSafePoint();
        assertEquals(longTx.startTs() - 1, safePoint);

        int reclaimedWhileBlocked = gcManager.gcOnce(100);
        assertEquals(0, reclaimedWhileBlocked);
        assertEquals(2, countRows(shard1DataSource, "mvcc_data"));

        manager.rollback(longTx);

        int reclaimed = gcManager.gcOnce(100);
        assertTrue(reclaimed >= 1);
        assertEquals(1, countRows(shard1DataSource, "mvcc_data"));
    }

    /** 验证真实事务快照被测试强推水位越过后拒绝点读和扫描。 */
    @Test
    void readBelowSafePointShouldReturnSnapshotTooOld() {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        TransactionManager manager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        Transaction seed1 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed1, "shard-1", "gc:002", bytes("v1"));
        manager.commit(seed1);

        Transaction seed2 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed2, "shard-1", "gc:002", bytes("v2"));
        manager.commit(seed2);

        Transaction oldSnapshot = manager.begin(IsolationLevel.REPEATABLE_READ);
        assertEquals("v2", readString(manager, oldSnapshot, "shard-1", "gc:002"));
        assertEquals(oldSnapshot.deadlineMillis(), metadataStore.snapshotExpiry(oldSnapshot.txId()).orElseThrow().longValue());
        // 仅在测试中越过真实活跃快照，验证读入口拒绝低于水位的快照。
        safePointStore.advanceTo(oldSnapshot.startTs() + 1);
        MvccException error = assertThrows(MvccException.class, () -> manager.get(oldSnapshot, "shard-1", "gc:002"));
        assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, error.errorCode());
        MvccException scanError = assertThrows(MvccException.class,
                () -> manager.scan(oldSnapshot, "shard-1", "gc:000", "gc:999"));
        assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, scanError.errorCode());
        manager.rollback(oldSnapshot);
    }

    /** 验证安全点之后完整回收墓碑链且删除值不可见。 */
    @Test
    void gcShouldReclaimTombstoneChainAfterSafePoint() throws Exception {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        TransactionManager manager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        Transaction seed = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed, "shard-1", "tomb:001", bytes("alive"));
        manager.commit(seed);

        Transaction deleteTx = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.delete(deleteTx, "shard-1", "tomb:001");
        manager.commit(deleteTx);

        assertEquals(2, countVersionsForKey(shard1DataSource, "tomb:001"));

        int reclaimed = gcManager.gcOnce(100);

        assertTrue(reclaimed >= 2);
        assertEquals(0, countVersionsForKey(shard1DataSource, "tomb:001"));
        Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
        assertTrue(manager.get(reader, "shard-1", "tomb:001").isEmpty());
    }

    /** 验证后台调度实际完成参与者恢复和版本垃圾回收。 */
    @Test
    void schedulerShouldAutomaticallyRunRecoveryAndGc() throws Exception {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);

        TransactionManager writerManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)
        ));
        TransactionManager readerManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        BackgroundRecoveryRunner recoveryRunner = new BackgroundRecoveryRunner(recoveryManager, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        Transaction tx = writerManager.begin(IsolationLevel.REPEATABLE_READ);
        writerManager.put(tx, "shard-1", "sched:001", bytes("v1"));
        writerManager.put(tx, "shard-2", "sched:002", bytes("v2"));
        assertThrows(MvccException.class, () -> writerManager.commit(tx));

        try (RecoveryScheduler scheduler = new RecoveryScheduler(recoveryRunner, gcManager, 100, 100, 50, 50)) {
            scheduler.start();
            waitUntil(() -> {
                if (scheduler.recoveryStats().runCount() == 0 || scheduler.gcStats().runCount() == 0
                        || shard2.participantRecord(tx.txId())
                        .filter(record -> record.status() == com.flyingzc.azir.common.TxStatus.COMMITTED).isEmpty()
                        || shard2.findPendingTransactionIds(100).contains(tx.txId())) {
                    return false;
                }
                Transaction readerTx = readerManager.begin(IsolationLevel.READ_COMMITTED);
                try {
                    return "v2".equals(readString(readerManager, readerTx, "shard-2", "sched:002"));
                } finally {
                    readerManager.rollback(readerTx);
                }
            }, 3000);
            assertTrue(scheduler.recoveryStats().runCount() > 0);
            assertTrue(scheduler.gcStats().runCount() > 0);
        }

        Transaction seed1 = readerManager.begin(IsolationLevel.READ_COMMITTED);
        readerManager.put(seed1, "shard-1", "sched-gc:001", bytes("a"));
        readerManager.commit(seed1);
        Transaction seed2 = readerManager.begin(IsolationLevel.READ_COMMITTED);
        readerManager.put(seed2, "shard-1", "sched-gc:001", bytes("b"));
        readerManager.commit(seed2);

        try (RecoveryScheduler scheduler = new RecoveryScheduler(recoveryRunner, gcManager, 100, 100, 50, 50)) {
            scheduler.start();
            waitUntil(() -> {
                try {
                    return scheduler.gcStats().runCount() > 0
                            && safePointStore.currentSafePoint() > 0
                            && countVersionsForKey(shard1DataSource, "sched-gc:001") == 1;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }, 3000);
        }
    }

    /** 读取当前事务快照中的字符串值。 */
    private static String readString(TransactionManager manager, Transaction tx, String shardId, String key) {
        return manager.get(tx, shardId, key).map(String::new).orElse(null);
    }

    /** 按 UTF-8 编码测试字符串。 */
    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** 清理用例协议表，调用者随后恢复本地水位单例。 */
    private static void truncate(DataSource dataSource, String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    /** 查询指定测试表的总行数。 */
    private static long countRows(DataSource dataSource, String table) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select count(*) from " + table)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    /** 统计指定业务键的物理版本数。 */
    private static long countVersionsForKey(DataSource dataSource, String key) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement("select count(*) from mvcc_data where user_key = ?");
        ) {
            statement.setBytes(1, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    /** 在有限期限内轮询异步测试条件。 */
    private static void waitUntil(Check check, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("condition not met within timeout");
    }

    /** 异步测试的可抛异常检查条件。 */
    @FunctionalInterface
    private interface Check {
        /** 检查当前异步条件是否已经满足。 */
        boolean ok() throws Exception;
    }

    /** 在测试容器中创建数据库并授予测试账号权限。 */
    private static void createDatabase(String databaseName) throws Exception {
        MYSQL.execInContainer(
                "mysql",
                "-uroot",
                "-p" + MYSQL.getPassword(),
                "-e",
                "create database if not exists `" + databaseName + "`; " +
                        "grant all privileges on `" + databaseName + "`.* to '" + MYSQL.getUsername() + "'@'%'; " +
                        "flush privileges;"
        );
    }

    /** 生成指定测试数据库的连接地址。 */
    private static String databaseUrl(String databaseName) {
        return MYSQL.getJdbcUrl().replace("/bootstrap", "/" + databaseName);
    }

    /** 关闭实现可关闭接口的数据源。 */
    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /** 首次提交调用在真实落盘前失败，后续调用保留真实参与者终态语义。 */
    private static final class FailOnceOnCommitStorageAdapter extends DelegatingStorageAdapter {
        private final AtomicBoolean shouldFail = new AtomicBoolean(true);

        /** 构造提交故障包装器。 */
        private FailOnceOnCommitStorageAdapter(StorageAdapter delegate) {
            super(delegate);
        }

        /** 按事务编号注入一次提交故障。 */
        @Override
        public void commit(long txId, long commitTs) {
            failOnce();
            super.commit(txId, commitTs);
        }

        /** 按写集合注入一次提交故障。 */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations) {
            failOnce();
            super.commit(txId, commitTs, mutations);
        }

        /** 保留清理策略并注入一次提交故障。 */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
            failOnce();
            super.commit(txId, commitTs, mutations, cleanupIntents);
        }

        /** 直接提交仍调用真实原子实现，失败时不伪造本地提交记录。 */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            failOnce();
            super.directCommit(txId, startTs, commitTs, mutations);
        }

        /** 在所有提交重载间共享一次性故障。 */
        private void failOnce() {
            if (shouldFail.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated commit failure");
            }
        }
    }
}

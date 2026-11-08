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

package com.flyingzc.azir.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

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

/**
 * Verify durable control, coordinator restart, snapshot lease and garbage collection.
 */
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

    /**
     * Creates a quarantine test database and initializes the protocol table.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * Closes the data source used for testing.
     *
     * @throws Exception if the operation cannot complete
     */
    @AfterAll
    static void afterAll() throws Exception {
        close(controlDataSource);
        close(shard1DataSource);
        close(shard2DataSource);
    }

    /**
     * Clears the protocol state and creates an independent adaptor for current use.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * After restarting verify, use the new adapter to read durable Center decision and complete recovery.
     */
    @Test
    void restartedCoordinatorShouldRecoverCommittedTransactionFromMysqlMetadata() {
        MysqlTimestampOracle tso1 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore1 = new MysqlTransactionMetadataStore(controlDataSource);
        TransactionManager manager1 = new TransactionManager(tso1, metadataStore1, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)));

        Transaction tx = manager1.begin(IsolationLevel.REPEATABLE_READ);
        manager1.put(tx, "shard-1", "acct:1", bytes("100"));
        manager1.put(tx, "shard-2", "acct:2", bytes("200"));

        MvccException error = assertThrows(MvccException.class, () -> manager1.commit(tx));
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
        // Rebuilds the adaptor when simulated to restart without tying up old examples of coordinator use.
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore2);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore2);
        TransactionManager manager2 = new TransactionManager(tso2, metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

        recoveryManager.recover(tx.txId());

        Transaction reader = manager2.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("100", readString(manager2, reader, "shard-1", "acct:1"));
        Assertions.assertEquals("200", readString(manager2, reader, "shard-2", "acct:2"));

        long nextTs = tso2.nextTimestamp();
        assertTrue(nextTs > tx.txId());
    }

    /**
     * Commit routine durable storage decision has been lost, but independent coordinator must replace materialization with the original committeeTs.
     */
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
                    } catch (final InvocationTargetException ex) {
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
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> coordinator.commit(tx)).errorCode());
        var persisted = backing.get(tx.txId()).orElseThrow();
        Assertions.assertEquals(TxStatus.COMMITTED, persisted.status());
        long commitTs = persisted.commitTs();

        var restartedStore = new MysqlTransactionMetadataStore(controlDataSource);
        var recoveredFirst = new MysqlStorageAdapter("shard-1", shard1DataSource, restartedStore);
        var recoveredSecond = new MysqlStorageAdapter("shard-2", shard2DataSource, restartedStore);
        new RecoveryManager(restartedStore, Map.of(recoveredFirst.shardId(), recoveredFirst,
                recoveredSecond.shardId(), recoveredSecond)).recover(tx.txId());
        Assertions.assertEquals(commitTs, restartedStore.get(tx.txId()).orElseThrow().commitTs().longValue());
        try (
                var reader = new TransactionManager(new MysqlTimestampOracle(controlDataSource), restartedStore,
                        Map.of(recoveredFirst.shardId(), recoveredFirst, recoveredSecond.shardId(), recoveredSecond))) {
            Transaction snapshot = reader.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals("one", readString(reader, snapshot, "shard-1", "lost:one"));
            Assertions.assertEquals("two", readString(reader, snapshot, "shard-2", "lost:two"));
            reader.commit(snapshot);
        }
        coordinator.close();
    }

    /**
     * Verify omitted transaction for visible preparation can still be recovery after the coordinator restart.
     */
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
                        false),
                Map.of(
                        shard1.shardId(), shard1,
                        shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)));

        Transaction tx = manager1.begin(IsolationLevel.REPEATABLE_READ);
        manager1.put(tx, "shard-1", "acct:no-prepare:1", bytes("100"));
        manager1.put(tx, "shard-2", "acct:no-prepare:2", bytes("200"));

        MvccException error = assertThrows(MvccException.class, () -> manager1.commit(tx));
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
        // Rebuilds the adaptor when simulated to restart without tying up old examples of coordinator use.
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore2);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore2);
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

        recoveryManager.recover(tx.txId());

        TransactionManager manager2 = new TransactionManager(tso2, metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        Transaction reader = manager2.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("100", readString(manager2, reader, "shard-1", "acct:no-prepare:1"));
        Assertions.assertEquals("200", readString(manager2, reader, "shard-2", "acct:no-prepare:2"));
    }

    /**
     * Verify background scans can detect residual intent and complete once transaction recovery.
     */
    @Test
    void backgroundRecoveryRunnerShouldScanLingeringIntentsAndRecover() {
        MysqlTimestampOracle tso1 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore1 = new MysqlTransactionMetadataStore(controlDataSource);
        TransactionManager manager1 = new TransactionManager(tso1, metadataStore1, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)));

        Transaction tx = manager1.begin(IsolationLevel.REPEATABLE_READ);
        manager1.put(tx, "shard-1", "order:1", bytes("created"));
        manager1.put(tx, "shard-2", "stock:1", bytes("reserved"));
        assertThrows(MvccException.class, () -> manager1.commit(tx));

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
        // Rebuilds the adaptor when simulated to restart without tying up old examples of coordinator use.
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore2);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore2);
        TransactionManager manager2 = new TransactionManager(tso2, metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore2, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        BackgroundRecoveryRunner runner = new BackgroundRecoveryRunner(recoveryManager, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

        int recovered = runner.recoverOnce(100);

        Assertions.assertEquals(1, recovered);
        Transaction reader = manager2.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("created", readString(manager2, reader, "shard-1", "order:1"));
        Assertions.assertEquals("reserved", readString(manager2, reader, "shard-2", "stock:1"));
    }

    /**
     * Verify Active snapshot prevents watermark propulsion and can be collection old version after completion.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void safePointShouldBeBlockedByOldestActiveTransactionAndGcShouldReclaimOldVersions() throws Exception {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        TransactionManager manager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

        Transaction seed1 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed1, "shard-1", "gc:001", bytes("v1"));
        manager.commit(seed1);

        Transaction longTx = manager.begin(IsolationLevel.REPEATABLE_READ);
        Assertions.assertEquals("v1", readString(manager, longTx, "shard-1", "gc:001"));

        Transaction seed2 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed2, "shard-1", "gc:001", bytes("v2"));
        manager.commit(seed2);

        long safePoint = gcManager.advanceSafePoint();
        Assertions.assertEquals(longTx.startTs() - 1, safePoint);

        int reclaimedWhileBlocked = gcManager.gcOnce(100);
        Assertions.assertEquals(0, reclaimedWhileBlocked);
        Assertions.assertEquals(2, countRows(shard1DataSource, "mvcc_data"));

        manager.rollback(longTx);

        int reclaimed = gcManager.gcOnce(100);
        assertTrue(reclaimed >= 1);
        Assertions.assertEquals(1, countRows(shard1DataSource, "mvcc_data"));
    }

    /**
     * Verify real transaction snapshot tested to push watermark over and refuse to read and scan.
     */
    @Test
    void readBelowSafePointShouldReturnSnapshotTooOld() {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        TransactionManager manager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        Transaction seed1 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed1, "shard-1", "gc:002", bytes("v1"));
        manager.commit(seed1);

        Transaction seed2 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed2, "shard-1", "gc:002", bytes("v2"));
        manager.commit(seed2);

        Transaction oldSnapshot = manager.begin(IsolationLevel.REPEATABLE_READ);
        Assertions.assertEquals("v2", readString(manager, oldSnapshot, "shard-1", "gc:002"));
        Assertions.assertEquals(oldSnapshot.deadlineMillis(), metadataStore.snapshotExpiry(oldSnapshot.txId()).orElseThrow().longValue());
        // Only snapshot below watermark is rejected in the test by passing real active snapshot, the verify reading entrance.
        safePointStore.advanceTo(oldSnapshot.startTs() + 1);
        MvccException error = assertThrows(MvccException.class, () -> manager.get(oldSnapshot, "shard-1", "gc:002"));
        Assertions.assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, error.errorCode());
        MvccException scanError = assertThrows(MvccException.class,
                () -> manager.scan(oldSnapshot, "shard-1", "gc:000", "gc:999"));
        Assertions.assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, scanError.errorCode());
        manager.rollback(oldSnapshot);
    }

    /**
     * Full collection tombstone chain after verify safe point and missing values.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcShouldReclaimTombstoneChainAfterSafePoint() throws Exception {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        TransactionManager manager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

        Transaction seed = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed, "shard-1", "tomb:001", bytes("alive"));
        manager.commit(seed);

        Transaction deleteTx = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.delete(deleteTx, "shard-1", "tomb:001");
        manager.commit(deleteTx);

        Assertions.assertEquals(2, countVersionsForKey(shard1DataSource, "tomb:001"));

        int reclaimed = gcManager.gcOnce(100);

        assertTrue(reclaimed >= 2);
        Assertions.assertEquals(0, countVersionsForKey(shard1DataSource, "tomb:001"));
        Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
        assertTrue(manager.get(reader, "shard-1", "tomb:001").isEmpty());
    }

    /**
     * Verify background schedule actually completed participant recovery and version garbage collection.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void schedulerShouldAutomaticallyRunRecoveryAndGc() throws Exception {
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        SafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);

        TransactionManager writerManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)));
        TransactionManager readerManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        RecoveryManager recoveryManager = new RecoveryManager(metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        BackgroundRecoveryRunner recoveryRunner = new BackgroundRecoveryRunner(recoveryManager, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

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
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
            }, 3000);
        }
    }

    /**
     * Reads the current string value in transaction snapshot.
     *
     * @param manager manager
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    private static String readString(final TransactionManager manager, final Transaction tx, final String shardId, final String key) {
        return manager.get(tx, shardId, key).map(String::new).orElse(null);
    }

    /**
     * Tests the string by UTF-8 encoding.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Clears the case protocol list, and the caller is then recovery local watermark.
     *
     * @param dataSource database connection source
     * @param tables tables
     * @throws Exception if the operation cannot complete
     */
    private static void truncate(final DataSource dataSource, final String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    /**
     * Query to specify the total number of lines for the test sheet.
     *
     * @param dataSource database connection source
     * @param table table
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long countRows(final DataSource dataSource, final String table) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select count(*) from " + table)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    /**
     * Statistics specify the physical version number of key(s).
     *
     * @param dataSource database connection source
     * @param key business key
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long countVersionsForKey(final DataSource dataSource, final String key) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                var statement = connection.prepareStatement("select count(*) from mvcc_data where user_key = ?");) {
            statement.setBytes(1, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    /**
     * Interrogate the conditions of the walk test for a limited period.
     *
     * @param check check
     * @param timeoutMillis timeout millis
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static void waitUntil(final Check check, final long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("condition not met within timeout");
    }

    /**
     * Creates database in the test container and gives test account privileges.
     *
     * @param databaseName database name
     * @throws Exception if the operation cannot complete
     */
    private static void createDatabase(final String databaseName) throws Exception {
        MYSQL.execInContainer(
                "mysql",
                "-uroot",
                "-p" + MYSQL.getPassword(),
                "-e",
                "create database if not exists `" + databaseName + "`; " +
                        "grant all privileges on `" + databaseName + "`.* to '" + MYSQL.getUsername() + "'@'%'; " +
                        "flush privileges;");
    }

    /**
     * Generates the connection address of the specified test database.
     *
     * @param databaseName database name
     * @return the operation result
     */
    private static String databaseUrl(final String databaseName) {
        return MYSQL.getJdbcUrl().replace("/bootstrap", "/" + databaseName);
    }

    /**
     * Closes the data source to close interface.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void close(final DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * Discretionary check conditions for staggered tests.
     */
    @FunctionalInterface
    private interface Check {

        /**
         * Checks whether the current walk conditions have been met.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        boolean ok() throws Exception;
    }

    /**
     * The first commit call failed before the real dropout, followed by a call to retain the true participant terminal state syntax.
     */
    private static final class FailOnceOnCommitStorageAdapter extends DelegatingStorageAdapter {

        private final AtomicBoolean shouldFail = new AtomicBoolean(true);

        /**
         * Construct commit failure packaging.
         *
         * @param delegate delegate
         */
        private FailOnceOnCommitStorageAdapter(final StorageAdapter delegate) {
            super(delegate);
        }

        /**
         * Press transaction identifier to inject commit failure.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            failOnce();
            super.commit(txId, commitTs);
        }

        /**
         * Press write set to inject commit failure.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations) {
            failOnce();
            super.commit(txId, commitTs, mutations);
        }

        /**
         * Retain the cleanup policy and inject it once.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            failOnce();
            super.commit(txId, commitTs, mutations, cleanupIntents);
        }

        /**
         * Direct commit still calls real atomic and does not falsify local commit records when it fails.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            failOnce();
            super.directCommit(txId, startTs, commitTs, mutations);
        }

        /**
         * Share one-time failure between all commit loads.
         *
         * @throws IllegalStateException if the operation cannot complete
         */
        private void failOnce() {
            if (shouldFail.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated commit failure");
            }
        }
    }
}

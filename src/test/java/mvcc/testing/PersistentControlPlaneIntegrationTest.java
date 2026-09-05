package mvcc.testing;

import mvcc.api.Transaction;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.gc.SafePointStore;
import mvcc.gc.mysql.MysqlSafePointStore;
import mvcc.gc.GcManager;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.recovery.RecoveryScheduler;
import mvcc.recovery.RecoveryManager;
import mvcc.recovery.BackgroundRecoveryRunner;
import mvcc.storage.KeyRange;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.TimestampOracle;
import mvcc.tso.mysql.MysqlTimestampOracle;
import mvcc.txn.TransactionManager;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    private static MysqlStorageAdapter shard1;
    private static MysqlStorageAdapter shard2;

    @BeforeAll
    static void beforeAll() throws Exception {
        createDatabase("control");
        createDatabase("shard_1");
        createDatabase("shard_2");
        controlDataSource = MysqlDataSourceFactory.create(databaseUrl("control"), MYSQL.getUsername(), MYSQL.getPassword());
        shard1DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_1"), MYSQL.getUsername(), MYSQL.getPassword());
        shard2DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_2"), MYSQL.getUsername(), MYSQL.getPassword());
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);
        shard1.initialize();
        shard2.initialize();
        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        tso.initialize(100);
        MysqlTransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        metadataStore.initialize();
        MysqlSafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        safePointStore.initialize(0);
    }

    @AfterAll
    static void afterAll() throws Exception {
        close(controlDataSource);
        close(shard1DataSource);
        close(shard2DataSource);
    }

    @BeforeEach
    void setUp() throws Exception {
        truncate(controlDataSource, "mvcc_txn", "mvcc_tso", "mvcc_safe_point");
        truncate(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
        truncate(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");

        MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
        tso.initialize(100);
        MysqlTransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
        metadataStore.initialize();
        MysqlSafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
        safePointStore.initialize(0);
    }

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
        assertTrue(error.retryable());

        MysqlTimestampOracle tso2 = new MysqlTimestampOracle(controlDataSource);
        TransactionMetadataStore metadataStore2 = new MysqlTransactionMetadataStore(controlDataSource);
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

    @Test
    void readBelowSafePointShouldReturnSnapshotTooOld() {
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
        manager.put(seed1, "shard-1", "gc:002", bytes("v1"));
        manager.commit(seed1);

        Transaction seed2 = manager.begin(IsolationLevel.READ_COMMITTED);
        manager.put(seed2, "shard-1", "gc:002", bytes("v2"));
        manager.commit(seed2);

        gcManager.gcOnce(100);

        Transaction oldSnapshot = new Transaction(1L, 1L, IsolationLevel.REPEATABLE_READ);
        MvccException error = assertThrows(MvccException.class, () -> manager.get(oldSnapshot, "shard-1", "gc:002"));
        assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, error.errorCode());
    }

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

    private static String readString(TransactionManager manager, Transaction tx, String shardId, String key) {
        return manager.get(tx, shardId, key).map(String::new).orElse(null);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void truncate(DataSource dataSource, String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    private static long countRows(DataSource dataSource, String table) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select count(*) from " + table)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

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

    @FunctionalInterface
    private interface Check {
        boolean ok() throws Exception;
    }

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

    private static String databaseUrl(String databaseName) {
        return MYSQL.getJdbcUrl().replace("/bootstrap", "/" + databaseName);
    }

    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private static final class FailOnceOnCommitStorageAdapter implements StorageAdapter {
        private final StorageAdapter delegate;
        private final AtomicBoolean shouldFail = new AtomicBoolean(true);

        private FailOnceOnCommitStorageAdapter(StorageAdapter delegate) {
            this.delegate = delegate;
        }

        @Override
        public String shardId() {
            return delegate.shardId();
        }

        @Override
        public void initialize() {
            delegate.initialize();
        }

        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) {
            return delegate.get(context, key);
        }

        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            return delegate.scan(context, keyRange);
        }

        @Override
        public void prewrite(long txId, long startTs, java.util.List<mvcc.storage.Mutation> mutations) {
            delegate.prewrite(txId, startTs, mutations);
        }

        @Override
        public void prepare(long txId) {
            delegate.prepare(txId);
        }

        @Override
        public void commit(long txId, long commitTs) {
            if (shouldFail.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated coordinator crash after metadata commit");
            }
            delegate.commit(txId, commitTs);
        }

        @Override
        public void abort(long txId) {
            delegate.abort(txId);
        }

        @Override
        public Set<Long> findPendingTransactionIds(int limit) {
            return delegate.findPendingTransactionIds(limit);
        }

        @Override
        public int gc(long safePoint, int limit) {
            return delegate.gc(safePoint, limit);
        }
    }
}

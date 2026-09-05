package mvcc.testing;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitMode;
import mvcc.api.TransactionCommitResult;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.recovery.RecoveryManager;
import mvcc.storage.ReadContext;
import mvcc.storage.KeyRange;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.AtomicTimestampOracle;
import mvcc.tso.TimestampOracle;
import mvcc.txn.ConflictMode;
import mvcc.txn.CrossShardCommitPolicy;
import mvcc.txn.LockWaitPolicy;
import mvcc.txn.TransactionManager;
import mvcc.txn.TransactionOptions;
import mvcc.txn.TransactionRetryPolicy;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class MysqlMvccIntegrationTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withUsername("test")
            .withPassword("test")
            .withDatabaseName("bootstrap");

    private static DataSource shard1DataSource;
    private static DataSource shard2DataSource;
    private static MysqlStorageAdapter shard1;
    private static MysqlStorageAdapter shard2;

    private TimestampOracle tso;
    private TransactionMetadataStore metadataStore;
    private TransactionManager transactionManager;
    private RecoveryManager recoveryManager;

    @BeforeAll
    static void beforeAll() throws Exception {
        createDatabase("shard_1");
        createDatabase("shard_2");
        shard1DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_1"), MYSQL.getUsername(), MYSQL.getPassword());
        shard2DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_2"), MYSQL.getUsername(), MYSQL.getPassword());
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);
        shard1.initialize();
        shard2.initialize();
    }

    @AfterAll
    static void afterAll() throws Exception {
        close(shard1DataSource);
        close(shard2DataSource);
    }

    @BeforeEach
    void setUp() throws Exception {
        truncate(shard1DataSource);
        truncate(shard2DataSource);
        tso = new AtomicTimestampOracle(100);
        metadataStore = new InMemoryTransactionMetadataStore();
        transactionManager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        recoveryManager = new RecoveryManager(metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
    }

    @Test
    void readCommittedShouldObserveLatestCommittedVersionAcrossStatements() {
        seedCommittedValue("shard-1", "account:1", "v1");

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("v1", readString(reader, "shard-1", "account:1"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "account:1", bytes("v2"));
        transactionManager.commit(writer);

        assertEquals("v2", readString(reader, "shard-1", "account:1"));
    }

    @Test
    void repeatableReadShouldKeepStartSnapshot() {
        seedCommittedValue("shard-1", "account:1", "v1");

        Transaction reader = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        assertEquals("v1", readString(reader, "shard-1", "account:1"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "account:1", bytes("v2"));
        transactionManager.commit(writer);

        assertEquals("v1", readString(reader, "shard-1", "account:1"));
    }

    @Test
    void shouldReadOwnIntentAndSkipOtherTransactionIntent() {
        seedCommittedValue("shard-1", "account:1", "base");

        Transaction tx1 = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx1, "shard-1", "account:1", bytes("tx1"));
        assertEquals("tx1", readString(tx1, "shard-1", "account:1"));

        Transaction tx2 = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("base", readString(tx2, "shard-1", "account:1"));
    }

    @Test
    void crossShardCommitShouldUseTwoPhaseCommit() {
        Transaction tx = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx, "shard-1", "order:1", bytes("created"));
        transactionManager.put(tx, "shard-2", "stock:1", bytes("reserved"));

        TransactionCommitResult result = transactionManager.commit(tx);

        assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, result.mode());
        assertEquals("created", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "order:1"));
        assertEquals("reserved", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "stock:1"));
    }

    @Test
    void repeatableReadScanShouldKeepSnapshotAcrossStatements() {
        seedCommittedValue("shard-1", "item:001", "a");
        seedCommittedValue("shard-1", "item:002", "b");

        Transaction reader = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        assertEquals(List.of("item:001", "item:002"), scanKeys(reader, "shard-1", "item:000", "item:999"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "item:003", bytes("c"));
        transactionManager.commit(writer);

        assertEquals(List.of("item:001", "item:002"), scanKeys(reader, "shard-1", "item:000", "item:999"));
    }

    @Test
    void readCommittedScanShouldSeeLatestCommittedAndOwnIntent() {
        seedCommittedValue("shard-1", "scan:001", "v1");

        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, "shard-1", "scan:002", bytes("v2"));
        assertEquals(List.of("scan:001", "scan:002"), scanKeys(tx, "shard-1", "scan:000", "scan:999"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "scan:003", bytes("v3"));
        transactionManager.commit(writer);

        assertEquals(List.of("scan:001", "scan:002", "scan:003"), scanKeys(tx, "shard-1", "scan:000", "scan:999"));
    }

    @Test
    void scanShouldSkipOtherTransactionIntentAndRespectDeleteTombstone() {
        seedCommittedValue("shard-1", "range:001", "a");
        seedCommittedValue("shard-1", "range:002", "b");
        seedCommittedValue("shard-1", "range:003", "c");

        Transaction pendingWriter = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(pendingWriter, "shard-1", "range:002", bytes("bb"));

        Transaction deletingWriter = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.delete(deletingWriter, "shard-1", "range:003");
        transactionManager.commit(deletingWriter);

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals(List.of("range:001", "range:002"), scanKeys(reader, "shard-1", "range:000", "range:999"));
    }

    @Test
    void recoveryShouldFinishCommittedTransactionAfterPartialShardFailure() {
        StorageAdapter flakyShard2 = new FailOnceOnCommitStorageAdapter(shard2);
        TransactionManager flakyManager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), flakyShard2
        ));

        Transaction tx = flakyManager.begin(IsolationLevel.REPEATABLE_READ);
        flakyManager.put(tx, "shard-1", "user:1", bytes("alice"));
        flakyManager.put(tx, "shard-2", "ledger:1", bytes("debit"));

        MvccException error = assertThrows(MvccException.class, () -> flakyManager.commit(tx));
        assertTrue(error.retryable());
        assertEquals("alice", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "user:1"));
        assertTrue(transactionManager.get(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "ledger:1").isEmpty());

        recoveryManager.recover(tx.txId());

        assertEquals("debit", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "ledger:1"));
    }

    @Test
    void rollbackShouldBeIdempotentAndPersistRollbackMarker() throws Exception {
        Transaction tx = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx, "shard-1", "account:9", bytes("pending"));

        transactionManager.rollback(tx);
        transactionManager.rollback(tx);

        assertEquals(1, countRows(shard1DataSource, "mvcc_rollback"));
        assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        assertTrue(transactionManager.get(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "account:9").isEmpty());
    }

    @Test
    void executeShouldWaitThenRetryAfterForeignIntentCommits() throws Exception {
        TransactionManager waitAndRetryManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        new LockWaitPolicy(ConflictMode.WAIT, 2_000L, 5L, 20L, 2.0d, false),
                        new TransactionRetryPolicy(3, 5L, 20L, 2.0d, false),
                        CrossShardCommitPolicy.serial()
                ),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        Transaction holder = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(holder, "shard-1", "wait:001", bytes("holder"));

        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            Future<Long> elapsedFuture = executor.submit(() -> {
                long startedAt = System.nanoTime();
                waitAndRetryManager.execute(IsolationLevel.REPEATABLE_READ, waiter -> {
                    started.countDown();
                    waitAndRetryManager.put(waiter, "shard-1", "wait:001", bytes("waiter"));
                    return null;
                });
                return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            });

            assertTrue(started.await(1, TimeUnit.SECONDS));
            Thread.sleep(150L);
            transactionManager.commit(holder);

            long elapsedMillis = getFuture(elapsedFuture);
            assertTrue(elapsedMillis >= 100L, "expected waiter to observe lock wait, elapsed=" + elapsedMillis);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("waiter", readString(reader, "shard-1", "wait:001"));
    }

    @Test
    void executeShouldRetryWholeTransactionAfterWriteConflict() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        TransactionManager retryingManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        new TransactionRetryPolicy(3, 5L, 20L, 2.0d, false),
                        CrossShardCommitPolicy.serial()
                ),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        String result = retryingManager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
            if (attempts.incrementAndGet() == 1) {
                Transaction concurrentWriter = transactionManager.begin(IsolationLevel.READ_COMMITTED);
                transactionManager.put(concurrentWriter, "shard-1", "retry:001", bytes("first"));
                transactionManager.commit(concurrentWriter);
            }
            retryingManager.put(tx, "shard-1", "retry:001", bytes("second"));
            return "second";
        });

        assertEquals("second", result);
        assertEquals(2, attempts.get());
        assertEquals(2, countVersionsForKey(shard1DataSource, "retry:001"));
        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("second", readString(reader, "shard-1", "retry:001"));
    }

    private void seedCommittedValue(String shardId, String key, String value) {
        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, shardId, key, bytes(value));
        transactionManager.commit(tx);
    }

    private String readString(Transaction tx, String shardId, String key) {
        return transactionManager.get(tx, shardId, key)
                .map(String::new)
                .orElse(null);
    }

    private List<String> scanKeys(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive) {
        return transactionManager.scan(tx, shardId, startKeyInclusive, endKeyExclusive)
                .stream()
                .map(VersionedValue::key)
                .collect(Collectors.toList());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static <T> T getFuture(Future<T> future) throws Exception {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw new RuntimeException(ex.getCause());
        }
    }

    private static void truncate(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("delete from mvcc_intent");
            statement.execute("delete from mvcc_data");
            statement.execute("delete from mvcc_rollback");
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
             var statement = connection.prepareStatement("select count(*) from mvcc_data where user_key = ?")) {
            statement.setBytes(1, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
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
                throw new IllegalStateException("simulated commit failure");
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

package mvcc.testing;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitMode;
import mvcc.api.TransactionCommitResult;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.recovery.RecoveryManager;
import mvcc.sql.AzirSqlEngine;
import mvcc.sql.ShardResolver;
import mvcc.sql.SqlExecutionResult;
import mvcc.sql.SqlSession;
import mvcc.sql.SqlValueCodec;
import mvcc.storage.ReadContext;
import mvcc.storage.Mutation;
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
import mvcc.txn.TransactionOptionsPresets;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
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

/** 使用真实 MySQL 分片验证事务协议、快照可见性和故障恢复。 */
@Testcontainers
class MysqlMvccIntegrationTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withUsername("test")
            .withPassword("test")
            .withDatabaseName("bootstrap");

    private static DataSource shard1DataSource;
    private static DataSource shard2DataSource;
    private MysqlStorageAdapter shard1;
    private MysqlStorageAdapter shard2;

    private TimestampOracle tso;
    private TransactionMetadataStore metadataStore;
    private TransactionManager transactionManager;
    private RecoveryManager recoveryManager;

    /** 创建隔离测试数据库并初始化协议表。 */
    @BeforeAll
    static void beforeAll() throws Exception {
        createDatabase("shard_1");
        createDatabase("shard_2");
        shard1DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_1"), MYSQL.getUsername(), MYSQL.getPassword());
        shard2DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_2"), MYSQL.getUsername(), MYSQL.getPassword());
        new MysqlStorageAdapter("shard-1", shard1DataSource).initialize();
        new MysqlStorageAdapter("shard-2", shard2DataSource).initialize();
    }

    /** 关闭测试使用的数据源。 */
    @AfterAll
    static void afterAll() throws Exception {
        close(shard1DataSource);
        close(shard2DataSource);
    }

    /** 清理协议状态并为当前用例创建独立适配器。 */
    @BeforeEach
    void setUp() throws Exception {
        truncate(shard1DataSource);
        truncate(shard2DataSource);
        tso = new AtomicTimestampOracle(100);
        metadataStore = new InMemoryTransactionMetadataStore();
        // 每个用例使用新适配器，避免向上一用例已绑定的适配器复绑事务中心。
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);
        shard1.initialize();
        shard2.initialize();
        transactionManager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
        recoveryManager = new RecoveryManager(metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));
    }

    /** 验证读已提交在不同语句间观察最新提交版本。 */
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

    /**
     * 验证 Calcite 复杂 SQL 通过真实 MySQL MVCC 适配器完成跨分片 DML、JOIN 和聚合。
     */
    @Test
    void calciteSqlShouldExecuteAgainstMysqlMvccShards() {
        AzirSqlEngine sqlEngine = AzirSqlEngine.calcite(
                transactionManager,
                ShardResolver.hashByKey(List.of(shard1.shardId(), shard2.shardId())),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED
        );

        try (SqlSession session = sqlEngine.openSession()) {
            session.execute("insert into account (id, value) values ('a', 'gold'), ('b', 'silver'), ('c', 'gold')");
            session.execute("insert into profile (id, value) values ('a', 'active'), ('b', 'inactive'), ('c', 'active')");
            session.execute("update account set value = upper(value) where id = 'b'");

            SqlExecutionResult result = session.execute("""
                    select a.value as category, count(*) as total
                    from account a join profile p on a.id = p.id
                    where p.value = 'active'
                    group by a.value
                    having count(*) >= 2
                    order by total desc
                    """);

            assertEquals(1, result.rows().size());
            assertEquals("gold", result.singleRow().get("category"));
            assertEquals("2", result.singleRow().get("total"));
            assertEquals(1, session.execute("delete from account where value = 'SILVER'").updateCount());
            assertEquals(2, session.execute("select * from account").rows().size());
        }
    }

    /** 验证可重复读保留事务开始快照。 */
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

    /** 验证读己之写并隔离其他事务的未决写入。 */
    @Test
    void shouldReadOwnIntentAndSkipOtherTransactionIntent() {
        seedCommittedValue("shard-1", "account:1", "base");

        Transaction tx1 = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx1, "shard-1", "account:1", bytes("tx1"));
        assertEquals("tx1", readString(tx1, "shard-1", "account:1"));

        Transaction tx2 = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("base", readString(tx2, "shard-1", "account:1"));
    }

    /** 验证缓冲写提交前的点读和范围读可见性。 */
    @Test
    void bufferedWritesShouldSupportReadYourOwnWriteBeforeCommit() throws Exception {
        TransactionManager bufferedManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        TransactionRetryPolicy.noRetry(),
                        CrossShardCommitPolicy.parallel(),
                        true,
                        false,
                        false
                ),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        bufferedManager.put(tx, "shard-1", "buffered:001", bytes("v1"));
        bufferedManager.put(tx, "shard-1", "buffered:002", bytes("v2"));

        assertEquals("v1", bufferedManager.get(tx, "shard-1", "buffered:001").map(String::new).orElse(null));
        assertEquals(
                List.of("buffered:001", "buffered:002"),
                bufferedManager.scan(tx, "shard-1", "buffered:000", "buffered:999")
                        .stream()
                        .map(VersionedValue::key)
                        .collect(Collectors.toList())
        );
        assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
    }

    /** 验证跨分片缓冲写在提交时物化并清理意向。 */
    @Test
    void bufferedWritesShouldFlushAtCommit() throws Exception {
        TransactionManager bufferedManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        TransactionRetryPolicy.noRetry(),
                        CrossShardCommitPolicy.parallel(),
                        true,
                        false,
                        false
                ),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        bufferedManager.put(tx, "shard-1", "buffered:commit:1", bytes("left"));
        bufferedManager.put(tx, "shard-2", "buffered:commit:2", bytes("right"));

        bufferedManager.commit(tx);

        assertEquals("left", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "buffered:commit:1"));
        assertEquals("right", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "buffered:commit:2"));
        assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        assertEquals(0, countRows(shard2DataSource, "mvcc_intent"));
    }

    /** 验证单分片原子直接提交生成版本且不落地意向。 */
    @Test
    void singleShardBufferedCommitShouldBypassIntentWritePath() throws Exception {
        PublicationCheckingStorageAdapter checkedShard = new PublicationCheckingStorageAdapter(shard1, metadataStore);
        TransactionManager bufferedManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        TransactionRetryPolicy.noRetry(),
                        CrossShardCommitPolicy.parallel(),
                        true,
                        false,
                        false
                ),
                Map.of(shard1.shardId(), checkedShard, shard2.shardId(), shard2)
        );

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        bufferedManager.put(tx, "shard-1", "single:buffered:1", bytes("v1"));
        bufferedManager.put(tx, "shard-1", "single:buffered:2", bytes("v2"));

        bufferedManager.commit(tx);

        assertEquals(0, checkedShard.prewriteCalls);
        assertEquals(1, checkedShard.directCommitCalls);
        assertEquals("v1", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "single:buffered:1"));
        assertEquals("v2", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "single:buffered:2"));
        assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        assertEquals(2, countRows(shard1DataSource, "mvcc_data"));
    }

    /** 验证延迟登记在写提交时保存中心及本地恢复决议。 */
    @Test
    void singleShardBufferedCommitShouldPersistRecoveryMetadataWhenLazyBeginEnabled() {
        PublicationCheckingStorageAdapter checkedShard = new PublicationCheckingStorageAdapter(shard1, metadataStore);
        TransactionManager bufferedManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        TransactionRetryPolicy.noRetry(),
                        CrossShardCommitPolicy.parallel(),
                        true,
                        true,
                        false
                ),
                Map.of(shard1.shardId(), checkedShard, shard2.shardId(), shard2)
        );

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        assertTrue(metadataStore.get(tx.txId()).isEmpty());
        assertEquals(tx.deadlineMillis(), metadataStore.snapshotExpiry(tx.txId()).orElseThrow().longValue());
        bufferedManager.put(tx, "shard-1", "single:lazy-metadata:1", bytes("v1"));
        assertTrue(metadataStore.get(tx.txId()).isEmpty());

        TransactionCommitResult result = bufferedManager.commit(tx);

        assertEquals(0, checkedShard.prewriteCalls);
        assertEquals(1, checkedShard.directCommitCalls);
        var record = metadataStore.get(tx.txId()).orElseThrow();
        var localRecord = shard1.participantRecord(tx.txId()).orElseThrow();
        assertEquals(TxStatus.COMMITTED, record.status());
        assertEquals(Set.of("shard-1"), record.participants());
        assertEquals(result.commitTs(), record.commitTs().longValue());
        assertEquals(TxStatus.COMMITTED, localRecord.status());
        assertEquals(tx.startTs(), localRecord.startTs());
        assertEquals(record.commitTs(), localRecord.commitTs());
        assertTrue(metadataStore.snapshotExpiry(tx.txId()).isEmpty());
        assertEquals("v1", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "single:lazy-metadata:1"));
    }

    /** 验证延迟登记的只读事务仅持有快照租约，提交后释放租约且不创建事务元数据。 */
    @Test
    void lazyReadOnlyTransactionShouldHoldOnlySnapshotLease() {
        TransactionOptions options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.serial(), true, true, false);
        try (TransactionManager manager = new TransactionManager(tso, metadataStore,
                options, Map.of(shard1.shardId(), shard1))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(metadataStore.get(tx.txId()).isEmpty());
            assertEquals(tx.deadlineMillis(), metadataStore.snapshotExpiry(tx.txId()).orElseThrow().longValue());
            assertTrue(manager.get(tx, "shard-1", "readonly:missing").isEmpty());
            manager.commit(tx);
            assertTrue(metadataStore.get(tx.txId()).isEmpty());
            assertTrue(metadataStore.snapshotExpiry(tx.txId()).isEmpty());
        }
    }

    /** 验证延迟登记的立即预写路径在真实意向写入前已经登记中心参与者。 */
    @Test
    void lazyEagerWriteShouldRegisterMetadataBeforePrewrite() {
        PublicationCheckingStorageAdapter checkedShard = new PublicationCheckingStorageAdapter(shard1, metadataStore);
        TransactionOptions options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.serial(), false, true, false);
        try (TransactionManager manager = new TransactionManager(tso, metadataStore, options,
                Map.of(checkedShard.shardId(), checkedShard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(metadataStore.get(tx.txId()).isEmpty());
            manager.put(tx, "shard-1", "single:lazy-prewrite", bytes("v1"));
            assertEquals(1, checkedShard.prewriteCalls);
            TransactionCommitResult result = manager.commit(tx);
            assertEquals(0, checkedShard.directCommitCalls);
            var localRecord = shard1.participantRecord(tx.txId()).orElseThrow();
            assertEquals(TxStatus.COMMITTED, localRecord.status());
            assertEquals(result.commitTs(), localRecord.commitTs().longValue());
            assertEquals(localRecord.commitTs(), metadataStore.get(tx.txId()).orElseThrow().commitTs());
        }
    }

    /** 验证跨分片事务采用两阶段提交模式。 */
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

    /** 验证真实适配器未要求准备时跳过显式准备。 */
    @Test
    void crossShardCommitShouldSkipPrepareForAdaptersThatDoNotRequireIt() {
        StorageAdapter failOnPrepareShard2 = new FailOnPrepareStorageAdapter(shard2);
        TransactionManager manager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), failOnPrepareShard2
        ));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "order:skip-prepare", bytes("created"));
        manager.put(tx, "shard-2", "stock:skip-prepare", bytes("reserved"));

        TransactionCommitResult result = manager.commit(tx);

        assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, result.mode());
        assertEquals("created", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "order:skip-prepare"));
        assertEquals("reserved", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "stock:skip-prepare"));
    }

    /**
     * 验证全局提交决策已落盘后，尚未物化的其他事务 intent 对新读者原子可见。
     */
    @Test
    void committedForeignIntentShouldBeVisibleBeforeMaterialization() throws Exception {
        MysqlStorageAdapter resolvingShard = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore);
        long txId = 201L;
        long startTs = 201L;
        long commitTs = 202L;

        metadataStore.create(txId, startTs, IsolationLevel.REPEATABLE_READ);
        metadataStore.registerParticipants(txId, Set.of("shard-1"));
        resolvingShard.prewrite(txId, startTs, List.of(Mutation.put("async-visible:1", bytes("v1"))));
        metadataStore.markCommitted(txId, commitTs);

        assertEquals(0, countRows(shard1DataSource, "mvcc_data"));
        assertEquals(
                "v1",
                resolvingShard.get(new ReadContext(300L, commitTs, IsolationLevel.READ_COMMITTED), "async-visible:1")
                        .map(value -> new String(value.value()))
                        .orElse(null)
        );
        assertTrue(resolvingShard.get(new ReadContext(301L, commitTs - 1, IsolationLevel.REPEATABLE_READ), "async-visible:1").isEmpty());
    }

    /**
     * 验证安全异步提交在物化完成前返回，并依赖已提交 intent 保持跨分片原子可见。
     */
    @Test
    void asyncCommitShouldReturnAfterCommittedDecisionAndExposeCommittedIntents() throws Exception {
        MysqlStorageAdapter resolvingShard1 = new MysqlStorageAdapter("shard-1", shard1DataSource, metadataStore);
        MysqlStorageAdapter resolvingShard2 = new MysqlStorageAdapter("shard-2", shard2DataSource, metadataStore);
        BlockingCommitStorageAdapter blockedShard1 = new BlockingCommitStorageAdapter(resolvingShard1);
        BlockingCommitStorageAdapter blockedShard2 = new BlockingCommitStorageAdapter(resolvingShard2);
        TransactionManager manager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.mysqlLikeDefaults(),
                        TransactionRetryPolicy.mysqlLikeDefaults(),
                        CrossShardCommitPolicy.asyncParallel(2),
                        true,
                        false,
                        false
                ),
                Map.of(blockedShard1.shardId(), blockedShard1, blockedShard2.shardId(), blockedShard2)
        );

        ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            Future<TransactionCommitResult> future = executor.submit(() -> {
                Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(tx, "shard-1", "async-safe:1", bytes("left"));
                manager.put(tx, "shard-2", "async-safe:2", bytes("right"));
                return manager.commit(tx);
            });

            TransactionCommitResult result = future.get(1, TimeUnit.SECONDS);

            assertEquals(TransactionCommitMode.ASYNC_PRIMARY_COMMIT, result.mode());
            assertEquals(0, countRows(shard1DataSource, "mvcc_data"));
            assertEquals(0, countRows(shard2DataSource, "mvcc_data"));
            assertEquals(
                    "left",
                    resolvingShard1.get(new ReadContext(500L, result.commitTs(), IsolationLevel.READ_COMMITTED), "async-safe:1")
                            .map(value -> new String(value.value()))
                            .orElse(null)
            );
            assertEquals(
                    "right",
                    resolvingShard2.get(new ReadContext(501L, result.commitTs(), IsolationLevel.READ_COMMITTED), "async-safe:2")
                            .map(value -> new String(value.value()))
                            .orElse(null)
            );
        } finally {
            blockedShard1.allowCommit();
            blockedShard2.allowCommit();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            manager.close();
        }
    }

    /** 验证显式准备能力会触发一次真实准备调用。 */
    @Test
    void crossShardCommitShouldCallPrepareWhenAdapterRequiresIt() {
        CountingPrepareStorageAdapter prepareAwareShard2 = new CountingPrepareStorageAdapter(shard2);
        TransactionManager manager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), prepareAwareShard2
        ));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "order:require-prepare", bytes("created"));
        manager.put(tx, "shard-2", "stock:require-prepare", bytes("reserved"));

        manager.commit(tx);

        assertEquals(1, prepareAwareShard2.prepareCalls.get());
    }

    /** 验证可重复读扫描保留语句间快照。 */
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

    /** 验证读已提交扫描包含最新版本和自身写入。 */
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

    /** 验证范围读隔离未决意向并遵守删除墓碑。 */
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

    /** 验证中心提交后立即可读，恢复完成真实版本和本地终态物化。 */
    @Test
    void recoveryShouldFinishCommittedTransactionAfterPartialShardFailure() throws Exception {
        StorageAdapter flakyShard2 = new FailOnceOnCommitStorageAdapter(shard2);
        TransactionManager flakyManager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), flakyShard2
        ));

        Transaction tx = flakyManager.begin(IsolationLevel.REPEATABLE_READ);
        flakyManager.put(tx, "shard-1", "user:1", bytes("alice"));
        flakyManager.put(tx, "shard-2", "ledger:1", bytes("debit"));

        MvccException error = assertThrows(MvccException.class, () -> flakyManager.commit(tx));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        assertEquals("alice", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "user:1"));
        assertEquals("debit", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "ledger:1"));
        assertEquals(1, countRows(shard2DataSource, "mvcc_intent"));
        assertEquals(0, countRows(shard2DataSource, "mvcc_data"));

        recoveryManager.recover(tx.txId());

        assertEquals("debit", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "ledger:1"));
        assertEquals(0, countRows(shard2DataSource, "mvcc_intent"));
        assertEquals(1, countRows(shard2DataSource, "mvcc_data"));
        var localRecord = shard2.participantRecord(tx.txId()).orElseThrow();
        assertEquals(TxStatus.COMMITTED, localRecord.status());
        assertEquals(metadataStore.get(tx.txId()).orElseThrow().commitTs(), localRecord.commitTs());
    }

    /** 验证重复回滚保持幂等并保留回滚标记。 */
    @Test
    void rollbackShouldBeIdempotentAndPersistRollbackMarker() throws Exception {
        TransactionManager eagerManager = new TransactionManager(
                tso,
                metadataStore,
                TransactionOptionsPresets.failFast(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );
        Transaction tx = eagerManager.begin(IsolationLevel.REPEATABLE_READ);
        eagerManager.put(tx, "shard-1", "account:9", bytes("pending"));

        eagerManager.rollback(tx);
        eagerManager.rollback(tx);

        assertEquals(1, countRows(shard1DataSource, "mvcc_rollback"));
        assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        assertTrue(transactionManager.get(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "account:9").isEmpty());
    }

    /** 验证锁持有者提交后等待事务可以整事务重试。 */
    @Test
    void executeShouldWaitThenRetryAfterForeignIntentCommits() throws Exception {
        TransactionManager waitAndRetryManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        new LockWaitPolicy(ConflictMode.WAIT, 2_000L, 5L, 20L, 2.0d, false),
                        new TransactionRetryPolicy(3, 5L, 20L, 2.0d, false),
                        CrossShardCommitPolicy.serial(),
                        false,
                        false,
                        false
                ),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );

        TransactionManager eagerHolderManager = new TransactionManager(
                tso,
                metadataStore,
                TransactionOptionsPresets.failFast(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2)
        );
        Transaction holder = eagerHolderManager.begin(IsolationLevel.REPEATABLE_READ);
        eagerHolderManager.put(holder, "shard-1", "wait:001", bytes("holder"));

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
            eagerHolderManager.commit(holder);

            long elapsedMillis = getFuture(elapsedFuture);
            assertTrue(elapsedMillis >= 100L, "expected waiter to observe lock wait, elapsed=" + elapsedMillis);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("waiter", readString(reader, "shard-1", "wait:001"));
    }

    /** 验证写冲突触发整事务重试并保留两个提交版本。 */
    @Test
    void executeShouldRetryWholeTransactionAfterWriteConflict() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        TransactionManager retryingManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        new TransactionRetryPolicy(3, 5L, 20L, 2.0d, false),
                        CrossShardCommitPolicy.serial(),
                        false,
                        false,
                        false
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

    /** 验证等待策略能够重试形似数据库死锁的存储异常。 */
    @Test
    void putShouldWaitThroughDeadlockShapedStorageUnavailable() {
        TransactionManager waitManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        new LockWaitPolicy(ConflictMode.WAIT, 1_000L, 1L, 5L, 2.0d, false),
                        TransactionRetryPolicy.noRetry(),
                        CrossShardCommitPolicy.serial(),
                        false,
                        false,
                        false
                ),
                Map.of(
                        shard1.shardId(), new FailOnceOnPrewriteStorageAdapter(shard1, deadlockLikeStorageError(1L)),
                        shard2.shardId(), shard2
                )
        );

        Transaction tx = waitManager.begin(IsolationLevel.REPEATABLE_READ);
        waitManager.put(tx, "shard-1", "deadlock:wait:001", bytes("v1"));
        waitManager.commit(tx);

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("v1", readString(reader, "shard-1", "deadlock:wait:001"));
    }

    /** 验证数据库死锁形态异常触发整事务重试。 */
    @Test
    void executeShouldRetryWholeTransactionAfterDeadlockShapedStorageUnavailable() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        TransactionManager retryingManager = new TransactionManager(
                tso,
                metadataStore,
                new TransactionOptions(
                        LockWaitPolicy.failFast(),
                        new TransactionRetryPolicy(3, 1L, 5L, 2.0d, false),
                        CrossShardCommitPolicy.serial(),
                        false,
                        false,
                        false
                ),
                Map.of(
                        shard1.shardId(), new FailOnceOnPrewriteStorageAdapter(shard1, deadlockLikeStorageError(2L)),
                        shard2.shardId(), shard2
                )
        );

        retryingManager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
            attempts.incrementAndGet();
            retryingManager.put(tx, "shard-1", "deadlock:retry:001", bytes("v2"));
            return null;
        });

        assertEquals(2, attempts.get());
        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        assertEquals("v2", readString(reader, "shard-1", "deadlock:retry:001"));
    }

    /** 通过事务管理器写入测试初始提交版本。 */
    private void seedCommittedValue(String shardId, String key, String value) {
        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, shardId, key, bytes(value));
        transactionManager.commit(tx);
    }

    /** 读取当前事务快照中的字符串值。 */
    private String readString(Transaction tx, String shardId, String key) {
        return transactionManager.get(tx, shardId, key)
                .map(String::new)
                .orElse(null);
    }

    /** 扫描当前快照并返回有序键列表。 */
    private List<String> scanKeys(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive) {
        return transactionManager.scan(tx, shardId, startKeyInclusive, endKeyExclusive)
                .stream()
                .map(VersionedValue::key)
                .collect(Collectors.toList());
    }

    /** 按 UTF-8 编码测试字符串。 */
    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** 等待异步结果并保留原始异常。 */
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

    /** 清理用例协议表，调用者随后恢复本地水位单例。 */
    private static void truncate(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("delete from mvcc_intent");
            statement.execute("delete from mvcc_data");
            statement.execute("delete from mvcc_rollback");
            statement.execute("delete from mvcc_participant_txn");
            statement.execute("delete from mvcc_key_lock");
            statement.execute("delete from mvcc_local_safe_point");
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
             var statement = connection.prepareStatement("select count(*) from mvcc_data where user_key = ?")) {
            statement.setBytes(1, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
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

    /** 构造带 MySQL 死锁错误码的存储异常。 */
    private static MvccException deadlockLikeStorageError(Long txId) {
        return new MvccException(
                ErrorCode.STORAGE_UNAVAILABLE,
                "simulated deadlock from mysql",
                true,
                txId,
                new SQLException("Deadlock found when trying to get lock", "40001", 1213)
        );
    }

    /** 在真实存储写入边界检查中心登记及单分片候选提交时间。 */
    private static final class PublicationCheckingStorageAdapter extends DelegatingStorageAdapter {
        private final TransactionMetadataStore metadataStore;
        private int prewriteCalls;
        private int directCommitCalls;

        /** 构造中心发布协议检查包装器。 */
        private PublicationCheckingStorageAdapter(StorageAdapter delegate, TransactionMetadataStore metadataStore) {
            super(delegate);
            this.metadataStore = metadataStore;
        }

        /** 确认参与者已经登记后才执行真实预写。 */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
            var record = metadataStore.get(txId).orElseThrow();
            assertEquals(TxStatus.ACTIVE, record.status());
            assertEquals(startTs, record.startTs());
            assertTrue(record.participants().contains(shardId()));
            prewriteCalls++;
            super.prewrite(txId, startTs, mutations);
        }

        /** 确认候选时间已持久化，再转发真实原子提交并核验本地终态。 */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            var record = metadataStore.get(txId).orElseThrow();
            assertEquals(TxStatus.PREPARING, record.status());
            assertEquals(Set.of(shardId()), record.participants());
            assertEquals(startTs, record.startTs());
            assertEquals(commitTs, record.commitTs().longValue());
            directCommitCalls++;
            super.directCommit(txId, startTs, commitTs, mutations);
            var localRecord = participantRecord(txId).orElseThrow();
            assertEquals(TxStatus.COMMITTED, localRecord.status());
            assertEquals(commitTs, localRecord.commitTs().longValue());
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

    /** 首次预写失败，其余调用转发给真实适配器。 */
    private static final class FailOnceOnPrewriteStorageAdapter extends DelegatingStorageAdapter {
        private final AtomicBoolean shouldFail = new AtomicBoolean(true);
        private final MvccException failure;

        /** 构造预写故障包装器。 */
        private FailOnceOnPrewriteStorageAdapter(StorageAdapter delegate, MvccException failure) {
            super(delegate);
            this.failure = failure;
        }

        /** 注入一次预写故障后转发真实预写。 */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
            if (shouldFail.compareAndSet(true, false)) {
                throw failure;
            }
            super.prewrite(txId, startTs, mutations);
        }
    }

    /** 检查不需要显式准备的真实适配器不会收到准备调用。 */
    private static final class FailOnPrepareStorageAdapter extends DelegatingStorageAdapter {
        /** 构造显式准备检查包装器。 */
        private FailOnPrepareStorageAdapter(StorageAdapter delegate) {
            super(delegate);
        }

        /** 拒绝不应发生的显式准备调用。 */
        @Override
        public void prepare(long txId) {
            throw new AssertionError("prepare should not be called");
        }
    }

    /** 阻塞真实提交物化，使测试能检查中心决议发布后的可见性。 */
    private static final class BlockingCommitStorageAdapter extends DelegatingStorageAdapter {
        private final CountDownLatch allowCommit = new CountDownLatch(1);

        /** 构造阻塞提交包装器。 */
        private BlockingCommitStorageAdapter(StorageAdapter delegate) {
            super(delegate);
        }

        /** 允许所有等待中的提交继续。 */
        private void allowCommit() {
            allowCommit.countDown();
        }

        /** 阻塞按事务编号提交。 */
        @Override
        public void commit(long txId, long commitTs) {
            awaitCommit();
            super.commit(txId, commitTs);
        }

        /** 阻塞按写集合提交。 */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations) {
            awaitCommit();
            super.commit(txId, commitTs, mutations);
        }

        /** 阻塞提交并保留真实清理策略。 */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
            awaitCommit();
            super.commit(txId, commitTs, mutations, cleanupIntents);
        }

        /** 阻塞直接提交并保留本地版本和参与者记录的原子性。 */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            awaitCommit();
            super.directCommit(txId, startTs, commitTs, mutations);
        }

        /** 等待测试释放提交，保留线程中断语义。 */
        private void awaitCommit() {
            try {
                allowCommit.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "interrupted in blocking commit adapter", true, null, ex);
            }
        }
    }

    /** 要求显式准备并统计真实准备调用。 */
    private static final class CountingPrepareStorageAdapter extends DelegatingStorageAdapter {
        private final AtomicInteger prepareCalls = new AtomicInteger();

        /** 构造准备调用计数包装器。 */
        private CountingPrepareStorageAdapter(StorageAdapter delegate) {
            super(delegate);
        }

        /** 为该测试显式启用准备阶段。 */
        @Override
        public boolean requiresExplicitPreparePhase() {
            return true;
        }

        /** 统计准备调用并转发给真实适配器。 */
        @Override
        public void prepare(long txId) {
            prepareCalls.incrementAndGet();
            super.prepare(txId);
        }
    }
}

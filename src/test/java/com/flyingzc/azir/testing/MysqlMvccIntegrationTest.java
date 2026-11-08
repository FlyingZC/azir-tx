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
import com.flyingzc.azir.api.TransactionCommitMode;
import com.flyingzc.azir.api.TransactionCommitResult;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.recovery.RecoveryManager;
import com.flyingzc.azir.sql.AzirSqlEngine;
import com.flyingzc.azir.sql.ShardResolver;
import com.flyingzc.azir.sql.SqlExecutionResult;
import com.flyingzc.azir.sql.SqlSession;
import com.flyingzc.azir.sql.SqlValueCodec;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.txn.ConflictMode;
import com.flyingzc.azir.txn.CrossShardCommitPolicy;
import com.flyingzc.azir.txn.LockWaitPolicy;
import com.flyingzc.azir.txn.TransactionManager;
import com.flyingzc.azir.txn.TransactionOptions;
import com.flyingzc.azir.txn.TransactionOptionsPresets;
import com.flyingzc.azir.txn.TransactionRetryPolicy;
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

/**
 * Use real MySQL shard verify transaction, snapshot Visibility and failure recovery.
 */
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

    /**
     * Creates a quarantine test database and initializes the protocol table.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeAll
    static void beforeAll() throws Exception {
        createDatabase("shard_1");
        createDatabase("shard_2");
        shard1DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_1"), MYSQL.getUsername(), MYSQL.getPassword());
        shard2DataSource = MysqlDataSourceFactory.create(databaseUrl("shard_2"), MYSQL.getUsername(), MYSQL.getPassword());
        new MysqlStorageAdapter("shard-1", shard1DataSource).initialize();
        new MysqlStorageAdapter("shard-2", shard2DataSource).initialize();
    }

    /**
     * Closes the data source used for testing.
     *
     * @throws Exception if the operation cannot complete
     */
    @AfterAll
    static void afterAll() throws Exception {
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
        truncate(shard1DataSource);
        truncate(shard2DataSource);
        tso = new AtomicTimestampOracle(100);
        metadataStore = new InMemoryTransactionMetadataStore();
        // Use the new adapter in each case to avoid reconnecting to the transaction centre of the previous example.
        shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
        shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);
        shard1.initialize();
        shard2.initialize();
        transactionManager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
        recoveryManager = new RecoveryManager(metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));
    }

    /**
     * Verify Reads commit Watched the latest commit version between different statements.
     */
    @Test
    void readCommittedShouldObserveLatestCommittedVersionAcrossStatements() {
        seedCommittedValue("shard-1", "account:1", "v1");

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("v1", readString(reader, "shard-1", "account:1"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "account:1", bytes("v2"));
        transactionManager.commit(writer);

        Assertions.assertEquals("v2", readString(reader, "shard-1", "account:1"));
    }

    /**
     * Verify Calcite Complex SQL completes cross-shard DML, JOIN and Aggregation by real MySQL MVCC adapter.
     */
    @Test
    void calciteSqlShouldExecuteAgainstMysqlMvccShards() {
        AzirSqlEngine sqlEngine = AzirSqlEngine.calcite(
                transactionManager,
                ShardResolver.hashByKey(List.of(shard1.shardId(), shard2.shardId())),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED);

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

            Assertions.assertEquals(1, result.rows().size());
            Assertions.assertEquals("gold", result.singleRow().get("category"));
            Assertions.assertEquals("2", result.singleRow().get("total"));
            Assertions.assertEquals(1, session.execute("delete from account where value = 'SILVER'").updateCount());
            Assertions.assertEquals(2, session.execute("select * from account").rows().size());
        }
    }

    /**
     * Verify Rereadable transaction Start snapshot.
     */
    @Test
    void repeatableReadShouldKeepStartSnapshot() {
        seedCommittedValue("shard-1", "account:1", "v1");

        Transaction reader = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        Assertions.assertEquals("v1", readString(reader, "shard-1", "account:1"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "account:1", bytes("v2"));
        transactionManager.commit(writer);

        Assertions.assertEquals("v1", readString(reader, "shard-1", "account:1"));
    }

    /**
     * Verify read-your-own-writes and separate pending from other transaction writing.
     */
    @Test
    void shouldReadOwnIntentAndSkipOtherTransactionIntent() {
        seedCommittedValue("shard-1", "account:1", "base");

        Transaction tx1 = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx1, "shard-1", "account:1", bytes("tx1"));
        Assertions.assertEquals("tx1", readString(tx1, "shard-1", "account:1"));

        Transaction tx2 = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("base", readString(tx2, "shard-1", "account:1"));
    }

    /**
     * Verify buffer Write point and range visibility before commit.
     *
     * @throws Exception if the operation cannot complete
     */
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
                        false),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        bufferedManager.put(tx, "shard-1", "buffered:001", bytes("v1"));
        bufferedManager.put(tx, "shard-1", "buffered:002", bytes("v2"));

        Assertions.assertEquals("v1", bufferedManager.get(tx, "shard-1", "buffered:001").map(String::new).orElse(null));
        Assertions.assertEquals(
                List.of("buffered:001", "buffered:002"),
                bufferedManager.scan(tx, "shard-1", "buffered:000", "buffered:999")
                        .stream()
                        .map(VersionedValue::key)
                        .collect(Collectors.toList()));
        Assertions.assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
    }

    /**
     * Verify cross-shard buffer Write materialization at commit and clear intent.
     *
     * @throws Exception if the operation cannot complete
     */
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
                        false),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        bufferedManager.put(tx, "shard-1", "buffered:commit:1", bytes("left"));
        bufferedManager.put(tx, "shard-2", "buffered:commit:2", bytes("right"));

        bufferedManager.commit(tx);

        Assertions.assertEquals("left", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "buffered:commit:1"));
        Assertions.assertEquals("right", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "buffered:commit:2"));
        Assertions.assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        Assertions.assertEquals(0, countRows(shard2DataSource, "mvcc_intent"));
    }

    /**
     * Verify Single shard atomic Direct commit Generate version without landing intent.
     *
     * @throws Exception if the operation cannot complete
     */
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
                        false),
                Map.of(shard1.shardId(), checkedShard, shard2.shardId(), shard2));

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        bufferedManager.put(tx, "shard-1", "single:buffered:1", bytes("v1"));
        bufferedManager.put(tx, "shard-1", "single:buffered:2", bytes("v2"));

        bufferedManager.commit(tx);

        Assertions.assertEquals(0, checkedShard.prewriteCalls);
        Assertions.assertEquals(1, checkedShard.directCommitCalls);
        Assertions.assertEquals("v1", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "single:buffered:1"));
        Assertions.assertEquals("v2", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "single:buffered:2"));
        Assertions.assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        Assertions.assertEquals(2, countRows(shard1DataSource, "mvcc_data"));
    }

    /**
     * Verify Delays in registration to save centre and local recovery decision at commit writing.
     */
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
                        false),
                Map.of(shard1.shardId(), checkedShard, shard2.shardId(), shard2));

        Transaction tx = bufferedManager.begin(IsolationLevel.REPEATABLE_READ);
        assertTrue(metadataStore.get(tx.txId()).isEmpty());
        Assertions.assertEquals(tx.deadlineMillis(), metadataStore.snapshotExpiry(tx.txId()).orElseThrow().longValue());
        bufferedManager.put(tx, "shard-1", "single:lazy-metadata:1", bytes("v1"));
        assertTrue(metadataStore.get(tx.txId()).isEmpty());

        TransactionCommitResult result = bufferedManager.commit(tx);

        Assertions.assertEquals(0, checkedShard.prewriteCalls);
        Assertions.assertEquals(1, checkedShard.directCommitCalls);
        var record = metadataStore.get(tx.txId()).orElseThrow();
        var localRecord = shard1.participantRecord(tx.txId()).orElseThrow();
        Assertions.assertEquals(TxStatus.COMMITTED, record.status());
        Assertions.assertEquals(Set.of("shard-1"), record.participants());
        Assertions.assertEquals(result.commitTs(), record.commitTs().longValue());
        Assertions.assertEquals(TxStatus.COMMITTED, localRecord.status());
        Assertions.assertEquals(tx.startTs(), localRecord.startTs());
        Assertions.assertEquals(record.commitTs(), localRecord.commitTs());
        assertTrue(metadataStore.snapshotExpiry(tx.txId()).isEmpty());
        Assertions.assertEquals("v1", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "single:lazy-metadata:1"));
    }

    /**
     * Verify Delayed registration read-only transaction releases lease only after holding snapshot lease, commit and does not create transaction metadata.
     */
    @Test
    void lazyReadOnlyTransactionShouldHoldOnlySnapshotLease() {
        TransactionOptions options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.serial(), true, true, false);
        try (
                TransactionManager manager = new TransactionManager(tso, metadataStore,
                        options, Map.of(shard1.shardId(), shard1))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(metadataStore.get(tx.txId()).isEmpty());
            Assertions.assertEquals(tx.deadlineMillis(), metadataStore.snapshotExpiry(tx.txId()).orElseThrow().longValue());
            assertTrue(manager.get(tx, "shard-1", "readonly:missing").isEmpty());
            manager.commit(tx);
            assertTrue(metadataStore.get(tx.txId()).isEmpty());
            assertTrue(metadataStore.snapshotExpiry(tx.txId()).isEmpty());
        }
    }

    /**
     * Verify The immediate prewrite path to delayed registration is already registered as centre participant before the real intent is written.
     */
    @Test
    void lazyEagerWriteShouldRegisterMetadataBeforePrewrite() {
        PublicationCheckingStorageAdapter checkedShard = new PublicationCheckingStorageAdapter(shard1, metadataStore);
        TransactionOptions options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.serial(), false, true, false);
        try (
                TransactionManager manager = new TransactionManager(tso, metadataStore, options,
                        Map.of(checkedShard.shardId(), checkedShard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(metadataStore.get(tx.txId()).isEmpty());
            manager.put(tx, "shard-1", "single:lazy-prewrite", bytes("v1"));
            Assertions.assertEquals(1, checkedShard.prewriteCalls);
            TransactionCommitResult result = manager.commit(tx);
            Assertions.assertEquals(0, checkedShard.directCommitCalls);
            var localRecord = shard1.participantRecord(tx.txId()).orElseThrow();
            Assertions.assertEquals(TxStatus.COMMITTED, localRecord.status());
            Assertions.assertEquals(result.commitTs(), localRecord.commitTs().longValue());
            Assertions.assertEquals(localRecord.commitTs(), metadataStore.get(tx.txId()).orElseThrow().commitTs());
        }
    }

    /**
     * Verify cross-shard transaction uses two-stage commit mode.
     */
    @Test
    void crossShardCommitShouldUseTwoPhaseCommit() {
        Transaction tx = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx, "shard-1", "order:1", bytes("created"));
        transactionManager.put(tx, "shard-2", "stock:1", bytes("reserved"));

        TransactionCommitResult result = transactionManager.commit(tx);

        Assertions.assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, result.mode());
        Assertions.assertEquals("created", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "order:1"));
        Assertions.assertEquals("reserved", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "stock:1"));
    }

    /**
     * Verify The real adaptor does not require a high-profile preparation to be prepared.
     */
    @Test
    void crossShardCommitShouldSkipPrepareForAdaptersThatDoNotRequireIt() {
        StorageAdapter failOnPrepareShard2 = new FailOnPrepareStorageAdapter(shard2);
        TransactionManager manager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), failOnPrepareShard2));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "order:skip-prepare", bytes("created"));
        manager.put(tx, "shard-2", "stock:skip-prepare", bytes("reserved"));

        TransactionCommitResult result = manager.commit(tx);

        Assertions.assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, result.mode());
        Assertions.assertEquals("created", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "order:skip-prepare"));
        Assertions.assertEquals("reserved", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "stock:skip-prepare"));
    }

    /**
     * Verify global commit decision-making is visible for new reader atomic when other transaction inents not yet materialization are available.
     *
     * @throws Exception if the operation cannot complete
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

        Assertions.assertEquals(0, countRows(shard1DataSource, "mvcc_data"));
        Assertions.assertEquals(
                "v1",
                resolvingShard.get(new ReadContext(300L, commitTs, IsolationLevel.READ_COMMITTED), "async-visible:1")
                        .map(value -> new String(value.value()))
                        .orElse(null));
        assertTrue(resolvingShard.get(new ReadContext(301L, commitTs - 1, IsolationLevel.REPEATABLE_READ), "async-visible:1").isEmpty());
    }

    /**
     * Verify safe walk commit returns before materialization is finished and relies on commit indent to keep cross-shard atomic visible.
     *
     * @throws Exception if the operation cannot complete
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
                        false),
                Map.of(blockedShard1.shardId(), blockedShard1, blockedShard2.shardId(), blockedShard2));

        ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        long[] committedTxId = new long[1];
        try {
            Future<TransactionCommitResult> future = executor.submit(() -> {
                Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(tx, "shard-1", "async-safe:1", bytes("left"));
                manager.put(tx, "shard-2", "async-safe:2", bytes("right"));
                return manager.commit(tx);
            });

            TransactionCommitResult result = future.get(1, TimeUnit.SECONDS);
            committedTxId[0] = result.txId();

            Assertions.assertEquals(TransactionCommitMode.ASYNC_PRIMARY_COMMIT, result.mode());
            assertTrue(metadataStore.snapshotExpiry(result.txId()).isPresent());
            Assertions.assertEquals(0, countRows(shard1DataSource, "mvcc_data"));
            Assertions.assertEquals(0, countRows(shard2DataSource, "mvcc_data"));
            Assertions.assertEquals(
                    "left",
                    resolvingShard1.get(new ReadContext(500L, result.commitTs(), IsolationLevel.READ_COMMITTED), "async-safe:1")
                            .map(value -> new String(value.value()))
                            .orElse(null));
            Assertions.assertEquals(
                    "right",
                    resolvingShard2.get(new ReadContext(501L, result.commitTs(), IsolationLevel.READ_COMMITTED), "async-safe:2")
                            .map(value -> new String(value.value()))
                            .orElse(null));
        } finally {
            blockedShard1.allowCommit();
            blockedShard2.allowCommit();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            manager.close();
        }
        assertTrue(metadataStore.snapshotExpiry(committedTxId[0]).isEmpty());
    }

    /**
     * Verify Visible readiness triggers a real ready call.
     */
    @Test
    void crossShardCommitShouldCallPrepareWhenAdapterRequiresIt() {
        CountingPrepareStorageAdapter prepareAwareShard2 = new CountingPrepareStorageAdapter(shard2);
        TransactionManager manager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), prepareAwareShard2));

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "shard-1", "order:require-prepare", bytes("created"));
        manager.put(tx, "shard-2", "stock:require-prepare", bytes("reserved"));

        manager.commit(tx);

        Assertions.assertEquals(1, prepareAwareShard2.prepareCalls.get());
    }

    /**
     * Verify Reread scanned reserved statement slot snapshot.
     */
    @Test
    void repeatableReadScanShouldKeepSnapshotAcrossStatements() {
        seedCommittedValue("shard-1", "item:001", "a");
        seedCommittedValue("shard-1", "item:002", "b");

        Transaction reader = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        Assertions.assertEquals(List.of("item:001", "item:002"), scanKeys(reader, "shard-1", "item:000", "item:999"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "item:003", bytes("c"));
        transactionManager.commit(writer);

        Assertions.assertEquals(List.of("item:001", "item:002"), scanKeys(reader, "shard-1", "item:000", "item:999"));
    }

    /**
     * Verify Reads an commit scan that contains the latest version and its own writing.
     */
    @Test
    void readCommittedScanShouldSeeLatestCommittedAndOwnIntent() {
        seedCommittedValue("shard-1", "scan:001", "v1");

        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, "shard-1", "scan:002", bytes("v2"));
        Assertions.assertEquals(List.of("scan:001", "scan:002"), scanKeys(tx, "shard-1", "scan:000", "scan:999"));

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "scan:003", bytes("v3"));
        transactionManager.commit(writer);

        Assertions.assertEquals(List.of("scan:001", "scan:002", "scan:003"), scanKeys(tx, "shard-1", "scan:000", "scan:999"));
    }

    /**
     * Verify Range Read-Segregation pending intent and comply with Delete tombstone.
     */
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
        Assertions.assertEquals(List.of("range:001", "range:002"), scanKeys(reader, "shard-1", "range:000", "range:999"));
    }

    /**
     * Verify Center commit readable immediately after recovery, real version and local terminal state materialization.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void recoveryShouldFinishCommittedTransactionAfterPartialShardFailure() throws Exception {
        StorageAdapter flakyShard2 = new FailOnceOnCommitStorageAdapter(shard2);
        TransactionManager flakyManager = new TransactionManager(tso, metadataStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), flakyShard2));

        Transaction tx = flakyManager.begin(IsolationLevel.REPEATABLE_READ);
        flakyManager.put(tx, "shard-1", "user:1", bytes("alice"));
        flakyManager.put(tx, "shard-2", "ledger:1", bytes("debit"));

        MvccException error = assertThrows(MvccException.class, () -> flakyManager.commit(tx));
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        Assertions.assertEquals("alice", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "user:1"));
        Assertions.assertEquals("debit", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "ledger:1"));
        Assertions.assertEquals(1, countRows(shard2DataSource, "mvcc_intent"));
        Assertions.assertEquals(0, countRows(shard2DataSource, "mvcc_data"));

        recoveryManager.recover(tx.txId());

        Assertions.assertEquals("debit", readString(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-2", "ledger:1"));
        Assertions.assertEquals(0, countRows(shard2DataSource, "mvcc_intent"));
        Assertions.assertEquals(1, countRows(shard2DataSource, "mvcc_data"));
        var localRecord = shard2.participantRecord(tx.txId()).orElseThrow();
        Assertions.assertEquals(TxStatus.COMMITTED, localRecord.status());
        Assertions.assertEquals(metadataStore.get(tx.txId()).orElseThrow().commitTs(), localRecord.commitTs());
    }

    /**
     * Verify Repeat rollback Keep idempotent and keep the rollback tag.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void rollbackShouldBeIdempotentAndPersistRollbackMarker() throws Exception {
        TransactionManager eagerManager = new TransactionManager(
                tso,
                metadataStore,
                TransactionOptionsPresets.failFast(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));
        Transaction tx = eagerManager.begin(IsolationLevel.REPEATABLE_READ);
        eagerManager.put(tx, "shard-1", "account:9", bytes("pending"));

        eagerManager.rollback(tx);
        eagerManager.rollback(tx);

        Assertions.assertEquals(1, countRows(shard1DataSource, "mvcc_rollback"));
        Assertions.assertEquals(0, countRows(shard1DataSource, "mvcc_intent"));
        assertTrue(transactionManager.get(transactionManager.begin(IsolationLevel.READ_COMMITTED), "shard-1", "account:9").isEmpty());
    }

    /**
     * Verify lock holder commit and wait for transaction to complete transaction retry.
     *
     * @throws Exception if the operation cannot complete
     */
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
                        false),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        TransactionManager eagerHolderManager = new TransactionManager(
                tso,
                metadataStore,
                TransactionOptionsPresets.failFast(),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));
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
        Assertions.assertEquals("waiter", readString(reader, "shard-1", "wait:001"));
    }

    /**
     * Verify write conflict triggers whole transaction retry and retains two commit version.
     *
     * @throws Exception if the operation cannot complete
     */
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
                        false),
                Map.of(shard1.shardId(), shard1, shard2.shardId(), shard2));

        String result = retryingManager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
            if (attempts.incrementAndGet() == 1) {
                Transaction concurrentWriter = transactionManager.begin(IsolationLevel.READ_COMMITTED);
                transactionManager.put(concurrentWriter, "shard-1", "retry:001", bytes("first"));
                transactionManager.commit(concurrentWriter);
            }
            retryingManager.put(tx, "shard-1", "retry:001", bytes("second"));
            return "second";
        });

        Assertions.assertEquals("second", result);
        Assertions.assertEquals(2, attempts.get());
        Assertions.assertEquals(2, countVersionsForKey(shard1DataSource, "retry:001"));
        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("second", readString(reader, "shard-1", "retry:001"));
    }

    /**
     * The verify waiting strategy allows retry -like database deadlock storage anomalies.
     */
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
                        false),
                Map.of(
                        shard1.shardId(), new FailOnceOnPrewriteStorageAdapter(shard1, deadlockLikeStorageError(1L)),
                        shard2.shardId(), shard2));

        Transaction tx = waitManager.begin(IsolationLevel.REPEATABLE_READ);
        waitManager.put(tx, "shard-1", "deadlock:wait:001", bytes("v1"));
        waitManager.commit(tx);

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("v1", readString(reader, "shard-1", "deadlock:wait:001"));
    }

    /**
     * Verify database Death Lock Form Unusual Trigger transaction retry.
     *
     * @throws Exception if the operation cannot complete
     */
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
                        false),
                Map.of(
                        shard1.shardId(), new FailOnceOnPrewriteStorageAdapter(shard1, deadlockLikeStorageError(2L)),
                        shard2.shardId(), shard2));

        retryingManager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
            attempts.incrementAndGet();
            retryingManager.put(tx, "shard-1", "deadlock:retry:001", bytes("v2"));
            return null;
        });

        Assertions.assertEquals(2, attempts.get());
        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        Assertions.assertEquals("v2", readString(reader, "shard-1", "deadlock:retry:001"));
    }

    /**
     * Write initial commit version test through the transaction manager.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    private void seedCommittedValue(final String shardId, final String key, final String value) {
        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, shardId, key, bytes(value));
        transactionManager.commit(tx);
    }

    /**
     * Reads the current string value in transaction snapshot.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    private String readString(final Transaction tx, final String shardId, final String key) {
        return transactionManager.get(tx, shardId, key)
                .map(String::new)
                .orElse(null);
    }

    /**
     * Scans the current snapshot and returns the ordered key list.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param startKeyInclusive start key inclusive
     * @param endKeyExclusive end key exclusive
     * @return the operation result
     */
    private List<String> scanKeys(final Transaction tx, final String shardId, final String startKeyInclusive, final String endKeyExclusive) {
        return transactionManager.scan(tx, shardId, startKeyInclusive, endKeyExclusive)
                .stream()
                .map(VersionedValue::key)
                .collect(Collectors.toList());
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
     * Waiting for a step and keeping the original anomaly.
     *
     * @param <T> value type
     * @param future future
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws RuntimeException if the operation cannot complete
     */
    private static <T> T getFuture(final Future<T> future) throws Exception {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw new RuntimeException(ex.getCause());
        }
    }

    /**
     * Clears the case protocol list, and the caller is then recovery local watermark.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void truncate(final DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("delete from mvcc_intent");
            statement.execute("delete from mvcc_data");
            statement.execute("delete from mvcc_rollback");
            statement.execute("delete from mvcc_participant_txn");
            statement.execute("delete from mvcc_key_lock");
            statement.execute("delete from mvcc_local_safe_point");
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
                var statement = connection.prepareStatement("select count(*) from mvcc_data where user_key = ?")) {
            statement.setBytes(1, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
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
     * Constructs storage anomalies with MySQL dead lock error codes.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private static MvccException deadlockLikeStorageError(final Long txId) {
        return new MvccException(
                ErrorCode.STORAGE_UNAVAILABLE,
                "simulated deadlock from mysql",
                true,
                txId,
                new SQLException("Deadlock found when trying to get lock", "40001", 1213));
    }

    /**
     * Registers and single shard candidate commit time in the real storage for border check.
     */
    private static final class PublicationCheckingStorageAdapter extends DelegatingStorageAdapter {

        private final TransactionMetadataStore metadataStore;

        private int prewriteCalls;

        private int directCommitCalls;

        /**
         * The construction centre issues protocols to check the packaging.
         *
         * @param delegate delegate
         * @param metadataStore metadata store
         */
        private PublicationCheckingStorageAdapter(final StorageAdapter delegate, final TransactionMetadataStore metadataStore) {
            super(delegate);
            this.metadataStore = metadataStore;
        }

        /**
         * Confirms that participant is registered before real prewrite is executed.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            var record = metadataStore.get(txId).orElseThrow();
            Assertions.assertEquals(TxStatus.ACTIVE, record.status());
            Assertions.assertEquals(startTs, record.startTs());
            assertTrue(record.participants().contains(shardId()));
            prewriteCalls++;
            super.prewrite(txId, startTs, mutations);
        }

        /**
         * Confirms that the candidate time is durable storage, forwarding real atomic commit and verifying local terminal state.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            var record = metadataStore.get(txId).orElseThrow();
            Assertions.assertEquals(TxStatus.PREPARING, record.status());
            Assertions.assertEquals(Set.of(shardId()), record.participants());
            Assertions.assertEquals(startTs, record.startTs());
            Assertions.assertEquals(commitTs, record.commitTs().longValue());
            directCommitCalls++;
            super.directCommit(txId, startTs, commitTs, mutations);
            var localRecord = participantRecord(txId).orElseThrow();
            Assertions.assertEquals(TxStatus.COMMITTED, localRecord.status());
            Assertions.assertEquals(commitTs, localRecord.commitTs().longValue());
        }
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

    /**
     * The first prewrite failed, the rest of the call was forwarded to the real adapter.
     */
    private static final class FailOnceOnPrewriteStorageAdapter extends DelegatingStorageAdapter {

        private final AtomicBoolean shouldFail = new AtomicBoolean(true);

        private final MvccException failure;

        /**
         * Construct prewrite failure packaging.
         *
         * @param delegate delegate
         * @param failure failure
         */
        private FailOnceOnPrewriteStorageAdapter(final StorageAdapter delegate, final MvccException failure) {
            super(delegate);
            this.failure = failure;
        }

        /**
         * Injecting prewrite failure and forwarding real prewrite.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            if (shouldFail.compareAndSet(true, false)) {
                throw failure;
            }
            super.prewrite(txId, startTs, mutations);
        }
    }

    /**
     * Check that the real adaptor, which does not require a visible preparation, will not receive a ready call.
     */
    private static final class FailOnPrepareStorageAdapter extends DelegatingStorageAdapter {

        /**
         * Construct a visible preparation to check the packaging.
         *
         * @param delegate delegate
         */
        private FailOnPrepareStorageAdapter(final StorageAdapter delegate) {
            super(delegate);
        }

        /**
         * Refusal to call undesired and visible.
         *
         * @param txId transaction identifier
         * @throws AssertionError if the operation cannot complete
         */
        @Override
        public void prepare(final long txId) {
            throw new AssertionError("prepare should not be called");
        }
    }

    /**
     * Blocks real commit materialization to allow testing to check the visibility of the decision release.
     */
    private static final class BlockingCommitStorageAdapter extends DelegatingStorageAdapter {

        private final CountDownLatch allowCommit = new CountDownLatch(1);

        /**
         * Construct a block commit packaging unit.
         *
         * @param delegate delegate
         */
        private BlockingCommitStorageAdapter(final StorageAdapter delegate) {
            super(delegate);
        }

        /**
         * Allow all waiting commit to continue.
         */
        private void allowCommit() {
            allowCommit.countDown();
        }

        /**
         * Block transaction identifier commit.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            awaitCommit();
            super.commit(txId, commitTs);
        }

        /**
         * Block write set commit.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations) {
            awaitCommit();
            super.commit(txId, commitTs, mutations);
        }

        /**
         * Block commit and keep the real cleanup strategy.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            awaitCommit();
            super.commit(txId, commitTs, mutations, cleanupIntents);
        }

        /**
         * Blocks direct commit and maintains the atomic properties of local version and participant records.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            awaitCommit();
            super.directCommit(txId, startTs, commitTs, mutations);
        }

        /**
         * Waiting for test to release commit, retention of semantics interrupted by thread.
         *
         * @throws MvccException if the operation cannot complete
         */
        private void awaitCommit() {
            try {
                allowCommit.await();
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "interrupted in blocking commit adapter", true, null, ex);
            }
        }
    }

    /**
     * It's called for visible preparation and statistics of real readiness to call.
     */
    private static final class CountingPrepareStorageAdapter extends DelegatingStorageAdapter {

        private final AtomicInteger prepareCalls = new AtomicInteger();

        /**
         * Constructing to call a number wrapper.
         *
         * @param delegate delegate
         */
        private CountingPrepareStorageAdapter(final StorageAdapter delegate) {
            super(delegate);
        }

        /**
         * Provides for the explicit start-up phase of this test.
         *
         * @return the operation result
         */
        @Override
        public boolean requiresExplicitPreparePhase() {
            return true;
        }

        /**
         * Statistical preparation to call and forward to real adapter.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
            prepareCalls.incrementAndGet();
            super.prepare(txId);
        }
    }
}

package mvcc.txn;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.api.Transaction;
import mvcc.api.TransactionCommitMode;
import mvcc.common.IsolationLevel;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.gc.NoOpSafePointStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.metrics.NoOpTransactionMetrics;
import mvcc.publication.mysql.MysqlPublicationLedger;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger;
import mvcc.publication.SegmentPublicationMaintenanceScheduler;
import mvcc.publication.SegmentPublicationRecoveryManager;
import mvcc.publication.SegmentPublicationRecoveryRunner;
import mvcc.publication.PublicationEntryStatus;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.storage.StorageAdapter;
import mvcc.storage.Mutation;
import mvcc.tso.mysql.MysqlTimestampOracle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 真实两分片 MySQL 下验证账本事务入口的全局 RC/RR 快照。 */
@Testcontainers
class LedgerTransactionManagerIntegrationTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");
    private static HikariDataSource control;
    private static HikariDataSource firstData;
    private static HikariDataSource secondData;
    private MysqlPublicationLedger ledger;

    /** 创建隔离控制库和两个物理分片。 */
    @BeforeAll
    static void initializeDatabases() throws Exception {
        createDatabase("control");
        createDatabase("shard_one");
        createDatabase("shard_two");
        control = pool("control");
        firstData = pool("shard_one");
        secondData = pool("shard_two");
        new MysqlTimestampOracle(control).initialize(100);
        new MysqlTransactionMetadataStore(control).initialize();
        new MysqlStorageAdapter("s1", firstData).initialize();
        new MysqlStorageAdapter("s2", secondData).initialize();
    }

    /** 用容器内管理员创建库并授权给隔离测试用户。 */
    private static void createDatabase(String database) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + database + "`; "
                        + "grant all privileges on `" + database + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /** 根据同一容器端口建立各库独立连接池。 */
    private static HikariDataSource pool(String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭本类创建的三个连接池。 */
    @AfterAll
    static void closeDatabases() {
        if (control != null) control.close();
        if (firstData != null) firstData.close();
        if (secondData != null) secondData.close();
    }

    /** 每个用例清空旧时间线并重新建立版本化账本状态。 */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists mvcc_publication_segment_slot_v1");
            statement.execute("drop table if exists mvcc_publication_segment_v1");
            statement.execute("drop table if exists mvcc_segment_publication_state_v1");
            statement.execute("drop table if exists mvcc_publication_state_v1_legacy_archived");
            statement.execute("drop table if exists mvcc_snapshot_lease");
            statement.execute("drop table if exists mvcc_legacy_lease_gate_v1");
            statement.execute("drop table if exists mvcc_protocol_epoch_v1");
            statement.execute("drop table if exists mvcc_snapshot_lease_v2");
            statement.execute("drop table if exists mvcc_coordinator_snapshot_lease_v1");
            statement.execute("drop table if exists mvcc_publication_entry_v1");
            statement.execute("drop table if exists mvcc_publication_state_v1");
            statement.executeUpdate("delete from mvcc_txn_finalized");
            statement.executeUpdate("delete from mvcc_txn");
            statement.executeUpdate("update mvcc_tso set current_value = 100 where sequence_name = 'global'");
        }
        clearShard(firstData);
        clearShard(secondData);
        new MysqlTransactionMetadataStore(control).initialize();
        ledger = new MysqlPublicationLedger(control);
        ledger.initialize();
    }

    /** 清除分片意向、版本和本地终态，保留初始化后的安全水位行。 */
    private void clearShard(HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
        }
    }

    /** 创建同步物化的账本模式，用相同控制权威连接两分片。 */
    private TransactionManager manager() {
        return manager(false);
    }

    /** 按需启用批量决议以验证与正常 RC/RR 入口的组合。 */
    private TransactionManager manager(boolean batchControlCommit) {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        var second = new MysqlStorageAdapter("s2", secondData, metadata);
        return new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options, Map.of("s1", first, "s2", second),
                NoOpTransactionMetrics.INSTANCE, ledger, batchControlCommit, 2, true);
    }

    /** 已持久激活账本后，旧 MySQL 管理器必须在构造期拒绝启动。 */
    @Test
    void legacyManagerMustRejectActivatedLedgerBeforeBegin() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        MvccException rejected = assertThrows(MvccException.class, () ->
                new TransactionManager(new MysqlTimestampOracle(control), metadata,
                        NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(),
                        Map.of("s1", first)));
        assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
        assertTrue(rejected.getMessage().contains("activated publication mode"));
    }

    /** 分段模式已归档旧时间线时，旧管理器也必须在构造期停止。 */
    @Test
    void legacyManagerMustRejectSegmentModeBeforeBegin() {
        new MysqlSegmentPublicationLedger(control).activateFromLedger();
        var metadata = new MysqlTransactionMetadataStore(control);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        MvccException rejected = assertThrows(MvccException.class, () ->
                new TransactionManager(new MysqlTimestampOracle(control), metadata,
                        NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(),
                        Map.of("s1", first)));
        assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
        assertTrue(rejected.getMessage().contains("activated publication mode"));
    }

    /** 事务 ID 独立于快照，外部跨分片提交仅对 RC 新语句可见。 */
    @Test
    void readCommittedRefreshesWhileRepeatableReadKeepsItsPublishedSnapshot() {
        try (var manager = manager()) {
            Transaction seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(seed.txId() != seed.startTs());
            manager.put(seed, "s1", "account", bytes("old"));
            manager.put(seed, "s2", "ledger", bytes("old"));
            long firstCommitTs = manager.commit(seed).commitTs();
            assertEquals(firstCommitTs, ledger.currentPublished());

            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            assertEquals(firstCommitTs, repeatable.startTs());
            assertArrayEquals(bytes("old"), manager.get(repeatable, "s1", "account").orElseThrow());
            assertArrayEquals(bytes("old"), manager.get(committed, "s2", "ledger").orElseThrow());

            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "account", bytes("new"));
            manager.put(writer, "s2", "ledger", bytes("new"));
            long secondCommitTs = manager.commit(writer).commitTs();
            assertTrue(secondCommitTs > firstCommitTs);
            assertArrayEquals(bytes("old"), manager.get(repeatable, "s1", "account").orElseThrow());
            assertArrayEquals(bytes("old"), manager.get(repeatable, "s2", "ledger").orElseThrow());
            assertArrayEquals(bytes("new"), manager.get(committed, "s1", "account").orElseThrow());
            assertArrayEquals(bytes("new"), manager.get(committed, "s2", "ledger").orElseThrow());
            manager.commit(repeatable);
            manager.commit(committed);
            assertTrue(ledger.snapshotExpiry(repeatable.txId()).isEmpty());
        }
    }

    /** 单分片直接提交跳过意向预写，仍须经账本水位向新快照发布。 */
    @Test
    void singleShardWriteUsesTheSameLedgerPublication() throws Exception {
        try (var manager = manager()) {
            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "single", bytes("value"));
            var result = manager.commit(writer);
            assertEquals(TransactionCommitMode.SINGLE_SHARD_FAST_PATH, result.mode());
            assertEquals(result.commitTs(), ledger.currentPublished());
            try (Connection connection = firstData.getConnection(); Statement statement = connection.createStatement();
                 var intents = statement.executeQuery("select count(*) from mvcc_intent")) {
                assertTrue(intents.next());
                assertEquals(0, intents.getInt(1));
            }
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("value"), manager.get(reader, "s1", "single").orElseThrow());
            assertTrue(manager.get(repeatable, "s1", "single").isEmpty());
            manager.commit(reader);
            manager.commit(repeatable);
        }
    }

    /** 默认账本构造器保持吞吐较高的单分片意向提交，直接 1PC 只在显式启用时运行。 */
    @Test
    void defaultLedgerSingleShardCommitMustRemainIntentBased() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        try (var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options, Map.of("s1", first),
                NoOpTransactionMetrics.INSTANCE, ledger)) {
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "intent:default", bytes("value"));
            var result = manager.commit(writer);
            assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, result.mode());
            assertEquals(result.commitTs(), ledger.currentPublished());
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("value"), manager.get(reader, "s1", "intent:default").orElseThrow());
            manager.commit(reader);
        }
    }

    /** 并发单分片预留共享一笔控制事务后，已确认写仍遵守全局 RC/RR 快照边界。 */
    @Test
    void batchedLocalReservationsMustPreserveReadCommittedAndRepeatableRead() throws Exception {
        try (var manager = manager(true);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            var ready = new CountDownLatch(8);
            var start = new CountDownLatch(1);
            var commits = new ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 8; index++) {
                final int key = index;
                commits.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
                    manager.put(writer, "s1", "local:batch:" + key, bytes("value:" + key));
                    return manager.commit(writer).commitTs();
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            long newest = 100;
            for (var commit : commits) {
                newest = Math.max(newest, commit.get(30, TimeUnit.SECONDS));
            }
            assertEquals(newest, ledger.currentPublished());
            Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
            for (int index = 0; index < 8; index++) {
                assertTrue(manager.get(oldReader, "s1", "local:batch:" + index).isEmpty());
                assertArrayEquals(bytes("value:" + index),
                        manager.get(newReader, "s1", "local:batch:" + index).orElseThrow());
            }
            manager.commit(newReader);
            manager.commit(oldReader);
        }
    }

    /** 单分片直接提交冲突须先取得本地 ABORTED 证明，再解除 PENDING 水位阻塞。 */
    @Test
    void conflictingSingleShardDirectCommitMustAbortReservedSlot() {
        try (var manager = manager()) {
            Transaction stale = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction winner = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(winner, "s1", "direct:conflict", bytes("winner"));
            long winnerTs = manager.commit(winner).commitTs();
            manager.put(stale, "s1", "direct:conflict", bytes("stale"));
            MvccException conflict = assertThrows(MvccException.class, () -> manager.commit(stale));
            assertEquals(ErrorCode.WRITE_CONFLICT, conflict.errorCode());
            assertEquals(PublicationEntryStatus.ABORTED,
                    ledger.findByTxId(stale.txId()).orElseThrow().status());
            assertEquals(winnerTs + 1, ledger.currentPublished());
            manager.rollback(stale);
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("winner"), manager.get(reader, "s1", "direct:conflict").orElseThrow());
            manager.commit(reader);
        }
    }

    /** 批量预留下的写冲突须回滚自己的时间槽，并唤醒等待连续发布的事务。 */
    @Test
    void batchedDirectConflictMustReleasePendingPublicationSlot() {
        try (var manager = manager(true)) {
            Transaction stale = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction winner = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(winner, "s1", "local:batch:conflict", bytes("winner"));
            long winnerTs = manager.commit(winner).commitTs();
            manager.put(stale, "s1", "local:batch:conflict", bytes("stale"));
            assertEquals(ErrorCode.WRITE_CONFLICT,
                    assertThrows(MvccException.class, () -> manager.commit(stale)).errorCode());
            assertEquals(PublicationEntryStatus.ABORTED,
                    ledger.findByTxId(stale.txId()).orElseThrow().status());
            assertEquals(winnerTs + 1, ledger.currentPublished());
            manager.rollback(stale);
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("winner"), manager.get(reader, "s1", "local:batch:conflict").orElseThrow());
            manager.commit(reader);
        }
    }

    /** 本地直接提交已经落盘但响应丢失时，前台以持久终态补齐中心决议。 */
    @Test
    void lostDirectCommitResponseMustResolveFromLocalCommittedProof() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata) {
            /** 在真实本地提交完成后模拟网络响应丢失。 */
            @Override
            public void directCommit(long txId, long startTs, long commitTs, java.util.List<Mutation> mutations) {
                super.directCommit(txId, startTs, commitTs, mutations);
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "simulated lost local commit response", false, txId);
            }
        };
        try (var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options, Map.of("s1", first),
                NoOpTransactionMetrics.INSTANCE, ledger, false, 2, true)) {
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "direct:response-lost", bytes("committed"));
            var result = manager.commit(writer);
            assertEquals(TransactionCommitMode.SINGLE_SHARD_FAST_PATH, result.mode());
            assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
            assertEquals(PublicationEntryStatus.COMMITTED,
                    ledger.findByTxId(writer.txId()).orElseThrow().status());
            assertEquals(result.commitTs(), ledger.currentPublished());
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("committed"),
                    manager.get(reader, "s1", "direct:response-lost").orElseThrow());
            manager.commit(reader);
        }
    }

    /** 并发批量决议返回后，新 RC 快照看到全部写入，旧 RR 快照仍保持原样。 */
    @Test
    void batchedCrossShardCommitsPreserveGlobalReadCommittedAndRepeatableRead() throws Exception {
        try (var manager = manager(true);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            var ready = new CountDownLatch(8);
            var start = new CountDownLatch(1);
            var commits = new ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 8; index++) {
                final int key = index;
                commits.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
                    manager.put(writer, "s1", "batch:left:" + key, bytes("left:" + key));
                    manager.put(writer, "s2", "batch:right:" + key, bytes("right:" + key));
                    return manager.commit(writer).commitTs();
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            long newest = 100;
            for (var commit : commits) {
                newest = Math.max(newest, commit.get(30, TimeUnit.SECONDS));
            }
            assertEquals(newest, ledger.currentPublished());
            Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
            for (int index = 0; index < 8; index++) {
                assertTrue(manager.get(oldReader, "s1", "batch:left:" + index).isEmpty());
                assertTrue(manager.get(oldReader, "s2", "batch:right:" + index).isEmpty());
                assertArrayEquals(bytes("left:" + index),
                        manager.get(newReader, "s1", "batch:left:" + index).orElseThrow());
                assertArrayEquals(bytes("right:" + index),
                        manager.get(newReader, "s2", "batch:right:" + index).orElseThrow());
            }
            manager.commit(oldReader);
            manager.commit(newReader);
        }
    }

    /** 中心决议已发布而参与者尚未物化时，RC 能解析已提交意向且旧 RR 仍固定旧快照。 */
    @Test
    void asyncLedgerCommitMustRemainVisibleWhileParticipantCommitIsBlocked() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        var second = new MysqlStorageAdapter("s2", secondData, metadata);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options,
                Map.of("s1", blockParticipantCommit(first, entered, release),
                        "s2", blockParticipantCommit(second, entered, release)),
                NoOpTransactionMetrics.INSTANCE, ledger, true, 2)) {
            try {
                Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
                Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(writer, "s1", "async:left", bytes("left"));
                manager.put(writer, "s2", "async:right", bytes("right"));
                var result = manager.commit(writer);
                assertEquals(TransactionCommitMode.ASYNC_PRIMARY_COMMIT, result.mode());
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertTrue(first.participantRecord(writer.txId()).orElseThrow().status() != TxStatus.COMMITTED
                        || second.participantRecord(writer.txId()).orElseThrow().status() != TxStatus.COMMITTED);

                Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
                assertEquals(result.commitTs(), committed.startTs());
                assertArrayEquals(bytes("left"), manager.get(committed, "s1", "async:left").orElseThrow());
                assertArrayEquals(bytes("right"), manager.get(committed, "s2", "async:right").orElseThrow());
                assertTrue(manager.get(repeatable, "s1", "async:left").isEmpty());
                assertTrue(manager.get(repeatable, "s2", "async:right").isEmpty());
                manager.commit(committed);
                manager.commit(repeatable);
            } finally {
                release.countDown();
            }
        }
    }

    /** 用代理暂停后台物化调用，保留真实分片的预写和快照读取行为。 */
    private StorageAdapter blockParticipantCommit(StorageAdapter delegate, CountDownLatch entered,
                                                  CountDownLatch release) {
        return (StorageAdapter) Proxy.newProxyInstance(StorageAdapter.class.getClassLoader(),
                new Class<?>[]{StorageAdapter.class}, (proxy, method, args) -> {
                    if (method.getName().equals("commit")) {
                        entered.countDown();
                        release.await();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /** 并发批量 begin 产生独立事务号，已确认写之后的新 RC/RR 快照覆盖该写。 */
    @Test
    void batchedLedgerBeginMustPreserveGlobalSnapshotOrder() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        var second = new MysqlStorageAdapter("s2", secondData, metadata);
        try (var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options, Map.of("s1", first, "s2", second),
                NoOpTransactionMetrics.INSTANCE, ledger, true, 2, false, true, 1);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = new CountDownLatch(8);
            var start = new CountDownLatch(1);
            var begun = new ArrayList<java.util.concurrent.Future<Transaction>>();
            for (int index = 0; index < 8; index++) {
                begun.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return manager.begin(IsolationLevel.REPEATABLE_READ);
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            var readers = new ArrayList<Transaction>();
            for (var future : begun) {
                readers.add(future.get(10, TimeUnit.SECONDS));
            }
            assertEquals(8, readers.stream().mapToLong(Transaction::txId).distinct().count());
            assertTrue(readers.stream().allMatch(tx -> tx.startTs() == 100));
            readers.forEach(manager::commit);

            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "begin:batch:left", bytes("left"));
            manager.put(writer, "s2", "begin:batch:right", bytes("right"));
            long committedTs = manager.commit(writer).commitTs();
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals(committedTs, committed.startTs());
            assertEquals(committedTs, repeatable.startTs());
            assertArrayEquals(bytes("left"), manager.get(committed, "s1", "begin:batch:left").orElseThrow());
            assertArrayEquals(bytes("right"), manager.get(repeatable, "s2", "begin:batch:right").orElseThrow());
            manager.commit(committed);
            manager.commit(repeatable);
        }
    }

    /** 显式运行时绑定独立后台池后，可提交两分片并周期运行恢复与 GC。 */
    @Test
    void explicitLedgerRuntimeMustUseIndependentBackgroundPools() throws Exception {
        var transactionOptions = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new LedgerRuntimeOptions(10, 10, 20, 50, true, 2, false, false, 1);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlLedgerTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     transactionOptions, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "runtime:left", bytes("left"));
            manager.put(writer, "s2", "runtime:right", bytes("right"));
            long commitTs = manager.commit(writer).commitTs();
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("left"), manager.get(reader, "s1", "runtime:left").orElseThrow());
            assertArrayEquals(bytes("right"), manager.get(reader, "s2", "runtime:right").orElseThrow());
            manager.commit(reader);
            assertEquals(commitTs, ledger.currentPublished());
            // 模拟控制决议落盘后协调器退出，后台池必须独立补齐本地参与者。
            var abandoned = ledger.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            var foregroundMetadata = new MysqlTransactionMetadataStore(control);
            foregroundMetadata.registerParticipants(abandoned.txId(), Set.of("s1"));
            var abandonedShard = new MysqlStorageAdapter("s1", firstData, foregroundMetadata);
            abandonedShard.prewrite(abandoned.txId(), abandoned.startTs(),
                    java.util.List.of(Mutation.put("runtime:recover", bytes("recovered"))));
            long recoveredCommitTs = ledger.commitPreparedIntents(abandoned.txId(), abandoned.deadlineMillis(),
                    Set.of("s1")).commitTs();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline && abandonedShard.participantRecord(abandoned.txId())
                    .orElseThrow().status() != TxStatus.COMMITTED) {
                Thread.sleep(20);
            }
            assertEquals(TxStatus.COMMITTED, abandonedShard.participantRecord(abandoned.txId())
                    .orElseThrow().status());
            assertEquals(recoveredCommitTs, ledger.currentPublished());
            assertTrue(runtime.maintenanceScheduler().recoveryStats().runCount() > 0);
            assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
        }
    }

    /** 协调者快照免除只读逐事务租约，首写提升后仍保持全局 RR 与 RC 可见性。 */
    @Test
    void coordinatorSnapshotsMustPromoteWritesAndPreserveIsolation() {
        var transactionOptions = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new LedgerRuntimeOptions(10, 10, 20, 50,
                true, 2, false, false, 1, true, 120_000, 1_000);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlLedgerTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     transactionOptions, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(repeatable.txId() < 0);
            assertTrue(committed.txId() < 0);
            assertTrue(repeatable.txId() != committed.txId());
            assertTrue(ledger.snapshotExpiry(repeatable.txId()).isEmpty());
            assertTrue(ledger.snapshotExpiry(committed.txId()).isEmpty());

            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "coordinator:one", bytes("one"));
            manager.put(writer, "s2", "coordinator:two", bytes("two"));
            assertTrue(ledger.snapshotExpiry(writer.txId()).isEmpty());
            long commitTs = manager.commit(writer).commitTs();
            assertTrue(writer.txId() < 0);
            assertTrue(ledger.snapshotExpiry(writer.txId()).isEmpty());
            assertTrue(manager.get(repeatable, "s1", "coordinator:one").isEmpty());
            assertArrayEquals(bytes("one"), manager.get(committed, "s1", "coordinator:one").orElseThrow());
            assertArrayEquals(bytes("two"), manager.get(committed, "s2", "coordinator:two").orElseThrow());
            assertEquals(commitTs, ledger.currentPublished());
            manager.commit(repeatable);
            manager.commit(committed);
            assertTrue(runtime.maintenanceScheduler().gcStats().failureCount() == 0);
        }
    }

    /** 本地分段 owner 使用最新持久代数决议，并在空闲后封存未分配尾槽。 */
    @Test
    void segmentedOwnerRegistryMustRenewAndSealIdleTail() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        try (HikariDataSource backgroundControl = pool("control");
             var owner = new PublicationSegmentOwnerRegistry(segments,
                     new MysqlSegmentPublicationLedger(backgroundControl), 4, 60_000, 10_000, 100)) {
            var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            metadata.registerParticipants(writer.txId(), Set.of("s1"));
            first.prewrite(writer.txId(), writer.startTs(),
                    java.util.List.of(Mutation.put("segment:registry", bytes("value"))));
            var assignment = owner.allocate(writer.startTs());
            assertEquals(101, assignment.commitTs());
            assertTrue(owner.refresh() > 0);
            assertEquals(2, segments.segmentLease(assignment.segmentStart()).orElseThrow().generation());
            assertEquals(101, owner.decide(assignment, writer.txId(), writer.deadlineMillis(), Set.of("s1")));
            assertEquals(101, segments.advancePublished(10));
            Thread.sleep(150);
            assertTrue(owner.refresh() > 0);
            assertEquals(104, segments.advancePublished(10));
            assertTrue(segments.segmentLease(assignment.segmentStart()).isEmpty());
            assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        }
    }

    /** 同一持久段上的并发本地 CAS 取槽不能重号或越过段上界。 */
    @Test
    void segmentedOwnerRegistryConcurrentLocalAllocationMustBeUnique() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        try (HikariDataSource backgroundControl = pool("control");
             var owner = new PublicationSegmentOwnerRegistry(segments,
                     new MysqlSegmentPublicationLedger(backgroundControl), 8, 60_000, 10_000, 100);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = new CountDownLatch(8);
            var start = new CountDownLatch(1);
            var assignments = new ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 8; index++) {
                assignments.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return owner.allocate(100).commitTs();
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            var slots = new java.util.HashSet<Long>();
            for (var assignment : assignments) {
                slots.add(assignment.get(10, TimeUnit.SECONDS));
            }
            assertEquals(8, slots.size());
            assertEquals(101, slots.stream().mapToLong(Long::longValue).min().orElseThrow());
            assertEquals(108, slots.stream().mapToLong(Long::longValue).max().orElseThrow());
        }
    }

    /** 决议响应未知后本地 owner 停止续租该段，让持久到期接管释放阻塞槽。 */
    @Test
    void segmentedOwnerRegistryMustStopRenewalAfterUnknownDecision() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        try (HikariDataSource backgroundControl = pool("control");
             var owner = new PublicationSegmentOwnerRegistry(segments,
                     new MysqlSegmentPublicationLedger(backgroundControl), 4, 60_000, 10_000, 100)) {
            var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            metadata.registerParticipants(writer.txId(), Set.of("s1"));
            var assignment = owner.allocate(writer.startTs());
            try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_txn add constraint mvcc_test_owner_unknown "
                        + "check (status <> 'COMMITTED')");
            }
            try {
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> owner.decide(assignment,
                                writer.txId(), writer.deadlineMillis(), Set.of("s1"))).errorCode());
            } finally {
                try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
                    statement.execute("alter table mvcc_txn drop check mvcc_test_owner_unknown");
                }
            }
            assertEquals(0, owner.refresh());
            assertEquals(1, segments.segmentLease(assignment.segmentStart()).orElseThrow().generation());
            try (Connection connection = control.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                    update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                    """)) {
                expire.setLong(1, assignment.segmentStart());
                expire.executeUpdate();
            }
            assertEquals(4, segments.sealExpiredSegment(assignment.segmentStart()));
            assertEquals(104, segments.advancePublished(10));
        }
    }

    /** 预留已提交但响应丢失后，后台重放同一请求键并封存未交付槽。 */
    @Test
    void segmentedOwnerRegistryMustRecoverLostReservationResponse() {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var loseCommitResponse = new AtomicBoolean(true);
        DataSource flakyForeground = loseFirstCommitResponse(control, loseCommitResponse);
        try (HikariDataSource backgroundControl = pool("control");
             var owner = new PublicationSegmentOwnerRegistry(
                     new MysqlSegmentPublicationLedger(flakyForeground),
                     new MysqlSegmentPublicationLedger(backgroundControl),
                     4, 60_000, 10_000, 100)) {
            assertThrows(MvccException.class, () -> owner.allocate(100));
            assertFalse(loseCommitResponse.get());
            assertTrue(segments.segmentLease(101).isPresent());
            assertEquals(100, segments.currentPublished());
            assertTrue(owner.refresh() > 0);
            assertTrue(segments.segmentLease(101).isEmpty());
            assertEquals(104, segments.advancePublished(10));
            assertEquals(105, segments.reserveSegment("owner-b", "reservation-b", 1, 60_000).firstTs());
        }
    }

    /** 整批决议 SQL 失败后 owner 放弃该段，避免未知槽被持续续租钉住水位。 */
    @Test
    void segmentedOwnerRegistryMustFenceUnknownBatchDecision() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        try (HikariDataSource backgroundControl = pool("control");
             var owner = new PublicationSegmentOwnerRegistry(segments,
                     new MysqlSegmentPublicationLedger(backgroundControl), 4, 60_000, 10_000, 100)) {
            var first = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            var second = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            metadata.registerParticipants(first.txId(), Set.of("s1"));
            metadata.registerParticipants(second.txId(), Set.of("s1"));
            var firstSlot = owner.allocate(first.startTs());
            var secondSlot = owner.allocate(second.startTs());
            var batch = List.of(
                    new PublicationSegmentOwnerRegistry.CommitRequest(firstSlot,
                            first.txId(), first.deadlineMillis(), Set.of("s1")),
                    new PublicationSegmentOwnerRegistry.CommitRequest(secondSlot,
                            second.txId(), second.deadlineMillis(), Set.of("s1")));
            try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("alter table mvcc_publication_segment_slot_v1 "
                        + "add constraint mvcc_test_owner_batch_unknown check (status <> 'COMMITTED')");
            }
            try {
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> owner.decideBatch(batch)).errorCode());
            } finally {
                try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
                    statement.execute("alter table mvcc_publication_segment_slot_v1 "
                            + "drop check mvcc_test_owner_batch_unknown");
                }
            }
            assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
            assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
            assertEquals(0, owner.refresh());
            assertEquals(1, segments.segmentLease(firstSlot.segmentStart()).orElseThrow().generation());
            try (Connection connection = control.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                    update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                    """)) {
                expire.setLong(1, firstSlot.segmentStart());
                expire.executeUpdate();
            }
            assertEquals(4, segments.sealExpiredSegment(firstSlot.segmentStart()));
            assertEquals(104, segments.advancePublished(10));
        }
    }

    /** 显式分段运行时两分片提交后，新 RC 看见全部写入而旧 RR 固定旧快照。 */
    @Test
    void segmentedRuntimeMustPreserveGlobalRcAndRrAcrossTwoShards() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     options, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "segment:runtime:left", bytes("left"));
            manager.put(writer, "s2", "segment:runtime:right", bytes("right"));
            long commitTs = manager.commit(writer).commitTs();
            assertTrue(commitTs > repeatable.startTs());
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(committed.startTs() >= commitTs);
            assertTrue(manager.get(repeatable, "s1", "segment:runtime:left").isEmpty());
            assertTrue(manager.get(repeatable, "s2", "segment:runtime:right").isEmpty());
            assertArrayEquals(bytes("left"), manager.get(committed, "s1", "segment:runtime:left").orElseThrow());
            assertArrayEquals(bytes("right"), manager.get(committed, "s2", "segment:runtime:right").orElseThrow());
            manager.commit(repeatable);
            manager.commit(committed);
            assertTrue(runtime.maintenanceScheduler().publicationStats().runCount() > 0);
            assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
            assertEquals(0, runtime.maintenanceScheduler().publicationStats().failureCount());
        }
    }

    /** 中心持久凭据直达两分片时，旧 RR 与新 RC 仍遵守同一发布水位。 */
    @Test
    void segmentedProofMaterializationMustPreserveGlobalSnapshots() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, false, 2, false, false, false, true, true);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     options, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "segment:proof:left", bytes("left"));
            manager.put(writer, "s2", "segment:proof:right", bytes("right"));
            long commitTs = manager.commit(writer).commitTs();
            assertEquals(PublicationEntryStatus.COMMITTED,
                    segments.findByTxId(writer.txId()).orElseThrow().status());
            assertTrue(segments.currentPublished() >= commitTs);
            Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(newReader.startTs() >= commitTs);
            assertTrue(manager.get(oldReader, "s1", "segment:proof:left").isEmpty());
            assertTrue(manager.get(oldReader, "s2", "segment:proof:right").isEmpty());
            assertArrayEquals(bytes("left"), manager.get(newReader, "s1", "segment:proof:left").orElseThrow());
            assertArrayEquals(bytes("right"), manager.get(newReader, "s2", "segment:proof:right").orElseThrow());
            manager.commit(oldReader);
            manager.commit(newReader);
            assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
        }
    }

    /** 分片已按中心凭据提交但响应丢失时，前台返回未知并由恢复重放原决议。 */
    @Test
    void segmentedProofMaterializationLostResponseMustRecover() throws Exception {
        var foreground = new MysqlSegmentPublicationLedger(control);
        foreground.activateFromLedger();
        var metadata = new MysqlTransactionMetadataStore(control);
        var responseLost = new MysqlStorageAdapter("s2", secondData, metadata) {
            /** 在本地持久提交后模拟响应丢失。 */
            @Override
            public void commitDecided(MysqlSegmentPublicationLedger.CommittedProof proof,
                                      List<Mutation> mutations, boolean cleanupIntents) {
                super.commitDecided(proof, mutations, cleanupIntents);
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "simulated lost proof materialization response", false, proof.txId());
            }
        };
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two")) {
            var background = new MysqlSegmentPublicationLedger(backgroundControl);
            var backgroundMetadata = new MysqlTransactionMetadataStore(backgroundControl);
            var recovery = new SegmentPublicationRecoveryManager(background, backgroundMetadata,
                    Map.of("s1", new MysqlStorageAdapter("s1", backgroundFirst, backgroundMetadata),
                            "s2", new MysqlStorageAdapter("s2", backgroundSecond, backgroundMetadata)));
            var runner = new SegmentPublicationRecoveryRunner(background, recovery, backgroundMetadata);
            try (var scheduler = new SegmentPublicationMaintenanceScheduler(runner, background,
                    null, 10, 256, 1, 30_000, 10, 1_000);
                 var owner = new PublicationSegmentOwnerRegistry(foreground, background,
                         8, 5_000, 1_000, 100);
                 var manager = TransactionManager.forSegment(new MysqlTimestampOracle(control),
                         metadata, options,
                         Map.of("s1", new MysqlStorageAdapter("s1", firstData, metadata),
                                 "s2", responseLost),
                         NoOpTransactionMetrics.INSTANCE, foreground, owner, scheduler,
                         null, false, false, true)) {
                scheduler.start();
                Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
                Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(writer, "s1", "segment:proof:lost:left", bytes("left"));
                manager.put(writer, "s2", "segment:proof:lost:right", bytes("right"));
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> manager.commit(writer)).errorCode());
                assertEquals(PublicationEntryStatus.COMMITTED,
                        foreground.findByTxId(writer.txId()).orElseThrow().status());
                assertEquals(TxStatus.COMMITTED, responseLost.participantRecord(writer.txId()).orElseThrow().status());
                assertTrue(recovery.recover(writer.txId()));
                Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
                assertTrue(manager.get(oldReader, "s1", "segment:proof:lost:left").isEmpty());
                assertTrue(manager.get(oldReader, "s2", "segment:proof:lost:right").isEmpty());
                assertArrayEquals(bytes("left"),
                        manager.get(newReader, "s1", "segment:proof:lost:left").orElseThrow());
                assertArrayEquals(bytes("right"),
                        manager.get(newReader, "s2", "segment:proof:lost:right").orElseThrow());
                manager.commit(oldReader);
                manager.commit(newReader);
            }
        }
    }

    /** 提交后的发布唤醒须早于长周期兜底，且两分片新旧快照保持一致。 */
    @Test
    void segmentedPublicationWakeMustConfirmBeforePeriodicFallback() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 30_000, 50, false, 2, false, true);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     options, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "segment:wake:left", bytes("left"));
            manager.put(writer, "s2", "segment:wake:right", bytes("right"));
            long started = System.nanoTime();
            long commitTs = manager.commit(writer).commitTs();
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5));
            assertTrue(segments.currentPublished() >= commitTs);
            Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(manager.get(oldReader, "s1", "segment:wake:left").isEmpty());
            assertTrue(manager.get(oldReader, "s2", "segment:wake:right").isEmpty());
            assertArrayEquals(bytes("left"), manager.get(newReader, "s1", "segment:wake:left").orElseThrow());
            assertArrayEquals(bytes("right"), manager.get(newReader, "s2", "segment:wake:right").orElseThrow());
            manager.commit(oldReader);
            manager.commit(newReader);
            assertEquals(0, runtime.maintenanceScheduler().publicationStats().failureCount());
        }
    }

    /** 发布已前进但一个分片仍在物化时，RC 必须解析已提交意向且提交尚未返回。 */
    @Test
    void segmentedOverlapMustExposeCommittedIntentBeforeDelayedMaterialization() throws Exception {
        var foreground = new MysqlSegmentPublicationLedger(control);
        foreground.activateFromLedger();
        var metadata = new MysqlTransactionMetadataStore(control);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var failNextCommit = new AtomicBoolean();
        var delayed = new MysqlStorageAdapter("s2", secondData, metadata) {
            /** 控制第二分片本地版本物化完成时机。 */
            @Override
            public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("delayed participant was not released");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("delayed participant interrupted", ex);
                }
                if (failNextCommit.getAndSet(false)) {
                    throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                            "simulated participant materialization failure", false, txId);
                }
                super.commit(txId, commitTs, mutations, cleanupIntents);
            }
        };
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var background = new MysqlSegmentPublicationLedger(backgroundControl);
            var backgroundMetadata = new MysqlTransactionMetadataStore(backgroundControl);
            var recovery = new SegmentPublicationRecoveryManager(background, backgroundMetadata,
                    Map.of("s1", new MysqlStorageAdapter("s1", backgroundFirst, backgroundMetadata),
                            "s2", new MysqlStorageAdapter("s2", backgroundSecond, backgroundMetadata)));
            var runner = new SegmentPublicationRecoveryRunner(background, recovery, backgroundMetadata);
            try (var scheduler = new SegmentPublicationMaintenanceScheduler(runner, background,
                    null, 10, 256, 1, 30_000, 10, 1_000, true);
                 var owner = new PublicationSegmentOwnerRegistry(foreground, background,
                         8, 5_000, 1_000, 100);
                 var manager = TransactionManager.forSegment(new MysqlTimestampOracle(control),
                         metadata, options,
                         Map.of("s1", new MysqlStorageAdapter("s1", firstData, metadata), "s2", delayed),
                         NoOpTransactionMetrics.INSTANCE, foreground, owner, scheduler,
                         null, false, true)) {
                scheduler.start();
                Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
                Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(writer, "s1", "segment:overlap:left", bytes("left"));
                manager.put(writer, "s2", "segment:overlap:right", bytes("right"));
                var commit = workers.submit(() -> manager.commit(writer));
                try {
                    assertTrue(entered.await(10, TimeUnit.SECONDS));
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (foreground.currentPublished() <= oldReader.startTs()
                            && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertTrue(foreground.currentPublished() > oldReader.startTs());
                    assertFalse(commit.isDone());
                    Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
                    assertTrue(manager.get(oldReader, "s1", "segment:overlap:left").isEmpty());
                    assertTrue(manager.get(oldReader, "s2", "segment:overlap:right").isEmpty());
                    assertArrayEquals(bytes("left"),
                            manager.get(newReader, "s1", "segment:overlap:left").orElseThrow());
                    assertArrayEquals(bytes("right"),
                            manager.get(newReader, "s2", "segment:overlap:right").orElseThrow());
                    manager.commit(newReader);
                    manager.commit(oldReader);
                } finally {
                    release.countDown();
                }
                assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, commit.get(10, TimeUnit.SECONDS).mode());
                failNextCommit.set(true);
                Transaction failedWriter = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(failedWriter, "s1", "segment:overlap:recover:left", bytes("left"));
                manager.put(failedWriter, "s2", "segment:overlap:recover:right", bytes("right"));
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> manager.commit(failedWriter)).errorCode());
                assertEquals(PublicationEntryStatus.COMMITTED,
                        foreground.findByTxId(failedWriter.txId()).orElseThrow().status());
                assertTrue(recovery.recover(failedWriter.txId()));
                Transaction recoveredReader = manager.begin(IsolationLevel.READ_COMMITTED);
                assertArrayEquals(bytes("left"),
                        manager.get(recoveredReader, "s1", "segment:overlap:recover:left").orElseThrow());
                assertArrayEquals(bytes("right"),
                        manager.get(recoveredReader, "s2", "segment:overlap:recover:right").orElseThrow());
                manager.commit(recoveredReader);
                assertEquals(0, scheduler.publicationStats().failureCount());
            }
        }
    }

    /** 分段单分片物理提交须绕过意向预写，且新旧快照仍遵守全局发布水位。 */
    @Test
    void segmentedDirectSingleShardMustPreserveGlobalSnapshotsWithoutIntent() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, false, 2, true);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData), Map.of("s1", backgroundFirst),
                     options, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "segment:direct", bytes("committed"));
            var result = manager.commit(writer);
            assertEquals(TransactionCommitMode.SINGLE_SHARD_FAST_PATH, result.mode());
            assertEquals(PublicationEntryStatus.COMMITTED,
                    segments.findByTxId(writer.txId()).orElseThrow().status());
            assertEquals(result.commitTs(), segments.currentPublished());
            try (Connection connection = firstData.getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                         "select count(*) from mvcc_intent where tx_id = ?")) {
                statement.setLong(1, writer.txId());
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals(0, rows.getLong(1));
                }
            }
            Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(manager.get(oldReader, "s1", "segment:direct").isEmpty());
            assertArrayEquals(bytes("committed"),
                    manager.get(newReader, "s1", "segment:direct").orElseThrow());
            manager.commit(oldReader);
            manager.commit(newReader);
            assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
        }
    }

    /** 旧快照对同一键直接提交失败后须形成已中止槽并解除后续发布阻塞。 */
    @Test
    void segmentedDirectConflictMustReleaseClaimedSlot() throws InterruptedException {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, false, 2, true);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData), Map.of("s1", backgroundFirst),
                     options, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction stale = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction winner = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(winner, "s1", "segment:direct:conflict", bytes("winner"));
            long winnerTs = manager.commit(winner).commitTs();
            manager.put(stale, "s1", "segment:direct:conflict", bytes("stale"));
            assertEquals(ErrorCode.WRITE_CONFLICT,
                    assertThrows(MvccException.class, () -> manager.commit(stale)).errorCode());
            assertEquals(PublicationEntryStatus.ABORTED,
                    segments.findByTxId(stale.txId()).orElseThrow().status());
            long publicationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (segments.currentPublished() < winnerTs + 1
                    && System.nanoTime() < publicationDeadline) {
                Thread.sleep(10);
            }
            assertTrue(segments.currentPublished() >= winnerTs + 1);
            manager.rollback(stale);
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("winner"),
                    manager.get(reader, "s1", "segment:direct:conflict").orElseThrow());
            manager.commit(reader);
        }
    }

    /** 同段批量中心决议并发返回后，新 RC 读全批结果，旧 RR 读仍固定旧水位。 */
    @Test
    void segmentedBatchRuntimeMustPreserveGlobalRcAndRr() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(32, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, true, 2);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     options, runtimeOptions, NoOpTransactionMetrics.INSTANCE);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            TransactionManager manager = runtime.transactionManager();
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            var ready = new CountDownLatch(8);
            var start = new CountDownLatch(1);
            var decisions = new ArrayList<java.util.concurrent.Future<Long>>();
            for (int index = 0; index < 8; index++) {
                final int key = index;
                decisions.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
                    manager.put(writer, "s1", "segment:batch:left:" + key, bytes("left:" + key));
                    manager.put(writer, "s2", "segment:batch:right:" + key, bytes("right:" + key));
                    return manager.commit(writer).commitTs();
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            long newest = 100;
            for (var decision : decisions) {
                newest = Math.max(newest, decision.get(30, TimeUnit.SECONDS));
            }
            assertTrue(segments.currentPublished() >= newest);
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            for (int index = 0; index < 8; index++) {
                assertTrue(manager.get(oldReader, "s1", "segment:batch:left:" + index).isEmpty());
                assertArrayEquals(bytes("left:" + index), manager.get(committed,
                        "s1", "segment:batch:left:" + index).orElseThrow());
                assertArrayEquals(bytes("right:" + index), manager.get(committed,
                        "s2", "segment:batch:right:" + index).orElseThrow());
            }
            manager.commit(oldReader);
            manager.commit(committed);
            assertTrue(manager.segmentBatchStats().groups() > 0);
            assertEquals(0, manager.segmentBatchStats().unknownGroups());
        }
    }

    /** 批处理入口关闭后已分配的槽仍同步决议，不能遗留长期 PENDING 缺口。 */
    @Test
    void segmentedBatcherClosedQueueMustCommitAssignedSlotDirectly() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("s1", firstData, metadata);
        try (HikariDataSource backgroundControl = pool("control");
             var owner = new PublicationSegmentOwnerRegistry(segments,
                     new MysqlSegmentPublicationLedger(backgroundControl), 4, 60_000, 10_000, 100)) {
            var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            metadata.registerParticipants(writer.txId(), Set.of("s1"));
            shard.prewrite(writer.txId(), writer.startTs(),
                    List.of(Mutation.put("segment:closed-batch", bytes("value"))));
            var assignment = owner.allocate(writer.startTs());
            var batcher = new PublicationSegmentCommitBatcher(owner, 1, 4, 1);
            batcher.close(5_000);
            assertEquals(101, batcher.commit(assignment, writer.txId(),
                    writer.deadlineMillis(), Set.of("s1")));
            assertEquals(1, batcher.stats().synchronousFallbacks());
            assertEquals(101, segments.advancePublished(10));
            assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        }
    }

    /** 分段模式缓冲写在预写前回滚，不应制造占位槽或中心写决议。 */
    @Test
    void segmentedRuntimeRollbackBeforePrewriteMustStayReadOnly() {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (HikariDataSource backgroundControl = pool("control");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two");
             var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                     Map.of("s1", firstData, "s2", secondData),
                     Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                     options, SegmentRuntimeOptions.defaults(), NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "segment:cancelled", bytes("temporary"));
            manager.rollback(tx);
            assertTrue(segments.snapshotExpiry(tx.txId()).isEmpty());
            assertTrue(manager.getRecord(tx.txId()).isEmpty());
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(manager.get(reader, "s1", "segment:cancelled").isEmpty());
            manager.commit(reader);
            assertEquals(100, segments.currentPublished());
        }
    }

    /** 后台控制池误接到另一数据库时，运行时必须在启动任务前拒绝。 */
    @Test
    void ledgerRuntimeMustRejectDifferentBackgroundAuthority() {
        var transactionOptions = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (HikariDataSource wrongControl = pool("shard_one");
             HikariDataSource backgroundFirst = pool("shard_one");
             HikariDataSource backgroundSecond = pool("shard_two")) {
            assertThrows(IllegalArgumentException.class, () -> MysqlLedgerTransactionRuntime.open(
                    control, wrongControl,
                    Map.of("s1", firstData, "s2", secondData),
                    Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                    transactionOptions, LedgerRuntimeOptions.defaults(), NoOpTransactionMetrics.INSTANCE));
        }
    }

    /** 未持久化意向的缓冲写回滚只清理新租约，不制造错误开始时间的参与者行。 */
    @Test
    void bufferedRollbackBeforePrewriteMustNotCreateParticipantDecision() {
        try (var manager = manager()) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "cancelled", bytes("temporary"));
            manager.rollback(tx);
            assertTrue(ledger.snapshotExpiry(tx.txId()).isEmpty());
            assertFalse(manager.getRecord(tx.txId()).isPresent());
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(manager.get(reader, "s1", "cancelled").isEmpty());
            manager.commit(reader);
        }
    }

    /** 提交预留已落盘但响应未知时，回滚须 fencing 参与者并解除水位阻塞。 */
    @Test
    void rollbackMustResolveDurablePendingReservation() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        try (var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options, Map.of("s1", first),
                NoOpTransactionMetrics.INSTANCE, ledger)) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "pending", bytes("value"));
            metadata.createWithParticipants(tx.txId(), tx.startTs(), tx.isolationLevel(), java.util.Set.of("s1"));
            tx.metadataPersisted(true);
            long commitTs = ledger.reserveCommit(tx.txId(), tx.deadlineMillis());
            manager.rollback(tx);
            assertEquals(TxStatus.ABORTED, metadata.get(tx.txId()).orElseThrow().status());
            assertEquals(PublicationEntryStatus.ABORTED, ledger.findByTxId(tx.txId()).orElseThrow().status());
            assertEquals(commitTs, ledger.currentPublished());
            assertTrue(ledger.snapshotExpiry(tx.txId()).isEmpty());
        }
    }

    /** 把常量转换为测试用二进制值。 */
    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** 只在首次 JDBC commit 真正落盘后丢失响应，模拟预留结果未知。 */
    private static DataSource loseFirstCommitResponse(DataSource delegate, AtomicBoolean pending) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == arguments[0];
                            default -> "lost-commit-response-datasource";
                        };
                    }
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (!method.getName().equals("getConnection")) {
                            return result;
                        }
                        Connection actual = (Connection) result;
                        return Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                new Class<?>[]{Connection.class}, (view, call, values) -> {
                                    if (call.getDeclaringClass() == Object.class) {
                                        return switch (call.getName()) {
                                            case "hashCode" -> System.identityHashCode(view);
                                            case "equals" -> view == values[0];
                                            default -> "lost-commit-response-connection";
                                        };
                                    }
                                    try {
                                        Object outcome = call.invoke(actual, values);
                                        if (call.getName().equals("commit")
                                                && pending.compareAndSet(true, false)) {
                                            throw new SQLException("committed but response was lost", "08S01");
                                        }
                                        return outcome;
                                    } catch (InvocationTargetException ex) {
                                        throw ex.getCause();
                                    }
                                });
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }
}

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

package com.flyingzc.azir.txn;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.api.TransactionCommitMode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.gc.NoOpSafePointStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.metrics.NoOpTransactionMetrics;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;
import com.flyingzc.azir.publication.SegmentPublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.SegmentPublicationRecoveryManager;
import com.flyingzc.azir.publication.SegmentPublicationRecoveryRunner;
import com.flyingzc.azir.publication.PublicationEntryStatus;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
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

/**
 * Global RC/RR snapshot at transaction entrance for real two shard MySQL accounts.
 */
@Testcontainers
class LedgerTransactionManagerIntegrationTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");

    private static HikariDataSource control;

    private static HikariDataSource firstData;

    private static HikariDataSource secondData;

    private MysqlPublicationLedger ledger;

    /**
     * Creates a quarantine control library and two physical shard.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * Create a library with an in-container administrator and authorize quarantine test users.
     *
     * @param database database
     * @throws Exception if the operation cannot complete
     */
    private static void createDatabase(final String database) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + database + "`; "
                        + "grant all privileges on `" + database + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /**
     * Create separate libraries based on the same container port connection pool.
     *
     * @param database database
     * @return the operation result
     */
    private static HikariDataSource pool(final String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Closes the three connection pool created by this class.
     */
    @AfterAll
    static void closeDatabases() {
        if (control != null)
            control.close();
        if (firstData != null)
            firstData.close();
        if (secondData != null)
            secondData.close();
    }

    /**
     * Each example emptys the old time line and recreates the version account book status.
     *
     * @throws Exception if the operation cannot complete
     */
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

    /**
     * Clear shard intent, version and local terminal state, keep the initialised security watermark rows.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private void clearShard(final HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent");
            statement.executeUpdate("delete from mvcc_data");
            statement.executeUpdate("delete from mvcc_rollback");
            statement.executeUpdate("delete from mvcc_participant_txn");
            statement.executeUpdate("delete from mvcc_key_lock");
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null");
        }
    }

    /**
     * Creates a synchronized materialization account mode with the same control of authority connection 2 shard.
     *
     * @return the operation result
     */
    private TransactionManager manager() {
        return manager(false);
    }

    /**
     * Enables a combination of batch decision with verify and the normal RC/RR entry on demand.
     *
     * @param batchControlCommit batch control commit
     * @return the operation result
     */
    private TransactionManager manager(final boolean batchControlCommit) {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        var second = new MysqlStorageAdapter("s2", secondData, metadata);
        return new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, options, Map.of("s1", first, "s2", second),
                NoOpTransactionMetrics.INSTANCE, ledger, batchControlCommit, 2, true);
    }

    /**
     * The old MySQL manager must refuse to start during the construction period after durable has been activated.
     */
    @Test
    void legacyManagerMustRejectActivatedLedgerBeforeBegin() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        MvccException rejected = assertThrows(MvccException.class, () -> new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(),
                Map.of("s1", first)));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
        assertTrue(rejected.getMessage().contains("activated publication mode"));
    }

    /**
     * The old manager must also stop during the construction period when the segment mode has been archived with the old time line.
     */
    @Test
    void legacyManagerMustRejectSegmentModeBeforeBegin() {
        new MysqlSegmentPublicationLedger(control).activateFromLedger();
        var metadata = new MysqlTransactionMetadataStore(control);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        MvccException rejected = assertThrows(MvccException.class, () -> new TransactionManager(new MysqlTimestampOracle(control), metadata,
                NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(),
                Map.of("s1", first)));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
        assertTrue(rejected.getMessage().contains("activated publication mode"));
    }

    /**
     * Transaction ID is independent of snapshot and external cross-shard commit is only visible for new RC statements.
     */
    @Test
    void readCommittedRefreshesWhileRepeatableReadKeepsItsPublishedSnapshot() {
        try (var manager = manager()) {
            Transaction seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(seed.txId() != seed.startTs());
            manager.put(seed, "s1", "account", bytes("old"));
            manager.put(seed, "s2", "ledger", bytes("old"));
            long firstCommitTs = manager.commit(seed).commitTs();
            Assertions.assertEquals(firstCommitTs, ledger.currentPublished());

            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals(firstCommitTs, repeatable.startTs());
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

    /**
     * Single shard direct commit skips intent prewrite and must still be issued to the new snapshot by account book watermark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleShardWriteUsesTheSameLedgerPublication() throws Exception {
        try (var manager = manager()) {
            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "single", bytes("value"));
            var result = manager.commit(writer);
            Assertions.assertEquals(TransactionCommitMode.SINGLE_SHARD_FAST_PATH, result.mode());
            Assertions.assertEquals(result.commitTs(), ledger.currentPublished());
            try (
                    Connection connection = firstData.getConnection(); Statement statement = connection.createStatement();
                    var intents = statement.executeQuery("select count(*) from mvcc_intent")) {
                assertTrue(intents.next());
                Assertions.assertEquals(0, intents.getInt(1));
            }
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("value"), manager.get(reader, "s1", "single").orElseThrow());
            assertTrue(manager.get(repeatable, "s1", "single").isEmpty());
            manager.commit(reader);
            manager.commit(repeatable);
        }
    }

    /**
     * The default account builder maintains a single shard intent commit higher than throughput, and runs directly 1PC only when it is visible.
     */
    @Test
    void defaultLedgerSingleShardCommitMustRemainIntentBased() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        try (
                var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                        NoOpSafePointStore.INSTANCE, options, Map.of("s1", first),
                        NoOpTransactionMetrics.INSTANCE, ledger)) {
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "intent:default", bytes("value"));
            var result = manager.commit(writer);
            Assertions.assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, result.mode());
            Assertions.assertEquals(result.commitTs(), ledger.currentPublished());
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("value"), manager.get(reader, "s1", "intent:default").orElseThrow());
            manager.commit(reader);
        }
    }

    /**
     * Concurrent Single shard has confirmed that the global RC/RR snapshot border is still being complied with once a shared control transaction has been set aside.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void batchedLocalReservationsMustPreserveReadCommittedAndRepeatableRead() throws Exception {
        try (
                var manager = manager(true);
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
            Assertions.assertEquals(newest, ledger.currentPublished());
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

    /**
     * Single shard direct commit conflict requires local ABORTED proof before PENDING watermark blockages are removed.
     */
    @Test
    void conflictingSingleShardDirectCommitMustAbortReservedSlot() {
        try (var manager = manager()) {
            Transaction stale = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction winner = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(winner, "s1", "direct:conflict", bytes("winner"));
            long winnerTs = manager.commit(winner).commitTs();
            manager.put(stale, "s1", "direct:conflict", bytes("stale"));
            MvccException conflict = assertThrows(MvccException.class, () -> manager.commit(stale));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, conflict.errorCode());
            Assertions.assertEquals(PublicationEntryStatus.ABORTED,
                    ledger.findByTxId(stale.txId()).orElseThrow().status());
            Assertions.assertEquals(winnerTs + 1, ledger.currentPublished());
            manager.rollback(stale);
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("winner"), manager.get(reader, "s1", "direct:conflict").orElseThrow());
            manager.commit(reader);
        }
    }

    /**
     * The batch pre-encumbered write conflict muster rollback its own time slot and awaken the transaction waiting to be released consecutively.
     */
    @Test
    void batchedDirectConflictMustReleasePendingPublicationSlot() {
        try (var manager = manager(true)) {
            Transaction stale = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction winner = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(winner, "s1", "local:batch:conflict", bytes("winner"));
            long winnerTs = manager.commit(winner).commitTs();
            manager.put(stale, "s1", "local:batch:conflict", bytes("stale"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT,
                    assertThrows(MvccException.class, () -> manager.commit(stale)).errorCode());
            Assertions.assertEquals(PublicationEntryStatus.ABORTED,
                    ledger.findByTxId(stale.txId()).orElseThrow().status());
            Assertions.assertEquals(winnerTs + 1, ledger.currentPublished());
            manager.rollback(stale);
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("winner"), manager.get(reader, "s1", "local:batch:conflict").orElseThrow());
            manager.commit(reader);
        }
    }

    /**
     * Local When the commit has landed but responded to loss, foreground completes the centre decision with durable terminal state.
     */
    @Test
    void lostDirectCommitResponseMustResolveFromLocalCommittedProof() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata) {

            /**
             * Simulates the loss of a network response after real local commit is completed.
             *
             * @param txId transaction identifier
             * @param startTs transaction start timestamp
             * @param commitTs durable commit timestamp
             * @param mutations transaction write set
             * @throws MvccException if the operation cannot complete
             */
            @Override
            public void directCommit(final long txId, final long startTs, final long commitTs, final java.util.List<Mutation> mutations) {
                super.directCommit(txId, startTs, commitTs, mutations);
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "simulated lost local commit response", false, txId);
            }
        };
        try (
                var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                        NoOpSafePointStore.INSTANCE, options, Map.of("s1", first),
                        NoOpTransactionMetrics.INSTANCE, ledger, false, 2, true)) {
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "direct:response-lost", bytes("committed"));
            var result = manager.commit(writer);
            Assertions.assertEquals(TransactionCommitMode.SINGLE_SHARD_FAST_PATH, result.mode());
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
            Assertions.assertEquals(PublicationEntryStatus.COMMITTED,
                    ledger.findByTxId(writer.txId()).orElseThrow().status());
            Assertions.assertEquals(result.commitTs(), ledger.currentPublished());
            Transaction reader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertArrayEquals(bytes("committed"),
                    manager.get(reader, "s1", "direct:response-lost").orElseThrow());
            manager.commit(reader);
        }
    }

    /**
     * Concurrent batch decision returns, new RC snapshot sees all written and old RR snapshot remains as it is.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void batchedCrossShardCommitsPreserveGlobalReadCommittedAndRepeatableRead() throws Exception {
        try (
                var manager = manager(true);
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
            Assertions.assertEquals(newest, ledger.currentPublished());
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

    /**
     * Center decision is capable of parsing commit intent and the old RR is still fixed with the old snapshot when participant has been released.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void asyncLedgerCommitMustRemainVisibleWhileParticipantCommitIsBlocked() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(2), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        var second = new MysqlStorageAdapter("s2", secondData, metadata);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (
                var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
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
                Assertions.assertEquals(TransactionCommitMode.ASYNC_PRIMARY_COMMIT, result.mode());
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertTrue(first.participantRecord(writer.txId()).orElseThrow().status() != TxStatus.COMMITTED
                        || second.participantRecord(writer.txId()).orElseThrow().status() != TxStatus.COMMITTED);

                Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
                Assertions.assertEquals(result.commitTs(), committed.startTs());
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

    /**
     * Suspend background materialization call with proxy to keep real shard prewrite and snapshot reading behaviors.
     *
     * @param delegate delegate
     * @param entered entered
     * @param release release
     * @return the operation result
     */
    private StorageAdapter blockParticipantCommit(final StorageAdapter delegate, final CountDownLatch entered,
                                                  final CountDownLatch release) {
        return (StorageAdapter) Proxy.newProxyInstance(StorageAdapter.class.getClassLoader(),
                new Class<?>[]{StorageAdapter.class}, (proxy, method, args) -> {
                    if (method.getName().equals("commit")) {
                        entered.countDown();
                        release.await();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (final InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /**
     * Concurrent batch begin produces an independent transaction ID, which is overwrited by a new RC/RR snapshot after it has been confirmed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void batchedLedgerBeginMustPreserveGlobalSnapshotOrder() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        var second = new MysqlStorageAdapter("s2", secondData, metadata);
        try (
                var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
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
            Assertions.assertEquals(8, readers.stream().mapToLong(Transaction::txId).distinct().count());
            assertTrue(readers.stream().allMatch(tx -> tx.startTs() == 100));
            readers.forEach(manager::commit);

            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "begin:batch:left", bytes("left"));
            manager.put(writer, "s2", "begin:batch:right", bytes("right"));
            long committedTs = manager.commit(writer).commitTs();
            Transaction committed = manager.begin(IsolationLevel.READ_COMMITTED);
            Transaction repeatable = manager.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals(committedTs, committed.startTs());
            Assertions.assertEquals(committedTs, repeatable.startTs());
            assertArrayEquals(bytes("left"), manager.get(committed, "s1", "begin:batch:left").orElseThrow());
            assertArrayEquals(bytes("right"), manager.get(repeatable, "s2", "begin:batch:right").orElseThrow());
            manager.commit(committed);
            manager.commit(repeatable);
        }
    }

    /**
     * When a visible run binds an independent background pool, commit and two shard can run recovery and GC in cycles.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void explicitLedgerRuntimeMustUseIndependentBackgroundPools() throws Exception {
        var transactionOptions = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new LedgerRuntimeOptions(10, 10, 20, 50, true, 2, false, false, 1);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(commitTs, ledger.currentPublished());
            // Simulate decision control coordinator exits, and the background pool must be independently completed local participant.
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
            Assertions.assertEquals(TxStatus.COMMITTED, abandonedShard.participantRecord(abandoned.txId())
                    .orElseThrow().status());
            Assertions.assertEquals(recoveredCommitTs, ledger.currentPublished());
            assertTrue(runtime.maintenanceScheduler().recoveryStats().runCount() > 0);
            Assertions.assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
        }
    }

    /**
     * The coordinator snapshot is exempt from read-only by transaction lease and maintains the visibility of global RR and RC after first writing has been upgraded.
     */
    @Test
    void coordinatorSnapshotsMustPromoteWritesAndPreserveIsolation() {
        var transactionOptions = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new LedgerRuntimeOptions(10, 10, 20, 50,
                true, 2, false, false, 1, true, 120_000, 1_000);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(commitTs, ledger.currentPublished());
            manager.commit(repeatable);
            manager.commit(committed);
            assertTrue(runtime.maintenanceScheduler().gcStats().failureCount() == 0);
        }
    }

    /**
     * The owner uses the latest durable algebra decision and closes undistributed tailings when free.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedOwnerRegistryMustRenewAndSealIdleTail() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        try (
                HikariDataSource backgroundControl = pool("control");
                var owner = new PublicationSegmentOwnerRegistry(segments,
                        new MysqlSegmentPublicationLedger(backgroundControl), 4, 60_000, 10_000, 100)) {
            var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            metadata.registerParticipants(writer.txId(), Set.of("s1"));
            first.prewrite(writer.txId(), writer.startTs(),
                    java.util.List.of(Mutation.put("segment:registry", bytes("value"))));
            var assignment = owner.allocate(writer.startTs());
            Assertions.assertEquals(101, assignment.commitTs());
            assertTrue(owner.refresh() > 0);
            Assertions.assertEquals(2, segments.segmentLease(assignment.segmentStart()).orElseThrow().generation());
            Assertions.assertEquals(101, owner.decide(assignment, writer.txId(), writer.deadlineMillis(), Set.of("s1")));
            Assertions.assertEquals(101, segments.advancePublished(10));
            Thread.sleep(150);
            assertTrue(owner.refresh() > 0);
            Assertions.assertEquals(104, segments.advancePublished(10));
            assertTrue(segments.segmentLease(assignment.segmentStart()).isEmpty());
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        }
    }

    /**
     * Concurrent local CAS slots in the same durable segment cannot be re-numbered or crossed the upper boundary of the section.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedOwnerRegistryConcurrentLocalAllocationMustBeUnique() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(8, slots.size());
            Assertions.assertEquals(101, slots.stream().mapToLong(Long::longValue).min().orElseThrow());
            Assertions.assertEquals(108, slots.stream().mapToLong(Long::longValue).max().orElseThrow());
        }
    }

    /**
     * Decision In response to unknown local owner stop renewing this paragraph and allow durable to take over the release of the blocker.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedOwnerRegistryMustStopRenewalAfterUnknownDecision() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        try (
                HikariDataSource backgroundControl = pool("control");
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
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> owner.decide(assignment,
                                writer.txId(), writer.deadlineMillis(), Set.of("s1"))).errorCode());
            } finally {
                try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
                    statement.execute("alter table mvcc_txn drop check mvcc_test_owner_unknown");
                }
            }
            Assertions.assertEquals(0, owner.refresh());
            Assertions.assertEquals(1, segments.segmentLease(assignment.segmentStart()).orElseThrow().generation());
            try (Connection connection = control.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                    update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                    """)) {
                expire.setLong(1, assignment.segmentStart());
                expire.executeUpdate();
            }
            Assertions.assertEquals(4, segments.sealExpiredSegment(assignment.segmentStart()));
            Assertions.assertEquals(104, segments.advancePublished(10));
        }
    }

    /**
     * Presets commit, but following the loss of the response background replay key with the same request and seals the undelivered slot.
     */
    @Test
    void segmentedOwnerRegistryMustRecoverLostReservationResponse() {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var loseCommitResponse = new AtomicBoolean(true);
        DataSource flakyForeground = loseFirstCommitResponse(control, loseCommitResponse);
        try (
                HikariDataSource backgroundControl = pool("control");
                var owner = new PublicationSegmentOwnerRegistry(
                        new MysqlSegmentPublicationLedger(flakyForeground),
                        new MysqlSegmentPublicationLedger(backgroundControl),
                        4, 60_000, 10_000, 100)) {
            assertThrows(MvccException.class, () -> owner.allocate(100));
            assertFalse(loseCommitResponse.get());
            assertTrue(segments.segmentLease(101).isPresent());
            Assertions.assertEquals(100, segments.currentPublished());
            assertTrue(owner.refresh() > 0);
            assertTrue(segments.segmentLease(101).isEmpty());
            Assertions.assertEquals(104, segments.advancePublished(10));
            Assertions.assertEquals(105, segments.reserveSegment("owner-b", "reservation-b", 1, 60_000).firstTs());
        }
    }

    /**
     * The whole batch of decision SQLs failed and owner abandoned the paragraph to avoid an unknown slot being nailed to watermark by a continuous lease.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedOwnerRegistryMustFenceUnknownBatchDecision() throws Exception {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        try (
                HikariDataSource backgroundControl = pool("control");
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
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> owner.decideBatch(batch)).errorCode());
            } finally {
                try (Connection connection = control.getConnection(); Statement statement = connection.createStatement()) {
                    statement.execute("alter table mvcc_publication_segment_slot_v1 "
                            + "drop check mvcc_test_owner_batch_unknown");
                }
            }
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(first.txId()).orElseThrow().status());
            Assertions.assertEquals(TxStatus.ACTIVE, metadata.get(second.txId()).orElseThrow().status());
            Assertions.assertEquals(0, owner.refresh());
            Assertions.assertEquals(1, segments.segmentLease(firstSlot.segmentStart()).orElseThrow().generation());
            try (Connection connection = control.getConnection(); PreparedStatement expire = connection.prepareStatement("""
                    update mvcc_publication_segment_v1 set expires_at = 1 where segment_start = ?
                    """)) {
                expire.setLong(1, firstSlot.segmentStart());
                expire.executeUpdate();
            }
            Assertions.assertEquals(4, segments.sealExpiredSegment(firstSlot.segmentStart()));
            Assertions.assertEquals(104, segments.advancePublished(10));
        }
    }

    /**
     * After running two shard commit in the visible segment, the new RC sees all written and old RR fixed snapshot.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedRuntimeMustPreserveGlobalRcAndRrAcrossTwoShards() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
            Assertions.assertEquals(0, runtime.maintenanceScheduler().publicationStats().failureCount());
        }
    }

    /**
     * The old RR and the new RC still adhere to the same release watermark when the center durable receipt reaches two shard.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedProofMaterializationMustPreserveGlobalSnapshots() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, false, 2, false, false, false, true, true);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(PublicationEntryStatus.COMMITTED,
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
            Assertions.assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
        }
    }

    /**
     * Shard has been centered by receipt commit, but when the response is lost, foreground returns unknown and recovery replay original decision.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedProofMaterializationLostResponseMustRecover() throws Exception {
        var foreground = new MysqlSegmentPublicationLedger(control);
        foreground.activateFromLedger();
        var metadata = new MysqlTransactionMetadataStore(control);
        var responseLost = new MysqlStorageAdapter("s2", secondData, metadata) {

            /**
             * Simulates loss of response after local durable commit.
             *
             * @param proof proof
             * @param mutations transaction write set
             * @param cleanupIntents cleanup intents
             * @throws MvccException if the operation cannot complete
             */
            @Override
            public void commitDecided(final MysqlSegmentPublicationLedger.CommittedProof proof,
                                      final List<Mutation> mutations, final boolean cleanupIntents) {
                super.commitDecided(proof, mutations, cleanupIntents);
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "simulated lost proof materialization response", false, proof.txId());
            }
        };
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (
                HikariDataSource backgroundControl = pool("control");
                HikariDataSource backgroundFirst = pool("shard_one");
                HikariDataSource backgroundSecond = pool("shard_two")) {
            var background = new MysqlSegmentPublicationLedger(backgroundControl);
            var backgroundMetadata = new MysqlTransactionMetadataStore(backgroundControl);
            var recovery = new SegmentPublicationRecoveryManager(background, backgroundMetadata,
                    Map.of("s1", new MysqlStorageAdapter("s1", backgroundFirst, backgroundMetadata),
                            "s2", new MysqlStorageAdapter("s2", backgroundSecond, backgroundMetadata)));
            var runner = new SegmentPublicationRecoveryRunner(background, recovery, backgroundMetadata);
            try (
                    var scheduler = new SegmentPublicationMaintenanceScheduler(runner, background,
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
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> manager.commit(writer)).errorCode());
                Assertions.assertEquals(PublicationEntryStatus.COMMITTED,
                        foreground.findByTxId(writer.txId()).orElseThrow().status());
                Assertions.assertEquals(TxStatus.COMMITTED, responseLost.participantRecord(writer.txId()).orElseThrow().status());
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

    /**
     * The post- commit release must wake up before the end of a long cycle and is consistent with two shard s, old and new snapshot.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedPublicationWakeMustConfirmBeforePeriodicFallback() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 30_000, 50, false, 2, false, true);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(0, runtime.maintenanceScheduler().publicationStats().failureCount());
        }
    }

    /**
     * The RC must resolve commit intent and the commit has not yet returned when a shard is released.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedOverlapMustExposeCommittedIntentBeforeDelayedMaterialization() throws Exception {
        var foreground = new MysqlSegmentPublicationLedger(control);
        foreground.activateFromLedger();
        var metadata = new MysqlTransactionMetadataStore(control);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var failNextCommit = new AtomicBoolean();
        var delayed = new MysqlStorageAdapter("s2", secondData, metadata) {

            /**
             * Controls the second shard local version materialization completion time.
             *
             * @param txId transaction identifier
             * @param commitTs durable commit timestamp
             * @param mutations transaction write set
             * @param cleanupIntents cleanup intents
             * @throws IllegalStateException if the operation cannot complete
             * @throws MvccException if the operation cannot complete
             */
            @Override
            public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("delayed participant was not released");
                    }
                } catch (final InterruptedException ex) {
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
        try (
                HikariDataSource backgroundControl = pool("control");
                HikariDataSource backgroundFirst = pool("shard_one");
                HikariDataSource backgroundSecond = pool("shard_two");
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var background = new MysqlSegmentPublicationLedger(backgroundControl);
            var backgroundMetadata = new MysqlTransactionMetadataStore(backgroundControl);
            var recovery = new SegmentPublicationRecoveryManager(background, backgroundMetadata,
                    Map.of("s1", new MysqlStorageAdapter("s1", backgroundFirst, backgroundMetadata),
                            "s2", new MysqlStorageAdapter("s2", backgroundSecond, backgroundMetadata)));
            var runner = new SegmentPublicationRecoveryRunner(background, recovery, backgroundMetadata);
            try (
                    var scheduler = new SegmentPublicationMaintenanceScheduler(runner, background,
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
                Assertions.assertEquals(TransactionCommitMode.TWO_PHASE_COMMIT, commit.get(10, TimeUnit.SECONDS).mode());
                failNextCommit.set(true);
                Transaction failedWriter = manager.begin(IsolationLevel.REPEATABLE_READ);
                manager.put(failedWriter, "s1", "segment:overlap:recover:left", bytes("left"));
                manager.put(failedWriter, "s2", "segment:overlap:recover:right", bytes("right"));
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, () -> manager.commit(failedWriter)).errorCode());
                Assertions.assertEquals(PublicationEntryStatus.COMMITTED,
                        foreground.findByTxId(failedWriter.txId()).orElseThrow().status());
                assertTrue(recovery.recover(failedWriter.txId()));
                Transaction recoveredReader = manager.begin(IsolationLevel.READ_COMMITTED);
                assertArrayEquals(bytes("left"),
                        manager.get(recoveredReader, "s1", "segment:overlap:recover:left").orElseThrow());
                assertArrayEquals(bytes("right"),
                        manager.get(recoveredReader, "s2", "segment:overlap:recover:right").orElseThrow());
                manager.commit(recoveredReader);
                Assertions.assertEquals(0, scheduler.publicationStats().failureCount());
            }
        }
    }

    /**
     * Division shard Physical commit must be bypassed by intent prewrite and old and new snapshot still complies with global release watermark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedDirectSingleShardMustPreserveGlobalSnapshotsWithoutIntent() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, false, 2, true);
        try (
                HikariDataSource backgroundControl = pool("control");
                HikariDataSource backgroundFirst = pool("shard_one");
                var runtime = MysqlSegmentTransactionRuntime.open(control, backgroundControl,
                        Map.of("s1", firstData), Map.of("s1", backgroundFirst),
                        options, runtimeOptions, NoOpTransactionMetrics.INSTANCE)) {
            TransactionManager manager = runtime.transactionManager();
            Transaction oldReader = manager.begin(IsolationLevel.REPEATABLE_READ);
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "segment:direct", bytes("committed"));
            var result = manager.commit(writer);
            Assertions.assertEquals(TransactionCommitMode.SINGLE_SHARD_FAST_PATH, result.mode());
            Assertions.assertEquals(PublicationEntryStatus.COMMITTED,
                    segments.findByTxId(writer.txId()).orElseThrow().status());
            Assertions.assertEquals(result.commitTs(), segments.currentPublished());
            try (
                    Connection connection = firstData.getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "select count(*) from mvcc_intent where tx_id = ?")) {
                statement.setLong(1, writer.txId());
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next());
                    Assertions.assertEquals(0, rows.getLong(1));
                }
            }
            Transaction newReader = manager.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(manager.get(oldReader, "s1", "segment:direct").isEmpty());
            assertArrayEquals(bytes("committed"),
                    manager.get(newReader, "s1", "segment:direct").orElseThrow());
            manager.commit(oldReader);
            manager.commit(newReader);
            Assertions.assertEquals(0, runtime.maintenanceScheduler().recoveryStats().failureCount());
        }
    }

    /**
     * The old snapshot for the same key directly commit shall be formed after failure to form a suspended slot and release subsequent release block.
     *
     * @throws InterruptedException if the operation cannot complete
     */
    @Test
    void segmentedDirectConflictMustReleaseClaimedSlot() throws InterruptedException {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(8, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, false, 2, true);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT,
                    assertThrows(MvccException.class, () -> manager.commit(stale)).errorCode());
            Assertions.assertEquals(PublicationEntryStatus.ABORTED,
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

    /**
     * After the same paragraph batch centre decision concurrent returns, the new RC reads the full batch results and the old RR reading is still fixed to the old watermark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void segmentedBatchRuntimeMustPreserveGlobalRcAndRr() throws Exception {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        var runtimeOptions = new SegmentRuntimeOptions(32, 5_000, 1_000, 100,
                10, 256, 10, 20, 10, 50, true, 2);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(0, manager.segmentBatchStats().unknownGroups());
        }
    }

    /**
     * The slots that have been distributed following the closure of the batch are still synchronized with decision and cannot leave a long-term PENDING gap.
     */
    @Test
    void segmentedBatcherClosedQueueMustCommitAssignedSlotDirectly() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var shard = new MysqlStorageAdapter("s1", firstData, metadata);
        try (
                HikariDataSource backgroundControl = pool("control");
                var owner = new PublicationSegmentOwnerRegistry(segments,
                        new MysqlSegmentPublicationLedger(backgroundControl), 4, 60_000, 10_000, 100)) {
            var writer = segments.begin(IsolationLevel.REPEATABLE_READ, 60_000, true);
            metadata.registerParticipants(writer.txId(), Set.of("s1"));
            shard.prewrite(writer.txId(), writer.startTs(),
                    List.of(Mutation.put("segment:closed-batch", bytes("value"))));
            var assignment = owner.allocate(writer.startTs());
            var batcher = new PublicationSegmentCommitBatcher(owner, 1, 4, 1);
            batcher.close(5_000);
            Assertions.assertEquals(101, batcher.commit(assignment, writer.txId(),
                    writer.deadlineMillis(), Set.of("s1")));
            Assertions.assertEquals(1, batcher.stats().synchronousFallbacks());
            Assertions.assertEquals(101, segments.advancePublished(10));
            Assertions.assertEquals(TxStatus.COMMITTED, metadata.get(writer.txId()).orElseThrow().status());
        }
    }

    /**
     * The segment mode buffer is written before the prewrite rollback, and should not create a slot or central decision.
     */
    @Test
    void segmentedRuntimeRollbackBeforePrewriteMustStayReadOnly() {
        var segments = new MysqlSegmentPublicationLedger(control);
        segments.activateFromLedger();
        var options = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (
                HikariDataSource backgroundControl = pool("control");
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
            Assertions.assertEquals(100, segments.currentPublished());
        }
    }

    /**
     * The background control pool must be refused before the start of the task when another database is missed.
     */
    @Test
    void ledgerRuntimeMustRejectDifferentBackgroundAuthority() {
        var transactionOptions = new TransactionOptions(LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(), CrossShardCommitPolicy.parallel(), true, true, false);
        try (
                HikariDataSource wrongControl = pool("shard_one");
                HikariDataSource backgroundFirst = pool("shard_one");
                HikariDataSource backgroundSecond = pool("shard_two")) {
            assertThrows(IllegalArgumentException.class, () -> MysqlLedgerTransactionRuntime.open(
                    control, wrongControl,
                    Map.of("s1", firstData, "s2", secondData),
                    Map.of("s1", backgroundFirst, "s2", backgroundSecond),
                    transactionOptions, LedgerRuntimeOptions.defaults(), NoOpTransactionMetrics.INSTANCE));
        }
    }

    /**
     * Buffer rollback for durable storage intent only clears new lease and does not create participant rows at the wrong start time.
     */
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

    /**
     * Commit presets the drop-off but does not respond to an unknown message, rollback shall be funcing participant and remove watermark blockage.
     */
    @Test
    void rollbackMustResolveDurablePendingReservation() {
        var metadata = new MysqlTransactionMetadataStore(control);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.parallel(), true, true, false);
        var first = new MysqlStorageAdapter("s1", firstData, metadata);
        try (
                var manager = new TransactionManager(new MysqlTimestampOracle(control), metadata,
                        NoOpSafePointStore.INSTANCE, options, Map.of("s1", first),
                        NoOpTransactionMetrics.INSTANCE, ledger)) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "pending", bytes("value"));
            metadata.createWithParticipants(tx.txId(), tx.startTs(), tx.isolationLevel(), java.util.Set.of("s1"));
            tx.metadataPersisted(true);
            long commitTs = ledger.reserveCommit(tx.txId(), tx.deadlineMillis());
            manager.rollback(tx);
            Assertions.assertEquals(TxStatus.ABORTED, metadata.get(tx.txId()).orElseThrow().status());
            Assertions.assertEquals(PublicationEntryStatus.ABORTED, ledger.findByTxId(tx.txId()).orElseThrow().status());
            Assertions.assertEquals(commitTs, ledger.currentPublished());
            assertTrue(ledger.snapshotExpiry(tx.txId()).isEmpty());
        }
    }

    /**
     * Converts the constant to a binary value for testing.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Only loses the response after the first JDBC call is really dropped, simulates preset unknown outcome.
     *
     * @param delegate delegate
     * @param pending pending
     * @return the operation result
     */
    private static DataSource loseFirstCommitResponse(final DataSource delegate, final AtomicBoolean pending) {
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
                                    } catch (final InvocationTargetException ex) {
                                        throw ex.getCause();
                                    }
                                });
                    } catch (final InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }
}

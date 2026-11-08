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

package com.flyingzc.azir.storage.mysql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.VersionedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The participant protocol's real database return example, using barrier to determine the concurrent starting point without a linear hibernation.
 */
@Testcontainers
class MysqlParticipantProtocolTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withUsername("test").withPassword("test").withDatabaseName("participant_protocol");

    private DataSource dataSource;

    private MysqlStorageAdapter adapter;

    private InMemoryTransactionMetadataStore metadata;

    /**
     * Emptys all protocol lists and binds new centre decision examples for each example.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void setUp() throws Exception {
        dataSource = MysqlDataSourceFactory.create(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
        metadata = new InMemoryTransactionMetadataStore();
        adapter = new MysqlStorageAdapter("s1", dataSource);
        adapter.initialize();
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : List.of("mvcc_data", "mvcc_intent", "mvcc_rollback", "mvcc_participant_txn", "mvcc_key_lock")) {
                statement.executeUpdate("delete from " + table);
            }
            statement.executeUpdate("update mvcc_local_safe_point set safe_point = 0, gc_after_key = null where singleton_id = 1");
        }
    }

    /**
     * Turns off the connection pool used for example.
     *
     * @throws Exception if the operation cannot complete
     */
    @AfterEach
    void tearDown() throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * An empty string key that does not exist on verify can only be held by one concurrent prewrite.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentPrewritesMustConflictOnAbsentEmptyKey() throws Exception {
        List<ErrorCode> outcomes = race(
                () -> adapter.prewrite(10, 10, List.of(put("", "first"))),
                () -> adapter.prewrite(11, 11, List.of(put("", "second"))));
        Assertions.assertEquals(1, outcomes.stream().filter(code -> code == null).count());
        Assertions.assertEquals(1, outcomes.stream().filter(code -> code == ErrorCode.LOCK_CONFLICT).count());
    }

    /**
     * Verify concurrent execution of direct commit and empty prewrite can only produce one successful writer.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void directCommitMustSerializeWithIntentCreation() throws Exception {
        List<ErrorCode> outcomes = race(
                () -> adapter.directCommit(10, 10, 30, List.of(put("key", "direct"))),
                () -> adapter.prewrite(11, 11, List.of(put("key", "intent"))));
        Assertions.assertEquals(1, outcomes.stream().filter(code -> code == null).count());
        Assertions.assertEquals(1, outcomes.stream().filter(code -> code == ErrorCode.LOCK_CONFLICT || code == ErrorCode.WRITE_CONFLICT).count());
    }

    /**
     * Verify rollback without intent still prevents late prewrite and direct commit.
     */
    @Test
    void abortWithoutIntentMustFenceLateRequests() {
        adapter.abort(10);
        Assertions.assertEquals(TxStatus.ABORTED, adapter.participantRecord(10).orElseThrow().status());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 30, List.of(put("key", "late"))));
    }

    /**
     * Verify Equivalent commit can be replay, and prewrite will be rejected after changes to commit timestamp and terminal state.
     */
    @Test
    void commitReplayMustPreserveTimestampAndTerminalState() {
        List<Mutation> writes = List.of(put("key", "value"));
        adapter.prewrite(10, 10, writes);
        commitDecision(10, 30);
        adapter.commit(10, 30, writes);
        adapter.commit(10, 30);
        adapter.commit(10, 30, writes);
        assertThrows(MvccException.class, () -> adapter.commit(10, 31));
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, writes));
        adapter.abort(10);
        Assertions.assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        Assertions.assertEquals(30L, adapter.participantRecord(10).orElseThrow().commitTs().longValue());
    }

    /**
     * The old shard, which did not install the second version of the routine, used the first version to complete the same durable protocol after restarting.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void oldSingleKeyCommitRoutineMustRemainAvailableAfterRestart() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop procedure if exists mvcc_commit_one_v2");
        }
        var restarted = new MysqlStorageAdapter("s1", dataSource, metadata);
        restarted.requireInitializedFastPath();
        restarted.prewrite(10, 10, List.of(put("legacy:routine", "value")));
        commitDecision(10, 20);
        restarted.commit(10, 20, List.of(put("legacy:routine", "value")), true);
        Assertions.assertEquals(TxStatus.COMMITTED, restarted.participantRecord(10).orElseThrow().status());
        assertArrayEquals(bytes("value"), restarted.get(read(99, 30), "legacy:routine").orElseThrow().value());
    }

    /**
     * Single key tombstone commit may be reserved for intent, and replay clean-up must check original values with terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void singleKeyDeleteCommitMustSupportDeferredCleanupAndVerifiedReplay() throws Exception {
        adapter.enableSingleKeyCommitV2();
        List<Mutation> writes = List.of(Mutation.delete("key"));
        adapter.prewrite(10, 10, writes);
        commitDecision(10, 30);
        adapter.commit(10, 30, writes, false);
        Assertions.assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                var rows = statement.executeQuery("select count(*) from mvcc_intent where tx_id = 10")) {
            assertTrue(rows.next());
            Assertions.assertEquals(1, rows.getInt(1));
        }
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, List.of(put("key", "changed")), true));
        adapter.commit(10, 30, writes, true);
        adapter.commit(10, 30, writes, true);
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                var rows = statement.executeQuery("select count(*) from mvcc_intent where tx_id = 10")) {
            assertTrue(rows.next());
            Assertions.assertEquals(0, rows.getInt(1));
        }
    }

    /**
     * Verify Memory write set cannot change the dropped intent or make version when intent is missing.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void knownMutationsMustMatchAllDurableIntents() throws Exception {
        adapter.enableSingleKeyCommitV2();
        List<Mutation> writes = List.of(put("key", "original"));
        adapter.prewrite(10, 10, writes);
        commitDecision(10, 30);
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, List.of(put("key", "changed"))));
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from mvcc_intent where tx_id = 10");
        }
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, writes));
        assertTrue(adapter.get(read(99, 40), "key").isEmpty());
    }

    /**
     * Verify Center cannot make known write set materialization as commit version without commit.
     */
    @Test
    void materializationRequiresCommittedDecision() {
        List<Mutation> writes = List.of(put("key", "value"));
        adapter.prewrite(10, 10, writes);
        assertThrows(MvccException.class, () -> adapter.commit(10, 30, writes));
        assertTrue(adapter.get(read(99, 40), "key").isEmpty());
    }

    /**
     * Verify Direct commit saves with local decision atomic and cannot be different from replay.
     */
    @Test
    void directCommitReplayMustVerifyPersistedValues() {
        List<Mutation> writes = List.of(put("key", "value"));
        adapter.directCommit(10, 10, 30, writes);
        adapter.directCommit(10, 10, 30, writes);
        Assertions.assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 31, writes));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 30, List.of(put("key", "changed"))));
    }

    /**
     * Verify primary key conflict does not overwrite other transaction already commit.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void duplicateVersionMustNeverOverwriteAnotherTransaction() throws Exception {
        adapter.prewrite(10, 10, List.of(put("key", "intent")));
        commitDecision(10, 30);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value) values ('key', 30, 20, 0, 'existing')");
        }
        assertThrows(MvccException.class, () -> adapter.commit(10, 30));
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                var rows = statement.executeQuery("select tx_id, value from mvcc_data where user_key = 'key' and commit_ts = 30")) {
            assertTrue(rows.next());
            Assertions.assertEquals(20, rows.getLong("tx_id"));
            assertArrayEquals(bytes("existing"), rows.getBytes("value"));
        }
    }

    /**
     * Verify page break cursor comes from the original key, and your own deletion can create empty pages and continues to scan.
     */
    @Test
    void pagesMustOverlayOwnWritesAndAdvancePastDeletedKeys() {
        adapter.directCommit(10, 10, 20, List.of(put("a", "old"), put("b", "old")));
        adapter.prewrite(30, 30, List.of(Mutation.delete("a"), put("b", "own"), put("c", "new")));
        KeyRange range = new KeyRange("a", "z");
        ScanPage first = adapter.scanPage(read(30, 30), range, null, 1);
        assertTrue(first.values().isEmpty());
        Assertions.assertEquals("a", first.nextKey());
        ScanPage second = adapter.scanPage(read(30, 30), range, first.nextKey(), 1);
        Assertions.assertEquals(List.of("b"), second.values().stream().map(VersionedValue::key).toList());
        assertTrue(second.values().getFirst().ownIntent());
        assertArrayEquals(bytes("own"), second.values().getFirst().value());
        Assertions.assertEquals(List.of("b", "c"), adapter.scan(read(30, 30), range).stream().map(VersionedValue::key).toList());
    }

    /**
     * Verify database binary keys that differ from Java UTF-16 in order do not create a disordered or missing page.
     */
    @Test
    void pagesMustFollowUnsignedUtf8Order() {
        String bmp = "\uE000";
        String supplementary = "\uD800\uDC00";
        adapter.directCommit(10, 10, 20, List.of(put(supplementary, "s"), put(bmp, "b")));
        KeyRange range = new KeyRange("", "\uDBFF\uDFFF");
        ScanPage first = adapter.scanPage(read(99, 30), range, null, 1);
        Assertions.assertEquals(bmp, first.values().getFirst().key());
        Assertions.assertEquals(supplementary, adapter.scanPage(read(99, 30), range, first.nextKey(), 1).values().getFirst().key());
    }

    /**
     * Verify Clean-up watermark followed by rejection of the old request, while retaining local commit evidence without centre decision.
     */
    @Test
    void gcMustFenceOldRequestsAndRetainUnpublishedDecisions() {
        adapter.configureMetadataStore(metadata);
        metadata.createPreparing(10, 10, IsolationLevel.REPEATABLE_READ, Set.of("s1"));
        metadata.markLocalCommitPending(10, 20);
        adapter.directCommit(10, 10, 20, List.of(put("key", "value")));
        adapter.gc(40, 32);
        assertTrue(adapter.participantRecord(10).isPresent());
        assertThrows(MvccException.class, () -> adapter.prewrite(11, 11, List.of(put("new", "late"))));
        assertThrows(MvccException.class, () -> adapter.directCommit(12, 12, 50, List.of(put("new", "late"))));
        assertThrows(MvccException.class, () -> adapter.get(read(99, 39), "key"));
    }

    /**
     * Verify The same participant cannot be changed silently to another center authority.
     */
    @Test
    void metadataAuthorityMustRemainStable() {
        adapter.configureMetadataStore(metadata);
        assertTrue(adapter.supportsAtomicDirectCommit());
        assertTrue(adapter.supportsCommittedIntentResolution());
        assertThrows(IllegalStateException.class, () -> adapter.configureMetadataStore(new InMemoryTransactionMetadataStore()));
    }

    /**
     * Verify Real COMMIT after success will not be miscalculated as direct rollback by the bottom lock conflict SQLstate.
     */
    @Test
    void lostCommitResponseMustRemainUnknownAndRecoverFromLocalRecord() {
        MysqlStorageAdapter unreliable = new MysqlStorageAdapter("s1", loseCommitResponse(dataSource));
        MvccException failure = assertThrows(MvccException.class,
                () -> unreliable.directCommit(10, 10, 20, List.of(put("key", "committed"))));
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, failure.errorCode());
        assertFalse(failure.retryable());
        adapter.abort(10);
        Assertions.assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
        assertArrayEquals(bytes("committed"), adapter.get(read(99, 30), "key").orElseThrow().value());
    }

    /**
     * Verify local terminal state will not be cleaned before the centerfinalized, after cleaning watermark prevents old request for resurrection.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcMustWaitForFinalizationAndFenceRequestsAfterRecordRemoval() throws Exception {
        adapter.prewrite(10, 10, List.of(put("key", "value")));
        commitDecision(10, 20);
        adapter.commit(10, 20);
        ageTerminalRecords();
        adapter.gc(40, 4);
        assertTrue(adapter.participantRecord(10).isPresent());
        metadata.markFinalized(10);
        adapter.gc(40, 4);
        assertTrue(adapter.participantRecord(10).isEmpty());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 20, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.commit(10, 20));
    }

    /**
     * Verify rollback tag and transaction terminal state share a strict GC delete lines budget.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcMustBoundRollbackAndParticipantDeletion() throws Exception {
        adapter.prewrite(10, 10, List.of(put("a", "value"), put("b", "value"), put("c", "value")));
        adapter.configureMetadataStore(metadata);
        metadata.create(10, 10, IsolationLevel.REPEATABLE_READ);
        metadata.markAborted(10);
        adapter.abort(10);
        metadata.markFinalized(10);
        ageTerminalRecords();
        for (int index = 0; index < 16; index++) {
            assertTrue(adapter.gc(40, 1) <= 1);
        }
        assertTrue(adapter.participantRecord(10).isEmpty());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("a", "late"))));
    }

    /**
     * Verify tombstone chain to new line collection, does not revive old values in clean-up and eventually releases empty key locks.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void tombstoneMustBeDeletedLastWithinSharedGcBudget() throws Exception {
        adapter.directCommit(10, 10, 20, List.of(put("key", "first")));
        adapter.directCommit(30, 30, 40, List.of(put("key", "second")));
        adapter.directCommit(50, 50, 60, List.of(Mutation.delete("key")));
        for (int index = 0; index < 4; index++) {
            Assertions.assertEquals(1, adapter.gc(80, 1));
            var value = adapter.get(read(99, 80), "key");
            assertTrue(value.isEmpty() || value.orElseThrow().deleted());
        }
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                var rows = statement.executeQuery("select count(*) from mvcc_key_lock where user_key = 'key'")) {
            assertTrue(rows.next());
            Assertions.assertEquals(0, rows.getInt(1));
        }
        assertThrows(MvccException.class, () -> adapter.prewrite(15, 15, List.of(put("key", "late"))));
        adapter.prewrite(90, 90, List.of(put("key", "new")));
        assertArrayEquals(bytes("new"), adapter.get(read(90, 90), "key").orElseThrow().value());
    }

    /**
     * Verify does not allow collection prekeys to advance durable cursor, and the new adaptor continues cleaning from the follow-up key.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcCursorMustSurviveAdapterRestartAndSkipUnreclaimableKeys() throws Exception {
        adapter.directCommit(10, 10, 20, List.of(put("a", "keep"), put("b", "old")));
        adapter.directCommit(30, 30, 40, List.of(put("b", "latest")));
        Assertions.assertEquals(0, adapter.gc(60, 1));
        try (
                Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                var rows = statement.executeQuery("select gc_after_key from mvcc_local_safe_point where singleton_id = 1")) {
            assertTrue(rows.next());
            assertArrayEquals(bytes("a"), rows.getBytes(1));
        }
        MysqlStorageAdapter restarted = new MysqlStorageAdapter("s1", dataSource, metadata);
        Assertions.assertEquals(1, restarted.gc(60, 1));
        assertArrayEquals(bytes("latest"), restarted.get(read(99, 60), "b").orElseThrow().value());
    }

    /**
     * The verify is the same transaction page, which only solves once, and the non- terminal state does not contaminate the next page reading.
     */
    @Test
    void pageMustResolveEachForeignTransactionOnceWithoutCachingActiveState() {
        adapter.prewrite(10, 10, List.of(put("a", "one"), put("b", "two"), Mutation.delete("c")));
        metadata.create(10, 10, IsolationLevel.REPEATABLE_READ);
        AtomicInteger lookups = new AtomicInteger();
        TransactionMetadataStore tracked = (TransactionMetadataStore) Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(), new Class<?>[]{TransactionMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("get")) {
                        lookups.incrementAndGet();
                    }
                    try {
                        return method.invoke(metadata, arguments);
                    } catch (final InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", dataSource, tracked);
        KeyRange range = new KeyRange("a", "z");
        assertTrue(reader.scanPage(read(99, 30), range, null, 10).values().isEmpty());
        Assertions.assertEquals(1, lookups.get());
        metadata.markCommitted(10, 20);
        Assertions.assertEquals(2, reader.scanPage(read(99, 30), range, null, 10).values().size());
        Assertions.assertEquals(2, lookups.get());
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void pageSizeMustRemainBounded() {
        assertThrows(IllegalArgumentException.class, () -> adapter.scanPage(read(99, 30), new KeyRange("", "z"), null, 0));
        assertThrows(IllegalArgumentException.class, () -> adapter.scanPage(read(99, 30), new KeyRange("", "z"), null, 4097));
    }

    /**
     * Verify Tie Center authority Direct commit rejects missing decision and missing candidate time and accepts the correct posting appointment.
     */
    @Test
    void boundDirectCommitMustRequireMatchingPublicationReservation() {
        adapter.configureMetadataStore(metadata);
        List<Mutation> writes = List.of(put("key", "value"));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 20, writes));
        metadata.createPreparing(10, 10, IsolationLevel.REPEATABLE_READ, Set.of("s1"));
        assertThrows(MvccException.class, () -> adapter.directCommit(10, 10, 20, writes));
        metadata.markLocalCommitPending(10, 20);
        adapter.directCommit(10, 10, 20, writes);
        adapter.abort(10);
        Assertions.assertEquals(TxStatus.COMMITTED, adapter.participantRecord(10).orElseThrow().status());
    }

    /**
     * Verify Center will still be able to rely on durable safe watermark clean-up terminal state after collection has finished transaction.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcMustReclaimLocalTerminalAfterMetadataWasPurged() throws Exception {
        adapter.prewrite(10, 10, List.of(put("key", "value")));
        commitDecision(10, 20);
        adapter.commit(10, 20);
        metadata.markFinalized(10);
        Assertions.assertEquals(1, metadata.purgeFinalized(40, Long.MAX_VALUE, 4));
        ageTerminalRecords();
        adapter.gc(40, 4);
        assertTrue(adapter.participantRecord(10).isEmpty());
        assertThrows(MvccException.class, () -> adapter.prewrite(10, 10, List.of(put("key", "late"))));
        assertThrows(MvccException.class, () -> adapter.commit(10, 20));
    }

    /**
     * When a normal query has been executed before the verify key lock, write to transaction still discovers a new version for subsequent commit.
     */
    @Test
    void writeTransactionMustReadCurrentVersionsAfterKeyLockAcquisition() {
        DataSource intercepted = interceptSql(dataSource, "insert into mvcc_key_lock", true,
                () -> adapter.directCommit(20, 20, 40, List.of(put("key", "winner"))));
        MysqlStorageAdapter contender = new MysqlStorageAdapter("s1", intercepted);
        MvccException conflict = assertThrows(MvccException.class,
                () -> contender.directCommit(10, 10, 50, List.of(put("key", "late"))));
        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, conflict.errorCode());
        assertArrayEquals(bytes("winner"), adapter.get(read(99, 60), "key").orElseThrow().value());
    }

    /**
     * Verify Completion of materialization between version and intent queries does not result in missing reading, which must be shared with the same RR snapshot.
     */
    @Test
    void readTransactionMustKeepOneSnapshotAcrossMaterializationSwitch() {
        adapter.prewrite(10, 10, List.of(put("key", "committed")));
        commitDecision(10, 20);
        DataSource intercepted = interceptSql(dataSource, "select d.user_key", false, () -> adapter.commit(10, 20));
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", intercepted, metadata);
        assertArrayEquals(bytes("committed"), reader.get(read(99, 30), "key").orElseThrow().value());
    }

    /**
     * When verify version queries are followed by materialization and decision clean-up, click to recreate local snapshot but keep global readTs.
     */
    @Test
    void getMustRebuildSnapshotWhenMaterializedIntentDecisionWasPurged() {
        adapter.directCommit(1, 1, 5, List.of(put("key", "old")));
        adapter.prewrite(10, 10, List.of(put("key", "committed")));
        commitDecision(10, 20);
        DataSource intercepted = interceptSql(dataSource, "select d.user_key", false,
                () -> materializeAndPurgeBeforeSnapshot30("key"));
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", intercepted, metadata);

        VersionedValue value = reader.get(read(99, 30), "key").orElseThrow();
        assertArrayEquals(bytes("committed"), value.value());
        Assertions.assertEquals(20, value.versionTs());
        assertFalse(value.ownIntent());
        assertTrue(metadata.get(10).isEmpty());
    }

    /**
     * Verify page breaks recollect the original range and cursor keys after decision has been cleaned, and delete semantics and global snapshot are retained.
     */
    @Test
    void pageMustRebuildSnapshotWithoutChangingRangeCursorOrReadTimestamp() {
        adapter.directCommit(1, 1, 5, List.of(put("a", "before"), put("b", "old"), put("c", "old"), put("d", "old")));
        adapter.prewrite(10, 10, List.of(put("b", "committed"), Mutation.delete("c"), put("d", "next")));
        commitDecision(10, 20);
        DataSource intercepted = interceptSql(dataSource, "select d.user_key", false,
                () -> materializeAndPurgeBeforeSnapshot30("b"));
        MysqlStorageAdapter reader = new MysqlStorageAdapter("s1", intercepted, metadata);

        ScanPage page = reader.scanPage(read(99, 30), new KeyRange("a", "z"), "a", 2);
        Assertions.assertEquals(List.of("b", "d"), page.values().stream().map(VersionedValue::key).toList());
        assertArrayEquals(bytes("committed"), page.values().getFirst().value());
        Assertions.assertEquals(20, page.values().getFirst().versionTs());
        assertNull(page.nextKey());
    }

    /**
     * Verify When the missing decision continues, read and page breaks are tried up to two times and cannot be returned to the old value.
     */
    @Test
    void missingDecisionMustFailClosedAfterExactlyOneSnapshotRebuild() {
        adapter.directCommit(1, 1, 5, List.of(put("key", "old")));
        adapter.prewrite(10, 10, List.of(put("key", "unknown")));
        AtomicInteger lookups = new AtomicInteger();
        MysqlStorageAdapter reader = readerCountingDecisions(lookups);

        MvccException getFailure = assertThrows(MvccException.class, () -> reader.get(read(99, 30), "key"));
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, getFailure.errorCode());
        assertFalse(getFailure.retryable());
        Assertions.assertEquals(2, lookups.get());
        lookups.set(0);
        MvccException pageFailure = assertThrows(MvccException.class,
                () -> reader.scanPage(read(99, 30), new KeyRange("a", "z"), null, 2));
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, pageFailure.errorCode());
        assertFalse(pageFailure.retryable());
        Assertions.assertEquals(2, lookups.get());

        // This transaction own intent does not require a search centre to retain the behaviour of the bottom adaptor that is not bound to resolver.
        assertArrayEquals(bytes("old"), adapter.get(read(99, 30), "key").orElseThrow().value());
        lookups.set(0);
        assertArrayEquals(bytes("unknown"), reader.get(read(10, 30), "key").orElseThrow().value());
        Assertions.assertEquals(0, lookups.get());
    }

    /**
     * Verify The real ACTIVE and PREPARING intent are still ignored without triggering local snapshot reconstruction.
     */
    @Test
    void undecidedExistingRecordsMustNotTriggerSnapshotRebuild() {
        adapter.directCommit(1, 1, 5, List.of(put("key", "old")));
        adapter.prewrite(10, 10, List.of(put("key", "pending")));
        metadata.create(10, 10, IsolationLevel.REPEATABLE_READ);
        AtomicInteger lookups = new AtomicInteger();
        MysqlStorageAdapter reader = readerCountingDecisions(lookups);
        assertArrayEquals(bytes("old"), reader.get(read(99, 30), "key").orElseThrow().value());
        Assertions.assertEquals(1, lookups.get());
        metadata.markPreparing(10, Set.of("s1"));
        Assertions.assertEquals(List.of("key"), reader.scanPage(read(99, 30), new KeyRange("a", "z"), null, 2)
                .values().stream().map(VersionedValue::key).toList());
        Assertions.assertEquals(2, lookups.get());
    }

    /**
     * Finish materialization and decision clean-up in valid watermark readTs-1 and add a new global snapshot invisible version.
     *
     * @param futureKey future key
     */
    private void materializeAndPurgeBeforeSnapshot30(final String futureKey) {
        adapter.commit(10, 20);
        metadata.markFinalized(10);
        Assertions.assertEquals(1, metadata.purgeFinalized(29, Long.MAX_VALUE, 4));
        new MysqlStorageAdapter("s1", dataSource).directCommit(35, 35, 40, List.of(put(futureKey, "future")));
        adapter.gc(29, 16);
    }

    /**
     * Records the number of actual centre queries without changing the transaction state returned from the real memory authority.
     *
     * @param lookups lookups
     * @return the operation result
     */
    private MysqlStorageAdapter readerCountingDecisions(final AtomicInteger lookups) {
        TransactionMetadataStore tracked = (TransactionMetadataStore) Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(), new Class<?>[]{TransactionMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("get")) {
                        lookups.incrementAndGet();
                    }
                    return invoke(metadata, method, arguments);
                });
        return new MysqlStorageAdapter("s1", dataSource, tracked);
    }

    /**
     * Inserts a certainty operation before and after the actual JDBC statement, which controls only the time series staggering instead of replacing database results.
     *
     * @param delegate delegate
     * @param sqlPrefix sql prefix
     * @param before before
     * @param action action
     * @return the operation result
     */
    private DataSource interceptSql(final DataSource delegate, final String sqlPrefix, final boolean before, final Runnable action) {
        AtomicBoolean pending = new AtomicBoolean(true);
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    Object result = invoke(delegate, method, arguments);
                    if (!(result instanceof Connection connection)) {
                        return result;
                    }
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (connectionProxy, connectionMethod, parameters) -> {
                                Object value = invoke(connection, connectionMethod, parameters);
                                if (!(value instanceof PreparedStatement statement) || parameters == null
                                        || !(parameters[0] instanceof String sql) || !sql.stripLeading().startsWith(sqlPrefix)) {
                                    return value;
                                }
                                return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                                        (statementProxy, statementMethod, binds) -> {
                                            boolean trigger = statementMethod.getName().startsWith("execute") && pending.compareAndSet(true, false);
                                            if (trigger && before) {
                                                action.run();
                                            }
                                            Object rows = invoke(statement, statementMethod, binds);
                                            if (trigger && !before) {
                                                action.run();
                                            }
                                            return rows;
                                        });
                            });
                });
    }

    /**
     * Fax-based JDBC anomaly to avoid the refraction package changing the type of abnormality by verify.
     *
     * @param target target
     * @param method method
     * @param arguments arguments
     * @return the operation result
     * @throws Throwable if the operation cannot complete
     */
    private Object invoke(final Object target, final Method method, final Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (final InvocationTargetException ex) {
            throw ex.getCause();
        }
    }

    /**
     * Directly moves to test the age of the wall clock recorded and avoids sleep for retention.
     *
     * @throws SQLException if the operation cannot complete
     */
    private void ageTerminalRecords() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("update mvcc_participant_txn set updated_at = 0");
            statement.executeUpdate("update mvcc_rollback set created_at = 0");
        }
    }

    /**
     * Proxy connection and only loses the first successful commit response without simulating database durable storage behavior.
     *
     * @param delegate delegate
     * @return the operation result
     */
    private DataSource loseCommitResponse(final DataSource delegate) {
        AtomicBoolean pending = new AtomicBoolean(true);
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (!(result instanceof Connection connection)) {
                            return result;
                        }
                        return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                                (connectionProxy, connectionMethod, parameters) -> {
                                    try {
                                        Object value = connectionMethod.invoke(connection, parameters);
                                        if (connectionMethod.getName().equals("commit") && pending.compareAndSet(true, false)) {
                                            throw new SQLException("lost successful COMMIT response", "40001", 1213);
                                        }
                                        return value;
                                    } catch (final InvocationTargetException ex) {
                                        throw ex.getCause();
                                    }
                                });
                    } catch (final InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /**
     * Starts two operations and collects protocol results after the same barrier, failure other than anomalies is spread by Future.
     *
     * @param first first
     * @param second second
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private List<ErrorCode> race(final Runnable first, final Runnable second) throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var left = executor.submit(contender(first, ready, start));
            var right = executor.submit(contender(second, ready, start));
            try {
                assertTrue(ready.await(10, TimeUnit.SECONDS));
                start.countDown();
                return java.util.Arrays.asList(left.get(20, TimeUnit.SECONDS), right.get(20, TimeUnit.SECONDS));
            } finally {
                start.countDown();
                left.cancel(true);
                right.cancel(true);
            }
        }
    }

    /**
     * Pack an concurrent operation as bounded to wait and return the task with a clear conflict code.
     *
     * @param operation operation
     * @param ready ready
     * @param start start
     * @return the operation result
     */
    private Callable<ErrorCode> contender(final Runnable operation, final CountDownLatch ready, final CountDownLatch start) {
        return () -> {
            ready.countDown();
            assertTrue(start.await(10, TimeUnit.SECONDS));
            try {
                operation.run();
                return null;
            } catch (final MvccException ex) {
                return ex.errorCode();
            }
        };
    }

    /**
     * Writing centre commit decision for participant reconciliation.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    private void commitDecision(final long txId, final long commitTs) {
        adapter.configureMetadataStore(metadata);
        metadata.createCommitted(txId, txId, IsolationLevel.REPEATABLE_READ, Set.of("s1"), commitTs);
    }

    /**
     * Constructs a readable string writing value.
     *
     * @param key business key
     * @param value value to write
     * @return the operation result
     */
    private static Mutation put(final String key, final String value) {
        return Mutation.put(key, bytes(value));
    }

    /**
     * Construct the read context for specified transaction and timestamp.
     *
     * @param txId transaction identifier
     * @param readTs snapshot read timestamp
     * @return the operation result
     */
    private static ReadContext read(final long txId, final long readTs) {
        return new ReadContext(txId, readTs, IsolationLevel.REPEATABLE_READ);
    }

    /**
     * Test data by UTF-8 encoding.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

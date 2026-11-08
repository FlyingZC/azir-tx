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

package com.flyingzc.azir.sql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.flyingzc.azir.percolator.PercolatorStore.Action;
import com.flyingzc.azir.percolator.PercolatorStore.Decision;
import com.flyingzc.azir.percolator.PercolatorStore.Lock;
import com.flyingzc.azir.percolator.PercolatorStore.Snapshot;
import com.flyingzc.azir.percolator.PercolatorStore.State;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.percolator.MysqlPercolatorStore;
import com.flyingzc.azir.percolator.PercolatorKey;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.percolator.PercolatorStore;
import com.flyingzc.azir.percolator.PercolatorTransactionStatusUnknownException;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.common.IsolationLevel.*;

/**
 * In two stand-alone MySQL examples verify Percolator SQL assembly, page breaks, errors terminal state and recovery.
 */
@Testcontainers
class PercolatorSqlIntegrationTest {

    @Container
    private static final MySQLContainer<?> MYSQL_A = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("pc_sql").withUsername("test").withPassword("test");

    @Container
    private static final MySQLContainer<?> MYSQL_B = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("pc_sql").withUsername("test").withPassword("test");

    private static DataSource poolA;

    private static DataSource poolB;

    private static final ShardResolver RESOLVER = ShardResolver.hashByKey(List.of("a", "b"));

    private static final SqlStorageKeyCodec KEYS = new SqlStorageKeyCodec();

    private MysqlPercolatorStore a;

    private MysqlPercolatorStore b;

    private MysqlTimestampOracle tso;

    private PercolatorSqlEngine engine;

    /**
     * Use open assembly access to initialize protocol data space and durable TSO.
     */
    @BeforeAll
    static void open() {
        poolA = MysqlDataSourceFactory.create(MYSQL_A.getJdbcUrl(), MYSQL_A.getUsername(), MYSQL_A.getPassword());
        poolB = MysqlDataSourceFactory.create(MYSQL_B.getJdbcUrl(), MYSQL_B.getUsername(), MYSQL_B.getPassword());
        try (var initialized = PercolatorSqlEngine.mysql(poolA, Map.of("a", poolA, "b", poolB), RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED)) {
        }

    }

    /**
     * All session closes after test owner closes connection pool.
     *
     * @throws Exception if the operation cannot complete
     */
    @AfterAll
    static void close() throws Exception {
        if (poolA instanceof AutoCloseable closeable) {
            closeable.close();
        }
        if (poolB instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * Each test emptys the separate protocol table but keeps a single tune TSO, defaulting that each page contains a true cross-page query.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void reset() throws Exception {
        for (DataSource source : List.of(poolA, poolB)) {
            try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                for (String table : List.of("pc_lock", "pc_write", "pc_data", "pc_rollback", "pc_guard")) {
                    statement.executeUpdate("delete from " + table);
                }
            }
        }
        a = new MysqlPercolatorStore("a", poolA);
        b = new MysqlPercolatorStore("b", poolB);
        tso = new MysqlTimestampOracle(poolA);
        engine = engine(new PercolatorRuntime(tso, Map.of("a", a, "b", b)), SqlExecutionOptions.defaults());
    }

    /**
     * Same SqlEngine/ SqlSsession interface visiblely selects the new model and returns its original purimay commit identity.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void mysqlFactorySupportsExistingSqlApiAndDurableCommitIdentity() throws Exception {
        try (
                var selected = PercolatorSqlEngine.mysql(poolA, Map.of("a", poolA, "b", poolB),
                        RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED);
                SqlSession session = selected.openSession()) {
            var result = session.execute("insert into account (id, value) values ('1', 'one'), ('2', 'two')");
            Assertions.assertEquals(2, result.updateCount());
            assertNull(result.commitResult());
            var commit = result.percolatorCommitResult();
            assertNotNull(commit);
            assertNotNull(commit.handle());
            assertTrue(commit.commitTs() > commit.handle().startTs());
            assertTrue(commit.pendingSecondaries().isEmpty());
            Assertions.assertEquals(State.COMMITTED, engine.runtime().lookup(commit.handle()).state());
            Assertions.assertEquals("one", session.execute("select value from account where id='1'").singleRow().get("value"));
            Assertions.assertEquals(0, session.execute("select value from account where id='1'").percolatorCommitResult().commitTs());
        }
        // The new plant only initializes its own protocol space and shared TSO, without creating an old model one by one transaction metadata.
        try (var connection = poolA.getConnection(); var query = connection.prepareStatement("""
                select count(*) from information_schema.tables
                where table_schema=database() and table_name='mvcc_txn'
                """); var rows = query.executeQuery()) {
            rows.next();
            Assertions.assertEquals(0, rows.getInt(1));
        }
        assertThrows(IllegalArgumentException.class, () -> PercolatorSqlEngine.mysql(poolA,
                Map.of("a", poolA, "alias", poolA), RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED));
    }

    /**
     * Visible SQL transaction cross-shard read-your-own-writes, rollback does not leak to another autocommit session.
     */
    @Test
    void explicitTransactionReadsOwnWritesAndRollsBackEveryShard() {
        try (var writer = engine.openSession(); var reader = engine.openSession()) {
            writer.execute("begin repeatable read");
            writer.execute("insert into account (id,value) values ('1','one'),('2','two')");
            Assertions.assertEquals(List.of("one", "two"), values(writer.execute("select value from account order by id")));
            Assertions.assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
            writer.execute("rollback");
            assertFalse(writer.active());
            Assertions.assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
            writer.begin(REPEATABLE_READ);
            writer.execute("insert into account (id,value) values ('1','saved'),('2','saved')");
            assertNotNull(writer.commit().percolatorCommitResult());
            Assertions.assertEquals("2", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /**
     * RC takes a new boundary between SQL statements, and RR keeps the old snapshot while scanned, deleted and rereading across the page.
     */
    @Test
    void sqlRcAndRrPreserveTheirSnapshotBoundaries() {
        insert("insert into account (id,value) values ('1','old'),('2','old'),('3','old')");
        try (var rr = engine.openSession(); var rc = engine.openSession(); var writer = engine.openSession()) {
            rr.begin(REPEATABLE_READ);
            rc.begin(READ_COMMITTED);
            Assertions.assertEquals(List.of("old", "old", "old"), values(rr.execute("select value from account order by id")));
            Assertions.assertEquals("old", rc.execute("select value from account where id='2'").singleRow().get("value"));
            writer.execute("update account set value='new' where id='2'");
            writer.execute("delete from account where id='1'");
            writer.execute("insert into account (id,value) values ('4','new')");
            Assertions.assertEquals(List.of("old", "old", "old"), values(rr.execute("select value from account order by id")));
            Assertions.assertEquals(List.of("new", "old", "new"), values(rc.execute("select value from account order by id")));
            rr.rollback();
            rc.rollback();
        }
    }

    /**
     * The same RC SQL takes the original readTs after the first page cross-shard commit, followed by the next page and another shard.
     */
    @Test
    void oneRcSqlUsesOneTimestampAcrossPagesAndShards() {
        insert("insert into account (id,value) values ('1','old'),('2','old'),('3','old')");
        var first = new ProbedStore(a);
        var allocations = new AtomicInteger();
        var runtime = new PercolatorRuntime(() -> {
            allocations.incrementAndGet();
            return tso.nextTimestamp();
        },
                Map.of("a", first, "b", b));
        first.afterKeys = () -> {
            try (var writer = engine.runtime().begin(REPEATABLE_READ)) {
                writer.put("a", KEYS.encode("account", "2"), bytes("new"));
                writer.put("b", KEYS.encode("account", "1"), bytes("new"));
                writer.put("a", KEYS.encode("account", "4"), bytes("new"));
                writer.commit();
            }
        };
        var paged = engine(runtime, SqlExecutionOptions.defaults());
        try (var session = paged.openSession()) {
            session.begin(READ_COMMITTED);
            int before = allocations.get();
            Assertions.assertEquals(List.of("old", "old", "old"), values(session.execute("select value from account order by id")));
            Assertions.assertEquals(before + 1, allocations.get(), "one RC read boundary for the entire SQL");
            Assertions.assertEquals(List.of("new", "new", "old", "new"), values(session.execute("select value from account order by id")));
            session.rollback();
        }
    }

    /**
     * Calcite conditions DML, JOIN, Syndication and INSERT SELECT are read and written by the new backend.
     */
    @Test
    void relationalSqlAndTableNamespacesUsePercolator() {
        insert("insert into account (id,value) values ('1','gold'),('2','gold'),('3','silver')");
        insert("insert into tag (id,value) values ('1','yes'),('2','yes')");
        try (var session = engine.openSession()) {
            var grouped = session.execute("select a.value as label,count(*) as n from account a join tag t on a.id=t.id group by a.value");
            Assertions.assertEquals("gold", grouped.singleRow().get("label"));
            Assertions.assertEquals("2", grouped.singleRow().get("n"));
            Assertions.assertEquals(2, session.execute("update account set value=upper(value) where id in ('1','2')").updateCount());
            Assertions.assertEquals(3, session.execute("insert into archive(id,value) select id,value from account").updateCount());
            Assertions.assertEquals(1, session.execute("delete from account where value='silver'").updateCount());
            Assertions.assertEquals(List.of("GOLD", "GOLD"), values(session.execute("select value from account order by id")));
            Assertions.assertEquals("3", session.execute("select count(*) as n from archive").singleRow().get("n"));
            Assertions.assertEquals(List.of("yes", "yes"), values(session.execute("select value from tag order by id")));
        }
    }

    /**
     * A failed SQL must undo the previous visible transaction writing and ROLLBACK cannot be converted to autocommit by implication.
     */
    @Test
    void failedDataSqlInvalidatesAndRollsBackEntireExplicitTransaction() {
        insert("insert into account(id,value) values ('1','saved')");
        try (var session = engine.openSession()) {
            session.begin(REPEATABLE_READ);
            session.execute("insert into account(id,value) values ('2','prefix')");
            assertThrows(MvccException.class, () -> session.execute("insert into account(id,value) values ('3','x'),('1','duplicate')"));
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            assertThrows(MvccException.class, () -> session.execute("insert into account(id,value) values ('4','bad')"));
            assertThrows(MvccException.class, () -> session.begin(READ_COMMITTED));
            session.rollback();
            Assertions.assertEquals(List.of("saved"), values(session.execute("select value from account order by id")));
            session.execute("insert into account(id,value) values ('2','reused')");
        }
    }

    /**
     * The parsing failed also cannot leave a visible transaction writing that was already buffer.
     */
    @Test
    void invalidSyntaxRollsBackTheExplicitTransaction() {
        try (var session = engine.openSession()) {
            session.begin(REPEATABLE_READ);
            session.execute("insert into account(id,value) values ('1','prefix')");
            assertThrows(MvccException.class, () -> session.execute("insert into account nonsense"));
            assertFalse(session.active());
            session.rollback();
            Assertions.assertEquals("0", session.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /**
     * When two transaction concurrent inserts the same primary key, the later commit must conflict and do not overwrite the confirmed insertion.
     */
    @Test
    void concurrentInsertUsesPercolatorWriteConflictValidation() {
        try (var first = engine.openSession(); var second = engine.openSession()) {
            first.begin(REPEATABLE_READ);
            second.begin(READ_COMMITTED);
            first.execute("insert into account(id,value) values ('1','first')");
            second.execute("insert into account(id,value) values ('1','second')");
            first.commit();
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(MvccException.class, second::commit).errorCode());
            second.rollback();
            Assertions.assertEquals("first", second.execute("select value from account where id='1'").singleRow().get("value"));
        }
    }

    /**
     * The real primary response and query failed. SQL anomalies must carry the complete identity that cross session recovery.
     */
    @Test
    void autocommitUnknownPreservesPrimaryIdentityAndCannotFakeRollback() {
        var fault = new ProbedStore(a);
        fault.loseCommitReply = true;
        fault.failInspect = true;
        var broken = engine(new PercolatorRuntime(tso, Map.of("a", fault, "b", b)), SqlExecutionOptions.defaults());
        var session = broken.openSession();
        var failure = assertThrows(PercolatorTransactionStatusUnknownException.class,
                () -> session.execute("insert into account(id,value) values ('1','one'),('2','two')"));
        assertFalse(failure.retryable());
        Assertions.assertEquals(failure.handle().startTs(), failure.txId());
        assertFalse(session.active());
        assertThrows(MvccException.class, () -> session.execute("insert into account(id,value) values ('3','bad')"));
        assertThrows(PercolatorTransactionStatusUnknownException.class, session::rollback);
        Assertions.assertEquals(1, b.pending(10).size());
        session.close();
        session.close();
        Assertions.assertEquals(State.COMMITTED, engine.runtime().lookup(failure.handle()).state());
        Assertions.assertEquals(1, engine.runtime().recover(10));
        try (var reader = engine.openSession()) {
            Assertions.assertEquals(List.of("one", "two"), values(reader.execute("select value from account order by id")));
        }
    }

    /**
     * Visible COMMIT also retains UNKNOWN, which does not allow commit again or to overwrite the original decision with ROLLBACK.
     */
    @Test
    void explicitCommitUnknownKeepsTheOriginalHandle() {
        var fault = new ProbedStore(a);
        fault.loseCommitReply = true;
        fault.failInspect = true;
        var broken = engine(new PercolatorRuntime(tso, Map.of("a", fault, "b", b)), SqlExecutionOptions.defaults());
        try (var session = broken.openSession()) {
            session.begin(REPEATABLE_READ);
            session.execute("insert into account(id,value) values ('1','one'),('2','two')");
            var unknown = assertThrows(PercolatorTransactionStatusUnknownException.class, session::commit);
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            var rollback = assertThrows(PercolatorTransactionStatusUnknownException.class, session::rollback);
            Assertions.assertEquals(unknown.handle(), rollback.handle());
            Assertions.assertEquals(State.COMMITTED, engine.runtime().lookup(unknown.handle()).state());
        }
        Assertions.assertEquals(1, b.pending(10).size());
        engine.runtime().recover(10);
        assertTrue(b.pending(10).isEmpty());
    }

    /**
     * Secondary When finalization SQL returns confirmed receipt and pending key, followed by scans completed with the original groupTs.
     */
    @Test
    void sqlCommitReportsPendingSecondaryAndReadResolvesIt() {
        var fault = new ProbedStore(b);
        fault.failFinish = true;
        var partial = engine(new PercolatorRuntime(tso, Map.of("a", a, "b", fault)), SqlExecutionOptions.defaults());
        try (var session = partial.openSession()) {
            session.begin(REPEATABLE_READ);
            session.execute("insert into account(id,value) values ('1','one'),('2','two')");
            var commit = session.commit().percolatorCommitResult();
            Assertions.assertEquals(List.of(new PercolatorKey("b", KEYS.encode("account", "1"))), commit.pendingSecondaries());
            Assertions.assertEquals(State.COMMITTED, engine.runtime().lookup(commit.handle()).state());
        }
        try (var reader = engine.openSession()) {
            Assertions.assertEquals(List.of("one", "two"), values(reader.execute("select value from account order by id")));
        }
        assertTrue(b.pending(10).isEmpty());
    }

    /**
     * The relevant pending lock cannot be skipped in the full-table scan to prevent possible decision transaction from being read as a partial result.
     */
    @Test
    void sqlScanDoesNotSkipUnresolvedLocks() {
        try (var writer = engine.runtime().begin(REPEATABLE_READ); var reader = engine.openSession()) {
            writer.put("a", KEYS.encode("account", "2"), bytes("pending"));
            writer.prewrite();
            Assertions.assertEquals(ErrorCode.LOCK_CONFLICT,
                    assertThrows(MvccException.class, () -> reader.execute("select count(*) as n from account")).errorCode());
            writer.commit();
            Assertions.assertEquals("1", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /**
     * Unvisible guard and tombstone owns SQL input budget and cannot bypass the scan limit with an empty page.
     */
    @Test
    void invisibleCandidatesAndTombstonesConsumeSqlScanBudget() {
        try (var tx = engine.runtime().begin(REPEATABLE_READ)) {
            tx.put("a", KEYS.encode("account", "2"), bytes("aborted"));
            tx.put("a", KEYS.encode("account", "4"), bytes("aborted"));
            tx.prewrite();
            tx.rollback();
        }
        var tight = engine(engine.runtime(), new SqlExecutionOptions(1, 100, 1024, 1, 1024));
        try (var reader = tight.openSession()) {
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                    assertThrows(MvccException.class, () -> reader.execute("select count(*) as n from account")).errorCode());
        }
        try (var reader = engine.openSession()) {
            Assertions.assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /**
     * Simple check-and-relationship queries comply with the result budget, which cannot be published after exceeding the limit.
     */
    @Test
    void sqlResultBudgetsApplyToBothPlannerPaths() {
        insert("insert into account(id,value) values ('1','one'),('2','two')");
        var rowLimited = engine(engine.runtime(), new SqlExecutionOptions(1, 1, 1024));
        try (var reader = rowLimited.openSession()) {
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                    assertThrows(MvccException.class, () -> reader.execute("select * from account order by id")).errorCode());
        }
        var byteLimited = engine(engine.runtime(), new SqlExecutionOptions(1, 100, 1));
        try (var reader = byteLimited.openSession()) {
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                    assertThrows(MvccException.class, () -> reader.execute("select value from account where id='1'")).errorCode());
        }
    }

    /**
     * The same-same transaction text is written, crosses the rollback residue and delete key in UTF-8 cursor.
     */
    @Test
    void kvPagesOverlayOwnWritesAndAdvancePastInvisibleKeys() {
        try (var aborted = engine.runtime().begin(REPEATABLE_READ)) {
            aborted.put("a", "a", bytes("gone"));
            aborted.prewrite();
            aborted.rollback();
        }
        try (var seed = engine.runtime().begin(REPEATABLE_READ)) {
            seed.put("a", "b", bytes("old"));
            seed.put("a", "c", bytes("old"));
            seed.put("a", "\ue000", bytes("bmp"));
            seed.put("a", "😀", bytes("supplementary"));
            seed.commit();
        }
        try (var tx = engine.runtime().begin(REPEATABLE_READ)) {
            tx.delete("a", "b");
            tx.put("a", "c", bytes("own"));
            tx.put("a", "d", bytes("new"));
            var view = tx.statement();
            var range = new KeyRange(null, null);
            List<String> keys = new ArrayList<>();
            List<String> visible = new ArrayList<>();
            String after = null;
            do {
                var page = view.scanPage("a", range, after, 1);
                Assertions.assertEquals(1, page.entries().size());
                var entry = page.entries().getFirst();
                keys.add(entry.key());
                if (entry.value() != null) {
                    visible.add(new String(entry.value(), StandardCharsets.UTF_8));
                }
                if (page.nextKey() != null) {
                    Assertions.assertNotEquals(after, page.nextKey());
                }
                after = page.nextKey();
            } while (after != null);
            Assertions.assertEquals(List.of("a", "b", "c", "d", "\ue000", "😀"), keys);
            Assertions.assertEquals(List.of("own", "new", "bmp", "supplementary"), visible);
            assertTrue(view.scanPage("a", new KeyRange("z", "a"), null, 2).entries().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> view.scanPage("a", range, null, 0));
        }
    }

    /**
     * Close session rollback without commit writing, close state irreversible and do not close the MySQL pool that is owned.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void closedSessionCannotExecuteOrReopenAndDoesNotOwnPools() throws Exception {
        var session = engine.openSession();
        session.begin(REPEATABLE_READ);
        session.execute("insert into account(id,value) values ('1','discarded')");
        session.close();
        session.close();
        assertFalse(session.active());
        assertThrows(MvccException.class, () -> session.begin(READ_COMMITTED));
        assertThrows(MvccException.class, () -> session.execute("select * from account"));
        assertThrows(MvccException.class, session::commit);
        assertThrows(MvccException.class, session::rollback);
        try (var reader = engine.openSession()) {
            Assertions.assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
        try (var connection = poolA.getConnection()) {
            assertFalse(connection.isClosed());
        }
    }

    /**
     * Use the specified run time and use a one-page key to overwrite SQL page break behavior.
     *
     * @param runtime runtime
     * @param options options
     * @return the operation result
     */
    private PercolatorSqlEngine engine(final PercolatorRuntime runtime, final SqlExecutionOptions options) {
        return new PercolatorSqlEngine(runtime, new HybridSqlPlanner(), RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED,
                new SqlExecutionOptions(1, options.maxResultRows(), options.maxResultBytes(),
                        options.maxScannedRows(), options.maxScannedBytes()));
    }

    /**
     * Creates durable test data with autocommit SQL.
     *
     * @param sql sql
     */
    private void insert(final String sql) {
        try (var session = engine.openSession()) {
            session.execute(sql);
        }
    }

    /**
     * Draw business value in order of results.
     *
     * @param result result
     * @return the operation result
     */
    private static List<String> values(final SqlExecutionResult result) {
        return result.rows().stream().map(row -> row.get("value")).toList();
    }

    /**
     * Generates the UTF-8 test value.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Retains the real MySQL act, only to clear the border for commit returns loss and page staggering.
     */
    private static final class ProbedStore implements PercolatorStore {

        private final PercolatorStore delegate;

        private final AtomicBoolean keysObserved = new AtomicBoolean();

        Runnable afterKeys;

        boolean loseCommitReply;

        boolean failInspect;

        boolean failFinish;

        /**
         * Packing is real shard and does not construct false commit records.
         *
         * @param delegate delegate
         */
        ProbedStore(final PercolatorStore delegate) {
            this.delegate = delegate;
        }

        /**
         * Keeps the logical shard number.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return delegate.shardId();
        }

        /**
         * Initializes the true protocol table.
         */
        @Override
        public void initialize() {
            delegate.initialize();
        }

        /**
         * Original prewrite data and lock.
         *
         * @param start start
         * @param primary original Percolator primary identity
         * @param ttl ttl
         * @param mutations transaction write set
         */
        @Override
        public void prewrite(final long start, final PercolatorKey primary, final long ttl, final List<Mutation> mutations) {
            delegate.prewrite(start, primary, ttl, mutations);
        }

        /**
         * Read the same local Consistency snapshot as it is.
         *
         * @param key business key
         * @param readTs snapshot read timestamp
         * @return the operation result
         */
        @Override
        public Snapshot snapshot(final String key, final long readTs) {
            return delegate.snapshot(key, readTs);
        }

        /**
         * The first page is returned before another transaction is triggered and does not recommence business when holding JDBC connection.
         *
         * @param range range
         * @param after after
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public List<String> keys(final KeyRange range, final String after, final int limit) {
            List<String> result = delegate.keys(range, after, limit);
            if (afterKeys != null && keysObserved.compareAndSet(false, true)) {
                afterKeys.run();
            }
            return result;
        }

        /**
         * COMMIT real durable storage drops a reply, INSPECT failure retains UNKNOWN.
         *
         * @param key business key
         * @param start start
         * @param action action
         * @param commitTs durable commit timestamp
         * @return the operation result
         */
        @Override
        public Decision primary(final PercolatorKey key, final long start, final Action action, final long commitTs) {
            if (action == Action.INSPECT && failInspect) {
                throw unavailable(start);
            }
            Decision result = delegate.primary(key, start, action, commitTs);
            if (action == Action.COMMIT && loseCommitReply) {
                throw unavailable(start);
            }
            return result;
        }

        /**
         * Unattainable secondary maintains locks for subsequent operation to resolve.
         *
         * @param start start
         * @param primary original Percolator primary identity
         * @param decision decision
         * @param keys keys
         */
        @Override
        public void finish(final long start, final PercolatorKey primary, final Decision decision, final List<String> keys) {
            if (failFinish) {
                throw unavailable(start);
            }
            delegate.finish(start, primary, decision, keys);
        }

        /**
         * Bounded Found real left-over locks.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public List<Lock> pending(final int limit) {
            return delegate.pending(limit);
        }

        /**
         * Injecting identifiable storage is not within failure.
         *
         * @param start start
         * @return the operation result
         */
        private static MvccException unavailable(final long start) {
            return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected Percolator SQL outage", true, start);
        }
    }
}

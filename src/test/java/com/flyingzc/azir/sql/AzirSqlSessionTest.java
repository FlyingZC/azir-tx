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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import com.flyingzc.azir.txn.TransactionManager;
import com.flyingzc.azir.txn.TransactionOptions;
import org.apache.calcite.DataContexts;
import org.apache.calcite.linq4j.Enumerator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Azir SQL session performs the test.
 */
class AzirSqlSessionTest {

    /**
     * Verify Visible transaction can read its own writing and be read after commit.
     */
    @Test
    void explicitTransactionShouldReadOwnWriteAndCommit() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        AzirSqlEngine engine = engine(storage);

        try (SqlSession session = engine.openSession()) {
            session.execute("begin repeatable read");
            session.execute("insert into account (id, value) values ('u1', 'v1')");

            SqlExecutionResult result = session.execute("select value from account where id = 'u1'");

            Assertions.assertEquals("v1", result.singleRow().get("value"));
            assertNotNull(session.execute("commit").commitResult());
        }

        try (SqlSession session = engine.openSession()) {
            SqlExecutionResult result = session.execute("select value from account where id = 'u1'");

            Assertions.assertEquals("v1", result.singleRow().get("value"));
        }
    }

    /**
     * Verify autocommit writing takes place after the statement commit.
     */
    @Test
    void autocommitWriteShouldBeVisibleToNextStatement() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            SqlExecutionResult write = session.execute("insert into account (id, value) values ('u2', 'v2')");
            SqlExecutionResult read = session.execute("select value from account where id = 'u2'");

            Assertions.assertEquals(1, write.updateCount());
            assertNotNull(write.commitResult());
            Assertions.assertEquals("v2", read.singleRow().get("value"));
        }
    }

    /**
     * Verify updates and removes both from the existing transaction core.
     */
    @Test
    void updateAndDeleteShouldUseMvccCore() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('u3', 'v3')");
            session.execute("update account set value = 'v4' where id = 'u3'");

            Assertions.assertEquals("v4", session.execute("select value from account where id = 'u3'").singleRow().get("value"));

            session.execute("delete from account where id = 'u3'");

            assertTrue(session.execute("select value from account where id = 'u3'").rows().isEmpty());
        }
    }

    /**
     * Verify planner can be replaced externally.
     */
    @Test
    void shouldUseInjectedPlanner() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        SqlPlanner planner = sql -> new SqlPlan.InsertByKeyPlan("account", "u4", "v4");
        AzirSqlEngine engine = new AzirSqlEngine(
                manager(storage),
                planner,
                ShardResolver.singleShard("s0"),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED);

        try (SqlSession session = engine.openSession()) {
            session.execute("external syntax");

            Assertions.assertEquals("v4", readCommittedValue(storage, "account", "u4"));
        }
    }

    /**
     * The verify shard solver receives a table name and primary key and decides to access the shard.
     */
    @Test
    void shouldUseInjectedShardResolver() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("custom");
        AtomicReference<String> routed = new AtomicReference<>();
        ShardResolver resolver = (table, key) -> {
            routed.set(table + ":" + key);
            return "custom";
        };
        AzirSqlEngine engine = new AzirSqlEngine(
                manager(storage),
                new SimpleSqlPlanner(),
                resolver,
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED);

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('u5', 'v5')");

            Assertions.assertEquals("account:u5", routed.get());
            Assertions.assertEquals("v5", session.execute("select value from account where id = 'u5'").singleRow().get("value"));
        }
    }

    /**
     * Verify does not function transaction when commit is wrong.
     */
    @Test
    void commitWithoutActiveTransactionShouldFail() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            MvccException ex = assertThrows(MvccException.class, () -> session.execute("commit"));

            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, ex.errorCode());
        }
    }

    /**
     * Verify RR Visible transaction does not see any other commit during transaction.
     */
    @Test
    void repeatableReadShouldKeepStableSnapshotAcrossStatements() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));
        try (SqlSession setup = engine.openSession()) {
            setup.execute("insert into account (id, value) values ('u6', 'v1')");
        }

        try (SqlSession rr = engine.openSession(); SqlSession writer = engine.openSession()) {
            rr.execute("begin repeatable read");
            Assertions.assertEquals("v1", rr.execute("select value from account where id = 'u6'").singleRow().get("value"));

            writer.execute("update account set value = 'v2' where id = 'u6'");

            Assertions.assertEquals("v1", rr.execute("select value from account where id = 'u6'").singleRow().get("value"));
            rr.execute("rollback");
        }
    }

    /**
     * Verify RC Visible transaction can see new commit version in each statement.
     */
    @Test
    void readCommittedShouldRefreshSnapshotAcrossStatements() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));
        try (SqlSession setup = engine.openSession()) {
            setup.execute("insert into account (id, value) values ('u7', 'v1')");
        }

        try (SqlSession rc = engine.openSession(); SqlSession writer = engine.openSession()) {
            rc.execute("begin read committed");
            Assertions.assertEquals("v1", rc.execute("select value from account where id = 'u7'").singleRow().get("value"));

            writer.execute("update account set value = 'v2' where id = 'u7'");

            Assertions.assertEquals("v2", rc.execute("select value from account where id = 'u7'").singleRow().get("value"));
            rc.execute("rollback");
        }
    }

    /**
     * Verify autocommit will not continue with rollback transaction when unknown outcome is encountered.
     */
    @Test
    void autocommitShouldNotRollbackUnknownCommitResult() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.failCommitWithStatusUnknown(true);
        AzirSqlEngine engine = engine(storage);

        try (SqlSession session = engine.openSession()) {
            MvccException ex = assertThrows(
                    MvccException.class,
                    () -> session.execute("insert into account (id, value) values ('u8', 'v1')"));

            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
            assertFalse(ex.retryable());
            Assertions.assertEquals(0, storage.abortCount());
        }
    }

    /**
     * Verify Visible commit will not close session after encountering unknown outcome. Nor will rollback transaction.
     */
    @Test
    void explicitCommitShouldDetachUnknownCommitResultBeforeClose() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        AzirSqlEngine engine = engine(storage);

        try (SqlSession session = engine.openSession()) {
            session.execute("begin");
            session.execute("insert into account (id, value) values ('u9', 'v1')");
            storage.failCommitWithStatusUnknown(true);

            MvccException ex = assertThrows(MvccException.class, () -> session.execute("commit"));

            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
            assertFalse(ex.retryable());
            assertFalse(session.active());
        }
        Assertions.assertEquals(0, storage.abortCount());
        try (SqlSession reader = engine.openSession()) {
            Assertions.assertEquals("v1", reader.execute("select value from account where id = 'u9'").singleRow().get("value"));
        }
    }

    /**
     * Verify Calcite can execute projectors, expressions, filters, sorting and limiting row numbers.
     */
    @Test
    void calciteShouldExecuteProjectionFilterSortAndLimit() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            SqlExecutionResult inserted = session.execute(
                    "insert into account (id, value) values ('1', 'alpha'), ('2', 'beta'), ('3', 'gamma')");
            SqlExecutionResult selected = session.execute("""
                    select id, upper(value) as normalized
                    from account
                    where value >= 'beta'
                    order by value desc
                    limit 2
                    """);

            Assertions.assertEquals(3, inserted.updateCount());
            Assertions.assertEquals(List.of("3", "2"), selected.rows().stream().map(row -> row.get("id")).toList());
            Assertions.assertEquals(List.of("GAMMA", "BETA"), selected.rows().stream().map(row -> row.get("normalized")).toList());
        }
    }

    /**
     * Verify Calcite JOIN, Group Aggregation, HAVING and Sorting executes on the MVCC snapshot line.
     */
    @Test
    void calciteShouldExecuteJoinAggregationAndHaving() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        AzirSqlEngine engine = calciteEngine(storage);

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('1', 'gold'), ('2', 'gold'), ('3', 'silver')");
            session.execute("insert into profile (id, value) values ('1', 'active'), ('2', 'active'), ('3', 'inactive')");
            storage.clearScanReadTimestamps();

            SqlExecutionResult result = session.execute("""
                    select a.value as category, count(*) as total
                    from account a
                    join profile p on a.id = p.id
                    where p.value = 'active'
                    group by a.value
                    having count(*) >= 2
                    order by total desc
                    """);

            Assertions.assertEquals(1, result.rows().size());
            Assertions.assertEquals("gold", result.singleRow().get("category"));
            Assertions.assertEquals("2", result.singleRow().get("total"));
            Assertions.assertEquals(2, storage.scanReadTimestamps().size());
            Assertions.assertEquals(1, new HashSet<>(storage.scanReadTimestamps()).size());
        }
    }

    /**
     * Verify INSERT SELECT, Expression UPDATE and Condition DELETE all write back to the MVCC transaction core.
     */
    @Test
    void calciteShouldExecuteInsertSelectUpdateAndDelete() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('1', 'alpha'), ('2', 'beta'), ('3', 'delta')");

            SqlExecutionResult updated = session.execute(
                    "update account set value = upper(value) where id in ('1', '2')");
            SqlExecutionResult copied = session.execute("""
                    insert into archive (id, value)
                    select id, value from account where value like 'A%'
                    """);
            SqlExecutionResult deleted = session.execute("delete from account where value like 'd%'");

            Assertions.assertEquals(2, updated.updateCount());
            Assertions.assertEquals(1, copied.updateCount());
            Assertions.assertEquals(1, deleted.updateCount());
            Assertions.assertEquals("ALPHA", session.execute("select value from archive where id = '1'").singleRow().get("value"));
            assertTrue(session.execute("select * from account where id = '3'").rows().isEmpty());
        }
    }

    /**
     * Verify Different logical tables of the same primary key in the same physics shard do not overwhelm each other.
     */
    @Test
    void sqlTablesShouldUseIndependentPhysicalKeyNamespaces() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('same', 'account-value')");
            session.execute("insert into profile (id, value) values ('same', 'profile-value')");

            Assertions.assertEquals(
                    "account-value",
                    session.execute("select value from account where id = 'same'").singleRow().get("value"));
            Assertions.assertEquals(
                    "profile-value",
                    session.execute("select value from profile where id = 'same'").singleRow().get("value"));
        }
    }

    /**
     * The complex scans in verify visible transaction can read the transaction buffer script that is not yet commit.
     */
    @Test
    void calciteQueryShouldReadOwnWritesInsideExplicitTransaction() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("begin repeatable read");
            session.execute("insert into account (id, value) values ('1', 'alpha'), ('2', 'beta')");

            SqlExecutionResult result = session.execute(
                    "select count(*) as total from account where value is not null");

            Assertions.assertEquals("2", result.singleRow().get("total"));
            session.execute("rollback");
        }

        try (SqlSession verification = engine.openSession()) {
            assertTrue(verification.execute("select * from account").rows().isEmpty());
        }
    }

    /**
     * A complex query under the verify Hash route scans all shard and merges the sorting results.
     */
    @Test
    void calciteQueryShouldScanAllHashShards() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        ShardResolver resolver = ShardResolver.hashByKey(List.of("s0", "s1"));
        AzirSqlEngine engine = AzirSqlEngine.calcite(
                manager(shard0, shard1),
                resolver,
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED);

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('a', 'one'), ('b', 'two'), ('c', 'three')");

            SqlExecutionResult result = session.execute("select id from account order by id");

            Assertions.assertEquals(List.of("a", "b", "c"), result.rows().stream().map(row -> row.get("id")).toList());
            assertFalse(shard0.committedValues.isEmpty());
            assertFalse(shard1.committedValues.isEmpty());
        }
    }

    /**
     * Verify You can insert a simple planner to retain the primary key uniqueness, and no new data will be added if you update or delete an existing line.
     */
    @Test
    void simplePlannerShouldRespectPrimaryKeyAndMissingRows() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "original", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = new AzirSqlEngine(manager, new SimpleSqlPlanner(), ShardResolver.singleShard("s0"),
                        SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED).openSession()) {
            assertThrows(MvccException.class,
                    () -> session.execute("insert into account (id, value) values ('a', 'replacement')"));
            Assertions.assertEquals(0, session.execute("update account set value = 'new' where id = 'missing'").updateCount());
            Assertions.assertEquals(0, session.execute("delete from account where id = 'missing'").updateCount());
            Assertions.assertEquals("original", session.execute("select value from account where id = 'a'").singleRow().get("value"));
            assertTrue(session.execute("select value from account where id = 'missing'").rows().isEmpty());
        }
    }

    /**
     * Verify Unsorted LIMIT only consumes the first bounded page and does not call the whole sheet scan interface.
     */
    @Test
    void calciteLimitShouldReadOnlyOnePage() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        for (int index = 0; index < 100; index++) {
            storage.seed("account", "k" + index, "value", 1L);
        }
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 2).openSession()) {
            Assertions.assertEquals(1, session.execute("select id from account limit 1").rows().size());
            Assertions.assertEquals(1, storage.pageRanges.size());
            Assertions.assertEquals(2, storage.scannedKeyCount);
            Assertions.assertEquals(0, storage.fullScanCount);
        }
    }

    /**
     * Verify LIMIT will not open a follow-up shard that has not yet been consumed.
     */
    @Test
    void calciteLimitShouldLeaveLaterShardsUnread() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "b", "first", 1L);
        shard1.seed("account", "a", "second", 1L);
        try (
                TransactionManager manager = manager(shard0, shard1);
                SqlSession session = pagedEngine(manager, ShardResolver.hashByKey(List.of("s0", "s1")), 1).openSession()) {
            Assertions.assertEquals("b", session.execute("select id from account limit 1").singleRow().get("id"));
            Assertions.assertEquals(1, shard0.pageRanges.size());
            assertTrue(shard1.pageRanges.isEmpty());
        }
    }

    /**
     * LIMIT cannot cut cross-shard scan in advance when there is a residual filter.
     */
    @Test
    void calciteFilteredLimitMustReachLaterShard() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "a", "skip", 1L);
        shard1.seed("account", "b", "match", 1L);
        try (
                TransactionManager manager = manager(shard0, shard1);
                SqlSession session = pagedEngine(manager, ShardResolver.hashByKey(List.of("s0", "s1")), 1).openSession()) {
            Assertions.assertEquals("b", session.execute("select id from account where value = 'match' limit 1")
                    .singleRow().get("id"));
            Assertions.assertEquals(1, shard0.pageRanges.size());
            Assertions.assertEquals(1, shard1.pageRanges.size());
        }
    }

    /**
     * Verify Table registration, creation and closure of an item count do not trigger additional reading and cannot continue to remove pages after closing.
     */
    @Test
    void calciteEnumeratorShouldReadLazilyAndStopAfterClose() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "one", 1L);
        storage.seed("account", "b", "two", 1L);
        try (TransactionManager manager = manager(storage)) {
            Transaction tx = manager.begin(IsolationLevel.READ_COMMITTED);
            CalciteSnapshotTable table = new CalciteSnapshotTable(manager, tx, manager.beginReadStatement(tx),
                    "account", ShardResolver.singleShard("s0"), new SqlStorageKeyCodec(), SqlValueCodec.utf8(), 1,
                    new SqlScanBudget(tx.txId(), SqlExecutionOptions.defaults()));
            try (Enumerator<Object[]> enumerator = table.scan(DataContexts.EMPTY, new ArrayList<>(), new int[]{0}).enumerator()) {
                assertTrue(storage.pageRanges.isEmpty());
                assertTrue(enumerator.moveNext());
                Assertions.assertEquals("a", enumerator.current()[0]);
                enumerator.close();
                assertFalse(enumerator.moveNext());
                assertThrows(IllegalStateException.class, enumerator::reset);
                Assertions.assertEquals(1, storage.pageRanges.size());
            }
            manager.rollback(tx);
        }
    }

    /**
     * Verify Recreates the page break cursor when recleaning, but still reads the original statement snapshot.
     */
    @Test
    void calciteEnumeratorResetShouldReuseStatementSnapshot() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "old", 1L);
        try (TransactionManager manager = manager(storage)) {
            Transaction tx = manager.begin(IsolationLevel.READ_COMMITTED);
            var snapshot = manager.beginReadStatement(tx);
            CalciteSnapshotTable table = new CalciteSnapshotTable(manager, tx, snapshot, "account",
                    ShardResolver.singleShard("s0"), new SqlStorageKeyCodec(), SqlValueCodec.utf8(), 1,
                    new SqlScanBudget(tx.txId(), SqlExecutionOptions.defaults()));
            try (Enumerator<Object[]> enumerator = table.scan(DataContexts.EMPTY, new ArrayList<>(), new int[]{1}).enumerator()) {
                assertTrue(enumerator.moveNext());
                Assertions.assertEquals("old", enumerator.current()[0]);
                assertFalse(enumerator.moveNext());
                storage.seed("account", "a", "new", snapshot.readTs() + 1);
                enumerator.reset();
                Assertions.assertEquals(1, storage.pageRanges.size());
                assertTrue(enumerator.moveNext());
                Assertions.assertEquals("old", enumerator.current()[0]);
                Assertions.assertEquals(1, new HashSet<>(storage.scanReadTimestamps).size());
            }
            manager.rollback(tx);
        }
    }

    /**
     * Verify Equivalent primary key pushes only to one shard and keeps Calcite non-main key filters.
     */
    @Test
    void calcitePointPredicateShouldUseOnlyRoutedGet() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "b", "other", 1L);
        shard1.seed("account", "a", "target", 1L);
        try (
                TransactionManager manager = manager(shard0, shard1);
                SqlSession session = pagedEngine(manager, ShardResolver.hashByKey(List.of("s0", "s1")), 1).openSession()) {
            Assertions.assertEquals("target", session.execute("select value from account where id = 'a'").singleRow().get("value"));
            Assertions.assertEquals(List.of(new SqlStorageKeyCodec().encode("account", "a")), shard1.pointReadKeys);
            assertTrue(shard0.pointReadKeys.isEmpty());
            assertTrue(shard0.pageRanges.isEmpty());
            assertTrue(shard1.pageRanges.isEmpty());
            assertTrue(session.execute("select id from account where id = 'a' and value = 'missing'").rows().isEmpty());
        }
    }

    /**
     * The empty visible pages created by verify tombstone continue with page breaks on non-empty raw cursor.
     */
    @Test
    void calciteScanShouldContinueAfterEmptyVisiblePage() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", null, 1L);
        storage.seed("account", "b", "visible", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            Assertions.assertEquals("b", session.execute("select id from account limit 1").singleRow().get("id"));
            Assertions.assertEquals(2, storage.pageRanges.size());
            Assertions.assertEquals(1, new HashSet<>(storage.scanReadTimestamps).size());
        }
    }

    /**
     * Verify INSERT SELECT multipage reading and target primary key recheck snapshot, with new commit on the page visible only in the next sentence.
     */
    @Test
    void calciteInsertSelectShouldShareSnapshotWithDuplicateChecks() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "old", 1L);
        storage.afterFirstPage = context -> storage.seed("account", "b", "new", context.readTs() + 1);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals(2, session.execute("insert into archive (id, value) select id, value from account").updateCount());
            Assertions.assertEquals(2, storage.scanReadTimestamps.size());
            Assertions.assertEquals(2, storage.pointReadTimestamps.size());
            Set<Long> allReadTimestamps = new HashSet<>(storage.scanReadTimestamps);
            allReadTimestamps.addAll(storage.pointReadTimestamps);
            Assertions.assertEquals(1, allReadTimestamps.size());
            Assertions.assertEquals("old", session.execute("select value from archive where id = 'b'").singleRow().get("value"));
            Assertions.assertEquals("new", session.execute("select value from account where id = 'b'").singleRow().get("value"));
            session.rollback();
        }
    }

    /**
     * Verify UPDATE New Primary Check Existence Checks Select Row snapshot.
     */
    @Test
    void calciteUpdateShouldShareSnapshotWithNewKeyChecks() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals(2, session.execute("update account set id = concat(id, 'x')").updateCount());
            Assertions.assertEquals(2, storage.pointReadTimestamps.size());
            Set<Long> allReadTimestamps = new HashSet<>(storage.scanReadTimestamps);
            allReadTimestamps.addAll(storage.pointReadTimestamps);
            Assertions.assertEquals(1, allReadTimestamps.size());
            Assertions.assertEquals(List.of("ax", "bx"), session.execute("select id from account order by id").rows().stream()
                    .map(row -> row.get("id")).toList());
            session.rollback();
        }
    }

    /**
     * The verify range does not contain primary keys with the same prefix but longer by mistake.
     */
    @Test
    void calciteAsciiRangeShouldUseExactBinaryBoundary() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "equal", 1L);
        storage.seed("account", "a\u0000", "suffix", 1L);
        storage.seed("account", "ab", "longer", 1L);
        storage.seed("account", "b", "last", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            Assertions.assertEquals("a", session.execute("select id from account where id <= 'a'").singleRow().get("id"));
            Assertions.assertEquals(new SqlStorageKeyCodec().encode("account", "a") + '\u0000',
                    storage.pageRanges.getFirst().endKeyExclusive());
            storage.pageRanges.clear();
            Assertions.assertEquals(List.of("a\u0000", "ab", "b"),
                    session.execute("select id from account where 'a' < id order by id").rows().stream()
                            .map(row -> row.get("id")).toList());
            Assertions.assertEquals(new SqlStorageKeyCodec().encode("account", "a") + '\u0000',
                    storage.pageRanges.getFirst().startKeyInclusive());
        }
    }

    /**
     * Verify value conversion, non-ASCII range and NULL will not be misinterpreted as binary primary key check.
     */
    @Test
    void calciteShouldKeepUnsafePredicatesForResidualEvaluation() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "02", "leading zero", 1L);
        storage.seed("account", "2", "plain", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            Assertions.assertEquals(2, session.execute("select id from account where cast(id as integer) = 2").rows().size());
            assertTrue(storage.pointReadKeys.isEmpty());
            storage.pageRanges.clear();
            Assertions.assertEquals(2, session.execute("select id from account where id < '\u00e9'").rows().size());
            SqlStorageKeyCodec keys = new SqlStorageKeyCodec();
            assertTrue(storage.pageRanges.stream().allMatch(range -> range.startKeyInclusive().equals(keys.rangeStart("account"))
                    && range.endKeyExclusive().equals(keys.rangeEnd("account"))));
            assertTrue(session.execute("select id from account where id = cast(null as varchar)").rows().isEmpty());
            assertTrue(storage.pointReadKeys.isEmpty());
        }
    }

    /**
     * Verify Sorting and Aggregation can consume multiple pages, and page size cannot be misconstrued as a line number limit for the whole relationship.
     */
    @Test
    void calciteSortAndAggregateShouldConsumeAllPages() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "1", 1L);
        storage.seed("account", "b", "2", 1L);
        storage.seed("account", "c", "3", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            Assertions.assertEquals("c", session.execute("select id from account order by id desc limit 1").singleRow().get("id"));
            Assertions.assertEquals(3, storage.pageRanges.size());
            Assertions.assertEquals("3", session.execute("select count(*) as total from account").singleRow().get("total"));
            Assertions.assertEquals(6, storage.pageRanges.size());
        }
    }

    /**
     * The sorting, grouping and residual filter of the verify single-line result must also follow input line number budget.
     */
    @Test
    void calciteBlockingOperatorsShouldRespectInputRowBudget() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "same", 1L);
        storage.seed("account", "b", "same", 1L);
        storage.seed("account", "c", "same", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 2, 1024)).openSession()) {
            for (String sql : List.of("select id from account order by id desc limit 1",
                    "select value, count(*) as total from account group by value",
                    "select count(*) as total from account where value = 'absent'")) {
                MvccException failure = assertThrows(MvccException.class, () -> session.execute(sql));
                Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
                assertTrue(failure.getMessage().contains("input scan row budget"));
            }
            Assertions.assertEquals(0, storage.fullScanCount);
        }
    }

    /**
     * Verify Enter budget to overwhelm the page prep, even if LIMIT only consumes the first line of the page.
     */
    @Test
    void calciteInputBudgetShouldChargePrefetchedRows() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(2, 1, 1024, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select id from account limit 1"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan row budget"));
            Assertions.assertEquals(1, storage.pageRanges.size());
        }
    }

    /**
     * The verify cross-logic table shares the same input budget, and individual tables do not exceed or exceed their totals.
     */
    @Test
    void calciteInputRowBudgetShouldBeSharedAcrossTables() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("profile", "b", "second", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("""
                    select count(*) as total from (
                        select id from account union all select id from profile
                    ) combined
                    """));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan row budget"));
            Assertions.assertEquals(2, storage.pageRanges.stream().map(KeyRange::startKeyInclusive).distinct().count());
        }
    }

    /**
     * Verify Different physical shard s of the same logical table also co-consumption budget.
     */
    @Test
    void calciteInputRowBudgetShouldBeSharedAcrossShards() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "b", "first", 1L);
        shard1.seed("account", "a", "second", 1L);
        try (
                TransactionManager manager = manager(shard0, shard1);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.hashByKey(List.of("s0", "s1")),
                        SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED,
                        new SqlExecutionOptions(1, 1, 1024, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("select count(*) as total from account"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            Assertions.assertEquals(1, shard0.pageRanges.size());
            Assertions.assertEquals(1, shard1.pageRanges.size());
        }
    }

    /**
     * Verify Projection does not include value at the original bytes and complete physical key billing.
     */
    @Test
    void calciteInputByteBudgetShouldIncludeUnprojectedValue() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "中", 1L);
        long tooSmall = Utf8KeyOrder.encode(new SqlStorageKeyCodec().encode("account", "a")).length + 2L;
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 10, tooSmall)).openSession()) {
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("select count(*) as total from account"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan byte budget"));
        }
    }

    /**
     * An verify check and another table page read a common cumulative byte input, which cannot be recreated separately.
     */
    @Test
    void calciteInputByteBudgetShouldCombinePointAndPageReads() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "one", 1L);
        storage.seed("profile", "b", "two", 1L);
        SqlStorageKeyCodec keys = new SqlStorageKeyCodec();
        long tooSmall = Utf8KeyOrder.encode(keys.encode("account", "a")).length
                + (long) Utf8KeyOrder.encode(keys.encode("profile", "b")).length + 5L;
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 10, tooSmall)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("""
                    select count(*) as total from (
                        select id from account where id = 'a' union all select id from profile
                    ) combined
                    """));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan byte budget"));
            Assertions.assertEquals(1, storage.pointReadKeys.size());
            Assertions.assertEquals(1, storage.pageRanges.size());
        }
    }

    /**
     * The cumulative cost of multiple verify liftings and reset re-cleaning is such that no more than the limit of any one lift can continue to be exported.
     */
    @Test
    void calciteInputBudgetShouldSurviveEnumeratorReset() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "value", 1L);
        try (TransactionManager manager = manager(storage)) {
            Transaction tx = manager.begin(IsolationLevel.READ_COMMITTED);
            SqlScanBudget budget = new SqlScanBudget(tx.txId(), new SqlExecutionOptions(1, 1, 1024, 2, 1024));
            CalciteSnapshotTable table = new CalciteSnapshotTable(manager, tx, manager.beginReadStatement(tx), "account",
                    ShardResolver.singleShard("s0"), new SqlStorageKeyCodec(), SqlValueCodec.utf8(), 1, budget);
            var rows = table.scan(DataContexts.EMPTY, new ArrayList<>(), new int[]{0});
            try (Enumerator<Object[]> first = rows.enumerator(); Enumerator<Object[]> second = rows.enumerator()) {
                assertTrue(first.moveNext());
                assertTrue(second.moveNext());
                first.reset();
                MvccException failure = assertThrows(MvccException.class, first::moveNext);
                Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
                assertThrows(MvccException.class, second::moveNext);
            }
            manager.rollback(tx);
        }
    }

    /**
     * Verify new statement obtained a new input budget and the snapshot review before return will not assign an additional RC timestamp.
     */
    @Test
    void calciteStatementShouldRenewBudgetWithoutExtraReadTimestamp() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "value", 1L);
        AtomicLong sequence = new AtomicLong(1L);
        try (
                TransactionManager manager = new TransactionManager(sequence::incrementAndGet,
                        new InMemoryTransactionMetadataStore(), TransactionOptions.defaults(), Map.of("s0", storage));
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 1, 1024)).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            for (int index = 0; index < 2; index++) {
                long before = sequence.get();
                Assertions.assertEquals("1", session.execute("select count(*) as total from account").singleRow().get("total"));
                Assertions.assertEquals(before + 1, sequence.get());
            }
            session.rollback();
        }
    }

    /**
     * Verify expires at decoding or relationship calculation stage lease after the last page is read, visible transaction cannot return the expired result.
     */
    @Test
    void calciteShouldValidateLeaseAfterFinalPageProcessing() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "value", 1L);
        AtomicLong now = new AtomicLong(1_000L);
        SqlValueCodec expiringCodec = new SqlValueCodec() {

            /**
             * Keeps the default UTF-8 encoded behaviour.
             *
             * @param value value to write
             * @return the operation result
             */
            @Override
            public byte[] encode(final String value) {
                return SqlValueCodec.utf8().encode(value);
            }

            /**
             * Moves the test clock after you have stored a returned page and does not perform waiting or real time-consuming work.
             *
             * @param value value to write
             * @return the operation result
             */
            @Override
            public String decode(final byte[] value) {
                now.addAndGet(TransactionOptions.defaults().limits().timeoutMillis() + 1);
                return SqlValueCodec.utf8().decode(value);
            }
        };
        try (
                TransactionManager manager = new TransactionManager(new AtomicTimestampOracle(1L), metadataWithClock(now),
                        TransactionOptions.defaults(), Map.of("s0", storage));
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), expiringCodec,
                        IsolationLevel.READ_COMMITTED).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("select value from account order by value desc limit 1"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_TIMEOUT, failure.errorCode());
            Assertions.assertEquals(1, storage.pageRanges.size());
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            session.rollback();
        }
    }

    /**
     * Verify resulted in errors over the number of lines rather than a return to successful results that had been cut off.
     */
    @Test
    void calciteResultRowBudgetShouldFailExplicitly() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select id from account"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("row budget"));
        }
    }

    /**
     * The collected prefix will not be applied when the verify DML source result exceeds its limit.
     */
    @Test
    void calciteInsertSelectBudgetFailureShouldNotWritePrefix() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("insert into archive (id, value) select id, value from account"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(session.execute("select * from archive").rows().isEmpty());
        }
    }

    /**
     * Verify byte budget coded on UTF-8 instead of character numbers. Unusual and visible transaction cannot continue commit.
     */
    @Test
    void calciteResultByteBudgetShouldCountUtf8AndInvalidateTransaction() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "中", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 10, 2)).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select value from account"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("byte budget"));
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            session.rollback();
            Assertions.assertEquals("a", session.execute("select id from account").singleRow().get("id"));
        }
    }

    /**
     * Verify The second line prewrite failed rollback -wide transaction, including the writing of previous successful statements, and prevented hidden autocommit.
     */
    @Test
    void calciteDmlFailureShouldRollbackWholeExplicitTransaction() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (
                TransactionManager manager = new TransactionManager(new AtomicTimestampOracle(1L),
                        new InMemoryTransactionMetadataStore(), TransactionOptions.failFastDefaults(), Map.of("s0", storage));
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            session.execute("insert into account (id, value) values ('prior', 'saved')");
            storage.failPrewriteKey = new SqlStorageKeyCodec().encode("account", "b");
            MvccException failure = assertThrows(MvccException.class, () -> session.execute(
                    "insert into account (id, value) values ('a', 'one'), ('b', 'two')"));
            Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, failure.errorCode());
            assertTrue(failure.getMessage().contains("entire explicit transaction rolled back"));
            Assertions.assertEquals(1, storage.abortCount());
            assertTrue(storage.intentsByTx.isEmpty());
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            assertThrows(MvccException.class, () -> session.execute("select * from account"));
            session.rollback();
            assertTrue(session.execute("select * from account").rows().isEmpty());
        }
    }

    /**
     * A repeat primary key in an verify INSERT does not leave a part to write.
     */
    @Test
    void calciteInsertShouldRejectDuplicateKeysWithinStatement() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertThrows(MvccException.class, () -> session.execute(
                    "insert into account (id, value) values ('a', 'first'), ('a', 'second')"));
            assertTrue(session.execute("select * from account").rows().isEmpty());
        }
    }

    /**
     * Verify UPDATE does not overwrite or delete one of the rows when exchanging two primary keys.
     */
    @Test
    void calciteUpdateShouldPreserveBothRowsWhenKeysSwap() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            Assertions.assertEquals(2, session.execute("update account set id = case when id = 'a' then 'b' else 'a' end").updateCount());
            Assertions.assertEquals(List.of("second", "first"), session.execute("select value from account order by id").rows().stream()
                    .map(row -> row.get("value")).toList());
        }
    }

    /**
     * The verify test adapter states that the capability is real: intent after commit and not yet materialization by committee Ts visible, tombstone still hides old values.
     */
    @Test
    void testAdapterShouldResolveCommittedIntentsAtSnapshotBoundary() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        InMemoryTransactionMetadataStore metadata = new InMemoryTransactionMetadataStore();
        storage.configureMetadataStore(metadata);
        String key = new SqlStorageKeyCodec().encode("account", "a");
        storage.seed("account", "a", "old", 1L);
        metadata.create(10L, 10L, IsolationLevel.READ_COMMITTED);
        storage.prewrite(10L, 10L, List.of(new Mutation(key, MutationType.DELETE, null)));
        assertFalse(storage.get(new ReadContext(30L, 30L, IsolationLevel.READ_COMMITTED), key).orElseThrow().deleted());
        metadata.markCommitted(10L, 20L);
        assertFalse(storage.get(new ReadContext(30L, 19L, IsolationLevel.READ_COMMITTED), key).orElseThrow().deleted());
        assertTrue(storage.get(new ReadContext(30L, 20L, IsolationLevel.READ_COMMITTED), key).orElseThrow().deleted());
        assertTrue(storage.scanPage(new ReadContext(30L, 20L, IsolationLevel.READ_COMMITTED),
                new KeyRange(new SqlStorageKeyCodec().rangeStart("account"), new SqlStorageKeyCodec().rangeEnd("account")),
                null, 1).values().isEmpty());
    }

    /**
     * Verify does not call the planner or create transaction, all transaction operations are rejected and repeated off without side effects.
     */
    @Test
    void closedSessionShouldRejectFurtherUseBeforePlanning() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        AtomicReference<Boolean> planned = new AtomicReference<>(false);
        SqlPlanner planner = sql -> {
            planned.set(true);
            return new SqlPlan.SelectByKeyPlan("account", "a");
        };
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = new AzirSqlEngine(manager, planner, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                        IsolationLevel.READ_COMMITTED).openSession()) {
            session.close();
            assertFalse(session.active());
            assertThrows(MvccException.class, () -> session.begin(IsolationLevel.READ_COMMITTED));
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select anything"));
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, failure.errorCode());
            assertTrue(failure.getMessage().contains("session is closed"));
            assertFalse(planned.get());
            assertThrows(MvccException.class, session::commit);
            assertThrows(MvccException.class, session::rollback);
            session.close();
            Assertions.assertEquals(0, storage.abortCount());
        }
    }

    /**
     * Verify closes active session only once cleans up the whole transaction and previous writing is invisible.
     */
    @Test
    void closeShouldRollbackTransactionOnlyOnce() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (TransactionManager manager = manager(storage)) {
            AzirSqlEngine engine = pagedEngine(manager, ShardResolver.singleShard("s0"), 1);
            try (SqlSession session = engine.openSession()) {
                session.begin(IsolationLevel.READ_COMMITTED);
                session.execute("insert into account (id, value) values ('a', 'value')");
                session.close();
                session.close();
                Assertions.assertEquals(1, storage.abortCount());
                assertThrows(MvccException.class, () -> session.begin(IsolationLevel.READ_COMMITTED));
            }
            try (SqlSession reader = engine.openSession()) {
                assertTrue(reader.execute("select * from account").rows().isEmpty());
            }
        }
    }

    /**
     * The verify clean-up transaction failed and the closed session closed will not be reopened. Repeated shut-down does not repeat failed clean-up requests.
     */
    @Test
    void rollbackFailureDuringCloseShouldKeepSessionClosed() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (
                TransactionManager manager = manager(storage);
                SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            session.execute("insert into account (id, value) values ('a', 'value')");
            storage.failAbort = true;
            try {
                assertThrows(MvccException.class, session::close);
                assertFalse(session.active());
                assertThrows(MvccException.class, () -> session.begin(IsolationLevel.READ_COMMITTED));
                assertThrows(MvccException.class, () -> session.execute("select * from account"));
                session.close();
                Assertions.assertEquals(1, storage.abortCount());
            } finally {
                storage.failAbort = false;
            }
        }
    }

    /**
     * Replaces only metadata interface clocks, and the rest of operations commission real memory realization to determine sexual overlay SQL returns before lease check.
     *
     * @param now now
     * @return the operation result
     */
    private TransactionMetadataStore metadataWithClock(final AtomicLong now) {
        TransactionMetadataStore delegate = new InMemoryTransactionMetadataStore();
        return (TransactionMetadataStore) Proxy.newProxyInstance(TransactionMetadataStore.class.getClassLoader(),
                new Class<?>[]{TransactionMetadataStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("currentTimeMillis")) {
                        return now.get();
                    }
                    // The default method for achieving coverage without memory is proxy-receiver, and internal clock calls are still tested as a double.
                    if (method.isDefault() && delegate.getClass()
                            .getMethod(method.getName(), method.getParameterTypes()).getDeclaringClass().isInterface()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (final InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /**
     * Creates a test engine for the specified page size and maintains the default result budget.
     *
     * @param manager manager
     * @param resolver resolver
     * @param pageSize page size
     * @return the operation result
     */
    private AzirSqlEngine pagedEngine(final TransactionManager manager, final ShardResolver resolver, final int pageSize) {
        SqlExecutionOptions defaults = SqlExecutionOptions.defaults();
        return AzirSqlEngine.calcite(manager, resolver, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED,
                new SqlExecutionOptions(pageSize, defaults.maxResultRows(), defaults.maxResultBytes()));
    }

    /**
     * Creates the default SQL engine.
     *
     * @param storage storage
     * @return the operation result
     */
    private AzirSqlEngine engine(final InMemoryTestStorageAdapter storage) {
        return new AzirSqlEngine(
                manager(storage),
                new SimpleSqlPlanner(),
                ShardResolver.singleShard(storage.shardId()),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED);
    }

    /**
     * Creates a test SQL engine using Calcite Relationship Planner.
     *
     * @param storage storage
     * @return the operation result
     */
    private AzirSqlEngine calciteEngine(final InMemoryTestStorageAdapter storage) {
        return AzirSqlEngine.calcite(
                manager(storage),
                ShardResolver.singleShard(storage.shardId()),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED);
    }

    /**
     * Creates the default test transaction manager.
     *
     * @param storage storage
     * @return the operation result
     */
    private TransactionManager manager(final InMemoryTestStorageAdapter storage) {
        return manager(new InMemoryTestStorageAdapter[]{storage});
    }

    /**
     * Creates an transaction manager with multiple tests for shard.
     *
     * @param storages storages
     * @return the operation result
     */
    private TransactionManager manager(final InMemoryTestStorageAdapter... storages) {
        return new TransactionManager(
                new AtomicTimestampOracle(1L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                java.util.Arrays.stream(storages).collect(java.util.stream.Collectors.toMap(
                        InMemoryTestStorageAdapter::shardId,
                        storage -> storage)));
    }

    /**
     * Reads the latest commit string in the test storage.
     *
     * @param storage storage
     * @param table table
     * @param key business key
     * @return the operation result
     */
    private String readCommittedValue(final InMemoryTestStorageAdapter storage, final String table, final String key) {
        String physicalKey = new SqlStorageKeyCodec().encode(table, key);
        return storage.get(new ReadContext(-1L, Long.MAX_VALUE, IsolationLevel.READ_COMMITTED), physicalKey)
                .map(VersionedValue::value)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElseThrow();
    }

    /**
     * Memory storage adapter for SQL session testing only.
     */
    private static final class InMemoryTestStorageAdapter implements StorageAdapter {

        private final String shardId;

        private final Map<String, NavigableMap<Long, VersionedValue>> committedValues = new HashMap<>();

        private final Map<Long, Map<String, Mutation>> intentsByTx = new HashMap<>();

        private final List<Long> scanReadTimestamps = new ArrayList<>();

        private final List<Long> pointReadTimestamps = new ArrayList<>();

        private final List<String> pointReadKeys = new ArrayList<>();

        private final List<KeyRange> pageRanges = new ArrayList<>();

        private TransactionMetadataStore metadataStore;

        private Consumer<ReadContext> afterFirstPage;

        private boolean failCommitWithStatusUnknown;

        private boolean failAbort;

        private String failPrewriteKey;

        private int scannedKeyCount;

        private int fullScanCount;

        private int abortCount;

        /**
         * Construct a memory test storage adaptor.
         *
         * @param shardId logical shard identifier
         */
        private InMemoryTestStorageAdapter(final String shardId) {
            this.shardId = shardId;
        }

        /**
         * Sets whether the participant materialization simulation failed and the commit unknown anomaly is packaged by the transaction manager as not retry.
         *
         * @param failCommitWithStatusUnknown fail commit with status unknown
         */
        private void failCommitWithStatusUnknown(final boolean failCommitWithStatusUnknown) {
            this.failCommitWithStatusUnknown = failCommitWithStatusUnknown;
        }

        /**
         * Bind authority metadata, only other transaction intent s that are already commit and not later than snapshot will be considered version.
         *
         * @param store store
         */
        @Override
        public void configureMetadataStore(final TransactionMetadataStore store) {
            metadataStore = store;
        }

        /**
         * The dot reading and scan of this adaptor achieves the commit intent resolution that allows for cross-shard snapshot testing.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * Creates a line that specifies the commit time to avoid additional reading at the preparatory stage of delayed scanning tests.
         *
         * @param table table
         * @param id id
         * @param value value to write
         * @param commitTs durable commit timestamp
         */
        private void seed(final String table, final String id, final String value, final long commitTs) {
            String key = new SqlStorageKeyCodec().encode(table, id);
            committedValues.computeIfAbsent(key, ignored -> new TreeMap<>()).put(commitTs,
                    new VersionedValue(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8),
                            value == null, commitTs, false));
        }

        /**
         * Returns the number of rollback s received for testing storage.
         *
         * @return the operation result
         */
        private int abortCount() {
            return abortCount;
        }

        /**
         * Empty scan to read timestamp records.
         */
        private void clearScanReadTimestamps() {
            scanReadTimestamps.clear();
        }

        /**
         * Returns to scan for timestamp records.
         *
         * @return the operation result
         */
        private List<Long> scanReadTimestamps() {
            return List.copyOf(scanReadTimestamps);
        }

        /**
         * Returns the current test shard number.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * Initialised test storage.
         */
        @Override
        public void initialize() {
        }

        /**
         * Press MVCC snapshot to read visible version.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            pointReadTimestamps.add(context.readTs());
            pointReadKeys.add(key);
            return visibleValue(context, key);
        }

        /**
         * Merges its own intent, visible commit version with the commit unx4 intent, and retains the latest tombstone mask synonym.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        private Optional<VersionedValue> visibleValue(final ReadContext context, final String key) {
            Mutation ownIntent = intentsByTx.getOrDefault(context.txId(), Map.of()).get(key);
            if (ownIntent != null) {
                return Optional.of(toIntentValue(ownIntent, context.readTs()));
            }
            NavigableMap<Long, VersionedValue> versions = committedValues.get(key);
            Map.Entry<Long, VersionedValue> visible = versions == null ? null : versions.floorEntry(context.readTs());
            VersionedValue result = visible == null ? null : visible.getValue();
            for (Map.Entry<Long, Map<String, Mutation>> entry : intentsByTx.entrySet()) {
                Mutation mutation = entry.getValue().get(key);
                if (mutation == null || metadataStore == null) {
                    continue;
                }
                var record = metadataStore.get(entry.getKey());
                if (record.isPresent() && record.get().status() == TxStatus.COMMITTED
                        && record.get().commitTs() != null && record.get().commitTs() <= context.readTs()
                        && (result == null || record.get().commitTs() > result.versionTs())) {
                    result = toCommittedValue(mutation, record.get().commitTs());
                }
            }
            return Optional.ofNullable(result);
        }

        /**
         * The key range scan is visible version.
         *
         * @param context context
         * @param keyRange key range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
            fullScanCount++;
            scanReadTimestamps.add(context.readTs());
            List<VersionedValue> result = new ArrayList<>();
            for (String key : keysInRange(keyRange)) {
                visibleValue(context, key).filter(value -> !value.deleted()).ifPresent(result::add);
            }
            return result;
        }

        /**
         * Press the original key to break and record reading, tombstone or invisible intent can generate empty pages with continued cursor.
         *
         * @param context context
         * @param keyRange key range
         * @param afterKeyExclusive after key exclusive
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public ScanPage scanPage(final ReadContext context, final KeyRange keyRange, final String afterKeyExclusive, final int limit) {
            scanReadTimestamps.add(context.readTs());
            pageRanges.add(keyRange);
            List<String> candidates = keysInRange(keyRange).stream()
                    .filter(key -> afterKeyExclusive == null || Utf8KeyOrder.compare(key, afterKeyExclusive) > 0)
                    .limit((long) limit + 1).toList();
            boolean hasMore = candidates.size() > limit;
            List<String> keys = hasMore ? candidates.subList(0, limit) : candidates;
            scannedKeyCount += keys.size();
            List<VersionedValue> values = keys.stream().map(key -> visibleValue(context, key))
                    .flatMap(Optional::stream).filter(value -> !value.deleted()).toList();
            ScanPage result = new ScanPage(values, hasMore ? keys.getLast() : null);
            if (afterFirstPage != null) {
                Consumer<ReadContext> hook = afterFirstPage;
                afterFirstPage = null;
                hook.accept(context);
            }
            return result;
        }

        /**
         * Holds the physical key space common to version and intent in a UTF-8 series.
         *
         * @param range range
         * @return the operation result
         */
        private Set<String> keysInRange(final KeyRange range) {
            Set<String> keys = new TreeSet<>(Utf8KeyOrder::compare);
            keys.addAll(committedValues.keySet());
            intentsByTx.values().forEach(intents -> keys.addAll(intents.keySet()));
            keys.removeIf(key -> Utf8KeyOrder.compare(key, range.startKeyInclusive()) < 0
                    || Utf8KeyOrder.compare(key, range.endKeyExclusive()) >= 0);
            return keys;
        }

        /**
         * Write to test inent and detect simple write conflict.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
            for (Mutation mutation : mutations) {
                if (mutation.key().equals(failPrewriteKey)) {
                    throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected prewrite failure", false, txId);
                }
                detectConflict(txId, startTs, mutation.key());
            }
            Map<String, Mutation> intents = intentsByTx.computeIfAbsent(txId, ignored -> new HashMap<>());
            for (Mutation mutation : mutations) {
                intents.put(mutation.key(), mutation);
            }
        }

        /**
         * Prepare to test incent.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
        }

        /**
         * Commit Test inent.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            commit(txId, commitTs, List.of(), true);
        }

        /**
         * Commit Test inent or directly use input changes to generate version.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            if (failCommitWithStatusUnknown) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected finalize failure", false, txId);
            }
            Map<String, Mutation> intentMutations = mutations.isEmpty()
                    ? intentsByTx.getOrDefault(txId, Map.of())
                    : mutations.stream().collect(java.util.stream.Collectors.toMap(Mutation::key, mutation -> mutation));
            for (Mutation mutation : intentMutations.values()) {
                committedValues
                        .computeIfAbsent(mutation.key(), ignored -> new TreeMap<>())
                        .put(commitTs, toCommittedValue(mutation, commitTs));
            }
            if (cleanupIntents) {
                intentsByTx.remove(txId);
            }
        }

        /**
         * Directly generate commit test version.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            for (Mutation mutation : mutations) {
                detectConflict(txId, startTs, mutation.key());
            }
            commit(txId, commitTs, mutations, true);
        }

        /**
         * Clears the specified transaction test inent.
         *
         * @param txId transaction identifier
         * @param mutations transaction write set
         */
        @Override
        public void cleanupIntents(final long txId, final List<Mutation> mutations) {
            intentsByTx.remove(txId);
        }

        /**
         * Roll back the test transaction.
         *
         * @param txId transaction identifier
         * @throws MvccException if the operation cannot complete
         */
        @Override
        public void abort(final long txId) {
            abortCount++;
            if (failAbort) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected abort failure", false, txId);
            }
            intentsByTx.remove(txId);
        }

        /**
         * Finds transaction where there is still a test incent.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return new HashSet<>(intentsByTx.keySet());
        }

        /**
         * Test storage does not execute GC.
         *
         * @param safePoint oldest retained snapshot timestamp
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            return 0;
        }

        /**
         * Test writing write conflict.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param key business key
         * @throws MvccException if the operation cannot complete
         */
        private void detectConflict(final long txId, final long startTs, final String key) {
            for (Map.Entry<Long, Map<String, Mutation>> entry : intentsByTx.entrySet()) {
                if (entry.getKey() != txId && entry.getValue().containsKey(key)) {
                    throw new MvccException(ErrorCode.WRITE_CONFLICT, "intent conflict on key " + key, true, txId);
                }
            }
            NavigableMap<Long, VersionedValue> versions = committedValues.get(key);
            if (versions != null && versions.higherKey(startTs) != null) {
                throw new MvccException(ErrorCode.WRITE_CONFLICT, "committed conflict on key " + key, true, txId);
            }
        }

        /**
         * Converts changes to test indent values.
         *
         * @param mutation mutation
         * @param readTs snapshot read timestamp
         * @return the operation result
         */
        private VersionedValue toIntentValue(final Mutation mutation, final long readTs) {
            return new VersionedValue(
                    mutation.key(),
                    mutation.value(),
                    mutation.type() == MutationType.DELETE,
                    readTs,
                    true);
        }

        /**
         * Convert changes to commit test version.
         *
         * @param mutation mutation
         * @param commitTs durable commit timestamp
         * @return the operation result
         */
        private VersionedValue toCommittedValue(final Mutation mutation, final long commitTs) {
            return new VersionedValue(
                    mutation.key(),
                    mutation.value(),
                    mutation.type() == MutationType.DELETE,
                    commitTs,
                    false);
        }
    }
}

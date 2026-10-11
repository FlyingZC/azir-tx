package mvcc.sql;

import mvcc.api.Transaction;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MutationType;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.ScanPage;
import mvcc.storage.StorageAdapter;
import mvcc.storage.Utf8KeyOrder;
import mvcc.storage.VersionedValue;
import mvcc.tso.AtomicTimestampOracle;
import mvcc.txn.TransactionManager;
import mvcc.txn.TransactionOptions;
import org.apache.calcite.DataContexts;
import org.apache.calcite.linq4j.Enumerator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Azir SQL 会话执行测试。
 */
class AzirSqlSessionTest {
    /**
     * 验证显式事务可以读到自己的写入并在提交后被新会话读取。
     */
    @Test
    void explicitTransactionShouldReadOwnWriteAndCommit() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        AzirSqlEngine engine = engine(storage);

        try (SqlSession session = engine.openSession()) {
            session.execute("begin repeatable read");
            session.execute("insert into account (id, value) values ('u1', 'v1')");

            SqlExecutionResult result = session.execute("select value from account where id = 'u1'");

            assertEquals("v1", result.singleRow().get("value"));
            assertNotNull(session.execute("commit").commitResult());
        }

        try (SqlSession session = engine.openSession()) {
            SqlExecutionResult result = session.execute("select value from account where id = 'u1'");

            assertEquals("v1", result.singleRow().get("value"));
        }
    }

    /**
     * 验证自动提交写入会在语句结束后提交。
     */
    @Test
    void autocommitWriteShouldBeVisibleToNextStatement() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            SqlExecutionResult write = session.execute("insert into account (id, value) values ('u2', 'v2')");
            SqlExecutionResult read = session.execute("select value from account where id = 'u2'");

            assertEquals(1, write.updateCount());
            assertNotNull(write.commitResult());
            assertEquals("v2", read.singleRow().get("value"));
        }
    }

    /**
     * 验证更新和删除都走现有事务核心。
     */
    @Test
    void updateAndDeleteShouldUseMvccCore() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('u3', 'v3')");
            session.execute("update account set value = 'v4' where id = 'u3'");

            assertEquals("v4", session.execute("select value from account where id = 'u3'").singleRow().get("value"));

            session.execute("delete from account where id = 'u3'");

            assertTrue(session.execute("select value from account where id = 'u3'").rows().isEmpty());
        }
    }

    /**
     * 验证 planner 可以被外部实现替换。
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
                IsolationLevel.READ_COMMITTED
        );

        try (SqlSession session = engine.openSession()) {
            session.execute("external syntax");

            assertEquals("v4", readCommittedValue(storage, "account", "u4"));
        }
    }

    /**
     * 验证分片解析器收到表名和主键并决定访问的分片。
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
                IsolationLevel.READ_COMMITTED
        );

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('u5', 'v5')");

            assertEquals("account:u5", routed.get());
            assertEquals("v5", session.execute("select value from account where id = 'u5'").singleRow().get("value"));
        }
    }

    /**
     * 验证没有活动事务时提交会报错。
     */
    @Test
    void commitWithoutActiveTransactionShouldFail() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            MvccException ex = assertThrows(MvccException.class, () -> session.execute("commit"));

            assertEquals(ErrorCode.ILLEGAL_STATE, ex.errorCode());
        }
    }

    /**
     * 验证 RR 显式事务不会看到事务期间的其他提交。
     */
    @Test
    void repeatableReadShouldKeepStableSnapshotAcrossStatements() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));
        try (SqlSession setup = engine.openSession()) {
            setup.execute("insert into account (id, value) values ('u6', 'v1')");
        }

        try (SqlSession rr = engine.openSession(); SqlSession writer = engine.openSession()) {
            rr.execute("begin repeatable read");
            assertEquals("v1", rr.execute("select value from account where id = 'u6'").singleRow().get("value"));

            writer.execute("update account set value = 'v2' where id = 'u6'");

            assertEquals("v1", rr.execute("select value from account where id = 'u6'").singleRow().get("value"));
            rr.execute("rollback");
        }
    }

    /**
     * 验证 RC 显式事务每条语句可以看到新的已提交版本。
     */
    @Test
    void readCommittedShouldRefreshSnapshotAcrossStatements() {
        AzirSqlEngine engine = engine(new InMemoryTestStorageAdapter("s0"));
        try (SqlSession setup = engine.openSession()) {
            setup.execute("insert into account (id, value) values ('u7', 'v1')");
        }

        try (SqlSession rc = engine.openSession(); SqlSession writer = engine.openSession()) {
            rc.execute("begin read committed");
            assertEquals("v1", rc.execute("select value from account where id = 'u7'").singleRow().get("value"));

            writer.execute("update account set value = 'v2' where id = 'u7'");

            assertEquals("v2", rc.execute("select value from account where id = 'u7'").singleRow().get("value"));
            rc.execute("rollback");
        }
    }

    /**
     * 验证自动提交遇到结果未知时不会继续回滚事务。
     */
    @Test
    void autocommitShouldNotRollbackUnknownCommitResult() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.failCommitWithStatusUnknown(true);
        AzirSqlEngine engine = engine(storage);

        try (SqlSession session = engine.openSession()) {
            MvccException ex = assertThrows(
                    MvccException.class,
                    () -> session.execute("insert into account (id, value) values ('u8', 'v1')")
            );

            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
            assertFalse(ex.retryable());
            assertEquals(0, storage.abortCount());
        }
    }

    /**
     * 验证显式提交遇到结果未知后关闭会话也不会回滚事务。
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

            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
            assertFalse(ex.retryable());
            assertFalse(session.active());
        }
        assertEquals(0, storage.abortCount());
        try (SqlSession reader = engine.openSession()) {
            assertEquals("v1", reader.execute("select value from account where id = 'u9'").singleRow().get("value"));
        }
    }

    /**
     * 验证 Calcite 可以执行投影、表达式、过滤、排序和限制行数。
     */
    @Test
    void calciteShouldExecuteProjectionFilterSortAndLimit() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            SqlExecutionResult inserted = session.execute(
                    "insert into account (id, value) values ('1', 'alpha'), ('2', 'beta'), ('3', 'gamma')"
            );
            SqlExecutionResult selected = session.execute("""
                    select id, upper(value) as normalized
                    from account
                    where value >= 'beta'
                    order by value desc
                    limit 2
                    """);

            assertEquals(3, inserted.updateCount());
            assertEquals(List.of("3", "2"), selected.rows().stream().map(row -> row.get("id")).toList());
            assertEquals(List.of("GAMMA", "BETA"), selected.rows().stream().map(row -> row.get("normalized")).toList());
        }
    }

    /**
     * 验证 Calcite JOIN、分组聚合、HAVING 和排序在 MVCC 快照行上执行。
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

            assertEquals(1, result.rows().size());
            assertEquals("gold", result.singleRow().get("category"));
            assertEquals("2", result.singleRow().get("total"));
            assertEquals(2, storage.scanReadTimestamps().size());
            assertEquals(1, new HashSet<>(storage.scanReadTimestamps()).size());
        }
    }

    /**
     * 验证 INSERT SELECT、表达式 UPDATE 和条件 DELETE 均写回 MVCC 事务核心。
     */
    @Test
    void calciteShouldExecuteInsertSelectUpdateAndDelete() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('1', 'alpha'), ('2', 'beta'), ('3', 'delta')");

            SqlExecutionResult updated = session.execute(
                    "update account set value = upper(value) where id in ('1', '2')"
            );
            SqlExecutionResult copied = session.execute("""
                    insert into archive (id, value)
                    select id, value from account where value like 'A%'
                    """);
            SqlExecutionResult deleted = session.execute("delete from account where value like 'd%'");

            assertEquals(2, updated.updateCount());
            assertEquals(1, copied.updateCount());
            assertEquals(1, deleted.updateCount());
            assertEquals("ALPHA", session.execute("select value from archive where id = '1'").singleRow().get("value"));
            assertTrue(session.execute("select * from account where id = '3'").rows().isEmpty());
        }
    }

    /**
     * 验证同一物理分片中相同主键的不同逻辑表不会互相覆盖。
     */
    @Test
    void sqlTablesShouldUseIndependentPhysicalKeyNamespaces() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('same', 'account-value')");
            session.execute("insert into profile (id, value) values ('same', 'profile-value')");

            assertEquals(
                    "account-value",
                    session.execute("select value from account where id = 'same'").singleRow().get("value")
            );
            assertEquals(
                    "profile-value",
                    session.execute("select value from profile where id = 'same'").singleRow().get("value")
            );
        }
    }

    /**
     * 验证显式事务中的复杂扫描可以读取尚未提交的本事务缓冲写。
     */
    @Test
    void calciteQueryShouldReadOwnWritesInsideExplicitTransaction() {
        AzirSqlEngine engine = calciteEngine(new InMemoryTestStorageAdapter("s0"));

        try (SqlSession session = engine.openSession()) {
            session.execute("begin repeatable read");
            session.execute("insert into account (id, value) values ('1', 'alpha'), ('2', 'beta')");

            SqlExecutionResult result = session.execute(
                    "select count(*) as total from account where value is not null"
            );

            assertEquals("2", result.singleRow().get("total"));
            session.execute("rollback");
        }

        try (SqlSession verification = engine.openSession()) {
            assertTrue(verification.execute("select * from account").rows().isEmpty());
        }
    }

    /**
     * 验证哈希路由下的复杂查询会扫描全部分片并合并排序结果。
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
                IsolationLevel.READ_COMMITTED
        );

        try (SqlSession session = engine.openSession()) {
            session.execute("insert into account (id, value) values ('a', 'one'), ('b', 'two'), ('c', 'three')");

            SqlExecutionResult result = session.execute("select id from account order by id");

            assertEquals(List.of("a", "b", "c"), result.rows().stream().map(row -> row.get("id")).toList());
            assertFalse(shard0.committedValues.isEmpty());
            assertFalse(shard1.committedValues.isEmpty());
        }
    }

    /**
     * 验证可插拔简单计划器同样保留主键唯一性，更新或删除不存在的行不会新增数据。
     */
    @Test
    void simplePlannerShouldRespectPrimaryKeyAndMissingRows() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "original", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = new AzirSqlEngine(manager, new SimpleSqlPlanner(), ShardResolver.singleShard("s0"),
                     SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED).openSession()) {
            assertThrows(MvccException.class,
                    () -> session.execute("insert into account (id, value) values ('a', 'replacement')"));
            assertEquals(0, session.execute("update account set value = 'new' where id = 'missing'").updateCount());
            assertEquals(0, session.execute("delete from account where id = 'missing'").updateCount());
            assertEquals("original", session.execute("select value from account where id = 'a'").singleRow().get("value"));
            assertTrue(session.execute("select value from account where id = 'missing'").rows().isEmpty());
        }
    }

    /**
     * 验证无排序 LIMIT 只消费首个有界页面，不调用整表扫描接口。
     */
    @Test
    void calciteLimitShouldReadOnlyOnePage() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        for (int index = 0; index < 100; index++) {
            storage.seed("account", "k" + index, "value", 1L);
        }
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 2).openSession()) {
            assertEquals(1, session.execute("select id from account limit 1").rows().size());
            assertEquals(1, storage.pageRanges.size());
            assertEquals(2, storage.scannedKeyCount);
            assertEquals(0, storage.fullScanCount);
        }
    }

    /**
     * 验证 LIMIT 不会打开尚未消费的后续分片。
     */
    @Test
    void calciteLimitShouldLeaveLaterShardsUnread() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "b", "first", 1L);
        shard1.seed("account", "a", "second", 1L);
        try (TransactionManager manager = manager(shard0, shard1);
             SqlSession session = pagedEngine(manager, ShardResolver.hashByKey(List.of("s0", "s1")), 1).openSession()) {
            assertEquals("b", session.execute("select id from account limit 1").singleRow().get("id"));
            assertEquals(1, shard0.pageRanges.size());
            assertTrue(shard1.pageRanges.isEmpty());
        }
    }

    /**
     * 验证表注册、枚举器创建和关闭均不触发额外读取，关闭后无法继续取页。
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
                assertEquals("a", enumerator.current()[0]);
                enumerator.close();
                assertFalse(enumerator.moveNext());
                assertThrows(IllegalStateException.class, enumerator::reset);
                assertEquals(1, storage.pageRanges.size());
            }
            manager.rollback(tx);
        }
    }

    /**
     * 验证关系运算重扫时重新建立分页游标，但仍读取原语句快照。
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
                assertEquals("old", enumerator.current()[0]);
                assertFalse(enumerator.moveNext());
                storage.seed("account", "a", "new", snapshot.readTs() + 1);
                enumerator.reset();
                assertEquals(1, storage.pageRanges.size());
                assertTrue(enumerator.moveNext());
                assertEquals("old", enumerator.current()[0]);
                assertEquals(1, new HashSet<>(storage.scanReadTimestamps).size());
            }
            manager.rollback(tx);
        }
    }

    /**
     * 验证等值主键下推只路由到一个分片并保留 Calcite 的非主键过滤。
     */
    @Test
    void calcitePointPredicateShouldUseOnlyRoutedGet() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "b", "other", 1L);
        shard1.seed("account", "a", "target", 1L);
        try (TransactionManager manager = manager(shard0, shard1);
             SqlSession session = pagedEngine(manager, ShardResolver.hashByKey(List.of("s0", "s1")), 1).openSession()) {
            assertEquals("target", session.execute("select value from account where id = 'a'").singleRow().get("value"));
            assertEquals(List.of(new SqlStorageKeyCodec().encode("account", "a")), shard1.pointReadKeys);
            assertTrue(shard0.pointReadKeys.isEmpty());
            assertTrue(shard0.pageRanges.isEmpty());
            assertTrue(shard1.pageRanges.isEmpty());
            assertTrue(session.execute("select id from account where id = 'a' and value = 'missing'").rows().isEmpty());
        }
    }

    /**
     * 验证墓碑造成的空可见页仍按非空原始游标继续分页。
     */
    @Test
    void calciteScanShouldContinueAfterEmptyVisiblePage() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", null, 1L);
        storage.seed("account", "b", "visible", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertEquals("b", session.execute("select id from account limit 1").singleRow().get("id"));
            assertEquals(2, storage.pageRanges.size());
            assertEquals(1, new HashSet<>(storage.scanReadTimestamps).size());
        }
    }

    /**
     * 验证 INSERT SELECT 多页读取和目标主键查重共用快照，页间新提交只在下一语句可见。
     */
    @Test
    void calciteInsertSelectShouldShareSnapshotWithDuplicateChecks() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "old", 1L);
        storage.afterFirstPage = context -> storage.seed("account", "b", "new", context.readTs() + 1);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            assertEquals(2, session.execute("insert into archive (id, value) select id, value from account").updateCount());
            assertEquals(2, storage.scanReadTimestamps.size());
            assertEquals(2, storage.pointReadTimestamps.size());
            Set<Long> allReadTimestamps = new HashSet<>(storage.scanReadTimestamps);
            allReadTimestamps.addAll(storage.pointReadTimestamps);
            assertEquals(1, allReadTimestamps.size());
            assertEquals("old", session.execute("select value from archive where id = 'b'").singleRow().get("value"));
            assertEquals("new", session.execute("select value from account where id = 'b'").singleRow().get("value"));
            session.rollback();
        }
    }

    /**
     * 验证 UPDATE 新主键存在性检查复用选行快照。
     */
    @Test
    void calciteUpdateShouldShareSnapshotWithNewKeyChecks() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            assertEquals(2, session.execute("update account set id = concat(id, 'x')").updateCount());
            assertEquals(2, storage.pointReadTimestamps.size());
            Set<Long> allReadTimestamps = new HashSet<>(storage.scanReadTimestamps);
            allReadTimestamps.addAll(storage.pointReadTimestamps);
            assertEquals(1, allReadTimestamps.size());
            assertEquals(List.of("ax", "bx"), session.execute("select id from account order by id").rows().stream()
                    .map(row -> row.get("id")).toList());
            session.rollback();
        }
    }

    /**
     * 验证范围包含边界不会误包含具有相同前缀但更长的主键。
     */
    @Test
    void calciteAsciiRangeShouldUseExactBinaryBoundary() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "equal", 1L);
        storage.seed("account", "a\u0000", "suffix", 1L);
        storage.seed("account", "ab", "longer", 1L);
        storage.seed("account", "b", "last", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertEquals("a", session.execute("select id from account where id <= 'a'").singleRow().get("id"));
            assertEquals(new SqlStorageKeyCodec().encode("account", "a") + '\u0000',
                    storage.pageRanges.getFirst().endKeyExclusive());
            storage.pageRanges.clear();
            assertEquals(List.of("a\u0000", "ab", "b"),
                    session.execute("select id from account where 'a' < id order by id").rows().stream()
                            .map(row -> row.get("id")).toList());
            assertEquals(new SqlStorageKeyCodec().encode("account", "a") + '\u0000',
                    storage.pageRanges.getFirst().startKeyInclusive());
        }
    }

    /**
     * 验证数值转换、非 ASCII 范围和 NULL 不会被误解释为二进制主键点查。
     */
    @Test
    void calciteShouldKeepUnsafePredicatesForResidualEvaluation() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "02", "leading zero", 1L);
        storage.seed("account", "2", "plain", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertEquals(2, session.execute("select id from account where cast(id as integer) = 2").rows().size());
            assertTrue(storage.pointReadKeys.isEmpty());
            storage.pageRanges.clear();
            assertEquals(2, session.execute("select id from account where id < '\u00e9'").rows().size());
            SqlStorageKeyCodec keys = new SqlStorageKeyCodec();
            assertTrue(storage.pageRanges.stream().allMatch(range -> range.startKeyInclusive().equals(keys.rangeStart("account"))
                    && range.endKeyExclusive().equals(keys.rangeEnd("account"))));
            assertTrue(session.execute("select id from account where id = cast(null as varchar)").rows().isEmpty());
            assertTrue(storage.pointReadKeys.isEmpty());
        }
    }

    /**
     * 验证排序与聚合可以消费多页，不能把页面大小误当作整个关系的行数限制。
     */
    @Test
    void calciteSortAndAggregateShouldConsumeAllPages() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "1", 1L);
        storage.seed("account", "b", "2", 1L);
        storage.seed("account", "c", "3", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertEquals("c", session.execute("select id from account order by id desc limit 1").singleRow().get("id"));
            assertEquals(3, storage.pageRanges.size());
            assertEquals("3", session.execute("select count(*) as total from account").singleRow().get("total"));
            assertEquals(6, storage.pageRanges.size());
        }
    }

    /**
     * 验证单行结果的排序、分组和残余过滤也必须遵守输入行数预算。
     */
    @Test
    void calciteBlockingOperatorsShouldRespectInputRowBudget() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "same", 1L);
        storage.seed("account", "b", "same", 1L);
        storage.seed("account", "c", "same", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 2, 1024)).openSession()) {
            for (String sql : List.of("select id from account order by id desc limit 1",
                    "select value, count(*) as total from account group by value",
                    "select count(*) as total from account where value = 'absent'")) {
                MvccException failure = assertThrows(MvccException.class, () -> session.execute(sql));
                assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
                assertTrue(failure.getMessage().contains("input scan row budget"));
            }
            assertEquals(0, storage.fullScanCount);
        }
    }

    /**
     * 验证输入预算覆盖整页预取，即使 LIMIT 只消费该页第一行也不能绕过限制。
     */
    @Test
    void calciteInputBudgetShouldChargePrefetchedRows() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(2, 1, 1024, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select id from account limit 1"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan row budget"));
            assertEquals(1, storage.pageRanges.size());
        }
    }

    /**
     * 验证跨逻辑表共享同一个输入预算，各表单独未超限也不能使总量超限。
     */
    @Test
    void calciteInputRowBudgetShouldBeSharedAcrossTables() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("profile", "b", "second", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("""
                    select count(*) as total from (
                        select id from account union all select id from profile
                    ) combined
                    """));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan row budget"));
            assertEquals(2, storage.pageRanges.stream().map(KeyRange::startKeyInclusive).distinct().count());
        }
    }

    /**
     * 验证同一逻辑表的不同物理分片也共同消耗输入预算。
     */
    @Test
    void calciteInputRowBudgetShouldBeSharedAcrossShards() {
        InMemoryTestStorageAdapter shard0 = new InMemoryTestStorageAdapter("s0");
        InMemoryTestStorageAdapter shard1 = new InMemoryTestStorageAdapter("s1");
        shard0.seed("account", "b", "first", 1L);
        shard1.seed("account", "a", "second", 1L);
        try (TransactionManager manager = manager(shard0, shard1);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.hashByKey(List.of("s0", "s1")),
                     SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED,
                     new SqlExecutionOptions(1, 1, 1024, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("select count(*) as total from account"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertEquals(1, shard0.pageRanges.size());
            assertEquals(1, shard1.pageRanges.size());
        }
    }

    /**
     * 验证投影不含 value 时仍按取回的原始值字节和完整物理键计费。
     */
    @Test
    void calciteInputByteBudgetShouldIncludeUnprojectedValue() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "中", 1L);
        long tooSmall = Utf8KeyOrder.encode(new SqlStorageKeyCodec().encode("account", "a")).length + 2L;
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 10, tooSmall)).openSession()) {
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("select count(*) as total from account"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan byte budget"));
        }
    }

    /**
     * 验证点查和另一张表的分页读取共同累计输入字节，不能各自重建计数器。
     */
    @Test
    void calciteInputByteBudgetShouldCombinePointAndPageReads() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "one", 1L);
        storage.seed("profile", "b", "two", 1L);
        SqlStorageKeyCodec keys = new SqlStorageKeyCodec();
        long tooSmall = Utf8KeyOrder.encode(keys.encode("account", "a")).length
                + (long) Utf8KeyOrder.encode(keys.encode("profile", "b")).length + 5L;
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 10, tooSmall)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("""
                    select count(*) as total from (
                        select id from account where id = 'a' union all select id from profile
                    ) combined
                    """));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("input scan byte budget"));
            assertEquals(1, storage.pointReadKeys.size());
            assertEquals(1, storage.pageRanges.size());
        }
    }

    /**
     * 验证多枚举器和 reset 重扫累计计费，任何枚举器超限后其他枚举器也不能继续输出。
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
                assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
                assertThrows(MvccException.class, second::moveNext);
            }
            manager.rollback(tx);
        }
    }

    /**
     * 验证新语句取得新的输入预算，返回前的快照复核不会额外分配 RC 时间戳。
     */
    @Test
    void calciteStatementShouldRenewBudgetWithoutExtraReadTimestamp() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "value", 1L);
        AtomicLong sequence = new AtomicLong(1L);
        try (TransactionManager manager = new TransactionManager(sequence::incrementAndGet,
                new InMemoryTransactionMetadataStore(), TransactionOptions.defaults(), Map.of("s0", storage));
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024, 1, 1024)).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            for (int index = 0; index < 2; index++) {
                long before = sequence.get();
                assertEquals("1", session.execute("select count(*) as total from account").singleRow().get("total"));
                assertEquals(before + 1, sequence.get());
            }
            session.rollback();
        }
    }

    /**
     * 验证最后一页读取之后在解码或关系计算阶段租约到期，显式事务不能返回过期结果。
     */
    @Test
    void calciteShouldValidateLeaseAfterFinalPageProcessing() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "value", 1L);
        AtomicLong now = new AtomicLong(1_000L);
        SqlValueCodec expiringCodec = new SqlValueCodec() {
            /**
             * 保留默认 UTF-8 编码行为。
             */
            @Override
            public byte[] encode(String value) {
                return SqlValueCodec.utf8().encode(value);
            }

            /**
             * 在存储已经返回页面后推进测试时钟，不执行等待或真实耗时工作。
             */
            @Override
            public String decode(byte[] value) {
                now.addAndGet(TransactionOptions.defaults().limits().timeoutMillis() + 1);
                return SqlValueCodec.utf8().decode(value);
            }
        };
        try (TransactionManager manager = new TransactionManager(new AtomicTimestampOracle(1L), metadataWithClock(now),
                TransactionOptions.defaults(), Map.of("s0", storage));
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), expiringCodec,
                     IsolationLevel.READ_COMMITTED).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("select value from account order by value desc limit 1"));
            assertEquals(ErrorCode.TRANSACTION_TIMEOUT, failure.errorCode());
            assertEquals(1, storage.pageRanges.size());
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            session.rollback();
        }
    }

    /**
     * 验证结果行数超限会报错而不是返回被截断的成功结果。
     */
    @Test
    void calciteResultRowBudgetShouldFailExplicitly() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select id from account"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("row budget"));
        }
    }

    /**
     * 验证 DML 源结果超限时不会应用已收集的前缀行。
     */
    @Test
    void calciteInsertSelectBudgetFailureShouldNotWritePrefix() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 1, 1024)).openSession()) {
            MvccException failure = assertThrows(MvccException.class,
                    () -> session.execute("insert into archive (id, value) select id, value from account"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(session.execute("select * from archive").rows().isEmpty());
        }
    }

    /**
     * 验证字节预算按 UTF-8 编码而非字符数计费，异常后显式事务无法继续提交。
     */
    @Test
    void calciteResultByteBudgetShouldCountUtf8AndInvalidateTransaction() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "中", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = AzirSqlEngine.calcite(manager, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED, new SqlExecutionOptions(1, 10, 2)).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select value from account"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(failure.getMessage().contains("byte budget"));
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            session.rollback();
            assertEquals("a", session.execute("select id from account").singleRow().get("id"));
        }
    }

    /**
     * 验证第二行预写失败会回滚整个显式事务，包括先前成功语句的写入，并阻止隐式自动提交。
     */
    @Test
    void calciteDmlFailureShouldRollbackWholeExplicitTransaction() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (TransactionManager manager = new TransactionManager(new AtomicTimestampOracle(1L),
                new InMemoryTransactionMetadataStore(), TransactionOptions.failFastDefaults(), Map.of("s0", storage));
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            session.begin(IsolationLevel.READ_COMMITTED);
            session.execute("insert into account (id, value) values ('prior', 'saved')");
            storage.failPrewriteKey = new SqlStorageKeyCodec().encode("account", "b");
            MvccException failure = assertThrows(MvccException.class, () -> session.execute(
                    "insert into account (id, value) values ('a', 'one'), ('b', 'two')"));
            assertEquals(ErrorCode.STORAGE_UNAVAILABLE, failure.errorCode());
            assertTrue(failure.getMessage().contains("entire explicit transaction rolled back"));
            assertEquals(1, storage.abortCount());
            assertTrue(storage.intentsByTx.isEmpty());
            assertFalse(session.active());
            assertThrows(MvccException.class, session::commit);
            assertThrows(MvccException.class, () -> session.execute("select * from account"));
            session.rollback();
            assertTrue(session.execute("select * from account").rows().isEmpty());
        }
    }

    /**
     * 验证一条 INSERT 内重复主键不会留下部分写入。
     */
    @Test
    void calciteInsertShouldRejectDuplicateKeysWithinStatement() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertThrows(MvccException.class, () -> session.execute(
                    "insert into account (id, value) values ('a', 'first'), ('a', 'second')"));
            assertTrue(session.execute("select * from account").rows().isEmpty());
        }
    }

    /**
     * 验证 UPDATE 交换两个主键时不覆盖或删除其中一行。
     */
    @Test
    void calciteUpdateShouldPreserveBothRowsWhenKeysSwap() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        storage.seed("account", "a", "first", 1L);
        storage.seed("account", "b", "second", 1L);
        try (TransactionManager manager = manager(storage);
             SqlSession session = pagedEngine(manager, ShardResolver.singleShard("s0"), 1).openSession()) {
            assertEquals(2, session.execute("update account set id = case when id = 'a' then 'b' else 'a' end").updateCount());
            assertEquals(List.of("second", "first"), session.execute("select value from account order by id").rows().stream()
                    .map(row -> row.get("value")).toList());
        }
    }

    /**
     * 验证测试适配器声明的能力真实可用：提交后尚未物化的意向按 commitTs 可见，墓碑仍遮蔽旧值。
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
     * 验证关闭后不会调用计划器或创建事务，所有事务操作均被拒绝且重复关闭无副作用。
     */
    @Test
    void closedSessionShouldRejectFurtherUseBeforePlanning() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        AtomicReference<Boolean> planned = new AtomicReference<>(false);
        SqlPlanner planner = sql -> {
            planned.set(true);
            return new SqlPlan.SelectByKeyPlan("account", "a");
        };
        try (TransactionManager manager = manager(storage);
             SqlSession session = new AzirSqlEngine(manager, planner, ShardResolver.singleShard("s0"), SqlValueCodec.utf8(),
                     IsolationLevel.READ_COMMITTED).openSession()) {
            session.close();
            assertFalse(session.active());
            assertThrows(MvccException.class, () -> session.begin(IsolationLevel.READ_COMMITTED));
            MvccException failure = assertThrows(MvccException.class, () -> session.execute("select anything"));
            assertEquals(ErrorCode.ILLEGAL_STATE, failure.errorCode());
            assertTrue(failure.getMessage().contains("session is closed"));
            assertFalse(planned.get());
            assertThrows(MvccException.class, session::commit);
            assertThrows(MvccException.class, session::rollback);
            session.close();
            assertEquals(0, storage.abortCount());
        }
    }

    /**
     * 验证关闭活动会话只清理一次整个事务，且之前的写入不可见。
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
                assertEquals(1, storage.abortCount());
                assertThrows(MvccException.class, () -> session.begin(IsolationLevel.READ_COMMITTED));
            }
            try (SqlSession reader = engine.openSession()) {
                assertTrue(reader.execute("select * from account").rows().isEmpty());
            }
        }
    }

    /**
     * 验证清理事务失败也不会重新开放已关闭的会话，重复关闭不会重复发送失败的清理请求。
     */
    @Test
    void rollbackFailureDuringCloseShouldKeepSessionClosed() {
        InMemoryTestStorageAdapter storage = new InMemoryTestStorageAdapter("s0");
        try (TransactionManager manager = manager(storage);
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
                assertEquals(1, storage.abortCount());
            } finally {
                storage.failAbort = false;
            }
        }
    }

    /**
     * 仅替换元数据接口时钟，其余操作委托真实内存实现，以确定性覆盖 SQL 返回前租约检查。
     */
    private TransactionMetadataStore metadataWithClock(AtomicLong now) {
        TransactionMetadataStore delegate = new InMemoryTransactionMetadataStore();
        return (TransactionMetadataStore) Proxy.newProxyInstance(TransactionMetadataStore.class.getClassLoader(),
                new Class<?>[]{TransactionMetadataStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("currentTimeMillis")) {
                        return now.get();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /**
     * 创建指定页大小并保留默认结果预算的测试引擎。
     */
    private AzirSqlEngine pagedEngine(TransactionManager manager, ShardResolver resolver, int pageSize) {
        SqlExecutionOptions defaults = SqlExecutionOptions.defaults();
        return AzirSqlEngine.calcite(manager, resolver, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED,
                new SqlExecutionOptions(pageSize, defaults.maxResultRows(), defaults.maxResultBytes()));
    }

    /**
     * 创建默认测试 SQL 引擎。
     */
    private AzirSqlEngine engine(InMemoryTestStorageAdapter storage) {
        return new AzirSqlEngine(
                manager(storage),
                new SimpleSqlPlanner(),
                ShardResolver.singleShard(storage.shardId()),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED
        );
    }

    /**
     * 创建使用 Calcite 关系计划器的测试 SQL 引擎。
     */
    private AzirSqlEngine calciteEngine(InMemoryTestStorageAdapter storage) {
        return AzirSqlEngine.calcite(
                manager(storage),
                ShardResolver.singleShard(storage.shardId()),
                SqlValueCodec.utf8(),
                IsolationLevel.READ_COMMITTED
        );
    }

    /**
     * 创建默认测试事务管理器。
     */
    private TransactionManager manager(InMemoryTestStorageAdapter storage) {
        return manager(new InMemoryTestStorageAdapter[]{storage});
    }

    /**
     * 创建包含多个测试分片的事务管理器。
     */
    private TransactionManager manager(InMemoryTestStorageAdapter... storages) {
        return new TransactionManager(
                new AtomicTimestampOracle(1L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                java.util.Arrays.stream(storages).collect(java.util.stream.Collectors.toMap(
                        InMemoryTestStorageAdapter::shardId,
                        storage -> storage
                ))
        );
    }

    /**
     * 读取测试存储中的最新已提交字符串。
     */
    private String readCommittedValue(InMemoryTestStorageAdapter storage, String table, String key) {
        String physicalKey = new SqlStorageKeyCodec().encode(table, key);
        return storage.get(new ReadContext(-1L, Long.MAX_VALUE, IsolationLevel.READ_COMMITTED), physicalKey)
                .map(VersionedValue::value)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElseThrow();
    }

    /**
     * 仅用于 SQL 会话测试的内存存储适配器。
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
         * 构造内存测试存储适配器。
         */
        private InMemoryTestStorageAdapter(String shardId) {
            this.shardId = shardId;
        }

        /**
         * 设置参与者物化时是否模拟失败，由事务管理器包装为不可重试的提交未知异常。
         */
        private void failCommitWithStatusUnknown(boolean failCommitWithStatusUnknown) {
            this.failCommitWithStatusUnknown = failCommitWithStatusUnknown;
        }

        /**
         * 绑定权威元数据，读路径只将已经提交且不晚于快照的其他事务意向视为版本。
         */
        @Override
        public void configureMetadataStore(TransactionMetadataStore store) {
            metadataStore = store;
        }

        /**
         * 本适配器的点读和扫描均实现已提交意向解析，可参与跨分片快照测试。
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return true;
        }

        /**
         * 直接建立指定提交时间的行，避免延迟扫描测试的准备阶段产生额外读取。
         */
        private void seed(String table, String id, String value, long commitTs) {
            String key = new SqlStorageKeyCodec().encode(table, id);
            committedValues.computeIfAbsent(key, ignored -> new TreeMap<>()).put(commitTs,
                    new VersionedValue(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8),
                            value == null, commitTs, false));
        }

        /**
         * 返回测试存储收到的回滚次数。
         */
        private int abortCount() {
            return abortCount;
        }

        /**
         * 清空扫描读取时间戳记录。
         */
        private void clearScanReadTimestamps() {
            scanReadTimestamps.clear();
        }

        /**
         * 返回扫描读取时间戳记录。
         */
        private List<Long> scanReadTimestamps() {
            return List.copyOf(scanReadTimestamps);
        }

        /**
         * 返回当前测试分片编号。
         */
        @Override
        public String shardId() {
            return shardId;
        }

        /**
         * 初始化测试存储。
         */
        @Override
        public void initialize() {
        }

        /**
         * 按 MVCC 快照读取可见版本。
         */
        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) {
            pointReadTimestamps.add(context.readTs());
            pointReadKeys.add(key);
            return visibleValue(context, key);
        }

        /**
         * 合并自身意向、可见已提交版本与中心已提交的未物化意向，保留最新墓碑遮蔽语义。
         */
        private Optional<VersionedValue> visibleValue(ReadContext context, String key) {
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
         * 按键范围扫描可见版本。
         */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            fullScanCount++;
            scanReadTimestamps.add(context.readTs());
            List<VersionedValue> result = new ArrayList<>();
            for (String key : keysInRange(keyRange)) {
                visibleValue(context, key).filter(value -> !value.deleted()).ifPresent(result::add);
            }
            return result;
        }

        /**
         * 按原始键分页并记录读取量，墓碑或不可见意向可产生带继续游标的空页。
         */
        @Override
        public ScanPage scanPage(ReadContext context, KeyRange keyRange, String afterKeyExclusive, int limit) {
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
         * 以 UTF-8 键序列举版本和意向共有的物理键空间。
         */
        private Set<String> keysInRange(KeyRange range) {
            Set<String> keys = new TreeSet<>(Utf8KeyOrder::compare);
            keys.addAll(committedValues.keySet());
            intentsByTx.values().forEach(intents -> keys.addAll(intents.keySet()));
            keys.removeIf(key -> Utf8KeyOrder.compare(key, range.startKeyInclusive()) < 0
                    || Utf8KeyOrder.compare(key, range.endKeyExclusive()) >= 0);
            return keys;
        }

        /**
         * 写入测试 intent 并检测简单写冲突。
         */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
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
         * 准备测试 intent。
         */
        @Override
        public void prepare(long txId) {
        }

        /**
         * 提交测试 intent。
         */
        @Override
        public void commit(long txId, long commitTs) {
            commit(txId, commitTs, List.of(), true);
        }

        /**
         * 提交测试 intent 或直接使用传入变更生成版本。
         */
        @Override
        public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
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
         * 直接生成已提交测试版本。
         */
        @Override
        public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            for (Mutation mutation : mutations) {
                detectConflict(txId, startTs, mutation.key());
            }
            commit(txId, commitTs, mutations, true);
        }

        /**
         * 清理指定事务的测试 intent。
         */
        @Override
        public void cleanupIntents(long txId, List<Mutation> mutations) {
            intentsByTx.remove(txId);
        }

        /**
         * 回滚测试事务。
         */
        @Override
        public void abort(long txId) {
            abortCount++;
            if (failAbort) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected abort failure", false, txId);
            }
            intentsByTx.remove(txId);
        }

        /**
         * 查找仍有测试 intent 的事务。
         */
        @Override
        public Set<Long> findPendingTransactionIds(int limit) {
            return new HashSet<>(intentsByTx.keySet());
        }

        /**
         * 测试存储不执行 GC。
         */
        @Override
        public int gc(long safePoint, int limit) {
            return 0;
        }

        /**
         * 检测写写冲突。
         */
        private void detectConflict(long txId, long startTs, String key) {
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
         * 把变更转换成测试 intent 值。
         */
        private VersionedValue toIntentValue(Mutation mutation, long readTs) {
            return new VersionedValue(
                    mutation.key(),
                    mutation.value(),
                    mutation.type() == MutationType.DELETE,
                    readTs,
                    true
            );
        }

        /**
         * 把变更转换成已提交测试版本。
         */
        private VersionedValue toCommittedValue(Mutation mutation, long commitTs) {
            return new VersionedValue(
                    mutation.key(),
                    mutation.value(),
                    mutation.type() == MutationType.DELETE,
                    commitTs,
                    false
            );
        }
    }
}

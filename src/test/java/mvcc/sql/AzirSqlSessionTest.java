package mvcc.sql;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MutationType;
import mvcc.common.MvccException;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.tso.AtomicTimestampOracle;
import mvcc.txn.TransactionManager;
import mvcc.txn.TransactionOptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

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
        storage.failDirectCommitWithStatusUnknown(true);
        AzirSqlEngine engine = engine(storage);

        try (SqlSession session = engine.openSession()) {
            MvccException ex = assertThrows(
                    MvccException.class,
                    () -> session.execute("insert into account (id, value) values ('u8', 'v1')")
            );

            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
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
            storage.failDirectCommitWithStatusUnknown(true);

            MvccException ex = assertThrows(MvccException.class, () -> session.execute("commit"));

            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, ex.errorCode());
            assertFalse(session.active());
        }
        assertEquals(0, storage.abortCount());
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
        private boolean failDirectCommitWithStatusUnknown;
        private int abortCount;

        /**
         * 构造内存测试存储适配器。
         */
        private InMemoryTestStorageAdapter(String shardId) {
            this.shardId = shardId;
        }

        /**
         * 设置直接提交时是否模拟结果未知异常。
         */
        private void failDirectCommitWithStatusUnknown(boolean failDirectCommitWithStatusUnknown) {
            this.failDirectCommitWithStatusUnknown = failDirectCommitWithStatusUnknown;
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
            Mutation ownIntent = intentsByTx.getOrDefault(context.txId(), Map.of()).get(key);
            if (ownIntent != null) {
                return Optional.of(toIntentValue(ownIntent, context.readTs()));
            }
            NavigableMap<Long, VersionedValue> versions = committedValues.get(key);
            if (versions == null) {
                return Optional.empty();
            }
            Map.Entry<Long, VersionedValue> visible = versions.floorEntry(context.readTs());
            return visible == null ? Optional.empty() : Optional.of(visible.getValue());
        }

        /**
         * 按键范围扫描可见版本。
         */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            scanReadTimestamps.add(context.readTs());
            List<VersionedValue> result = new ArrayList<>();
            for (String key : committedValues.keySet()) {
                if (key.compareTo(keyRange.startKeyInclusive()) >= 0 && key.compareTo(keyRange.endKeyExclusive()) < 0) {
                    get(context, key).filter(value -> !value.deleted()).ifPresent(result::add);
                }
            }
            result.sort(Comparator.comparing(VersionedValue::key));
            return result;
        }

        /**
         * 写入测试 intent 并检测简单写冲突。
         */
        @Override
        public void prewrite(long txId, long startTs, List<Mutation> mutations) {
            for (Mutation mutation : mutations) {
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
            if (failDirectCommitWithStatusUnknown) {
                throw new MvccException(
                        ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "unknown direct commit result",
                        true,
                        txId
                );
            }
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

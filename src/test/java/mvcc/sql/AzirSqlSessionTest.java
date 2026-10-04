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

            assertEquals("v4", readCommittedValue(storage, "u4"));
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
     * 创建默认测试事务管理器。
     */
    private TransactionManager manager(InMemoryTestStorageAdapter storage) {
        return new TransactionManager(
                new AtomicTimestampOracle(1L),
                new InMemoryTransactionMetadataStore(),
                TransactionOptions.defaults(),
                Map.of(storage.shardId(), storage)
        );
    }

    /**
     * 读取测试存储中的最新已提交字符串。
     */
    private String readCommittedValue(InMemoryTestStorageAdapter storage, String key) {
        return storage.get(new ReadContext(-1L, Long.MAX_VALUE, IsolationLevel.READ_COMMITTED), key)
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

package mvcc.e2e;

import mvcc.api.Transaction;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.gc.GcManager;
import mvcc.gc.SafePointStore;
import mvcc.gc.mysql.MysqlSafePointStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.recovery.BackgroundRecoveryRunner;
import mvcc.recovery.RecoveryManager;
import mvcc.storage.KeyRange;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.mysql.MysqlTimestampOracle;
import mvcc.txn.TransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * MySQL MVCC 端到端验证入口。
 */
public final class MysqlMvccE2E {
    /**
     * 构造 MysqlMvccE2E 实例。
     */
    private MysqlMvccE2E() {
    }

    /**
     * 程序入口方法。
     */
    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());

        try {
            MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
            MysqlTransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
            MysqlSafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
            MysqlStorageAdapter shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
            MysqlStorageAdapter shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);

            shard1.initialize();
            shard2.initialize();
            tso.initialize(100);
            metadataStore.initialize();
            safePointStore.initialize(0L);

            if (config.resetTables()) {
                reset(controlDataSource, "mvcc_txn", "mvcc_tso", "mvcc_safe_point");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
                reset(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback");
                tso.initialize(100);
                metadataStore.initialize();
                safePointStore.initialize(0L);
            }

            TransactionManager transactionManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2
            ));
            RecoveryManager recoveryManager = new RecoveryManager(metadataStore, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2
            ));
            BackgroundRecoveryRunner recoveryRunner = new BackgroundRecoveryRunner(recoveryManager, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2
            ));
            GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2
            ));

            scenarioRcRr(transactionManager);
            scenarioScan(transactionManager);
            scenarioCrossShard(transactionManager);
            scenarioRecovery(tso, metadataStore, safePointStore, shard1, shard2, recoveryRunner);
            scenarioGc(transactionManager, gcManager, safePointStore, shard1DataSource);

            System.out.println("E2E PASS");
            System.out.println("safe_point=" + safePointStore.currentSafePoint());
            System.out.println("remaining_versions_for_e2e_gc_001=" + countVersionsForKey(shard1DataSource, "e2e:gc:001"));
        } finally {
            close(controlDataSource);
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

    /**
     * 执行读已提交和可重复读隔离级别验证场景。
     */
    private static void scenarioRcRr(TransactionManager transactionManager) {
        seed(transactionManager, "shard-1", "e2e:acct:1", "v1");

        Transaction rrReader = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        requireEquals("v1", read(transactionManager, rrReader, "shard-1", "e2e:acct:1"), "RR initial read");

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "e2e:acct:1", bytes("v2"));
        transactionManager.commit(writer);

        requireEquals("v1", read(transactionManager, rrReader, "shard-1", "e2e:acct:1"), "RR should keep snapshot");
        transactionManager.rollback(rrReader);

        Transaction rcReader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        requireEquals("v2", read(transactionManager, rcReader, "shard-1", "e2e:acct:1"), "RC should see latest committed");
        transactionManager.rollback(rcReader);

        System.out.println("[PASS] RC/RR");
    }

    /**
     * 执行范围扫描可见性验证场景。
     */
    private static void scenarioScan(TransactionManager transactionManager) {
        seed(transactionManager, "shard-1", "e2e:scan:001", "a");
        seed(transactionManager, "shard-1", "e2e:scan:002", "b");

        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, "shard-1", "e2e:scan:003", bytes("c"));
        List<String> keys = transactionManager.scan(tx, "shard-1", "e2e:scan:000", "e2e:scan:999")
                .stream()
                .map(VersionedValue::key)
                .collect(Collectors.toList());
        requireEquals(List.of("e2e:scan:001", "e2e:scan:002", "e2e:scan:003"), keys, "scan should include own intent");
        transactionManager.rollback(tx);

        System.out.println("[PASS] scan(range)");
    }

    /**
     * 执行跨分片提交验证场景。
     */
    private static void scenarioCrossShard(TransactionManager transactionManager) {
        Transaction tx = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx, "shard-1", "e2e:order:1", bytes("created"));
        transactionManager.put(tx, "shard-2", "e2e:stock:1", bytes("reserved"));
        transactionManager.commit(tx);

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        requireEquals("created", read(transactionManager, reader, "shard-1", "e2e:order:1"), "cross-shard shard-1");
        requireEquals("reserved", read(transactionManager, reader, "shard-2", "e2e:stock:1"), "cross-shard shard-2");
        transactionManager.rollback(reader);

        System.out.println("[PASS] cross-shard 2PC");
    }

    /**
     * 执行事务恢复验证场景。
     */
    private static void scenarioRecovery(MysqlTimestampOracle tso,
                                         TransactionMetadataStore metadataStore,
                                         SafePointStore safePointStore,
                                         StorageAdapter shard1,
                                         StorageAdapter shard2,
                                         BackgroundRecoveryRunner recoveryRunner) {
        TransactionManager flakyManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)
        ));
        TransactionManager readerManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2
        ));

        Transaction tx = flakyManager.begin(IsolationLevel.REPEATABLE_READ);
        flakyManager.put(tx, "shard-1", "e2e:recover:1", bytes("left"));
        flakyManager.put(tx, "shard-2", "e2e:recover:2", bytes("right"));
        try {
            flakyManager.commit(tx);
            throw new IllegalStateException("expected commit failure");
        } catch (MvccException expected) {
            recoveryRunner.recoverOnce(100);
        }

        Transaction reader = readerManager.begin(IsolationLevel.READ_COMMITTED);
        requireEquals("right", read(readerManager, reader, "shard-2", "e2e:recover:2"), "recovery should finish commit");
        readerManager.rollback(reader);

        System.out.println("[PASS] recovery");
    }

    /**
     * 执行历史版本 GC 验证场景。
     */
    private static void scenarioGc(TransactionManager transactionManager,
                                   GcManager gcManager,
                                   SafePointStore safePointStore,
                                   DataSource shard1DataSource) throws Exception {
        Transaction seed1 = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(seed1, "shard-1", "e2e:gc:001", bytes("alive"));
        transactionManager.commit(seed1);

        Transaction deleteTx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.delete(deleteTx, "shard-1", "e2e:gc:001");
        transactionManager.commit(deleteTx);

        int reclaimed = gcManager.gcOnce(100);
        if (reclaimed < 2) {
            throw new IllegalStateException("expected tombstone chain reclaimed, reclaimed=" + reclaimed);
        }
        if (countVersionsForKey(shard1DataSource, "e2e:gc:001") != 0) {
            throw new IllegalStateException("expected tombstone chain fully reclaimed");
        }
        if (safePointStore.currentSafePoint() <= 0) {
            throw new IllegalStateException("expected safe point to advance");
        }

        System.out.println("[PASS] safe point + GC + tombstone cleanup");
    }

    /**
     * 写入端到端场景的初始数据。
     */
    private static void seed(TransactionManager transactionManager, String shardId, String key, String value) {
        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, shardId, key, bytes(value));
        transactionManager.commit(tx);
    }

    /**
     * 读取端到端场景中的指定键值。
     */
    private static String read(TransactionManager transactionManager, Transaction tx, String shardId, String key) {
        return transactionManager.get(tx, shardId, key).map(String::new).orElse(null);
    }

    /**
     * 将字符串转换为字节数组。
     */
    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 断言实际值与期望值一致。
     */
    private static void requireEquals(Object expected, Object actual, String message) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new IllegalStateException(message + ", expected=" + expected + ", actual=" + actual);
        }
    }

    /**
     * 重置基准测试表数据。
     */
    private static void reset(DataSource dataSource, String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    /**
     * 统计指定键的历史版本数量。
     */
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

    /**
     * 关闭资源或后台执行器。
     */
    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * 基准测试配置对象。
     */
    private record Config(String controlJdbcUrl,
                          String shard1JdbcUrl,
                          String shard2JdbcUrl,
                          String username,
                          String password,
                          boolean resetTables) {
        /**
         * 从环境变量读取配置对象。
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control"),
                    env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1"),
                    env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true"))
            );
        }
    }

    /**
     * 读取字符串环境变量配置。
     */
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /**
     * 提交时故障注入的存储适配器包装器。
     */
    private static final class FailOnceOnCommitStorageAdapter implements StorageAdapter {
        private final StorageAdapter delegate;
        private final AtomicBoolean shouldFail = new AtomicBoolean(true);

        /**
         * 构造 FailOnceOnCommitStorageAdapter 实例。
         */
        private FailOnceOnCommitStorageAdapter(StorageAdapter delegate) {
            this.delegate = delegate;
        }

        /**
         * 返回当前存储适配器所属分片编号。
         */
        @Override
        public String shardId() {
            return delegate.shardId();
        }

        /**
         * 初始化所需的数据库表结构或运行环境。
         */
        @Override
        public void initialize() {
            delegate.initialize();
        }

        /**
         * 读取指定键或事务的当前可见数据。
         */
        @Override
        public Optional<VersionedValue> get(ReadContext context, String key) {
            return delegate.get(context, key);
        }

        /**
         * 按键范围扫描当前快照可见的数据。
         */
        @Override
        public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
            return delegate.scan(context, keyRange);
        }

        /**
         * 执行原型事务预写。
         */
        @Override
        public void prewrite(long txId, long startTs, List<mvcc.storage.Mutation> mutations) {
            delegate.prewrite(txId, startTs, mutations);
        }

        /**
         * 检查事务意向是否存在并进入准备阶段。
         */
        @Override
        public void prepare(long txId) {
            delegate.prepare(txId);
        }

        /**
         * 提交事务意向并生成已提交版本。
         */
        @Override
        public void commit(long txId, long commitTs) {
            if (shouldFail.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated commit failure");
            }
            delegate.commit(txId, commitTs);
        }

        /**
         * 回滚事务并清理写意向。
         */
        @Override
        public void abort(long txId) {
            delegate.abort(txId);
        }

        /**
         * 查找仍有待处理意向的事务编号。
         */
        @Override
        public Set<Long> findPendingTransactionIds(int limit) {
            return delegate.findPendingTransactionIds(limit);
        }

        /**
         * 清理安全点之前的历史版本。
         */
        @Override
        public int gc(long safePoint, int limit) {
            return delegate.gc(safePoint, limit);
        }
    }
}

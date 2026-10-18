package mvcc.sql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.percolator.MysqlPercolatorStore;
import mvcc.percolator.PercolatorKey;
import mvcc.percolator.PercolatorRuntime;
import mvcc.percolator.PercolatorStore;
import mvcc.percolator.PercolatorTransactionStatusUnknownException;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.tso.mysql.MysqlTimestampOracle;
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
import static mvcc.common.IsolationLevel.*;
import static mvcc.percolator.PercolatorStore.*;
import static org.junit.jupiter.api.Assertions.*;

/** 在两个独立 MySQL 实例验证 Percolator SQL 装配、分页隔离、错误终态和恢复。 */
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

    /** 使用公开装配入口初始化协议数据空间和持久 TSO。 */
    @BeforeAll
    static void open() {
        poolA = MysqlDataSourceFactory.create(MYSQL_A.getJdbcUrl(), MYSQL_A.getUsername(), MYSQL_A.getPassword());
        poolB = MysqlDataSourceFactory.create(MYSQL_B.getJdbcUrl(), MYSQL_B.getUsername(), MYSQL_B.getPassword());
        try (var initialized = PercolatorSqlEngine.mysql(poolA, Map.of("a", poolA, "b", poolB), RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED)) { }

    }

    /** 所有会话关闭后由测试拥有者关闭连接池。 */
    @AfterAll
    static void close() throws Exception {
        if (poolA instanceof AutoCloseable closeable) { closeable.close(); }
        if (poolB instanceof AutoCloseable closeable) { closeable.close(); }
    }

    /** 每项测试清空独立协议表但保留单调 TSO，默认每页一键覆盖真实跨页查询。 */
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

    /** 相同 SqlEngine/SqlSession 接口可显式选择新模型，并返回原 primary 提交身份。 */
    @Test
    void mysqlFactorySupportsExistingSqlApiAndDurableCommitIdentity() throws Exception {
        try (var selected = PercolatorSqlEngine.mysql(poolA, Map.of("a", poolA, "b", poolB),
                RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED);
             SqlSession session = selected.openSession()) {
            var result = session.execute("insert into account (id, value) values ('1', 'one'), ('2', 'two')");
            assertEquals(2, result.updateCount());
            assertNull(result.commitResult());
            var commit = result.percolatorCommitResult();
            assertNotNull(commit);
            assertNotNull(commit.handle());
            assertTrue(commit.commitTs() > commit.handle().startTs());
            assertTrue(commit.pendingSecondaries().isEmpty());
            assertEquals(State.COMMITTED, engine.runtime().lookup(commit.handle()).state());
            assertEquals("one", session.execute("select value from account where id='1'").singleRow().get("value"));
            assertEquals(0, session.execute("select value from account where id='1'").percolatorCommitResult().commitTs());
        }
        // 新工厂只初始化自己的协议空间与共用 TSO，不创建旧模型逐笔事务元数据。
        try (var connection = poolA.getConnection(); var query = connection.prepareStatement("""
                select count(*) from information_schema.tables
                where table_schema=database() and table_name='mvcc_txn'
                """); var rows = query.executeQuery()) {
            rows.next();
            assertEquals(0, rows.getInt(1));
        }
        assertThrows(IllegalArgumentException.class, () -> PercolatorSqlEngine.mysql(poolA,
                Map.of("a", poolA, "alias", poolA), RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED));
    }

    /** 显式 SQL 事务跨片读己之写，回滚不会泄漏到另一个自动提交会话。 */
    @Test
    void explicitTransactionReadsOwnWritesAndRollsBackEveryShard() {
        try (var writer = engine.openSession(); var reader = engine.openSession()) {
            writer.execute("begin repeatable read");
            writer.execute("insert into account (id,value) values ('1','one'),('2','two')");
            assertEquals(List.of("one", "two"), values(writer.execute("select value from account order by id")));
            assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
            writer.execute("rollback");
            assertFalse(writer.active());
            assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
            writer.begin(REPEATABLE_READ);
            writer.execute("insert into account (id,value) values ('1','saved'),('2','saved')");
            assertNotNull(writer.commit().percolatorCommitResult());
            assertEquals("2", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /** RC 在 SQL 语句之间取新边界，RR 在跨页扫描、删除和重读期间保持旧快照。 */
    @Test
    void sqlRcAndRrPreserveTheirSnapshotBoundaries() {
        insert("insert into account (id,value) values ('1','old'),('2','old'),('3','old')");
        try (var rr = engine.openSession(); var rc = engine.openSession(); var writer = engine.openSession()) {
            rr.begin(REPEATABLE_READ);
            rc.begin(READ_COMMITTED);
            assertEquals(List.of("old", "old", "old"), values(rr.execute("select value from account order by id")));
            assertEquals("old", rc.execute("select value from account where id='2'").singleRow().get("value"));
            writer.execute("update account set value='new' where id='2'");
            writer.execute("delete from account where id='1'");
            writer.execute("insert into account (id,value) values ('4','new')");
            assertEquals(List.of("old", "old", "old"), values(rr.execute("select value from account order by id")));
            assertEquals(List.of("new", "old", "new"), values(rc.execute("select value from account order by id")));
            rr.rollback();
            rc.rollback();
        }
    }

    /** 同一 RC SQL 首次取页后发生跨片提交，后续页和另一分片仍使用原 readTs。 */
    @Test
    void oneRcSqlUsesOneTimestampAcrossPagesAndShards() {
        insert("insert into account (id,value) values ('1','old'),('2','old'),('3','old')");
        var first = new ProbedStore(a);
        var allocations = new AtomicInteger();
        var runtime = new PercolatorRuntime(() -> { allocations.incrementAndGet(); return tso.nextTimestamp(); },
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
            assertEquals(List.of("old", "old", "old"), values(session.execute("select value from account order by id")));
            assertEquals(before + 1, allocations.get(), "one RC read boundary for the entire SQL");
            assertEquals(List.of("new", "new", "old", "new"), values(session.execute("select value from account order by id")));
            session.rollback();
        }
    }

    /** Calcite 的条件 DML、JOIN、聚合和 INSERT SELECT 都经新后端实际读写。 */
    @Test
    void relationalSqlAndTableNamespacesUsePercolator() {
        insert("insert into account (id,value) values ('1','gold'),('2','gold'),('3','silver')");
        insert("insert into tag (id,value) values ('1','yes'),('2','yes')");
        try (var session = engine.openSession()) {
            var grouped = session.execute("select a.value as label,count(*) as n from account a join tag t on a.id=t.id group by a.value");
            assertEquals("gold", grouped.singleRow().get("label"));
            assertEquals("2", grouped.singleRow().get("n"));
            assertEquals(2, session.execute("update account set value=upper(value) where id in ('1','2')").updateCount());
            assertEquals(3, session.execute("insert into archive(id,value) select id,value from account").updateCount());
            assertEquals(1, session.execute("delete from account where value='silver'").updateCount());
            assertEquals(List.of("GOLD", "GOLD"), values(session.execute("select value from account order by id")));
            assertEquals("3", session.execute("select count(*) as n from archive").singleRow().get("n"));
            assertEquals(List.of("yes", "yes"), values(session.execute("select value from tag order by id")));
        }
    }

    /** 一条失败 SQL 必须撤销此前显式事务的写入，ROLLBACK 前不能隐含转为自动提交。 */
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
            assertEquals(List.of("saved"), values(session.execute("select value from account order by id")));
            session.execute("insert into account(id,value) values ('2','reused')");
        }
    }

    /** 解析失败同样不能遗留此前已经缓冲的显式事务写入。 */
    @Test
    void invalidSyntaxRollsBackTheExplicitTransaction() {
        try (var session = engine.openSession()) {
            session.begin(REPEATABLE_READ);
            session.execute("insert into account(id,value) values ('1','prefix')");
            assertThrows(MvccException.class, () -> session.execute("insert into account nonsense"));
            assertFalse(session.active());
            session.rollback();
            assertEquals("0", session.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /** 两个事务并发插入相同主键时，后提交者必须冲突而不能覆盖已经确认的插入。 */
    @Test
    void concurrentInsertUsesPercolatorWriteConflictValidation() {
        try (var first = engine.openSession(); var second = engine.openSession()) {
            first.begin(REPEATABLE_READ);
            second.begin(READ_COMMITTED);
            first.execute("insert into account(id,value) values ('1','first')");
            second.execute("insert into account(id,value) values ('1','second')");
            first.commit();
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(MvccException.class, second::commit).errorCode());
            second.rollback();
            assertEquals("first", second.execute("select value from account where id='1'").singleRow().get("value"));
        }
    }

    /** 真实 primary 落盘后回复和查询均失败，SQL 异常必须携带可跨会话恢复的完整身份。 */
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
        assertEquals(failure.handle().startTs(), failure.txId());
        assertFalse(session.active());
        assertThrows(MvccException.class, () -> session.execute("insert into account(id,value) values ('3','bad')"));
        assertThrows(PercolatorTransactionStatusUnknownException.class, session::rollback);
        assertEquals(1, b.pending(10).size());
        session.close();
        session.close();
        assertEquals(State.COMMITTED, engine.runtime().lookup(failure.handle()).state());
        assertEquals(1, engine.runtime().recover(10));
        try (var reader = engine.openSession()) {
            assertEquals(List.of("one", "two"), values(reader.execute("select value from account order by id")));
        }
    }

    /** 显式 COMMIT 同样保留 UNKNOWN，不允许再次提交或用 ROLLBACK 覆盖原决议。 */
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
            assertEquals(unknown.handle(), rollback.handle());
            assertEquals(State.COMMITTED, engine.runtime().lookup(unknown.handle()).state());
        }
        assertEquals(1, b.pending(10).size());
        engine.runtime().recover(10);
        assertTrue(b.pending(10).isEmpty());
    }

    /** secondary 未补交时 SQL 返回已确认凭据和待处理键，之后的扫描按原 commitTs 补齐。 */
    @Test
    void sqlCommitReportsPendingSecondaryAndReadResolvesIt() {
        var fault = new ProbedStore(b);
        fault.failFinish = true;
        var partial = engine(new PercolatorRuntime(tso, Map.of("a", a, "b", fault)), SqlExecutionOptions.defaults());
        try (var session = partial.openSession()) {
            session.begin(REPEATABLE_READ);
            session.execute("insert into account(id,value) values ('1','one'),('2','two')");
            var commit = session.commit().percolatorCommitResult();
            assertEquals(List.of(new PercolatorKey("b", KEYS.encode("account", "1"))), commit.pendingSecondaries());
            assertEquals(State.COMMITTED, engine.runtime().lookup(commit.handle()).state());
        }
        try (var reader = engine.openSession()) {
            assertEquals(List.of("one", "two"), values(reader.execute("select value from account order by id")));
        }
        assertTrue(b.pending(10).isEmpty());
    }

    /** 相关未决锁不能在全表扫描中被跳过，防止把可能已决议的事务读成部分结果。 */
    @Test
    void sqlScanDoesNotSkipUnresolvedLocks() {
        try (var writer = engine.runtime().begin(REPEATABLE_READ); var reader = engine.openSession()) {
            writer.put("a", KEYS.encode("account", "2"), bytes("pending"));
            writer.prewrite();
            assertEquals(ErrorCode.LOCK_CONFLICT,
                    assertThrows(MvccException.class, () -> reader.execute("select count(*) as n from account")).errorCode());
            writer.commit();
            assertEquals("1", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /** 不可见 guard 与墓碑占用 SQL 输入预算，不能用空页绕过扫描限制。 */
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
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                    assertThrows(MvccException.class, () -> reader.execute("select count(*) as n from account")).errorCode());
        }
        try (var reader = engine.openSession()) {
            assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
    }

    /** 简单点查与关系查询都遵守结果预算，超限后不能发布部分写入。 */
    @Test
    void sqlResultBudgetsApplyToBothPlannerPaths() {
        insert("insert into account(id,value) values ('1','one'),('2','two')");
        var rowLimited = engine(engine.runtime(), new SqlExecutionOptions(1, 1, 1024));
        try (var reader = rowLimited.openSession()) {
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                    assertThrows(MvccException.class, () -> reader.execute("select * from account order by id")).errorCode());
        }
        var byteLimited = engine(engine.runtime(), new SqlExecutionOptions(1, 100, 1));
        try (var reader = byteLimited.openSession()) {
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                    assertThrows(MvccException.class, () -> reader.execute("select value from account where id='1'")).errorCode());
        }
    }

    /** 同页归并本事务写入，按 UTF-8 游标顺序跨过回滚残留和删除键。 */
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
                assertEquals(1, page.entries().size());
                var entry = page.entries().getFirst();
                keys.add(entry.key());
                if (entry.value() != null) { visible.add(new String(entry.value(), StandardCharsets.UTF_8)); }
                if (page.nextKey() != null) { assertNotEquals(after, page.nextKey()); }
                after = page.nextKey();
            } while (after != null);
            assertEquals(List.of("a", "b", "c", "d", "\ue000", "😀"), keys);
            assertEquals(List.of("own", "new", "bmp", "supplementary"), visible);
            assertTrue(view.scanPage("a", new KeyRange("z", "a"), null, 2).entries().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> view.scanPage("a", range, null, 0));
        }
    }

    /** 关闭会话回滚未提交写，关闭状态不可逆且不关闭应用拥有的 MySQL 池。 */
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
            assertEquals("0", reader.execute("select count(*) as n from account").singleRow().get("n"));
        }
        try (var connection = poolA.getConnection()) { assertFalse(connection.isClosed()); }
    }

    /** 借用指定运行时并采用一键一页以覆盖 SQL 分页行为。 */
    private PercolatorSqlEngine engine(PercolatorRuntime runtime, SqlExecutionOptions options) {
        return new PercolatorSqlEngine(runtime, new HybridSqlPlanner(), RESOLVER, SqlValueCodec.utf8(), READ_COMMITTED,
                new SqlExecutionOptions(1, options.maxResultRows(), options.maxResultBytes(),
                        options.maxScannedRows(), options.maxScannedBytes()));
    }

    /** 用自动提交 SQL 建立持久测试数据。 */
    private void insert(String sql) { try (var session = engine.openSession()) { session.execute(sql); } }

    /** 按结果顺序提取业务 value。 */
    private static List<String> values(SqlExecutionResult result) { return result.rows().stream().map(row -> row.get("value")).toList(); }

    /** 生成 UTF-8 测试值。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 保留真实 MySQL 行为，仅在明确边界执行提交回复丢失和分页交错。 */
    private static final class ProbedStore implements PercolatorStore {
        private final PercolatorStore delegate;
        private final AtomicBoolean keysObserved = new AtomicBoolean();
        Runnable afterKeys;
        boolean loseCommitReply;
        boolean failInspect;
        boolean failFinish;

        /** 包装真实分片，不构造伪造提交记录。 */
        ProbedStore(PercolatorStore delegate) { this.delegate = delegate; }

        /** 保留逻辑分片编号。 */
        @Override
        public String shardId() { return delegate.shardId(); }

        /** 初始化真实协议表。 */
        @Override
        public void initialize() { delegate.initialize(); }

        /** 原样预写数据与锁。 */
        @Override
        public void prewrite(long start, PercolatorKey primary, long ttl, List<Mutation> mutations) {
            delegate.prewrite(start, primary, ttl, mutations);
        }

        /** 原样读取同一本地一致性快照。 */
        @Override
        public Snapshot snapshot(String key, long readTs) { return delegate.snapshot(key, readTs); }

        /** 首次取页返回后才触发另一事务，不在持有 JDBC 连接时重入业务。 */
        @Override
        public List<String> keys(KeyRange range, String after, int limit) {
            List<String> result = delegate.keys(range, after, limit);
            if (afterKeys != null && keysObserved.compareAndSet(false, true)) { afterKeys.run(); }
            return result;
        }

        /** COMMIT 真实持久化后丢回复，INSPECT 故障保留为 UNKNOWN。 */
        @Override
        public Decision primary(PercolatorKey key, long start, Action action, long commitTs) {
            if (action == Action.INSPECT && failInspect) { throw unavailable(start); }
            Decision result = delegate.primary(key, start, action, commitTs);
            if (action == Action.COMMIT && loseCommitReply) { throw unavailable(start); }
            return result;
        }

        /** 不可达的 secondary 保留锁供后续运行时解析。 */
        @Override
        public void finish(long start, PercolatorKey primary, Decision decision, List<String> keys) {
            if (failFinish) { throw unavailable(start); }
            delegate.finish(start, primary, decision, keys);
        }

        /** 有界发现真实残留锁。 */
        @Override
        public List<Lock> pending(int limit) { return delegate.pending(limit); }

        /** 注入可识别的存储不可达故障。 */
        private static MvccException unavailable(long start) {
            return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected Percolator SQL outage", true, start);
        }
    }
}

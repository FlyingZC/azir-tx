package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 MySQL 验证有界游标、墓碑分批删除、配置拒绝和托管自动维护。 */
@Testcontainers
class WalMysqlOperationsTest {
    @Container static final MySQLContainer<?> DATABASE = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;
    private HikariDataSource pool;

    /** 每轮只重置自有容器的协议表。 */
    @BeforeEach void prepare() throws Exception {
        pool = WalCheckpointMysqlSupport.pool(DATABASE);
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("drop table if exists azir_wal_version"); query.executeUpdate("drop table if exists azir_wal_binding");
            query.executeUpdate("drop table if exists AZIR_WAL_VERSION");
        }
    }

    /** 数据源仅由本用例拥有。 */
    @AfterEach void closePool() { if (pool != null) { pool.close(); } }

    /** 墓碑必须在所有旧值删除后才退休，重开仍恢复游标和轮转。 */
    @Test void boundedGcPersistsCursorAndNeverResurrectsDelete() throws Exception {
        UUID engine = UUID.randomUUID(); var store = new MysqlWalMaterializedStore(pool); store.initialize(engine, "a");
        var commits = new ArrayList<WalCommit>();
        commits.add(new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "", bytes("old")), new WalMutation("a", "z", bytes("live")))));
        for (int i = 2; i <= 20; i++) { commits.add(new WalCommit(UUID.randomUUID(), i, i - 1, List.of(new WalMutation("a", "", bytes("v" + i))))); }
        commits.add(new WalCommit(UUID.randomUUID(), 21, 20, List.of(new WalMutation("a", "", null)))); store.apply(commits);
        int removed = 0; boolean wrapped = false;
        for (int cycle = 0; cycle < 20 && removed < 21; cycle++) {
            var progress = store.gcBounded(21, 1, 5, 5000);
            assertTrue(progress.inspectedKeys() <= 1); assertTrue(progress.removedRows() <= 5); removed += progress.removedRows(); wrapped |= progress.wrapped();
            var value = store.get("", Long.MAX_VALUE); assertTrue(value.isEmpty() || value.orElseThrow().deleted());
            assertEquals("live", text(store.get("z", Long.MAX_VALUE).orElseThrow().value()));
            store = new MysqlWalMaterializedStore(pool); store.initialize(engine, "a");
        }
        assertEquals(21, removed); assertTrue(wrapped); assertEquals(1, count());
    }

    /** 自动维护保留旧 RR，结束后再逐批回收历史。 */
    @Test void managedMysqlReclaimsAfterOldSnapshotEnds() throws Exception {
        var store = new MysqlWalMaterializedStore(pool);
        try (var service = WalManagedRuntime.open(directory, Map.of("a", store), WalOptions.defaults().withHotCacheBytes(0),
                WalRetentionOptions.preserveAll(), new WalMaintenanceOptions(20, 1, 2, 100, 5000, 80))) {
            var runtime = service.runtime(); write(runtime, "a", "k", "old"); runtime.flush(); var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 10; i++) { write(runtime, "a", "k", "v" + i); } runtime.flush();
            waitFor(() -> service.health().maintenanceCycles() > 0); assertEquals("old", text(runtime.get(reader, "a", "k").orElseThrow()));
            runtime.rollback(reader); waitFor(() -> countUnchecked() == 1);
            var latest = runtime.begin(IsolationLevel.READ_COMMITTED); assertEquals("v9", text(runtime.get(latest, "a", "k").orElseThrow())); runtime.rollback(latest);
            assertTrue(service.health().ready()); assertTrue(service.metrics().contains("azir_wal_gc_removed_rows_total "));
        }
    }

    /** 不可靠持久化配置在创建 WAL 和协议表之前拒绝。 */
    @Test void unsafeDurabilityIsRejectedBeforeWalCreation() throws Exception {
        setGlobal(0);
        try {
            assertThrows(WalMvccException.class, () -> WalManagedRuntime.open(directory.resolve("unsafe"), Map.of("a", new MysqlWalMaterializedStore(pool)),
                    WalOptions.defaults(), WalRetentionOptions.preserveAll(), WalMaintenanceOptions.defaults()));
            assertFalse(Files.exists(directory.resolve("unsafe")));
            try (var connection = pool.getConnection(); var query = connection.createStatement(); var rows = query.executeQuery("show tables")) { assertFalse(rows.next()); }
        } finally { setGlobal(1); }
    }

    /** 任一无界 I/O 超时都不能进入托管入口。 */
    @ParameterizedTest @ValueSource(strings = {"connect", "socket", "pool"}) void unboundedTimeoutIsRejected(String setting) {
        var config = new HikariConfig(); String url = DATABASE.getJdbcUrl();
        if (setting.equals("connect")) { url = url.replace("connectTimeout=2000", "connectTimeout=0"); }
        if (setting.equals("socket")) { url = url.replace("socketTimeout=2000", "socketTimeout=0"); }
        config.setJdbcUrl(url); config.setUsername(DATABASE.getUsername()); config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(2); config.setMinimumIdle(0); config.setConnectionTimeout(setting.equals("pool") ? 6000 : 5000);
        try (var bad = new HikariDataSource(config)) {
            assertThrows(WalMvccException.class, () -> WalManagedRuntime.open(directory.resolve(setting), Map.of("a", new MysqlWalMaterializedStore(bad)),
                    WalOptions.defaults(), WalRetentionOptions.preserveAll(), WalMaintenanceOptions.defaults()));
            assertFalse(Files.exists(directory.resolve(setting)));
        }
    }

    /** 非事务表、非二进制键、错误主键和额外唯一约束都必须拒绝。 */
    @ParameterizedTest @ValueSource(strings = {"engine", "key_type", "primary", "unique"}) void incompatibleSchemasFailClosed(String problem) throws Exception {
        String type = problem.equals("key_type") ? "varchar(512)" : "varbinary(512)";
        String primary = problem.equals("primary") ? "commit_seq,user_key" : "user_key,commit_seq";
        String extra = problem.equals("unique") ? ",unique key wrong(user_key)" : "";
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("create table azir_wal_version(user_key " + type + " not null,commit_seq bigint not null,payload longblob null,primary key(" + primary + ")" + extra + ") engine=" + (problem.equals("engine") ? "MyISAM" : "InnoDB"));
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory.resolve(problem), Map.of("a", new MysqlWalMaterializedStore(pool)), WalOptions.defaults()));
        assertEquals(0, count());
    }

    /** 非法原始字节不允许被备份或扫描静默改成另一个键。 */
    @Test void invalidBackendKeyBytesAreRejected() throws Exception {
        var store = new MysqlWalMaterializedStore(pool); store.initialize(UUID.randomUUID(), "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value")))); var boundary = store.apply(List.of(commit));
        try (var connection = pool.getConnection(); var query = connection.createStatement()) { query.executeUpdate("update azir_wal_version set user_key=x'ff'"); }
        assertThrows(WalMvccException.class, () -> store.exportSnapshot(boundary, row -> { })); assertThrows(WalMvccException.class, () -> store.scan(null, null, 1));
    }

    /** 大小写不同的相似表不能参与引擎、字段或索引检查，也不能被迁移覆盖。 */
    @Test void caseDistinctUnrelatedTableIsIgnored() throws Exception {
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("create table AZIR_WAL_VERSION(marker varchar(16)) engine=MyISAM");
            query.executeUpdate("insert into AZIR_WAL_VERSION values ('keep')");
        }
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(pool)), WalOptions.defaults())) {
            write(runtime, "a", "k", "value"); runtime.flush();
        }
        try (var connection = pool.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select marker from AZIR_WAL_VERSION")) {
            assertTrue(row.next()); assertEquals("keep", row.getString(1));
        }
    }

    /** RU 不能把其他连接尚未提交的前缀当成落盘确认，未知结果应安全失败后重试。 */
    @Test void uncommittedBackendPrefixCannotAuthorizeWalRetirement() throws Exception {
        var config = new HikariConfig(); config.setJdbcUrl(DATABASE.getJdbcUrl()); config.setUsername(DATABASE.getUsername()); config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(2); config.setMinimumIdle(0); config.setConnectionTimeout(5000); config.setTransactionIsolation("TRANSACTION_READ_UNCOMMITTED");
        try (var ru = new HikariDataSource(config)) {
            var store = new MysqlWalMaterializedStore(ru); store.initialize(UUID.randomUUID(), "a");
            var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value")))); var confirmed = store.apply(List.of(commit));
            try (var connection = pool.getConnection(); var query = connection.createStatement()) {
                connection.setAutoCommit(false);
                try {
                    query.executeUpdate("update azir_wal_binding set applied_seq=2");
                    assertThrows(WalMvccException.class, store::checkpoint);
                } finally { connection.rollback(); }
            }
            assertEquals(confirmed, store.checkpoint());
            assertThrows(WalMvccException.class, store::validateProduction);
        }
    }

    /** 不安全读取超时必须在任何可能访问服务器的连接状态方法之前被拒绝。 */
    @Test void unsafeTimeoutIsRejectedBeforeServerStatusQueries() {
        var config = new HikariConfig(); config.setJdbcUrl(DATABASE.getJdbcUrl().replace("socketTimeout=2000", "socketTimeout=0"));
        config.setUsername(DATABASE.getUsername()); config.setPassword(DATABASE.getPassword()); config.setMaximumPoolSize(2);
        config.setMinimumIdle(0); config.setConnectionTimeout(5000);
        try (var bad = new HikariDataSource(config)) {
            javax.sql.DataSource guarded = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                Object result = delegated(bad, method, args);
                if (!method.getName().equals("getConnection")) { return result; }
                return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Connection.class}, (c, operation, values) -> {
                    if (List.of("isReadOnly", "getTransactionIsolation", "createStatement", "prepareStatement").contains(operation.getName())) {
                        throw new AssertionError("server query attempted before timeout validation");
                    }
                    return delegated(result, operation, values);
                });
            });
            assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(guarded).validateProduction());
        }
    }

    /** 即使引擎和分片相同，不同副本也不能共享数据库，普通模式同样不能接管。 */
    @Test void replicaBackendOwnershipRejectsOtherNodesAndStandaloneAccess() throws Exception {
        UUID engine = UUID.randomUUID(); UUID group = UUID.randomUUID();
        var first = new WalReplicaIdentity(engine, group, "n1"); var second = new WalReplicaIdentity(engine, group, "n2");
        var owner = new MysqlWalMaterializedStore(pool); owner.initializeReplica(first, "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("owned")))); owner.apply(List.of(commit));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(pool).initializeReplica(second, "a"));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(pool).initialize(engine, "a"));
        assertEquals("owned", text(owner.get("k", 1).orElseThrow().value()));
        var reopened = new MysqlWalMaterializedStore(pool); reopened.initializeReplica(first, "a"); assertEquals(1, reopened.checkpoint().sequence());
        try (var connection = pool.getConnection(); var query = connection.createStatement()) { query.executeUpdate("update azir_wal_binding set replica_owner='" + "0".repeat(64) + "'"); }
        assertThrows(WalMvccException.class, () -> owner.get("k", 1));
        assertThrows(WalMvccException.class, () -> owner.latestSequence("k"));
    }

    /** 已物化并截断日志后丢失版本表必须拒绝，不能补建空表后把确认数据读成不存在。 */
    @Test void missingColdVersionTableCannotBeSilentlyRecreated() throws Exception {
        Path wal = directory.resolve("lost-cold-table"); var options = WalOptions.defaults().withHotCacheBytes(0);
        try (var runtime = WalMvccRuntime.open(wal, Map.of("a", new MysqlWalMaterializedStore(pool)), options)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes("confirmed")); runtime.commit(tx); runtime.flush();
            assertTrue(runtime.checkpoint().created()); assertEquals(0, runtime.stats().retainedBytes()); assertEquals(100, runtime.stats().walBytes());
        }
        try (var connection = pool.getConnection(); var ddl = connection.createStatement()) { ddl.executeUpdate("drop table azir_wal_version"); }
        assertThrows(WalMvccException.class, () -> { try (var ignored = WalMvccRuntime.open(wal, Map.of("a", new MysqlWalMaterializedStore(pool)), options)) { } });
        try (var connection = pool.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select count(*) from information_schema.tables where table_schema=database() and table_name='azir_wal_version'")) {
            row.next(); assertEquals(0, row.getInt(1));
        }
    }

    /** 首次 DDL 只留下空绑定表时没有任何确认前缀，可以安全补建版本表完成初始化。 */
    @Test void emptyBindingBeforeFirstPublicationCanFinishInterruptedTableCreation() throws Exception {
        UUID engine = UUID.randomUUID(); new MysqlWalMaterializedStore(pool).initialize(engine, "a");
        try (var connection = pool.getConnection(); var ddl = connection.createStatement()) { ddl.executeUpdate("delete from azir_wal_binding"); ddl.executeUpdate("drop table azir_wal_version"); }
        var resumed = new MysqlWalMaterializedStore(pool); resumed.initialize(engine, "a"); assertEquals(0, resumed.checkpoint().sequence()); assertEquals(0, count());
    }

    /** 代理保留真实异常，测试失败不能被反射包装掩盖。 */
    private static Object delegated(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }

    /** 独立物理计数不会被缓存掩盖。 */
    private long count() throws Exception {
        try (var connection = pool.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select count(*) from azir_wal_version")) { row.next(); return row.getLong(1); }
    }

    /** 等待条件不能把查询异常当作零行。 */
    private long countUnchecked() { try { return count(); } catch (Exception failure) { throw new IllegalStateException(failure); } }

    /** 只调整本测试容器，结束时恢复配置。 */
    private static void setGlobal(int value) throws Exception {
        var result = DATABASE.execInContainer("mysql", "-uroot", "-p" + DATABASE.getPassword(), "-e", "set global innodb_flush_log_at_trx_commit=" + value);
        assertEquals(0, result.getExitCode(), result.getStderr());
    }
}

package mvcc.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 用真实数据库提交后异常及强制容器重启覆盖物化的持久性边界。 */
@Testcontainers
class WalMysqlBoundaryTest {
    @Container static final MySQLContainer<?> DATABASE = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("wal_boundary").withUsername("wal_test").withPassword("wal_test")
            .withUrlParam("rewriteBatchedStatements", "true").withUrlParam("connectTimeout", "2000")
            .withUrlParam("socketTimeout", "2000")
            .withCommand("--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1");
    @TempDir Path directory;
    private volatile HikariDataSource pool;

    /** 正式验收各 100 次，普通回归每种故障一次。 */
    static IntStream repetitions() { return IntStream.range(0, Integer.getInteger("azir.wal.mysqlBoundaryRepeats", 1)); }

    /** 每轮只清理本测试创建的容器专属表。 */
    @BeforeEach void prepare() throws Exception {
        pool = createPool(actualJdbcUrl());
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("drop table if exists azir_wal_version"); query.executeUpdate("drop table if exists azir_wal_binding");
        }
    }

    /** 按当前实际端点创建连接池，不把 Docker 随机发布端口当成永久地址。 */
    private static HikariDataSource createPool(String url) {
        var config = new HikariConfig(); config.setJdbcUrl(url);
        config.setUsername(DATABASE.getUsername()); config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(3); config.setMinimumIdle(0); config.setConnectionTimeout(3000); config.setValidationTimeout(1000);
        // 容器 start 返回并不表示数据库已完成恢复，连接建立由有界就绪检查等待。
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }

    /** 每轮核对 Docker 的当前随机映射，不能复用首次启动时缓存的端口。 */
    private static String actualJdbcUrl() {
        var inspected = DATABASE.getDockerClient().inspectContainerCmd(DATABASE.getContainerId()).exec();
        var bindings = inspected.getNetworkSettings().getPorts().getBindings()
                .get(new com.github.dockerjava.api.model.ExposedPort(3306));
        assertNotNull(bindings); assertTrue(bindings.length > 0);
        return "jdbc:mysql://" + DATABASE.getHost() + ":" + bindings[0].getHostPortSpec()
                + "/wal_boundary?rewriteBatchedStatements=true&connectTimeout=2000&socketTimeout=2000";
    }

    /** 关闭本轮连接池，容器交由 Testcontainers 生命周期管理。 */
    @AfterEach void cleanup() { if (pool != null) { pool.close(); } }

    /** 在真实 commit 成功后抛出通信异常，检查未知结果处理路径而非仅忽略正常返回。 */
    @ParameterizedTest @MethodSource("repetitions")
    void databaseCommittedButResponseFailedIsSafelyReplayed(int repetition) {
        var armed = new AtomicBoolean();
        DataSource uncertain = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    Object result = delegate(pool, method, arguments);
                    if (!method.getName().equals("getConnection")) { return result; }
                    var connection = (Connection) result;
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (p, operation, values) -> {
                        Object returned = delegate(connection, operation, values);
                        if (operation.getName().equals("commit") && armed.compareAndSet(true, false)) {
                            throw new SQLException("injected response loss after real database commit", "08S01");
                        }
                        return returned;
                    });
                });
        var store = new MysqlWalMaterializedStore(uncertain); store.initialize(UUID.randomUUID(), "a");
        var first = record(1, "one"); armed.set(true);
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE, assertThrows(WalMvccException.class, () -> store.apply(List.of(first))).errorCode());
        assertEquals(1, store.checkpoint().sequence()); assertEquals("one", text(store.get("k", 1).orElseThrow().value()));
        assertEquals(first.digest(), store.apply(List.of(first)).digest());
        var second = record(2, "two"); assertEquals(2, store.apply(List.of(first, second)).sequence());
        assertEquals("one", text(store.get("k", 1).orElseThrow().value()));
        assertEquals("two", text(store.get("k", 2).orElseThrow().value()));
    }

    /** 强制停止本测试的 MySQL，保留本地 WAL，重启数据库后恢复物化并核对冷历史。 */
    @ParameterizedTest @MethodSource("repetitions")
    void killedMysqlRestartsWithoutLosingOrDuplicatingMaterializedVersions(int repetition) throws Exception {
        // 测试容器重新启动可能换宿主端口，稳定 DataSource 门面将新连接交给当前池。
        DataSource routed = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> delegate(pool, method, arguments));
        var store = new MysqlWalMaterializedStore(routed);
        var defaults = WalOptions.defaults();
        var options = new WalOptions(defaults.maxActiveTransactions(), defaults.maxWriteKeys(), defaults.maxWriteBytes(),
                defaults.queueCapacity(), defaults.commitBatchSize(), defaults.batchDelayMillis(), defaults.maxWalBytes(),
                defaults.maxMemoryBytes(), defaults.transactionTimeoutMillis(), defaults.flushBatchSize(), 60_000);
        UUID decision;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), options)) {
            write(runtime, "a", "k", "old"); runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            String containerId = DATABASE.getContainerId();
            DATABASE.getDockerClient().stopContainerCmd(containerId).withTimeout(0).exec();
            try {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); decision = tx.id();
                runtime.put(tx, "a", "k", bytes("new")); runtime.commit(tx);
                var fresh = runtime.begin(IsolationLevel.READ_COMMITTED);
                assertEquals("new", text(runtime.get(fresh, "a", "k").orElseThrow())); runtime.rollback(fresh);
                assertThrows(WalMvccException.class, runtime::flush);
            } finally {
                DATABASE.getDockerClient().startContainerCmd(containerId).exec();
                var previous = pool;
                pool = createPool(actualJdbcUrl());
                previous.close();
            }
            awaitDatabase();
            runtime.flush(); runtime.collectGarbage(100);
            assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            assertEquals("old", text(store.get("k", 1).orElseThrow().value()));
            assertEquals("new", text(store.get("k", 2).orElseThrow().value()));
            runtime.rollback(old); runtime.collectGarbage(100);
        }
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(routed)), options)) {
            assertEquals(2, restored.findCommitted(decision).orElseThrow().commitSequence());
            var reader = restored.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals("new", text(restored.get(reader, "a", "k").orElseThrow())); restored.rollback(reader);
        }
        try (var connection = pool.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("select count(*) from azir_wal_version")) {
            rows.next(); assertEquals(1, rows.getLong(1));
        }
    }

    /** 反射代理保留真实 JDBC 错误及提交顺序。 */
    private static Object delegate(Object target, Method method, Object[] arguments) throws Throwable {
        try { return method.invoke(target, arguments); }
        catch (InvocationTargetException error) { throw error.getCause(); }
    }

    /** 创建稳定序号的物化日志项。 */
    private static WalCommit record(long sequence, String value) {
        return new WalCommit(UUID.randomUUID(), sequence, sequence - 1, List.of(new WalMutation("a", "k", bytes(value))));
    }

    /** 只轮询本测试容器的数据库，重建连接失败也有明确超时。 */
    private void awaitDatabase() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        SQLException latest = null;
        do {
            try (var connection = pool.getConnection(); var query = connection.createStatement()) { query.execute("select 1"); return; }
            catch (SQLException transientFailure) { latest = transientFailure; }
            Thread.sleep(20);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("owned MySQL did not recover", latest);
    }
}

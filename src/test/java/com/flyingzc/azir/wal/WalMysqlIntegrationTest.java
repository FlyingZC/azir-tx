package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 用两个独立 MySQL 实例验证新模式，不接触已有环境和旧模式协议表。 */
@Testcontainers
class WalMysqlIntegrationTest {
    @Container static final MySQLContainer<?> A = container();
    @Container static final MySQLContainer<?> B = container();
    @TempDir Path directory;
    private HikariDataSource a;
    private HikariDataSource b;

    /** 普通回归各执行一次，正式验收显式指定真实后端重复次数。 */
    static java.util.stream.IntStream repetitions() {
        return java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.mysqlRepeats", 1));
    }

    /** 创建强刷盘配置的隔离数据库容器。 */
    private static MySQLContainer<?> container() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("wal_mvp")
                .withUsername("wal_test").withPassword("wal_test")
                .withUrlParam("rewriteBatchedStatements", "true")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000")
                .withCommand("--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1");
    }

    /** 为每个用例清理专属空表并创建有界连接池。 */
    @BeforeEach void prepare() throws Exception {
        a = pool(A); b = pool(B);
        for (var dataSource : List.of(a, b)) {
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version");
                statement.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /** 仅关闭本用例创建的连接池。 */
    @AfterEach void cleanup() { if (a != null) { a.close(); } if (b != null) { b.close(); } }

    /** 配置短 JDBC 超时，避免后端故障阻塞关闭和资源回收。 */
    private static HikariDataSource pool(MySQLContainer<?> container) {
        return pool(container, 0);
    }

    /** 用较小客户端单包限制验证物化按字节拆批。 */
    private static HikariDataSource pool(MySQLContainer<?> container, int packetLimit) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl()); config.setUsername(container.getUsername()); config.setPassword(container.getPassword());
        config.setMaximumPoolSize(4); config.setMinimumIdle(0);
        config.setConnectionTimeout(2500); config.setValidationTimeout(1000);
        if (packetLimit > 0) {
            config.addDataSourceProperty("maxAllowedPacket", Integer.toString(packetLimit));
            config.addDataSourceProperty("useServerPrepStmts", "true");
        }
        return new HikariDataSource(config);
    }

    /** 合批总大小超过单包上限时仍在同一数据库事务中完整物化。 */
    @ParameterizedTest @MethodSource("repetitions") void largeMaterializationBatchIsSplitByPacketBudget(int repetition) throws Exception {
        try (var smallPackets = pool(A, 65_536)) {
            var store = new MysqlWalMaterializedStore(smallPackets);
            try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            byte[] payload = new byte[1024]; java.util.Arrays.fill(payload, (byte) 7);
            for (int i = 0; i < 300; i++) { runtime.put(tx, "a", "packet:" + i, payload); }
            runtime.commit(tx); drain(runtime);
            assertEquals(300, count(a));
            var persisted = store.scan(null, null, 1);
            assertEquals(300, persisted.size());
            for (int i = 0; i < 300; i++) { assertArrayEquals(payload, persisted.get("packet:" + i).value()); }
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertArrayEquals(payload, runtime.get(reader, "a", "packet:299").orElseThrow());
            runtime.rollback(reader);
            }
        }
    }

    /** 拆批后第二个 SQL 批次失败必须回滚前一批，检查点也不能推进。 */
    @ParameterizedTest @MethodSource("repetitions") void secondPacketChunkFailureRollsBackAllChunksAndCheckpoint(int repetition) throws Exception {
        try (var smallPackets = pool(A, 65_536)) {
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            javax.sql.DataSource failing = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                        Object result = delegated(smallPackets, method, args);
                        if (method.getName().equals("getConnection")) {
                            var connection = (java.sql.Connection) result;
                            return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[]{java.sql.Connection.class}, (p, connectionMethod, connectionArgs) -> {
                                        Object statement = delegated(connection, connectionMethod, connectionArgs);
                                        if (connectionMethod.getName().equals("prepareStatement")
                                                && ((String) connectionArgs[0]).startsWith("insert into azir_wal_version")) {
                                            return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                                    new Class<?>[]{java.sql.PreparedStatement.class}, (s, operation, values) -> {
                                                        if (operation.getName().equals("executeBatch") && armed.get()
                                                                && calls.incrementAndGet() == 2) {
                                                            throw new java.sql.SQLException("injected second packet chunk failure");
                                                        }
                                                        return delegated(statement, operation, values);
                                                    });
                                        }
                                        return statement;
                                    });
                        }
                        return result;
                    });
            var store = new MysqlWalMaterializedStore(failing); store.initialize(UUID.randomUUID(), "a");
            var mutations = new java.util.ArrayList<WalMutation>();
            for (int i = 0; i < 300; i++) { mutations.add(new WalMutation("a", "chunk:" + i, new byte[1024])); }
            var commit = new WalCommit(UUID.randomUUID(), 1, 0, mutations);
            assertThrows(WalMvccException.class, () -> store.apply(List.of(commit)));
            assertEquals(0, count(a)); assertEquals(0, store.checkpoint().sequence());
            armed.set(false); assertEquals(1, store.apply(List.of(commit)).sequence()); assertEquals(300, count(a));
        }
    }

    /** 反射代理保留原始 JDBC 异常，避免把注入失败包成反射包装异常。 */
    private static Object delegated(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }

    /** 单行超过后端可物化预算时必须在持久提交之前拒绝。 */
    @ParameterizedTest @MethodSource("repetitions") void oversizedRowIsRejectedBeforeWalConfirmation(int repetition) throws Exception {
        try (var smallPackets = pool(A, 65_536);
             var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(smallPackets)), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals(com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                    () -> runtime.put(tx, "a", "too-large", new byte[40_000])).errorCode());
            assertEquals(0, runtime.stats().publishedSequence()); assertEquals(60, runtime.stats().walBytes());
            assertEquals(0, runtime.stats().bufferedBytes()); runtime.rollback(tx);
        }
    }

    /** 一片只物化旧值、另一片已物化新值时，全局快照仍保持完整。 */
    @ParameterizedTest @MethodSource("repetitions") void partialFlushOldRrGcAndRestartRemainConsistent(int repetition) throws Exception {
        var left = new MysqlWalMaterializedStore(a);
        var right = new ControlledStore(new MysqlWalMaterializedStore(b));
        UUID committedId;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", left, "b", right), WalOptions.defaults())) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "balance", bytes("100")); runtime.put(seed, "b", "balance", bytes("100"));
            runtime.commit(seed); drain(runtime);
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            right.paused = true;
            var transfer = runtime.begin(IsolationLevel.REPEATABLE_READ);
            committedId = transfer.id();
            runtime.put(transfer, "a", "balance", bytes("90")); runtime.put(transfer, "b", "balance", bytes("110"));
            runtime.commit(transfer);
            assertThrows(WalMvccException.class, runtime::flush);
            assertEquals("90", text(left.get("balance", 2).orElseThrow().value()));
            assertEquals("100", text(right.delegate.get("balance", 2).orElseThrow().value()));
            assertEquals("100", text(runtime.get(old, "a", "balance").orElseThrow()));
            assertEquals("100", text(runtime.get(old, "b", "balance").orElseThrow()));
            var fresh = runtime.begin(IsolationLevel.READ_COMMITTED);
            var snapshot = runtime.beginReadStatement(fresh);
            assertEquals("90", text(runtime.get(fresh, "a", "balance", snapshot).orElseThrow()));
            assertEquals("110", text(runtime.get(fresh, "b", "balance", snapshot).orElseThrow()));
            right.paused = false; drain(runtime);
            runtime.collectGarbage(100);
            assertEquals("100", text(runtime.get(old, "b", "balance").orElseThrow()));
            runtime.rollback(old); runtime.rollback(fresh);
            assertEquals(2, runtime.collectGarbage(100));
            assertEquals(1, count(a)); assertEquals(1, count(b));
        }
        try (var recovered = WalMvccRuntime.open(directory,
                Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b)), WalOptions.defaults())) {
            var tx = recovered.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals("90", text(recovered.get(tx, "a", "balance").orElseThrow()));
            assertEquals("110", text(recovered.get(tx, "b", "balance").orElseThrow()));
            assertEquals(2, recovered.findCommitted(committedId).orElseThrow().commitSequence());
            recovered.rollback(tx);
        }
        try (var connection = a.getConnection(); var query = connection.createStatement(); var tables = query.executeQuery("show tables")) {
            var names = new java.util.HashSet<String>();
            while (tables.next()) { names.add(tables.getString(1)); }
            assertEquals(java.util.Set.of("azir_wal_binding", "azir_wal_version"), names);
        }
    }

    /** 数据库提交成功而响应丢失后的重放不能重复写，也不能接受冲突日志。 */
    @ParameterizedTest @MethodSource("repetitions") void committedDatabaseBatchCanBeReplayedAfterLostResponse(int repetition) throws Exception {
        var store = new MysqlWalMaterializedStore(a);
        store.initialize(UUID.randomUUID(), "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("v"))));
        assertEquals(1, store.apply(List.of(commit)).sequence());
        // 调用方忽略第一次返回，按未知结果重新发送完全相同的批次。
        assertEquals(1, store.apply(List.of(commit)).sequence());
        assertEquals(1, count(a));
        var conflicting = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("wrong"))));
        assertThrows(WalMvccException.class, () -> store.apply(List.of(conflicting)));
        var gap = new WalCommit(UUID.randomUUID(), 3, 1, List.of(new WalMutation("a", "k", bytes("gap"))));
        assertThrows(WalMvccException.class, () -> store.apply(List.of(gap)));
        assertEquals("v", text(store.get("k", 3).orElseThrow().value()));
    }

    /** 墓碑和重建在回收及重启后不会复活已删除数据。 */
    @ParameterizedTest @MethodSource("repetitions") void tombstoneHistorySurvivesGcAndRecovery(int repetition) throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults())) {
            write(runtime, "a", "k", "old"); drain(runtime);
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k"); runtime.commit(deletion);
            write(runtime, "a", "k", "new");
            var finalDelete = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(finalDelete, "a", "k"); runtime.commit(finalDelete);
            drain(runtime); runtime.collectGarbage(100);
            assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            assertEquals(List.of("k"), new java.util.ArrayList<>(runtime.scan(old, "a", null, null).keySet()));
            runtime.rollback(old); assertEquals(4, runtime.collectGarbage(100));
        }
        try (var recovered = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults())) {
            var tx = recovered.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(recovered.get(tx, "a", "k").isEmpty());
            assertTrue(recovered.scan(tx, "a", null, null).isEmpty());
        }
        assertEquals(0, count(a));
    }

    /** 新日志身份和落后日志都不得接管已物化的存储。 */
    @ParameterizedTest @MethodSource("repetitions") void bindingAndAheadCheckpointRejectUnsafeStartup(int repetition) throws Exception {
        byte[] oldLog;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults())) {
            write(runtime, "a", "k", "one"); drain(runtime);
            oldLog = Files.readAllBytes(directory.resolve("transactions.wal"));
            write(runtime, "a", "k", "two"); drain(runtime);
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory.resolve("foreign"),
                Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults()));
        Files.write(directory.resolve("transactions.wal"), oldLog);
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory,
                Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults()));
        assertEquals(2, count(a));
    }

    /** 暂停一个真实 MySQL 进程，前台 WAL 提交与新版本读取仍能完成。 */
    @ParameterizedTest @MethodSource("repetitions") void pausedMysqlDoesNotEnterTheCommitCriticalPath(int repetition) throws Exception {
        try (var runtime = WalMvccRuntime.open(directory,
                Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b)), WalOptions.defaults())) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "k", bytes("old-left")); runtime.put(seed, "b", "k", bytes("old-right"));
            runtime.commit(seed); drain(runtime);
            B.getDockerClient().pauseContainerCmd(B.getContainerId()).exec();
            try {
                // 已物化的最新头仍在缓存中，不能因数据库暂停退化为必需的同步数据库读取。
                var cached = runtime.begin(IsolationLevel.READ_COMMITTED);
                assertEquals("old-right", text(runtime.get(cached, "b", "k").orElseThrow()));
                runtime.rollback(cached);
                long start = System.nanoTime();
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.put(tx, "a", "k", bytes("left")); runtime.put(tx, "b", "k", bytes("right"));
                runtime.commit(tx);
                assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
                var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
                assertEquals("left", text(runtime.get(reader, "a", "k").orElseThrow()));
                assertEquals("right", text(runtime.get(reader, "b", "k").orElseThrow()));
                runtime.rollback(reader);
                assertThrows(WalMvccException.class, runtime::flush);
            } finally { B.getDockerClient().unpauseContainerCmd(B.getContainerId()).exec(); }
            drain(runtime);
            assertEquals(2, count(a)); assertEquals(2, count(b));
        }
    }

    /** 等待有限日志批次全部物化，同时允许故障恢复后的连接池重建。 */
    private static void drain(WalMvccRuntime runtime) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        do {
            try { runtime.flush(); } catch (WalMvccException transientFailure) {
                if (System.nanoTime() > until) { throw transientFailure; }
            }
            var stats = runtime.stats();
            if (stats.materializedSequences().values().stream().allMatch(sequence -> sequence == stats.publishedSequence())) { return; }
            Thread.sleep(10);
        } while (System.nanoTime() <= until);
        fail("materialization did not catch up");
    }

    /** 读取专属版本表行数，用于证明重放和回收没有重复或丢失。 */
    private static long count(HikariDataSource dataSource) throws Exception {
        try (var connection = dataSource.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("select count(*) from azir_wal_version")) { rows.next(); return rows.getLong(1); }
    }

    /** 可暂停一片物化的测试包装，不改变真实 MySQL 读写实现。 */
    private static final class ControlledStore implements WalMaterializedStore {
        final WalMaterializedStore delegate;
        volatile boolean paused;

        /** 保存被包装的真实物化存储。 */
        ControlledStore(WalMaterializedStore delegate) { this.delegate = delegate; }
        /** 初始化固定绑定。 */
        @Override public void initialize(UUID id, String shard) { delegate.initialize(id, shard); }
        /** 读取真实检查点。 */
        @Override public WalCheckpoint checkpoint() { return delegate.checkpoint(); }
        /** 暂停时拒绝物化，恢复时继续调用真实后端。 */
        @Override public WalCheckpoint apply(List<WalCommit> commits) {
            if (paused) { throw new IllegalStateException("materialization paused"); }
            return delegate.apply(commits);
        }
        /** 读取快照版本。 */
        @Override public java.util.Optional<WalValue> get(String key, long sequence) { return delegate.get(key, sequence); }
        /** 扫描快照版本。 */
        @Override public java.util.NavigableMap<String, WalValue> scan(String from, String to, long sequence) {
            return delegate.scan(from, to, sequence);
        }
        /** 有界清理旧版本。 */
        @Override public int gc(long safePoint, int limit) { return delegate.gc(safePoint, limit); }
    }
}

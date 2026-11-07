package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 受限双 MySQL 验证冷版本索引、旧快照、物理墓碑回收和幂等重放。 */
@Testcontainers
class WalBoundedMysqlTest {
    @Container static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();
    @Container static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;
    private HikariDataSource left;
    private HikariDataSource right;

    /** 每个用例使用独立测试容器中的专属表，不连接已有数据库服务。 */
    @BeforeEach void prepare() throws Exception {
        left = WalCheckpointMysqlSupport.pool(A); right = WalCheckpointMysqlSupport.pool(B);
        for (var pool : List.of(left, right)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version");
                statement.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /** 只关闭本轮创建的池，容器由测试生命周期清理。 */
    @AfterEach void cleanup() { if (left != null) { left.close(); } if (right != null) { right.close(); } }

    /** 全量活键远超缓存，冷 RR、分页、跨片冲突与低内存重开仍正确。 */
    @Test void coldTwoShardVersionsSurviveCheckpointAndSmallMemoryRestart() throws Exception {
        var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(left), "b", new MysqlWalMaterializedStore(right));
        try (var runtime = WalMvccRuntime.open(directory, stores, options())) {
            for (int i = 0; i < 200; i++) { pair(runtime, "key:" + i, "old:" + i); runtime.flush(); }
            assertEquals(200, count(left)); assertEquals(200, count(right));
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 200; i++) { pair(runtime, "key:" + i, "new:" + i); runtime.flush(); }
            assertTrue(runtime.stats().hotCacheBytes() <= 1024); assertTrue(runtime.checkpoint().created()); runtime.collectGarbage(4096);
            var snapshot = runtime.beginReadStatement(old);
            for (int i = 0; i < 200; i++) {
                for (String shard : stores.keySet()) { assertEquals("old:" + i, text(runtime.get(old, shard, "key:" + i, snapshot).orElseThrow())); }
            }
            var page = runtime.scanPage(old, "a", null, null, null, 7, snapshot);
            assertEquals(7, page.rows().size()); assertEquals("old:0", text(page.rows().get("key:0")));
            runtime.put(old, "b", "key:0", bytes("wrong"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
            runtime.collectGarbage(4096); runtime.checkpoint(); assertTrue(runtime.stats().conflictStoreLookups() > 200);
        }
        try (var restored = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            assertEquals(0, restored.stats().retainedBytes()); var tx = restored.begin(IsolationLevel.READ_COMMITTED);
            for (int i = 0; i < 200; i++) {
                for (String shard : stores.keySet()) { assertEquals("new:" + i, text(restored.get(tx, shard, "key:" + i).orElseThrow())); }
            }
            restored.rollback(tx); assertEquals(200, count(left)); assertEquals(200, count(right));
        }
    }

    /** 不断新增删除不同键时物理行数回到零，旧 WAL 重放和重开均不能复活。 */
    @Test void uniqueKeyDeleteChurnRetiresPhysicalRowsWithoutResurrection() throws Exception {
        var a = new MysqlWalMaterializedStore(left); var b = new MysqlWalMaterializedStore(right);
        var stores = Map.<String, WalMaterializedStore>of("a", a, "b", b);
        try (var runtime = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            for (int i = 0; i < 96; i++) {
                pair(runtime, "gone:" + i, "value"); var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.delete(tx, "a", "gone:" + i); runtime.delete(tx, "b", "gone:" + i); runtime.commit(tx);
                runtime.flush(); runtime.collectGarbage(8); assertEquals(0, count(left)); assertEquals(0, count(right));
            }
        }
        // 模拟数据库成功应用但调用方仍重放旧日志；前缀已经推进，不能重新插入退休的旧值。
        try (var log = new WalLog(directory, Set.of("a", "b"), options().maxWalBytes())) {
            a.apply(log.read(1, 1024)); b.apply(log.read(1, 1024)); assertEquals(0, count(left)); assertEquals(0, count(right));
        }
        try (var restored = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(restored.scan(tx, "a", null, null).isEmpty()); assertTrue(restored.scan(tx, "b", null, null).isEmpty()); restored.rollback(tx);
            pair(restored, "gone:0", "reborn"); restored.flush(); restored.checkpoint();
        }
        try (var restored = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals("reborn", text(restored.get(tx, "a", "gone:0").orElseThrow()));
            assertEquals("reborn", text(restored.get(tx, "b", "gone:0").orElseThrow())); restored.rollback(tx);
            assertEquals(1, count(left)); assertEquals(1, count(right));
        }
    }

    /** 旧 RR 结束前不得物理删除它需要的历史；删除和重建后的旧写者仍冲突。 */
    @Test void physicalRetirementWaitsForOldReaderAndPreservesNewerVersion() throws Exception {
        var store = new MysqlWalMaterializedStore(left);
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), options().withHotCacheBytes(0))) {
            write(runtime, "a", "k", "old"); runtime.flush(); var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k"); runtime.commit(deletion); runtime.flush();
            runtime.collectGarbage(8); assertEquals(2, count(left)); assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            var afterDelete = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "new"); runtime.flush(); runtime.rollback(old); runtime.collectGarbage(8);
            assertEquals(1, count(left)); assertTrue(runtime.get(afterDelete, "a", "k").isEmpty());
            runtime.put(afterDelete, "a", "k", bytes("wrong"));
            assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(afterDelete)).errorCode());
            assertEquals("new", text(store.get("k", Long.MAX_VALUE).orElseThrow().value()));
        }
    }

    /** 回收批次第二次删除失败时，首次删除也必须回滚，应用前缀不受影响。 */
    @Test void tombstoneGcFailureRollsBackWholeLocalBatch() throws Exception {
        var armed = new AtomicBoolean(); var calls = new AtomicInteger();
        javax.sql.DataSource failing = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                    Object result = delegated(left, method, args);
                    if (!method.getName().equals("getConnection")) { return result; }
                    return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Connection.class},
                            (p, operation, values) -> {
                                Object statement = delegated(result, operation, values);
                                if (!operation.getName().equals("prepareStatement") || !((String) values[0]).startsWith("delete from azir_wal_version")) { return statement; }
                                return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.PreparedStatement.class},
                                        (s, action, parameters) -> {
                                            if (action.getName().equals("executeUpdate") && armed.get() && calls.incrementAndGet() == 2) {
                                                throw new java.sql.SQLException("injected GC failure after first physical delete");
                                            }
                                            return delegated(statement, action, parameters);
                                        });
                            });
                });
        var store = new MysqlWalMaterializedStore(failing);
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), options().withHotCacheBytes(0))) {
            for (String key : new String[]{"a", "b"}) {
                write(runtime, "a", key, "old"); var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.delete(deletion, "a", key); runtime.commit(deletion);
            }
            runtime.flush(); assertEquals(4, count(left)); armed.set(true);
            assertThrows(WalMvccException.class, () -> runtime.collectGarbage(8)); assertEquals(4, count(left));
            assertEquals(4, store.checkpoint().sequence()); armed.set(false);
            assertEquals(4, runtime.collectGarbage(8)); assertEquals(0, count(left));
        }
    }

    /** 解包反射异常，确保注入仍表现为真实 JDBC 错误。 */
    private static Object delegated(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }

    /** 小热点预算用于迫使冷数据路径参与，后台任务保持有限批次。 */
    private static WalOptions options() {
        return new WalOptions(32, 16, 8192, 32, 8, 1, 8L * 1024 * 1024, 32_768, 60_000, 1024, 50).withHotCacheBytes(1024);
    }

    /** 直接核对专属版本表的物理行数，不依赖运行时缓存。 */
    private static long count(HikariDataSource pool) throws Exception {
        try (var connection = pool.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("select count(*) from azir_wal_version")) {
            assertTrue(rows.next()); return rows.getLong(1);
        }
    }

    /** 用同一提交域更新两个物理后端。 */
    private static void pair(WalMvccRuntime runtime, String key, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", key, bytes(value)); runtime.put(tx, "b", key, bytes(value)); runtime.commit(tx);
    }
}

package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实数据库验证恢复中隔离、分批重试、完整摘要确认及目标占用保护。 */
@Testcontainers
class WalMysqlBackupStoreTest {
    @Container static final MySQLContainer<?> SOURCE = WalCheckpointMysqlSupport.container();
    @Container static final MySQLContainer<?> TARGET = WalCheckpointMysqlSupport.container().withUrlParam("useAffectedRows", "true");
    @TempDir Path directory;
    private HikariDataSource source;
    private HikariDataSource target;

    /** 每轮只清空测试容器内的专属表，已存在的业务服务不参与测试。 */
    @BeforeEach void prepare() throws Exception {
        source = WalCheckpointMysqlSupport.pool(SOURCE); target = WalCheckpointMysqlSupport.pool(TARGET);
        for (var pool : List.of(source, target)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version"); statement.executeUpdate("drop table if exists azir_wal_binding");
                statement.executeUpdate("drop table if exists azirXwalYbinding");
            }
        }
    }

    /** 连接池仅由测试拥有，容器按 Testcontainers 生命周期回收。 */
    @AfterEach void closePools() { if (source != null) { source.close(); } if (target != null) { target.close(); } }

    /** 流式快照保留原版本和墓碑，恢复重复批次与重新装配后仍得到同一完整前缀。 */
    @Test void restoreCanResumeAndPublishOnlyAfterFullDigestMatches() throws Exception {
        var from = new MysqlWalMaterializedStore(source); var engine = UUID.randomUUID(); from.initialize(engine, "a");
        var first = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "a", bytes("old")), new WalMutation("a", "中", bytes("live"))));
        var second = new WalCommit(UUID.randomUUID(), 2, 1, List.of(new WalMutation("a", "a", null), new WalMutation("a", "😀", bytes("new"))));
        var boundary = from.apply(List.of(first, second)); var rows = new ArrayList<WalBackupRow>(); from.exportSnapshot(boundary, rows::add);
        assertEquals(3, rows.size()); assertTrue(rows.getFirst().version().deleted());
        var spec = new WalRestoreSpec(UUID.randomUUID(), engine, "a", boundary); var into = new MysqlWalMaterializedStore(target);
        assertTrue(into.beginRestore(spec)); into.restoreBatch(spec, rows.subList(0, 1)); into.restoreBatch(spec, rows.subList(0, 1));
        assertThrows(WalMvccException.class, into::checkpoint);
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(target).initialize(engine, "a"));
        assertThrows(WalMvccException.class, () -> into.finishRestore(spec, rows.size(), digest(spec, rows)));
        assertThrows(WalMvccException.class, into::checkpoint);
        var resumed = new MysqlWalMaterializedStore(target); assertTrue(resumed.beginRestore(spec)); resumed.restoreBatch(spec, rows);
        resumed.finishRestore(spec, rows.size(), digest(spec, rows)); assertEquals(boundary, resumed.checkpoint());
        assertFalse(resumed.beginRestore(spec)); resumed.finishRestore(spec, rows.size(), digest(spec, rows));
        var recovered = new MysqlWalMaterializedStore(target); recovered.initialize(engine, "a");
        assertEquals(boundary, recovered.checkpoint()); assertEquals("live", text(recovered.get("中", 2).orElseThrow().value()));
        assertTrue(recovered.get("a", 2).orElseThrow().deleted());
        var next = new WalCommit(UUID.randomUUID(), 3, 2, List.of(new WalMutation("a", "a", bytes("reborn"))));
        recovered.apply(List.of(next)); assertThrows(WalMvccException.class, () -> recovered.beginRestore(spec));
        assertEquals("reborn", text(recovered.get("a", 3).orElseThrow().value()));
    }

    /** 同备份号改动引擎、路由或边界不能接管已经写入一部分数据的恢复。 */
    @Test void wrongIdentityAndNormalExistingTargetsAreRejected() throws Exception {
        var spec = new WalRestoreSpec(UUID.randomUUID(), UUID.randomUUID(), "a", new WalCheckpoint(5, "1".repeat(64)));
        var into = new MysqlWalMaterializedStore(target); assertTrue(into.beginRestore(spec));
        for (var wrong : List.of(new WalRestoreSpec(UUID.randomUUID(), spec.engineId(), "a", spec.boundary()),
                new WalRestoreSpec(spec.backupId(), spec.engineId(), "a", new WalCheckpoint(6, spec.boundary().digest())),
                new WalRestoreSpec(spec.backupId(), spec.engineId(), "b", spec.boundary()))) {
            assertThrows(RuntimeException.class, () -> new MysqlWalMaterializedStore(target).beginRestore(wrong));
        }
        var normal = new MysqlWalMaterializedStore(source); normal.initialize(spec.engineId(), "a");
        assertThrows(WalMvccException.class, () -> normal.beginRestore(spec));
        assertEquals(new WalCheckpoint(0, ""), normal.checkpoint());
    }

    /** 后端尚未追平或导出消费方失败不能输出一个伪完整备份。 */
    @Test void boundaryMismatchAndSinkFailurePropagate() throws Exception {
        var store = new MysqlWalMaterializedStore(source); store.initialize(UUID.randomUUID(), "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value"))));
        var boundary = store.apply(List.of(commit));
        assertThrows(WalMvccException.class, () -> store.exportSnapshot(new WalCheckpoint(2, commit.digest()), row -> { }));
        assertThrows(java.io.IOException.class, () -> store.exportSnapshot(boundary, row -> { throw new java.io.IOException("backup disk full"); }));
        assertEquals(boundary, store.checkpoint());
    }

    /** 旧版五列绑定表安全增加恢复状态，已有数据和正常前缀保持原义。 */
    @Test void legacyBindingSchemaMigratesWithoutChangingData() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("create table azir_wal_binding (id tinyint primary key,engine_id char(36) not null,shard_id varbinary(128) not null,applied_seq bigint not null,digest char(64) not null) engine=InnoDB");
            // JDBC metadata 的下划线是模式通配符，另一个相似名字的表不能冒充本表的恢复字段。
            statement.executeUpdate("create table azirXwalYbinding (restore_id char(64),restoring tinyint) engine=InnoDB");
            statement.executeUpdate("insert into azirXwalYbinding values ('unrelated',1)");
        }
        var store = new MysqlWalMaterializedStore(source);
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "value"); runtime.flush();
            var rows = new ArrayList<WalBackupRow>(); store.exportSnapshot(store.checkpoint(), rows::add);
            assertEquals(1, rows.size()); assertEquals("value", text(rows.getFirst().version().value()));
        }
        try (var connection = source.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select restore_id,restoring from azirXwalYbinding")) {
            assertTrue(row.next()); assertEquals("unrelated", row.getString(1)); assertEquals(1, row.getInt(2));
        }
    }

    /** 完成恢复的真实数据库提交确认丢失时，同一备份可确认已完成并安全重试。 */
    @Test void finishCommitResponseLossCanBeConfirmedAndRetried() throws Exception {
        var armed = new java.util.concurrent.atomic.AtomicBoolean();
        javax.sql.DataSource uncertain = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class},
                (proxy, method, args) -> {
                    Object result = delegated(target, method, args);
                    if (!method.getName().equals("getConnection")) { return result; }
                    return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Connection.class}, (connection, operation, values) -> {
                        Object returned = delegated(result, operation, values);
                        if (operation.getName().equals("commit") && armed.compareAndSet(true, false)) { throw new java.sql.SQLException("lost restore commit response", "08S01"); }
                        return returned;
                    });
                });
        var spec = new WalRestoreSpec(UUID.randomUUID(), UUID.randomUUID(), "a", new WalCheckpoint(1, "2".repeat(64)));
        var rows = List.of(new WalBackupRow("k", new WalValue(1, bytes("value")))); var store = new MysqlWalMaterializedStore(uncertain);
        store.beginRestore(spec); store.restoreBatch(spec, rows); armed.set(true);
        assertThrows(WalMvccException.class, () -> store.finishRestore(spec, 1, digest(spec, rows)));
        assertEquals(spec.boundary(), store.checkpoint());
        var resumed = new MysqlWalMaterializedStore(target); assertFalse(resumed.beginRestore(spec)); resumed.finishRestore(spec, 1, digest(spec, rows));
        assertEquals("value", text(resumed.get("k", 1).orElseThrow().value()));
    }

    /** 代理必须保留原始 JDBC 异常，让真实提交后响应失败进入相同恢复路径。 */
    private static Object delegated(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }

    /** 使用与真实备份文件相同的格式计算摘要，确保确认覆盖全部物理行及其身份。 */
    private static String digest(WalRestoreSpec spec, List<WalBackupRow> rows) throws Exception {
        var bytes = new java.io.ByteArrayOutputStream(); var writer = new WalBackupData.Writer(bytes, new WalBackupData.Header(spec.engineId(), spec.shard(), spec.boundary()));
        for (var row : rows) { writer.accept(row); } writer.finish(); return WalCodec.digest(bytes.toByteArray());
    }
}

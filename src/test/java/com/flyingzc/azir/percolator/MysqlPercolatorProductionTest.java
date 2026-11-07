package com.flyingzc.azir.percolator;

import com.flyingzc.azir.common.*;
import com.flyingzc.azir.maintenance.MaintenanceOptions;
import com.flyingzc.azir.sql.*;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.*;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.lang.reflect.*;
import java.util.concurrent.atomic.AtomicBoolean;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import static org.junit.jupiter.api.Assertions.*;

/** 在真实独立分片上验收生产入口的快照租约、GC、恢复及持久身份保护。 */
@Testcontainers
class MysqlPercolatorProductionTest {
    @Container static final MySQLContainer<?> A = new MySQLContainer<>("mysql:8.4").withDatabaseName("pc_production").withUsername("test").withPassword("test");
    @Container static final MySQLContainer<?> B = new MySQLContainer<>("mysql:8.4").withDatabaseName("pc_production").withUsername("test").withPassword("test");
    private static DataSource a;
    private static DataSource b;
    private static final ShardResolver ROUTES = ShardResolver.hashByKey(List.of("a", "b"));
    /** 创建调用方拥有的池，全部生产入口共用同一组持久路由。 */
    @BeforeAll static void open() {
        a = MysqlDataSourceFactory.create(A.getJdbcUrl(), A.getUsername(), A.getPassword());
        b = MysqlDataSourceFactory.create(B.getJdbcUrl(), B.getUsername(), B.getPassword());
    }
    /** 所有维护线程退出之后才关闭池。 */
    @AfterAll static void close() throws Exception { ((AutoCloseable) a).close(); ((AutoCloseable) b).close(); }

    /** 跨客户端 GC 保护 RR 与固定 RC 页，再物理回收旧载荷并保留原提交查询。 */
    @Test void gcProtectsSnapshotsAcrossClientsAndRestart() throws Exception {
        String key = "history:" + UUID.randomUUID();
        PercolatorRuntime.Commit original;
        long floor;
        try (var writer = engine(); var collector = engine()) {
            original = write(writer.runtime(), key, "v0");
            try (var rr = writer.runtime().begin(IsolationLevel.REPEATABLE_READ); var rc = writer.runtime().begin(IsolationLevel.READ_COMMITTED)) {
                var old = rr.statement(); var statement = rc.statement();
                for (int i = 1; i <= 5; i++) { write(writer.runtime(), key, "v" + i); }
                floor = collector.runtime().maintain(1).safePoint();
                assertTrue(floor < rr.startTs());
                assertEquals("v0", text(old.get("a", key))); assertEquals("v0", text(statement.get("b", key)));
                assertEquals("v0", new String(statement.scanPage("a", new KeyRange(key, key + "~"), null, 1).entries().getFirst().value(), StandardCharsets.UTF_8));
                assertEquals("v5", text(rc.statement().get("a", key)));
            }
            for (int i = 0; i < 20; i++) {
                var result = collector.runtime().maintain(1);
                assertTrue(result.safePoint() >= floor); floor = result.safePoint(); assertTrue(result.deletedVersions() <= 2);
            }
            assertEquals(1, count(a, key)); assertEquals(1, count(b, key));
        }
        try (var restarted = engine(); var tx = restarted.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
            assertEquals("v5", text(tx.get("a", key)));
            assertEquals(original.commitTs(), restarted.runtime().lookup(original.handle()).commitTs());
            assertTrue(restarted.runtime().maintain(1).safePoint() >= floor);
        }
    }
    /** 删除墓碑必须留下，空字节业务值也不能被当作墓碑删除。 */
    @Test void gcRetainsDeleteAndEmptyValue() throws Exception {
        String key = "tombstone:" + UUID.randomUUID();
        try (var engine = engine()) {
            write(engine.runtime(), key, "before");
            try (var tx = engine.runtime().begin(IsolationLevel.REPEATABLE_READ)) { tx.delete("a", key); tx.put("b", key, new byte[0]); tx.commit(); }
            for (int i = 0; i < 10; i++) { engine.runtime().maintain(128); }
            assertEquals(0, count(a, key)); assertEquals(1, count(b, key));
        }
        try (var engine = engine(); var tx = engine.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
            assertTrue(tx.get("a", key).isEmpty()); assertArrayEquals(new byte[0], tx.get("b", key).orElseThrow());
            assertNull(tx.scanPage("a", new KeyRange(key, key + "~"), null, 1).entries().getFirst().value());
        }
    }
    /** 租约过期后旧语句、自己的缓冲值、分页、预写和提交全部拒绝。 */
    @Test void expiredReadersCannotOutliveGcOrPublishLateWrites() throws Exception {
        String key = "expired:" + UUID.randomUUID();
        try (var engine = engine(); var tx = engine.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
            var view = tx.statement(); tx.put("a", key, bytes("private"));
            try (var c = a.getConnection(); var q = c.prepareStatement("update pc_readers set expires_at=0 where start_ts=?")) { q.setLong(1, tx.startTs()); assertEquals(1, q.executeUpdate()); }
            engine.runtime().maintain(128);
            assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, assertThrows(MvccException.class, () -> view.get("a", key)).errorCode());
            assertThrows(MvccException.class, () -> view.scanPage("a", new KeyRange(null, null), null, 1));
            assertThrows(MvccException.class, () -> tx.put("b", key, bytes("late")));
            assertThrows(MvccException.class, tx::prewrite); assertThrows(MvccException.class, tx::commit);
            tx.rollback(); assertEquals(0, count(a, key));
        }
    }
    /** 新凭据必须含持久命名空间，旧式或伪造空间不能在另一权威上误确认。 */
    @Test void namespaceAndRoutesCannotBeConfused() {
        String key = "identity:" + UUID.randomUUID();
        try (var engine = engine()) {
            var receipt = write(engine.runtime(), key, "saved");
            assertNotNull(receipt.handle().namespace());
            assertThrows(IllegalArgumentException.class, () -> engine.runtime().lookup(new PercolatorRuntime.Handle(receipt.handle().startTs(), receipt.handle().primary())));
            assertThrows(IllegalArgumentException.class, () -> engine.runtime().lookup(new PercolatorRuntime.Handle(receipt.handle().startTs(), receipt.handle().primary(), UUID.randomUUID().toString())));
            assertThrows(IllegalArgumentException.class, () -> PercolatorSqlEngine.mysql(a, Map.of("a", b, "b", a), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED));
            assertThrows(IllegalArgumentException.class, () -> PercolatorSqlEngine.mysql(b, Map.of("a", a, "b", b), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED));
        }
    }
    /** 已绑定空间丢失 TSO 时拒绝从初始值重新发号。 */
    @Test void missingTimestampRefusesReinitialization() throws Exception {
        long sequence;
        try (var ignored = engine(); var c = a.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select current_value from mvcc_tso where sequence_name='global'")) { r.next(); sequence = r.getLong(1); }
        try (var c = a.getConnection(); var q = c.createStatement()) { q.executeUpdate("delete from mvcc_tso where sequence_name='global'"); }
        try { assertThrows(IllegalArgumentException.class, MysqlPercolatorProductionTest::engine); }
        finally { try (var c = a.getConnection(); var q = c.prepareStatement("insert into mvcc_tso values('global',?)")) { q.setLong(1, sequence); q.executeUpdate(); } }
    }
    /** 后台恢复必须清理失联预写；引擎关闭后会话与后台都不能继续运行。 */
    @Test void schedulerRecoversOrphansAndCloseStopsWork() throws Exception {
        String key = "orphan:" + UUID.randomUUID();
        var engine = PercolatorSqlEngine.mysql(a, Map.of("a", a, "b", b), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED,
                PercolatorRuntime.Options.defaults(), SqlExecutionOptions.defaults(), new MaintenanceOptions(20, 128, 10_000));
        PercolatorTransaction orphan = engine.runtime().begin(IsolationLevel.REPEATABLE_READ);
        PercolatorRuntime.Handle id;
        try (engine) {
            orphan.put("a", key, bytes("a")); orphan.put("b", key, bytes("b")); orphan.prewrite(); id = orphan.handle().orElseThrow();
            for (DataSource source : List.of(a, b)) {
                try (var c = source.getConnection(); var q = c.prepareStatement("update pc_lock set expires_at=0 where start_ts=?")) { q.setLong(1, orphan.startTs()); q.executeUpdate(); }
            }
            long deadline = System.nanoTime() + 10_000_000_000L;
            while ((count(a, key) > 0 || count(b, key) > 0 || engine.maintenanceHealth().orElseThrow().successes() == 0) && System.nanoTime() < deadline) { Thread.sleep(20); }
            assertEquals(0, count(a, key)); assertEquals(0, count(b, key));
            assertEquals(PercolatorStore.State.ROLLED_BACK, engine.runtime().lookup(id).state());
            assertTrue(engine.maintenanceHealth().orElseThrow().successes() > 0);
        }
        long rounds = engine.maintenanceHealth().orElseThrow().successes(); Thread.sleep(60);
        assertEquals(rounds, engine.maintenanceHealth().orElseThrow().successes());
        assertThrows(IllegalStateException.class, engine::openSession);
        assertThrows(IllegalStateException.class, () -> orphan.get("a", key));
    }
    /** 预写结束时失去控制会话后，旧调用不得另借连接分配提交号并发布 primary。 */
    @Test void lostGateCannotResumeTheNextCommitPhase() throws Exception {
        try (var initialized = engine()) { }
        var lifecycle = new MysqlPercolatorLifecycle(a, Map.of("a", a, "b", b), 60_000);
        var rawA = new MysqlPercolatorStore("a", a); var rawB = new MysqlPercolatorStore("b", b);
        AtomicBoolean killed = new AtomicBoolean();
        PercolatorStore faultyB = (PercolatorStore) Proxy.newProxyInstance(PercolatorStore.class.getClassLoader(), new Class<?>[]{PercolatorStore.class}, (proxy, method, args) -> {
            try {
                Object result = method.invoke(rawB, args);
                if (method.getName().equals("prewrite") && killed.compareAndSet(false, true)) {
                    long owner;
                    try (var c = a.getConnection(); var q = c.prepareStatement("select is_used_lock(?)")) {
                        q.setString(1, "azir:pc:" + lifecycle.namespace()); try (var r = q.executeQuery()) { r.next(); owner = r.getLong(1); }
                    }
                    assertTrue(owner > 0);
                    try (var c = a.getConnection(); var q = c.createStatement()) { q.execute("kill connection " + owner); }
                }
                return result;
            } catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
        String key = "fenced:" + UUID.randomUUID();
        try (var runtime = new PercolatorRuntime(new MysqlTimestampOracle(a), Map.of("a", rawA, "b", faultyB), PercolatorRuntime.Options.defaults(), lifecycle);
             var tx = runtime.begin(IsolationLevel.REPEATABLE_READ)) {
            tx.put("a", key, bytes("unpublished")); tx.put("b", key, bytes("unpublished"));
            assertThrows(MvccException.class, tx::commit);
            assertNotEquals(PercolatorTransaction.State.COMMITTED, tx.state());
            var id = tx.handle().orElseThrow();
            assertNotEquals(PercolatorStore.State.COMMITTED, runtime.lookup(id).state());
            tx.rollback(); assertEquals(PercolatorStore.State.ROLLED_BACK, runtime.lookup(id).state());
            assertEquals(0, count(a, key)); assertEquals(0, count(b, key));
        }
    }

    /** 默认生产工厂使用持久读租约和自动维护。 */
    private static PercolatorSqlEngine engine() { return PercolatorSqlEngine.mysql(a, Map.of("a", a, "b", b), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED); }
    /** 两个分片提交同一业务值。 */
    private static PercolatorRuntime.Commit write(PercolatorRuntime runtime, String key, String value) {
        try (var tx = runtime.begin(IsolationLevel.REPEATABLE_READ)) { tx.put("a", key, bytes(value)); tx.put("b", key, bytes(value)); return tx.commit(); }
    }
    /** 核对实际载荷数量，write 决议允许继续保留。 */
    private static int count(DataSource source, String key) throws Exception {
        try (var c = source.getConnection(); var q = c.prepareStatement("select count(*) from pc_data where user_key=?")) {
            q.setBytes(1, bytes(key)); try (var r = q.executeQuery()) { r.next(); return r.getInt(1); }
        }
    }
    /** 固定文本编码用于物理主键与业务值。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    /** 解码可见业务值。 */
    private static String text(Optional<byte[]> value) { return new String(value.orElseThrow(), StandardCharsets.UTF_8); }
}

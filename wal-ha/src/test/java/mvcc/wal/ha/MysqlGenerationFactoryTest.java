package mvcc.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.wal.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;
import org.apache.ratis.protocol.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 MySQL 代次分配、故障续做、未知空间拒绝和并发额度验证。 */
class MysqlGenerationFactoryTest {
    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();
    private HikariDataSource control;
    @TempDir Path directory;

    /** 测试类只创建一个限资源实例，不连接用户已有数据库。 */
    @BeforeAll static void startDatabase() { DATABASE.start(); }

    /** 每项测试创建独立控制库，避免修改上一项已登记的空间。 */
    @BeforeEach void prepareControl() throws Exception {
        String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASE, schema); control = MysqlGenerationSupport.control(DATABASE, schema);
    }

    /** 只关闭当前测试拥有的控制连接池。 */
    @AfterEach void closeControl() { if (control != null) { control.close(); } }

    /** 测试完成移除唯一自有容器。 */
    @AfterAll static void stopDatabase() { DATABASE.stop(); }

    /** 同一代次跨工厂重开保持数据，新代次隔离；关闭租约不关闭控制池或删除旧数据。 */
    @Test void generationsRemainStableIsolatedAndPreserveCallerControls() throws Exception {
        var identity = identity(); var first = UUID.randomUUID(); var second = UUID.randomUUID(); var factory = factory();
        try (var lease = factory.open(identity, first)) {
            var store = lease.stores().get("a"); store.initializeReplica(identity, "a"); store.apply(List.of(new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", new byte[]{7})))));
        }
        assertFalse(control.isClosed()); assertEquals(factory.databaseName(identity, first, "a"), factory().databaseName(identity, first, "a"));
        try (var reopened = factory().open(identity, first); var separate = factory.open(identity, second)) {
            reopened.stores().get("a").initializeReplica(identity, "a"); assertArrayEquals(new byte[]{7}, reopened.stores().get("a").get("k", 1).orElseThrow().value());
            separate.stores().get("a").initializeReplica(identity, "a"); assertTrue(separate.stores().get("a").get("k", Long.MAX_VALUE).isEmpty());
            assertNotEquals(factory.databaseName(identity, first, "a"), factory.databaseName(identity, second, "a"));
        }
        assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /** 未登记的空库和含业务表的库都拒绝接管，原内容和分配额度不变。 */
    @Test void preexistingUnknownDatabasesCannotBeAdoptedEvenWhenEmpty() throws Exception {
        var identity = identity(); var factory = factory();
        for (boolean withData : List.of(false, true)) {
            UUID generation = UUID.randomUUID(); String schema = factory.databaseName(identity, generation, "a"); execute("create database `" + schema + "`");
            if (withData) { execute("create table `" + schema + "`.business_data (id int primary key) engine=InnoDB"); execute("insert into `" + schema + "`.business_data values (7)"); }
            assertThrows(IOException.class, () -> factory.open(identity, generation)); assertTrue(databaseExists(schema));
            if (withData) { assertEquals(7, scalar("select id from `" + schema + "`.business_data")); }
        }
        assertEquals(0, scalar("select count(*) from azir_wal_generation_catalog")); assertEquals(0, scalar("select coalesce(sum(used_slots),0) from azir_wal_namespace_budget"));
    }

    /** READY 后数据库或身份标记丢失只能拒绝，不能将已经用过的代次重建为空库。 */
    @Test void readyDatabaseLossAndWrongOwnerFailClosed() throws Exception {
        var identity = identity(); var generation = UUID.randomUUID(); var factory = factory(); String schema = factory.databaseName(identity, generation, "a");
        try (var lease = factory.open(identity, generation)) { lease.stores().get("a").initializeReplica(identity, "a"); }
        execute("update `" + schema + "`.azir_wal_generation set owner='" + "0".repeat(64) + "'");
        assertThrows(IOException.class, () -> factory.open(identity, generation)); assertEquals(1, scalar("select state from azir_wal_generation_catalog"));
        execute("update `" + schema + "`.azir_wal_generation set owner='" + MysqlWalReplicaBackendFactory.owner(identity, generation, "a", schema) + "'");
        execute("drop table `" + schema + "`.azir_wal_version"); assertThrows(IOException.class, () -> factory.open(identity, generation));
        execute("drop database `" + schema + "`"); assertThrows(IOException.class, () -> factory.open(identity, generation)); assertFalse(databaseExists(schema));
    }

    /** 并发预留也不能突破预算，拒绝在登记事务中发生，成功租约仍可独立关闭。 */
    @Test void concurrentAllocationCannotOversubscribeGenerationAndSchemaBudgets() throws Exception {
        var identity = identity(); var factory = new MysqlWalReplicaBackendFactory(Map.of("a", control), new MysqlWalReplicaBackendFactory.Options("azw", 2, 2, 1)); int success = 0;
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var jobs = new ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < 3; i++) { jobs.add(tasks.submit(() -> { try (var ignored = factory.open(identity, UUID.randomUUID())) { return true; } catch (IOException rejected) { return false; } })); }
            for (var job : jobs) { if (job.get(30, java.util.concurrent.TimeUnit.SECONDS)) { success++; } }
        }
        assertEquals(2, success); assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget")); assertEquals(2, scalar("select count(*) from azir_wal_generation_catalog")); assertFalse(control.isClosed());
    }

    /** 错误事务引擎不能被同名控制表掩盖，不允许先创建目标空间再发现元数据不可靠。 */
    @Test void invalidControlSchemaIsRejectedBeforeTargetCreation() throws Exception {
        execute("create table azir_wal_namespace_budget (prefix varbinary(8) primary key,protocol_version int not null,max_slots int not null,used_slots int not null) engine=MyISAM");
        var identity = identity(); var generation = UUID.randomUUID(); var factory = factory(); String schema = factory.databaseName(identity, generation, "a");
        assertThrows(IOException.class, () -> factory.open(identity, generation)); assertFalse(databaseExists(schema));
    }

    /** 额度计数被误改时拒绝分配，不能让计数漂移成为无限创建数据库的入口。 */
    @Test void corruptedBudgetCounterCannotAuthorizeMoreDatabases() throws Exception {
        var identity = identity(); var factory = factory(); try (var ignored = factory.open(identity, UUID.randomUUID())) { }
        execute("update azir_wal_namespace_budget set used_slots=0"); UUID next = UUID.randomUUID();
        assertThrows(IOException.class, () -> factory.open(identity, next)); assertFalse(databaseExists(factory.databaseName(identity, next, "a")));
    }

    /** READY 后首次事务表 DDL 中断但没有任何绑定或数据，可以由相同代次完成初始化。 */
    @Test void uninitializedPartialProtocolDdlCanResumeSameGeneration() throws Exception {
        var identity = identity(); UUID generation = UUID.randomUUID(); var factory = factory(); String schema = factory.databaseName(identity, generation, "a");
        try (var lease = factory.open(identity, generation)) { lease.stores().get("a").initializeReplica(identity, "a"); }
        execute("delete from `" + schema + "`.azir_wal_binding"); execute("drop table `" + schema + "`.azir_wal_version");
        try (var lease = factory.open(identity, generation)) { lease.stores().get("a").initializeReplica(identity, "a"); assertEquals(0, lease.stores().get("a").checkpoint().sequence()); }
        assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /** 首次节点装配在工厂 READY 后失败，重试沿用原代次；参与过共识的身份仍不能丢投票文件重开。 */
    @Test void initialNodeProvisionFailureKeepsStableBootstrapWithoutWeakeningVotingFence() throws Exception {
        var groupId = RaftGroupId.randomId(); var peers = new ArrayList<RaftPeer>(); var held = new ArrayList<java.net.ServerSocket>();
        try { for (int i = 0; i < 3; i++) { var socket = new java.net.ServerSocket(0); held.add(socket); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build()); } }
        finally { for (var socket : held) { socket.close(); } }
        var group = RaftGroup.valueOf(groupId, peers); var armed = new AtomicBoolean(true);
        var broken = new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults(), hit -> {
            if (hit == MysqlWalReplicaBackendFactory.Stage.AFTER_READY && armed.compareAndSet(true, false)) { throw new IOException("initial provision interrupted"); }
        });
        Path node = directory.resolve("node"); var limits = new WalBackupOptions(8_000_000, 8, 64_000, 60_000);
        assertThrows(IOException.class, () -> RatisWalNode.openGenerationalLoopback(node, RaftPeerId.valueOf("n0"), group, Set.of("a"), broken, WalOptions.defaults(), limits));
        var bootstrap = RatisGenerationRecord.read(node.resolve("business/BOOTSTRAPPING")); assertFalse(Files.exists(node.resolve("business/ACTIVE")));
        try (var started = RatisWalNode.openGenerationalLoopback(node, RaftPeerId.valueOf("n0"), group, Set.of("a"), factory(), WalOptions.defaults(), limits)) {
            assertTrue(started.ready()); assertEquals(bootstrap.generation(), RatisGenerationRecord.read(node.resolve("business/ACTIVE")).generation());
            assertFalse(Files.exists(node.resolve("business/BOOTSTRAPPING"))); assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
        }
        Path voting = node.resolve("raft").resolve(groupId.getUuid().toString()).resolve("current/raft-meta"); Files.delete(voting);
        assertThrows(IOException.class, () -> RatisWalNode.openGenerationalLoopback(node, RaftPeerId.valueOf("n0"), group, Set.of("a"), factory(), WalOptions.defaults(), limits)); assertFalse(Files.exists(voting));
    }

    /** 登记、DDL、身份标记、READY 和池初始化后任一失败，原代次重试不重复占用额度。 */
    @ParameterizedTest @EnumSource(MysqlWalReplicaBackendFactory.Stage.class)
    void provisioningFaultResumesSameOwnedNamespace(MysqlWalReplicaBackendFactory.Stage stage) throws Exception {
        var identity = identity(); var generation = UUID.randomUUID(); var armed = new AtomicBoolean(true);
        var broken = new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults(), hit -> { if (hit == stage && armed.compareAndSet(true, false)) { throw new IOException("injected provision boundary " + stage); } });
        assertThrows(IOException.class, () -> broken.open(identity, generation)); assertFalse(control.isClosed());
        try (var restored = factory().open(identity, generation)) { restored.stores().get("a").initializeReplica(identity, "a"); assertEquals(0, restored.stores().get("a").checkpoint().sequence()); }
        assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget")); assertEquals(1, scalar("select count(*) from azir_wal_generation_catalog"));
        String schema = broken.databaseName(identity, generation, "a");
        try (var connection = control.getConnection(); var query = connection.prepareStatement("select count(*) from information_schema.processlist where db=?")) {
            query.setString(1, schema); try (var row = query.executeQuery()) { assertTrue(row.next()); assertEquals(0, row.getInt(1), "all factory-owned business pools must close"); }
        }
    }

    /** 每个场景拥有独立业务引擎和节点身份。 */
    private static WalReplicaIdentity identity() { UUID group = UUID.randomUUID(); return new WalReplicaIdentity(group, group, "factory-node"); }

    /** 当前专项只需要一片，集群专项另验证完整双片工厂。 */
    private MysqlWalReplicaBackendFactory factory() { return new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults()); }

    /** 仅执行本测试构造的固定 DDL，动态名字全部来自工厂或 UUID。 */
    private void execute(String sql) throws Exception { try (var connection = control.getConnection(); var statement = connection.createStatement()) { statement.executeUpdate(sql); } }

    /** 原始 SQL 核对独立于工厂返回值，避免用自己更新的内存计数证明额度。 */
    private int scalar(String sql) throws Exception { try (var connection = control.getConnection(); var query = connection.createStatement(); var row = query.executeQuery(sql)) { assertTrue(row.next()); return row.getInt(1); } }

    /** 直接核对目标是否存在，丢失 READY 空间后不允许补建。 */
    private boolean databaseExists(String schema) throws Exception {
        try (var connection = control.getConnection(); var query = connection.prepareStatement("select count(*) from information_schema.schemata where cast(schema_name as binary)=?")) { query.setBytes(1, schema.getBytes(StandardCharsets.US_ASCII)); try (var row = query.executeQuery()) { row.next(); return row.getInt(1) == 1; } }
    }
}

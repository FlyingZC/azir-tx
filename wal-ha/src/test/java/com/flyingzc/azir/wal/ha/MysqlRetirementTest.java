package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 MySQL 退休、额度恰好释放一次、租约保护和不复活专项。 */
class MysqlRetirementTest {
    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();
    private HikariDataSource control;
    /** 仅创建一个自有受限数据库。 */
    @BeforeAll static void start() { DATABASE.start(); }
    /** 每项操作在新的控制库中登记。 */
    @BeforeEach void prepare() throws Exception { String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASE, schema); control = MysqlGenerationSupport.control(DATABASE, schema); }
    /** 只关闭本测试控制池。 */
    @AfterEach void close() { control.close(); }
    /** 移除唯一自有容器。 */
    @AfterAll static void stop() { DATABASE.stop(); }

    /** 分片游标限制每轮一库，重试墓碑不重复释放，旧代次永不再被初始化为空。 */
    @Test void cursorReleasesQuotaExactlyOnceAndPreventsResurrection() throws Exception {
        var identity = identity(); UUID old = UUID.randomUUID(); var factory = factory(); try (var ignored = factory.open(identity, old)) { }
        assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget")); assertThrows(IOException.class, () -> factory.open(identity, UUID.randomUUID()));
        var permit = permit(identity, old); var first = factory.retire(permit, "", 1); assertFalse(first.complete()); assertEquals(1, first.inspected()); assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
        assertTrue(factory.retire(permit, first.cursor(), 1).complete()); assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget"));
        assertTrue(factory.retire(permit, "", 2).complete()); assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget")); assertThrows(IOException.class, () -> factory.open(identity, old));
        try (var ignored = factory.open(identity, UUID.randomUUID())) { } assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /** 不同工厂对象也必须看见活动租约，许可在离开节点维护上下文后不能复用。 */
    @Test void activeLeaseAndRevokedPermitCannotDeleteSchemas() throws Exception {
        var identity = identity(); UUID generation = UUID.randomUUID(); var factory = factory();
        try (var lease = factory.open(identity, generation)) { assertThrows(IOException.class, () -> factory().retire(permit(identity, generation), "", 2)); assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget")); }
        var revoked = new WalGenerationRetirement(identity, generation, () -> { throw new IOException("permit revoked"); }); assertThrows(IOException.class, () -> factory.retire(revoked, "", 2));
        assertEquals(2, scalar("select count(*) from azir_wal_generation_catalog where state=1"));
    }

    /** 未登记库、错误归属和新增未知业务表均不能被 DROP。 */
    @Test void unknownOrWrongOwnedDatabaseCannotBeDropped() throws Exception {
        var identity = identity(); UUID generation = UUID.randomUUID(); var factory = factory(); String schema = factory.databaseName(identity, generation, "a"); execute("create database `" + schema + "`"); execute("create table `" + schema + "`.important (id int primary key)"); execute("insert into `" + schema + "`.important values(7)");
        assertThrows(IOException.class, () -> factory.retire(permit(identity, generation), "", 2)); assertEquals(7, scalar("select id from `" + schema + "`.important"));
        UUID owned = UUID.randomUUID(); try (var ignored = factory.open(identity, owned)) { } String target = factory.databaseName(identity, owned, "a"); execute("update `" + target + "`.azir_wal_generation set owner='" + "0".repeat(64) + "'");
        assertThrows(IOException.class, () -> factory.retire(permit(identity, owned), "", 2)); assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /** 退休状态、DROP 和额度提交任一边界失败均能从同一身份续做。 */
    @ParameterizedTest @EnumSource(MysqlWalReplicaBackendFactory.RetirementStage.class)
    void interruptionPreservesTombstoneAndBudgetInvariants(MysqlWalReplicaBackendFactory.RetirementStage stage) throws Exception {
        var identity = identity(); UUID generation = UUID.randomUUID(); try (var ignored = factory().open(identity, generation)) { }
        var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
        var broken = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), options(), hit -> { }, hit -> { if (hit == stage && armed.compareAndSet(true, false)) { throw new IOException("injected retirement " + stage); } });
        assertThrows(IOException.class, () -> broken.retire(permit(identity, generation), "", 1)); assertThrows(IOException.class, () -> factory().open(identity, generation));
        assertTrue(factory().retire(permit(identity, generation), "", 2).complete()); assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget")); assertEquals(2, scalar("select count(*) from azir_wal_generation_catalog where state=3"));
        assertTrue(factory().retire(permit(identity, generation), "", 2).complete()); assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /** 单元测试直接签发能力验证后端协议，真实当前代次保护由节点集成另验。 */
    private static WalGenerationRetirement permit(WalReplicaIdentity identity, UUID generation) { return new WalGenerationRetirement(identity, generation, () -> { }); }
    /** 每片仅允许一代，使额度释放成为继续分配的必要条件。 */
    private static MysqlWalReplicaBackendFactory.Options options() { return new MysqlWalReplicaBackendFactory.Options("azw", 1, 2, 1); }
    /** 两片共用一个控制库，但物理命名空间不同。 */
    private MysqlWalReplicaBackendFactory factory() { return new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), options()); }
    /** 场景使用独立引擎身份。 */
    private static WalReplicaIdentity identity() { UUID group = UUID.randomUUID(); return new WalReplicaIdentity(group, group, "retire-node"); }
    /** 所有 SQL 都由测试构造，数据库名来自正式工厂。 */
    private void execute(String sql) throws Exception { try (var connection = control.getConnection(); var statement = connection.createStatement()) { statement.executeUpdate(sql); } }
    /** 原始 SQL 计数独立于工厂返回值。 */
    private int scalar(String sql) throws Exception { try (var connection = control.getConnection(); var query = connection.createStatement(); var row = query.executeQuery(sql)) { assertTrue(row.next()); return row.getInt(1); } }
}

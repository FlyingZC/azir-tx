package mvcc.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 两个受限数据库上的四个专属 schema 验证冷数据迁移、接收所有权和部分恢复隔离。 */
@Testcontainers
class WalReplicaSnapshotMysqlTest {
    @Container static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();
    @Container static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;
    private HikariDataSource a, b, targetA, targetB;

    /** 只在本测试容器中创建接收空间，不清空或迁移已有业务数据库。 */
    @BeforeAll static void createTargets() throws Exception {
        for (var container : List.of(A, B)) {
            var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                    "create database wal_replica_receive; grant all on wal_replica_receive.* to 'wal_test'@'%'"); assertEquals(0, result.getExitCode(), result.getStderr());
        }
    }

    /** 每项测试重置自有四套表，不能重用上一项的所有权和恢复指纹。 */
    @BeforeEach void prepare() throws Exception {
        a = WalCheckpointMysqlSupport.pool(A); b = WalCheckpointMysqlSupport.pool(B); targetA = target(A); targetB = target(B);
        for (var pool : List.of(a, b, targetA, targetB)) {
            try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
                ddl.executeUpdate("drop table if exists azir_wal_version"); ddl.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /** 连接池只由本测试拥有，容器由测试生命周期回收。 */
    @AfterEach void closePools() { for (var pool : new HikariDataSource[]{a, b, targetA, targetB}) { if (pool != null) { pool.close(); } } }

    /** 源热点缓存和本地尾日志均已排空，接收方恢复冷数据、墓碑及 UUID 后继续提交。 */
    @Test void fullColdStateAndReceiptsMoveToReceiversOwnBackendIdentity() throws Exception {
        var source = WalReplicaSnapshotTest.identity("source"); var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path backup = directory.resolve("backup"); var authority = new WalReplicaSnapshotTest.Authority(); WalReceipt first; WalReceipt deleted;
        try (var runtime = open(directory.resolve("source"), source, sources(), authority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 64; i++) { for (String shard : Set.of("a", "b")) { runtime.put(tx, shard, "k" + i, bytes(shard + ":" + i)); } }
            first = runtime.commit(tx); var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : Set.of("a", "b")) { runtime.delete(deletion, shard, "k0"); } deleted = runtime.commit(deletion);
            assertEquals(Map.of("a", 64L, "b", 64L), runtime.backupReplica(authority, backup, limits()).rows()); assertEquals(0, runtime.stats().retainedBytes()); assertEquals(100, runtime.stats().walBytes());
        }
        a.close(); b.close(); var targets = targets(); Path restored = directory.resolve("restored");
        WalBackup.restoreReplica(backup, restored, targets, options(), limits(), receiver);
        for (var pool : List.of(targetA, targetB)) {
            try (var connection = pool.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select replica_owner,restoring,applied_seq from azir_wal_binding where id=1")) {
                assertTrue(row.next()); assertEquals(receiver.fingerprint(), row.getString(1)); assertFalse(row.getBoolean(2)); assertEquals(2, row.getLong(3));
            }
        }
        var receiverAuthority = new WalReplicaSnapshotTest.Authority();
        try (var runtime = open(restored, receiver, targets, receiverAuthority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var view = runtime.beginReadStatement(tx);
            for (int i = 0; i < 64; i++) { for (String shard : Set.of("a", "b")) {
                if (i == 0) { assertTrue(runtime.get(tx, shard, "k0", view).isEmpty()); }
                else { assertEquals(shard + ":" + i, text(runtime.get(tx, shard, "k" + i, view).orElseThrow())); }
            } }
            runtime.rollback(tx); assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow()); assertEquals(deleted, runtime.findCommitted(deleted.transactionId()).orElseThrow());
            assertEquals(3, WalReplicaSnapshotTest.pair(runtime, "next").commitSequence()); runtime.flush();
        }
        assertThrows(IllegalStateException.class, () -> targets.get("a").initialize(source.engineId(), "a"));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(targetA).initializeReplica(source, "a"));
    }

    /** 批次中断保留恢复中和接收身份，错误节点及重复目录不能接管，原操作可安全续做。 */
    @Test void partialColdRestoreRejectsWrongOwnerAndResumesOnlySameOperation() throws Exception {
        var source = WalReplicaSnapshotTest.identity("source"); var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path backup = directory.resolve("backup"); var authority = new WalReplicaSnapshotTest.Authority();
        try (var runtime = open(directory.resolve("source"), source, sources(), authority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); for (int i = 0; i < 8; i++) { for (String shard : Set.of("a", "b")) { runtime.put(tx, shard, "k" + i, bytes(shard + ":" + i)); } }
            runtime.commit(tx); runtime.backupReplica(authority, backup, limits());
        }
        Path target = directory.resolve("restored"); var targets = targets();
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreReplicaForTest(backup, target, targets, options(), limits(), receiver,
                stage -> { if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_BATCH) { throw new IllegalStateException("interrupted cold import"); } }, (path, channel) -> channel.force(true)));
        assertFalse(Files.exists(target)); assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(targetA).initializeReplica(receiver, "a"));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(targetA).initialize(source.engineId(), "a"));
        var other = new WalReplicaIdentity(source.engineId(), source.groupId(), "other");
        assertThrows(java.io.IOException.class, () -> WalBackup.restoreReplica(backup, target, targets(), options(), limits(), other));
        WalBackup.restoreReplica(backup, target, targets(), options(), limits(), receiver);
        assertThrows(WalMvccException.class, () -> WalBackup.restoreReplica(backup, directory.resolve("duplicate-target"), targets(), options(), limits(), receiver));
        var restoredAuthority = new WalReplicaSnapshotTest.Authority();
        try (var runtime = open(target, receiver, targets(), restoredAuthority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); assertEquals("a:7", text(runtime.get(tx, "a", "k7").orElseThrow())); assertEquals("b:7", text(runtime.get(tx, "b", "k7").orElseThrow())); runtime.rollback(tx);
        }
    }

    /** 副本缓存禁用，避免热数据掩盖后端镜像导出或恢复缺失。 */
    private static WalOptions options() { return WalOptions.defaults().withHotCacheBytes(0); }

    /** 两行一批使真实恢复必经多个有限批次。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(16_000_000, 2, 64_000, 60_000); }

    /** 为当前场景装配独立后端并绑定测试用可信端口。 */
    private static WalMvccRuntime open(Path path, WalReplicaIdentity identity, Map<String, WalMaterializedStore> stores, WalReplicaSnapshotTest.Authority authority) {
        var runtime = WalMvccRuntime.openReplica(path, Set.of("a", "b"), stores, options(), WalRetentionOptions.preserveAll(), identity, authority); authority.runtime = runtime; return runtime;
    }

    /** 源节点两片各有自己的逻辑数据库。 */
    private Map<String, WalMaterializedStore> sources() { return Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b)); }

    /** 每次返回新适配器对象，确保所有权由数据库校验而非对象缓存保证。 */
    private Map<String, WalMaterializedStore> targets() { return Map.of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB)); }

    /** 接收连接池只连接本测试明确创建的 schema，沿用有限连接和 socket 超时。 */
    private static HikariDataSource target(MySQLContainer<?> container) {
        var config = new HikariConfig(); config.setJdbcUrl(container.getJdbcUrl().replace("/" + container.getDatabaseName(), "/wal_replica_receive"));
        config.setUsername(container.getUsername()); config.setPassword(container.getPassword()); config.setMaximumPoolSize(2); config.setMinimumIdle(0); config.setConnectionTimeout(5000); return new HikariDataSource(config);
    }
}

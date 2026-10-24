package mvcc.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 复制快照固定读视图后并发提交、物化、GC 与恢复边界的真实 MySQL 验证。 */
@Testcontainers
class WalReplicaPinnedBackupTest {
    @Container static final MySQLContainer<?> DATABASE = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;
    private final List<HikariDataSource> pools = new ArrayList<>();
    /** 只关闭本轮拥有的连接池，测试实例由生命周期管理。 */
    @AfterEach void closePools() { for (var pool : pools) { pool.close(); } }

    /** 固定时刻之后的提交与物理 GC 不得进入镜像，也不能删除镜像应保留的数据。 */
    @Test void pinnedViewsSurviveConcurrentPublicationCheckpointAndGc() throws Exception {
        var identity = WalReplicaSnapshotTest.identity("source"); var authority = new WalReplicaSnapshotTest.Authority(); var sourceStores = stores("source"); Path backup = directory.resolve("backup"); WalReceipt first;
        try (var source = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), sourceStores, WalOptions.defaults().withHotCacheBytes(0), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = source; var initial = source.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) { source.put(initial, shard, "k", bytes("old")); source.put(initial, shard, "gone", bytes("present")); } first = source.commit(initial);
            try (var plan = source.prepareReplicaBackup(authority, backup, limits())) {
                assertEquals(1, plan.boundary().sequence()); assertTrue(source.lifecycle().backup());
                var newer = source.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { source.put(newer, shard, "k", bytes("new")); source.delete(newer, shard, "gone"); source.put(newer, shard, "future", bytes("new")); } assertEquals(2, source.commit(newer).commitSequence());
                source.flush(); for (int i = 0; i < 8; i++) { source.collectGarbageBounded(32, 128, 1000); } assertTrue(source.checkpoint().created());
                assertEquals(1, plan.complete().sequence()); assertEquals(2, source.stats().publishedSequence()); assertFalse(source.lifecycle().backup());
            }
            assertEquals(3, WalReplicaSnapshotTest.pair(source, "later").commitSequence());
        }
        var receiver = new WalReplicaIdentity(identity.engineId(), identity.groupId(), "receiver"); var targetStores = stores("target"); Path target = directory.resolve("target"); WalBackup.restoreReplica(backup, target, targetStores, WalOptions.defaults(), limits(), receiver);
        var port = new WalReplicaSnapshotTest.Authority();
        try (var restored = WalMvccRuntime.openReplica(target, Set.of("a", "b"), targetStores, WalOptions.defaults().withHotCacheBytes(0), WalRetentionOptions.preserveAll(), receiver, port)) {
            port.runtime = restored; var tx = restored.begin(IsolationLevel.REPEATABLE_READ); var view = restored.beginReadStatement(tx);
            for (String shard : List.of("a", "b")) { assertEquals("old", text(restored.get(tx, shard, "k", view).orElseThrow())); assertEquals("present", text(restored.get(tx, shard, "gone", view).orElseThrow())); assertTrue(restored.get(tx, shard, "future", view).isEmpty()); }
            restored.rollback(tx); assertEquals(first, restored.findCommitted(first.transactionId()).orElseThrow()); assertEquals(2, WalReplicaSnapshotTest.pair(restored, "receiver").commitSequence());
        }
    }

    /** 放弃尚未导出的计划不能发布 COMPLETE，连接和维护资格均可继续使用。 */
    @Test void abandoningPinnedPlanReleasesViewsAndKeepsSourceWritable() throws Exception {
        var identity = WalReplicaSnapshotTest.identity("source"); var authority = new WalReplicaSnapshotTest.Authority(); var stores = stores("abandon");
        try (var source = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), stores, WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = source; WalReplicaSnapshotTest.pair(source, "one"); var plan = source.prepareReplicaBackup(authority, directory.resolve("partial"), limits()); plan.close(); plan.close();
            assertFalse(source.lifecycle().backup()); assertFalse(Files.exists(directory.resolve("partial/COMPLETE"))); assertThrows(java.io.IOException.class, () -> WalBackup.inspect(directory.resolve("partial"), limits()));
            assertEquals(2, WalReplicaSnapshotTest.pair(source, "two").commitSequence());
            try (var retry = source.prepareReplicaBackup(authority, directory.resolve("complete"), limits())) { assertEquals(2, retry.complete().sequence()); }
        }
    }

    /** 准备预算失败要释放维护资格，后续事务与新备份仍能正常执行。 */
    @Test void preparationFailureDoesNotLeaveRuntimeInBackupMode() throws Exception {
        var identity = WalReplicaSnapshotTest.identity("source"); var authority = new WalReplicaSnapshotTest.Authority();
        try (var source = WalReplicaSnapshotTest.open(directory.resolve("source"), identity, authority)) {
            var tx = source.begin(IsolationLevel.REPEATABLE_READ); source.put(tx, "a", "large", new byte[16 * 1024]); source.commit(tx);
            assertThrows(WalMvccException.class, () -> source.prepareReplicaBackup(authority, directory.resolve("small"), new WalBackupOptions(1024, 8, 64000, 30_000))); assertFalse(source.lifecycle().backup());
            assertEquals(2, WalReplicaSnapshotTest.pair(source, "later").commitSequence());
            try (var next = source.prepareReplicaBackup(authority, directory.resolve("valid"), limits())) { assertEquals(2, next.complete().sequence()); }
        }
    }

    /** 每轮创建本实例中的专属 schema，不复用其他测试的数据空间。 */
    private Map<String, WalMaterializedStore> stores(String prefix) throws Exception {
        var stores = new HashMap<String, WalMaterializedStore>();
        for (String shard : List.of("a", "b")) {
            String schema = "p_" + prefix + "_" + shard + "_" + UUID.randomUUID().toString().replace("-", "");
            try (var admin = WalCheckpointMysqlSupport.pool(DATABASE); var connection = admin.getConnection(); var query = connection.createStatement()) {
                // 建库只使用本测试的容器 root 账户，名字由固定字符和 UUID 派生。
                var result = DATABASE.execInContainer("mysql", "-uroot", "-p" + DATABASE.getPassword(), "-e", "create database `" + schema + "`; grant all on `" + schema + "`.* to 'wal_test'@'%'"); assertEquals(0, result.getExitCode(), result.getStderr());
            }
            var config = new HikariConfig(); config.setJdbcUrl(DATABASE.getJdbcUrl()); config.setCatalog(schema); config.setUsername(DATABASE.getUsername()); config.setPassword(DATABASE.getPassword()); config.setMaximumPoolSize(3); config.setMinimumIdle(0); config.setConnectionTimeout(5000);
            var pool = new HikariDataSource(config); pools.add(pool); stores.put(shard, new MysqlWalMaterializedStore(pool));
        }
        return Map.copyOf(stores);
    }
    /** 本轮小镜像有固定字节和时间预算。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000); }
}

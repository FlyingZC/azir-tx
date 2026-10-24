package mvcc.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 两个受限 MySQL 实例中的四个专属数据库验证完整备份和跨片恢复。 */
@Testcontainers
class WalMysqlBackupTest {
    @Container static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();
    @Container static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;
    private HikariDataSource a;
    private HikariDataSource b;
    private HikariDataSource targetA;
    private HikariDataSource targetB;

    /** 在测试实例中创建恢复专属数据库，避免把原存储当成恢复目标覆盖。 */
    @BeforeAll static void createTargets() throws Exception {
        for (var container : List.of(A, B)) {
            var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                    "create database wal_restore_target; grant all on wal_restore_target.* to 'wal_test'@'%'");
            assertEquals(0, result.getExitCode(), result.getStderr());
        }
    }

    /** 每轮只重置本测试拥有的四套专属表。 */
    @BeforeEach void prepare() throws Exception {
        a = WalCheckpointMysqlSupport.pool(A); b = WalCheckpointMysqlSupport.pool(B); targetA = targetPool(A); targetB = targetPool(B);
        for (var pool : List.of(a, b, targetA, targetB)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version"); statement.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /** 关闭全部自有连接池，容器交给测试生命周期处理。 */
    @AfterEach void closePools() { for (var pool : new HikariDataSource[]{a, b, targetA, targetB}) { if (pool != null) { pool.close(); } } }

    /** 超过普通扫描一万行上限的数据仍能流式备份，恢复后冷数据和提交凭据完整。 */
    @Test void moreThanScanLimitCanBeBackedUpAndRestoredWithTinyCache() throws Exception {
        int count = 10_101; var sourceStores = sources(); Path wal = directory.resolve("large-source"); UUID transaction;
        // 直接构造合法完整日志并原子物化，避免测试准备阶段对两万个新键逐个进行冷冲突查询。
        try (var log = new WalLog(wal, Set.of("a", "b"), WalOptions.defaults().maxWalBytes())) {
            for (var entry : sourceStores.entrySet()) { entry.getValue().initialize(log.engineId(), entry.getKey()); }
            var mutations = new ArrayList<WalMutation>();
            for (int i = 0; i < count; i++) { for (String shard : sourceStores.keySet()) { mutations.add(new WalMutation(shard, key(i), bytes("value:" + i))); } }
            var commit = new WalCommit(UUID.randomUUID(), 1, 0, mutations); transaction = commit.transactionId(); log.append(List.of(commit));
            for (var store : sourceStores.values()) { store.apply(List.of(commit)); }
        }
        Path backup = directory.resolve("large-backup"); WalBackup.Info backupInfo; long backupWall;
        for (var container : List.of(A, B)) {
            var host = container.getDockerClient().inspectContainerCmd(container.getContainerId()).exec().getHostConfig();
            assertEquals(512L * 1024 * 1024, host.getMemory()); assertEquals(500_000_000L, host.getNanoCPUs());
        }
        try (var runtime = WalMvccRuntime.open(wal, sourceStores, WalOptions.defaults().withHotCacheBytes(0))) {
            long start = System.nanoTime(); backupInfo = runtime.backup(backup, limits()); backupWall = System.nanoTime() - start;
            assertEquals(Map.of("a", (long) count, "b", (long) count), backupInfo.rows());
        }
        var targets = targets(); Path restored = directory.resolve("large-restored");
        long restoreStarted = System.nanoTime();
        WalBackup.Info restoredInfo = WalBackup.restore(backup, restored, targets, WalOptions.defaults().withHotCacheBytes(0), limits());
        long readyElapsed;
        try (var runtime = WalMvccRuntime.open(restored, targets, WalOptions.defaults().withHotCacheBytes(0))) {
            assertEquals(1, runtime.findCommitted(transaction).orElseThrow().commitSequence());
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
            for (int i : new int[]{0, 5000, 10_000, count - 1}) {
                for (String shard : targets.keySet()) { assertEquals("value:" + i, text(runtime.get(tx, shard, key(i), snapshot).orElseThrow())); }
            }
            readyElapsed = System.nanoTime() - restoreStarted;
            assertThrows(WalMvccException.class, () -> runtime.scan(tx, "a", null, null, snapshot)); runtime.rollback(tx);
            assertEquals(count, rows(targetA)); assertEquals(count, rows(targetB)); assertEquals(0, runtime.stats().retainedBytes());
            assertPhysicalValues(targetA, count); assertPhysicalValues(targetB, count);
            assertThrows(WalMvccException.class, () -> WalBackup.restore(backup, directory.resolve("duplicate-writer"), targets,
                    WalOptions.defaults(), limits()), "same backup must not bind a second WAL writer to already restored storage");
            var update = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(update, "a", key(0), bytes("after")); runtime.put(update, "b", key(0), bytes("after")); assertEquals(2, runtime.commit(update).commitSequence());
            runtime.flush(); runtime.checkpoint();
        }
        String evidence = System.getProperty("azir.wal.backupLargeEvidence");
        if (evidence != null) {
            Path output = Path.of(evidence); Files.createDirectories(output.getParent());
            Files.writeString(output, "{\"status\":\"passed\",\"rowsEachShard\":" + count + ",\"physicalRowsIndependentlyChecked\":" + (2 * count)
                    + ",\"backupBytes\":" + backupInfo.bytes() + ",\"backupElapsedMillis\":" + backupInfo.elapsedMillis()
                    + ",\"backupWallMillis\":" + (backupWall / 1_000_000) + ",\"restoreElapsedMillis\":" + restoredInfo.elapsedMillis()
                    + ",\"restoreToFirstConsistentReadMillis\":" + (readyElapsed / 1_000_000)
                    + ",\"snapshotSequence\":1,\"nextCommitSequence\":2,\"hotCacheBytes\":0,\"mysqlCpuCoresEach\":0.5,\"mysqlMemoryBytesEach\":536870912,"
                    + "\"containerIds\":[\"" + A.getContainerId() + "\",\"" + B.getContainerId() + "\"],\"isCapacityBenchmark\":false}\n",
                    java.nio.file.StandardOpenOption.CREATE_NEW);
        }
    }

    /** 一片完成、另一片只导入首批时发生失败，部分恢复不能服务，重试完整恢复而不回滚完成分片。 */
    @Test void resumeAfterOneShardFinishedAndAnotherShardPartiallyImported() throws Exception {
        var handle = makeBackup(10, true); var targets = targets(); Path target = directory.resolve("resumed"); var batches = new AtomicInteger();
        var small = new WalBackupOptions(32L * 1024 * 1024, 4, 8192, 120_000);
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreForTest(handle.archive(), target, targets, WalOptions.defaults(), small, stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_BATCH && batches.incrementAndGet() == 4) { throw new IllegalStateException("restore process failed after partial second shard"); }
        }));
        assertEquals(handle.cut(), targets.get("a").checkpoint().sequence()); assertThrows(WalMvccException.class, () -> targets.get("b").checkpoint());
        assertFalse(Files.exists(target));
        Path staging;
        try (var entries = Files.list(directory)) { staging = entries.filter(path -> Files.isDirectory(path) && path.getFileName().toString().startsWith(".azir-restore-")).findFirst().orElseThrow(); }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(staging, targets, WalOptions.defaults()));
        WalBackup.restore(handle.archive(), target, targets, WalOptions.defaults(), small);
        try (var runtime = WalMvccRuntime.open(target, targets, WalOptions.defaults().withHotCacheBytes(0))) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
            for (String shard : targets.keySet()) {
                assertTrue(runtime.get(tx, shard, key(0), snapshot).isEmpty());
                for (int i = 1; i < 10; i++) { assertEquals("value:" + i, text(runtime.get(tx, shard, key(i), snapshot).orElseThrow())); }
            }
            runtime.rollback(tx); assertTrue(runtime.findCommitted(handle.receipt().transactionId()).isPresent());
            runtime.collectGarbage(32); assertEquals(9, rows(targetA)); assertEquals(9, rows(targetB));
        }
    }

    /** 数据文件被修改时，在创建恢复数据库表和目标 WAL 目录前就拒绝。 */
    @Test void damagedDataIsRejectedBeforeTargetsAreInitialized() throws Exception {
        var handle = makeBackup(5, false); Path data = handle.archive().resolve("data/0000.bin");
        byte[] bytes = Files.readAllBytes(data); bytes[bytes.length / 2] ^= 1; Files.write(data, bytes);
        Path target = directory.resolve("damaged-target");
        assertThrows(IOException.class, () -> WalBackup.restore(handle.archive(), target, targets(), WalOptions.defaults(), limits()));
        assertFalse(Files.exists(target));
        for (var pool : List.of(targetA, targetB)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("show tables")) { assertFalse(rows.next()); }
        }
    }

    /** 空数据库快照保留零边界并可在恢复后正常开始第一笔跨片事务。 */
    @Test void emptyMaterializedEngineCanBeRestored() throws Exception {
        Path backup = directory.resolve("empty-db-backup");
        try (var runtime = WalMvccRuntime.open(directory.resolve("empty-db-source"), sources(), WalOptions.defaults())) { assertEquals(0, runtime.backup(backup, limits()).sequence()); }
        var targets = targets(); Path target = directory.resolve("empty-db-target"); WalBackup.restore(backup, target, targets, WalOptions.defaults(), limits());
        try (var runtime = WalMvccRuntime.open(target, targets, WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "first", bytes("one")); runtime.put(tx, "b", "first", bytes("one"));
            assertEquals(1, runtime.commit(tx).commitSequence()); runtime.flush(); assertEquals(1, rows(targetA)); assertEquals(1, rows(targetB));
        }
    }

    /** 保存一份真实事务生成的跨片备份，并在源中用旧 RR 保护删除前版本。 */
    private Handle makeBackup(int count, boolean deletion) {
        Path backup = directory.resolve("backup"); WalReceipt receipt; long cut;
        try (var runtime = WalMvccRuntime.open(directory.resolve("source"), sources(), WalOptions.defaults().withHotCacheBytes(0))) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < count; i++) { for (String shard : new String[]{"a", "b"}) { runtime.put(tx, shard, key(i), bytes("value:" + i)); } }
            receipt = runtime.commit(tx); var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            if (deletion) {
                var remove = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(remove, "a", key(0)); runtime.delete(remove, "b", key(0)); receipt = runtime.commit(remove);
            }
            cut = runtime.backup(backup, limits()).sequence(); assertEquals("value:0", text(runtime.get(old, "a", key(0)).orElseThrow())); runtime.rollback(old);
        }
        return new Handle(backup, cut, receipt);
    }

    /** 保留备份身份、边界和需要确认的业务事务。 */
    private record Handle(Path archive, long cut, WalReceipt receipt) { }

    /** 源数据库保持独立路由，恢复不会复用这些目标。 */
    private Map<String, WalMaterializedStore> sources() { return Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b)); }

    /** 恢复使用同一实例中的另一个专属数据库，模拟全新存储介质。 */
    private Map<String, WalMaterializedStore> targets() { return Map.of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB)); }

    /** 测试预算限制为 32 MiB，导入最多 256 键和 1 MiB 一批。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(32L * 1024 * 1024, 256, 1024 * 1024, 120_000); }

    /** 构造稳定的二进制键序，以便完整数据摘要可复现。 */
    private static String key(int ordinal) { return String.format(java.util.Locale.ROOT, "key:%05d", ordinal); }

    /** 创建有界恢复连接池，凭据只属于测试容器。 */
    private static HikariDataSource targetPool(MySQLContainer<?> container) {
        var config = new HikariConfig(); config.setJdbcUrl(container.getJdbcUrl().replace("/wal_checkpoint", "/wal_restore_target"));
        config.setUsername(container.getUsername()); config.setPassword(container.getPassword()); config.setMaximumPoolSize(3); config.setMinimumIdle(0);
        config.setConnectionTimeout(5000); config.setValidationTimeout(1000); return new HikariDataSource(config);
    }

    /** 绕过框架缓存直接核对恢复后端的完整物理行数。 */
    private static long rows(HikariDataSource pool) throws Exception {
        try (var connection = pool.getConnection(); var statement = connection.createStatement(); var row = statement.executeQuery("select count(*) from azir_wal_version")) {
            row.next(); return row.getLong(1);
        }
    }

    /** 用独立 SQL 逐行核对全部原始业务值和版本，不依赖备份编码器的自校验结果。 */
    private static void assertPhysicalValues(HikariDataSource pool, int expected) throws Exception {
        try (var connection = pool.getConnection(); var query = connection.prepareStatement("select user_key,commit_seq,payload from azir_wal_version order by user_key")) {
            query.setFetchSize(Integer.MIN_VALUE);
            try (var rows = query.executeQuery()) {
                int ordinal = 0;
                while (rows.next()) {
                    assertEquals(key(ordinal), new String(rows.getBytes(1), java.nio.charset.StandardCharsets.UTF_8));
                    assertEquals(1, rows.getLong(2)); assertEquals("value:" + ordinal, text(rows.getBytes(3))); ordinal++;
                }
                assertEquals(expected, ordinal);
            }
        }
    }
}

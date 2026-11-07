package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 两个受限真实数据库上的备份及恢复强杀矩阵，逐次保留可复核的冷数据和凭据证据。 */
@Testcontainers
class WalBackupCrashTest {
    @Container static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();
    @Container static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;

    /** 创建隔离恢复数据库，源和目标不共用协议表。 */
    @BeforeAll static void setupTargets() throws Exception {
        for (var container : List.of(A, B)) {
            var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                    "create database wal_restore_target; grant all on wal_restore_target.* to 'wal_test'@'%'");
            assertEquals(0, result.getExitCode(), result.getStderr());
        }
    }

    /** 普通回归每种一次，正式矩阵由参数显式指定重复次数。 */
    static Stream<String> boundaries() {
        return java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.backupCrashRepeats", 1)).boxed().flatMap(round -> Stream.of(
                "AFTER_BACKUP_FILE_COPY", "BEFORE_BACKUP_COMPLETE", "AFTER_BACKUP_COMPLETE", "AFTER_RESTORE_FILE_COPY",
                "AFTER_RESTORE_BATCH", "BEFORE_RESTORE_PUBLISH", "AFTER_RESTORE_MOVE", "AFTER_RESTORE_READY"));
    }

    /** 强杀后确认源仍正确、部分恢复不能打开，并通过同一备份重试或确认已经完成的发布。 */
    @ParameterizedTest @MethodSource("boundaries") void crashArtifactsRemainFailClosedAndRecoverable(String boundary) throws Exception {
        long started = System.nanoTime(); String saved = System.getProperty("azir.wal.backupCrashEvidence");
        if (saved != null) { Path root = Path.of(saved); Files.createDirectories(root); directory = Files.createTempDirectory(root, boundary + "-"); }
        String targetUrlA = A.getJdbcUrl().replace("/wal_checkpoint", "/wal_restore_target");
        String targetUrlB = B.getJdbcUrl().replace("/wal_checkpoint", "/wal_restore_target");
        try (var a = WalCheckpointMysqlSupport.pool(A); var b = WalCheckpointMysqlSupport.pool(B); var targetA = pool(targetUrlA); var targetB = pool(targetUrlB)) {
            for (var source : new javax.sql.DataSource[]{a, b, targetA, targetB}) {
                try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                    statement.executeUpdate("drop table if exists azir_wal_version"); statement.executeUpdate("drop table if exists azir_wal_binding");
                }
            }
            var sources = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
            var targets = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB));
            boolean backupCrash = boundary.contains("BACKUP"); long cut = backupCrash ? 1 : 2;
            if (!backupCrash) { prepareBackup(sources); }
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-XX:ActiveProcessorCount=2", "-cp", cp,
                    WalBackupCrashProcess.class.getName(), directory.toString(), boundary, A.getJdbcUrl(), B.getJdbcUrl(), targetUrlA, targetUrlB)
                    .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
            try {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (child.isAlive() && !Files.exists(directory.resolve("ready")) && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(10); }
                assertTrue(Files.exists(directory.resolve("ready")), () -> {
                    try { return Files.readString(directory.resolve("child.log")); } catch (IOException failure) { return failure.toString(); }
                });
            } finally { child.destroyForcibly(); assertTrue(child.waitFor(10, TimeUnit.SECONDS)); }
            assertNotEquals(0, child.exitValue()); UUID seed = UUID.fromString(Files.readString(directory.resolve("seed-id")));
            Path backup = directory.resolve("backup"); Path target = directory.resolve("restored");
            if (backupCrash) {
                if (boundary.equals("AFTER_BACKUP_COMPLETE")) { assertEquals(cut, WalBackup.inspect(backup, WalBackupCrashProcess.limits()).sequence()); }
                else { assertThrows(IOException.class, () -> WalBackup.inspect(directory.resolve("backup"), WalBackupCrashProcess.limits())); }
                try (var runtime = WalMvccRuntime.open(directory.resolve("source"), sources, WalOptions.defaults().withHotCacheBytes(0))) {
                    verify(runtime, false); assertTrue(runtime.findCommitted(seed).isPresent());
                    if (!boundary.equals("AFTER_BACKUP_COMPLETE")) { backup = directory.resolve("retry-backup"); runtime.backup(backup, WalBackupCrashProcess.limits()); }
                }
                WalBackup.restore(backup, target, targets, WalOptions.defaults().withHotCacheBytes(0), WalBackupCrashProcess.limits());
            } else if (!boundary.equals("AFTER_RESTORE_READY")) {
                Path partial;
                if (Files.exists(target)) { partial = target; }
                else {
                    try (var entries = Files.list(directory)) { partial = entries.filter(path -> Files.isDirectory(path) && path.getFileName().toString().startsWith(".azir-restore-")).findFirst().orElseThrow(); }
                }
                assertTrue(Files.exists(partial.resolve("RESTORING")));
                assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(partial, targets, WalOptions.defaults()));
                WalBackup.restore(backup, target, targets, WalOptions.defaults().withHotCacheBytes(0), WalBackupCrashProcess.limits());
            }
            try (var runtime = WalMvccRuntime.open(target, targets, WalOptions.defaults().withHotCacheBytes(0))) {
                assertEquals(cut, runtime.stats().publishedSequence()); verify(runtime, !backupCrash); assertTrue(runtime.findCommitted(seed).isPresent());
                if (!backupCrash) { assertEquals(2, runtime.findCommitted(UUID.fromString(Files.readString(directory.resolve("delete-id")))).orElseThrow().commitSequence()); }
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "next", bytes("next")); runtime.put(tx, "b", "next", bytes("next"));
                assertEquals(cut + 1, runtime.commit(tx).commitSequence()); runtime.flush(); runtime.checkpoint();
            }
            if (saved != null) {
                Files.writeString(directory.resolve("verification.json"), "{\"status\":\"passed\",\"boundary\":\"" + boundary + "\",\"childExitCode\":" + child.exitValue()
                        + ",\"restoredSequence\":" + cut + ",\"nextCommitSequence\":" + (cut + 1) + ",\"physicalProcessKill\":true,\"keysCheckedEachShard\":20}\n");
            }
        } finally {
            long remaining = TimeUnit.SECONDS.toNanos(1) - (System.nanoTime() - started); if (remaining > 0) { TimeUnit.NANOSECONDS.sleep(remaining); }
        }
    }

    /** 两笔真实提交形成包含删除墓碑的完整备份，旧 RR 保留删除之前的数据库版本。 */
    private void prepareBackup(Map<String, WalMaterializedStore> sources) throws Exception {
        try (var runtime = WalMvccRuntime.open(directory.resolve("source"), sources, WalOptions.defaults().withHotCacheBytes(0))) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 20; i++) { for (String shard : sources.keySet()) { runtime.put(tx, shard, "k" + i, bytes("v:" + i)); } }
            runtime.commit(tx); Files.writeString(directory.resolve("seed-id"), tx.id().toString()); var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.delete(deletion, "a", "k0"); runtime.delete(deletion, "b", "k0"); runtime.commit(deletion);
            Files.writeString(directory.resolve("delete-id"), deletion.id().toString()); runtime.backup(directory.resolve("backup"), WalBackupCrashProcess.limits()); runtime.rollback(reader);
        }
    }

    /** 独立业务预期逐键核对两片完整快照，不能只比较两片相等。 */
    private static void verify(WalMvccRuntime runtime, boolean deleted) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
        for (int i = 0; i < 20; i++) {
            for (String shard : new String[]{"a", "b"}) {
                var value = runtime.get(tx, shard, "k" + i, snapshot);
                if (deleted && i == 0) { assertTrue(value.isEmpty()); } else { assertEquals("v:" + i, text(value.orElseThrow())); }
            }
        }
        runtime.rollback(tx);
    }

    /** 恢复数据库的连接数和超时与源保持同一资源边界。 */
    private static HikariDataSource pool(String url) {
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername("wal_test"); config.setPassword("wal_test");
        config.setMaximumPoolSize(3); config.setMinimumIdle(0); config.setConnectionTimeout(5000); config.setValidationTimeout(1000); return new HikariDataSource(config);
    }
}

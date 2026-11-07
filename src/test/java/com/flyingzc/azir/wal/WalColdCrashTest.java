package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 双真实后端与零热点缓存的进程强杀矩阵，恢复不得依赖丢失的内存头。 */
@Testcontainers
class WalColdCrashTest {
    @Container static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();
    @Container static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();
    @TempDir Path directory;

    /** 串行重复各边界，默认每种一次，正式重复数由执行命令显式记录。 */
    static Stream<String> boundaries() {
        return java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.coldCrashRepeats", 1)).boxed().flatMap(repetition -> Stream.of(
                "AFTER_FORCE", "BEFORE_PUBLISH", "AFTER_PUBLISH", "BEFORE_CHECKPOINT_FILES", "AFTER_RECEIPTS_FORCE",
                "AFTER_SNAPSHOT_FORCE", "BEFORE_CURRENT_MOVE", "AFTER_CURRENT_MOVE", "AFTER_CURRENT_FORCE",
                "BEFORE_OLD_GENERATION_DELETE", "AFTER_OLD_GENERATION_DELETE", "GC_AFTER_FIRST_DELETE"))
                .filter(boundary -> System.getProperty("azir.wal.coldCrashBoundary") == null
                        || boundary.equals(System.getProperty("azir.wal.coldCrashBoundary")));
    }

    /** 强杀后两次重开核对完整版本、提交凭据、墓碑退休和后续连续提交。 */
    @ParameterizedTest @MethodSource("boundaries") void acknowledgedColdStateSurvivesProcessKill(String boundary) throws Exception {
        long started = System.nanoTime();
        String saved = System.getProperty("azir.wal.coldCrashEvidence");
        if (saved != null) {
            Path evidence = Path.of(saved); Files.createDirectories(evidence);
            directory = Files.createTempDirectory(evidence, boundary + "-");
        }
        try (var a = WalCheckpointMysqlSupport.pool(A); var b = WalCheckpointMysqlSupport.pool(B)) {
            for (var pool : new javax.sql.DataSource[]{a, b}) {
                try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                    statement.executeUpdate("drop table if exists azir_wal_version"); statement.executeUpdate("drop table if exists azir_wal_binding");
                }
            }
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx128m", "-XX:ActiveProcessorCount=2", "-cp", cp, WalColdCrashProcess.class.getName(),
                    directory.toString(), boundary, A.getJdbcUrl(), B.getJdbcUrl())
                    .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
            try {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (!Files.exists(directory.resolve("ready")) && child.isAlive() && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(10); }
                assertTrue(Files.exists(directory.resolve("ready")), () -> {
                    try { return Files.readString(directory.resolve("child.log")); } catch (Exception failure) { return failure.toString(); }
                });
            } finally { child.destroyForcibly(); assertTrue(child.waitFor(10, TimeUnit.SECONDS)); }
            assertNotEquals(0, child.exitValue(), "crash verification must not use normal shutdown");
            var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
            UUID seed = UUID.fromString(Files.readString(directory.resolve("seed-ack")));
            UUID changed = UUID.fromString(Files.readString(directory.resolve("transaction-id")));
            for (int reopening = 0; reopening < 2; reopening++) {
                try (var runtime = WalMvccRuntime.open(directory, stores, WalColdCrashProcess.options())) {
                    assertEquals(2 + reopening, runtime.stats().publishedSequence());
                    assertTrue(runtime.findCommitted(seed).isPresent()); assertTrue(runtime.findCommitted(changed).isPresent());
                    var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
                    for (int i = 0; i < 16; i++) {
                        for (String shard : stores.keySet()) {
                            var value = runtime.get(tx, shard, "key:" + i, snapshot);
                            if (boundary.equals("GC_AFTER_FIRST_DELETE") && i < 2) { assertTrue(value.isEmpty()); }
                            else { assertEquals(i == 0 ? "new" : "old:" + i, text(value.orElseThrow())); }
                        }
                    }
                    if (boundary.equals("GC_AFTER_FIRST_DELETE") && reopening == 0) {
                        long leftRows = physicalRows(a); long rightRows = physicalRows(b);
                        assertTrue(leftRows == 14 || leftRows == 18); assertTrue(rightRows == 14 || rightRows == 18);
                        assertTrue(leftRows == 18 || rightRows == 18, "killed GC transaction must roll back its first physical delete");
                    }
                    runtime.rollback(tx); runtime.flush(); runtime.collectGarbage(32); runtime.checkpoint();
                    if (reopening == 0) {
                        var next = runtime.begin(IsolationLevel.REPEATABLE_READ);
                        runtime.put(next, "a", "next", bytes("next")); runtime.put(next, "b", "next", bytes("next"));
                        assertEquals(3, runtime.commit(next).commitSequence()); runtime.flush(); runtime.checkpoint();
                    }
                }
            }
            if (saved != null) {
                Files.writeString(directory.resolve("verification.json"), "{\"status\":\"passed\",\"boundary\":\"" + boundary
                        + "\",\"childExitCode\":" + child.exitValue() + ",\"reopenings\":2,\"recoveredCommittedTransactions\":2,"
                        + "\"acknowledgedBeforeCrash\":" + (Files.exists(directory.resolve("new-ack")) ? 2 : 1) + ","
                        + "\"nextCommitSequence\":3,\"hotCacheBytes\":0,\"actualProcessKill\":true}\n");
            }
        } finally {
            long remaining = TimeUnit.SECONDS.toNanos(1) - (System.nanoTime() - started);
            if (remaining > 0) { TimeUnit.NANOSECONDS.sleep(remaining); }
        }
    }

    /** 直接检查真实数据库行数，部分执行的回收事务不能留下半批物理删除。 */
    private static long physicalRows(javax.sql.DataSource pool) throws Exception {
        try (var connection = pool.getConnection(); var statement = connection.createStatement();
             var row = statement.executeQuery("select count(*) from azir_wal_version")) { row.next(); return row.getLong(1); }
    }
}

package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** 两个真实 MySQL 上逐轮证明旧读保护、物理删除、日志截断及零缓存冷恢复。 */
class MysqlCheckpointCycleAcceptanceTest {
    @TempDir Path temporary;

    /** 每轮都发生实际物理回收和新检查点，不把无数据的维护调用算作周期。 */
    @Test void oneHundredPhysicalCyclesPreserveProtectedReadsAndReceipts() throws Exception {
        int rounds = Integer.getInteger("azir.wal.physicalCycles", 100);
        assertTrue(rounds >= 1 && rounds <= 1000);
        String destination = System.getProperty("azir.wal.physicalCycleEvidence");
        Path root = destination == null ? temporary : Path.of(destination); Files.createDirectories(root);
        Path trace = root.resolve("cycles.tsv");
        Files.writeString(trace, "cycle\tfirstId\tfirstSequence\tlastId\tlastSequence\tprotectedRows\tbeforeGcRows\tafterGcRows\tgcRemoved\twalBefore\twalAfter\tcoldReads\tdeleted\n", StandardOpenOption.CREATE_NEW);
        var databases = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
        var options = WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0);
        long published = 0; WalReceipt earliest = null;
        try {
            Startables.deepStart(databases.stream()).join();
            Files.writeString(root.resolve("resources.json"), "{\"containerIds\":[\"" + databases.get(0).getContainerId() + "\",\"" + databases.get(1).getContainerId() + "\"],\"cpuEach\":0.25,\"memoryEach\":536870912}\n");
            try (var a = MysqlGenerationSupport.control(databases.get(0), databases.get(0).getDatabaseName());
                 var b = MysqlGenerationSupport.control(databases.get(1), databases.get(1).getDatabaseName())) {
                var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
                for (int cycle = 1; cycle <= rounds; cycle++) {
                    boolean deleted = cycle % 10 == 0; WalReceipt first, last;
                    long protectedRows, beforeRows, afterRows, removed, walBefore, walAfter, coldReads;
                    try (var runtime = WalMvccRuntime.open(root.resolve("wal"), stores, options)) {
                        assertEquals(published, runtime.stats().publishedSequence());
                        first = write(runtime, "old:" + cycle); if (earliest == null) { earliest = first; }
                        var held = runtime.begin(IsolationLevel.REPEATABLE_READ);
                        try {
                            var snapshot = runtime.beginReadStatement(held);
                            last = write(runtime, "new:" + cycle); runtime.flush(); runtime.collectGarbage(64);
                            // 每个分片必须实际保留被长读保护的旧版本与最新版本。
                            assertEquals(2, rows(a)); assertEquals(2, rows(b)); protectedRows = rows(a) + rows(b);
                            for (String shard : stores.keySet()) { assertEquals("old:" + cycle, text(runtime.get(held, shard, "balance", snapshot).orElseThrow())); }
                        } finally { runtime.rollback(held); }
                        if (deleted) { last = write(runtime, null); runtime.flush(); }
                        beforeRows = rows(a) + rows(b); removed = runtime.collectGarbage(64); afterRows = rows(a) + rows(b);
                        assertTrue(removed > 0); assertTrue(beforeRows > afterRows); assertEquals(deleted ? 0 : 2, afterRows);
                        current(runtime, deleted ? null : "new:" + cycle);
                        var checkpoint = runtime.checkpoint(); assertTrue(checkpoint.created()); assertEquals(last.commitSequence(), checkpoint.sequence());
                        walBefore = checkpoint.walBytesBefore(); walAfter = checkpoint.walBytesAfter(); assertTrue(walBefore > walAfter); assertEquals(100, walAfter);
                        assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(first.transactionId()).status());
                        assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(earliest.transactionId()).status());
                        coldReads = runtime.stats().cacheMisses(); assertTrue(coldReads > 0); assertEquals(0, runtime.stats().hotCacheBytes()); published = last.commitSequence();
                    }
                    // 关闭内核后重开相同目录，两片冷数据和首轮凭据必须仍可恢复。
                    try (var restored = WalMvccRuntime.open(root.resolve("wal"), stores, options)) {
                        assertEquals(published, restored.stats().publishedSequence()); assertEquals(published, restored.stats().checkpointSequence());
                        current(restored, deleted ? null : "new:" + cycle);
                        assertEquals(WalCommitLookup.Status.COMMITTED, restored.lookupCommit(first.transactionId()).status());
                        assertEquals(WalCommitLookup.Status.COMMITTED, restored.lookupCommit(last.transactionId()).status());
                        assertEquals(WalCommitLookup.Status.COMMITTED, restored.lookupCommit(earliest.transactionId()).status());
                    }
                    append(trace, cycle + "\t" + first.transactionId() + "\t" + first.commitSequence() + "\t" + last.transactionId() + "\t" + last.commitSequence()
                            + "\t" + protectedRows + "\t" + beforeRows + "\t" + afterRows + "\t" + removed + "\t" + walBefore + "\t" + walAfter + "\t" + coldReads + "\t" + deleted + "\n");
                }
            }
        } finally { for (var database : databases) { database.stop(); } }
        Files.writeString(root.resolve("verification.json"), "{\"status\":\"passed\",\"cycles\":" + rounds + ",\"published\":" + published + ",\"independentMysqlInstances\":2,\"hotCacheBytes\":0,\"recoveryEachCycle\":true,\"productionReady\":false}\n", StandardOpenOption.CREATE_NEW);
    }

    /** 两个逻辑分片在同一事务写入相同确定内容。 */
    private static WalReceipt write(WalMvccRuntime runtime, String value) {
        var transaction = runtime.begin(IsolationLevel.REPEATABLE_READ);
        for (String shard : List.of("a", "b")) { if (value == null) { runtime.delete(transaction, shard, "balance"); } else { runtime.put(transaction, shard, "balance", value.getBytes(StandardCharsets.UTF_8)); } }
        return runtime.commit(transaction);
    }
    /** 同一语句检查跨片精确值，不以两端相等代替期望值检查。 */
    private static void current(WalMvccRuntime runtime, String expected) {
        var transaction = runtime.begin(IsolationLevel.REPEATABLE_READ);
        try { var snapshot = runtime.beginReadStatement(transaction); for (String shard : List.of("a", "b")) { assertEquals(expected, runtime.get(transaction, shard, "balance", snapshot).map(MysqlCheckpointCycleAcceptanceTest::text).orElse(null)); } }
        finally { runtime.rollback(transaction); }
    }
    /** 直接读取真实版本表数量，不能用内存计数冒充物理删除。 */
    private static long rows(HikariDataSource pool) throws Exception { try (var connection = pool.getConnection(); var query = connection.createStatement(); var result = query.executeQuery("select count(*) from azir_wal_version")) { assertTrue(result.next()); return result.getLong(1); } }
    /** 样例载荷为 UTF8 字符串。 */
    private static String text(byte[] bytes) { return new String(bytes, StandardCharsets.UTF_8); }
    /** 仅在整轮关闭与重开核对通过后同步保存该周期证据。 */
    private static void append(Path file, String row) throws Exception { try (var channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) { var bytes = ByteBuffer.wrap(row.getBytes(StandardCharsets.UTF_8)); while (bytes.hasRemaining()) { channel.write(bytes); } channel.force(true); } }
}

/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.MysqlWalMaterializedStore;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.lifecycle.Startables;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

/**
 * Two real MySQLs on a rotational basis to prove old reading protection, physical deletion, log breaking and zero cache cold recovery.
 */
class MysqlCheckpointCycleAcceptanceTest {

    @TempDir
    Path temporary;

    /**
     * Physical collection and new checkpoints occur on each round, and the non-data-available maintenance is not counted as a cycle.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void oneHundredPhysicalCyclesPreserveProtectedReadsAndReceipts() throws Exception {
        int rounds = Integer.getInteger("azir.wal.physicalCycles", 100);
        assertTrue(rounds >= 1 && rounds <= 1000);
        String destination = System.getProperty("azir.wal.physicalCycleEvidence");
        Path root = destination == null ? temporary : Path.of(destination);
        Files.createDirectories(root);
        Path trace = root.resolve("cycles.tsv");
        Files.writeString(trace, "cycle\tfirstId\tfirstSequence\tlastId\tlastSequence\tprotectedRows\tbeforeGcRows\tafterGcRows\tgcRemoved\twalBefore\twalAfter\tcoldReads\tdeleted\n",
                StandardOpenOption.CREATE_NEW);
        var databases = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
        var options = WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0);
        long published = 0;
        WalReceipt earliest = null;
        try {
            Startables.deepStart(databases.stream()).join();
            Files.writeString(root.resolve("resources.json"),
                    "{\"containerIds\":[\"" + databases.get(0).getContainerId() + "\",\"" + databases.get(1).getContainerId() + "\"],\"cpuEach\":0.25,\"memoryEach\":536870912}\n");
            try (
                    var a = MysqlGenerationSupport.control(databases.get(0), databases.get(0).getDatabaseName());
                    var b = MysqlGenerationSupport.control(databases.get(1), databases.get(1).getDatabaseName())) {
                var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
                for (int cycle = 1; cycle <= rounds; cycle++) {
                    boolean deleted = cycle % 10 == 0;
                    WalReceipt first, last;
                    long protectedRows, beforeRows, afterRows, removed, walBefore, walAfter, coldReads;
                    try (var runtime = WalMvccRuntime.open(root.resolve("wal"), stores, options)) {
                        Assertions.assertEquals(published, runtime.stats().publishedSequence());
                        first = write(runtime, "old:" + cycle);
                        if (earliest == null) {
                            earliest = first;
                        }
                        var held = runtime.begin(IsolationLevel.REPEATABLE_READ);
                        try {
                            var snapshot = runtime.beginReadStatement(held);
                            last = write(runtime, "new:" + cycle);
                            runtime.flush();
                            runtime.collectGarbage(64);
                            // Each shard must actually keep a long-read protected old version with the latest version.
                            Assertions.assertEquals(2, rows(a));
                            Assertions.assertEquals(2, rows(b));
                            protectedRows = rows(a) + rows(b);
                            for (String shard : stores.keySet()) {
                                Assertions.assertEquals("old:" + cycle, text(runtime.get(held, shard, "balance", snapshot).orElseThrow()));
                            }
                        } finally {
                            runtime.rollback(held);
                        }
                        if (deleted) {
                            last = write(runtime, null);
                            runtime.flush();
                        }
                        beforeRows = rows(a) + rows(b);
                        removed = runtime.collectGarbage(64);
                        afterRows = rows(a) + rows(b);
                        assertTrue(removed > 0);
                        assertTrue(beforeRows > afterRows);
                        Assertions.assertEquals(deleted ? 0 : 2, afterRows);
                        current(runtime, deleted ? null : "new:" + cycle);
                        var checkpoint = runtime.checkpoint();
                        assertTrue(checkpoint.created());
                        Assertions.assertEquals(last.commitSequence(), checkpoint.sequence());
                        walBefore = checkpoint.walBytesBefore();
                        walAfter = checkpoint.walBytesAfter();
                        assertTrue(walBefore > walAfter);
                        Assertions.assertEquals(100, walAfter);
                        Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(first.transactionId()).status());
                        Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, runtime.lookupCommit(earliest.transactionId()).status());
                        coldReads = runtime.stats().cacheMisses();
                        assertTrue(coldReads > 0);
                        Assertions.assertEquals(0, runtime.stats().hotCacheBytes());
                        published = last.commitSequence();
                    }
                    // Reopen the same directory after closing the kernel, two cold data and the first wheel receipt must still be recovery.
                    try (var restored = WalMvccRuntime.open(root.resolve("wal"), stores, options)) {
                        Assertions.assertEquals(published, restored.stats().publishedSequence());
                        Assertions.assertEquals(published, restored.stats().checkpointSequence());
                        current(restored, deleted ? null : "new:" + cycle);
                        Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, restored.lookupCommit(first.transactionId()).status());
                        Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, restored.lookupCommit(last.transactionId()).status());
                        Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, restored.lookupCommit(earliest.transactionId()).status());
                    }
                    append(trace, cycle + "\t" + first.transactionId() + "\t" + first.commitSequence() + "\t" + last.transactionId() + "\t" + last.commitSequence()
                            + "\t" + protectedRows + "\t" + beforeRows + "\t" + afterRows + "\t" + removed + "\t" + walBefore + "\t" + walAfter + "\t" + coldReads + "\t" + deleted + "\n");
                }
            }
        } finally {
            for (var database : databases) {
                database.stop();
            }
        }
        Files.writeString(root.resolve("verification.json"), "{\"status\":\"passed\",\"cycles\":" + rounds + ",\"published\":" + published
                + ",\"independentMysqlInstances\":2,\"hotCacheBytes\":0,\"recoveryEachCycle\":true,\"productionReady\":false}\n", StandardOpenOption.CREATE_NEW);
    }

    /**
     * Two logic shard s are written to the same determined content in the same transaction.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt write(final WalMvccRuntime runtime, final String value) {
        var transaction = runtime.begin(IsolationLevel.REPEATABLE_READ);
        for (String shard : List.of("a", "b")) {
            if (value == null) {
                runtime.delete(transaction, shard, "balance");
            } else {
                runtime.put(transaction, shard, "balance", value.getBytes(StandardCharsets.UTF_8));
            }
        }
        return runtime.commit(transaction);
    }

    /**
     * The same sentence examines the cross-shard accuracy and does not replace the expected value check with an equivalent of two ends.
     *
     * @param runtime runtime
     * @param expected expected
     */
    private static void current(final WalMvccRuntime runtime, final String expected) {
        var transaction = runtime.begin(IsolationLevel.REPEATABLE_READ);
        try {
            var snapshot = runtime.beginReadStatement(transaction);
            for (String shard : List.of("a", "b")) {
                Assertions.assertEquals(expected, runtime.get(transaction, shard, "balance", snapshot).map(MysqlCheckpointCycleAcceptanceTest::text).orElse(null));
            }
        } finally {
            runtime.rollback(transaction);
        }
    }

    /**
     * Reads the real number of version tables directly, and cannot impersonate physical deletion with memory counts.
     *
     * @param pool pool
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long rows(final HikariDataSource pool) throws Exception {
        try (var connection = pool.getConnection(); var query = connection.createStatement(); var result = query.executeQuery("select count(*) from azir_wal_version")) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    /**
     * Sample payload is a UTF8 string.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static String text(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Only the chronology of evidence for the cycle is kept after the round closes and re-opens.
     *
     * @param file file
     * @param row row
     * @throws Exception if the operation cannot complete
     */
    private static void append(final Path file, final String row) throws Exception {
        try (var channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            var bytes = ByteBuffer.wrap(row.getBytes(StandardCharsets.UTF_8));
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        }
    }
}

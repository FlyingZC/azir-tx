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

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
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
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Four of the restricted MySQL examples are exclusive database verify complete backup and cross-shard recovery.
 */
@Testcontainers
class WalMysqlBackupTest {

    @Container
    static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();

    @Container
    static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();

    @TempDir
    Path directory;

    private HikariDataSource a;

    private HikariDataSource b;

    private HikariDataSource targetA;

    private HikariDataSource targetB;

    /**
     * Creates an recovery exclusive database in the test instance to avoid using original storage as an recovery target overwrite.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeAll
    static void createTargets() throws Exception {
        for (var container : List.of(A, B)) {
            var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                    "create database wal_restore_target; grant all on wal_restore_target.* to 'wal_test'@'%'");
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        }
    }

    /**
     * Only four sets of exclusive forms per round will be replaced for this test.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        a = WalCheckpointMysqlSupport.pool(A);
        b = WalCheckpointMysqlSupport.pool(B);
        targetA = targetPool(A);
        targetB = targetPool(B);
        for (var pool : List.of(a, b, targetA, targetB)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version");
                statement.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /**
     * All connection pool is closed and the container is handed over to the test life cycle for processing.
     */
    @AfterEach
    void closePools() {
        for (var pool : new HikariDataSource[]{a, b, targetA, targetB}) {
            if (pool != null) {
                pool.close();
            }
        }
    }

    /**
     * Data above the normal scanned maximum of 10,000 lines is still current, recovery post-cool data and commit receipt complete.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void moreThanScanLimitCanBeBackedUpAndRestoredWithTinyCache() throws Exception {
        int count = 10_101;
        var sourceStores = sources();
        Path wal = directory.resolve("large-source");
        UUID transaction;
        // Directly constructs a valid complete log and atomic materialization to avoid the cold-conflicting query of 20,000 new keys at the test preparation stage.
        try (var log = new WalLog(wal, Set.of("a", "b"), WalOptions.defaults().maxWalBytes())) {
            for (var entry : sourceStores.entrySet()) {
                entry.getValue().initialize(log.engineId(), entry.getKey());
            }
            var mutations = new ArrayList<WalMutation>();
            for (int i = 0; i < count; i++) {
                for (String shard : sourceStores.keySet()) {
                    mutations.add(new WalMutation(shard, key(i), bytes("value:" + i)));
                }
            }
            var commit = new WalCommit(UUID.randomUUID(), 1, 0, mutations);
            transaction = commit.transactionId();
            log.append(List.of(commit));
            for (var store : sourceStores.values()) {
                store.apply(List.of(commit));
            }
        }
        Path backup = directory.resolve("large-backup");
        WalBackup.Info backupInfo;
        long backupWall;
        for (var container : List.of(A, B)) {
            var host = container.getDockerClient().inspectContainerCmd(container.getContainerId()).exec().getHostConfig();
            Assertions.assertEquals(512L * 1024 * 1024, host.getMemory());
            Assertions.assertEquals(500_000_000L, host.getNanoCPUs());
        }
        try (var runtime = WalMvccRuntime.open(wal, sourceStores, WalOptions.defaults().withHotCacheBytes(0))) {
            long start = System.nanoTime();
            backupInfo = runtime.backup(backup, limits());
            backupWall = System.nanoTime() - start;
            Assertions.assertEquals(Map.of("a", (long) count, "b", (long) count), backupInfo.rows());
        }
        var targets = targets();
        Path restored = directory.resolve("large-restored");
        long restoreStarted = System.nanoTime();
        WalBackup.Info restoredInfo = WalBackup.restore(backup, restored, targets, WalOptions.defaults().withHotCacheBytes(0), limits());
        long readyElapsed;
        try (var runtime = WalMvccRuntime.open(restored, targets, WalOptions.defaults().withHotCacheBytes(0))) {
            Assertions.assertEquals(1, runtime.findCommitted(transaction).orElseThrow().commitSequence());
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = runtime.beginReadStatement(tx);
            for (int i : new int[]{0, 5000, 10_000, count - 1}) {
                for (String shard : targets.keySet()) {
                    Assertions.assertEquals("value:" + i, text(runtime.get(tx, shard, key(i), snapshot).orElseThrow()));
                }
            }
            readyElapsed = System.nanoTime() - restoreStarted;
            assertThrows(WalMvccException.class, () -> runtime.scan(tx, "a", null, null, snapshot));
            runtime.rollback(tx);
            Assertions.assertEquals(count, rows(targetA));
            Assertions.assertEquals(count, rows(targetB));
            Assertions.assertEquals(0, runtime.stats().retainedBytes());
            assertPhysicalValues(targetA, count);
            assertPhysicalValues(targetB, count);
            assertThrows(WalMvccException.class, () -> WalBackup.restore(backup, directory.resolve("duplicate-writer"), targets,
                    WalOptions.defaults(), limits()), "same backup must not bind a second WAL writer to already restored storage");
            var update = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(update, "a", key(0), bytes("after"));
            runtime.put(update, "b", key(0), bytes("after"));
            Assertions.assertEquals(2, runtime.commit(update).commitSequence());
            runtime.flush();
            runtime.checkpoint();
        }
        String evidence = System.getProperty("azir.wal.backupLargeEvidence");
        if (evidence != null) {
            Path output = Path.of(evidence);
            Files.createDirectories(output.getParent());
            Files.writeString(output, "{\"status\":\"passed\",\"rowsEachShard\":" + count + ",\"physicalRowsIndependentlyChecked\":" + (2 * count)
                    + ",\"backupBytes\":" + backupInfo.bytes() + ",\"backupElapsedMillis\":" + backupInfo.elapsedMillis()
                    + ",\"backupWallMillis\":" + (backupWall / 1_000_000) + ",\"restoreElapsedMillis\":" + restoredInfo.elapsedMillis()
                    + ",\"restoreToFirstConsistentReadMillis\":" + (readyElapsed / 1_000_000)
                    + ",\"snapshotSequence\":1,\"nextCommitSequence\":2,\"hotCacheBytes\":0,\"mysqlCpuCoresEach\":0.5,\"mysqlMemoryBytesEach\":536870912,"
                    + "\"containerIds\":[\"" + A.getContainerId() + "\",\"" + B.getContainerId() + "\"],\"isCapacityBenchmark\":false}\n",
                    java.nio.file.StandardOpenOption.CREATE_NEW);
        }
    }

    /**
     * One completed piece and another imported only failed in the first instance. Some recovery is not serviceable, retry complete recovery instead of rollback finished shard.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void resumeAfterOneShardFinishedAndAnotherShardPartiallyImported() throws Exception {
        var handle = makeBackup(10, true);
        var targets = targets();
        Path target = directory.resolve("resumed");
        var batches = new AtomicInteger();
        var small = new WalBackupOptions(32L * 1024 * 1024, 4, 8192, 120_000);
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreForTest(handle.archive(), target, targets, WalOptions.defaults(), small, stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_BATCH && batches.incrementAndGet() == 4) {
                throw new IllegalStateException("restore process failed after partial second shard");
            }
        }));
        Assertions.assertEquals(handle.cut(), targets.get("a").checkpoint().sequence());
        assertThrows(WalMvccException.class, () -> targets.get("b").checkpoint());
        assertFalse(Files.exists(target));
        Path staging;
        try (var entries = Files.list(directory)) {
            staging = entries.filter(path -> Files.isDirectory(path) && path.getFileName().toString().startsWith(".azir-restore-")).findFirst().orElseThrow();
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(staging, targets, WalOptions.defaults()));
        WalBackup.restore(handle.archive(), target, targets, WalOptions.defaults(), small);
        try (var runtime = WalMvccRuntime.open(target, targets, WalOptions.defaults().withHotCacheBytes(0))) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = runtime.beginReadStatement(tx);
            for (String shard : targets.keySet()) {
                assertTrue(runtime.get(tx, shard, key(0), snapshot).isEmpty());
                for (int i = 1; i < 10; i++) {
                    Assertions.assertEquals("value:" + i, text(runtime.get(tx, shard, key(i), snapshot).orElseThrow()));
                }
            }
            runtime.rollback(tx);
            assertTrue(runtime.findCommitted(handle.receipt().transactionId()).isPresent());
            runtime.collectGarbage(32);
            Assertions.assertEquals(9, rows(targetA));
            Assertions.assertEquals(9, rows(targetB));
        }
    }

    /**
     * When the data file is modified, it is rejected before creating a recovery database table and target WAL directory.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void damagedDataIsRejectedBeforeTargetsAreInitialized() throws Exception {
        var handle = makeBackup(5, false);
        Path data = handle.archive().resolve("data/0000.bin");
        byte[] bytes = Files.readAllBytes(data);
        bytes[bytes.length / 2] ^= 1;
        Files.write(data, bytes);
        Path target = directory.resolve("damaged-target");
        assertThrows(IOException.class, () -> WalBackup.restore(handle.archive(), target, targets(), WalOptions.defaults(), limits()));
        assertFalse(Files.exists(target));
        for (var pool : List.of(targetA, targetB)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("show tables")) {
                assertFalse(rows.next());
            }
        }
    }

    /**
     * Empty database snapshot maintains zero borders and can start the first cross-shard transaction normally after recovery.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void emptyMaterializedEngineCanBeRestored() throws Exception {
        Path backup = directory.resolve("empty-db-backup");
        try (var runtime = WalMvccRuntime.open(directory.resolve("empty-db-source"), sources(), WalOptions.defaults())) {
            Assertions.assertEquals(0, runtime.backup(backup, limits()).sequence());
        }
        var targets = targets();
        Path target = directory.resolve("empty-db-target");
        WalBackup.restore(backup, target, targets, WalOptions.defaults(), limits());
        try (var runtime = WalMvccRuntime.open(target, targets, WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "first", bytes("one"));
            runtime.put(tx, "b", "first", bytes("one"));
            Assertions.assertEquals(1, runtime.commit(tx).commitSequence());
            runtime.flush();
            Assertions.assertEquals(1, rows(targetA));
            Assertions.assertEquals(1, rows(targetB));
        }
    }

    /**
     * Saves a real cross-shard backup generated by transaction and deletes the pre- version with old RR protection in the source.
     *
     * @param count count
     * @param deletion deletion
     * @return the operation result
     */
    private Handle makeBackup(final int count, final boolean deletion) {
        Path backup = directory.resolve("backup");
        WalReceipt receipt;
        long cut;
        try (var runtime = WalMvccRuntime.open(directory.resolve("source"), sources(), WalOptions.defaults().withHotCacheBytes(0))) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < count; i++) {
                for (String shard : new String[]{"a", "b"}) {
                    runtime.put(tx, shard, key(i), bytes("value:" + i));
                }
            }
            receipt = runtime.commit(tx);
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            if (deletion) {
                var remove = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.delete(remove, "a", key(0));
                runtime.delete(remove, "b", key(0));
                receipt = runtime.commit(remove);
            }
            cut = runtime.backup(backup, limits()).sequence();
            Assertions.assertEquals("value:0", text(runtime.get(old, "a", key(0)).orElseThrow()));
            runtime.rollback(old);
        }
        return new Handle(backup, cut, receipt);
    }

    /**
     * Source database maintains an independent route, recovery will not reuse these targets.
     *
     * @return the operation result
     */
    private Map<String, WalMaterializedStore> sources() {
        return Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
    }

    /**
     * Recovery Simulates a completely new storage medium using another unique database in the same example.
     *
     * @return the operation result
     */
    private Map<String, WalMaterializedStore> targets() {
        return Map.of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB));
    }

    /**
     * Test budget limited to 32 MiB, importing a maximum of 256 keys and 1 MiB batch.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(32L * 1024 * 1024, 256, 1024 * 1024, 120_000);
    }

    /**
     * Constructs a stable binary sequence so that the complete data summary can be repeated.
     *
     * @param ordinal ordinal
     * @return the operation result
     */
    private static String key(final int ordinal) {
        return String.format(java.util.Locale.ROOT, "key:%05d", ordinal);
    }

    /**
     * Create bounded recovery connection pool, receipt is a test container only.
     *
     * @param container container
     * @return the operation result
     */
    private static HikariDataSource targetPool(final MySQLContainer<?> container) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl().replace("/wal_checkpoint", "/wal_restore_target"));
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /**
     * Checks the full physical lines of the recovery back end directly by bypassing the frame cache.
     *
     * @param pool pool
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long rows(final HikariDataSource pool) throws Exception {
        try (var connection = pool.getConnection(); var statement = connection.createStatement(); var row = statement.executeQuery("select count(*) from azir_wal_version")) {
            row.next();
            return row.getLong(1);
        }
    }

    /**
     * Checks all original business values and version on a line-by-line independent SQL, without relying on the backup encoder 's own validate result.
     *
     * @param pool pool
     * @param expected expected
     * @throws Exception if the operation cannot complete
     */
    private static void assertPhysicalValues(final HikariDataSource pool, final int expected) throws Exception {
        try (var connection = pool.getConnection(); var query = connection.prepareStatement("select user_key,commit_seq,payload from azir_wal_version order by user_key")) {
            query.setFetchSize(Integer.MIN_VALUE);
            try (var rows = query.executeQuery()) {
                int ordinal = 0;
                while (rows.next()) {
                    Assertions.assertEquals(key(ordinal), new String(rows.getBytes(1), java.nio.charset.StandardCharsets.UTF_8));
                    Assertions.assertEquals(1, rows.getLong(2));
                    Assertions.assertEquals("value:" + ordinal, text(rows.getBytes(3)));
                    ordinal++;
                }
                Assertions.assertEquals(expected, ordinal);
            }
        }
    }

    /**
     * Keep backup identity, borders and operations to confirm transaction.
     *
     * @param archive archive
     * @param cut cut
     * @param receipt receipt
     */
    private record Handle(Path archive, long cut, WalReceipt receipt) {
    }
}

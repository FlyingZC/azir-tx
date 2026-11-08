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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

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

/**
 * Two authentic database -limited backups and recovery -strike matrices, each of which preserves the reversible cold data and receipt evidence.
 */
@Testcontainers
class WalBackupCrashTest {

    @Container
    static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();

    @Container
    static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();

    @TempDir
    Path directory;

    /**
     * Creates a separate recovery database, source and target are not shared protocol tables.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeAll
    static void setupTargets() throws Exception {
        for (var container : List.of(A, B)) {
            var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                    "create database wal_restore_target; grant all on wal_restore_target.* to 'wal_test'@'%'");
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        }
    }

    /**
     * The normal return takes place once each time, and the formal matrix is marked by the number of repetitions specified by parameters.
     *
     * @return the operation result
     */
    static Stream<String> boundaries() {
        return java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.backupCrashRepeats", 1)).boxed().flatMap(round -> Stream.of(
                "AFTER_BACKUP_FILE_COPY", "BEFORE_BACKUP_COMPLETE", "AFTER_BACKUP_COMPLETE", "AFTER_RESTORE_FILE_COPY",
                "AFTER_RESTORE_BATCH", "BEFORE_RESTORE_PUBLISH", "AFTER_RESTORE_MOVE", "AFTER_RESTORE_READY"));
    }

    /**
     * Confirms that the source is still correct, part of recovery cannot be opened, and that the release has been completed by the same backup retry or confirmation.
     *
     * @param boundary boundary
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("boundaries")
    void crashArtifactsRemainFailClosedAndRecoverable(final String boundary) throws Exception {
        long started = System.nanoTime();
        String saved = System.getProperty("azir.wal.backupCrashEvidence");
        if (saved != null) {
            Path root = Path.of(saved);
            Files.createDirectories(root);
            directory = Files.createTempDirectory(root, boundary + "-");
        }
        String targetUrlA = A.getJdbcUrl().replace("/wal_checkpoint", "/wal_restore_target");
        String targetUrlB = B.getJdbcUrl().replace("/wal_checkpoint", "/wal_restore_target");
        try (var a = WalCheckpointMysqlSupport.pool(A); var b = WalCheckpointMysqlSupport.pool(B); var targetA = pool(targetUrlA); var targetB = pool(targetUrlB)) {
            for (var source : new javax.sql.DataSource[]{a, b, targetA, targetB}) {
                try (var connection = source.getConnection(); var statement = connection.createStatement()) {
                    statement.executeUpdate("drop table if exists azir_wal_version");
                    statement.executeUpdate("drop table if exists azir_wal_binding");
                }
            }
            var sources = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
            var targets = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB));
            boolean backupCrash = boundary.contains("BACKUP");
            long cut = backupCrash ? 1 : 2;
            if (!backupCrash) {
                prepareBackup(sources);
            }
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-XX:ActiveProcessorCount=2", "-cp", cp,
                    WalBackupCrashProcess.class.getName(), directory.toString(), boundary, A.getJdbcUrl(), B.getJdbcUrl(), targetUrlA, targetUrlB)
                    .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
            try {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (child.isAlive() && !Files.exists(directory.resolve("ready")) && System.nanoTime() < until) {
                    TimeUnit.MILLISECONDS.sleep(10);
                }
                assertTrue(Files.exists(directory.resolve("ready")), () -> {
                    try {
                        return Files.readString(directory.resolve("child.log"));
                    } catch (final IOException failure) {
                        return failure.toString();
                    }
                });
            } finally {
                child.destroyForcibly();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS));
            }
            Assertions.assertNotEquals(0, child.exitValue());
            UUID seed = UUID.fromString(Files.readString(directory.resolve("seed-id")));
            Path backup = directory.resolve("backup");
            Path target = directory.resolve("restored");
            if (backupCrash) {
                if (boundary.equals("AFTER_BACKUP_COMPLETE")) {
                    Assertions.assertEquals(cut, WalBackup.inspect(backup, WalBackupCrashProcess.limits()).sequence());
                } else {
                    assertThrows(IOException.class, () -> WalBackup.inspect(directory.resolve("backup"), WalBackupCrashProcess.limits()));
                }
                try (var runtime = WalMvccRuntime.open(directory.resolve("source"), sources, WalOptions.defaults().withHotCacheBytes(0))) {
                    verify(runtime, false);
                    assertTrue(runtime.findCommitted(seed).isPresent());
                    if (!boundary.equals("AFTER_BACKUP_COMPLETE")) {
                        backup = directory.resolve("retry-backup");
                        runtime.backup(backup, WalBackupCrashProcess.limits());
                    }
                }
                WalBackup.restore(backup, target, targets, WalOptions.defaults().withHotCacheBytes(0), WalBackupCrashProcess.limits());
            } else if (!boundary.equals("AFTER_RESTORE_READY")) {
                Path partial;
                if (Files.exists(target)) {
                    partial = target;
                } else {
                    try (var entries = Files.list(directory)) {
                        partial = entries.filter(path -> Files.isDirectory(path) && path.getFileName().toString().startsWith(".azir-restore-")).findFirst().orElseThrow();
                    }
                }
                assertTrue(Files.exists(partial.resolve("RESTORING")));
                assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(partial, targets, WalOptions.defaults()));
                WalBackup.restore(backup, target, targets, WalOptions.defaults().withHotCacheBytes(0), WalBackupCrashProcess.limits());
            }
            try (var runtime = WalMvccRuntime.open(target, targets, WalOptions.defaults().withHotCacheBytes(0))) {
                Assertions.assertEquals(cut, runtime.stats().publishedSequence());
                verify(runtime, !backupCrash);
                assertTrue(runtime.findCommitted(seed).isPresent());
                if (!backupCrash) {
                    Assertions.assertEquals(2, runtime.findCommitted(UUID.fromString(Files.readString(directory.resolve("delete-id")))).orElseThrow().commitSequence());
                }
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.put(tx, "a", "next", bytes("next"));
                runtime.put(tx, "b", "next", bytes("next"));
                Assertions.assertEquals(cut + 1, runtime.commit(tx).commitSequence());
                runtime.flush();
                runtime.checkpoint();
            }
            if (saved != null) {
                Files.writeString(directory.resolve("verification.json"), "{\"status\":\"passed\",\"boundary\":\"" + boundary + "\",\"childExitCode\":" + child.exitValue()
                        + ",\"restoredSequence\":" + cut + ",\"nextCommitSequence\":" + (cut + 1) + ",\"physicalProcessKill\":true,\"keysCheckedEachShard\":20}\n");
            }
        } finally {
            long remaining = TimeUnit.SECONDS.toNanos(1) - (System.nanoTime() - started);
            if (remaining > 0) {
                TimeUnit.NANOSECONDS.sleep(remaining);
            }
        }
    }

    /**
     * Two real commit s form a complete backup that contains the deletion of tombstone, and the old RR retains the database version before it is deleted.
     *
     * @param sources sources
     * @throws Exception if the operation cannot complete
     */
    private void prepareBackup(final Map<String, WalMaterializedStore> sources) throws Exception {
        try (var runtime = WalMvccRuntime.open(directory.resolve("source"), sources, WalOptions.defaults().withHotCacheBytes(0))) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 20; i++) {
                for (String shard : sources.keySet()) {
                    runtime.put(tx, shard, "k" + i, bytes("v:" + i));
                }
            }
            runtime.commit(tx);
            Files.writeString(directory.resolve("seed-id"), tx.id().toString());
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k0");
            runtime.delete(deletion, "b", "k0");
            runtime.commit(deletion);
            Files.writeString(directory.resolve("delete-id"), deletion.id().toString());
            runtime.backup(directory.resolve("backup"), WalBackupCrashProcess.limits());
            runtime.rollback(reader);
        }
    }

    /**
     * Independent operations expect to cross-check two complete snapshot s, which cannot be compared with just two equals.
     *
     * @param runtime runtime
     * @param deleted deleted
     */
    private static void verify(final WalMvccRuntime runtime, final boolean deleted) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        var snapshot = runtime.beginReadStatement(tx);
        for (int i = 0; i < 20; i++) {
            for (String shard : new String[]{"a", "b"}) {
                var value = runtime.get(tx, shard, "k" + i, snapshot);
                if (deleted && i == 0) {
                    assertTrue(value.isEmpty());
                } else {
                    Assertions.assertEquals("v:" + i, text(value.orElseThrow()));
                }
            }
        }
        runtime.rollback(tx);
    }

    /**
     * Connection and timeout of recovery database maintain the same resource boundary as the source.
     *
     * @param url url
     * @return the operation result
     */
    private static HikariDataSource pool(final String url) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("wal_test");
        config.setPassword("wal_test");
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }
}

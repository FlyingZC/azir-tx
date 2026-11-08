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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Four of the two restricted database exclusive schema verify cold data migration, receiving ownership and partial recovery isolation.
 */
@Testcontainers
class WalReplicaSnapshotMysqlTest {

    @Container
    static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();

    @Container
    static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();

    @TempDir
    Path directory;

    private HikariDataSource a, b, targetA, targetB;

    /**
     * Only the receiving space is created in this test container and it is not clear that there is an empty or moving business database.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeAll
    static void createTargets() throws Exception {
        for (var container : List.of(A, B)) {
            var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                    "create database wal_replica_receive; grant all on wal_replica_receive.* to 'wal_test'@'%'");
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        }
    }

    /**
     * Each test was replaced by four sets of tables and the ownership and recovery fingerprints of the previous item could not be reused.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        a = WalCheckpointMysqlSupport.pool(A);
        b = WalCheckpointMysqlSupport.pool(B);
        targetA = target(A);
        targetB = target(B);
        for (var pool : List.of(a, b, targetA, targetB)) {
            try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
                ddl.executeUpdate("drop table if exists azir_wal_version");
                ddl.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /**
     * Connection pool is only owned by this test and the packagings are tested for life cycle collection.
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
     * The source hotspot cache and local tailing log are empty, receiving recovery cold data, tombstone and UUID continue with commit.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void fullColdStateAndReceiptsMoveToReceiversOwnBackendIdentity() throws Exception {
        var source = WalReplicaSnapshotTest.identity("source");
        var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path backup = directory.resolve("backup");
        var authority = new WalReplicaSnapshotTest.Authority();
        WalReceipt first;
        WalReceipt deleted;
        try (var runtime = open(directory.resolve("source"), source, sources(), authority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 64; i++) {
                for (String shard : Set.of("a", "b")) {
                    runtime.put(tx, shard, "k" + i, bytes(shard + ":" + i));
                }
            }
            first = runtime.commit(tx);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : Set.of("a", "b")) {
                runtime.delete(deletion, shard, "k0");
            }
            deleted = runtime.commit(deletion);
            Assertions.assertEquals(Map.of("a", 64L, "b", 64L), runtime.backupReplica(authority, backup, limits()).rows());
            Assertions.assertEquals(0, runtime.stats().retainedBytes());
            Assertions.assertEquals(100, runtime.stats().walBytes());
        }
        a.close();
        b.close();
        var targets = targets();
        Path restored = directory.resolve("restored");
        WalBackup.restoreReplica(backup, restored, targets, options(), limits(), receiver);
        for (var pool : List.of(targetA, targetB)) {
            try (
                    var connection = pool.getConnection(); var query = connection.createStatement();
                    var row = query.executeQuery("select replica_owner,restoring,applied_seq from azir_wal_binding where id=1")) {
                assertTrue(row.next());
                Assertions.assertEquals(receiver.fingerprint(), row.getString(1));
                assertFalse(row.getBoolean(2));
                Assertions.assertEquals(2, row.getLong(3));
            }
        }
        var receiverAuthority = new WalReplicaSnapshotTest.Authority();
        try (var runtime = open(restored, receiver, targets, receiverAuthority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var view = runtime.beginReadStatement(tx);
            for (int i = 0; i < 64; i++) {
                for (String shard : Set.of("a", "b")) {
                    if (i == 0) {
                        assertTrue(runtime.get(tx, shard, "k0", view).isEmpty());
                    } else {
                        Assertions.assertEquals(shard + ":" + i, text(runtime.get(tx, shard, "k" + i, view).orElseThrow()));
                    }
                }
            }
            runtime.rollback(tx);
            Assertions.assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow());
            Assertions.assertEquals(deleted, runtime.findCommitted(deleted.transactionId()).orElseThrow());
            Assertions.assertEquals(3, WalReplicaSnapshotTest.pair(runtime, "next").commitSequence());
            runtime.flush();
        }
        assertThrows(IllegalStateException.class, () -> targets.get("a").initialize(source.engineId(), "a"));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(targetA).initializeReplica(source, "a"));
    }

    /**
     * Batch Interrupts the retention and receipt of identity, error nodes and duplicate directories cannot be taken over, and the original operation can be safely continued.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void partialColdRestoreRejectsWrongOwnerAndResumesOnlySameOperation() throws Exception {
        var source = WalReplicaSnapshotTest.identity("source");
        var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path backup = directory.resolve("backup");
        var authority = new WalReplicaSnapshotTest.Authority();
        try (var runtime = open(directory.resolve("source"), source, sources(), authority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 8; i++) {
                for (String shard : Set.of("a", "b")) {
                    runtime.put(tx, shard, "k" + i, bytes(shard + ":" + i));
                }
            }
            runtime.commit(tx);
            runtime.backupReplica(authority, backup, limits());
        }
        Path target = directory.resolve("restored");
        var targets = targets();
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreReplicaForTest(backup, target, targets, options(), limits(), receiver,
                stage -> {
                    if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_BATCH) {
                        throw new IllegalStateException("interrupted cold import");
                    }
                }, (path, channel) -> channel.force(true)));
        assertFalse(Files.exists(target));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(targetA).initializeReplica(receiver, "a"));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(targetA).initialize(source.engineId(), "a"));
        var other = new WalReplicaIdentity(source.engineId(), source.groupId(), "other");
        assertThrows(java.io.IOException.class, () -> WalBackup.restoreReplica(backup, target, targets(), options(), limits(), other));
        WalBackup.restoreReplica(backup, target, targets(), options(), limits(), receiver);
        assertThrows(WalMvccException.class, () -> WalBackup.restoreReplica(backup, directory.resolve("duplicate-target"), targets(), options(), limits(), receiver));
        var restoredAuthority = new WalReplicaSnapshotTest.Authority();
        try (var runtime = open(target, receiver, targets(), restoredAuthority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("a:7", text(runtime.get(tx, "a", "k7").orElseThrow()));
            Assertions.assertEquals("b:7", text(runtime.get(tx, "b", "k7").orElseThrow()));
            runtime.rollback(tx);
        }
    }

    /**
     * Replica Cache Disable to avoid thermal data masking backend mirror export or recovery missing.
     *
     * @return the operation result
     */
    private static WalOptions options() {
        return WalOptions.defaults().withHotCacheBytes(0);
    }

    /**
     * Two rows of real recovery must pass a limited number of batch.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(16_000_000, 2, 64_000, 60_000);
    }

    /**
     * Provides a stand-alone backend for the current scene and binds the test to a credible port.
     *
     * @param path path
     * @param identity identity
     * @param stores stores
     * @param authority authority
     * @return the operation result
     */
    private static WalMvccRuntime open(final Path path, final WalReplicaIdentity identity, final Map<String, WalMaterializedStore> stores, final WalReplicaSnapshotTest.Authority authority) {
        var runtime = WalMvccRuntime.openReplica(path, Set.of("a", "b"), stores, options(), WalRetentionOptions.preserveAll(), identity, authority);
        authority.runtime = runtime;
        return runtime;
    }

    /**
     * The source nodes each have their own logic database.
     *
     * @return the operation result
     */
    private Map<String, WalMaterializedStore> sources() {
        return Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
    }

    /**
     * Every time you return to the new adaptor object, ensure that ownership is guaranteed by database validate rather than object caches.
     *
     * @return the operation result
     */
    private Map<String, WalMaterializedStore> targets() {
        return Map.of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB));
    }

    /**
     * Receive connection pool only the schema explicitly created by this test, following limited connection and socket timeout.
     *
     * @param container container
     * @return the operation result
     */
    private static HikariDataSource target(final MySQLContainer<?> container) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl().replace("/" + container.getDatabaseName(), "/wal_replica_receive"));
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        return new HikariDataSource(config);
    }
}

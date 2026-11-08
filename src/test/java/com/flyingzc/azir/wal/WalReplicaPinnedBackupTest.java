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
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * The real MySQL verify of the snapshot fixed-read view concurrent commit, materialization, GC and recovery borders.
 */
@Testcontainers
class WalReplicaPinnedBackupTest {

    @Container
    static final MySQLContainer<?> DATABASE = WalCheckpointMysqlSupport.container();

    @TempDir
    Path directory;

    private final List<HikariDataSource> pools = new ArrayList<>();

    /**
     * Only connection pool held in this round is closed, and test examples are managed through the life cycle.
     */
    @AfterEach
    void closePools() {
        for (var pool : pools) {
            pool.close();
        }
    }

    /**
     * Commit and physical GC after a fixed time cannot enter the mirror or delete the data that should be retained.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void pinnedViewsSurviveConcurrentPublicationCheckpointAndGc() throws Exception {
        var identity = WalReplicaSnapshotTest.identity("source");
        var authority = new WalReplicaSnapshotTest.Authority();
        var sourceStores = stores("source");
        Path backup = directory.resolve("backup");
        WalReceipt first;
        try (
                var source = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), sourceStores, WalOptions.defaults().withHotCacheBytes(0), WalRetentionOptions.preserveAll(),
                        identity, authority)) {
            authority.runtime = source;
            var initial = source.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) {
                source.put(initial, shard, "k", bytes("old"));
                source.put(initial, shard, "gone", bytes("present"));
            }
            first = source.commit(initial);
            try (var plan = source.prepareReplicaBackup(authority, backup, limits())) {
                Assertions.assertEquals(1, plan.boundary().sequence());
                assertTrue(source.lifecycle().backup());
                var newer = source.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : List.of("a", "b")) {
                    source.put(newer, shard, "k", bytes("new"));
                    source.delete(newer, shard, "gone");
                    source.put(newer, shard, "future", bytes("new"));
                }
                Assertions.assertEquals(2, source.commit(newer).commitSequence());
                source.flush();
                for (int i = 0; i < 8; i++) {
                    source.collectGarbageBounded(32, 128, 1000);
                }
                assertTrue(source.checkpoint().created());
                Assertions.assertEquals(1, plan.complete().sequence());
                Assertions.assertEquals(2, source.stats().publishedSequence());
                assertFalse(source.lifecycle().backup());
            }
            Assertions.assertEquals(3, WalReplicaSnapshotTest.pair(source, "later").commitSequence());
        }
        var receiver = new WalReplicaIdentity(identity.engineId(), identity.groupId(), "receiver");
        var targetStores = stores("target");
        Path target = directory.resolve("target");
        WalBackup.restoreReplica(backup, target, targetStores, WalOptions.defaults(), limits(), receiver);
        var port = new WalReplicaSnapshotTest.Authority();
        try (var restored = WalMvccRuntime.openReplica(target, Set.of("a", "b"), targetStores, WalOptions.defaults().withHotCacheBytes(0), WalRetentionOptions.preserveAll(), receiver, port)) {
            port.runtime = restored;
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            var view = restored.beginReadStatement(tx);
            for (String shard : List.of("a", "b")) {
                Assertions.assertEquals("old", text(restored.get(tx, shard, "k", view).orElseThrow()));
                Assertions.assertEquals("present", text(restored.get(tx, shard, "gone", view).orElseThrow()));
                assertTrue(restored.get(tx, shard, "future", view).isEmpty());
            }
            restored.rollback(tx);
            Assertions.assertEquals(first, restored.findCommitted(first.transactionId()).orElseThrow());
            Assertions.assertEquals(2, WalReplicaSnapshotTest.pair(restored, "receiver").commitSequence());
        }
    }

    /**
     * The waiver of a plan that has not yet been exported cannot release COMPLETE, connection and maintenance eligibility to continue.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void abandoningPinnedPlanReleasesViewsAndKeepsSourceWritable() throws Exception {
        var identity = WalReplicaSnapshotTest.identity("source");
        var authority = new WalReplicaSnapshotTest.Authority();
        var stores = stores("abandon");
        try (var source = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), stores, WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = source;
            WalReplicaSnapshotTest.pair(source, "one");
            var plan = source.prepareReplicaBackup(authority, directory.resolve("partial"), limits());
            plan.close();
            plan.close();
            assertFalse(source.lifecycle().backup());
            assertFalse(Files.exists(directory.resolve("partial/COMPLETE")));
            assertThrows(java.io.IOException.class, () -> WalBackup.inspect(directory.resolve("partial"), limits()));
            Assertions.assertEquals(2, WalReplicaSnapshotTest.pair(source, "two").commitSequence());
            try (var retry = source.prepareReplicaBackup(authority, directory.resolve("complete"), limits())) {
                Assertions.assertEquals(2, retry.complete().sequence());
            }
        }
    }

    /**
     * Preparing for budget failed to release maintenance eligibility, and subsequent transaction and new backups are still functioning properly.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void preparationFailureDoesNotLeaveRuntimeInBackupMode() throws Exception {
        var identity = WalReplicaSnapshotTest.identity("source");
        var authority = new WalReplicaSnapshotTest.Authority();
        try (var source = WalReplicaSnapshotTest.open(directory.resolve("source"), identity, authority)) {
            var tx = source.begin(IsolationLevel.REPEATABLE_READ);
            source.put(tx, "a", "large", new byte[16 * 1024]);
            source.commit(tx);
            assertThrows(WalMvccException.class, () -> source.prepareReplicaBackup(authority, directory.resolve("small"), new WalBackupOptions(1024, 8, 64000, 30_000)));
            assertFalse(source.lifecycle().backup());
            Assertions.assertEquals(2, WalReplicaSnapshotTest.pair(source, "later").commitSequence());
            try (var next = source.prepareReplicaBackup(authority, directory.resolve("valid"), limits())) {
                Assertions.assertEquals(2, next.complete().sequence());
            }
        }
    }

    /**
     * Creates an exclusive schema per round of this instance without using data space from other tests.
     *
     * @param prefix prefix
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Map<String, WalMaterializedStore> stores(final String prefix) throws Exception {
        var stores = new HashMap<String, WalMaterializedStore>();
        for (String shard : List.of("a", "b")) {
            String schema = "p_" + prefix + "_" + shard + "_" + UUID.randomUUID().toString().replace("-", "");
            try (var admin = WalCheckpointMysqlSupport.pool(DATABASE); var connection = admin.getConnection(); var query = connection.createStatement()) {
                // The library is built using only the container root account of this test, whose name is derived from fixed characters and UUID.
                var result = DATABASE.execInContainer("mysql", "-uroot", "-p" + DATABASE.getPassword(), "-e", "create database `" + schema + "`; grant all on `" + schema + "`.* to 'wal_test'@'%'");
                Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
            }
            var config = new HikariConfig();
            config.setJdbcUrl(DATABASE.getJdbcUrl());
            config.setCatalog(schema);
            config.setUsername(DATABASE.getUsername());
            config.setPassword(DATABASE.getPassword());
            config.setMaximumPoolSize(3);
            config.setMinimumIdle(0);
            config.setConnectionTimeout(5000);
            var pool = new HikariDataSource(config);
            pools.add(pool);
            stores.put(shard, new MysqlWalMaterializedStore(pool));
        }
        return Map.copyOf(stores);
    }

    /**
     * This round of small mirrors has fixed bytes and time budget.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(32_000_000, 8, 1_000_000, 30_000);
    }
}

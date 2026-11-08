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
import com.flyingzc.azir.wal.WalOptions;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Three stand-alone MySQL cold data cannot be used to disguise replica with shared database or unset disk memory.
 */
class RatisMysqlCopiesTest {

    @TempDir
    Path directory;

    /**
     * After cutting local WAL and phasing out all hot spots, the original leader and his MySQL are still offline to read confirmed data.
     *
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    @Test
    void independentColdCopiesSurviveOneWholeNodeUnavailable() throws Exception {
        var databases = List.of(database(), database(), database());
        var nodes = new ArrayList<RatisWalNode>();
        var pools = new ArrayList<HikariDataSource>();
        try {
            Startables.deepStart(databases.stream()).join();
            for (var database : databases) {
                var result = database.execInContainer("mysql", "-uroot", "-p" + database.getPassword(), "-e",
                        "create database ha_b; grant all on ha_b.* to 'wal_ha'@'%'");
                Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
                var host = database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec().getHostConfig();
                Assertions.assertEquals(250_000_000L, host.getNanoCPUs());
                Assertions.assertEquals(512L * 1024 * 1024, host.getMemory());
            }
            var peers = new ArrayList<RaftPeer>();
            for (int i = 0; i < 3; i++) {
                try (var socket = MysqlGenerationSupport.reservePeerPort()) {
                    peers.add(RaftPeer.newBuilder().setId("sql" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            }
            var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers);
            for (int i = 0; i < 3; i++) {
                var a = pool(databases.get(i), "ha_a");
                var b = pool(databases.get(i), "ha_b");
                pools.add(a);
                pools.add(b);
                var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
                nodes.add(RatisWalNode.openLoopback(directory.resolve("sql" + i), RaftPeerId.valueOf("sql" + i), group, Set.of("a", "b"), stores,
                        WalOptions.defaults().withTimeout(10_000).withHotCacheBytes(0)));
            }
            int first = leader(nodes, -1);
            var node = nodes.get(first);
            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            node.put(tx, "a", "balance", bytes("90"));
            node.put(tx, "b", "balance", bytes("110"));
            var receipt = node.commit(tx);
            for (var replica : nodes) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (replica.appliedSequence() < 1 || replica.localStats().materializedSequences().values().stream().anyMatch(sequence -> sequence < 1)) {
                    if (System.nanoTime() > until) {
                        throw new AssertionError("cold copy did not materialize");
                    }
                    TimeUnit.MILLISECONDS.sleep(20);
                }
                assertTrue(replica.checkpointLocal().created());
                Assertions.assertEquals(100, replica.localStats().walBytes());
                Assertions.assertEquals(0, replica.localStats().retainedBytes());
            }
            node.close();
            nodes.set(first, null);
            pools.get(first * 2).close();
            pools.get(first * 2 + 1).close();
            databases.get(first).stop();
            int next = leader(nodes, first);
            var serving = nodes.get(next);
            var reader = serving.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = serving.beginReadStatement(reader);
            Assertions.assertEquals("90", new String(serving.get(reader, "a", "balance", snapshot).orElseThrow(), StandardCharsets.UTF_8));
            Assertions.assertEquals("110", new String(serving.get(reader, "b", "balance", snapshot).orElseThrow(), StandardCharsets.UTF_8));
            serving.rollback(reader);
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, serving.lookupCommit(receipt.transactionId()).status());
            var update = serving.begin(IsolationLevel.REPEATABLE_READ);
            serving.put(update, "a", "balance", bytes("80"));
            serving.put(update, "b", "balance", bytes("120"));
            Assertions.assertEquals(2, serving.commit(update).commitSequence());
        } finally {
            Exception error = null;
            for (var node : nodes) {
                if (node != null) {
                    try {
                        node.close();
                    } catch (final Exception failure) {
                        if (error == null) {
                            error = failure;
                        } else {
                            error.addSuppressed(failure);
                        }
                    }
                }
            }
            for (var pool : pools) {
                pool.close();
            }
            for (var database : databases) {
                database.stop();
            }
            if (error != null) {
                throw error;
            }
        }
    }

    /**
     * Three database s of a maximum of three-quarters of the core, and the number of buffer ponds and connection s are also limited.
     *
     * @return the operation result
     */
    private static MySQLContainer<?> database() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("ha_a").withUsername("wal_ha").withPassword("wal_ha")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000").withUrlParam("rewriteBatchedStatements", "true")
                .withCommand("--innodb-buffer-pool-size=64M", "--performance-schema=OFF", "--max-connections=32", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1")
                .withStartupTimeout(Duration.ofMinutes(3)).withCreateContainerCmdModifier(command -> command.getHostConfig().withNanoCPUs(250_000_000L).withMemory(512L * 1024 * 1024));
    }

    /**
     * Every logic shard uses a stand-alone schema, testing the source URL will not be used if it is not replaced.
     *
     * @param database database
     * @param schema schema
     * @return the operation result
     */
    private static HikariDataSource pool(final MySQLContainer<?> database, final String schema) {
        var config = new HikariConfig();
        config.setJdbcUrl(database.getJdbcUrl().replace("/ha_a", "/" + schema));
        config.setUsername(database.getUsername());
        config.setPassword(database.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        return new HikariDataSource(config);
    }

    /**
     * Observe that real players are ready and do not count the sequence of start-up as an election result.
     *
     * @param nodes nodes
     * @param excluded excluded
     * @return the operation result
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static int leader(final List<RatisWalNode> nodes, final int excluded) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < until) {
            for (int i = 0; i < nodes.size(); i++) {
                if (i != excluded && nodes.get(i) != null && nodes.get(i).leaderEpoch() > 0) {
                    return i;
                }
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        throw new AssertionError("no ready leader");
    }

    /**
     * The two pieces use different values to prevent a comparison between the two sides of an equal mask error.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

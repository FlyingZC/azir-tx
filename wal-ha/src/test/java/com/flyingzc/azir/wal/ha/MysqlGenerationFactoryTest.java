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

import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real MySQL generation distribution, failure continuation, unknown space rejection and concurrent level verify.
 */
class MysqlGenerationFactoryTest {

    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();

    private HikariDataSource control;

    @TempDir
    Path directory;

    /**
     * Test class only creates an example of a limited resource; no connection user already has database.
     */
    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
    }

    /**
     * Each test creates an independent control library and avoids modifying the last registered space.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepareControl() throws Exception {
        String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
        MysqlGenerationSupport.createControl(DATABASE, schema);
        control = MysqlGenerationSupport.control(DATABASE, schema);
    }

    /**
     * Only close the control connection pool of the current test.
     */
    @AfterEach
    void closeControl() {
        if (control != null) {
            control.close();
        }
    }

    /**
     * Test complete to remove the only self-contained container.
     */
    @AfterAll
    static void stopDatabase() {
        DATABASE.stop();
    }

    /**
     * Reopen data for the same generation plant, new generation isolation; close lease without closing control pools or deleting old data.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void generationsRemainStableIsolatedAndPreserveCallerControls() throws Exception {
        var identity = identity();
        var first = UUID.randomUUID();
        var second = UUID.randomUUID();
        var factory = factory();
        try (var lease = factory.open(identity, first)) {
            var store = lease.stores().get("a");
            store.initializeReplica(identity, "a");
            store.apply(List.of(new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", new byte[]{7})))));
        }
        assertFalse(control.isClosed());
        Assertions.assertEquals(factory.databaseName(identity, first, "a"), factory().databaseName(identity, first, "a"));
        try (var reopened = factory().open(identity, first); var separate = factory.open(identity, second)) {
            reopened.stores().get("a").initializeReplica(identity, "a");
            assertArrayEquals(new byte[]{7}, reopened.stores().get("a").get("k", 1).orElseThrow().value());
            separate.stores().get("a").initializeReplica(identity, "a");
            assertTrue(separate.stores().get("a").get("k", Long.MAX_VALUE).isEmpty());
            Assertions.assertNotEquals(factory.databaseName(identity, first, "a"), factory.databaseName(identity, second, "a"));
        }
        Assertions.assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /**
     * Both the unregistered library and those containing the schedule of operations refused to take over, with no change in their original content or distribution.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void preexistingUnknownDatabasesCannotBeAdoptedEvenWhenEmpty() throws Exception {
        var identity = identity();
        var factory = factory();
        for (boolean withData : List.of(false, true)) {
            UUID generation = UUID.randomUUID();
            String schema = factory.databaseName(identity, generation, "a");
            execute("create database `" + schema + "`");
            if (withData) {
                execute("create table `" + schema + "`.business_data (id int primary key) engine=InnoDB");
                execute("insert into `" + schema + "`.business_data values (7)");
            }
            assertThrows(IOException.class, () -> factory.open(identity, generation));
            assertTrue(databaseExists(schema));
            if (withData) {
                Assertions.assertEquals(7, scalar("select id from `" + schema + "`.business_data"));
            }
        }
        Assertions.assertEquals(0, scalar("select count(*) from azir_wal_generation_catalog"));
        Assertions.assertEquals(0, scalar("select coalesce(sum(used_slots),0) from azir_wal_namespace_budget"));
    }

    /**
     * The loss of the READY post- database or identity tag can only be refused, and the already used generation cannot be rebuilt into an empty library.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void readyDatabaseLossAndWrongOwnerFailClosed() throws Exception {
        var identity = identity();
        var generation = UUID.randomUUID();
        var factory = factory();
        String schema = factory.databaseName(identity, generation, "a");
        try (var lease = factory.open(identity, generation)) {
            lease.stores().get("a").initializeReplica(identity, "a");
        }
        execute("update `" + schema + "`.azir_wal_generation set owner='" + "0".repeat(64) + "'");
        assertThrows(IOException.class, () -> factory.open(identity, generation));
        Assertions.assertEquals(1, scalar("select state from azir_wal_generation_catalog"));
        execute("update `" + schema + "`.azir_wal_generation set owner='" + MysqlWalReplicaBackendFactory.owner(identity, generation, "a", schema) + "'");
        execute("drop table `" + schema + "`.azir_wal_version");
        assertThrows(IOException.class, () -> factory.open(identity, generation));
        execute("drop database `" + schema + "`");
        assertThrows(IOException.class, () -> factory.open(identity, generation));
        assertFalse(databaseExists(schema));
    }

    /**
     * Concurrent set-ups cannot break budget, refuse to occur in registration transaction, and successful lease can still be closed on its own.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentAllocationCannotOversubscribeGenerationAndSchemaBudgets() throws Exception {
        var identity = identity();
        var factory = new MysqlWalReplicaBackendFactory(Map.of("a", control), new MysqlWalReplicaBackendFactory.Options("azw", 2, 2, 1));
        int success = 0;
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var jobs = new ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < 3; i++) {
                jobs.add(tasks.submit(() -> {
                    try (var ignored = factory.open(identity, UUID.randomUUID())) {
                        return true;
                    } catch (final IOException rejected) {
                        return false;
                    }
                }));
            }
            for (var job : jobs) {
                if (job.get(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    success++;
                }
            }
        }
        Assertions.assertEquals(2, success);
        Assertions.assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
        Assertions.assertEquals(2, scalar("select count(*) from azir_wal_generation_catalog"));
        assertFalse(control.isClosed());
    }

    /**
     * The error transaction engine cannot be masked by the same name control table, which does not allow the creation of a target space before it discovers that metadata are unreliable.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidControlSchemaIsRejectedBeforeTargetCreation() throws Exception {
        execute("create table azir_wal_namespace_budget (prefix varbinary(8) primary key,protocol_version int not null,max_slots int not null,used_slots int not null) engine=MyISAM");
        var identity = identity();
        var generation = UUID.randomUUID();
        var factory = factory();
        String schema = factory.databaseName(identity, generation, "a");
        assertThrows(IOException.class, () -> factory.open(identity, generation));
        assertFalse(databaseExists(schema));
    }

    /**
     * When the amount is miscalculated, no distribution can be made so that the number drifts as an infinity entry to create database.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptedBudgetCounterCannotAuthorizeMoreDatabases() throws Exception {
        var identity = identity();
        var factory = factory();
        try (var ignored = factory.open(identity, UUID.randomUUID())) {
        }
        execute("update azir_wal_namespace_budget set used_slots=0");
        UUID next = UUID.randomUUID();
        assertThrows(IOException.class, () -> factory.open(identity, next));
        assertFalse(databaseExists(factory.databaseName(identity, next, "a")));
    }

    /**
     * READY can be initialized by the same generation when the first transaction table DDL is interrupted without binding or data.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void uninitializedPartialProtocolDdlCanResumeSameGeneration() throws Exception {
        var identity = identity();
        UUID generation = UUID.randomUUID();
        var factory = factory();
        String schema = factory.databaseName(identity, generation, "a");
        try (var lease = factory.open(identity, generation)) {
            lease.stores().get("a").initializeReplica(identity, "a");
        }
        execute("delete from `" + schema + "`.azir_wal_binding");
        execute("drop table `" + schema + "`.azir_wal_version");
        try (var lease = factory.open(identity, generation)) {
            lease.stores().get("a").initializeReplica(identity, "a");
            Assertions.assertEquals(0, lease.stores().get("a").checkpoint().sequence());
        }
        Assertions.assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /**
     * The first node assembly failed after the READY plant, retry followed the original generation; consensus identity could not be reopened without voting files.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void initialNodeProvisionFailureKeepsStableBootstrapWithoutWeakeningVotingFence() throws Exception {
        var groupId = RaftGroupId.randomId();
        var peers = new ArrayList<RaftPeer>();
        var held = new ArrayList<java.net.ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                var socket = new java.net.ServerSocket(0);
                held.add(socket);
                peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
            }
        } finally {
            for (var socket : held) {
                socket.close();
            }
        }
        var group = RaftGroup.valueOf(groupId, peers);
        var armed = new AtomicBoolean(true);
        var broken = new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults(), hit -> {
            if (hit == MysqlWalReplicaBackendFactory.Stage.AFTER_READY && armed.compareAndSet(true, false)) {
                throw new IOException("initial provision interrupted");
            }
        });
        Path node = directory.resolve("node");
        var limits = new WalBackupOptions(8_000_000, 8, 64_000, 60_000);
        assertThrows(IOException.class, () -> RatisWalNode.openGenerationalLoopback(node, RaftPeerId.valueOf("n0"), group, Set.of("a"), broken, WalOptions.defaults(), limits));
        var bootstrap = RatisGenerationRecord.read(node.resolve("business/BOOTSTRAPPING"));
        assertFalse(Files.exists(node.resolve("business/ACTIVE")));
        try (var started = RatisWalNode.openGenerationalLoopback(node, RaftPeerId.valueOf("n0"), group, Set.of("a"), factory(), WalOptions.defaults(), limits)) {
            assertTrue(started.ready());
            Assertions.assertEquals(bootstrap.generation(), RatisGenerationRecord.read(node.resolve("business/ACTIVE")).generation());
            assertFalse(Files.exists(node.resolve("business/BOOTSTRAPPING")));
            Assertions.assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
        }
        Path voting = node.resolve("raft").resolve(groupId.getUuid().toString()).resolve("current/raft-meta");
        Files.delete(voting);
        assertThrows(IOException.class, () -> RatisWalNode.openGenerationalLoopback(node, RaftPeerId.valueOf("n0"), group, Set.of("a"), factory(), WalOptions.defaults(), limits));
        assertFalse(Files.exists(voting));
    }

    /**
     * The original generation retry does not repeat the amount of occupancy, either after registration, DDL, identity tags, READY or pool initialization has failed.
     *
     * @param stage stage
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(MysqlWalReplicaBackendFactory.Stage.class)
    void provisioningFaultResumesSameOwnedNamespace(final MysqlWalReplicaBackendFactory.Stage stage) throws Exception {
        var identity = identity();
        var generation = UUID.randomUUID();
        var armed = new AtomicBoolean(true);
        var broken = new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults(), hit -> {
            if (hit == stage && armed.compareAndSet(true, false)) {
                throw new IOException("injected provision boundary " + stage);
            }
        });
        assertThrows(IOException.class, () -> broken.open(identity, generation));
        assertFalse(control.isClosed());
        try (var restored = factory().open(identity, generation)) {
            restored.stores().get("a").initializeReplica(identity, "a");
            Assertions.assertEquals(0, restored.stores().get("a").checkpoint().sequence());
        }
        Assertions.assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
        Assertions.assertEquals(1, scalar("select count(*) from azir_wal_generation_catalog"));
        String schema = broken.databaseName(identity, generation, "a");
        try (var connection = control.getConnection(); var query = connection.prepareStatement("select count(*) from information_schema.processlist where db=?")) {
            query.setString(1, schema);
            try (var row = query.executeQuery()) {
                assertTrue(row.next());
                Assertions.assertEquals(0, row.getInt(1), "all factory-owned business pools must close");
            }
        }
    }

    /**
     * Each scenario has an independent business engine and node identity.
     *
     * @return the operation result
     */
    private static WalReplicaIdentity identity() {
        UUID group = UUID.randomUUID();
        return new WalReplicaIdentity(group, group, "factory-node");
    }

    /**
     * There's only one specialty at the moment, and a cluster dedicated to another verify full-scale double factory.
     *
     * @return the operation result
     */
    private MysqlWalReplicaBackendFactory factory() {
        return new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults());
    }

    /**
     * Fixed DDLs for this test only, with all dynamic names coming from the plant or UUID.
     *
     * @param sql sql
     * @throws Exception if the operation cannot complete
     */
    private void execute(final String sql) throws Exception {
        try (var connection = control.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    /**
     * The original SQL check is independent of the return value of the plant and avoids evidencing the amount with an updated memory count.
     *
     * @param sql sql
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private int scalar(final String sql) throws Exception {
        try (var connection = control.getConnection(); var query = connection.createStatement(); var row = query.executeQuery(sql)) {
            assertTrue(row.next());
            return row.getInt(1);
        }
    }

    /**
     * Direct verification of the existence of a target is not allowed after loss of READY space.
     *
     * @param schema schema
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private boolean databaseExists(final String schema) throws Exception {
        try (var connection = control.getConnection(); var query = connection.prepareStatement("select count(*) from information_schema.schemata where cast(schema_name as binary)=?")) {
            query.setBytes(1, schema.getBytes(StandardCharsets.US_ASCII));
            try (var row = query.executeQuery()) {
                row.next();
                return row.getInt(1) == 1;
            }
        }
    }
}

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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.wal.WalReplicaIdentity;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;

/**
 * The real MySQL retirement, the amount of one release, lease protection and non-rehabilitation.
 */
class MysqlRetirementTest {

    static final org.testcontainers.containers.MySQLContainer<?> DATABASE = MysqlGenerationSupport.database();

    private HikariDataSource control;

    /**
     * Only create a self-limited database.
     */
    @BeforeAll
    static void start() {
        DATABASE.start();
    }

    /**
     * Each operation is registered in the new control library.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
        MysqlGenerationSupport.createControl(DATABASE, schema);
        control = MysqlGenerationSupport.control(DATABASE, schema);
    }

    /**
     * Only this test control pool is closed.
     */
    @AfterEach
    void close() {
        control.close();
    }

    /**
     * Removes the only self-contained container.
     */
    @AfterAll
    static void stop() {
        DATABASE.stop();
    }

    /**
     * Shard cursor restricted to one library per wheel, retry tombstone was not re-released and the old generation would never be emptied.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void cursorReleasesQuotaExactlyOnceAndPreventsResurrection() throws Exception {
        var identity = identity();
        UUID old = UUID.randomUUID();
        var factory = factory();
        try (var ignored = factory.open(identity, old)) {
        }
        Assertions.assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
        assertThrows(IOException.class, () -> factory.open(identity, UUID.randomUUID()));
        var permit = permit(identity, old);
        var first = factory.retire(permit, "", 1);
        assertFalse(first.complete());
        Assertions.assertEquals(1, first.inspected());
        Assertions.assertEquals(1, scalar("select used_slots from azir_wal_namespace_budget"));
        assertTrue(factory.retire(permit, first.cursor(), 1).complete());
        Assertions.assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget"));
        assertTrue(factory.retire(permit, "", 2).complete());
        Assertions.assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget"));
        assertThrows(IOException.class, () -> factory.open(identity, old));
        try (var ignored = factory.open(identity, UUID.randomUUID())) {
        }
        Assertions.assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /**
     * The different plant objects must also see activity lease, which may not be reused after leaving the node to maintain context.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void activeLeaseAndRevokedPermitCannotDeleteSchemas() throws Exception {
        var identity = identity();
        UUID generation = UUID.randomUUID();
        var factory = factory();
        try (var lease = factory.open(identity, generation)) {
            assertThrows(IOException.class, () -> factory().retire(permit(identity, generation), "", 2));
            Assertions.assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
        }
        var revoked = new WalGenerationRetirement(identity, generation, () -> {
            throw new IOException("permit revoked");
        });
        assertThrows(IOException.class, () -> factory.retire(revoked, "", 2));
        Assertions.assertEquals(2, scalar("select count(*) from azir_wal_generation_catalog where state=1"));
    }

    /**
     * Unregistered library, error ownership and new unknown schedule cannot be DROP.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unknownOrWrongOwnedDatabaseCannotBeDropped() throws Exception {
        var identity = identity();
        UUID generation = UUID.randomUUID();
        var factory = factory();
        String schema = factory.databaseName(identity, generation, "a");
        execute("create database `" + schema + "`");
        execute("create table `" + schema + "`.important (id int primary key)");
        execute("insert into `" + schema + "`.important values(7)");
        assertThrows(IOException.class, () -> factory.retire(permit(identity, generation), "", 2));
        Assertions.assertEquals(7, scalar("select id from `" + schema + "`.important"));
        UUID owned = UUID.randomUUID();
        try (var ignored = factory.open(identity, owned)) {
        }
        String target = factory.databaseName(identity, owned, "a");
        execute("update `" + target + "`.azir_wal_generation set owner='" + "0".repeat(64) + "'");
        assertThrows(IOException.class, () -> factory.retire(permit(identity, owned), "", 2));
        Assertions.assertEquals(2, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /**
     * Retirement state, DROP and Line commit failed from the same identity border.
     *
     * @param stage stage
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(MysqlWalReplicaBackendFactory.RetirementStage.class)
    void interruptionPreservesTombstoneAndBudgetInvariants(final MysqlWalReplicaBackendFactory.RetirementStage stage) throws Exception {
        var identity = identity();
        UUID generation = UUID.randomUUID();
        try (var ignored = factory().open(identity, generation)) {
        }
        var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
        var broken = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), options(), hit -> {
        }, hit -> {
            if (hit == stage && armed.compareAndSet(true, false)) {
                throw new IOException("injected retirement " + stage);
            }
        });
        assertThrows(IOException.class, () -> broken.retire(permit(identity, generation), "", 1));
        assertThrows(IOException.class, () -> factory().open(identity, generation));
        assertTrue(factory().retire(permit(identity, generation), "", 2).complete());
        Assertions.assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget"));
        Assertions.assertEquals(2, scalar("select count(*) from azir_wal_generation_catalog where state=3"));
        assertTrue(factory().retire(permit(identity, generation), "", 2).complete());
        Assertions.assertEquals(0, scalar("select used_slots from azir_wal_namespace_budget"));
    }

    /**
     * Unit test direct issuer capability verify backend protocol, the real current generation protection is independently determined by node integration.
     *
     * @param identity identity
     * @param generation generation
     * @return the operation result
     */
    private static WalGenerationRetirement permit(final WalReplicaIdentity identity, final UUID generation) {
        return new WalGenerationRetirement(identity, generation, () -> {
        });
    }

    /**
     * Each piece allows only one generation, making the level of release a necessary condition for continued distribution.
     *
     * @return the operation result
     */
    private static MysqlWalReplicaBackendFactory.Options options() {
        return new MysqlWalReplicaBackendFactory.Options("azw", 1, 2, 1);
    }

    /**
     * The two pieces share a control library, but the physics namespace is different.
     *
     * @return the operation result
     */
    private MysqlWalReplicaBackendFactory factory() {
        return new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), options());
    }

    /**
     * The scene uses an independent engine identity.
     *
     * @return the operation result
     */
    private static WalReplicaIdentity identity() {
        UUID group = UUID.randomUUID();
        return new WalReplicaIdentity(group, group, "retire-node");
    }

    /**
     * All SQLs are constructed by testing, database names from official plants.
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
     * The original SQL count is independent of the factory return value.
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
}

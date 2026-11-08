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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Real MySQL verify bounded cursor, tombstone batch deletion, configuration rejection and hosting automatic maintenance.
 */
@Testcontainers
class WalMysqlOperationsTest {

    @Container
    static final MySQLContainer<?> DATABASE = WalCheckpointMysqlSupport.container();

    @TempDir
    Path directory;

    private HikariDataSource pool;

    /**
     * Each wheel shall be replaced only by a packaging schedule.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        pool = WalCheckpointMysqlSupport.pool(DATABASE);
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("drop table if exists azir_wal_version");
            query.executeUpdate("drop table if exists azir_wal_binding");
            query.executeUpdate("drop table if exists AZIR_WAL_VERSION");
        }
    }

    /**
     * Data sources are only available for this example.
     */
    @AfterEach
    void closePool() {
        if (pool != null) {
            pool.close();
        }
    }

    /**
     * Tombstone must be deleted only after all old values retirement, restarting recovery cursor and rotation.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void boundedGcPersistsCursorAndNeverResurrectsDelete() throws Exception {
        UUID engine = UUID.randomUUID();
        var store = new MysqlWalMaterializedStore(pool);
        store.initialize(engine, "a");
        var commits = new ArrayList<WalCommit>();
        commits.add(new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "", bytes("old")), new WalMutation("a", "z", bytes("live")))));
        for (int i = 2; i <= 20; i++) {
            commits.add(new WalCommit(UUID.randomUUID(), i, i - 1, List.of(new WalMutation("a", "", bytes("v" + i)))));
        }
        commits.add(new WalCommit(UUID.randomUUID(), 21, 20, List.of(new WalMutation("a", "", null))));
        store.apply(commits);
        int removed = 0;
        boolean wrapped = false;
        for (int cycle = 0; cycle < 20 && removed < 21; cycle++) {
            var progress = store.gcBounded(21, 1, 5, 5000);
            assertTrue(progress.inspectedKeys() <= 1);
            assertTrue(progress.removedRows() <= 5);
            removed += progress.removedRows();
            wrapped |= progress.wrapped();
            var value = store.get("", Long.MAX_VALUE);
            assertTrue(value.isEmpty() || value.orElseThrow().deleted());
            Assertions.assertEquals("live", text(store.get("z", Long.MAX_VALUE).orElseThrow().value()));
            store = new MysqlWalMaterializedStore(pool);
            store.initialize(engine, "a");
        }
        Assertions.assertEquals(21, removed);
        assertTrue(wrapped);
        Assertions.assertEquals(1, count());
    }

    /**
     * Automatically preserves the old RR and then batch collection history.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void managedMysqlReclaimsAfterOldSnapshotEnds() throws Exception {
        var store = new MysqlWalMaterializedStore(pool);
        try (
                var service = WalManagedRuntime.open(directory, Map.of("a", store), WalOptions.defaults().withHotCacheBytes(0),
                        WalRetentionOptions.preserveAll(), new WalMaintenanceOptions(20, 1, 2, 100, 5000, 80))) {
            var runtime = service.runtime();
            write(runtime, "a", "k", "old");
            runtime.flush();
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 10; i++) {
                write(runtime, "a", "k", "v" + i);
            }
            runtime.flush();
            waitFor(() -> service.health().maintenanceCycles() > 0);
            Assertions.assertEquals("old", text(runtime.get(reader, "a", "k").orElseThrow()));
            runtime.rollback(reader);
            waitFor(() -> countUnchecked() == 1);
            var latest = runtime.begin(IsolationLevel.READ_COMMITTED);
            Assertions.assertEquals("v9", text(runtime.get(latest, "a", "k").orElseThrow()));
            runtime.rollback(latest);
            assertTrue(service.health().ready());
            assertTrue(service.metrics().contains("azir_wal_gc_removed_rows_total "));
        }
    }

    /**
     * Unreliable durable storage configuration denied before creating WAL and protocol tables.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unsafeDurabilityIsRejectedBeforeWalCreation() throws Exception {
        setGlobal(0);
        try {
            assertThrows(WalMvccException.class, () -> WalManagedRuntime.open(directory.resolve("unsafe"), Map.of("a", new MysqlWalMaterializedStore(pool)),
                    WalOptions.defaults(), WalRetentionOptions.preserveAll(), WalMaintenanceOptions.defaults()));
            assertFalse(Files.exists(directory.resolve("unsafe")));
            try (var connection = pool.getConnection(); var query = connection.createStatement(); var rows = query.executeQuery("show tables")) {
                assertFalse(rows.next());
            }
        } finally {
            setGlobal(1);
        }
    }

    /**
     * No unbounded I/O timeout can enter the hosting entrance.
     *
     * @param setting setting
     */
    @ParameterizedTest
    @ValueSource(strings = {"connect", "socket", "pool"})
    void unboundedTimeoutIsRejected(final String setting) {
        var config = new HikariConfig();
        String url = DATABASE.getJdbcUrl();
        if (setting.equals("connect")) {
            url = url.replace("connectTimeout=2000", "connectTimeout=0");
        }
        if (setting.equals("socket")) {
            url = url.replace("socketTimeout=2000", "socketTimeout=0");
        }
        config.setJdbcUrl(url);
        config.setUsername(DATABASE.getUsername());
        config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(setting.equals("pool") ? 6000 : 5000);
        try (var bad = new HikariDataSource(config)) {
            assertThrows(WalMvccException.class, () -> WalManagedRuntime.open(directory.resolve(setting), Map.of("a", new MysqlWalMaterializedStore(bad)),
                    WalOptions.defaults(), WalRetentionOptions.preserveAll(), WalMaintenanceOptions.defaults()));
            assertFalse(Files.exists(directory.resolve(setting)));
        }
    }

    /**
     * Non- transaction table, non-bid key, error primary key and additional only constraint must be rejected.
     *
     * @param problem problem
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @ValueSource(strings = {"engine", "key_type", "primary", "unique"})
    void incompatibleSchemasFailClosed(final String problem) throws Exception {
        String type = problem.equals("key_type") ? "varchar(512)" : "varbinary(512)";
        String primary = problem.equals("primary") ? "commit_seq,user_key" : "user_key,commit_seq";
        String extra = problem.equals("unique") ? ",unique key wrong(user_key)" : "";
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("create table azir_wal_version(user_key " + type + " not null,commit_seq bigint not null,payload longblob null,primary key(" + primary + ")" + extra + ") engine="
                    + (problem.equals("engine") ? "MyISAM" : "InnoDB"));
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory.resolve(problem), Map.of("a", new MysqlWalMaterializedStore(pool)), WalOptions.defaults()));
        Assertions.assertEquals(0, count());
    }

    /**
     * The illegal original bytes do not allow to be backed up or scanned silently into another key.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidBackendKeyBytesAreRejected() throws Exception {
        var store = new MysqlWalMaterializedStore(pool);
        store.initialize(UUID.randomUUID(), "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value"))));
        var boundary = store.apply(List.of(commit));
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("update azir_wal_version set user_key=x'ff'");
        }
        assertThrows(WalMvccException.class, () -> store.exportSnapshot(boundary, row -> {
        }));
        assertThrows(WalMvccException.class, () -> store.scan(null, null, 1));
    }

    /**
     * Case-specific similar tables cannot be checked in engines, fields or indexes, nor can they be migrated to cover them.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void caseDistinctUnrelatedTableIsIgnored() throws Exception {
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("create table AZIR_WAL_VERSION(marker varchar(16)) engine=MyISAM");
            query.executeUpdate("insert into AZIR_WAL_VERSION values ('keep')");
        }
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(pool)), WalOptions.defaults())) {
            write(runtime, "a", "k", "value");
            runtime.flush();
        }
        try (var connection = pool.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select marker from AZIR_WAL_VERSION")) {
            assertTrue(row.next());
            Assertions.assertEquals("keep", row.getString(1));
        }
    }

    /**
     * RU cannot use other connection prefixs not yet commit as a landing disk to confirm the unknown result after it has failed safely.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void uncommittedBackendPrefixCannotAuthorizeWalRetirement() throws Exception {
        var config = new HikariConfig();
        config.setJdbcUrl(DATABASE.getJdbcUrl());
        config.setUsername(DATABASE.getUsername());
        config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setTransactionIsolation("TRANSACTION_READ_UNCOMMITTED");
        try (var ru = new HikariDataSource(config)) {
            var store = new MysqlWalMaterializedStore(ru);
            store.initialize(UUID.randomUUID(), "a");
            var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value"))));
            var confirmed = store.apply(List.of(commit));
            try (var connection = pool.getConnection(); var query = connection.createStatement()) {
                connection.setAutoCommit(false);
                try {
                    query.executeUpdate("update azir_wal_binding set applied_seq=2");
                    assertThrows(WalMvccException.class, store::checkpoint);
                } finally {
                    connection.rollback();
                }
            }
            Assertions.assertEquals(confirmed, store.checkpoint());
            assertThrows(WalMvccException.class, store::validateProduction);
        }
    }

    /**
     * Unsafely read timeout must be rejected before any connection status method that may access the server is available.
     */
    @Test
    void unsafeTimeoutIsRejectedBeforeServerStatusQueries() {
        var config = new HikariConfig();
        config.setJdbcUrl(DATABASE.getJdbcUrl().replace("socketTimeout=2000", "socketTimeout=0"));
        config.setUsername(DATABASE.getUsername());
        config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        try (var bad = new HikariDataSource(config)) {
            javax.sql.DataSource guarded =
                    (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                        Object result = delegated(bad, method, args);
                        if (!method.getName().equals("getConnection")) {
                            return result;
                        }
                        return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Connection.class}, (c, operation, values) -> {
                            if (List.of("isReadOnly", "getTransactionIsolation", "createStatement", "prepareStatement").contains(operation.getName())) {
                                throw new AssertionError("server query attempted before timeout validation");
                            }
                            return delegated(result, operation, values);
                        });
                    });
            assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(guarded).validateProduction());
        }
    }

    /**
     * Even if the engine is the same as shard, the replica cannot share the database and the normal mode cannot take over.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void replicaBackendOwnershipRejectsOtherNodesAndStandaloneAccess() throws Exception {
        UUID engine = UUID.randomUUID();
        UUID group = UUID.randomUUID();
        var first = new WalReplicaIdentity(engine, group, "n1");
        var second = new WalReplicaIdentity(engine, group, "n2");
        var owner = new MysqlWalMaterializedStore(pool);
        owner.initializeReplica(first, "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("owned"))));
        owner.apply(List.of(commit));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(pool).initializeReplica(second, "a"));
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(pool).initialize(engine, "a"));
        Assertions.assertEquals("owned", text(owner.get("k", 1).orElseThrow().value()));
        var reopened = new MysqlWalMaterializedStore(pool);
        reopened.initializeReplica(first, "a");
        Assertions.assertEquals(1, reopened.checkpoint().sequence());
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("update azir_wal_binding set replica_owner='" + "0".repeat(64) + "'");
        }
        assertThrows(WalMvccException.class, () -> owner.get("k", 1));
        assertThrows(WalMvccException.class, () -> owner.latestSequence("k"));
    }

    /**
     * The version table must be rejected after the materialization has been removed from the log and cannot be filled with empty tables to read confirmation data as non-existent.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingColdVersionTableCannotBeSilentlyRecreated() throws Exception {
        Path wal = directory.resolve("lost-cold-table");
        var options = WalOptions.defaults().withHotCacheBytes(0);
        try (var runtime = WalMvccRuntime.open(wal, Map.of("a", new MysqlWalMaterializedStore(pool)), options)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", bytes("confirmed"));
            runtime.commit(tx);
            runtime.flush();
            assertTrue(runtime.checkpoint().created());
            Assertions.assertEquals(0, runtime.stats().retainedBytes());
            Assertions.assertEquals(100, runtime.stats().walBytes());
        }
        try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
            ddl.executeUpdate("drop table azir_wal_version");
        }
        assertThrows(WalMvccException.class, () -> {
            try (var ignored = WalMvccRuntime.open(wal, Map.of("a", new MysqlWalMaterializedStore(pool)), options)) {
            }
        });
        try (
                var connection = pool.getConnection(); var query = connection.createStatement();
                var row = query.executeQuery("select count(*) from information_schema.tables where table_schema=database() and table_name='azir_wal_version'")) {
            row.next();
            Assertions.assertEquals(0, row.getInt(1));
        }
    }

    /**
     * The initial DDL leaves only an empty binding form without any confirmation prefix, so the version table can be safely completed for initialization.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void emptyBindingBeforeFirstPublicationCanFinishInterruptedTableCreation() throws Exception {
        UUID engine = UUID.randomUUID();
        new MysqlWalMaterializedStore(pool).initialize(engine, "a");
        try (var connection = pool.getConnection(); var ddl = connection.createStatement()) {
            ddl.executeUpdate("delete from azir_wal_binding");
            ddl.executeUpdate("drop table azir_wal_version");
        }
        var resumed = new MysqlWalMaterializedStore(pool);
        resumed.initialize(engine, "a");
        Assertions.assertEquals(0, resumed.checkpoint().sequence());
        Assertions.assertEquals(0, count());
    }

    /**
     * The agent retains the real anomaly and the test failure cannot be masked by a reflective package.
     *
     * @param target target
     * @param method method
     * @param args args
     * @return the operation result
     * @throws Throwable if the operation cannot complete
     */
    private static Object delegated(final Object target, final java.lang.reflect.Method method, final Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (final java.lang.reflect.InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    /**
     * The independent physical count will not be masked by the cache.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private long count() throws Exception {
        try (var connection = pool.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select count(*) from azir_wal_version")) {
            row.next();
            return row.getLong(1);
        }
    }

    /**
     * The waiting condition does not consider the query abnormal to be a zero line.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private long countUnchecked() {
        try {
            return count();
        } catch (final Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Only adjusts this test container to the recovery configuration at the end.
     *
     * @param value value to write
     * @throws Exception if the operation cannot complete
     */
    private static void setGlobal(final int value) throws Exception {
        var result = DATABASE.execInContainer("mysql", "-uroot", "-p" + DATABASE.getPassword(), "-e", "set global innodb_flush_log_at_trx_commit=" + value);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}

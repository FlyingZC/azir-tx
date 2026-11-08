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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Real database verify recovery quarantine, batch retry, full summary confirmation and target occupancy protection.
 */
@Testcontainers
class WalMysqlBackupStoreTest {

    @Container
    static final MySQLContainer<?> SOURCE = WalCheckpointMysqlSupport.container();

    @Container
    static final MySQLContainer<?> TARGET = WalCheckpointMysqlSupport.container().withUrlParam("useAffectedRows", "true");

    @TempDir
    Path directory;

    private HikariDataSource source;

    private HikariDataSource target;

    /**
     * Each round only emptys the exclusive form in the test container, and existing operational services do not participate in the testing.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        source = WalCheckpointMysqlSupport.pool(SOURCE);
        target = WalCheckpointMysqlSupport.pool(TARGET);
        for (var pool : List.of(source, target)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version");
                statement.executeUpdate("drop table if exists azir_wal_binding");
                statement.executeUpdate("drop table if exists azirXwalYbinding");
            }
        }
    }

    /**
     * Connection pool is only in the possession of a test, and the packaging is based on Testcontainers Life Cycle collection.
     */
    @AfterEach
    void closePools() {
        if (source != null) {
            source.close();
        }
        if (target != null) {
            target.close();
        }
    }

    /**
     * Flow snapshot retains the original version and tombstone, and recovery repeats batch with reassembly still receives the same full prefix.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void restoreCanResumeAndPublishOnlyAfterFullDigestMatches() throws Exception {
        var from = new MysqlWalMaterializedStore(source);
        var engine = UUID.randomUUID();
        from.initialize(engine, "a");
        var first = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "a", bytes("old")), new WalMutation("a", "中", bytes("live"))));
        var second = new WalCommit(UUID.randomUUID(), 2, 1, List.of(new WalMutation("a", "a", null), new WalMutation("a", "😀", bytes("new"))));
        var boundary = from.apply(List.of(first, second));
        var rows = new ArrayList<WalBackupRow>();
        from.exportSnapshot(boundary, rows::add);
        Assertions.assertEquals(3, rows.size());
        assertTrue(rows.getFirst().version().deleted());
        var spec = new WalRestoreSpec(UUID.randomUUID(), engine, "a", boundary);
        var into = new MysqlWalMaterializedStore(target);
        assertTrue(into.beginRestore(spec));
        into.restoreBatch(spec, rows.subList(0, 1));
        into.restoreBatch(spec, rows.subList(0, 1));
        assertThrows(WalMvccException.class, into::checkpoint);
        assertThrows(WalMvccException.class, () -> new MysqlWalMaterializedStore(target).initialize(engine, "a"));
        assertThrows(WalMvccException.class, () -> into.finishRestore(spec, rows.size(), digest(spec, rows)));
        assertThrows(WalMvccException.class, into::checkpoint);
        var resumed = new MysqlWalMaterializedStore(target);
        assertTrue(resumed.beginRestore(spec));
        resumed.restoreBatch(spec, rows);
        resumed.finishRestore(spec, rows.size(), digest(spec, rows));
        Assertions.assertEquals(boundary, resumed.checkpoint());
        assertFalse(resumed.beginRestore(spec));
        resumed.finishRestore(spec, rows.size(), digest(spec, rows));
        var recovered = new MysqlWalMaterializedStore(target);
        recovered.initialize(engine, "a");
        Assertions.assertEquals(boundary, recovered.checkpoint());
        Assertions.assertEquals("live", text(recovered.get("中", 2).orElseThrow().value()));
        assertTrue(recovered.get("a", 2).orElseThrow().deleted());
        var next = new WalCommit(UUID.randomUUID(), 3, 2, List.of(new WalMutation("a", "a", bytes("reborn"))));
        recovered.apply(List.of(next));
        assertThrows(WalMvccException.class, () -> recovered.beginRestore(spec));
        Assertions.assertEquals("reborn", text(recovered.get("a", 3).orElseThrow().value()));
    }

    /**
     * Recovery can not take over a part of the data already written on the same backup number as the engine, route or boundary.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void wrongIdentityAndNormalExistingTargetsAreRejected() throws Exception {
        var spec = new WalRestoreSpec(UUID.randomUUID(), UUID.randomUUID(), "a", new WalCheckpoint(5, "1".repeat(64)));
        var into = new MysqlWalMaterializedStore(target);
        assertTrue(into.beginRestore(spec));
        for (var wrong : List.of(new WalRestoreSpec(UUID.randomUUID(), spec.engineId(), "a", spec.boundary()),
                new WalRestoreSpec(spec.backupId(), spec.engineId(), "a", new WalCheckpoint(6, spec.boundary().digest())),
                new WalRestoreSpec(spec.backupId(), spec.engineId(), "b", spec.boundary()))) {
            assertThrows(RuntimeException.class, () -> new MysqlWalMaterializedStore(target).beginRestore(wrong));
        }
        var normal = new MysqlWalMaterializedStore(source);
        normal.initialize(spec.engineId(), "a");
        assertThrows(WalMvccException.class, () -> normal.beginRestore(spec));
        Assertions.assertEquals(new WalCheckpoint(0, ""), normal.checkpoint());
    }

    /**
     * The backend has not been smoothed or the consumer export failed to produce a false and complete backup.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void boundaryMismatchAndSinkFailurePropagate() throws Exception {
        var store = new MysqlWalMaterializedStore(source);
        store.initialize(UUID.randomUUID(), "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("value"))));
        var boundary = store.apply(List.of(commit));
        assertThrows(WalMvccException.class, () -> store.exportSnapshot(new WalCheckpoint(2, commit.digest()), row -> {
        }));
        assertThrows(java.io.IOException.class, () -> store.exportSnapshot(boundary, row -> {
            throw new java.io.IOException("backup disk full");
        }));
        Assertions.assertEquals(boundary, store.checkpoint());
    }

    /**
     * The security of the old five-column binder has increased recovery status, data are available and normal prefix is maintained.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void legacyBindingSchemaMigratesWithoutChangingData() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate(
                    "create table azir_wal_binding (id tinyint primary key,engine_id char(36) not null,shard_id varbinary(128) not null,applied_seq bigint not null,digest char(64) not null) engine=InnoDB");
            // The underlined for JDBC metadata is the mode wildcard, and another table with a similar name cannot impersonate the recovery field of this table.
            statement.executeUpdate("create table azirXwalYbinding (restore_id char(64),restoring tinyint) engine=InnoDB");
            statement.executeUpdate("insert into azirXwalYbinding values ('unrelated',1)");
        }
        var store = new MysqlWalMaterializedStore(source);
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "value");
            runtime.flush();
            var rows = new ArrayList<WalBackupRow>();
            store.exportSnapshot(store.checkpoint(), rows::add);
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals("value", text(rows.getFirst().version().value()));
        }
        try (var connection = source.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select restore_id,restoring from azirXwalYbinding")) {
            assertTrue(row.next());
            Assertions.assertEquals("unrelated", row.getString(1));
            Assertions.assertEquals(1, row.getInt(2));
        }
    }

    /**
     * When the real recovery database commit is completed and the missing is confirmed, the same backup can confirm that the completed retry is safe.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void finishCommitResponseLossCanBeConfirmedAndRetried() throws Exception {
        var armed = new java.util.concurrent.atomic.AtomicBoolean();
        javax.sql.DataSource uncertain = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class},
                (proxy, method, args) -> {
                    Object result = delegated(target, method, args);
                    if (!method.getName().equals("getConnection")) {
                        return result;
                    }
                    return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Connection.class}, (connection, operation, values) -> {
                        Object returned = delegated(result, operation, values);
                        if (operation.getName().equals("commit") && armed.compareAndSet(true, false)) {
                            throw new java.sql.SQLException("lost restore commit response", "08S01");
                        }
                        return returned;
                    });
                });
        var spec = new WalRestoreSpec(UUID.randomUUID(), UUID.randomUUID(), "a", new WalCheckpoint(1, "2".repeat(64)));
        var rows = List.of(new WalBackupRow("k", new WalValue(1, bytes("value"))));
        var store = new MysqlWalMaterializedStore(uncertain);
        store.beginRestore(spec);
        store.restoreBatch(spec, rows);
        armed.set(true);
        assertThrows(WalMvccException.class, () -> store.finishRestore(spec, 1, digest(spec, rows)));
        Assertions.assertEquals(spec.boundary(), store.checkpoint());
        var resumed = new MysqlWalMaterializedStore(target);
        assertFalse(resumed.beginRestore(spec));
        resumed.finishRestore(spec, 1, digest(spec, rows));
        Assertions.assertEquals("value", text(resumed.get("k", 1).orElseThrow().value()));
    }

    /**
     * The agent must keep the original JDBC anomaly so that the real commit response fails into the same recovery path.
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
     * Calculates the summary using the same format as the real backup file to ensure that all physical lines and identity are overwritten.
     *
     * @param spec spec
     * @param rows rows
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String digest(final WalRestoreSpec spec, final List<WalBackupRow> rows) throws Exception {
        var bytes = new java.io.ByteArrayOutputStream();
        var writer = new WalBackupData.Writer(bytes, new WalBackupData.Header(spec.engineId(), spec.shard(), spec.boundary()));
        for (var row : rows) {
            writer.accept(row);
        }
        writer.finish();
        return WalCodec.digest(bytes.toByteArray());
    }
}

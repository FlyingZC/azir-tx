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

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Limited double MySQL verify cold version, old snapshot, physics tombstone collection and idempotent replay.
 */
@Testcontainers
class WalBoundedMysqlTest {

    @Container
    static final MySQLContainer<?> A = WalCheckpointMysqlSupport.container();

    @Container
    static final MySQLContainer<?> B = WalCheckpointMysqlSupport.container();

    @TempDir
    Path directory;

    private HikariDataSource left;

    private HikariDataSource right;

    /**
     * Use an exclusive list in a stand-alone test container for each example, not connection already exists for database services.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        left = WalCheckpointMysqlSupport.pool(A);
        right = WalCheckpointMysqlSupport.pool(B);
        for (var pool : List.of(left, right)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version");
                statement.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /**
     * Only the ponds created by this round are closed and containers are cleared from the test life cycle.
     */
    @AfterEach
    void cleanup() {
        if (left != null) {
            left.close();
        }
        if (right != null) {
            right.close();
        }
    }

    /**
     * Full working keys are well overcut and cold RR, page break, cross-shard conflict with low memory re-opening is still correct.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void coldTwoShardVersionsSurviveCheckpointAndSmallMemoryRestart() throws Exception {
        var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(left), "b", new MysqlWalMaterializedStore(right));
        try (var runtime = WalMvccRuntime.open(directory, stores, options())) {
            for (int i = 0; i < 200; i++) {
                pair(runtime, "key:" + i, "old:" + i);
                runtime.flush();
            }
            Assertions.assertEquals(200, count(left));
            Assertions.assertEquals(200, count(right));
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int i = 0; i < 200; i++) {
                pair(runtime, "key:" + i, "new:" + i);
                runtime.flush();
            }
            assertTrue(runtime.stats().hotCacheBytes() <= 1024);
            assertTrue(runtime.checkpoint().created());
            runtime.collectGarbage(4096);
            var snapshot = runtime.beginReadStatement(old);
            for (int i = 0; i < 200; i++) {
                for (String shard : stores.keySet()) {
                    Assertions.assertEquals("old:" + i, text(runtime.get(old, shard, "key:" + i, snapshot).orElseThrow()));
                }
            }
            var page = runtime.scanPage(old, "a", null, null, null, 7, snapshot);
            Assertions.assertEquals(7, page.rows().size());
            Assertions.assertEquals("old:0", text(page.rows().get("key:0")));
            runtime.put(old, "b", "key:0", bytes("wrong"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(old)).errorCode());
            runtime.collectGarbage(4096);
            runtime.checkpoint();
            assertTrue(runtime.stats().conflictStoreLookups() > 200);
        }
        try (var restored = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            Assertions.assertEquals(0, restored.stats().retainedBytes());
            var tx = restored.begin(IsolationLevel.READ_COMMITTED);
            for (int i = 0; i < 200; i++) {
                for (String shard : stores.keySet()) {
                    Assertions.assertEquals("new:" + i, text(restored.get(tx, shard, "key:" + i).orElseThrow()));
                }
            }
            restored.rollback(tx);
            Assertions.assertEquals(200, count(left));
            Assertions.assertEquals(200, count(right));
        }
    }

    /**
     * Physical lines return to zero when new and deleted keys are added, neither the old WAL replay nor the restart can be revived.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void uniqueKeyDeleteChurnRetiresPhysicalRowsWithoutResurrection() throws Exception {
        var a = new MysqlWalMaterializedStore(left);
        var b = new MysqlWalMaterializedStore(right);
        var stores = Map.<String, WalMaterializedStore>of("a", a, "b", b);
        try (var runtime = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            for (int i = 0; i < 96; i++) {
                pair(runtime, "gone:" + i, "value");
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.delete(tx, "a", "gone:" + i);
                runtime.delete(tx, "b", "gone:" + i);
                runtime.commit(tx);
                runtime.flush();
                runtime.collectGarbage(8);
                Assertions.assertEquals(0, count(left));
                Assertions.assertEquals(0, count(right));
            }
        }
        // Simulates the database successfully applied but the caller is still an old replay log; the prefix has been advanced and cannot be reinserted into the old retirement.
        try (var log = new WalLog(directory, Set.of("a", "b"), options().maxWalBytes())) {
            a.apply(log.read(1, 1024));
            b.apply(log.read(1, 1024));
            Assertions.assertEquals(0, count(left));
            Assertions.assertEquals(0, count(right));
        }
        try (var restored = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(restored.scan(tx, "a", null, null).isEmpty());
            assertTrue(restored.scan(tx, "b", null, null).isEmpty());
            restored.rollback(tx);
            pair(restored, "gone:0", "reborn");
            restored.flush();
            restored.checkpoint();
        }
        try (var restored = WalMvccRuntime.open(directory, stores, options().withHotCacheBytes(0))) {
            var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("reborn", text(restored.get(tx, "a", "gone:0").orElseThrow()));
            Assertions.assertEquals("reborn", text(restored.get(tx, "b", "gone:0").orElseThrow()));
            restored.rollback(tx);
            Assertions.assertEquals(1, count(left));
            Assertions.assertEquals(1, count(right));
        }
    }

    /**
     * The old RR cannot be physically removed from the history it needs before its completion; the deletion and reconstruction of the older author is still in conflict.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void physicalRetirementWaitsForOldReaderAndPreservesNewerVersion() throws Exception {
        var store = new MysqlWalMaterializedStore(left);
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), options().withHotCacheBytes(0))) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k");
            runtime.commit(deletion);
            runtime.flush();
            runtime.collectGarbage(8);
            Assertions.assertEquals(2, count(left));
            Assertions.assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            var afterDelete = runtime.begin(IsolationLevel.REPEATABLE_READ);
            write(runtime, "a", "k", "new");
            runtime.flush();
            runtime.rollback(old);
            runtime.collectGarbage(8);
            Assertions.assertEquals(1, count(left));
            assertTrue(runtime.get(afterDelete, "a", "k").isEmpty());
            runtime.put(afterDelete, "a", "k", bytes("wrong"));
            Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(afterDelete)).errorCode());
            Assertions.assertEquals("new", text(store.get("k", Long.MAX_VALUE).orElseThrow().value()));
        }
    }

    /**
     * When the second collection batch deletion fails, the first deletion must also be rollback and application prefixes are not affected.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void tombstoneGcFailureRollsBackWholeLocalBatch() throws Exception {
        var armed = new AtomicBoolean();
        var calls = new AtomicInteger();
        javax.sql.DataSource failing = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                    Object result = delegated(left, method, args);
                    if (!method.getName().equals("getConnection")) {
                        return result;
                    }
                    return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.Connection.class},
                            (p, operation, values) -> {
                                Object statement = delegated(result, operation, values);
                                if (!operation.getName().equals("prepareStatement") || !((String) values[0]).startsWith("delete from azir_wal_version")) {
                                    return statement;
                                }
                                return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{java.sql.PreparedStatement.class},
                                        (s, action, parameters) -> {
                                            if (action.getName().equals("executeUpdate") && armed.get() && calls.incrementAndGet() == 2) {
                                                throw new java.sql.SQLException("injected GC failure after first physical delete");
                                            }
                                            return delegated(statement, action, parameters);
                                        });
                            });
                });
        var store = new MysqlWalMaterializedStore(failing);
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), options().withHotCacheBytes(0))) {
            for (String key : new String[]{"a", "b"}) {
                write(runtime, "a", key, "old");
                var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.delete(deletion, "a", key);
                runtime.commit(deletion);
            }
            runtime.flush();
            Assertions.assertEquals(4, count(left));
            armed.set(true);
            assertThrows(WalMvccException.class, () -> runtime.collectGarbage(8));
            Assertions.assertEquals(4, count(left));
            Assertions.assertEquals(4, store.checkpoint().sequence());
            armed.set(false);
            Assertions.assertEquals(4, runtime.collectGarbage(8));
            Assertions.assertEquals(0, count(left));
        }
    }

    /**
     * Unpack reflection anomaly to ensure that injection remains true JDBC error.
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
     * The small hotspot budget is used to force cold data path participation, and the background task remains limited batch.
     *
     * @return the operation result
     */
    private static WalOptions options() {
        return new WalOptions(32, 16, 8192, 32, 8, 1, 8L * 1024 * 1024, 32_768, 60_000, 1024, 50).withHotCacheBytes(1024);
    }

    /**
     * Checks the number of physical lines that are exclusive to the version table, and does not depend on running-time caches.
     *
     * @param pool pool
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long count(final HikariDataSource pool) throws Exception {
        try (var connection = pool.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("select count(*) from azir_wal_version")) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    /**
     * Updates two physical backends with the same commit domain.
     *
     * @param runtime runtime
     * @param key business key
     * @param value value to write
     */
    private static void pair(final WalMvccRuntime runtime, final String key, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", key, bytes(value));
        runtime.put(tx, "b", key, bytes(value));
        runtime.commit(tx);
    }
}

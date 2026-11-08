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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Reactivate the durable sexual boundary covering materialization with real database commit post- commit abnormal and mandatory containers.
 */
@Testcontainers
class WalMysqlBoundaryTest {

    @Container
    static final MySQLContainer<?> DATABASE = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("wal_boundary").withUsername("wal_test").withPassword("wal_test")
            .withUrlParam("rewriteBatchedStatements", "true").withUrlParam("connectTimeout", "2000")
            .withUrlParam("socketTimeout", "2000")
            .withCommand("--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1");

    @TempDir
    Path directory;

    private volatile HikariDataSource pool;

    /**
     * 100 formal acceptances each, with a normal return of every failure.
     *
     * @return the operation result
     */
    static IntStream repetitions() {
        return IntStream.range(0, Integer.getInteger("azir.wal.mysqlBoundaryRepeats", 1));
    }

    /**
     * Only the exclusive packaging table created by this test is cleared of each round.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        pool = createPool(actualJdbcUrl());
        try (var connection = pool.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("drop table if exists azir_wal_version");
            query.executeUpdate("drop table if exists azir_wal_binding");
        }
    }

    /**
     * Creates connection pool on the current actual endpoint without using Docker's random distribution port as a permanent address.
     *
     * @param url url
     * @return the operation result
     */
    private static HikariDataSource createPool(final String url) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(DATABASE.getUsername());
        config.setPassword(DATABASE.getPassword());
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(3000);
        config.setValidationTimeout(1000);
        // The return of the container start does not mean that database is finished recovery, connection creates an bounded readiness check waiting.
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }

    /**
     * The current random map of Docker is checked per round, and the cache port at first startup cannot be reused.
     *
     * @return the operation result
     */
    private static String actualJdbcUrl() {
        var inspected = DATABASE.getDockerClient().inspectContainerCmd(DATABASE.getContainerId()).exec();
        var bindings = inspected.getNetworkSettings().getPorts().getBindings()
                .get(new com.github.dockerjava.api.model.ExposedPort(3306));
        assertNotNull(bindings);
        assertTrue(bindings.length > 0);
        return "jdbc:mysql://" + DATABASE.getHost() + ":" + bindings[0].getHostPortSpec()
                + "/wal_boundary?rewriteBatchedStatements=true&connectTimeout=2000&socketTimeout=2000";
    }

    /**
     * Turn off the current connection pool and transfer the container to Testcontainers Life Cycle Management.
     */
    @AfterEach
    void cleanup() {
        if (pool != null) {
            pool.close();
        }
    }

    /**
     * Throws a communication anomaly after the real number is successful, checking for unknown result processing path instead of simply ignoring normal return.
     *
     * @param repetition repetition
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void databaseCommittedButResponseFailedIsSafelyReplayed(final int repetition) {
        var armed = new AtomicBoolean();
        DataSource uncertain = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    Object result = delegate(pool, method, arguments);
                    if (!method.getName().equals("getConnection")) {
                        return result;
                    }
                    var connection = (Connection) result;
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (p, operation, values) -> {
                        Object returned = delegate(connection, operation, values);
                        if (operation.getName().equals("commit") && armed.compareAndSet(true, false)) {
                            throw new SQLException("injected response loss after real database commit", "08S01");
                        }
                        return returned;
                    });
                });
        var store = new MysqlWalMaterializedStore(uncertain);
        store.initialize(UUID.randomUUID(), "a");
        var first = record(1, "one");
        armed.set(true);
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, assertThrows(WalMvccException.class, () -> store.apply(List.of(first))).errorCode());
        Assertions.assertEquals(1, store.checkpoint().sequence());
        Assertions.assertEquals("one", text(store.get("k", 1).orElseThrow().value()));
        Assertions.assertEquals(first.digest(), store.apply(List.of(first)).digest());
        var second = record(2, "two");
        Assertions.assertEquals(2, store.apply(List.of(first, second)).sequence());
        Assertions.assertEquals("one", text(store.get("k", 1).orElseThrow().value()));
        Assertions.assertEquals("two", text(store.get("k", 2).orElseThrow().value()));
    }

    /**
     * Force MySQL to stop this test, keep local WAL, restart database after recovery materialization and check the cold history.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void killedMysqlRestartsWithoutLosingOrDuplicatingMaterializedVersions(final int repetition) throws Exception {
        // The re-launching of the test container may replace the main host port and stabilize the DataSource front to deliver the new connection to the current pool.
        DataSource routed = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> delegate(pool, method, arguments));
        var store = new MysqlWalMaterializedStore(routed);
        var defaults = WalOptions.defaults();
        var options = new WalOptions(defaults.maxActiveTransactions(), defaults.maxWriteKeys(), defaults.maxWriteBytes(),
                defaults.queueCapacity(), defaults.commitBatchSize(), defaults.batchDelayMillis(), defaults.maxWalBytes(),
                defaults.maxMemoryBytes(), defaults.transactionTimeoutMillis(), defaults.flushBatchSize(), 60_000);
        UUID decision;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), options)) {
            write(runtime, "a", "k", "old");
            runtime.flush();
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            String containerId = DATABASE.getContainerId();
            DATABASE.getDockerClient().stopContainerCmd(containerId).withTimeout(0).exec();
            try {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                decision = tx.id();
                runtime.put(tx, "a", "k", bytes("new"));
                runtime.commit(tx);
                var fresh = runtime.begin(IsolationLevel.READ_COMMITTED);
                Assertions.assertEquals("new", text(runtime.get(fresh, "a", "k").orElseThrow()));
                runtime.rollback(fresh);
                assertThrows(WalMvccException.class, runtime::flush);
            } finally {
                DATABASE.getDockerClient().startContainerCmd(containerId).exec();
                var previous = pool;
                pool = createPool(actualJdbcUrl());
                previous.close();
            }
            awaitDatabase();
            runtime.flush();
            runtime.collectGarbage(100);
            Assertions.assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            Assertions.assertEquals("old", text(store.get("k", 1).orElseThrow().value()));
            Assertions.assertEquals("new", text(store.get("k", 2).orElseThrow().value()));
            runtime.rollback(old);
            runtime.collectGarbage(100);
        }
        try (var restored = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(routed)), options)) {
            Assertions.assertEquals(2, restored.findCommitted(decision).orElseThrow().commitSequence());
            var reader = restored.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("new", text(restored.get(reader, "a", "k").orElseThrow()));
            restored.rollback(reader);
        }
        try (
                var connection = pool.getConnection(); var query = connection.createStatement();
                var rows = query.executeQuery("select count(*) from azir_wal_version")) {
            rows.next();
            Assertions.assertEquals(1, rows.getLong(1));
        }
    }

    /**
     * The reflect agent keeps the real JDBC error and commit order.
     *
     * @param target target
     * @param method method
     * @param arguments arguments
     * @return the operation result
     * @throws Throwable if the operation cannot complete
     */
    private static Object delegate(final Object target, final Method method, final Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (final InvocationTargetException error) {
            throw error.getCause();
        }
    }

    /**
     * Creates an materialization log entry with a stable serial number.
     *
     * @param sequence sequence
     * @param value value to write
     * @return the operation result
     */
    private static WalCommit record(final long sequence, final String value) {
        return new WalCommit(UUID.randomUUID(), sequence, sequence - 1, List.of(new WalMutation("a", "k", bytes(value))));
    }

    /**
     * Database for this test container only, and it is clear that the re-establishment of connection failed.
     *
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private void awaitDatabase() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        SQLException latest = null;
        do {
            try (var connection = pool.getConnection(); var query = connection.createStatement()) {
                query.execute("select 1");
                return;
            } catch (final SQLException transientFailure) {
                latest = transientFailure;
            }
            Thread.sleep(20);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("owned MySQL did not recover", latest);
    }
}

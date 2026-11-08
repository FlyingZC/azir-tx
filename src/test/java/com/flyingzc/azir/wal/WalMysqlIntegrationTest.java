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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Use two stand-alone MySQL examples verify new models, without contact with the existing environment and old mode protocol table.
 */
@Testcontainers
class WalMysqlIntegrationTest {

    @Container
    static final MySQLContainer<?> A = container();

    @Container
    static final MySQLContainer<?> B = container();

    @TempDir
    Path directory;

    private HikariDataSource a;

    private HikariDataSource b;

    /**
     * The normal return is carried out once each time, with a formal acceptance pattern specifying the number of real backend repeats.
     *
     * @return the operation result
     */
    static java.util.stream.IntStream repetitions() {
        return java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.mysqlRepeats", 1));
    }

    /**
     * Creates an isolated database container with a strong brush configuration.
     *
     * @return the operation result
     */
    private static MySQLContainer<?> container() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("wal_mvp")
                .withUsername("wal_test").withPassword("wal_test")
                .withUrlParam("rewriteBatchedStatements", "true")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000")
                .withCommand("--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1");
    }

    /**
     * Clears an exclusive empty table and creates bounded connection pool for each example.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void prepare() throws Exception {
        a = pool(A);
        b = pool(B);
        for (var dataSource : List.of(a, b)) {
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("drop table if exists azir_wal_version");
                statement.executeUpdate("drop table if exists azir_wal_binding");
            }
        }
    }

    /**
     * Only close connection pool created by this example.
     */
    @AfterEach
    void cleanup() {
        if (a != null) {
            a.close();
        }
        if (b != null) {
            b.close();
        }
    }

    /**
     * Configure short JDBC timeout to avoid backend failure blocking off and resource collection.
     *
     * @param container container
     * @return the operation result
     */
    private static HikariDataSource pool(final MySQLContainer<?> container) {
        return pool(container, 0);
    }

    /**
     * Limit verify materialization bytes to a smaller client package.
     *
     * @param container container
     * @param packetLimit packet limit
     * @return the operation result
     */
    private static HikariDataSource pool(final MySQLContainer<?> container, final int packetLimit) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(2500);
        config.setValidationTimeout(1000);
        if (packetLimit > 0) {
            config.addDataSourceProperty("maxAllowedPacket", Integer.toString(packetLimit));
            config.addDataSourceProperty("useServerPrepStmts", "true");
        }
        return new HikariDataSource(config);
    }

    /**
     * Complete materialization in the same database transaction when the total size of the batch exceeds the maximum per package.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void largeMaterializationBatchIsSplitByPacketBudget(final int repetition) throws Exception {
        try (var smallPackets = pool(A, 65_536)) {
            var store = new MysqlWalMaterializedStore(smallPackets);
            try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                byte[] payload = new byte[1024];
                java.util.Arrays.fill(payload, (byte) 7);
                for (int i = 0; i < 300; i++) {
                    runtime.put(tx, "a", "packet:" + i, payload);
                }
                runtime.commit(tx);
                drain(runtime);
                Assertions.assertEquals(300, count(a));
                var persisted = store.scan(null, null, 1);
                Assertions.assertEquals(300, persisted.size());
                for (int i = 0; i < 300; i++) {
                    assertArrayEquals(payload, persisted.get("packet:" + i).value());
                }
                var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
                assertArrayEquals(payload, runtime.get(reader, "a", "packet:299").orElseThrow());
                runtime.rollback(reader);
            }
        }
    }

    /**
     * The second SQL batch failed after the batch had to be in front of the rollback and the checkpoint could not advance.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void secondPacketChunkFailureRollsBackAllChunksAndCheckpoint(final int repetition) throws Exception {
        try (var smallPackets = pool(A, 65_536)) {
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            javax.sql.DataSource failing = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                        Object result = delegated(smallPackets, method, args);
                        if (method.getName().equals("getConnection")) {
                            var connection = (java.sql.Connection) result;
                            return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[]{java.sql.Connection.class}, (p, connectionMethod, connectionArgs) -> {
                                        Object statement = delegated(connection, connectionMethod, connectionArgs);
                                        if (connectionMethod.getName().equals("prepareStatement")
                                                && ((String) connectionArgs[0]).startsWith("insert into azir_wal_version")) {
                                            return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                                    new Class<?>[]{java.sql.PreparedStatement.class}, (s, operation, values) -> {
                                                        if (operation.getName().equals("executeBatch") && armed.get()
                                                                && calls.incrementAndGet() == 2) {
                                                            throw new java.sql.SQLException("injected second packet chunk failure");
                                                        }
                                                        return delegated(statement, operation, values);
                                                    });
                                        }
                                        return statement;
                                    });
                        }
                        return result;
                    });
            var store = new MysqlWalMaterializedStore(failing);
            store.initialize(UUID.randomUUID(), "a");
            var mutations = new java.util.ArrayList<WalMutation>();
            for (int i = 0; i < 300; i++) {
                mutations.add(new WalMutation("a", "chunk:" + i, new byte[1024]));
            }
            var commit = new WalCommit(UUID.randomUUID(), 1, 0, mutations);
            assertThrows(WalMvccException.class, () -> store.apply(List.of(commit)));
            Assertions.assertEquals(0, count(a));
            Assertions.assertEquals(0, store.checkpoint().sequence());
            armed.set(false);
            Assertions.assertEquals(1, store.apply(List.of(commit)).sequence());
            Assertions.assertEquals(300, count(a));
        }
    }

    /**
     * The reflect agent retains the original JDBC anomaly and avoids wrapping the injection failure into a reflection package anomaly.
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
     * A single line exceeding the backend materialization budget must be refused before durable commit.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void oversizedRowIsRejectedBeforeWalConfirmation(final int repetition) throws Exception {
        try (
                var smallPackets = pool(A, 65_536);
                var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(smallPackets)), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals(com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED, assertThrows(WalMvccException.class,
                    () -> runtime.put(tx, "a", "too-large", new byte[40_000])).errorCode());
            Assertions.assertEquals(0, runtime.stats().publishedSequence());
            Assertions.assertEquals(60, runtime.stats().walBytes());
            Assertions.assertEquals(0, runtime.stats().bufferedBytes());
            runtime.rollback(tx);
        }
    }

    /**
     * Global snapshot remains complete when one materialization old value and another materialization new value.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void partialFlushOldRrGcAndRestartRemainConsistent(final int repetition) throws Exception {
        var left = new MysqlWalMaterializedStore(a);
        var right = new ControlledStore(new MysqlWalMaterializedStore(b));
        UUID committedId;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", left, "b", right), WalOptions.defaults())) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "balance", bytes("100"));
            runtime.put(seed, "b", "balance", bytes("100"));
            runtime.commit(seed);
            drain(runtime);
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            right.paused = true;
            var transfer = runtime.begin(IsolationLevel.REPEATABLE_READ);
            committedId = transfer.id();
            runtime.put(transfer, "a", "balance", bytes("90"));
            runtime.put(transfer, "b", "balance", bytes("110"));
            runtime.commit(transfer);
            assertThrows(WalMvccException.class, runtime::flush);
            Assertions.assertEquals("90", text(left.get("balance", 2).orElseThrow().value()));
            Assertions.assertEquals("100", text(right.delegate.get("balance", 2).orElseThrow().value()));
            Assertions.assertEquals("100", text(runtime.get(old, "a", "balance").orElseThrow()));
            Assertions.assertEquals("100", text(runtime.get(old, "b", "balance").orElseThrow()));
            var fresh = runtime.begin(IsolationLevel.READ_COMMITTED);
            var snapshot = runtime.beginReadStatement(fresh);
            Assertions.assertEquals("90", text(runtime.get(fresh, "a", "balance", snapshot).orElseThrow()));
            Assertions.assertEquals("110", text(runtime.get(fresh, "b", "balance", snapshot).orElseThrow()));
            right.paused = false;
            drain(runtime);
            runtime.collectGarbage(100);
            Assertions.assertEquals("100", text(runtime.get(old, "b", "balance").orElseThrow()));
            runtime.rollback(old);
            runtime.rollback(fresh);
            Assertions.assertEquals(2, runtime.collectGarbage(100));
            Assertions.assertEquals(1, count(a));
            Assertions.assertEquals(1, count(b));
        }
        try (
                var recovered = WalMvccRuntime.open(directory,
                        Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b)), WalOptions.defaults())) {
            var tx = recovered.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("90", text(recovered.get(tx, "a", "balance").orElseThrow()));
            Assertions.assertEquals("110", text(recovered.get(tx, "b", "balance").orElseThrow()));
            Assertions.assertEquals(2, recovered.findCommitted(committedId).orElseThrow().commitSequence());
            recovered.rollback(tx);
        }
        try (var connection = a.getConnection(); var query = connection.createStatement(); var tables = query.executeQuery("show tables")) {
            var names = new java.util.HashSet<String>();
            while (tables.next()) {
                names.add(tables.getString(1));
            }
            Assertions.assertEquals(java.util.Set.of("azir_wal_binding", "azir_wal_version"), names);
        }
    }

    /**
     * Database commit successfully responding to lost replay cannot duplicate or accept conflict logs.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void committedDatabaseBatchCanBeReplayedAfterLostResponse(final int repetition) throws Exception {
        var store = new MysqlWalMaterializedStore(a);
        store.initialize(UUID.randomUUID(), "a");
        var commit = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("v"))));
        Assertions.assertEquals(1, store.apply(List.of(commit)).sequence());
        // The caller ignores the first return and retransmits exactly the same batch as the unknown result.
        Assertions.assertEquals(1, store.apply(List.of(commit)).sequence());
        Assertions.assertEquals(1, count(a));
        var conflicting = new WalCommit(UUID.randomUUID(), 1, 0, List.of(new WalMutation("a", "k", bytes("wrong"))));
        assertThrows(WalMvccException.class, () -> store.apply(List.of(conflicting)));
        var gap = new WalCommit(UUID.randomUUID(), 3, 1, List.of(new WalMutation("a", "k", bytes("gap"))));
        assertThrows(WalMvccException.class, () -> store.apply(List.of(gap)));
        Assertions.assertEquals("v", text(store.get("k", 3).orElseThrow().value()));
    }

    /**
     * Tombstone and reconstruction will not revive deleted data after collection and restart.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void tombstoneHistorySurvivesGcAndRecovery(final int repetition) throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults())) {
            write(runtime, "a", "k", "old");
            drain(runtime);
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "k");
            runtime.commit(deletion);
            write(runtime, "a", "k", "new");
            var finalDelete = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(finalDelete, "a", "k");
            runtime.commit(finalDelete);
            drain(runtime);
            runtime.collectGarbage(100);
            Assertions.assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            Assertions.assertEquals(List.of("k"), new java.util.ArrayList<>(runtime.scan(old, "a", null, null).keySet()));
            runtime.rollback(old);
            Assertions.assertEquals(4, runtime.collectGarbage(100));
        }
        try (var recovered = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults())) {
            var tx = recovered.begin(IsolationLevel.READ_COMMITTED);
            assertTrue(recovered.get(tx, "a", "k").isEmpty());
            assertTrue(recovered.scan(tx, "a", null, null).isEmpty());
        }
        Assertions.assertEquals(0, count(a));
    }

    /**
     * Neither the new log identity nor the backward log can take over the stored materialization.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void bindingAndAheadCheckpointRejectUnsafeStartup(final int repetition) throws Exception {
        byte[] oldLog;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults())) {
            write(runtime, "a", "k", "one");
            drain(runtime);
            oldLog = Files.readAllBytes(directory.resolve("transactions.wal"));
            write(runtime, "a", "k", "two");
            drain(runtime);
        }
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory.resolve("foreign"),
                Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults()));
        Files.write(directory.resolve("transactions.wal"), oldLog);
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory,
                Map.of("a", new MysqlWalMaterializedStore(a)), WalOptions.defaults()));
        Assertions.assertEquals(2, count(a));
    }

    /**
     * Pauses a real MySQL process, foreground WAL commit and new version reading still complete.
     *
     * @param repetition repetition
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("repetitions")
    void pausedMysqlDoesNotEnterTheCommitCriticalPath(final int repetition) throws Exception {
        try (
                var runtime = WalMvccRuntime.open(directory,
                        Map.of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b)), WalOptions.defaults())) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", "k", bytes("old-left"));
            runtime.put(seed, "b", "k", bytes("old-right"));
            runtime.commit(seed);
            drain(runtime);
            B.getDockerClient().pauseContainerCmd(B.getContainerId()).exec();
            try {
                // The latest head of materialization is still in the cache and cannot be read as synchronous database as necessary by suspending degradation from database.
                var cached = runtime.begin(IsolationLevel.READ_COMMITTED);
                Assertions.assertEquals("old-right", text(runtime.get(cached, "b", "k").orElseThrow()));
                runtime.rollback(cached);
                long start = System.nanoTime();
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.put(tx, "a", "k", bytes("left"));
                runtime.put(tx, "b", "k", bytes("right"));
                runtime.commit(tx);
                assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
                var reader = runtime.begin(IsolationLevel.READ_COMMITTED);
                Assertions.assertEquals("left", text(runtime.get(reader, "a", "k").orElseThrow()));
                Assertions.assertEquals("right", text(runtime.get(reader, "b", "k").orElseThrow()));
                runtime.rollback(reader);
                assertThrows(WalMvccException.class, runtime::flush);
            } finally {
                B.getDockerClient().unpauseContainerCmd(B.getContainerId()).exec();
            }
            drain(runtime);
            Assertions.assertEquals(2, count(a));
            Assertions.assertEquals(2, count(b));
        }
    }

    /**
     * Waiting for a limited log batch all materialization while allowing connection pool to be rebuilt after failure recovery.
     *
     * @param runtime runtime
     * @throws Exception if the operation cannot complete
     */
    private static void drain(final WalMvccRuntime runtime) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        do {
            try {
                runtime.flush();
            } catch (final WalMvccException transientFailure) {
                if (System.nanoTime() > until) {
                    throw transientFailure;
                }
            }
            var stats = runtime.stats();
            if (stats.materializedSequences().values().stream().allMatch(sequence -> sequence == stats.publishedSequence())) {
                return;
            }
            Thread.sleep(10);
        } while (System.nanoTime() <= until);
        fail("materialization did not catch up");
    }

    /**
     * Reads the exclusive version Table Line Number to prove that replay and collection are not duplicated or lost.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long count(final HikariDataSource dataSource) throws Exception {
        try (
                var connection = dataSource.getConnection(); var query = connection.createStatement();
                var rows = query.executeQuery("select count(*) from azir_wal_version")) {
            rows.next();
            return rows.getLong(1);
        }
    }

    /**
     * You can suspend a test package of materialization without changing the real MySQL reading and writing.
     */
    private static final class ControlledStore implements WalMaterializedStore {

        final WalMaterializedStore delegate;

        volatile boolean paused;

        /**
         * Saves the actual packaged materialization storage.
         *
         * @param delegate delegate
         */
        ControlledStore(final WalMaterializedStore delegate) {
            this.delegate = delegate;
        }

        /**
         * Initialization of fixed bindings.
         *
         * @param id id
         * @param shard logical shard identifier
         */
        @Override
        public void initialize(final UUID id, final String shard) {
            delegate.initialize(id, shard);
        }

        /**
         * Reads the real checkpoint.
         *
         * @return the operation result
         */
        @Override
        public WalCheckpoint checkpoint() {
            return delegate.checkpoint();
        }

        /**
         * Continue to call the real backend when rejecting materialization, recovery.
         *
         * @param commits commits
         * @return the operation result
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public WalCheckpoint apply(final List<WalCommit> commits) {
            if (paused) {
                throw new IllegalStateException("materialization paused");
            }
            return delegate.apply(commits);
        }

        /**
         * Read snapshot version.
         *
         * @param key business key
         * @param sequence sequence
         * @return the operation result
         */
        @Override
        public java.util.Optional<WalValue> get(final String key, final long sequence) {
            return delegate.get(key, sequence);
        }

        /**
         * Scan snapshot version.
         *
         * @param from from
         * @param to to
         * @param sequence sequence
         * @return the operation result
         */
        @Override
        public java.util.NavigableMap<String, WalValue> scan(final String from, final String to, final long sequence) {
            return delegate.scan(from, to, sequence);
        }

        /**
         * Bounded Cleans up old version.
         *
         * @param safePoint oldest retained snapshot timestamp
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            return delegate.gc(safePoint, limit);
        }
    }
}

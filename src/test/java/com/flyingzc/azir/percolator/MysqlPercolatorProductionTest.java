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

package com.flyingzc.azir.percolator;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.sql.PercolatorSqlEngine;
import com.flyingzc.azir.sql.ShardResolver;
import com.flyingzc.azir.sql.SqlExecutionOptions;
import com.flyingzc.azir.sql.SqlValueCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.maintenance.MaintenanceOptions;

import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;

import org.testcontainers.containers.MySQLContainer;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import java.util.concurrent.atomic.AtomicBoolean;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;

/**
 * Accept and accept snapshot lease, GC, recovery and durable identity protections for production portals on genuine independent shard.
 */
@Testcontainers
class MysqlPercolatorProductionTest {

    @Container
    static final MySQLContainer<?> A = new MySQLContainer<>("mysql:8.4").withDatabaseName("pc_production").withUsername("test").withPassword("test");

    @Container
    static final MySQLContainer<?> B = new MySQLContainer<>("mysql:8.4").withDatabaseName("pc_production").withUsername("test").withPassword("test");

    private static DataSource a;

    private static DataSource b;

    private static final ShardResolver ROUTES = ShardResolver.hashByKey(List.of("a", "b"));

    /**
     * Creates a caller-owned pool with the same durable route for all production outlets.
     */
    @BeforeAll
    static void open() {
        a = MysqlDataSourceFactory.create(A.getJdbcUrl(), A.getUsername(), A.getPassword());
        b = MysqlDataSourceFactory.create(B.getJdbcUrl(), B.getUsername(), B.getPassword());
    }

    /**
     * All maintenance threads exit before closing the pool.
     *
     * @throws Exception if the operation cannot complete
     */
    @AfterAll
    static void close() throws Exception {
        ((AutoCloseable) a).close();
        ((AutoCloseable) b).close();
    }

    /**
     * Cross-client GC protects RR and fixed RC pages, rephysicalizes collection old payload and maintains the original commit query.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcProtectsSnapshotsAcrossClientsAndRestart() throws Exception {
        String key = "history:" + UUID.randomUUID();
        PercolatorRuntime.Commit original;
        long floor;
        try (var writer = engine(); var collector = engine()) {
            original = write(writer.runtime(), key, "v0");
            try (var rr = writer.runtime().begin(IsolationLevel.REPEATABLE_READ); var rc = writer.runtime().begin(IsolationLevel.READ_COMMITTED)) {
                var old = rr.statement();
                var statement = rc.statement();
                for (int i = 1; i <= 5; i++) {
                    write(writer.runtime(), key, "v" + i);
                }
                floor = collector.runtime().maintain(1).safePoint();
                assertTrue(floor < rr.startTs());
                Assertions.assertEquals("v0", text(old.get("a", key)));
                Assertions.assertEquals("v0", text(statement.get("b", key)));
                Assertions.assertEquals("v0", new String(statement.scanPage("a", new KeyRange(key, key + "~"), null, 1).entries().getFirst().value(), StandardCharsets.UTF_8));
                Assertions.assertEquals("v5", text(rc.statement().get("a", key)));
            }
            for (int i = 0; i < 20; i++) {
                var result = collector.runtime().maintain(1);
                assertTrue(result.safePoint() >= floor);
                floor = result.safePoint();
                assertTrue(result.deletedVersions() <= 2);
            }
            Assertions.assertEquals(1, count(a, key));
            Assertions.assertEquals(1, count(b, key));
        }
        try (var restarted = engine(); var tx = restarted.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
            Assertions.assertEquals("v5", text(tx.get("a", key)));
            Assertions.assertEquals(original.commitTs(), restarted.runtime().lookup(original.handle()).commitTs());
            assertTrue(restarted.runtime().maintain(1).safePoint() >= floor);
        }
    }

    /**
     * Delete tombstone must be left and empty byte business values cannot be deleted as tombstone.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcRetainsDeleteAndEmptyValue() throws Exception {
        String key = "tombstone:" + UUID.randomUUID();
        try (var engine = engine()) {
            write(engine.runtime(), key, "before");
            try (var tx = engine.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
                tx.delete("a", key);
                tx.put("b", key, new byte[0]);
                tx.commit();
            }
            for (int i = 0; i < 10; i++) {
                engine.runtime().maintain(128);
            }
            Assertions.assertEquals(0, count(a, key));
            Assertions.assertEquals(1, count(b, key));
        }
        try (var engine = engine(); var tx = engine.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
            assertTrue(tx.get("a", key).isEmpty());
            assertArrayEquals(new byte[0], tx.get("b", key).orElseThrow());
            assertNull(tx.scanPage("a", new KeyRange(key, key + "~"), null, 1).entries().getFirst().value());
        }
    }

    /**
     * Lease old statements, own buffer, page breaks, prewrite and commit are all rejected after they expire.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredReadersCannotOutliveGcOrPublishLateWrites() throws Exception {
        String key = "expired:" + UUID.randomUUID();
        try (var engine = engine(); var tx = engine.runtime().begin(IsolationLevel.REPEATABLE_READ)) {
            var view = tx.statement();
            tx.put("a", key, bytes("private"));
            try (var c = a.getConnection(); var q = c.prepareStatement("update pc_readers set expires_at=0 where start_ts=?")) {
                q.setLong(1, tx.startTs());
                Assertions.assertEquals(1, q.executeUpdate());
            }
            engine.runtime().maintain(128);
            Assertions.assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, assertThrows(MvccException.class, () -> view.get("a", key)).errorCode());
            assertThrows(MvccException.class, () -> view.scanPage("a", new KeyRange(null, null), null, 1));
            assertThrows(MvccException.class, () -> tx.put("b", key, bytes("late")));
            assertThrows(MvccException.class, tx::prewrite);
            assertThrows(MvccException.class, tx::commit);
            tx.rollback();
            Assertions.assertEquals(0, count(a, key));
        }
    }

    /**
     * New receipt must contain durable namespace, and old or counterfeit space cannot be wrongly identified on another authority.
     */
    @Test
    void namespaceAndRoutesCannotBeConfused() {
        String key = "identity:" + UUID.randomUUID();
        try (var engine = engine()) {
            var receipt = write(engine.runtime(), key, "saved");
            assertNotNull(receipt.handle().namespace());
            assertThrows(IllegalArgumentException.class, () -> engine.runtime().lookup(new PercolatorRuntime.Handle(receipt.handle().startTs(), receipt.handle().primary())));
            assertThrows(IllegalArgumentException.class,
                    () -> engine.runtime().lookup(new PercolatorRuntime.Handle(receipt.handle().startTs(), receipt.handle().primary(), UUID.randomUUID().toString())));
            assertThrows(IllegalArgumentException.class, () -> PercolatorSqlEngine.mysql(a, Map.of("a", b, "b", a), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED));
            assertThrows(IllegalArgumentException.class, () -> PercolatorSqlEngine.mysql(b, Map.of("a", a, "b", b), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED));
        }
    }

    /**
     * Reset timestamp allocation from initial value when TSO is lost bound.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingTimestampRefusesReinitialization() throws Exception {
        long sequence;
        try (var ignored = engine(); var c = a.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select current_value from mvcc_tso where sequence_name='global'")) {
            r.next();
            sequence = r.getLong(1);
        }
        try (var c = a.getConnection(); var q = c.createStatement()) {
            q.executeUpdate("delete from mvcc_tso where sequence_name='global'");
        }
        try {
            assertThrows(IllegalArgumentException.class, MysqlPercolatorProductionTest::engine);
        } finally {
            try (var c = a.getConnection(); var q = c.prepareStatement("insert into mvcc_tso values('global',?)")) {
                q.setLong(1, sequence);
                q.executeUpdate();
            }
        }
    }

    /**
     * Background recovery must clean up the missing prewrite; no session and background can continue after the engine has shut down.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void schedulerRecoversOrphansAndCloseStopsWork() throws Exception {
        String key = "orphan:" + UUID.randomUUID();
        var engine = PercolatorSqlEngine.mysql(a, Map.of("a", a, "b", b), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED,
                PercolatorRuntime.Options.defaults(), SqlExecutionOptions.defaults(), new MaintenanceOptions(20, 128, 10_000));
        PercolatorTransaction orphan = engine.runtime().begin(IsolationLevel.REPEATABLE_READ);
        PercolatorRuntime.Handle id;
        try (engine) {
            orphan.put("a", key, bytes("a"));
            orphan.put("b", key, bytes("b"));
            orphan.prewrite();
            id = orphan.handle().orElseThrow();
            for (DataSource source : List.of(a, b)) {
                try (var c = source.getConnection(); var q = c.prepareStatement("update pc_lock set expires_at=0 where start_ts=?")) {
                    q.setLong(1, orphan.startTs());
                    q.executeUpdate();
                }
            }
            long deadline = System.nanoTime() + 10_000_000_000L;
            while ((count(a, key) > 0 || count(b, key) > 0 || engine.maintenanceHealth().orElseThrow().successes() == 0) && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            Assertions.assertEquals(0, count(a, key));
            Assertions.assertEquals(0, count(b, key));
            Assertions.assertEquals(PercolatorStore.State.ROLLED_BACK, engine.runtime().lookup(id).state());
            assertTrue(engine.maintenanceHealth().orElseThrow().successes() > 0);
        }
        long rounds = engine.maintenanceHealth().orElseThrow().successes();
        Thread.sleep(60);
        Assertions.assertEquals(rounds, engine.maintenanceHealth().orElseThrow().successes());
        assertThrows(IllegalStateException.class, engine::openSession);
        assertThrows(IllegalStateException.class, () -> orphan.get("a", key));
    }

    /**
     * After prewrite has lost control of session, the old call may not be borrowed from connection distribution commit - concurrent -bub-primary.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void lostGateCannotResumeTheNextCommitPhase() throws Exception {
        try (var initialized = engine()) {
        }
        var lifecycle = new MysqlPercolatorLifecycle(a, Map.of("a", a, "b", b), 60_000);
        var rawA = new MysqlPercolatorStore("a", a);
        var rawB = new MysqlPercolatorStore("b", b);
        AtomicBoolean killed = new AtomicBoolean();
        PercolatorStore faultyB = (PercolatorStore) Proxy.newProxyInstance(PercolatorStore.class.getClassLoader(), new Class<?>[]{PercolatorStore.class}, (proxy, method, args) -> {
            try {
                Object result = method.invoke(rawB, args);
                if (method.getName().equals("prewrite") && killed.compareAndSet(false, true)) {
                    long owner;
                    try (var c = a.getConnection(); var q = c.prepareStatement("select is_used_lock(?)")) {
                        q.setString(1, "azir:pc:" + lifecycle.namespace());
                        try (var r = q.executeQuery()) {
                            r.next();
                            owner = r.getLong(1);
                        }
                    }
                    assertTrue(owner > 0);
                    try (var c = a.getConnection(); var q = c.createStatement()) {
                        q.execute("kill connection " + owner);
                    }
                }
                return result;
            } catch (final InvocationTargetException failure) {
                throw failure.getCause();
            }
        });
        String key = "fenced:" + UUID.randomUUID();
        try (
                var runtime = new PercolatorRuntime(new MysqlTimestampOracle(a), Map.of("a", rawA, "b", faultyB), PercolatorRuntime.Options.defaults(), lifecycle);
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ)) {
            tx.put("a", key, bytes("unpublished"));
            tx.put("b", key, bytes("unpublished"));
            assertThrows(MvccException.class, tx::commit);
            Assertions.assertNotEquals(PercolatorTransaction.State.COMMITTED, tx.state());
            var id = tx.handle().orElseThrow();
            Assertions.assertNotEquals(PercolatorStore.State.COMMITTED, runtime.lookup(id).state());
            tx.rollback();
            Assertions.assertEquals(PercolatorStore.State.ROLLED_BACK, runtime.lookup(id).state());
            Assertions.assertEquals(0, count(a, key));
            Assertions.assertEquals(0, count(b, key));
        }
    }

    /**
     * The default manufacturing plant uses durable to read lease and automaintenance.
     *
     * @return the operation result
     */
    private static PercolatorSqlEngine engine() {
        return PercolatorSqlEngine.mysql(a, Map.of("a", a, "b", b), ROUTES, SqlValueCodec.utf8(), IsolationLevel.READ_COMMITTED);
    }

    /**
     * Two shard commit s of the same business value.
     *
     * @param runtime runtime
     * @param key business key
     * @param value value to write
     * @return the operation result
     */
    private static PercolatorRuntime.Commit write(final PercolatorRuntime runtime, final String key, final String value) {
        try (var tx = runtime.begin(IsolationLevel.REPEATABLE_READ)) {
            tx.put("a", key, bytes(value));
            tx.put("b", key, bytes(value));
            return tx.commit();
        }
    }

    /**
     * Checks the actual number of payload, and write decision allows for retention.
     *
     * @param source database connection source
     * @param key business key
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static int count(final DataSource source, final String key) throws Exception {
        try (var c = source.getConnection(); var q = c.prepareStatement("select count(*) from pc_data where user_key=?")) {
            q.setBytes(1, bytes(key));
            try (var r = q.executeQuery()) {
                r.next();
                return r.getInt(1);
            }
        }
    }

    /**
     * Fixed text encoding is used for physical primary key and business values.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decoding visible business values.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String text(final Optional<byte[]> value) {
        return new String(value.orElseThrow(), StandardCharsets.UTF_8);
    }
}

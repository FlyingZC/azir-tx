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

package com.flyingzc.azir.storage.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The real MySQL control library verify process-level vector lease and the sequence below GC.
 */
@Testcontainers
class MysqlVectorSnapshotLeaseStoreTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("vector_reader").withUsername("test").withPassword("test");

    private static HikariDataSource center;

    private MysqlVectorSnapshotLeaseStore store;

    /**
     * Test connection only for isolation control database of this container.
     */
    @BeforeAll
    static void open() {
        center = (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Closes the connection pool created by this class.
     */
    @AfterAll
    static void close() {
        if (center != null) {
            center.close();
        }
    }

    /**
     * For each instance, the apparent reconstruction test is exclusive to lease tables. The initialization of production itself must not empty old lines.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = center.getConnection(); Statement sql = connection.createStatement()) {
            sql.execute("drop table if exists mvcc_vector_reader_lease_v1");
            sql.execute("drop table if exists mvcc_vector_gc_floor_v1");
        }
        store = new MysqlVectorSnapshotLeaseStore(center, List.of("s1", "s2"), 4);
        store.initializeProduct();
    }

    /**
     * Active processes protect old cut-off, move down the line after a single renewal of lease and release to current cut-off.
     */
    @Test
    void activeReaderMustPinAndReleaseVectorGcFloor() {
        try (var guard = store.acquire("reader-a", flat(0), 5_000)) {
            Assertions.assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            guard.renew(flat(5));
            Assertions.assertEquals(flat(5), store.advanceSafeFloor(flat(10)));
            Assertions.assertEquals(flat(5), store.currentSafeFloor());
        }
        Assertions.assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
        MvccException stale = assertThrows(MvccException.class,
                () -> store.acquire("reader-b", flat(5), 5_000));
        Assertions.assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, stale.errorCode());
    }

    /**
     * Each strip and centre decision takes the oldest effective component separately and cannot be degraded to a reference limit.
     */
    @Test
    void differentStripesMustKeepIndependentFloors() {
        var active = cut(7, List.of(2L, 8L, 3L, 9L), List.of(4L, 6L, 5L, 7L));
        var published = flat(10);
        try (var guard = store.acquire("striped-reader", active, 5_000)) {
            Assertions.assertEquals(active, store.advanceSafeFloor(published));
        }
        Assertions.assertEquals(published, store.advanceSafeFloor(published));
    }

    /**
     * The old process cannot be renewed after the conservative deadline, and GC can advance after database lease expires.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredGuardCannotKeepAnOldSnapshotAlive() throws Exception {
        var guard = store.acquire("expiring-reader", flat(0), 200);
        Thread.sleep(260);
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, guard::requireActive).errorCode());
        Assertions.assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> guard.renew(flat(0))).errorCode());
        guard.close();
    }

    /**
     * The same owner could not be replaced without expiry and the new algebra could not be used again after closure.
     */
    @Test
    void ownerGenerationMustFenceDuplicateAcquisition() {
        var original = store.acquire("same-owner", flat(0), 5_000);
        Assertions.assertEquals(1, original.generation());
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> store.acquire("same-owner", flat(0), 5_000)).errorCode());
        original.close();
        try (var next = store.acquire("same-owner", flat(0), 5_000)) {
            Assertions.assertEquals(2, next.generation());
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, original::requireActive).errorCode());
        }
    }

    /**
     * Restarts to keep only the same path layout, and error route collection does not explain the existing lower bounds of GC.
     */
    @Test
    void restartPreservesFloorAndRejectsDifferentLayout() {
        Assertions.assertEquals(flat(3), store.advanceSafeFloor(flat(3)));
        var reopened = new MysqlVectorSnapshotLeaseStore(center, List.of("s2", "s1"), 4);
        reopened.initializeProduct();
        Assertions.assertEquals(flat(3), reopened.currentSafeFloor());
        var mismatched = new MysqlVectorSnapshotLeaseStore(center, List.of("s1", "s3"), 4);
        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, mismatched::initializeProduct).errorCode());
    }

    /**
     * Only old reader or old lease can be protected when registration and GC are competing for a lower line.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void leaseRegistrationAndGcAdvanceMustSerialize() throws Exception {
        try (
                Connection blocker = center.getConnection();
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try (
                    Statement lock = blocker.createStatement();
                    var ignored = lock.executeQuery("""
                            select generation from mvcc_vector_gc_floor_v1
                            where singleton_id = 1 for update
                            """)) {
                assertTrue(ignored.next());
            }
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            var lease = workers.submit(() -> {
                ready.countDown();
                start.await();
                try {
                    return (Object) store.acquire("racing-reader", flat(0), 5_000);
                } catch (final MvccException ex) {
                    return ex;
                }
            });
            var advance = workers.submit(() -> {
                ready.countDown();
                start.await();
                return store.advanceSafeFloor(flat(10));
            });
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            blocker.commit();
            Object outcome = lease.get(10, TimeUnit.SECONDS);
            var floor = advance.get(10, TimeUnit.SECONDS);
            if (outcome instanceof MysqlVectorSnapshotLeaseStore.Guard guard) {
                try (guard) {
                    Assertions.assertEquals(flat(0), floor);
                }
            } else {
                Assertions.assertEquals(ErrorCode.SNAPSHOT_TOO_OLD,
                        ((MvccException) outcome).errorCode());
                Assertions.assertEquals(flat(10), floor);
            }
        }
    }

    /**
     * Multiple RRs share a process lease, which ends with the old reader exit before moving down.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void processRuntimeMustProtectAllActiveReadsWithOneLease() throws Exception {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        try (
                var runtime = new MysqlVectorReaderProtectionRuntime(
                        store, published::get, "one-process", 5_000, 1_000, 100)) {
            runtime.start();
            var rr = runtime.beginRead();
            var rc = runtime.beginRead();
            Assertions.assertEquals(flat(0), rr.snapshot());
            published.set(flat(10));
            runtime.refresh();
            Assertions.assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            Assertions.assertEquals(1, durableLeaseRows());
            rc.close();
            runtime.refresh();
            Assertions.assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            rr.close();
            runtime.refresh();
            Assertions.assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
        }
    }

    /**
     * The process closes without first reading or closing, but durable lease must be kept to its handle for exit.
     */
    @Test
    void closingProcessMustNotReleaseInFlightReadTooEarly() {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        var runtime = new MysqlVectorReaderProtectionRuntime(
                store, published::get, "closing-process", 5_000, 1_000, 100);
        runtime.start();
        var reader = runtime.beginRead();
        published.set(flat(10));
        runtime.close();
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, reader::requireActive).errorCode());
        Assertions.assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
        reader.close();
        Assertions.assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
    }

    /**
     * Reader Quantities bounded and an old release side cannot reopen collection version.
     */
    @Test
    void runtimeMustRejectReaderOverflowAndOldPublishedCut() {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        try (
                var runtime = new MysqlVectorReaderProtectionRuntime(
                        store, published::get, "bounded-process", 5_000, 1_000, 1)) {
            runtime.start();
            try (var reader = runtime.beginRead()) {
                Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                        assertThrows(MvccException.class, runtime::beginRead).errorCode());
            }
            published.set(flat(10));
            runtime.refresh();
            store.advanceSafeFloor(flat(10));
            published.set(flat(5));
            Assertions.assertEquals(ErrorCode.SNAPSHOT_TOO_OLD,
                    assertThrows(MvccException.class, runtime::beginRead).errorCode());
        }
    }

    /**
     * Centre failure allows to undo all read handles while running, while the old lease protects version until its natural maturity.
     */
    @Test
    void failedRefreshMustRevokeLocalReadersWithoutLosingProtection() {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        AtomicReference<Boolean> fail = new AtomicReference<>(false);
        try (
                var runtime = new MysqlVectorReaderProtectionRuntime(
                        store, () -> {
                            if (fail.get()) {
                                throw new IllegalStateException("publisher unavailable");
                            }
                            return published.get();
                        }, "failing-process", 5_000, 1_000, 100)) {
            runtime.start();
            try (var reader = runtime.beginRead()) {
                published.set(flat(10));
                fail.set(true);
                assertThrows(IllegalStateException.class, runtime::refresh);
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, reader::requireActive).errorCode());
                Assertions.assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            }
        }
    }

    /**
     * The old reading handle must also be shut down immediately when a new cut fails, and lease continues to be saved until release.
     */
    @Test
    void failedCutAcquisitionMustRevokeExistingReaders() {
        AtomicReference<Boolean> unavailable = new AtomicReference<>(false);
        var runtime = new MysqlVectorReaderProtectionRuntime(
                store, () -> {
                    if (unavailable.get()) {
                        throw new IllegalStateException("publisher disappeared");
                    }
                    return flat(0);
                }, "cut-failure", 5_000, 1_000, 100);
        runtime.start();
        var old = runtime.beginRead();
        unavailable.set(true);
        assertThrows(IllegalStateException.class, runtime::beginRead);
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, old::requireActive).errorCode());
        Assertions.assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
        runtime.close();
        old.close();
        Assertions.assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
    }

    /**
     * Durable lease rows are charged on a process basis instead of an transaction quantity.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private long durableLeaseRows() throws Exception {
        try (
                Connection connection = center.getConnection(); Statement query = connection.createStatement();
                var row = query.executeQuery("select count(*) from mvcc_vector_reader_lease_v1")) {
            row.next();
            return row.getLong(1);
        }
    }

    /**
     * Constructs the minimum repeatable two shard vectors with the same weight.
     *
     * @param value value to write
     * @return the operation result
     */
    private static MysqlStripedSnapshotOracle.Snapshot flat(final long value) {
        return cut(value, List.of(value, value, value, value),
                List.of(value, value, value, value));
    }

    /**
     * A fixed routing by name constructs each tangent that is independent of verify.
     *
     * @param decision decision
     * @param first first
     * @param second second
     * @return the operation result
     */
    private static MysqlStripedSnapshotOracle.Snapshot cut(final long decision,
                                                           final List<Long> first, final List<Long> second) {
        return new MysqlStripedSnapshotOracle.Snapshot(
                decision, Map.of("s1", first, "s2", second));
    }
}

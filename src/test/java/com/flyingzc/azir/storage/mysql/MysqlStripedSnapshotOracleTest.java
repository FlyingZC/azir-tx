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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two real MySQL shard verify subdivisions watermark stable tangent, RC retrieving and RR fixed visibility.
 */
@Testcontainers
class MysqlStripedSnapshotOracleTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");

    private static HikariDataSource left;

    private static HikariDataSource right;

    private MysqlStripedSnapshotOracle oracle;

    /**
     * Creates two separate database and connection pool on the same server.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeAll
    static void openDatabases() throws Exception {
        createDatabase("stripe_left");
        createDatabase("stripe_right");
        left = pool("stripe_left");
        right = pool("stripe_right");
    }

    /**
     * Test user only gets target isolation database permissions.
     *
     * @param name name
     * @throws Exception if the operation cannot complete
     */
    private static void createDatabase(final String name) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + name + "`; "
                        + "grant all privileges on `" + name + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /**
     * JDBC URL replaces only the physical database name of this test.
     *
     * @param database database
     * @return the operation result
     */
    private static HikariDataSource pool(final String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Closes two pieces of test connection pool.
     */
    @AfterAll
    static void closeDatabases() {
        if (left != null)
            left.close();
        if (right != null)
            right.close();
    }

    /**
     * Recreate four empty durable watermark and empty version chain for each case.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void reset() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeIsolated(left, 4);
        MysqlStripedMvccCapacityProbe.initializeIsolated(right, 4);
        oracle = new MysqlStripedSnapshotOracle(Map.<String, DataSource>of("s1", left, "s2", right), 4, 8);
    }

    /**
     * The old RR never saw the following two commit s. The new RC recaptured and saw two version s.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void fixedRrAndFreshRcMustUseTheirOwnStableVector() throws Exception {
        var oldRr = oracle.capture(() -> 0);
        commit(left, "striped:left", new byte[]{1}, 1);
        commit(right, "striped:right", new byte[]{2}, 2);
        assertTrue(oracle.read(oldRr, "s1", "striped:left", 1).isEmpty());
        assertTrue(oracle.read(oldRr, "s2", "striped:right", 2).isEmpty());
        var newRc = oracle.capture(() -> 0);
        Assertions.assertEquals(1, newRc.sequence("s1", 1));
        Assertions.assertEquals(1, newRc.sequence("s2", 2));
        assertArrayEquals(new byte[]{1}, oracle.read(newRc, "s1", "striped:left", 1).orElseThrow());
        assertArrayEquals(new byte[]{2}, oracle.read(newRc, "s2", "striped:right", 2).orElseThrow());
    }

    /**
     * Commit, which occurs after the first collection, must trigger retry, and cannot be returned to a mixed shard watermark before and after commit.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void commitBetweenCollectsMustRetryBeforeReturningSnapshot() throws Exception {
        AtomicInteger centerReads = new AtomicInteger();
        var snapshot = oracle.capture(() -> {
            if (centerReads.getAndIncrement() == 0) {
                try {
                    commit(left, "striped:between", new byte[]{3}, 1);
                } catch (final Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return 7;
        });
        Assertions.assertEquals(2, centerReads.get());
        Assertions.assertEquals(7, snapshot.decisionEpoch());
        Assertions.assertEquals(1, snapshot.sequence("s1", 1));
        assertArrayEquals(new byte[]{3}, oracle.read(snapshot, "s1", "striped:between", 1).orElseThrow());
    }

    /**
     * Parallel shard collections must also detect an intermediate commit and recapture before or after the control reading of watermark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void parallelCollectorMustRetryOnMidpointCommit() throws Exception {
        try (
                var parallel = new MysqlStripedSnapshotOracle(
                        Map.<String, DataSource>of("s1", left, "s2", right), 4, 8, true)) {
            AtomicInteger centerReads = new AtomicInteger();
            var snapshot = parallel.capture(() -> {
                if (centerReads.getAndIncrement() == 0) {
                    try {
                        commit(right, "striped:parallel", new byte[]{4}, 3);
                    } catch (final Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                return 9;
            });
            Assertions.assertEquals(2, centerReads.get());
            Assertions.assertEquals(2, parallel.collectionAttempts());
            Assertions.assertEquals(9, snapshot.decisionEpoch());
            assertArrayEquals(new byte[]{4}, parallel.read(snapshot, "s2", "striped:parallel", 3)
                    .orElseThrow());
        }
    }

    /**
     * When two series of X-0s form the chain of cause and effect, snapshot can't just keep the previous one and leave the latter one behind.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void dependentCommitsBetweenCollectsMustBeObservedTogether() throws Exception {
        AtomicInteger centerReads = new AtomicInteger();
        var snapshot = oracle.capture(() -> {
            if (centerReads.getAndIncrement() == 0) {
                try {
                    commit(left, "striped:causal:left", new byte[]{5}, 1);
                    commit(right, "striped:causal:right", new byte[]{6}, 2);
                } catch (final Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return 0;
        });
        Assertions.assertEquals(2, centerReads.get());
        Assertions.assertEquals(2, oracle.collectionAttempts());
        assertArrayEquals(new byte[]{5}, oracle.read(snapshot, "s1", "striped:causal:left", 1)
                .orElseThrow());
        assertArrayEquals(new byte[]{6}, oracle.read(snapshot, "s2", "striped:causal:right", 2)
                .orElseThrow());
    }

    /**
     * Refusal to guess the snapshot border when missing an arbitrary segment.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingStripeMustFailClosed() throws Exception {
        try (Connection connection = right.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from bench_striped_mvcc_state where stripe_id = 2");
        }
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class, () -> oracle.capture(() -> 0)).errorCode());
    }

    /**
     * Same-key follow-up writing must start watermark with the previous commit, otherwise returns write conflict.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void checkedLocalCommitMustRejectNewerSameKeyVersion() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(left, 4);
        Assertions.assertEquals(1, commitChecked(left, "striped:checked", new byte[]{1}, 1, 0));
        Assertions.assertEquals(36003, assertThrows(SQLException.class,
                () -> commitChecked(left, "striped:checked", new byte[]{2}, 1, 0))
                .getErrorCode());
        Assertions.assertEquals(2, commitChecked(left, "striped:checked", new byte[]{3}, 1, 1));
    }

    /**
     * Durable cross-shard intent cannot bypass the same key lock when local single shard commit exists.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void checkedLocalCommitMustRejectForeignIntent() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(left, 4);
        try (Connection connection = left.getConnection(); PreparedStatement guard = connection.prepareStatement("""
                insert into bench_striped_mvcc_key_lock(user_key) values (?)
                """); PreparedStatement intent = connection.prepareStatement("""
                insert into bench_striped_mvcc_intent(user_key, owner_tx_id) values (?, ?)
                """)) {
            connection.setAutoCommit(false);
            guard.setBytes(1, "striped:intent".getBytes(StandardCharsets.UTF_8));
            guard.executeUpdate();
            intent.setBytes(1, "striped:intent".getBytes(StandardCharsets.UTF_8));
            intent.setLong(2, 42);
            intent.executeUpdate();
            connection.commit();
        }
        Assertions.assertEquals(36002, assertThrows(SQLException.class,
                () -> commitChecked(left, "striped:intent", new byte[]{4}, 2, 0))
                .getErrorCode());
    }

    /**
     * Waiting for the same key lock to read the version pre-release commit and avoid the old reading view default write conflict.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void checkedLocalCommitMustSeeVersionCommittedDuringKeyLockWait() throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(left, 4);
        byte[] key = "striped:lock-wait".getBytes(StandardCharsets.UTF_8);
        try (Connection blocker = left.getConnection(); var worker = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try (PreparedStatement guard = blocker.prepareStatement("""
                    insert into bench_striped_mvcc_key_lock(user_key) values (?)
                    """); PreparedStatement state = blocker.prepareStatement("""
                    update bench_striped_mvcc_state set committed_seq = 1 where stripe_id = 1
                    """); PreparedStatement version = blocker.prepareStatement("""
                    insert into bench_striped_mvcc_data(user_key, local_seq, stripe_id, payload)
                    values (?, 1, 1, ?)
                    """)) {
                guard.setBytes(1, key);
                guard.executeUpdate();
                state.executeUpdate();
                version.setBytes(1, key);
                version.setBytes(2, new byte[]{7});
                version.executeUpdate();
            }
            CountDownLatch started = new CountDownLatch(1);
            var waiting = worker.submit(() -> {
                started.countDown();
                return commitChecked(left, "striped:lock-wait", new byte[]{8}, 1, 0);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
            blocker.commit();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> waiting.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(36003, ((SQLException) failure.getCause()).getErrorCode());
        }
    }

    /**
     * Single key version and target subsection watermark by the same shard storage process atomic commit.
     *
     * @param dataSource database connection source
     * @param key business key
     * @param value value to write
     * @param stripe stripe
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long commit(final HikariDataSource dataSource, final String key, final byte[] value, final int stripe) throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_commit_v1(?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setBytes(2, value);
            call.setInt(3, stripe);
            try (ResultSet row = call.executeQuery()) {
                assertTrue(row.next());
                return row.getLong(1);
            }
        }
    }

    /**
     * The single-key physical commit prototype that calls the key lock, intent and version conflict check.
     *
     * @param dataSource database connection source
     * @param key business key
     * @param value value to write
     * @param stripe stripe
     * @param startSeq start seq
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long commitChecked(final HikariDataSource dataSource, final String key, final byte[] value,
                                      final int stripe, final long startSeq) throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_commit_checked_v2(?, ?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setBytes(2, value);
            call.setInt(3, stripe);
            call.setLong(4, startSeq);
            try (ResultSet row = call.executeQuery()) {
                assertTrue(row.next());
                return row.getLong(1);
            }
        }
    }
}

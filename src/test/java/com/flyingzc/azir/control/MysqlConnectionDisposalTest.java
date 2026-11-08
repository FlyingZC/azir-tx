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

package com.flyingzc.azir.control;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.ProxyConnection;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Only the memory JDBC failure stake verify connection is disabled without database or network.
 */
class MysqlConnectionDisposalTest {

    /**
     * When the release lock and abort simultaneously failed, the general pool connection must be preserved and the entire same database domain stops borrowing connection.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void failedReleaseAndAbortQuarantineGenericPoolWithoutReturningConnection() throws SQLException {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        var peer = new FaultyJdbc(jdbc.serverId);
        try (Connection ignored = MysqlConnectionContext.connection(peer.source)) {
            // Pre-bound another data source, with the verify quarantine range covering the entire database domain.
        }
        var held = new AtomicReference<ConnectionState>();
        assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationLock(jdbc.source, 0, () -> {
            held.set(jdbc.last.get());
            jdbc.failRelease = true;
            jdbc.failAbort = true;
            return null;
        }));
        Assertions.assertEquals(1, held.get().abortCalls);
        Assertions.assertEquals(0, held.get().closeCalls);
        assertFalse(held.get().closed);
        int ownBorrows = jdbc.borrows.get();
        int peerBorrows = peer.borrows.get();
        assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(jdbc.source));
        assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(peer.source));
        Assertions.assertEquals(ownBorrows, jdbc.borrows.get());
        Assertions.assertEquals(peerBorrows, peer.borrows.get());
    }

    /**
     * Independent SQL transaction rollback and abort failed to return dirty connection to the pool by close or autoCommit.
     */
    @Test
    void failedRollbackAndAbortQuarantineStandaloneTransactionConnection() {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        jdbc.failRollback = true;
        jdbc.failAbort = true;
        var error = assertThrows(SQLException.class, () -> MysqlConnectionContext.inTransaction(jdbc.source, connection -> {
            throw new SQLException("original failure");
        }));
        Assertions.assertEquals("original failure", error.getMessage());
        Assertions.assertEquals(2, error.getSuppressed().length);
        Assertions.assertEquals(0, jdbc.last.get().closeCalls);
        Assertions.assertEquals(0, jdbc.last.get().autoCommitRestores);
        assertFalse(jdbc.last.get().autoCommit);
        assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(jdbc.source));
    }

    /**
     * The use of connection rollback failure must contaminate the outer release connection and cannot continue to be published or returned to the pond even if the business swallows an
     * anomaly.
     */
    @Test
    void failedBorrowedTransactionDiscardsTheOuterPublicationConnection() {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        var held = new AtomicReference<ConnectionState>();
        assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationLock(jdbc.source, 0, () -> {
            held.set(jdbc.last.get());
            jdbc.failRollback = true;
            jdbc.failAbort = true;
            Connection cached;
            try {
                cached = MysqlConnectionContext.connection(jdbc.source);
            } catch (final SQLException ex) {
                throw new AssertionError(ex);
            }
            assertThrows(SQLException.class, () -> MysqlConnectionContext.inTransaction(jdbc.source, connection -> {
                throw new SQLException("nested failure");
            }));
            assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(jdbc.source));
            assertThrows(SQLException.class, () -> cached.prepareStatement("select 1"));
            return null;
        }));
        Assertions.assertEquals(0, held.get().closeCalls);
        Assertions.assertEquals(0, held.get().autoCommitRestores);
        Assertions.assertEquals(1, held.get().abortCalls);
    }

    /**
     * Hikari must receive its original agent to perform a visible phase-out and cannot be transferred to the borrowed JDK agent.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Test
    void hikariEvictionReceivesTheOriginalPooledProxyAfterAbortFailure() throws SQLException {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        try (var pool = new RecordingHikariDataSource()) {
            pool.setDataSource(jdbc.source);
            pool.setMaximumPoolSize(1);
            pool.setMinimumIdle(0);
            pool.setConnectionTimeout(1000);
            pool.setInitializationFailTimeout(-1);
            assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationLock(pool, 0, () -> {
                jdbc.failRelease = true;
                jdbc.failAbort = true;
                return null;
            }));
            Assertions.assertInstanceOf(ProxyConnection.class, pool.evicted);
            Assertions.assertSame(pool.lastBorrowed, pool.evicted);
            assertFalse(Proxy.isProxyClass(pool.evicted.getClass()));
            int previousConnections = jdbc.borrows.get();
            jdbc.failRelease = false;
            jdbc.failAbort = false;
            Assertions.assertEquals(1, MysqlConnectionContext.withPublicationLock(pool, 0, () -> 1));
            assertTrue(jdbc.borrows.get() > previousConnections);
        }
    }

    /**
     * Returns an empty or original type default value that is not used for JDBC.
     *
     * @param type type
     * @return the operation result
     */
    private static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }

    /**
     * Record real Hikari pool agent phase-out, bottom connection still pure memory failure bars.
     */
    private static final class RecordingHikariDataSource extends HikariDataSource {

        private Connection lastBorrowed;

        private Connection evicted;

        /**
         * Creates a test object that has not yet started connection pool.
         */
        private RecordingHikariDataSource() {
        }

        /**
         * Keeps the currently actually borrowed Hikari proxy identity.
         *
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        @Override
        public Connection getConnection() throws SQLException {
            lastBorrowed = super.getConnection();
            return lastBorrowed;
        }

        /**
         * Recording phase-out objects and calling for real pool phase-out.
         *
         * @param connection connection
         */
        @Override
        public void evictConnection(final Connection connection) {
            evicted = connection;
            super.evictConnection(connection);
        }
    }

    /**
     * Creates a stand-alone session for each loan with a fixed server identity to simulate an same database alias.
     */
    private static final class FaultyJdbc implements InvocationHandler {

        private final String serverId;

        private final DataSource source;

        private final AtomicInteger borrows = new AtomicInteger();

        private final AtomicReference<ConnectionState> last = new AtomicReference<>();

        private volatile boolean failRelease;

        private volatile boolean failRollback;

        private volatile boolean failAbort;

        /**
         * Creates a data source that does not connection for any external service.
         *
         * @param serverId server id
         */
        private FaultyJdbc(final String serverId) {
            this.serverId = serverId;
            source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, this);
        }

        /**
         * Distributes memory session and provides the basic data source behaviour required for initialisation of connection pool.
         *
         * @param proxy proxy
         * @param method method
         * @param args args
         * @return the operation result
         * @throws Throwable if the operation cannot complete
         */
        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "getConnection" -> {
                    borrows.incrementAndGet();
                    var state = new ConnectionState(this);
                    last.set(state);
                    yield state.connection;
                }
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                case "toString" -> "InMemoryFaultDataSource";
                case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    /**
     * Independent records close, rollback, abort and autoCommit recovery to avoid only verify abnormal types.
     */
    private static final class ConnectionState implements InvocationHandler {

        private final FaultyJdbc jdbc;

        private final Connection connection;

        private volatile boolean autoCommit = true;

        private volatile boolean closed;

        private int closeCalls;

        private int abortCalls;

        private int autoCommitRestores;

        /**
         * Creates a bottom-up JDBC session proxy.
         *
         * @param jdbc jdbc
         */
        private ConnectionState(final FaultyJdbc jdbc) {
            this.jdbc = jdbc;
            connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, this);
        }

        /**
         * The minimum JDBC session syntax required to achieve control and Hikari initialization.
         *
         * @param proxy proxy
         * @param method method
         * @param args args
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         * @throws Throwable if the operation cannot complete
         */
        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                case "toString" -> "InMemoryFaultConnection";
                case "getAutoCommit" -> autoCommit;
                case "setAutoCommit" -> {
                    if (!autoCommit && (boolean) args[0]) {
                        autoCommitRestores++;
                    }
                    autoCommit = (boolean) args[0];
                    yield null;
                }
                case "getTransactionIsolation" -> Connection.TRANSACTION_READ_COMMITTED;
                case "getCatalog", "getSchema" -> "control";
                case "isValid" -> !closed;
                case "isClosed" -> closed;
                case "close" -> {
                    closeCalls++;
                    closed = true;
                    yield null;
                }
                case "abort" -> {
                    abortCalls++;
                    if (jdbc.failAbort) {
                        throw new SQLException("abort failed", "HY000");
                    }
                    closed = true;
                    yield null;
                }
                case "rollback" -> {
                    if (jdbc.failRollback) {
                        throw new SQLException("rollback failed", "HY000");
                    }
                    yield null;
                }
                case "prepareStatement" -> statement((String) args[0]);
                case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
                default -> defaultValue(method.getReturnType());
            };
        }

        /**
         * Simulates a naming lock and field query, injecting normal SQL errors into the specified release statement.
         *
         * @param sql sql
         * @return the operation result
         */
        private PreparedStatement statement(final String sql) {
            return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                        if (method.getName().equals("executeQuery")) {
                            if (sql.contains("release_lock") && jdbc.failRelease) {
                                throw new SQLException("release failed", "HY000");
                            }
                            return row(sql);
                        }
                        if (method.getName().equals("getConnection")) {
                            return connection;
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        /**
         * Returns a line result for the identity query and naming lock query.
         *
         * @param sql sql
         * @return the operation result
         */
        private ResultSet row(final String sql) {
            var position = new AtomicInteger();
            return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "next" -> position.getAndIncrement() == 0;
                        case "getString" -> sql.contains("server_uuid") && ((Integer) args[0]) == 1 ? jdbc.serverId : "control";
                        case "getObject", "getInt" -> 1;
                        case "getLong" -> 1L;
                        default -> defaultValue(method.getReturnType());
                    });
        }
    }
}

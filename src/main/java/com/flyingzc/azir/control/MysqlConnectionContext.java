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

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.ProxyConnection;
import com.mysql.cj.jdbc.JdbcConnection;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * MySQL control release lock and connection context; same database metadata, TSO and safe point reuse locked connection.
 */
public final class MysqlConnectionContext {

    private static final Map<DataSource, Domain> DOMAINS = Collections.synchronizedMap(new WeakHashMap<>());

    private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();

    private static final Map<Domain, ReentrantLock> LOCAL_LOCKS = new ConcurrentHashMap<>();

    private static final Map<Domain, Set<ConnectionGuard>> QUARANTINED = new ConcurrentHashMap<>();

    /**
     * Provide static connection coordination capacity only.
     */
    private MysqlConnectionContext() {
    }

    /**
     * Get control connection; the barrier is returned to close a non-side effect borrowed view, and it is forbidden to wait for co-pool connection.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public static Connection connection(final DataSource dataSource) throws SQLException {
        requireAvailable(DOMAINS.get(dataSource));
        Scope current = CURRENT.get();
        if (current != null) {
            Domain domain = DOMAINS.get(dataSource);
            if (!current.domain.equals(domain)) {
                // The data source must be bound by initialize before entering barrier, and the same database cannot be confirmed without closing.
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "control datasource must be initialized in the same database domain before publication", false, null);
            }
            current.requireOwnership();
            return borrowed(current);
        }
        Connection connection = dataSource.getConnection();
        try {
            try (
                    PreparedStatement statement = connection.prepareStatement("select @@server_uuid, database()");
                    ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getString(1) == null || result.getString(2) == null) {
                    throw new SQLException("control connection requires a server identity and selected database");
                }
                DOMAINS.put(dataSource, new Domain(result.getString(1), result.getString(2)));
            }
            requireAvailable(DOMAINS.get(dataSource));
            return connection;
        } catch (final SQLException | RuntimeException | Error ex) {
            try {
                connection.close();
            } catch (final SQLException closeFailure) {
                ex.addSuppressed(closeFailure);
            }
            throw ex;
        }
    }

    /**
     * Use the database domain to name lockstring lines for release; re-entry is not duplicated by the same threaded field.
     *
     * @param <T> value type
     * @param dataSource database connection source
     * @param timeoutSeconds timeout seconds
     * @param action action
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public static <T> T withPublicationLock(final DataSource dataSource, final int timeoutSeconds, final Supplier<T> action) {
        if (timeoutSeconds < 0) {
            throw new IllegalArgumentException("publication lock timeout must be >= 0");
        }
        Scope current = CURRENT.get();
        if (current != null) {
            if (!current.domain.equals(DOMAINS.get(dataSource))) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "nested publication domain mismatch", false, null);
            }
            try {
                current.requireOwnership();
            } catch (final SQLException ex) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication connection lost", false, null, ex);
            }
            return action.get();
        }
        Domain domain = domain(dataSource);
        ReentrantLock localLock = LOCAL_LOCKS.computeIfAbsent(domain, ignored -> new ReentrantLock(true));
        try {
            // You must wait for the internal lock of the process and then borrow the control pool connection; there is only one connection per process waiting for the same area GET_LONK.
            if (!localLock.tryLock(timeoutSeconds, TimeUnit.SECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT, "local publication lock timed out", true, null);
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication lock wait interrupted", true, null, ex);
        }
        try (ConnectionGuard guard = new ConnectionGuard(dataSource, connection(dataSource), domain)) {
            Connection connection = guard.connection;
            if (!connection.getAutoCommit()) {
                throw new SQLException("publication connection must use autoCommit");
            }
            if (!domain.equals(DOMAINS.get(dataSource))) {
                throw new SQLException("publication datasource changed database domain");
            }
            String lockName = lockName(domain.database());
            try (Scope scope = new Scope(guard, domain, lockName)) {
                try (PreparedStatement lock = connection.prepareStatement("select get_lock(?, ?)")) {
                    lock.setString(1, lockName);
                    lock.setInt(2, timeoutSeconds);
                    // The lock may have been obtained after the request has been sent, and a chain of results closure anomalies must also enter the release path.
                    scope.mayHoldLock = true;
                    try (ResultSet result = lock.executeQuery()) {
                        if (!result.next() || result.getObject(1) == null) {
                            throw new SQLException("GET_LOCK returned NULL for publication barrier");
                        }
                        if (result.getInt(1) != 1) {
                            scope.mayHoldLock = false;
                            throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                                    "publication GET_LOCK timed out after " + timeoutSeconds + " seconds", true, null);
                        }
                    }
                }
                CURRENT.set(scope);
                T result = action.get();
                scope.requireOwnership();
                return result;
            }
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication lock failed", true, null, ex);
        } finally {
            localLock.unlock();
        }
    }

    /**
     * Allows the server to access and release naming locks in the same connection, where only one connection is allowed to wait.
     *
     * @param <T> value type
     * @param dataSource database connection source
     * @param timeoutSeconds timeout seconds
     * @param action action
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public static <T> T withPublicationProcedure(final DataSource dataSource, final int timeoutSeconds,
                                                 final SqlAction<T> action) throws SQLException {
        if (timeoutSeconds < 0) {
            throw new IllegalArgumentException("publication lock timeout must be >= 0");
        }
        Scope current = CURRENT.get();
        if (current != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "server-side publication procedure cannot nest inside another publication scope", false, null);
        }
        Domain domain = domain(dataSource);
        ReentrantLock localLock = LOCAL_LOCKS.computeIfAbsent(domain, ignored -> new ReentrantLock(true));
        try {
            if (!localLock.tryLock(timeoutSeconds, TimeUnit.SECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT, "local publication lock timed out", true, null);
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication lock wait interrupted", true, null, ex);
        }
        try (ConnectionGuard guard = new ConnectionGuard(dataSource, connection(dataSource), domain)) {
            Connection connection = guard.connection;
            try {
                if (!connection.getAutoCommit() || !domain.equals(DOMAINS.get(dataSource))) {
                    throw new SQLException("publication procedure connection changed domain or transaction mode");
                }
                T result = action.execute(connection);
                ensureProcedureReleasedLock(connection, lockName(domain.database()));
                return result;
            } catch (final SQLException | RuntimeException | Error ex) {
                // The routine may have been locked or commit decision and unknown outcome connection cannot be returned to the normal request.
                guard.discard(ex);
                throw ex;
            }
        } finally {
            localLock.unlock();
        }
    }

    /**
     * When the current thread is already same database, the caller has to choose an old version routine that does not duplicate a naming lock.
     *
     * @param dataSource database connection source
     * @return the operation result
     */
    public static boolean hasPublicationScope(final DataSource dataSource) {
        Scope current = CURRENT.get();
        return current != null && current.domain.equals(DOMAINS.get(dataSource));
    }

    /**
     * Confirms that this connection acquired name lock is not left in the pool before returning.
     *
     * @param connection connection
     * @param lockName lock name
     * @throws SQLException if the operation cannot complete
     */
    private static void ensureProcedureReleasedLock(final Connection connection, final String lockName) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select is_used_lock(?) = connection_id()")) {
            statement.setString(1, lockName);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getObject(1) != null && result.getInt(1) == 1) {
                    throw new SQLException("publication procedure retained its named lock");
                }
            }
        }
        // MySQL response package state is driver-maintenance; no additional execution here will change the detection of control cost SQL.
        JdbcConnection mysql = connection.unwrap(JdbcConnection.class);
        if (mysql.getSession().getServerSession().inTransactionOnServer()) {
            throw new SQLException("publication procedure retained a database transaction");
        }
    }

    /**
     * For the first time, use a short binding database identity without holding a pool connection waiting to release locks in the process.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private static Domain domain(final DataSource dataSource) {
        Domain domain = DOMAINS.get(dataSource);
        if (domain != null) {
            return domain;
        }
        try (Connection ignored = connection(dataSource)) {
            return DOMAINS.get(dataSource);
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "bind publication database failed", true, null, ex);
        }
    }

    /**
     * The database domain for the destruction of connection could not be confirmed to have ceased all control operations until it was repaired and rebuilt.
     *
     * @param domain domain
     * @throws SQLException if the operation cannot complete
     */
    private static void requireAvailable(final Domain domain) throws SQLException {
        if (domain != null && QUARANTINED.containsKey(domain)) {
            throw new SQLException("control database domain quarantined after connection disposal failure");
        }
    }

    /**
     * Destroy connection when the short transaction and rollback implementation on loan or exclusive connection failed instead of using an autoCommit hidden commit.
     *
     * @param <T> value type
     * @param dataSource database connection source
     * @param action action
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    public static <T> T inTransaction(final DataSource dataSource, final SqlAction<T> action) throws SQLException {
        Scope scope = CURRENT.get();
        if (scope != null) {
            return executeTransaction(connection(dataSource), scope.guard, action);
        }
        Connection connection = connection(dataSource);
        try (ConnectionGuard guard = new ConnectionGuard(dataSource, connection, DOMAINS.get(dataSource))) {
            return executeTransaction(connection, guard, action);
        }
    }

    /**
     * The original connection right of restitution is managed only by guard, and the use of connection rollback failed to do so must also pollute the outer release domain.
     *
     * @param <T> value type
     * @param connection connection
     * @param guard guard
     * @param action action
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static <T> T executeTransaction(final Connection connection, final ConnectionGuard guard, final SqlAction<T> action) throws SQLException {
        if (!connection.getAutoCommit()) {
            throw new SQLException("nested control SQL transactions are not supported");
        }
        try {
            connection.setAutoCommit(false);
            T result = action.execute(connection);
            connection.commit();
            connection.setAutoCommit(true);
            return result;
        } catch (final SQLException | RuntimeException | Error ex) {
            try {
                connection.rollback();
                connection.setAutoCommit(true);
            } catch (final SQLException | RuntimeException | Error rollbackFailure) {
                if (rollbackFailure != ex) {
                    ex.addSuppressed(rollbackFailure);
                }
                guard.discard(ex);
            }
            throw ex;
        }
    }

    /**
     * Construct a lockname not exceeding the MySQL 64 character limit by database.
     *
     * @param database database
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private static String lockName(final String database) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(database.getBytes(StandardCharsets.UTF_8));
            return "mvcc:pub:" + HexFormat.of().formatHex(digest, 0, 24);
        } catch (final NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    /**
     * Returns to borrow connection; close does not return the pool, unwrap does not expose locked connection.
     *
     * @param scope scope
     * @return the operation result
     */
    private static Connection borrowed(final Scope scope) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    if (CURRENT.get() != scope || scope.broken || scope.guard.discarded) {
                        throw new SQLException("publication connection view is no longer usable");
                    }
                    requireAvailable(scope.domain);
                    if (method.getName().equals("unwrap")) {
                        if (((Class<?>) args[0]).isInstance(proxy)) {
                            return proxy;
                        }
                        throw new SQLException("cannot unwrap a publication connection");
                    }
                    if (method.getName().equals("isWrapperFor")) {
                        return ((Class<?>) args[0]).isInstance(proxy);
                    }
                    try {
                        return method.invoke(scope.connection, args);
                    } catch (final InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /**
     * Use the server UUID and actual database to bind the field so that different JDBC addresses have their aliases locked in the process.
     *
     * @param serverId server id
     * @param database database
     */
    private record Domain(String serverId, String database) {
    }

    /**
     * The JDBC transaction function, the anomaly is handled by a unified cleanup path.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    public interface SqlAction<T> {

        /**
         * Use the current control connection to complete a paragraph database transaction.
         *
         * @param connection connection
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        T execute(Connection connection) throws SQLException;
    }

    /**
     * Manages the restitution rights of the original pool connection, which will never be followed by a normal close back-to-pool path when discarded.
     */
    private static final class ConnectionGuard implements AutoCloseable {

        private final DataSource dataSource;

        private final Connection connection;

        private final Domain domain;

        private boolean discarded;

        /**
         * Saves the actual loan from the connection pool and cannot pass the borrowed view to it at the time of phase-out.
         *
         * @param dataSource database connection source
         * @param connection connection
         * @param domain domain
         */
        private ConnectionGuard(final DataSource dataSource, final Connection connection, final Domain domain) {
            this.dataSource = dataSource;
            this.connection = connection;
            this.domain = domain;
        }

        /**
         * Abort session first, then visibly phase out Hikari proxy; keep connection and separate if the common pool cannot be phased out.
         *
         * @param failure failure
         */
        private void discard(final Throwable failure) {
            if (discarded) {
                return;
            }
            discarded = true;
            try {
                connection.abort(Runnable::run);
            } catch (final SQLException | RuntimeException | Error abortFailure) {
                if (abortFailure != failure) {
                    failure.addSuppressed(abortFailure);
                }
            }
            try {
                // EvictConnect does not deal with non-Hikari agents in silence and must check the pool and original agent type.
                if (dataSource instanceof HikariDataSource hikari && connection instanceof ProxyConnection
                        && connection.getClass().getName().startsWith("com.zaxxer.hikari.")
                        && !hikari.isClosed() && hikari.getHikariPoolMXBean() != null) {
                    hikari.evictConnection(connection);
                    return;
                }
            } catch (final RuntimeException | Error evictionFailure) {
                if (evictionFailure != failure) {
                    failure.addSuppressed(evictionFailure);
                }
            }
            // Use the original connection to prevent ordinary return paths from being handed over to other requests that remain locked or transaction has not been terminated.
            QUARANTINED.computeIfAbsent(domain, ignored -> ConcurrentHashMap.newKeySet()).add(this);
        }

        /**
         * Only normal connection can be returned to the pool if it is confirmed that it has not been discarded.
         *
         * @throws SQLException if the operation cannot complete
         */
        @Override
        public void close() throws SQLException {
            if (!discarded) {
                connection.close();
            }
        }
    }

    /**
     * Barrier clears the context of the thread at exit and destroys connection when release fails in order to avoid locking with connection back into the pool.
     */
    private static final class Scope implements AutoCloseable {

        private final ConnectionGuard guard;

        private final Connection connection;

        private final Domain domain;

        private final String lockName;

        private boolean mayHoldLock;

        private boolean broken;

        /**
         * Saves locked connection and its only database domain.
         *
         * @param guard guard
         * @param domain domain
         * @param lockName lock name
         */
        private Scope(final ConnectionGuard guard, final Domain domain, final String lockName) {
            this.guard = guard;
            this.connection = guard.connection;
            this.domain = domain;
            this.lockName = lockName;
        }

        /**
         * Validate session still holds a name lock every time it is borrowed; this barrier never changes connection to continue after it has expired.
         *
         * @throws SQLException if the operation cannot complete
         */
        private void requireOwnership() throws SQLException {
            requireAvailable(domain);
            if (broken || guard.discarded || connection.isClosed()) {
                broken = true;
                throw new SQLException("publication connection is no longer usable");
            }
            try (PreparedStatement check = connection.prepareStatement("select is_used_lock(?) = connection_id()")) {
                check.setString(1, lockName);
                try (ResultSet result = check.executeQuery()) {
                    if (!result.next() || result.getObject(1) == null || result.getInt(1) != 1) {
                        throw new SQLException("publication connection no longer owns GET_LOCK");
                    }
                }
            } catch (final SQLException ex) {
                broken = true;
                throw ex;
            }
        }

        /**
         * Unusual paths also release naming locks, and maintain an abnormal chain of failed calls and failed releases.
         *
         * @throws SQLException if the operation cannot complete
         */
        @Override
        public void close() throws SQLException {
            CURRENT.remove();
            if (!mayHoldLock || guard.discarded) {
                return;
            }
            try (PreparedStatement release = connection.prepareStatement("select release_lock(?)")) {
                release.setString(1, lockName);
                try (ResultSet result = release.executeQuery()) {
                    if (!result.next() || result.getObject(1) == null || result.getInt(1) != 1) {
                        throw new SQLException("RELEASE_LOCK did not confirm publication lock release");
                    }
                }
            } catch (final SQLException | RuntimeException | Error ex) {
                guard.discard(ex);
                throw ex;
            }
        }
    }
}

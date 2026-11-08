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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Objects;

/**
 * The session border of the protocol named lock; destruction of session when locking or unlocking is uncertain cannot return to connection pool.
 */
public final class MysqlNamedLock {

    /**
     * Bans the Case Naming Locker tool.
     */
    private MysqlNamedLock() {
    }

    /**
     * Creates a lock that can be used for try-with-resources, and database locks cannot be left without the distribution of the locked resource.
     *
     * @param connection connection
     * @param name name
     * @param waitSeconds wait seconds
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    public static Lease lock(final Connection connection, final String name, final int waitSeconds) throws SQLException {
        try {
            acquire(connection, name, waitSeconds);
            return new Lease(connection, name);
        } catch (final SQLException | RuntimeException | Error failure) {
            discard(connection, failure);
            throw failure;
        }
    }

    /**
     * Limited waiting is allowed, and any failure may be deemed to have been locked by the service and no speculation as to whether or not a network request has arrived.
     *
     * @param connection connection
     * @param name name
     * @param waitSeconds wait seconds
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     * @throws SQLTimeoutException if the operation cannot complete
     */
    public static void acquire(final Connection connection, final String name, final int waitSeconds) throws SQLException {
        requireName(name);
        if (waitSeconds < 0 || waitSeconds > 3600) {
            throw new IllegalArgumentException("invalid named lock wait budget");
        }
        try (var q = connection.prepareStatement("select get_lock(?,?)")) {
            if (!connection.getAutoCommit()) {
                throw new SQLException("protocol authority lock requires auto-commit");
            }
            q.setString(1, name);
            q.setInt(2, waitSeconds);
            try (var r = q.executeQuery()) {
                if (!r.next()) {
                    throw new SQLException("protocol named lock result missing");
                }
                int result = r.getInt(1);
                if (r.wasNull()) {
                    throw new SQLException("protocol named lock acquisition failed");
                }
                if (result != 1) {
                    throw new SQLTimeoutException("protocol named lock wait exhausted", "HYT00");
                }
            }
        } catch (final SQLException | RuntimeException | Error failure) {
            discard(connection, failure);
            throw failure;
        }
    }

    /**
     * Only this session was accepted for successful release, empty results and non-owners must destroy the session and cannot continue to reuse the remaining locks.
     *
     * @param connection connection
     * @param name name
     * @throws SQLException if the operation cannot complete
     */
    public static void release(final Connection connection, final String name) throws SQLException {
        requireName(name);
        try (var q = connection.prepareStatement("select release_lock(?)")) {
            q.setString(1, name);
            try (var r = q.executeQuery()) {
                if (!r.next() || r.getInt(1) != 1 || r.wasNull()) {
                    throw new SQLException("protocol named lock ownership lost");
                }
            }
        } catch (final SQLException | RuntimeException | Error failure) {
            discard(connection, failure);
            throw failure;
        }
    }

    /**
     * Destroys the original session and maintains a cleanup error. connection closes with the holder 's Finally or resource field.
     *
     * @param connection connection
     * @param original original
     */
    public static void discard(final Connection connection, final Throwable original) {
        try {
            connection.abort(Runnable::run);
        } catch (final SQLException | RuntimeException | Error cleanup) {
            if (cleanup != original) {
                original.addSuppressed(cleanup);
            }
        }
    }

    /**
     * The protocol lockname is constructed as a fixed prefix and UUID. Invalid length is denied without output of specific identity in the anomaly.
     *
     * @param name name
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireName(final String name) {
        Objects.requireNonNull(name);
        if (name.isEmpty() || name.length() > 64) {
            throw new IllegalArgumentException("invalid named lock name");
        }
    }

    /**
     * The resource syntax automatically adds the release failure to the original anomaly of a locked resource in the same performance domain.
     */
    public static final class Lease implements AutoCloseable {

        private final Connection connection;

        private final String name;

        private boolean closed;

        /**
         * The database lock factory is the only plant that has been successfully established and cannot forge a locked resource.
         *
         * @param connection connection
         * @param name name
         */
        private Lease(final Connection connection, final String name) {
            this.connection = connection;
            this.name = name;
        }

        /**
         * Repeated closing of the same field has no side effects and failed release has been destroyed from bottom session.
         *
         * @throws SQLException if the operation cannot complete
         */
        @Override
        public void close() throws SQLException {
            if (!closed) {
                closed = true;
                release(connection, name);
            }
        }
    }
}

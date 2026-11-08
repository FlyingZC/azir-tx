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
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;
import java.io.PrintWriter;

import java.util.Objects;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Check durable authority with local durable storage conditions each time you borrow connection; the wrong route or downgrade configuration must stop service.
 */
public final class ProtocolMysqlDataSource implements DataSource {

    private final DataSource delegate;

    private final MysqlDatabaseAuthority.Identity expected;

    private final MysqlGroupDataSource group;

    /**
     * Ties the current physical database; does not own the life cycle of the original connection pool.
     *
     * @param delegate delegate
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public ProtocolMysqlDataSource(final DataSource delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        group = delegate instanceof MysqlGroupDataSource cluster ? cluster : delegate instanceof ProtocolMysqlDataSource protocol ? protocol.groupAuthority() : null;
        MysqlDatabaseAuthority.Identity physical;
        try (var connection = delegate.getConnection()) {
            MysqlSessionRequirements.require(connection);
            physical = MysqlDatabaseAuthority.inspectConnection(connection);
        } catch (final SQLException failure) {
            throw new IllegalArgumentException("unsafe MySQL protocol driver configuration", failure);
        }
        expected = group == null ? physical : new MysqlDatabaseAuthority.Identity("group:" + group.group(), physical.database());
        try (var ignored = getConnection()) {
        } catch (final SQLException failure) {
            throw new IllegalArgumentException("unsafe MySQL protocol authority", failure);
        }
    }

    /**
     * Each actual session loan must be original authority and synchronized with local do durable.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public Connection getConnection() throws SQLException {
        return validate(delegate.getConnection());
    }

    /**
     * Visible receipt connection cannot skip validate.
     *
     * @param username username
     * @param password password
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return validate(delegate.getConnection(username, password));
    }

    /**
     * Unable to follow UUID transparently with another server, otherwise GET_LONK may have two different authority s.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Connection validate(final Connection connection) throws SQLException {
        try {
            verifySession(connection);
            return connection;
        } catch (final SQLException | RuntimeException | Error failure) {
            MysqlNamedLock.discard(connection, failure);
            try {
                connection.close();
            } catch (final SQLException close) {
                failure.addSuppressed(close);
            }
            throw failure;
        }
    }

    /**
     * The long-term locked management session must review the original connection and group commit barrier and cannot be replaced by another new connection.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    public void verifySession(final Connection connection) throws SQLException {
        MysqlSessionRequirements.require(connection);
        if (!connection.getAutoCommit()) {
            throw new SQLException("protocol pools require auto-commit");
        }
        if (group != null) {
            group.verify(connection);
        }
        try (
                var q = connection.createStatement(); var r = q.executeQuery(
                        "select @@server_uuid,database(),@@global.innodb_flush_log_at_trx_commit,@@global.read_only,@@global.super_read_only,@@session.sql_mode,@@session.innodb_strict_mode,@@session.transaction_read_only")) {
            if (!r.next() || (group == null && !expected.serverUuid().equals(r.getString(1))) || !expected.database().equals(r.getString(2))
                    || r.getInt(3) != 1 || r.getBoolean(4) || r.getBoolean(5)) {
                throw new SQLException("MySQL authority changed, is read-only, or synchronous redo durability is disabled");
            }
            // Easier mode may report success after the break of an ultra-long key, payload or spill timestamp and cannot be used in the transaction protocol table.
            String sqlMode = r.getString(6);
            boolean strict = sqlMode != null && Arrays.stream(sqlMode.split(",")).map(String::trim)
                    .anyMatch(mode -> mode.equalsIgnoreCase("STRICT_TRANS_TABLES") || mode.equalsIgnoreCase("STRICT_ALL_TABLES"));
            if (!strict || !r.getBoolean(7) || r.getBoolean(8)) {
                throw new SQLException("protocol sessions require strict SQL and writable transactions");
            }
        }
    }

    /**
     * Returns a group detector by verify; normal single machine returns empty.
     *
     * @return the operation result
     */
    public MysqlGroupDataSource groupAuthority() {
        return group;
    }

    /**
     * Returns authority stabilization through verify; normal single machine still binds the physical UUID, group replication binding group UUID.
     *
     * @return the operation result
     */
    public MysqlDatabaseAuthority.Identity authorityIdentity() {
        return expected;
    }

    /**
     * Transfer log output configuration, no receipt exposure.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    /**
     * Transfers the log output configuration.
     *
     * @param writer writer
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public void setLogWriter(final PrintWriter writer) throws SQLException {
        delegate.setLogWriter(writer);
    }

    /**
     * Forward connection to create timeout settings.
     *
     * @param seconds seconds
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    /**
     * Returns connection from the original data source to create timeout.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    /**
     * Returns the data source log name.
     *
     * @return the operation result
     * @throws SQLFeatureNotSupportedException if the operation cannot complete
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    /**
     * Supports the acquisition of packaging or original data source interface.
     *
     * @param <T> value type
     * @param type type
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public <T> T unwrap(final Class<T> type) throws SQLException {
        return type.isInstance(this) ? type.cast(this) : delegate.unwrap(type);
    }

    /**
     * Supports standard JDBC packagingr detection.
     *
     * @param type type
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public boolean isWrapperFor(final Class<?> type) throws SQLException {
        return type.isInstance(this) || delegate.isWrapperFor(type);
    }
}

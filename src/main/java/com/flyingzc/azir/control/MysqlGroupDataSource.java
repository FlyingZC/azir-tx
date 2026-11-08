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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import java.io.PrintWriter;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Strictly single-master group replication found entry; only ONLINE PRIMIRY of the configuration member is selected, and it is not downgraded to a different step replica.
 */
public final class MysqlGroupDataSource implements DataSource {

    private final String group;

    private final List<DataSource> members;

    private final Set<String> identities;

    private final AtomicInteger preferred = new AtomicInteger();

    private final java.util.concurrent.atomic.AtomicBoolean quorumTableInitialized = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * At least three independent members are bound; connection pool and node life cycles remain applied-owned.
     *
     * @param group group
     * @param members members
     */
    public MysqlGroupDataSource(final String group, final List<? extends DataSource> members) {
        this(group, discover(members));
    }

    /**
     * Visible configuration of a member UUID allows the restarting of application without lowering the required majority when a member stops.
     *
     * @param group group
     * @param members members
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MysqlGroupDataSource(final String group, final Map<String, ? extends DataSource> members) {
        UUID.fromString(group);
        this.group = group;
        if (members.size() < 3 || members.size() > 9) {
            throw new IllegalArgumentException("group requires 3..9 configured members");
        }
        members.keySet().forEach(UUID::fromString);
        this.members = List.copyOf(new TreeMap<>(members).values());
        identities = Set.copyOf(members.keySet());
        try (var ignored = getConnection()) {
        } catch (final SQLException failure) {
            throw new IllegalArgumentException("group has no verified primary", failure);
        }
    }

    /**
     * For the first discovery that is accessible to all nodes only, the production can be restarted using a clearly saved UUID configuration.
     *
     * @param members members
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static Map<String, DataSource> discover(final List<? extends DataSource> members) {
        var found = new LinkedHashMap<String, DataSource>();
        for (var source : members) {
            try (var c = source.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select @@server_uuid")) {
                r.next();
                if (found.put(r.getString(1), source) != null) {
                    throw new IllegalArgumentException("duplicate MySQL group member");
                }
            } catch (final SQLException failure) {
                throw new IllegalArgumentException("cannot initially identify MySQL group members", failure);
            }
        }
        return found;
    }

    /**
     * Checks the real group view every time you borrow it, and the cache's preferred index is not a substitute for the service-end role.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public Connection getConnection() throws SQLException {
        SQLException failure = new SQLException("no quorum-backed ONLINE PRIMARY");
        int first = preferred.get();
        for (int attempt = 0; attempt < members.size(); attempt++) {
            int index = (first + attempt) % members.size();
            Connection connection = null;
            try {
                connection = members.get(index).getConnection();
                verify(connection);
                preferred.set(index);
                return connection;
            } catch (final SQLException problem) {
                failure.addSuppressed(problem);
                if (connection != null) {
                    MysqlNamedLock.discard(connection, problem);
                    try {
                        connection.close();
                    } catch (final SQLException close) {
                        failure.addSuppressed(close);
                    }
                }
            } catch (final RuntimeException | Error fatal) {
                if (connection != null) {
                    MysqlNamedLock.discard(connection, fatal);
                    try {
                        connection.close();
                    } catch (final SQLException close) {
                        fatal.addSuppressed(close);
                    }
                }
                throw fatal;
            }
        }
        throw failure;
    }

    /**
     * A visible user receipt cannot bypass the configuration of a fixed member pool.
     *
     * @param username username
     * @param password password
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     * @throws SQLFeatureNotSupportedException if the operation cannot complete
     */
    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw new SQLFeatureNotSupportedException("configure credentials on member pools");
    }

    /**
     * Validate Main node, required majority of known members, Synchronization Log durable storage and single master mode.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    public void verify(final Connection connection) throws SQLException {
        MysqlSessionRequirements.require(connection);
        if (!connection.getAutoCommit()) {
            throw new SQLException("group member pools require auto-commit");
        }
        try (
                var q = connection.createStatement(); var r = q.executeQuery(
                        "select @@global.group_replication_group_name,@@global.group_replication_single_primary_mode,@@global.sync_binlog,@@global.innodb_flush_log_at_trx_commit,@@global.read_only,@@global.super_read_only")) {
            if (!r.next() || !group.equals(r.getString(1)) || !r.getBoolean(2) || r.getLong(3) != 1 || r.getLong(4) != 1 || r.getBoolean(5) || r.getBoolean(6)) {
                throw new SQLException("unsafe group role or durability configuration");
            }
        }
        Set<String> online = new HashSet<>();
        String local = null;
        boolean primary = false;
        try (var q = connection.createStatement(); var r = q.executeQuery("select member_id,member_state,member_role,member_id=@@server_uuid from performance_schema.replication_group_members")) {
            while (r.next()) {
                String id = r.getString(1);
                if (!identities.contains(id)) {
                    throw new SQLException("unconfigured group membership change; rebuild the member roster");
                }
                if (r.getString(2).equals("ONLINE")) {
                    online.add(id);
                }
                if (r.getBoolean(4)) {
                    local = id;
                    primary = r.getString(2).equals("ONLINE") && r.getString(3).equals("PRIMARY");
                }
            }
        }
        if (!primary || local == null || online.size() < members.size() / 2 + 1) {
            throw new SQLException("group member is not a quorum-backed primary");
        }
        try (var q = connection.createStatement()) {
            q.execute("set session group_replication_consistency='BEFORE_AND_AFTER'");
            if (!quorumTableInitialized.get()) {
                q.execute("create table if not exists azir_group_quorum(id int primary key,pulse bigint not null) engine=InnoDB");
                q.executeUpdate("insert ignore into azir_group_quorum values(1,0)");
                quorumTableInitialized.set(true);
            }
            // A member view may not yet reflect a network partition; the current connection still has the required majority in a sync group commit.
            if (q.executeUpdate("update azir_group_quorum set pulse=pulse+1 where id=1 and pulse<9223372036854775807") != 1) {
                throw new SQLException("group quorum pulse unavailable");
            }
        }
    }

    /**
     * Return to stability group identity, member leader failover will not reset frame namespace or timestamp.
     *
     * @return the operation result
     */
    public String group() {
        return group;
    }

    /**
     * Transfers the log setup.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return members.getFirst().getLogWriter();
    }

    /**
     * Sets the same log for all members.
     *
     * @param writer writer
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public void setLogWriter(final PrintWriter writer) throws SQLException {
        for (var source : members) {
            source.setLogWriter(writer);
        }
    }

    /**
     * Set a limited connection for all members to create timeout.
     *
     * @param seconds seconds
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        for (var source : members) {
            source.setLoginTimeout(seconds);
        }
    }

    /**
     * Returns connection to member pool to create timeout.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return members.getFirst().getLoginTimeout();
    }

    /**
     * Returns the Standard Data Source Log.
     *
     * @return the operation result
     */
    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(MysqlGroupDataSource.class.getName());
    }

    /**
     * Only unpack the current group detector is allowed, and member validate cannot be bypassed.
     *
     * @param <T> value type
     * @param type type
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public <T> T unwrap(final Class<T> type) throws SQLException {
        if (type.isInstance(this)) {
            return type.cast(this);
        }
        throw new SQLException("not a group datasource wrapper");
    }

    /**
     * Interface supported by the query group finder.
     *
     * @param type type
     * @return the operation result
     */
    @Override
    public boolean isWrapperFor(final Class<?> type) {
        return type.isInstance(this);
    }
}

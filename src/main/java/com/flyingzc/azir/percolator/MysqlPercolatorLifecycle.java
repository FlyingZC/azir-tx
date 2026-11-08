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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.maintenance.MaintenanceResult;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;

import java.util.function.Supplier;

/**
 * Protects Percolator snapshots with persistent leases and coordinates bounded recovery and safe version collection.
 */
public final class MysqlPercolatorLifecycle implements AutoCloseable {

    private final DataSource control;

    private final Map<String, DataSource> shards;

    private final MysqlTimestampOracle tso;

    private final ThreadLocal<Connection> bound = new ThreadLocal<>();

    private final String namespace;

    private final long leaseMillis;

    private final com.flyingzc.azir.operations.MysqlProtocolState operations;

    private volatile boolean closed;

    /**
     * Initialize durable binding; guess recovery is rejected when no protocol data are bound or TSO is missing.
     *
     * @param control control
     * @param sources sources
     * @param leaseMillis lease millis
     */
    public MysqlPercolatorLifecycle(final DataSource control, final Map<String, DataSource> sources, final long leaseMillis) {
        this(control, sources, leaseMillis, false);
    }

    /**
     * Only for visible offline upgrades cannot automatically take over the enclosed protocol space.
     *
     * @param control control
     * @param sources sources
     * @param leaseMillis lease millis
     * @return the operation result
     */
    public static MysqlPercolatorLifecycle adoptLegacyOffline(final DataSource control, final Map<String, DataSource> sources, final long leaseMillis) {
        return new MysqlPercolatorLifecycle(control, sources, leaseMillis, true);
    }

    /**
     * Integrated assembly and a clear distinction between the initialization of new spaces and the taking over of older offline data.
     *
     * @param control control
     * @param sources sources
     * @param leaseMillis lease millis
     * @param adopt adopt
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private MysqlPercolatorLifecycle(final DataSource control, final Map<String, DataSource> sources, final long leaseMillis, final boolean adopt) {
        this.control = Objects.requireNonNull(control, "control");
        this.shards = Map.copyOf(sources);
        if (shards.isEmpty() || leaseMillis < 1 || leaseMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid lifecycle configuration");
        }
        this.leaseMillis = leaseMillis;
        var routes = new TreeMap<String, String>();
        routes.put("control", MysqlDatabaseAuthority.inspect(control).toString());
        var unique = new HashSet<MysqlDatabaseAuthority.Identity>();
        var physical = new HashSet<MysqlDatabaseAuthority.Identity>();
        shards.forEach((shard, source) -> {
            new PercolatorKey(shard, "");
            var identity = MysqlDatabaseAuthority.inspect(source);
            if (!unique.add(identity)) {
                throw new IllegalArgumentException("Percolator shards alias the same database");
            }
            try (var connection = source.getConnection()) {
                // Group data sources are different from straight-link sources for stability identity, but the same physical database cannot be registered as two operations shard.
                if (!physical.add(MysqlDatabaseAuthority.inspectConnection(connection))) {
                    throw new IllegalArgumentException("Percolator shards currently alias the same physical database");
                }
            } catch (final SQLException failure) {
                throw storage(failure);
            }
            routes.put("shard:" + shard, identity.toString());
        });
        tso = new MysqlTimestampOracle(control);
        // Initialization locks are not related to existing UUIDs, avoiding the creation of two initial starters that do not match identity.
        namespace = initialize(routes.toString(), adopt);
        operations = new com.flyingzc.azir.operations.MysqlProtocolState("pc", namespace, control, shards);
    }

    /**
     * Returns durable space identity, which must be saved with the Primary receipt.
     *
     * @return the operation result
     */
    public String namespace() {
        return namespace;
    }

    /**
     * Returns the production to write fence, and commit authority is still saved by Primary.
     *
     * @return the operation result
     */
    public com.flyingzc.azir.operations.MysqlProtocolState operations() {
        return operations;
    }

    /**
     * Distribute and register the start snapshot within the lease and safe point door restrictions.
     *
     * @return the operation result
     */
    public long begin() {
        return gate(() -> {
            long start = tso.nextTimestamp();
            sql(c -> {
                try (var q = c.prepareStatement("insert into pc_readers(start_ts,expires_at) values(?,unix_timestamp(current_timestamp(3))*1000+?)")) {
                    q.setLong(1, start);
                    q.setLong(2, leaseMillis);
                    q.executeUpdate();
                }
                return null;
            });
            return start;
        });
    }

    /**
     * Reject snapshot expired, closed or crossed safe point, preventing GC staggering throughout the operation.
     *
     * @param <T> value type
     * @param start start
     * @param read read
     * @param work work
     * @return the operation result
     */
    public <T> T live(final long start, final long read, final Supplier<T> work) {
        return live(start, read, work, () -> false);
    }

    /**
     * The internal commit phase has confirmed that the Primary terminal state will be retained, and effective reading protection must remain in place before the rest of the operation
     * returns.
     *
     * @param <T> value type
     * @param start start
     * @param read read
     * @param work work
     * @param terminal terminal
     * @return the operation result
     */
    <T> T live(final long start, final long read, final Supplier<T> work, final java.util.function.BooleanSupplier terminal) {
        return gate(() -> {
            requireSnapshot(start, read);
            T result = work.get();
            try {
                verifyGate();
                if (!terminal.getAsBoolean()) {
                    requireSnapshot(start, read);
                }
            } catch (final RuntimeException failure) {
                if (!terminal.getAsBoolean()) {
                    throw failure;
                }
            }
            return result;
        });
    }

    /**
     * Read lease and safe point in the same control statement. snapshot already new authority collection cannot be accepted.
     *
     * @param start start
     * @param read read
     * @throws MvccException if the operation cannot complete
     */
    private void requireSnapshot(final long start, final long read) {
        boolean valid = sql(c -> {
            try (var q = c.prepareStatement("select r.expires_at>unix_timestamp(current_timestamp(3))*1000 and ? >= g.safe_point from pc_readers r join pc_control g on g.id=1 where r.start_ts=?")) {
                q.setLong(1, read);
                q.setLong(2, start);
                try (var r = q.executeQuery()) {
                    return r.next() && r.getBoolean(1);
                }
            }
        });
        if (!valid) {
            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD, "Percolator snapshot lease expired or fenced", false, start);
        }
    }

    /**
     * Release the local session's read protection without changing the original primary decision.
     *
     * @param start start
     */
    public void release(final long start) {
        gate(() -> sql(c -> {
            try (var q = c.prepareStatement("delete from pc_readers where start_ts=?")) {
                q.setLong(1, start);
                q.executeUpdate();
            }
            return null;
        }));
    }

    /**
     * Query active snapshot and oldest reader from the control library, with residual locks supplemented by each shard.
     *
     * @param pendingLocks pending locks
     * @return the operation result
     */
    public com.flyingzc.azir.maintenance.MaintenanceDiagnostics diagnostics(final long pendingLocks) {
        return gate(() -> sql(c -> {
            long floor;
            try (var q = c.createStatement(); var r = q.executeQuery("select safe_point from pc_control where id=1")) {
                if (!r.next()) {
                    throw new SQLException("Percolator durable control missing");
                }
                floor = r.getLong(1);
            }
            try (
                    var q = c.createStatement(); var r = q.executeQuery(
                            "select coalesce(min(case when expires_at>unix_timestamp(current_timestamp(3))*1000 then start_ts end),0),coalesce(sum(expires_at>unix_timestamp(current_timestamp(3))*1000),0),coalesce(sum(expires_at<=unix_timestamp(current_timestamp(3))*1000),0) from pc_readers")) {
                r.next();
                return new com.flyingzc.azir.maintenance.MaintenanceDiagnostics(floor, r.getLong(1), r.getLong(2), r.getLong(3), pendingLocks);
            }
        }));
    }

    /**
     * Bounded Cleanup failed lease, press all active reader and incomplete locks to advance durable safe point.
     *
     * @param stores stores
     * @param limit maximum number of items
     * @param recovered recovered
     * @return the operation result
     */
    public MaintenanceResult collect(final Map<String, PercolatorStore> stores, final int limit, final int recovered) {
        return gate(() -> {
            long candidate = tso.nextTimestamp() - 1;
            Long reader = sql(c -> {
                try (var q = c.prepareStatement("delete from pc_readers where expires_at<=unix_timestamp(current_timestamp(3))*1000 order by start_ts limit ?")) {
                    q.setInt(1, limit);
                    q.executeUpdate();
                }
                try (var q = c.createStatement(); var r = q.executeQuery("select min(start_ts) from pc_readers where expires_at>unix_timestamp(current_timestamp(3))*1000")) {
                    r.next();
                    long value = r.getLong(1);
                    return r.wasNull() ? null : value;
                }
            });
            if (reader != null) {
                candidate = Math.min(candidate, reader - 1);
            }
            for (var store : stores.values()) {
                for (var lock : store.pending(1)) {
                    candidate = Math.min(candidate, lock.startTs() - 1);
                }
            }
            long proposed = candidate;
            long floor = sql(c -> {
                try (var q = c.prepareStatement("update pc_control set safe_point=greatest(safe_point,?) where id=1")) {
                    q.setLong(1, proposed);
                    if (q.executeUpdate() != 1) {
                        throw new SQLException("Percolator control state missing");
                    }
                }
                try (var q = c.createStatement(); var r = q.executeQuery("select safe_point from pc_control where id=1")) {
                    r.next();
                    return r.getLong(1);
                }
            });
            int deleted = 0;
            for (var store : stores.values()) {
                deleted += store.collect(floor, limit);
            }
            new com.flyingzc.azir.operations.MysqlProtocolAdmin(operations, ignored -> 0).retireLocked(bound.get(), floor, limit);
            return new MaintenanceResult(recovered, deleted, floor);
        });
    }

    /**
     * The same namespace cross-process to share the door disabled; cannot automatically borrow connection after losing the original session.
     *
     * @param <T> value type
     * @param work work
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    public <T> T gate(final Supplier<T> work) {
        if (closed) {
            throw new IllegalStateException("Percolator runtime closed");
        }
        if (bound.get() != null) {
            verifyGate();
            return work.get();
        }
        return com.flyingzc.azir.operations.MysqlProtocolState.admitted("azir:pc:" + namespace, 10, () -> {
            try (var c = control.getConnection()) {
                requireAutoCommit(c);
                String name = "azir:pc:" + namespace;
                try (var authority = com.flyingzc.azir.control.MysqlNamedLock.lock(c, name, 10)) {
                    bound.set(c);
                    try (var owner = operations.enterControl(c)) {
                        T result = work.get();
                        verifyGate();
                        return result;
                    } finally {
                        bound.remove();
                    }
                }
            } catch (final SQLException failure) {
                throw storage(failure);
            }
        });
    }

    /**
     * Each nesting operation confirms that the naming lock is original database session.
     */
    private void verifyGate() {
        sql(c -> {
            try (var q = c.prepareStatement("select is_used_lock(?)=connection_id()")) {
                q.setString(1, "azir:pc:" + namespace);
                try (var r = q.executeQuery()) {
                    if (!r.next() || !r.getBoolean(1)) {
                        throw new SQLException("Percolator lifecycle gate lost");
                    }
                }
            }
            return null;
        });
    }

    /**
     * Controls SQL and run generation lines are locked in the same short transaction commit and cannot be staggered with late reader cleanup.
     *
     * @param <T> value type
     * @param work work
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private <T> T sql(final SqlWork<T> work) {
        Connection c = bound.get();
        if (c == null) {
            throw new IllegalStateException("Percolator lifecycle gate required");
        }
        try {
            if (!c.getAutoCommit()) {
                try {
                    return work.run(c);
                } catch (final SQLException | RuntimeException | Error failure) {
                    operations.invalidateControl();
                    throw failure;
                }
            }
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);
            boolean ended = false;
            Throwable original = null;
            try {
                operations.lockControl(c);
                T value = work.run(c);
                operations.verifyOwner(c);
                c.commit();
                ended = true;
                return value;
            } catch (final SQLException | RuntimeException | Error failure) {
                original = failure;
                operations.invalidateControl();
                try {
                    c.rollback();
                    ended = true;
                } catch (final SQLException | RuntimeException | Error rollback) {
                    if (failure != rollback) {
                        failure.addSuppressed(rollback);
                    }
                    com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure);
                }
                throw failure;
            } finally {
                if (ended) {
                    try {
                        c.setAutoCommit(true);
                    } catch (final SQLException | RuntimeException | Error reset) {
                        operations.invalidateControl();
                        com.flyingzc.azir.control.MysqlNamedLock.discard(c, reset);
                        if (original == null) {
                            throw reset;
                        }
                        if (original != reset) {
                            original.addSuppressed(reset);
                        }
                    }
                }
            }
        } catch (final SQLException failure) {
            operations.invalidateControl();
            com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure);
            throw storage(failure);
        } catch (final RuntimeException | Error failure) {
            operations.invalidateControl();
            com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure);
            throw failure;
        }
    }

    /**
     * The first assembly of the serials requires a visible migration of old data, which cannot be extrapolated online.
     *
     * @param routes routes
     * @param adopt adopt
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private String initialize(final String routes, final boolean adopt) {
        String initName = "azir:pc:init:" + UUID.nameUUIDFromBytes(routes.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try (var c = control.getConnection()) {
            requireAutoCommit(c);
            try (var initialization = com.flyingzc.azir.control.MysqlNamedLock.lock(c, initName, 10)) {
                // Recovery log does not participate in data replacement; no regular startup or TSO can be configured when import is not completed.
                com.flyingzc.azir.operations.MysqlProtocolRestore.requireNoImport(control, "pc");
                try (var q = c.createStatement()) {
                    q.execute("create table if not exists pc_control(id int primary key,namespace varchar(36) not null,routes text not null,safe_point bigint not null) engine=InnoDB");
                    q.execute("create table if not exists pc_readers(start_ts bigint primary key,expires_at bigint not null,index expiry(expires_at)) engine=InnoDB");
                }
                String identity = null;
                try (var q = c.createStatement(); var r = q.executeQuery("select namespace,routes from pc_control where id=1")) {
                    if (r.next()) {
                        identity = r.getString(1);
                        if (!routes.equals(r.getString(2))) {
                            throw new IllegalArgumentException("Percolator persistent routes or control authority changed");
                        }
                    }
                }
                if (identity != null) {
                    if (adopt) {
                        throw new IllegalArgumentException("Percolator is already bound; refuse legacy adoption");
                    }
                    try (var q = c.prepareStatement("select current_value from mvcc_tso where sequence_name='global'"); var r = q.executeQuery()) {
                        if (!r.next()) {
                            throw new IllegalArgumentException("Percolator TSO missing; refuse reset");
                        }
                    }
                } else {
                    if (adopt) {
                        com.flyingzc.azir.operations.MysqlProtocolUpgrade.validateLegacy(control, shards);
                    } else {
                        for (var source : shards.values()) {
                            requireEmptyUnbound(source);
                        }
                        tso.initialize(100);
                    }
                    identity = UUID.randomUUID().toString();
                    try (var q = c.prepareStatement("insert into pc_control values(1,?,?,0)")) {
                        q.setString(1, identity);
                        q.setString(2, routes);
                        q.executeUpdate();
                    }
                }
                for (var entry : shards.entrySet()) {
                    bind(entry.getValue(), entry.getKey(), identity);
                }
                return identity;
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Controlled X-0s can only bind space to the protocol and avoid mixing old models with new lease mechanisms.
     *
     * @param source database connection source
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static void requireEmptyUnbound(final DataSource source) throws SQLException {
        try (var c = source.getConnection(); var q = c.createStatement()) {
            requireAutoCommit(c);
            for (String table : List.of("pc_data", "pc_lock", "pc_write", "pc_rollback", "pc_guard")) {
                boolean exists;
                try (var check = c.prepareStatement("select count(*) from information_schema.tables where table_schema=database() and table_name=?")) {
                    check.setString(1, table);
                    try (var r = check.executeQuery()) {
                        r.next();
                        exists = r.getInt(1) != 0;
                    }
                }
                if (exists) {
                    try (var r = q.executeQuery("select exists(select 1 from " + table + " limit 1)")) {
                        r.next();
                        if (r.getBoolean(1)) {
                            throw new IllegalArgumentException("unbound Percolator data requires offline migration");
                        }
                    }
                }
            }
        }
    }

    /**
     * Shard binds to control namespace at the same time as logical shard and refuses to control library mismatches.
     *
     * @param source database connection source
     * @param shard logical shard identifier
     * @param namespace persistent protocol namespace
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static void bind(final DataSource source, final String shard, final String namespace) throws SQLException {
        try (var c = source.getConnection(); var q = c.createStatement()) {
            requireAutoCommit(c);
            q.execute("create table if not exists pc_binding(id int primary key,namespace varchar(36) not null,shard varchar(64) not null) engine=InnoDB");
            try (var insert = c.prepareStatement("insert into pc_binding values(1,?,?) on duplicate key update id=id")) {
                insert.setString(1, namespace);
                insert.setString(2, shard);
                insert.executeUpdate();
            }
            try (var r = q.executeQuery("select namespace,shard from pc_binding where id=1")) {
                if (!r.next() || !namespace.equals(r.getString(1)) || !shard.equals(r.getString(2))) {
                    throw new IllegalArgumentException("Percolator shard/control binding mismatch");
                }
            }
        }
    }

    /**
     * The life cycle protocol allows only the default connection of autocommit.
     *
     * @param c c
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static void requireAutoCommit(final Connection c) throws SQLException {
        if (!c.getAutoCommit()) {
            throw new IllegalArgumentException("Percolator requires auto-commit pools");
        }
    }

    /**
     * Keeps the database failure reason, and determines whether the result is unknown on the commit Layer.
     *
     * @param failure failure
     * @return the operation result
     */
    private static MvccException storage(final SQLException failure) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "Percolator lifecycle JDBC operation failed", true, null, failure);
    }

    /**
     * Stop new operations, and the pools borrowed continue to be managed by applications.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * Controls the SQL callback only for connection held by the current closed door.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    private interface SqlWork<T> {

        /**
         * Implementation of a limited control step.
         *
         * @param connection connection
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        T run(Connection connection) throws SQLException;
    }
}

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
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.storage.VersionedValue;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements Percolator locks, primary decisions and version storage with local MySQL transactions.
 */
public final class MysqlPercolatorStore implements PercolatorStore {

    private final String shard;

    private final DataSource dataSource;

    private final com.flyingzc.azir.operations.MysqlProtocolState operations;

    /**
     * Relocate caller connection pool; storage objects are not responsible for closing connection pool.
     *
     * @param shard logical shard identifier
     * @param dataSource database connection source
     */
    public MysqlPercolatorStore(final String shard, final DataSource dataSource) {
        this(shard, dataSource, null);
    }

    /**
     * Additional bound generation at the production point to prevent late release of SQL after freezing.
     *
     * @param shard logical shard identifier
     * @param dataSource database connection source
     * @param operations operations
     */
    public MysqlPercolatorStore(final String shard, final DataSource dataSource, final com.flyingzc.azir.operations.MysqlProtocolState operations) {
        this.operations = operations;
        this.shard = new PercolatorKey(shard, "").shard();
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Returns local shard number.
     *
     * @return the operation result
     */
    @Override
    public String shardId() {
        return shard;
    }

    /**
     * Create a list of isolation protocols; history decision reserved, payload GC protected by durable reading lease.
     *
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public void initialize() {
        List<String> ddl = List.of(
                "create table if not exists pc_guard (user_key varbinary(512) primary key) engine=InnoDB",
                """
                        create table if not exists pc_data (
                          user_key varbinary(512) not null, start_ts bigint not null,
                          deleted boolean not null, payload longblob,
                          primary key(user_key, start_ts)
                        ) engine=InnoDB
                        """,
                """
                        create table if not exists pc_lock (
                          user_key varbinary(512) primary key, start_ts bigint not null,
                          primary_shard varchar(64) character set ascii collate ascii_bin not null,
                          primary_key varbinary(512) not null, expires_at bigint not null,
                          index pending_order(start_ts, user_key)
                        ) engine=InnoDB
                        """,
                """
                        create table if not exists pc_write (
                          user_key varbinary(512) not null, commit_ts bigint not null, start_ts bigint not null,
                          primary key(user_key, commit_ts), unique key original_write(user_key, start_ts)
                        ) engine=InnoDB
                        """,
                """
                        create table if not exists pc_rollback (
                          user_key varbinary(512) not null, start_ts bigint not null,
                          primary key(user_key, start_ts)
                        ) engine=InnoDB
                        """);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            if (!connection.getAutoCommit()) {
                throw new IllegalArgumentException("Percolator requires an auto-commit pool");
            }
            for (String sql : ddl) {
                statement.execute(sql);
            }
            com.flyingzc.azir.operations.MysqlProtocolAdmin.ensureFinishedColumn(dataSource, "pc_write");
            com.flyingzc.azir.operations.MysqlProtocolAdmin.ensureFinishedColumn(dataSource, "pc_rollback");
        } catch (final SQLException ex) {
            throw storageError("initialize Percolator schema", null, ex);
        }
    }

    /**
     * The whole batch prewrite shares one connection and local transaction, sorts in bits by key and strictly rejects replay changes.
     *
     * @param startTs transaction start timestamp
     * @param primary original Percolator primary identity
     * @param ttlMillis ttl millis
     * @param mutations transaction write set
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public void prewrite(final long startTs, final PercolatorKey primary, final long ttlMillis, final List<Mutation> mutations) {
        validateTimestamp(startTs);
        Objects.requireNonNull(primary, "primary");
        if (ttlMillis < 1 || ttlMillis > 86_400_000L || mutations.isEmpty()) {
            throw new IllegalArgumentException("ttl must be in 1..86400000 and mutations nonempty");
        }
        List<Mutation> ordered = mutations.stream()
                .sorted(Comparator.comparing(Mutation::key, Utf8KeyOrder::compare)).toList();
        if (ordered.stream().map(Mutation::key).distinct().count() != ordered.size()) {
            throw new IllegalArgumentException("duplicate mutation key");
        }
        for (Mutation mutation : ordered) {
            new PercolatorKey(shard, mutation.key());
            Objects.requireNonNull(mutation.type(), "mutation type");
            if ((mutation.type() == MutationType.PUT) != (mutation.value() != null)) {
                throw new IllegalArgumentException("invalid mutation payload");
            }
        }
        atomic(startTs, connection -> {
            if (operations != null) {
                operations.lockData(connection, shard, startTs, false);
            }
            for (Mutation mutation : ordered) {
                guard(connection, mutation.key());
            }
            for (Mutation mutation : ordered) {
                String key = mutation.key();
                if (hasRollback(connection, key, startTs)) {
                    throw aborted(startTs, "prewrite fenced by rollback marker");
                }
                if (writeTimestamp(connection, key, startTs).isPresent()) {
                    throw error(ErrorCode.ILLEGAL_STATE, "prewrite replay after commit", false, startTs);
                }
                Optional<Lock> existing = lock(connection, key);
                if (existing.isPresent()) {
                    Lock held = existing.get();
                    if (held.startTs() != startTs || !held.primary().equals(primary)) {
                        throw error(ErrorCode.LOCK_CONFLICT, "key locked: " + key, true, startTs);
                    }
                    verifyReplay(connection, startTs, mutation);
                    continue;
                }
                try (
                        var select = connection.prepareStatement(
                                "select commit_ts from pc_write where user_key=? order by commit_ts desc limit 1")) {
                    select.setBytes(1, bytes(key));
                    try (var result = select.executeQuery()) {
                        if (result.next() && result.getLong(1) >= startTs) {
                            throw error(ErrorCode.WRITE_CONFLICT, "newer write: " + key, true, startTs);
                        }
                    }
                }
                try (
                        var data = connection.prepareStatement(
                                "insert into pc_data(user_key,start_ts,deleted,payload) values(?,?,?,?)");
                        var lock = connection.prepareStatement("""
                                insert into pc_lock(user_key,start_ts,primary_shard,primary_key,expires_at)
                                values(?,?,?,?,cast(unix_timestamp(current_timestamp(3))*1000 as signed)+?)
                                """)) {
                    data.setBytes(1, bytes(key));
                    data.setLong(2, startTs);
                    data.setBoolean(3, mutation.type() == MutationType.DELETE);
                    data.setBytes(4, mutation.value());
                    data.executeUpdate();
                    lock.setBytes(1, bytes(key));
                    lock.setLong(2, startTs);
                    lock.setString(3, primary.shard());
                    lock.setBytes(4, bytes(primary.key()));
                    lock.setLong(5, ttlMillis);
                    lock.executeUpdate();
                }
            }
            return null;
        });
    }

    /**
     * Read lock and version with a RR Consistency view; this local snapshot is used only to prevent double SQL torn.
     *
     * @param key business key
     * @param readTs snapshot read timestamp
     * @return the operation result
     */
    @Override
    public Snapshot snapshot(final String key, final long readTs) {
        new PercolatorKey(shard, key);
        validateTimestamp(readTs);
        try (var connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try {
                Optional<Lock> held = lock(connection, key);
                Optional<VersionedValue> value;
                try (var select = connection.prepareStatement("""
                        select w.commit_ts,d.deleted,d.payload from pc_write w join pc_data d
                        on w.user_key=d.user_key and w.start_ts=d.start_ts
                        where w.user_key=? and w.commit_ts<=? order by w.commit_ts desc limit 1
                        """)) {
                    select.setBytes(1, bytes(key));
                    select.setLong(2, readTs);
                    try (var result = select.executeQuery()) {
                        value = result.next() ? Optional.of(new VersionedValue(key, result.getBytes(3),
                                result.getBoolean(2), result.getLong(1), false)) : Optional.empty();
                    }
                }
                connection.commit();
                return new Snapshot(held, value);
            } catch (final SQLException | RuntimeException | Error ex) {
                rollback(connection, ex);
                throw ex;
            }
        } catch (final SQLException ex) {
            throw storageError("read Percolator snapshot", null, ex);
        }
    }

    /**
     * Bounded Lists the permanent candidate key; the associated prewrite has been created before the committee Ts distribution, without missing the commit in snapshot.
     *
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public List<String> keys(final KeyRange range, final String afterKeyExclusive, final int limit) {
        Objects.requireNonNull(range, "range");
        if (limit < 1 || limit > 4097) {
            throw new IllegalArgumentException("candidate limit must be in 1..4097");
        }
        List<String> parameters = new ArrayList<>();
        StringBuilder sql = new StringBuilder("select user_key from pc_guard where 1=1");
        if (range.startKeyInclusive() != null) {
            sql.append(" and user_key>=?");
            parameters.add(range.startKeyInclusive());
        }
        if (range.endKeyExclusive() != null) {
            sql.append(" and user_key<?");
            parameters.add(range.endKeyExclusive());
        }
        if (afterKeyExclusive != null) {
            sql.append(" and user_key>?");
            parameters.add(afterKeyExclusive);
        }
        sql.append(" order by user_key limit ?");
        // The range boundary may be the maximum length business key followed by a suffix, and it is not cut off as an actual storage key.
        parameters.forEach(Utf8KeyOrder::encode);
        try (var connection = dataSource.getConnection(); var select = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (String parameter : parameters) {
                select.setBytes(index++, bytes(parameter));
            }
            select.setInt(index, limit);
            try (var result = select.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (result.next()) {
                    keys.add(text(result.getBytes(1)));
                }
                return List.copyOf(keys);
            }
        } catch (final SQLException ex) {
            throw storageError("scan Percolator candidates", null, ex);
        }
    }

    /**
     * Serializes commit and rollback for the primary; missing primary must write fence before cleaning secondary.
     *
     * @param key business key
     * @param startTs transaction start timestamp
     * @param action action
     * @param commitTs durable commit timestamp
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public Decision primary(final PercolatorKey key, final long startTs, final Action action, final long commitTs) {
        validateTimestamp(startTs);
        if (!key.shard().equals(shard) || action == null
                || (action == Action.COMMIT && commitTs <= startTs)) {
            throw new IllegalArgumentException("invalid primary operation");
        }
        return atomic(startTs, connection -> {
            // Terminal state Permanent immutable, historical receipt query does not require any further changes to the permanent candidate line.
            Optional<Long> known = writeTimestamp(connection, key.key(), startTs);
            if (known.isPresent()) {
                return new Decision(State.COMMITTED, known.get());
            }
            if (hasRollback(connection, key.key(), startTs)) {
                return new Decision(State.ROLLED_BACK, 0);
            }
            if (operations != null && startTs <= operations.lockData(connection, shard, 0, true)) {
                var stale = lock(connection, key.key());
                if (stale.isPresent() && stale.get().startTs() == startTs) {
                    guard(connection, key.key());
                    rollbackKey(connection, key.key(), startTs);
                    return new Decision(State.ROLLED_BACK, 0);
                }
                return new Decision(State.RETIRED, 0);
            }
            guard(connection, key.key());
            Optional<Long> committed = writeTimestamp(connection, key.key(), startTs);
            if (committed.isPresent()) {
                return new Decision(State.COMMITTED, committed.get());
            }
            if (hasRollback(connection, key.key(), startTs)) {
                return new Decision(State.ROLLED_BACK, 0);
            }
            Optional<Lock> held = lock(connection, key.key());
            boolean owned = held.isPresent() && held.get().startTs() == startTs;
            if (owned && !held.get().primary().equals(key)) {
                throw error(ErrorCode.ILLEGAL_STATE, "address is not the primary", false, startTs);
            }
            if (!owned) {
                // Durable must prevent late startTs prewrite even before the first prewrite.
                rollbackKey(connection, key.key(), startTs);
                return new Decision(State.ROLLED_BACK, 0);
            }
            if (action == Action.ROLLBACK || expired(connection, key.key())) {
                rollbackKey(connection, key.key(), startTs);
                return new Decision(State.ROLLED_BACK, 0);
            }
            if (action == Action.COMMIT) {
                commitKey(connection, key.key(), startTs, commitTs);
                return new Decision(State.COMMITTED, commitTs);
            }
            return new Decision(State.PENDING, 0);
        });
    }

    /**
     * Only terminal state completed keys identified by the caller; old transaction replay cannot touch new transaction locks.
     *
     * @param startTs transaction start timestamp
     * @param primary original Percolator primary identity
     * @param decision decision
     * @param keys keys
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public void finish(final long startTs, final PercolatorKey primary, final Decision decision, final List<String> keys) {
        validateTimestamp(startTs);
        if ((decision.state() != State.COMMITTED && decision.state() != State.ROLLED_BACK) || (decision.state() == State.COMMITTED && decision.commitTs() <= startTs)) {
            throw new IllegalArgumentException("finish requires a valid terminal decision");
        }
        List<String> ordered = keys.stream().distinct().sorted(Utf8KeyOrder::compare).toList();
        for (String key : ordered) {
            new PercolatorKey(shard, key);
        }
        atomic(startTs, connection -> {
            for (String key : ordered) {
                guard(connection, key);
            }
            for (String key : ordered) {
                Optional<Long> committed = writeTimestamp(connection, key, startTs);
                if (committed.isPresent()) {
                    if (decision.state() != State.COMMITTED || committed.get() != decision.commitTs()) {
                        throw error(ErrorCode.ILLEGAL_STATE, "inconsistent finish decision", false, startTs);
                    }
                    continue;
                }
                Optional<Lock> held = lock(connection, key);
                if (held.isPresent() && held.get().startTs() == startTs && !held.get().primary().equals(primary)) {
                    throw error(ErrorCode.ILLEGAL_STATE, "primary mismatch", false, startTs);
                }
                if (decision.state() == State.ROLLED_BACK) {
                    rollbackKey(connection, key, startTs);
                } else {
                    if (hasRollback(connection, key, startTs)
                            || held.isEmpty() || held.get().startTs() != startTs) {
                        throw error(ErrorCode.ILLEGAL_STATE, "committed secondary prewrite missing", false, startTs);
                    }
                    commitKey(connection, key, startTs, decision.commitTs());
                }
            }
            return null;
        });
    }

    /**
     * A limited lock is found in the original transaction order of each round to avoid loading all historical data on start-up.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public List<Lock> pending(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("limit must be in 1..4096");
        }
        try (
                var connection = dataSource.getConnection(); var select = connection.prepareStatement(
                        "select user_key,start_ts,primary_shard,primary_key from pc_lock order by start_ts,user_key limit ?")) {
            select.setInt(1, limit);
            try (var result = select.executeQuery()) {
                List<Lock> locks = new ArrayList<>();
                while (result.next()) {
                    locks.add(new Lock(new PercolatorKey(shard, text(result.getBytes(1))), result.getLong(2),
                            new PercolatorKey(result.getString(3), text(result.getBytes(4)))));
                }
                return List.copyOf(locks);
            }
        } catch (final SQLException ex) {
            throw storageError("discover Percolator locks", null, ex);
        }
    }

    /**
     * Statistics durable locks, used for active diagnostic purposes, do not load all of them in stacks.
     *
     * @return the operation result
     */
    @Override
    public long pendingCount() {
        try (var c = dataSource.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select count(*) from pc_lock")) {
            r.next();
            return r.getLong(1);
        } catch (final SQLException failure) {
            throw storageError("count Percolator locks", null, failure);
        }
    }

    /**
     * Keep safe point anchor with each key, safe tombstone to retirement; write decision and rollback fence do not delete.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public int collect(final long safePoint, final int limit) {
        if (safePoint < 0 || limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid GC budget");
        }
        return atomic(0, connection -> {
            /**
             * Exact original payload keys can be deleted to avoid unsafe cropping by historical number.
             *
             * @param key business key
             * @param start start
             */
            record Obsolete(String key, long start) {
            }

            List<Obsolete> old = new ArrayList<>();
            try (
                    var q = connection.prepareStatement(
                            "select d.user_key,d.start_ts from pc_data d join pc_write w on w.user_key=d.user_key and w.start_ts=d.start_ts where w.commit_ts<=? and exists(select 1 from pc_write n where n.user_key=w.user_key and n.commit_ts>w.commit_ts and n.commit_ts<=?) order by d.user_key,d.start_ts limit ?")) {
                q.setLong(1, safePoint);
                q.setLong(2, safePoint);
                q.setInt(3, limit);
                try (var r = q.executeQuery()) {
                    while (r.next()) {
                        old.add(new Obsolete(text(r.getBytes(1)), r.getLong(2)));
                    }
                }
            }
            int deleted = 0;
            for (var version : old) {
                com.flyingzc.azir.operations.MysqlProtocolState.checkInterrupted();
                guard(connection, version.key());
                try (var q = connection.prepareStatement("delete from pc_data where user_key=? and start_ts=?")) {
                    bindKey(q, version.key(), version.start());
                    deleted += q.executeUpdate();
                }
            }
            if (old.size() < limit) {
                List<Obsolete> tombstones = new ArrayList<>();
                try (
                        var q = connection.prepareStatement(
                                "select d.user_key,d.start_ts from pc_data d join pc_write w on w.user_key=d.user_key and w.start_ts=d.start_ts where d.deleted=true and w.commit_ts<=? and not exists(select 1 from pc_write n where n.user_key=w.user_key and n.commit_ts>w.commit_ts) and not exists(select 1 from pc_data n where n.user_key=d.user_key and n.start_ts<>d.start_ts) and not exists(select 1 from pc_lock l where l.user_key=d.user_key) order by d.user_key limit ?")) {
                    q.setLong(1, safePoint);
                    q.setInt(2, limit - old.size());
                    try (var r = q.executeQuery()) {
                        while (r.next()) {
                            tombstones.add(new Obsolete(text(r.getBytes(1)), r.getLong(2)));
                        }
                    }
                }
                for (var version : tombstones) {
                    guard(connection, version.key());
                    try (var q = connection.prepareStatement("delete from pc_data where user_key=? and start_ts=?")) {
                        bindKey(q, version.key(), version.start());
                        deleted += q.executeUpdate();
                    }
                }
            }
            return deleted;
        });
    }

    /**
     * Cross-table protocols lock the permanent Guard line to avoid a race between rows, locking and plugging.
     *
     * @param connection connection
     * @param key business key
     * @throws SQLException if the operation cannot complete
     */
    private void guard(final Connection connection, final String key) throws SQLException {
        try (
                var insert = connection.prepareStatement(
                        "insert into pc_guard(user_key) values(?) on duplicate key update user_key=values(user_key)")) {
            insert.setBytes(1, bytes(key));
            insert.executeUpdate();
        }
    }

    /**
     * Reads the current key lock; all changes have been made to call by holding a guard and using RC, read to use an independent RR view.
     *
     * @param connection connection
     * @param key business key
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Optional<Lock> lock(final Connection connection, final String key) throws SQLException {
        try (
                var select = connection.prepareStatement(
                        "select start_ts,primary_shard,primary_key from pc_lock where user_key=?")) {
            select.setBytes(1, bytes(key));
            try (var result = select.executeQuery()) {
                return result.next() ? Optional.of(new Lock(new PercolatorKey(shard, key), result.getLong(1),
                        new PercolatorKey(result.getString(2), text(result.getBytes(3))))) : Optional.empty();
            }
        }
    }

    /**
     * Use the only reverse index to startTs to check commit time and avoid scanning the entire history of the project.
     *
     * @param connection connection
     * @param key business key
     * @param startTs transaction start timestamp
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Optional<Long> writeTimestamp(final Connection connection, final String key, final long startTs) throws SQLException {
        try (var select = connection.prepareStatement("select commit_ts from pc_write where user_key=? and start_ts=?")) {
            bindKey(select, key, startTs);
            try (var result = select.executeQuery()) {
                return result.next() ? Optional.of(result.getLong(1)) : Optional.empty();
            }
        }
    }

    /**
     * Check if there is a permanent rollback fence.
     *
     * @param connection connection
     * @param key business key
     * @param startTs transaction start timestamp
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private boolean hasRollback(final Connection connection, final String key, final long startTs) throws SQLException {
        try (var select = connection.prepareStatement("select 1 from pc_rollback where user_key=? and start_ts=?")) {
            bindKey(select, key, startTs);
            try (var result = select.executeQuery()) {
                return result.next();
            }
        }
    }

    /**
     * Deciding on a primary database clock expired without using logic TSO as milliseconds.
     *
     * @param connection connection
     * @param key business key
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private boolean expired(final Connection connection, final String key) throws SQLException {
        try (var select = connection.prepareStatement("""
                select expires_at<=cast(unix_timestamp(current_timestamp(3))*1000 as signed)
                from pc_lock where user_key=?
                """)) {
            select.setBytes(1, bytes(key));
            try (var result = select.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }

    /**
     * Repeating prewrite must be identical and cannot modify the write set that has been used for allocating committs.
     *
     * @param connection connection
     * @param startTs transaction start timestamp
     * @param mutation mutation
     * @throws SQLException if the operation cannot complete
     */
    private void verifyReplay(final Connection connection, final long startTs, final Mutation mutation) throws SQLException {
        try (var select = connection.prepareStatement("select deleted,payload from pc_data where user_key=? and start_ts=?")) {
            bindKey(select, mutation.key(), startTs);
            try (var result = select.executeQuery()) {
                if (!result.next() || result.getBoolean(1) != (mutation.type() == MutationType.DELETE)
                        || !Arrays.equals(result.getBytes(2), mutation.value())) {
                    throw error(ErrorCode.ILLEGAL_STATE, "changed prewrite replay", false, startTs);
                }
            }
        }
    }

    /**
     * Publishs write and removes the lock of the original startTs in local transaction where you hold the guard.
     *
     * @param connection connection
     * @param key business key
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @throws SQLException if the operation cannot complete
     */
    private void commitKey(final Connection connection, final String key, final long startTs, final long commitTs) throws SQLException {
        try (
                var insert = connection.prepareStatement("insert into pc_write(user_key,commit_ts,start_ts) values(?,?,?)");
                var delete = connection.prepareStatement("delete from pc_lock where user_key=? and start_ts=?")) {
            insert.setBytes(1, bytes(key));
            insert.setLong(2, commitTs);
            insert.setLong(3, startTs);
            insert.executeUpdate();
            bindKey(delete, key, startTs);
            if (delete.executeUpdate() != 1) {
                throw error(ErrorCode.ILLEGAL_STATE, "commit lock missing", false, startTs);
            }
        }
    }

    /**
     * Record the non-restorable rollback tags first, then delete the original transaction data and lock.
     *
     * @param connection connection
     * @param key business key
     * @param startTs transaction start timestamp
     * @throws SQLException if the operation cannot complete
     */
    private void rollbackKey(final Connection connection, final String key, final long startTs) throws SQLException {
        if (operations != null && startTs <= operations.lockData(connection, shard, 0, true) && !hasRollback(connection, key, startTs)
                && lock(connection, key).filter(held -> held.startTs() == startTs).isEmpty()) {
            throw com.flyingzc.azir.operations.MysqlProtocolState.expired(startTs);
        }
        try (var marker = connection.prepareStatement("""
                insert into pc_rollback(user_key,start_ts) values(?,?)
                on duplicate key update start_ts=values(start_ts)
                """)) {
            bindKey(marker, key, startTs);
            marker.executeUpdate();
        }
        for (String table : List.of("pc_lock", "pc_data")) {
            try (var delete = connection.prepareStatement("delete from " + table + " where user_key=? and start_ts=?")) {
                bindKey(delete, key, startTs);
                delete.executeUpdate();
            }
        }
    }

    /**
     * Align binding binary keys with original transaction timestamp.
     *
     * @param statement statement
     * @param key business key
     * @param startTs transaction start timestamp
     * @throws SQLException if the operation cannot complete
     */
    private static void bindKey(final PreparedStatement statement, final String key, final long startTs) throws SQLException {
        statement.setBytes(1, bytes(key));
        statement.setLong(2, startTs);
    }

    /**
     * Local transaction Unified rollback for side-effects; commit Response missing from run-time parsing Primay.
     *
     * @param <T> value type
     * @param startTs transaction start timestamp
     * @param work work
     * @return the operation result
     */
    private <T> T atomic(final long startTs, final SqlWork<T> work) {
        try (var connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                if (operations != null) {
                    operations.lockData(connection, shard, startTs, true);
                }
                T result = work.apply(connection);
                if (operations != null) {
                    operations.requireDataPublication();
                }
                connection.commit();
                return result;
            } catch (final SQLException | RuntimeException | Error ex) {
                rollback(connection, ex);
                throw ex;
            }
        } catch (final SQLException ex) {
            throw storageError("Percolator local transaction", startTs, ex);
        }
    }

    /**
     * Rollback Failed to retain as additional evidence of original anomaly.
     *
     * @param connection connection
     * @param original original
     */
    private static void rollback(final Connection connection, final Throwable original) {
        try {
            connection.rollback();
        } catch (final SQLException | RuntimeException | Error failure) {
            if (original != failure) {
                original.addSuppressed(failure);
            }
            // The rollback unknown connection cannot be returned to the pool and subsequent autocommit or connection reuse will undermine local atomic.
            com.flyingzc.azir.control.MysqlNamedLock.discard(connection, original);
        }
    }

    /**
     * Accept only normal logic timestamp.
     *
     * @param timestamp timestamp
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void validateTimestamp(final long timestamp) {
        if (timestamp <= 0) {
            throw new IllegalArgumentException("timestamp must be positive");
        }
    }

    /**
     * Use the existing binary key code.
     *
     * @param key business key
     * @return the operation result
     */
    private static byte[] bytes(final String key) {
        return Utf8KeyOrder.encode(key);
    }

    /**
     * Resumes the UTF-8 key that passes validate from the protocol table.
     *
     * @param key business key
     * @return the operation result
     */
    private static String text(final byte[] key) {
        return new String(key, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * An abnormal protocol to construct the original transaction identity belt.
     *
     * @param code code
     * @param message message
     * @param retryable retryable
     * @param startTs transaction start timestamp
     * @return the operation result
     */
    private static MvccException error(final ErrorCode code, final String message, final boolean retryable, final long startTs) {
        return new MvccException(code, message, retryable, startTs);
    }

    /**
     * Allows business to rerun the whole transaction when rollback is clear.
     *
     * @param startTs transaction start timestamp
     * @param message message
     * @return the operation result
     */
    private static MvccException aborted(final long startTs, final String message) {
        return error(ErrorCode.TRANSACTION_ABORTED, message, true, startTs);
    }

    /**
     * Keeps an database error; the error itself does not indicate whether Primary is already commit.
     *
     * @param operation operation
     * @param startTs transaction start timestamp
     * @param cause cause
     * @return the operation result
     */
    private static MvccException storageError(final String operation, final Long startTs, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, operation + " failed", true, startTs, cause);
    }

    /**
     * SQL operation executed in the same local transaction.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    private interface SqlWork<T> {

        /**
         * Execute protocol atomic operation.
         *
         * @param connection connection
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        T apply(Connection connection) throws SQLException;
    }
}

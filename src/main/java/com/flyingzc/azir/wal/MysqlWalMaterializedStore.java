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

package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The new mode is reserved for MySQL history version storage and only two tables with azir_wal prefix are created.
 */
public final class MysqlWalMaterializedStore implements WalMaterializedStore {

    private final DataSource dataSource;

    private UUID engineId;

    private String shard;

    private long packetBudget;

    private String replicaOwner;

    /**
     * Saves the data source managed by the caller, each shard must use a stand-alone database.
     *
     * @param dataSource database connection source
     */
    public MysqlWalMaterializedStore(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    /**
     * Initialize an exclusive form and permanently bind WAL identity, rejecting the old library or other mode being implicitly taken over.
     *
     * @param engineId engine id
     * @param shard logical shard identifier
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public synchronized void initialize(final UUID engineId, final String shard) {
        if (replicaOwner != null) {
            throw new IllegalStateException("replica backend requires explicit replica initialization");
        }
        initializeBound(engineId, shard);
    }

    /**
     * The caller has clearly selected the mode after checking the database binding and does not automatically convert the already existing mode.
     *
     * @param engineId engine id
     * @param shard logical shard identifier
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void initializeBound(final UUID engineId, final String shard) {
        if (this.engineId != null && (!this.engineId.equals(engineId) || !this.shard.equals(shard))) {
            throw new IllegalStateException("materialized store already bound");
        }
        try (Connection connection = dataSource.getConnection()) {
            createTables(connection);
            connection.setAutoCommit(false);
            try {
                try (
                        var query = connection.createStatement();
                        var row = query.executeQuery("select id from azir_wal_binding where id=1 for update")) {
                    if (!row.next()) {
                        try (
                                var count = connection.createStatement();
                                var existing = count.executeQuery("select count(*) from azir_wal_version")) {
                            existing.next();
                            if (existing.getLong(1) != 0) {
                                throw new SQLException("orphan materialized versions");
                            }
                        }
                        try (var insert = connection.prepareStatement("insert into azir_wal_binding (id,engine_id,shard_id,applied_seq,digest,replica_owner) values (1,?,?,0,'',?)")) {
                            insert.setString(1, engineId.toString());
                            insert.setBytes(2, shard.getBytes(StandardCharsets.UTF_8));
                            insert.setString(3, replicaOwner);
                            insert.executeUpdate();
                        }
                    }
                }
                verifyBinding(connection, engineId, shard, false);
                connection.commit();
                this.engineId = engineId;
                this.shard = shard;
                configurePacketBudget(connection);
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            }
        } catch (final SQLException ex) {
            throw storage("initialize materialized store", ex);
        }
    }

    /**
     * Local replica has an independent logic database. The shared library could not be configured wrongly into three data replica.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsReplicaOwnership() {
        return true;
    }

    /**
     * Only new storage bound nodes identity cannot be upgraded from the initialised normal engine.
     *
     * @param identity identity
     * @param shard logical shard identifier
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public synchronized void initializeReplica(final WalReplicaIdentity identity, final String shard) {
        String owner = identity.fingerprint();
        if (engineId != null && replicaOwner == null || replicaOwner != null && !replicaOwner.equals(owner)) {
            throw new IllegalStateException("backend already has another owner");
        }
        replicaOwner = owner;
        initializeBound(identity.engineId(), shard);
    }

    /**
     * Creates a new exclusive table, the old mode table does not participate in the migration; the old version exclusive binding table only adds an recovery status field.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private static void createTables(final Connection connection) throws SQLException {
        WalMysqlSchema.engines(connection);
        try (var ddl = connection.createStatement()) {
            ddl.executeUpdate("create table if not exists azir_wal_binding ("
                    + "id tinyint primary key, engine_id char(36) character set ascii not null, "
                    + "shard_id varbinary(128) not null, applied_seq bigint not null, digest char(64) character set ascii not null, "
                    + "restore_id char(64) character set ascii null, restoring tinyint not null default 0, "
                    + "gc_cursor varbinary(512) null,gc_repeat int not null default 0,replica_owner char(64) character set ascii null) engine=InnoDB");
            ddl.executeUpdate("create table if not exists azir_wal_version ("
                    + "user_key varbinary(512) not null, commit_seq bigint not null, payload longblob null, "
                    + "primary key (user_key, commit_seq)) engine=InnoDB");
        }
        int columns = restoreColumns(connection);
        if (columns == 0) {
            try (var ddl = connection.createStatement()) {
                ddl.executeUpdate("alter table azir_wal_binding add column restore_id char(64) character set ascii null, "
                        + "add column restoring tinyint not null default 0");
            } catch (final SQLException race) {
                if (race.getErrorCode() != 1060 || restoreColumns(connection) != 2) {
                    throw race;
                }
            }
        } else if (columns != 2) {
            throw new SQLException("partial restore metadata schema");
        }
        WalMysqlSchema.migrateGc(connection);
        WalMysqlSchema.migrateReplica(connection);
        WalMysqlSchema.validate(connection);
    }

    /**
     * Use fixed table metadata to identify old version table schema of the new mode and not read other tables.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static int restoreColumns(final Connection connection) throws SQLException {
        int count = 0;
        try (var columns = connection.getMetaData().getColumns(connection.getCatalog(), null, "azir_wal_binding", null)) {
            while (columns.next()) {
                if (!columns.getString("TABLE_NAME").equals("azir_wal_binding")) {
                    continue;
                }
                String name = columns.getString("COLUMN_NAME");
                if (name.equals("restore_id") || name.equals("restoring")) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Normal writing and recovery import follows the same backend package limit.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void configurePacketBudget(final Connection connection) throws SQLException {
        try (var query = connection.createStatement(); var row = query.executeQuery("select @@max_allowed_packet")) {
            row.next();
            long serverLimit = row.getLong(1);
            long clientLimit = connection.unwrap(com.mysql.cj.jdbc.JdbcConnection.class).getPropertySet()
                    .getIntegerProperty(com.mysql.cj.conf.PropertyKey.maxAllowedPacket).getValue();
            packetBudget = Math.min(serverLimit, clientLimit) - 4096;
            if (packetBudget < 4096) {
                throw new SQLException("configured packet limit is too small");
            }
        }
    }

    /**
     * Conservatively charged to the cost of text conversion, rejecting a single line that cannot be materialization before transaction is confirmed.
     *
     * @param mutation mutation
     * @throws WalMvccException if the operation cannot complete
     */
    @Override
    public void validateMutation(final WalMutation mutation) {
        if (packetBudget < 1 || estimatedPacketBytes(mutation) > packetBudget) {
            throw new WalMvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "mutation exceeds configured MySQL/JDBC packet budget", null, null);
        }
    }

    /**
     * A uniform conservative estimate of binary and text protocols, including key, payload, parameters and SQL costs.
     *
     * @param mutation mutation
     * @return the operation result
     */
    private static long estimatedPacketBytes(final WalMutation mutation) {
        return 256L + 2L * (mutation.key().getBytes(StandardCharsets.UTF_8).length + mutation.valueLength());
    }

    /**
     * Get the current durable prefix and check the engine with shard identity.
     *
     * @return the operation result
     */
    @Override
    public WalCheckpoint checkpoint() {
        try (Connection connection = dataSource.getConnection()) {
            return verifyBinding(connection, engineId, shard, false, true);
        } catch (final SQLException ex) {
            throw storage("read materialization checkpoint", ex);
        }
    }

    /**
     * Batch Write version and advance prefix in an local transaction to confirm safe retry if lost.
     *
     * @param commits commits
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public WalCheckpoint apply(final List<WalCommit> commits) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WalCheckpoint current = verifyBinding(connection, engineId, shard, true);
                long sequence = current.sequence();
                String digest = current.digest();
                try (
                        var insert = connection.prepareStatement(
                                "insert into azir_wal_version (user_key,commit_seq,payload) values (?,?,?)")) {
                    long batchBytes = 0;
                    for (WalCommit commit : commits) {
                        if (commit.sequence() <= sequence) {
                            if (commit.sequence() == sequence && !commit.digest().equals(digest)) {
                                throw new SQLException("replay digest mismatch");
                            }
                            continue;
                        }
                        if (commit.sequence() != sequence + 1) {
                            throw new SQLException("non-contiguous materialization");
                        }
                        for (WalMutation mutation : commit.mutations()) {
                            if (!mutation.shard().equals(shard)) {
                                continue;
                            }
                            validateMutation(mutation);
                            long bytes = estimatedPacketBytes(mutation);
                            if (batchBytes > 0 && batchBytes + bytes > packetBudget) {
                                insert.executeBatch();
                                insert.clearBatch();
                                batchBytes = 0;
                            }
                            insert.setBytes(1, mutation.key().getBytes(StandardCharsets.UTF_8));
                            insert.setLong(2, commit.sequence());
                            insert.setBytes(3, mutation.value());
                            insert.addBatch();
                            batchBytes += bytes;
                        }
                        sequence = commit.sequence();
                        digest = commit.digest();
                    }
                    if (batchBytes > 0) {
                        insert.executeBatch();
                    }
                }
                if (sequence > current.sequence()) {
                    try (
                            var update = connection.prepareStatement("update azir_wal_binding set applied_seq=?,digest=? "
                                    + "where id=1 and engine_id=? and shard_id=?")) {
                        update.setLong(1, sequence);
                        update.setString(2, digest);
                        update.setString(3, engineId.toString());
                        update.setBytes(4, shard.getBytes(StandardCharsets.UTF_8));
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("materialization authority changed");
                        }
                    }
                }
                connection.commit();
                return new WalCheckpoint(sequence, digest);
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            }
        } catch (final SQLException ex) {
            throw storage("apply materialized versions", ex);
        }
    }

    /**
     * Read history version by frame commit serial number without relying on MySQL local RR definition global snapshot.
     *
     * @param key business key
     * @param readSequence read sequence
     * @return the operation result
     */
    @Override
    public Optional<WalValue> get(final String key, final long readSequence) {
        if (replicaOwner != null) {
            checkpoint();
        }
        try (
                Connection connection = dataSource.getConnection();
                var query = connection.prepareStatement("select commit_seq,payload from azir_wal_version "
                        + "where user_key=? and commit_seq<=? order by commit_seq desc limit 1")) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            query.setLong(2, readSequence);
            try (var row = query.executeQuery()) {
                return row.next() ? Optional.of(new WalValue(row.getLong(1), row.getBytes(2))) : Optional.empty();
            }
        } catch (final SQLException ex) {
            throw storage("read materialized version", ex);
        }
    }

    /**
     * Reads conflict fence using primary key version index without bringing cold data payload into the commit path.
     *
     * @param key business key
     * @return the operation result
     */
    @Override
    public long latestSequence(final String key) {
        if (replicaOwner != null) {
            checkpoint();
        }
        try (
                Connection connection = dataSource.getConnection();
                var query = connection.prepareStatement("select commit_seq from azir_wal_version "
                        + "where user_key=? order by commit_seq desc limit 1")) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            try (var row = query.executeQuery()) {
                return row.next() ? row.getLong(1) : 0;
            }
        } catch (final SQLException ex) {
            throw storage("read materialized conflict fence", ex);
        }
    }

    /**
     * Scan candidates from a fixed version cut-off and set line numbers and bytes caps to avoid unlimited materialization query results.
     *
     * @param from from
     * @param to to
     * @param readSequence read sequence
     * @return the operation result
     * @throws WalMvccException if the operation cannot complete
     */
    @Override
    public NavigableMap<String, WalValue> scan(final String from, final String to, final long readSequence) {
        if (replicaOwner != null) {
            checkpoint();
        }
        String range = (from == null ? "" : " and user_key>=?") + (to == null ? "" : " and user_key<?");
        String sql = "select v.user_key,v.commit_seq,v.payload from azir_wal_version v join "
                + "(select user_key,max(commit_seq) seq from azir_wal_version where commit_seq<=?"
                + range + " group by user_key) visible on v.user_key=visible.user_key and v.commit_seq=visible.seq "
                + "order by v.user_key limit 10001";
        try (Connection connection = dataSource.getConnection(); var query = connection.prepareStatement(sql)) {
            int parameter = 1;
            query.setLong(parameter++, readSequence);
            if (from != null) {
                query.setBytes(parameter++, from.getBytes(StandardCharsets.UTF_8));
            }
            if (to != null) {
                query.setBytes(parameter, to.getBytes(StandardCharsets.UTF_8));
            }
            var result = new TreeMap<String, WalValue>(Utf8KeyOrder::compare);
            long bytes = 0;
            try (var rows = query.executeQuery()) {
                while (rows.next()) {
                    byte[] value = rows.getBytes(3);
                    bytes += 128L + (value == null ? 0 : value.length);
                    if (result.size() >= 10_000 || bytes > 8L * 1024 * 1024) {
                        throw new WalMvccException(ErrorCode.RESOURCE_EXHAUSTED, "backend scan budget exhausted", null, null);
                    }
                    result.put(decodeKey(rows.getBytes(1)), new WalValue(rows.getLong(2), value));
                }
            }
            return result;
        } catch (final SQLException ex) {
            throw storage("scan materialized versions", ex);
        }
    }

    /**
     * The manual collection entry, the number of scanning keys and the number of deleted lines are subject to independent budget.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param keyLimit key limit
     * @return the operation result
     */
    @Override
    public int gc(final long safePoint, final int keyLimit) {
        return gcBounded(safePoint, keyLimit, 4096, 5000).removedRows();
    }

    /**
     * This realization provides durable cursor and limited deletions to allow hosting.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsBoundedMaintenance() {
        return true;
    }

    /**
     * Key-by-key primary key seek, do not aggregate the full library; old version before tombstone deletes and part batch does not revive old values.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param keyLimit key limit
     * @param rowLimit row limit
     * @param budgetMillis budget millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public GcProgress gcBounded(final long safePoint, final int keyLimit, final int rowLimit, final long budgetMillis) {
        if (safePoint < 0 || keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096 || budgetMillis < 1 || budgetMillis > 5000) {
            throw new IllegalArgumentException("invalid bounded GC limits");
        }
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(budgetMillis);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WalCheckpoint applied = verifyBinding(connection, engineId, shard, true);
                byte[] cursor;
                int repeat;
                try (var query = connection.createStatement(); var row = query.executeQuery("select gc_cursor,gc_repeat from azir_wal_binding where id=1")) {
                    row.next();
                    cursor = row.getBytes(1);
                    repeat = row.getInt(2);
                }
                if (repeat < 0 || repeat > 3) {
                    throw new SQLException("invalid GC cursor repeat state");
                }
                int inspected = 0;
                int removed = 0;
                boolean wrapped = false;
                while (inspected < keyLimit && removed < rowLimit && (inspected == 0 || System.nanoTime() - deadline < 0)) {
                    byte[] key;
                    String seek = "select user_key from azir_wal_version" + (cursor == null ? "" : " where user_key>?") + " order by user_key,commit_seq limit 1";
                    try (var query = connection.prepareStatement(seek)) {
                        query.setQueryTimeout(1);
                        if (cursor != null) {
                            query.setBytes(1, cursor);
                        }
                        try (var row = query.executeQuery()) {
                            key = row.next() ? row.getBytes(1) : null;
                        }
                    }
                    if (key == null) {
                        cursor = null;
                        repeat = 0;
                        wrapped = true;
                        break;
                    }
                    inspected++;
                    long anchor = -1;
                    boolean tombstone = false;
                    try (var query = connection.prepareStatement("select commit_seq,payload is null from azir_wal_version where user_key=? and commit_seq<=? order by commit_seq desc limit 1")) {
                        query.setQueryTimeout(1);
                        query.setBytes(1, key);
                        query.setLong(2, Math.min(safePoint, applied.sequence()));
                        try (var row = query.executeQuery()) {
                            if (row.next()) {
                                anchor = row.getLong(1);
                                tombstone = row.getBoolean(2);
                            }
                        }
                    }
                    int deleted = 0;
                    boolean remaining = false;
                    if (anchor >= 0) {
                        String bound = tombstone ? "<=?" : "<?";
                        try (var delete = connection.prepareStatement("delete from azir_wal_version where user_key=? and commit_seq" + bound + " order by commit_seq asc limit ?")) {
                            delete.setQueryTimeout(1);
                            delete.setBytes(1, key);
                            delete.setLong(2, anchor);
                            delete.setInt(3, rowLimit - removed);
                            deleted = delete.executeUpdate();
                        }
                        if (deleted == rowLimit - removed) {
                            try (var query = connection.prepareStatement("select commit_seq from azir_wal_version where user_key=? and commit_seq" + bound + " order by commit_seq limit 1")) {
                                query.setQueryTimeout(1);
                                query.setBytes(1, key);
                                query.setLong(2, anchor);
                                try (var row = query.executeQuery()) {
                                    remaining = row.next();
                                }
                            }
                        }
                    }
                    removed += deleted;
                    // One key is used for a maximum of three consecutive rounds of budget and then rotates, avoiding the permanent starvation of other keys in hot spots history.
                    if (remaining && repeat < 3) {
                        repeat++;
                        break;
                    }
                    cursor = key;
                    repeat = 0;
                }
                try (var update = connection.prepareStatement("update azir_wal_binding set gc_cursor=?,gc_repeat=? where id=1")) {
                    update.setBytes(1, cursor);
                    update.setInt(2, repeat);
                    update.executeUpdate();
                }
                connection.commit();
                return new GcProgress(inspected, removed, wrapped);
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            }
        } catch (final SQLException ex) {
            throw storage("collect materialized history", ex);
        }
    }

    /**
     * Host entrance requests verify loan connection, connection and read-time, as well as a reliable local commit configuration.
     *
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public void validateProduction() {
        try {
            var pool = dataSource.unwrap(com.zaxxer.hikari.HikariDataSource.class);
            if (pool.getConnectionTimeout() > 5000) {
                throw new SQLException("managed MySQL pool connectionTimeout must be at most 5000ms");
            }
            try (var connection = dataSource.getConnection()) {
                var properties = connection.unwrap(com.mysql.cj.jdbc.JdbcConnection.class).getPropertySet();
                for (var property : List.of(com.mysql.cj.conf.PropertyKey.connectTimeout, com.mysql.cj.conf.PropertyKey.socketTimeout)) {
                    int value = properties.getIntegerProperty(property).getValue();
                    if (value < 1 || value > 5000) {
                        throw new SQLException("managed MySQL " + property.getKeyName() + " must be 1..5000ms");
                    }
                }
                // The status method may query the server and must be called after the confirmation driver I/O has reached an existing deadline.
                if (connection.isReadOnly() || connection.getTransactionIsolation() == Connection.TRANSACTION_READ_UNCOMMITTED) {
                    throw new SQLException("managed MySQL requires writable committed reads");
                }
                try (var query = connection.createStatement()) {
                    query.setQueryTimeout(5);
                    try (var row = query.executeQuery("select @@innodb_flush_log_at_trx_commit,@@sync_binlog,@@log_bin,@@read_only,@@super_read_only")) {
                        row.next();
                        if (row.getInt(1) != 1 || row.getBoolean(3) && row.getInt(2) != 1 || row.getBoolean(4) || row.getBoolean(5)) {
                            throw new SQLException("managed MySQL requires durable writable storage");
                        }
                    }
                }
                WalMysqlSchema.engines(connection);
            }
        } catch (final SQLException failure) {
            throw storage("validate production backend", failure);
        }
    }

    /**
     * MySQL completes the snapshot flow and recovery isolation protocols, which cannot be replaced by a normal bounded scan.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsBackup() {
        return true;
    }

    /**
     * Replica recovery fingerprints tied to metadata and received identity at the same time, with normal initialization still refusing to take over after completion.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsReplicaRestore() {
        return true;
    }

    /**
     * RR transaction of MySQL reads the view for fixed consistency when binding a query.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsPinnedSnapshot() {
        return true;
    }

    /**
     * Compatible with the original line back-up entry, using the same fixed-read view protocol.
     *
     * @param boundary boundary
     * @param sink sink
     * @throws java.io.IOException if the operation cannot complete
     */
    @Override
    public void exportSnapshot(final WalCheckpoint boundary, final BackupSink sink) throws java.io.IOException {
        try (var view = openSnapshot(boundary)) {
            view.export(sink);
        }
    }

    /**
     * Retain transaction after the precise boundary has been identified in the preparatory phase, followed by materialization and GC to continue with other connection.
     *
     * @param boundary boundary
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public SnapshotView openSnapshot(final WalCheckpoint boundary) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setReadOnly(true);
            connection.setAutoCommit(false);
            if (!boundary.equals(verifyBinding(connection, engineId, shard, false))) {
                throw new SQLException("backup boundary is not fully materialized");
            }
            return new MysqlSnapshotView(connection, boundary);
        } catch (final Throwable failure) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (final SQLException close) {
                    failure.addSuppressed(close);
                }
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw storage("open materialized snapshot", (SQLException) failure);
        }
    }

    /**
     * Only empty database or the same backup recovery state is accepted, and initialized normal database cannot be taken over even if it is empty.
     *
     * @param spec spec
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public synchronized boolean beginRestore(final WalRestoreSpec spec) {
        String owner = spec.replica() == null ? null : spec.replica().fingerprint();
        if (engineId != null && (!engineId.equals(spec.engineId()) || !shard.equals(spec.shard()) || !Objects.equals(replicaOwner, owner))) {
            throw new IllegalStateException("restore store already bound");
        }
        try (Connection connection = dataSource.getConnection()) {
            createTables(connection);
            configurePacketBudget(connection);
            connection.setAutoCommit(false);
            try {
                boolean exists;
                try (var query = connection.createStatement(); var row = query.executeQuery("select id from azir_wal_binding where id=1 for update")) {
                    exists = row.next();
                }
                if (!exists) {
                    try (var query = connection.createStatement(); var row = query.executeQuery("select count(*) from azir_wal_version")) {
                        row.next();
                        if (row.getLong(1) != 0) {
                            throw new SQLException("restore target contains orphan data");
                        }
                    }
                    try (
                            var insert = connection.prepareStatement("insert into azir_wal_binding "
                                    + "(id,engine_id,shard_id,applied_seq,digest,restore_id,restoring,replica_owner) values (1,?,?,0,'',?,1,?)")) {
                        insert.setString(1, spec.engineId().toString());
                        insert.setBytes(2, spec.shard().getBytes(StandardCharsets.UTF_8));
                        insert.setString(3, restoreFingerprint(spec));
                        insert.setString(4, owner);
                        insert.executeUpdate();
                    }
                }
                boolean importing = verifyRestoreBinding(connection, spec);
                connection.commit();
                engineId = spec.engineId();
                shard = spec.shard();
                replicaOwner = owner;
                return importing;
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            }
        } catch (final SQLException failure) {
            throw storage("begin backend restore", failure);
        }
    }

    /**
     * Batch Imports the latest version with a fixed identity, and retry only overwrites uncompleted recovery space from the same backup.
     *
     * @param spec spec
     * @param rows rows
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public void restoreBatch(final WalRestoreSpec spec, final List<WalBackupRow> rows) {
        if (rows.isEmpty() || rows.size() > 1024) {
            throw new IllegalArgumentException("invalid restore batch size");
        }
        long total = 0;
        for (var row : rows) {
            if (row.version().commitSequence() > spec.boundary().sequence()) {
                throw new IllegalArgumentException("restore version exceeds boundary");
            }
            var mutation = new WalMutation(spec.shard(), row.key(), row.version().value());
            validateMutation(mutation);
            total += mutation.retainedBytes();
        }
        if (total > 16L * 1024 * 1024) {
            throw new IllegalArgumentException("restore batch exceeds memory budget");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (!verifyRestoreBinding(connection, spec)) {
                    connection.commit();
                    return;
                }
                try (
                        var insert = connection.prepareStatement("insert into azir_wal_version (user_key,commit_seq,payload) values (?,?,?) "
                                + "as incoming on duplicate key update payload=incoming.payload")) {
                    long bytes = 0;
                    for (var row : rows) {
                        var mutation = new WalMutation(spec.shard(), row.key(), row.version().value());
                        long size = estimatedPacketBytes(mutation);
                        if (bytes > 0 && bytes + size > packetBudget) {
                            insert.executeBatch();
                            insert.clearBatch();
                            bytes = 0;
                        }
                        insert.setBytes(1, row.key().getBytes(StandardCharsets.UTF_8));
                        insert.setLong(2, row.version().commitSequence());
                        insert.setBytes(3, row.version().value());
                        insert.addBatch();
                        bytes += size;
                    }
                    insert.executeBatch();
                }
                connection.commit();
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            }
        } catch (final SQLException failure) {
            throw storage("import backend restore batch", failure);
        }
    }

    /**
     * Streaming double-counts the complete physical data summary, and only applies prefixes and removes recovery status when fully matched.
     *
     * @param spec spec
     * @param expectedRows expected rows
     * @param expectedDataHash expected data hash
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public void finishRestore(final WalRestoreSpec spec, final long expectedRows, final String expectedDataHash) {
        if (expectedRows < 0 || !expectedDataHash.matches("[a-f0-9]{64}")) {
            throw new IllegalArgumentException("invalid restore verification");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                boolean importing = verifyRestoreBinding(connection, spec);
                var digest = java.security.MessageDigest.getInstance("SHA-256");
                var output = new java.security.DigestOutputStream(java.io.OutputStream.nullOutputStream(), digest);
                var writer = new WalBackupData.Writer(output, new WalBackupData.Header(spec.engineId(), spec.shard(), spec.boundary()));
                try (var query = connection.prepareStatement("select user_key,commit_seq,payload from azir_wal_version order by user_key,commit_seq")) {
                    query.setFetchSize(Integer.MIN_VALUE);
                    try (var row = query.executeQuery()) {
                        while (row.next()) {
                            writer.accept(backupRow(row));
                        }
                    }
                }
                long actualRows = writer.finish();
                String actualHash = java.util.HexFormat.of().formatHex(digest.digest());
                if (actualRows != expectedRows || !actualHash.equals(expectedDataHash)) {
                    throw new SQLException("restored backend data checksum mismatch");
                }
                if (importing) {
                    try (var update = connection.prepareStatement("update azir_wal_binding set applied_seq=?,digest=?,restoring=0 where id=1 and restore_id=?")) {
                        update.setLong(1, spec.boundary().sequence());
                        update.setString(2, spec.boundary().digest());
                        update.setString(3, restoreFingerprint(spec));
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("restore authority changed");
                        }
                    }
                }
                connection.commit();
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            }
        } catch (final SQLException | java.io.IOException | java.security.NoSuchAlgorithmException failure) {
            throw storage("verify and finish backend restore", new SQLException(failure));
        }
    }

    /**
     * JDBC flowline conversion retains UTF-8, version and tombstone, illegal content rejected in backup format.
     *
     * @param row row
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static WalBackupRow backupRow(final ResultSet row) throws SQLException {
        return new WalBackupRow(decodeKey(row.getBytes(1)), new WalValue(row.getLong(2), row.getBytes(3)));
    }

    /**
     * An illegal back-end byte cannot be replaced with another business key.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static String decodeKey(final byte[] bytes) throws SQLException {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (final java.nio.charset.CharacterCodingException failure) {
            throw new SQLException("invalid UTF-8 in materialized key", failure);
        }
    }

    /**
     * Recovery Fingerprints cover all identity fields and cannot be continued with the same backup number but different commit borders.
     *
     * @param spec spec
     * @return the operation result
     */
    private static String restoreFingerprint(final WalRestoreSpec spec) {
        String plain = String.join("\u0000", spec.backupId().toString(), spec.operationId().toString(), spec.engineId().toString(), spec.shard(),
                Long.toString(spec.boundary().sequence()), spec.boundary().digest());
        if (spec.replica() != null) {
            plain += "\u0000" + spec.replica().fingerprint();
        }
        return WalCodec.digest(plain.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Lock the recovery status line and check for fixed identity; new commit storage after completion cannot be backed up by old rollback.
     *
     * @param connection connection
     * @param spec spec
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean verifyRestoreBinding(final Connection connection, final WalRestoreSpec spec) throws SQLException {
        try (
                var query = connection.prepareStatement("select engine_id,shard_id,applied_seq,digest,restore_id,restoring,replica_owner from azir_wal_binding where id=1 for update");
                var row = query.executeQuery()) {
            if (!row.next() || !spec.engineId().toString().equals(row.getString(1))
                    || !spec.shard().equals(new String(row.getBytes(2), StandardCharsets.UTF_8)) || !restoreFingerprint(spec).equals(row.getString(5))
                    || !Objects.equals(spec.replica() == null ? null : spec.replica().fingerprint(), row.getString(7))) {
                throw new SQLException("restore target is occupied or belongs to another backup");
            }
            boolean importing = row.getBoolean(6);
            long sequence = row.getLong(3);
            String digest = row.getString(4);
            if (importing ? sequence != 0 || !digest.isEmpty()
                    : sequence != spec.boundary().sequence() || !digest.equals(spec.boundary().digest())) {
                throw new SQLException("restore progress or completed prefix changed");
            }
            return importing;
        }
    }

    /**
     * Locks the metadata line at current connection check for fixed storage identity, when writing and collection.
     *
     * @param connection connection
     * @param engineId engine id
     * @param shard logical shard identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private WalCheckpoint verifyBinding(final Connection connection, final UUID engineId, final String shard, final boolean lock) throws SQLException {
        return verifyBinding(connection, engineId, shard, lock, false);
    }

    /**
     * Prefix confirmation with lock commit, connection cannot see unx2 materialization progress even if configured to RU.
     *
     * @param connection connection
     * @param engineId engine id
     * @param shard logical shard identifier
     * @param lock lock
     * @param committedRead committed read
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private WalCheckpoint verifyBinding(final Connection connection, final UUID engineId, final String shard, final boolean lock, final boolean committedRead) throws SQLException {
        if (engineId == null || shard == null) {
            throw new SQLException("store not initialized");
        }
        try (
                PreparedStatement query = connection.prepareStatement("select engine_id,shard_id,applied_seq,digest,restoring,replica_owner "
                        + "from azir_wal_binding where id=1" + (lock ? " for update" : committedRead ? " for share" : ""));
                ResultSet row = query.executeQuery()) {
            if (!row.next() || !engineId.toString().equals(row.getString(1))
                    || !shard.equals(new String(row.getBytes(2), StandardCharsets.UTF_8)) || row.getBoolean(5) || !Objects.equals(replicaOwner, row.getString(6))) {
                throw new SQLException("WAL identity or shard binding mismatch");
            }
            return new WalCheckpoint(row.getLong(3), row.getString(4));
        }
    }

    /**
     * Converts a JDBC failure to a new mode anomaly without modifying an old pattern error map.
     *
     * @param operation operation
     * @param cause cause
     * @return the operation result
     */
    private static WalMvccException storage(final String operation, final SQLException cause) {
        return new WalMvccException(ErrorCode.STORAGE_UNAVAILABLE, operation + " failed", null, cause);
    }

    /**
     * This export is a single connection and binds the query and data query to the same RR Consistency view.
     */
    private final class MysqlSnapshotView implements SnapshotView {

        private final Connection connection;

        private final WalCheckpoint boundary;

        private boolean exported, closed;

        /**
         * The transaction reading view has been created by the opening phase and cannot be reopened here.
         *
         * @param connection connection
         * @param boundary boundary
         */
        MysqlSnapshotView(final Connection connection, final WalCheckpoint boundary) {
            this.connection = connection;
            this.boundary = boundary;
        }

        /**
         * Then commit can advance the binding table, and this view can only see prefixes when they are open.
         *
         * @param sink sink
         * @throws java.io.IOException if the operation cannot complete
         */
        @Override
        public synchronized void export(final BackupSink sink) throws java.io.IOException {
            if (closed || exported) {
                throw new java.io.IOException("materialized snapshot already consumed");
            }
            exported = true;
            String sql = "select v.user_key,v.commit_seq,v.payload from azir_wal_version v join "
                    + "(select user_key,max(commit_seq) seq from azir_wal_version where commit_seq<=? group by user_key) a "
                    + "on v.user_key=a.user_key and v.commit_seq=a.seq order by v.user_key";
            try (var query = connection.prepareStatement(sql)) {
                query.setFetchSize(Integer.MIN_VALUE);
                query.setLong(1, boundary.sequence());
                try (var row = query.executeQuery()) {
                    while (row.next()) {
                        sink.accept(backupRow(row));
                    }
                }
            } catch (final SQLException failure) {
                throw storage("export materialized backup", failure);
            }
        }

        /**
         * Rollback read-only transaction finished reading view, and connection closed abnormalities cannot continue using this object.
         *
         * @throws java.io.IOException if the operation cannot complete
         */
        @Override
        public synchronized void close() throws java.io.IOException {
            if (closed) {
                return;
            }
            closed = true;
            SQLException failure = null;
            try {
                connection.rollback();
            } catch (final SQLException rollback) {
                failure = rollback;
            }
            try {
                connection.close();
            } catch (final SQLException close) {
                if (failure == null) {
                    failure = close;
                } else {
                    failure.addSuppressed(close);
                }
            }
            if (failure != null) {
                throw storage("close materialized snapshot", failure);
            }
        }
    }
}

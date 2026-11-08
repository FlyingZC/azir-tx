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

package com.flyingzc.azir.wal.ha;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The X-0 registration and restricted creation of MySQL space is not clear, and the loss of readiness space is not considered a new initialization.
 */
final class MysqlReplicaNamespaces {

    private static final String CATALOG = "azir_wal_generation_catalog";

    private static final String BUDGET = "azir_wal_namespace_budget";

    private static final String MARKER = "azir_wal_generation";

    private final HikariDataSource control;

    private final MysqlWalReplicaBackendFactory.Options options;

    private final MysqlWalReplicaBackendFactory.Hook hook;

    /**
     * All SQLs serve only to control explicitly the library and the fixed names that are derived from it.
     *
     * @param control control
     * @param options options
     * @param hook hook
     */
    MysqlReplicaNamespaces(final HikariDataSource control, final MysqlWalReplicaBackendFactory.Options options, final MysqlWalReplicaBackendFactory.Hook hook) {
        this.control = control;
        this.options = options;
        this.hook = hook;
    }

    /**
     * Register before database is created, which can be continued with idempotent; only checking after READY does not recreate the lost library.
     *
     * @param identity identity
     * @param generation generation
     * @param shard logical shard identifier
     * @param schema schema
     * @throws IOException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    void provision(final WalReplicaIdentity identity, final UUID generation, final String shard, final String schema) throws IOException {
        String owner = MysqlWalReplicaBackendFactory.owner(identity, generation, shard, schema);
        try (var connection = control.getConnection()) {
            connection.setAutoCommit(true);
            createControlTables(connection);
            int state = reserve(connection, identity, generation, shard, schema, owner);
            hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_RESERVATION);
            boolean exists = databaseExists(connection, schema);
            if (state == 1) {
                if (!exists) {
                    throw new SQLException("ready generation database missing; snapshot recovery required");
                }
                validateTarget(connection, schema, owner, false);
                return;
            }
            if (!exists) {
                try (var statement = connection.createStatement()) {
                    statement.setQueryTimeout(5);
                    statement.executeUpdate("create database `" + schema + "` character set utf8mb4 collate utf8mb4_bin");
                } catch (final SQLException race) {
                    if (race.getErrorCode() != 1007 || !databaseExists(connection, schema)) {
                        throw race;
                    }
                }
            }
            hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_CREATE_DATABASE);
            Set<String> tables = tables(connection, schema);
            if (!tables.isEmpty() && !tables.equals(Set.of(MARKER))) {
                throw new SQLException("reserved generation contains unknown data");
            }
            try (var ddl = connection.createStatement()) {
                ddl.setQueryTimeout(5);
                ddl.executeUpdate("create table if not exists `" + schema + "`." + MARKER
                        + " (id tinyint not null primary key,protocol_version int not null,owner char(64) character set ascii collate ascii_bin not null) engine=InnoDB");
            }
            validateMarkerTable(connection, schema);
            connection.setAutoCommit(false);
            try {
                try (var insert = connection.prepareStatement("insert ignore into `" + schema + "`." + MARKER + " (id,protocol_version,owner) values (1,1,?)")) {
                    insert.setString(1, owner);
                    insert.executeUpdate();
                }
                marker(connection, schema, owner);
                connection.commit();
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            } finally {
                connection.setAutoCommit(true);
            }
            hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_MARKER);
            finish(connection, identity, generation, shard, schema, owner);
            hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_READY);
        } catch (final SQLException failure) {
            throw new IOException("provision MySQL generation namespace failed", failure);
        }
    }

    /**
     * Fixed control sheet verify strict, error engine, column and only constraint cannot be masked by IF NOT EXISTS.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void createControlTables(final Connection connection) throws SQLException {
        try (var ddl = connection.createStatement()) {
            ddl.setQueryTimeout(5);
            ddl.executeUpdate(
                    "create table if not exists " + BUDGET + " (prefix varbinary(8) not null primary key,protocol_version int not null,max_slots int not null,used_slots int not null) engine=InnoDB");
            ddl.executeUpdate("create table if not exists " + CATALOG + " (schema_name varbinary(64) not null primary key,prefix varbinary(8) not null,"
                    + "node_owner char(64) character set ascii collate ascii_bin not null,generation char(36) character set ascii collate ascii_bin not null,"
                    + "shard_id varbinary(128) not null,owner char(64) character set ascii collate ascii_bin not null,state tinyint not null,"
                    + "key scope (prefix,node_owner,shard_id,state)) engine=InnoDB");
        }
        String schema = currentDatabase(connection);
        validateTable(connection, schema, BUDGET, Map.of("prefix", "varbinary:8", "protocol_version", "int", "max_slots", "int", "used_slots", "int"), List.of("prefix"));
        validateTable(connection, schema, CATALOG,
                Map.of("schema_name", "varbinary:64", "prefix", "varbinary:8", "node_owner", "char:64", "generation", "char:36", "shard_id", "varbinary:128", "owner", "char:64", "state", "tinyint"),
                List.of("schema_name"));
    }

    /**
     * The amount set aside for the serials in the control library is not registered and database has refused to take over even if it is empty.
     *
     * @param connection connection
     * @param identity identity
     * @param generation generation
     * @param shard logical shard identifier
     * @param schema schema
     * @param owner owner
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int reserve(final Connection connection, final WalReplicaIdentity identity, final UUID generation, final String shard, final String schema, final String owner) throws SQLException {
        connection.setAutoCommit(false);
        try {
            int used = lockBudget(connection);
            Integer existing = catalog(connection, identity, generation, shard, schema, owner);
            if (existing != null) {
                if (existing >= 2) {
                    throw new SQLException("generation is retiring or retired");
                }
                connection.commit();
                return existing;
            }
            if (databaseExists(connection, schema)) {
                throw new SQLException("unregistered generation database already exists");
            }
            try (var query = connection.prepareStatement("select count(*) from " + CATALOG + " where prefix=? and node_owner=? and shard_id=? and state<>3")) {
                query.setBytes(1, bytes(options.prefix()));
                query.setString(2, MysqlWalReplicaBackendFactory.nodeOwner(identity));
                query.setBytes(3, bytes(shard));
                try (var row = query.executeQuery()) {
                    row.next();
                    if (row.getInt(1) >= options.maxGenerationsPerShard()) {
                        throw new SQLException("generation count budget exhausted");
                    }
                }
            }
            if (used >= options.maxSchemasPerControl() || catalogSize(connection) >= 65_536) {
                throw new SQLException("namespace schema or tombstone budget exhausted");
            }
            try (var insert = connection.prepareStatement("insert into " + CATALOG + " (schema_name,prefix,node_owner,generation,shard_id,owner,state) values (?,?,?,?,?,?,0)")) {
                insert.setBytes(1, bytes(schema));
                insert.setBytes(2, bytes(options.prefix()));
                insert.setString(3, MysqlWalReplicaBackendFactory.nodeOwner(identity));
                insert.setString(4, generation.toString());
                insert.setBytes(5, bytes(shard));
                insert.setString(6, owner);
                insert.executeUpdate();
            }
            try (var update = connection.prepareStatement("update " + BUDGET + " set used_slots=used_slots+1 where prefix=?")) {
                update.setBytes(1, bytes(options.prefix()));
                if (update.executeUpdate() != 1) {
                    throw new SQLException("namespace budget authority missing");
                }
            }
            connection.commit();
            return 0;
        } catch (final Throwable failure) {
            connection.rollback();
            throw failure;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    /**
     * Complete registration does not automatically change the amount, and recovery interrupts duplicate completion without double counting.
     *
     * @param connection connection
     * @param identity identity
     * @param generation generation
     * @param shard logical shard identifier
     * @param schema schema
     * @param owner owner
     * @throws SQLException if the operation cannot complete
     */
    private void finish(final Connection connection, final WalReplicaIdentity identity, final UUID generation, final String shard, final String schema, final String owner) throws SQLException {
        connection.setAutoCommit(false);
        try {
            lockBudget(connection);
            Integer state = catalog(connection, identity, generation, shard, schema, owner);
            if (state == null) {
                throw new SQLException("generation reservation missing");
            }
            if (state >= 2) {
                throw new SQLException("cannot finish a retired reservation");
            }
            if (state == 0) {
                try (var update = connection.prepareStatement("update " + CATALOG + " set state=1 where schema_name=? and owner=? and state=0")) {
                    update.setBytes(1, bytes(schema));
                    update.setString(2, owner);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("generation reservation changed");
                    }
                }
            }
            connection.commit();
        } catch (final Throwable failure) {
            connection.rollback();
            throw failure;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    /**
     * Each prefix budget line acts as a distribution cross-repeated, strictly matching the configuration version and the amount used.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int lockBudget(final Connection connection) throws SQLException {
        // Get an exclusionary lock and avoid multiple INSERT Ignoré shared locks being upgraded to death locks.
        try (var insert = connection.prepareStatement("insert into " + BUDGET + " (prefix,protocol_version,max_slots,used_slots) values (?,2,?,0) on duplicate key update prefix=prefix")) {
            insert.setBytes(1, bytes(options.prefix()));
            insert.setInt(2, options.maxSchemasPerControl());
            insert.executeUpdate();
        }
        int used;
        int version;
        try (var query = connection.prepareStatement("select protocol_version,max_slots,used_slots from " + BUDGET + " where prefix=? for update")) {
            query.setBytes(1, bytes(options.prefix()));
            try (var row = query.executeQuery()) {
                if (!row.next() || row.getInt(1) < 1 || row.getInt(1) > 2 || row.getInt(2) != options.maxSchemasPerControl() || row.getInt(3) < 0 || row.getInt(3) > row.getInt(2)) {
                    throw new SQLException("namespace budget configuration mismatch");
                }
                used = row.getInt(3);
                version = row.getInt(1);
            }
        }
        try (var query = connection.prepareStatement("select count(*) from " + CATALOG + " where prefix=? and state<>3")) {
            query.setBytes(1, bytes(options.prefix()));
            try (var row = query.executeQuery()) {
                row.next();
                if (row.getInt(1) != used) {
                    throw new SQLException("namespace budget count mismatch");
                }
            }
        }
        if (version == 1) {
            try (var update = connection.prepareStatement("update " + BUDGET + " set protocol_version=2 where prefix=?")) {
                update.setBytes(1, bytes(options.prefix()));
                update.executeUpdate();
            }
        }
        return used;
    }

    /**
     * Short name collisions, error nodes, errors generation or abnormal state must be rejected without using the overwhelm update.
     *
     * @param connection connection
     * @param identity identity
     * @param generation generation
     * @param shard logical shard identifier
     * @param schema schema
     * @param owner owner
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Integer catalog(final Connection connection, final WalReplicaIdentity identity, final UUID generation, final String shard, final String schema, final String owner) throws SQLException {
        try (var query = connection.prepareStatement("select prefix,node_owner,generation,shard_id,owner,state from " + CATALOG + " where schema_name=? for update")) {
            query.setBytes(1, bytes(schema));
            try (var row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                if (!Arrays.equals(bytes(options.prefix()), row.getBytes(1)) || !MysqlWalReplicaBackendFactory.nodeOwner(identity).equals(row.getString(2))
                        || !generation.toString().equals(row.getString(3)) || !Arrays.equals(bytes(shard), row.getBytes(4)) || !owner.equals(row.getString(5)) || row.getInt(6) < 0
                        || row.getInt(6) > 3) {
                    throw new SQLException("generation catalog identity mismatch");
                }
                return row.getInt(6);
            }
        }
    }

    /**
     * Retirement First durable to unopenable, then DDL will be completed before the atomic release amount is released and identity tombstone will remain permanently.
     *
     * @param permit permit
     * @param shard logical shard identifier
     * @param schema schema
     * @param retirementHook retirement hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    boolean retire(final WalGenerationRetirement permit, final String shard, final String schema, final MysqlWalReplicaBackendFactory.RetirementHook retirementHook) throws IOException {
        permit.verify();
        var identity = permit.identity();
        var generation = permit.generation();
        String owner = MysqlWalReplicaBackendFactory.owner(identity, generation, shard, schema);
        try (var connection = control.getConnection()) {
            connection.setAutoCommit(true);
            createControlTables(connection);
            connection.setAutoCommit(false);
            Integer state;
            try {
                lockBudget(connection);
                state = catalog(connection, identity, generation, shard, schema, owner);
                if (state == null) {
                    if (databaseExists(connection, schema) || catalogSize(connection) >= 65_536) {
                        throw new SQLException("unknown namespace or tombstone budget exhausted");
                    }
                    try (var insert = connection.prepareStatement("insert into " + CATALOG + " (schema_name,prefix,node_owner,generation,shard_id,owner,state) values (?,?,?,?,?,?,3)")) {
                        insert.setBytes(1, bytes(schema));
                        insert.setBytes(2, bytes(options.prefix()));
                        insert.setString(3, MysqlWalReplicaBackendFactory.nodeOwner(identity));
                        insert.setString(4, generation.toString());
                        insert.setBytes(5, bytes(shard));
                        insert.setString(6, owner);
                        insert.executeUpdate();
                    }
                    connection.commit();
                    return false;
                }
                if (state == 3) {
                    if (databaseExists(connection, schema)) {
                        throw new SQLException("retired namespace was externally recreated");
                    }
                    connection.commit();
                    return false;
                }
                if (databaseExists(connection, schema)) {
                    retirementOwnership(connection, schema, identity, shard, owner, state);
                }
                if (state < 2) {
                    try (var update = connection.prepareStatement("update " + CATALOG + " set state=2 where schema_name=? and owner=?")) {
                        update.setBytes(1, bytes(schema));
                        update.setString(2, owner);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("retirement authority changed");
                        }
                    }
                }
                connection.commit();
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            } finally {
                connection.setAutoCommit(true);
            }
            retirementHook.hit(MysqlWalReplicaBackendFactory.RetirementStage.AFTER_RETIRING);
            permit.verify();
            boolean dropped = databaseExists(connection, schema);
            if (dropped) {
                retirementOwnership(connection, schema, identity, shard, owner, 2);
                permit.verify();
                try (var ddl = connection.createStatement()) {
                    ddl.setQueryTimeout(5);
                    ddl.executeUpdate("drop database `" + schema + "`");
                }
            }
            retirementHook.hit(MysqlWalReplicaBackendFactory.RetirementStage.AFTER_DATABASE_DROP);
            permit.verify();
            connection.setAutoCommit(false);
            try {
                lockBudget(connection);
                state = catalog(connection, identity, generation, shard, schema, owner);
                if (state == null || state < 2 || databaseExists(connection, schema)) {
                    throw new SQLException("namespace retirement is not complete");
                }
                if (state == 2) {
                    try (var update = connection.prepareStatement("update " + CATALOG + " set state=3 where schema_name=? and owner=? and state=2")) {
                        update.setBytes(1, bytes(schema));
                        update.setString(2, owner);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("retirement progress changed");
                        }
                    }
                    try (var update = connection.prepareStatement("update " + BUDGET + " set used_slots=used_slots-1 where prefix=? and used_slots>0")) {
                        update.setBytes(1, bytes(options.prefix()));
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("retirement budget underflow");
                        }
                    }
                }
                connection.commit();
            } catch (final Throwable failure) {
                connection.rollback();
                throw failure;
            } finally {
                connection.setAutoCommit(true);
            }
            retirementHook.hit(MysqlWalReplicaBackendFactory.RetirementStage.AFTER_RELEASE);
            return dropped;
        } catch (final SQLException failure) {
            throw new IOException("retire MySQL namespace failed", failure);
        }
    }

    /**
     * Confirming fixed ownership before deleting; permission to continue cleaning the list of protocols left behind after durable RETIRING.
     *
     * @param connection connection
     * @param schema schema
     * @param identity identity
     * @param shard logical shard identifier
     * @param owner owner
     * @param state state
     * @throws SQLException if the operation cannot complete
     */
    private static void retirementOwnership(final Connection connection, final String schema, final WalReplicaIdentity identity, final String shard, final String owner,
                                            final int state) throws SQLException {
        Set<String> existing = tables(connection, schema);
        if (!Set.of(MARKER, "azir_wal_binding", "azir_wal_version").containsAll(existing)) {
            throw new SQLException("retirement target contains unknown tables");
        }
        if (existing.contains(MARKER)) {
            validateMarkerTable(connection, schema);
            marker(connection, schema, owner);
        } else if (state == 1 || state == 0 && !existing.isEmpty()) {
            throw new SQLException("retirement target ownership marker missing");
        }
        if (existing.contains("azir_wal_binding")) {
            try (var query = connection.createStatement(); var row = query.executeQuery("select engine_id,shard_id,replica_owner from `" + schema + "`.azir_wal_binding")) {
                if (row.next() && (!identity.engineId().toString().equals(row.getString(1)) || !Arrays.equals(bytes(shard), row.getBytes(2))
                        || !MysqlWalReplicaBackendFactory.nodeOwner(identity).equals(row.getString(3)) || row.next())) {
                    throw new SQLException("retirement protocol binding mismatch");
                }
            }
        }
    }

    /**
     * History tombstone also has a total ceiling and cannot increase control metadata indefinitely by releasing database levels.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int catalogSize(final Connection connection) throws SQLException {
        try (var query = connection.prepareStatement("select count(*) from " + CATALOG + " where prefix=?")) {
            query.setBytes(1, bytes(options.prefix()));
            try (var row = query.executeQuery()) {
                row.next();
                return row.getInt(1);
            }
        }
    }

    /**
     * READY space is verify and does not fill the tags; two initialized transaction tables are available, other tables reject.
     *
     * @param connection connection
     * @param schema schema
     * @param owner owner
     * @param reserved reserved
     * @throws SQLException if the operation cannot complete
     */
    private static void validateTarget(final Connection connection, final String schema, final String owner, final boolean reserved) throws SQLException {
        Set<String> tables = tables(connection, schema);
        Set<String> allowed = reserved ? Set.of(MARKER) : Set.of(MARKER, "azir_wal_binding", "azir_wal_version");
        if (!tables.contains(MARKER) || !allowed.containsAll(tables)) {
            throw new SQLException("generation marker missing or unknown data present");
        }
        if (tables.contains("azir_wal_binding") != tables.contains("azir_wal_version")) {
            String remaining = tables.contains("azir_wal_binding") ? "azir_wal_binding" : "azir_wal_version";
            try (var query = connection.createStatement(); var row = query.executeQuery("select 1 from `" + schema + "`." + remaining + " limit 1")) {
                if (row.next()) {
                    throw new SQLException("initialized generation protocol table pair incomplete");
                }
            }
        }
        validateMarkerTable(connection, schema);
        marker(connection, schema, owner);
    }

    /**
     * The pool actually selects the target before checking identity to prevent the catalog selection or the pool template from initializing SQL in the wrong space.
     *
     * @param connection connection
     * @param schema schema
     * @param owner owner
     * @throws SQLException if the operation cannot complete
     */
    static void verifyOpened(final Connection connection, final String schema, final String owner) throws SQLException {
        validateTarget(connection, schema, owner, false);
    }

    /**
     * Identity Marked structures must be complete and use the transaction engine.
     *
     * @param connection connection
     * @param schema schema
     * @throws SQLException if the operation cannot complete
     */
    private static void validateMarkerTable(final Connection connection, final String schema) throws SQLException {
        validateTable(connection, schema, MARKER, Map.of("id", "tinyint", "protocol_version", "int", "owner", "char:64"), List.of("id"));
    }

    /**
     * The mark can only be one row, and original identity cannot be replaced once it has been published.
     *
     * @param connection connection
     * @param schema schema
     * @param owner owner
     * @throws SQLException if the operation cannot complete
     */
    private static void marker(final Connection connection, final String schema, final String owner) throws SQLException {
        try (var query = connection.createStatement(); var row = query.executeQuery("select id,protocol_version,owner from `" + schema + "`." + MARKER)) {
            if (!row.next() || row.getInt(1) != 1 || row.getInt(2) != 1 || !owner.equals(row.getString(3)) || row.next()) {
                throw new SQLException("generation database owner mismatch");
            }
        }
    }

    /**
     * The metadata accurately match schema by binary. Another database cannot be misidentified by case or wildcard.
     *
     * @param connection connection
     * @param schema schema
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean databaseExists(final Connection connection, final String schema) throws SQLException {
        try (var query = connection.prepareStatement("select 1 from information_schema.schemata where cast(schema_name as binary)=?")) {
            query.setBytes(1, bytes(schema));
            try (var row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * A limited list of objects in this namespace, views and unknown objects are also rejected.
     *
     * @param connection connection
     * @param schema schema
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Set<String> tables(final Connection connection, final String schema) throws SQLException {
        var result = new HashSet<String>();
        try (var query = connection.prepareStatement("select table_name from information_schema.tables where cast(table_schema as binary)=? limit 5")) {
            query.setBytes(1, bytes(schema));
            try (var row = query.executeQuery()) {
                while (row.next()) {
                    result.add(row.getString(1));
                }
            }
        }
        return result;
    }

    /**
     * Control connection must clearly select an database and cannot create metadata on the default empty catalog.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static String currentDatabase(final Connection connection) throws SQLException {
        try (var query = connection.createStatement(); var row = query.executeQuery("select database()")) {
            if (!row.next() || row.getString(1) == null) {
                throw new SQLException("namespace control database required");
            }
            return row.getString(1);
        }
    }

    /**
     * Strictly start validate for engines, precise columns, lengths, character sets, empty values and primary keys.
     *
     * @param connection connection
     * @param schema schema
     * @param table table
     * @param expected expected
     * @param primary original Percolator primary identity
     * @throws SQLException if the operation cannot complete
     */
    private static void validateTable(final Connection connection, final String schema, final String table, final Map<String, String> expected, final List<String> primary) throws SQLException {
        try (var query = connection.prepareStatement("select engine,table_type from information_schema.tables where cast(table_schema as binary)=? and cast(table_name as binary)=?")) {
            query.setBytes(1, bytes(schema));
            query.setBytes(2, bytes(table));
            try (var row = query.executeQuery()) {
                if (!row.next() || !"InnoDB".equals(row.getString(1)) || !"BASE TABLE".equals(row.getString(2))) {
                    throw new SQLException("namespace table must be InnoDB");
                }
            }
        }
        var actual = new HashMap<String, String>();
        try (
                var query = connection.prepareStatement(
                        "select column_name,data_type,character_maximum_length,is_nullable,character_set_name,collation_name,extra,column_type from information_schema.columns where cast(table_schema as binary)=? and cast(table_name as binary)=?")) {
            query.setBytes(1, bytes(schema));
            query.setBytes(2, bytes(table));
            try (var row = query.executeQuery()) {
                while (row.next()) {
                    String type = row.getString(2);
                    if (!"NO".equals(row.getString(4)) || !row.getString(7).isEmpty() || row.getString(8).contains("unsigned")) {
                        throw new SQLException("namespace columns must be non-null, signed and ordinary");
                    }
                    if (type.equals("char")) {
                        if (!"ascii".equals(row.getString(5)) || !"ascii_bin".equals(row.getString(6))) {
                            throw new SQLException("namespace identity must use binary ASCII");
                        }
                    }
                    actual.put(row.getString(1), type + (type.equals("char") || type.equals("varbinary") ? ":" + row.getLong(3) : ""));
                }
            }
        }
        if (!actual.equals(expected)) {
            throw new SQLException("namespace table columns mismatch");
        }
        var keys = new ArrayList<String>();
        try (
                var query = connection.prepareStatement(
                        "select index_name,column_name,non_unique,sub_part from information_schema.statistics where cast(table_schema as binary)=? and cast(table_name as binary)=? order by index_name,seq_in_index")) {
            query.setBytes(1, bytes(schema));
            query.setBytes(2, bytes(table));
            try (var row = query.executeQuery()) {
                while (row.next()) {
                    if (row.getString(1).equals("PRIMARY")) {
                        if (row.getObject(4) != null) {
                            throw new SQLException("namespace prefix primary key unsupported");
                        }
                        keys.add(row.getString(2));
                    } else if (!row.getBoolean(3)) {
                        throw new SQLException("unexpected namespace unique constraint");
                    }
                }
            }
        }
        if (!keys.equals(primary)) {
            throw new SQLException("namespace primary key mismatch");
        }
    }

    /**
     * All dynamic metadata parameters are entered by a fixed UTF-8 code, and the SQL identifier uses only plant-derived names.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

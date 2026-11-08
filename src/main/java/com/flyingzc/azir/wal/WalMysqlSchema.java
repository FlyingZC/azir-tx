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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A structural check of a new mode exclusive table to prevent the destruction of commit and snapshot syntaxes by non- transaction engines or error primary keys.
 */
final class WalMysqlSchema {

    /**
     * Prevent instantiation of this utility class.
     */
    private WalMysqlSchema() {
    }

    /**
     * Check the two already existing protocols transaction engines without first moving unsupported user lists.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void engines(final Connection connection) throws SQLException {
        var existing = new java.util.HashSet<String>();
        try (
                var query = connection.prepareStatement("select TABLE_NAME,ENGINE from information_schema.TABLES "
                        + "where cast(TABLE_SCHEMA as binary)=cast(database() as binary) and cast(TABLE_NAME as binary) "
                        + "in (cast('azir_wal_binding' as binary),cast('azir_wal_version' as binary))");
                var rows = query.executeQuery()) {
            while (rows.next()) {
                existing.add(rows.getString(1));
                if (!"InnoDB".equalsIgnoreCase(rows.getString(2))) {
                    throw new SQLException("WAL protocol tables require InnoDB");
                }
            }
        }
        // The missing version table represents the loss of cold data after the binding records have been published; the completed empty table will mask confirmed data.
        if (existing.contains("azir_wal_binding") && !existing.contains("azir_wal_version")) {
            try (var query = connection.createStatement(); var rows = query.executeQuery("select 1 from azir_wal_binding limit 1")) {
                if (rows.next()) {
                    throw new SQLException("initialized WAL backend version table missing; snapshot recovery required");
                }
            }
        }
    }

    /**
     * It is known that the old table can be increased by atomic to a pair of cursor fields, and semi-metadata sets are not guessed for fixes.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void migrateGc(final Connection connection) throws SQLException {
        var columns = columns(connection, "azir_wal_binding");
        boolean cursor = columns.containsKey("gc_cursor");
        boolean repeat = columns.containsKey("gc_repeat");
        if (cursor != repeat) {
            throw new SQLException("partial GC cursor schema");
        }
        if (!cursor) {
            try (var query = connection.createStatement()) {
                query.executeUpdate("alter table azir_wal_binding add column gc_cursor varbinary(512) null, add column gc_repeat int not null default 0");
            } catch (final SQLException race) {
                var changed = columns(connection, "azir_wal_binding");
                if (race.getErrorCode() != 1060 || !changed.containsKey("gc_cursor") || !changed.containsKey("gc_repeat")) {
                    throw race;
                }
            }
        }
    }

    /**
     * Only the realized field, length, empty semantics and primary keys are accepted; additional only bound changes version for semantics.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void validate(final Connection connection) throws SQLException {
        engines(connection);
        var binding = columns(connection, "azir_wal_binding");
        var version = columns(connection, "azir_wal_version");
        if (!binding.keySet().equals(Set.of("id", "engine_id", "shard_id", "applied_seq", "digest", "restore_id", "restoring", "gc_cursor", "gc_repeat", "replica_owner"))
                || !version.keySet().equals(Set.of("user_key", "commit_seq", "payload"))) {
            throw new SQLException("unexpected WAL protocol columns");
        }
        expect(binding, "id", "tinyint", 0, false);
        expect(binding, "engine_id", "char", 36, false);
        expect(binding, "shard_id", "varbinary", 128, false);
        expect(binding, "applied_seq", "bigint", 0, false);
        expect(binding, "digest", "char", 64, false);
        expect(binding, "restore_id", "char", 64, true);
        expect(binding, "restoring", "tinyint", 0, false);
        expect(binding, "gc_cursor", "varbinary", 512, true);
        expect(binding, "gc_repeat", "int", 0, false);
        expect(binding, "replica_owner", "char", 64, true);
        expect(version, "user_key", "varbinary", 512, false);
        expect(version, "commit_seq", "bigint", 0, false);
        expect(version, "payload", "longblob", 0, true);
        indexes(connection, "azir_wal_binding", List.of("id"));
        indexes(connection, "azir_wal_version", List.of("user_key", "commit_seq"));
    }

    /**
     * The common old table retains an empty replica ownership and only the initialisation of a visible reproduction is used to write node fingerprints.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void migrateReplica(final Connection connection) throws SQLException {
        if (!columns(connection, "azir_wal_binding").containsKey("replica_owner")) {
            try (var query = connection.createStatement()) {
                query.executeUpdate("alter table azir_wal_binding add column replica_owner char(64) character set ascii null");
            } catch (final SQLException race) {
                if (race.getErrorCode() != 1060 || !columns(connection, "azir_wal_binding").containsKey("replica_owner")) {
                    throw race;
                }
            }
        }
    }

    /**
     * Use a precise table name to query metadata and not use JDBC mode with underlined wildcards.
     *
     * @param connection connection
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Map<String, Column> columns(final Connection connection, final String table) throws SQLException {
        var result = new HashMap<String, Column>();
        try (
                var query = connection.prepareStatement("select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE,COLUMN_TYPE "
                        + "from information_schema.COLUMNS where cast(TABLE_SCHEMA as binary)=cast(database() as binary) and cast(TABLE_NAME as binary)=cast(? as binary)")) {
            query.setString(1, table);
            try (var rows = query.executeQuery()) {
                while (rows.next()) {
                    result.put(rows.getString(1), new Column(rows.getString(2), rows.getLong(3), "YES".equals(rows.getString(4)), rows.getString(5).contains("unsigned")));
                }
            }
        }
        return result;
    }

    /**
     * The value must have a symbol integer and the key must keep a binary sorting and full length.
     *
     * @param columns columns
     * @param name name
     * @param type type
     * @param length length
     * @param nullable nullable
     * @throws SQLException if the operation cannot complete
     */
    private static void expect(final Map<String, Column> columns, final String name, final String type, final long length, final boolean nullable) throws SQLException {
        var value = columns.get(name);
        if (value == null || !value.type().equals(type) || length != 0 && value.length() != length
                || value.nullable() != nullable || value.unsigned()) {
            throw new SQLException("unsupported WAL protocol column: " + name);
        }
    }

    /**
     * Validate main key order and full index column, rejecting prefix primary key and the unrealised only additional constraint.
     *
     * @param connection connection
     * @param table table
     * @param expected expected
     * @throws SQLException if the operation cannot complete
     */
    private static void indexes(final Connection connection, final String table, final List<String> expected) throws SQLException {
        var primary = new java.util.ArrayList<String>();
        try (
                var query = connection.prepareStatement("select INDEX_NAME,COLUMN_NAME,NON_UNIQUE,SUB_PART from information_schema.STATISTICS "
                        + "where cast(TABLE_SCHEMA as binary)=cast(database() as binary) and cast(TABLE_NAME as binary)=cast(? as binary) order by INDEX_NAME,SEQ_IN_INDEX")) {
            query.setString(1, table);
            try (var rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getString(1).equals("PRIMARY")) {
                        if (rows.getObject(4) != null) {
                            throw new SQLException("prefix primary key unsupported");
                        }
                        primary.add(rows.getString(2));
                    } else if (!rows.getBoolean(3)) {
                        throw new SQLException("additional unique constraint unsupported");
                    }
                }
            }
        }
        if (!primary.equals(expected)) {
            throw new SQLException("WAL primary key mismatch");
        }
    }

    /**
     * Fixed field description, without reading the business line contents.
     *
     * @param type type
     * @param length length
     * @param nullable nullable
     * @param unsigned unsigned
     */
    private record Column(String type, long length, boolean nullable, boolean unsigned) {
    }
}

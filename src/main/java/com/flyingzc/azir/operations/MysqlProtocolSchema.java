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

package com.flyingzc.azir.operations;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.sql.DataSource;

import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/**
 * Validate The transaction engine for the actual protocol table and the only constraint cannot be used as a structural acceptance for CREATE IF NOT EXISTS.
 */
public final class MysqlProtocolSchema {

    /**
     * Prohibits case-based layout checking tools.
     */
    private MysqlProtocolSchema() {
    }

    /**
     * Check all initialized protocol forms for fixed roles before the commencement of normal operations.
     *
     * @param source database connection source
     * @param prefix prefix
     * @param role role
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void requireRole(final DataSource source, final String prefix, final String role) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix) || !(role.equals("control") || role.matches("shard:[A-Za-z0-9_-]{1,64}"))) {
            throw new IllegalArgumentException("invalid protocol role");
        }
        requireTables(source, MysqlProtocolBackup.tables(prefix, role));
    }

    /**
     * Use the same short connection to check a set of known tables, do not scan business lines and do not automatically re-engineer existing engines.
     *
     * @param source database connection source
     * @param tables tables
     */
    public static void requireTables(final DataSource source, final List<String> tables) {
        try (var c = source.getConnection()) {
            for (String table : tables) {
                requireTable(c, table);
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Not accept view, non- transaction engine, error primary or missing percolator original only bounds.
     *
     * @param c c
     * @param table table
     * @throws SQLException if the operation cannot complete
     */
    static void requireTable(final Connection c, final String table) throws SQLException {
        requireTable(c, table, false);
    }

    /**
     * The old Percolator allows only that there are no Finished_at, and the remaining transaction engines, keys and arrays must have been established.
     *
     * @param c c
     * @param table table
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static void requireLegacyTable(final Connection c, final String table) throws SQLException {
        if (!table.matches("pc_(guard|data|lock|write|rollback)|mvcc_tso")) {
            throw new IllegalArgumentException("invalid legacy protocol table");
        }
        requireTable(c, table, true);
    }

    /**
     * The actual layout check is shared, and the controlled old version entry does not relax the business data column or primary key requirements.
     *
     * @param c c
     * @param table table
     * @param legacy legacy
     * @throws SQLException if the operation cannot complete
     */
    private static void requireTable(final Connection c, final String table, final boolean legacy) throws SQLException {
        List<String> expected = primaryKey(table);
        try (var q = c.prepareStatement("select engine,table_type from information_schema.tables where table_schema=database() and table_name=?")) {
            q.setString(1, table);
            try (var r = q.executeQuery()) {
                if (!r.next() || !"InnoDB".equalsIgnoreCase(r.getString(1)) || !"BASE TABLE".equals(r.getString(2))) {
                    throw new SQLException("protocol table requires InnoDB: " + table);
                }
            }
        }
        var indexes = new HashMap<String, SortedMap<Integer, String>>();
        try (
                var q = c.prepareStatement(
                        "select index_name,seq_in_index,column_name,sub_part,expression from information_schema.statistics where table_schema=database() and table_name=? and non_unique=0 order by index_name,seq_in_index")) {
            q.setString(1, table);
            try (var r = q.executeQuery()) {
                while (r.next()) {
                    // Prefix or function only index cannot replace a full protocol key, and the extra index does not change a valid write set.
                    if (r.getObject(4) != null || r.getString(5) != null || r.getString(3) == null) {
                        throw new SQLException("unsupported protocol unique index: " + table);
                    }
                    indexes.computeIfAbsent(r.getString(1), ignored -> new TreeMap<>()).put(r.getInt(2), r.getString(3));
                }
            }
        }
        var actual = indexes.get("PRIMARY");
        if (actual == null || !expected.equals(List.copyOf(actual.values()))) {
            throw new SQLException("protocol primary key differs: " + table);
        }
        boolean originalWrite = false;
        for (var entry : indexes.entrySet()) {
            if (entry.getKey().equals("PRIMARY")) {
                continue;
            }
            List<String> columns = List.copyOf(entry.getValue().values());
            if (table.equals("pc_write") && columns.equals(List.of("user_key", "start_ts"))) {
                originalWrite = true;
            } else if (!columns.containsAll(expected)) {
                throw new SQLException("additional unique constraint changes protocol semantics: " + table);
            }
        }
        if (table.equals("pc_write") && !originalWrite) {
            throw new SQLException("Percolator original write uniqueness is missing");
        }
        requireValueColumns(c, table, legacy);
    }

    /**
     * Keys must be compared by original bytes, payload cannot be reduced to text or timestamp not down to narrow integers or unsigned integers.
     *
     * @param c c
     * @param table table
     * @param legacy legacy
     * @throws SQLException if the operation cannot complete
     */
    private static void requireValueColumns(final Connection c, final String table, final boolean legacy) throws SQLException {
        Map<String, Integer> binary = switch (table) {
            case "pc_guard", "pc_data", "pc_write", "pc_rollback" -> Map.of("user_key", 512);
            case "pc_lock" -> Map.of("user_key", 512, "primary_key", 512);
            case "omid_keys", "atlas_keys" -> Map.of("row_key", 512);
            case "omid_versions", "atlas_versions" -> Map.of("row_key", 512, "col_key", 256);
            default -> Map.of();
        };
        var missing = new HashSet<>(binary.keySet());
        missing.addAll(counterColumns(table));
        if (legacy) {
            missing.remove("finished_at");
        }
        if (Set.of("pc_data", "omid_versions", "atlas_versions").contains(table)) {
            missing.add("payload");
        }
        if (table.endsWith("_keys") || table.endsWith("_versions")) {
            missing.add("table_name");
        }
        Set<String> counters = Set.of("start_ts", "commit_ts", "expires_at", "finished_at", "stamp", "shadow_ts", "cell_id", "seq", "current_value", "safe_point", "generation", "retired_before",
                "retention_ms", "retire_cursor", "pulse", "source_generation", "target_generation", "epoch", "updated_at");
        try (
                var q = c.prepareStatement(
                        "select column_name,data_type,column_type,character_maximum_length,is_nullable,collation_name,extra from information_schema.columns where table_schema=database() and table_name=?")) {
            q.setString(1, table);
            try (var r = q.executeQuery()) {
                while (r.next()) {
                    String name = r.getString(1), type = r.getString(2);
                    boolean nullable = "YES".equals(r.getString(5));
                    if (r.getString(7).toUpperCase(Locale.ROOT).contains("GENERATED") && !r.getString(7).toUpperCase(Locale.ROOT).contains("DEFAULT_GENERATED")) {
                        throw new SQLException("protocol columns cannot be generated: " + table);
                    }
                    if (binary.containsKey(name) && (!type.equals("varbinary") || r.getLong(4) < binary.get(name) || nullable)) {
                        throw new SQLException("protocol binary key layout differs: " + table + "." + name);
                    }
                    if (name.equals("payload") && (!type.equals("longblob") || nullable != table.equals("pc_data"))) {
                        throw new SQLException("protocol payload layout differs: " + table);
                    }
                    if (name.equals("table_name") && (!type.equals("varchar") || r.getLong(4) < 100 || !"ascii_bin".equals(r.getString(6)) || nullable)) {
                        throw new SQLException("protocol table key collation differs: " + table);
                    }
                    if (counters.contains(name) && (!type.equals("bigint") || r.getString(3).toLowerCase(Locale.ROOT).contains("unsigned") || nullable != name.equals("shadow_ts"))) {
                        throw new SQLException("protocol counter layout differs: " + table + "." + name);
                    }
                    missing.remove(name);
                }
            }
        }
        if (!missing.isEmpty()) {
            throw new SQLException("protocol value columns missing: " + table);
        }
    }

    /**
     * The columns necessary for the layout should not bypass the column-by-line type check because the whole column is missing.
     *
     * @param table table
     * @return the operation result
     */
    private static Set<String> counterColumns(final String table) {
        return switch (table) {
            case "mvcc_tso" -> Set.of("current_value");
            case "pc_control", "omid_gc", "atlas_gc" -> Set.of("safe_point");
            case "pc_readers", "pc_lock" -> Set.of("start_ts", "expires_at");
            case "pc_data" -> Set.of("start_ts");
            case "pc_write" -> Set.of("start_ts", "commit_ts", "finished_at");
            case "pc_rollback" -> Set.of("start_ts", "finished_at");
            case "omid_txn", "atlas_txn" -> Set.of("start_ts", "commit_ts", "expires_at", "finished_at");
            case "omid_sequence", "atlas_sequence" -> Set.of("seq");
            case "omid_conflicts", "atlas_conflicts" -> Set.of("cell_id", "commit_ts");
            case "omid_versions", "atlas_versions" -> Set.of("stamp", "shadow_ts");
            case "pc_runtime", "omid_runtime", "atlas_runtime" -> Set.of("generation", "retired_before", "retention_ms", "retire_cursor", "pulse");
            case "pc_restore", "omid_restore", "atlas_restore" -> Set.of("source_generation", "target_generation", "epoch", "updated_at");
            case "pc_restore_fence", "omid_restore_fence", "atlas_restore_fence" -> Set.of("epoch");
            default -> Set.of();
        };
    }

    /**
     * All receiving and inspection layouts are fixed white lists, which do not allow external names to enter the protocol operation.
     *
     * @param table table
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static List<String> primaryKey(final String table) {
        if (table.equals("mvcc_tso")) {
            return List.of("sequence_name");
        }
        if (!table.matches("(pc|omid|atlas)_[a-z_]+")) {
            throw new IllegalArgumentException("invalid protocol table");
        }
        String prefix = table.substring(0, table.indexOf('_'));
        String name = table.substring(prefix.length() + 1);
        return switch (prefix) {
            case "pc" -> switch (name) {
                case "control", "binding", "restore" -> List.of("id");
                case "readers" -> List.of("start_ts");
                case "runtime", "restore_fence" -> List.of("role");
                case "guard", "lock" -> List.of("user_key");
                case "data", "rollback" -> List.of("user_key", "start_ts");
                case "write" -> List.of("user_key", "commit_ts");
                default -> throw new IllegalArgumentException("unknown Percolator table");
            };
            case "omid", "atlas" -> switch (name) {
                case "config", "sequence", "gc", "binding", "restore" -> List.of("id");
                case "txn" -> List.of("start_ts");
                case "conflicts" -> List.of("cell_id");
                case "runtime", "restore_fence" -> List.of("role");
                case "keys" -> List.of("table_name", "row_key");
                case "versions" -> List.of("table_name", "row_key", "col_key", "stamp");
                default -> throw new IllegalArgumentException("unknown source backend table");
            };
            default -> throw new IllegalArgumentException("unknown protocol");
        };
    }
}

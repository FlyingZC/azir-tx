package mvcc.operations;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import static mvcc.operations.MysqlProtocolState.storage;

/** 校验实际协议表的事务引擎和唯一约束，不能把 CREATE IF NOT EXISTS 当成结构验收。 */
public final class MysqlProtocolSchema {
    /** 禁止实例化布局检查工具。 */
    private MysqlProtocolSchema() { }

    /** 在普通业务开始前检查固定角色的全部已初始化协议表。 */
    public static void requireRole(DataSource source, String prefix, String role) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix) || !(role.equals("control") || role.matches("shard:[A-Za-z0-9_-]{1,64}"))) { throw new IllegalArgumentException("invalid protocol role"); }
        requireTables(source, MysqlProtocolBackup.tables(prefix, role));
    }

    /** 用同一短连接检查一组已知表，不扫描业务行，也不自动改造既有引擎。 */
    public static void requireTables(DataSource source, List<String> tables) {
        try (var c = source.getConnection()) { for (String table : tables) { requireTable(c, table); } }
        catch (SQLException failure) { throw storage(failure); }
    }

    /** 不接受视图、非事务引擎、错误主键或缺失的 Percolator 原始写唯一约束。 */
    static void requireTable(Connection c, String table) throws SQLException {
        requireTable(c, table, false);
    }
    /** 旧 Percolator 仅允许尚无 finished_at，其余事务引擎、键和计数列约束必须已经成立。 */
    static void requireLegacyTable(Connection c, String table) throws SQLException {
        if (!table.matches("pc_(guard|data|lock|write|rollback)|mvcc_tso")) { throw new IllegalArgumentException("invalid legacy protocol table"); }
        requireTable(c, table, true);
    }
    /** 共享实际布局检查，受控旧版入口不能放宽业务数据列或主键的要求。 */
    private static void requireTable(Connection c, String table, boolean legacy) throws SQLException {
        List<String> expected = primaryKey(table);
        try (var q = c.prepareStatement("select engine,table_type from information_schema.tables where table_schema=database() and table_name=?")) {
            q.setString(1, table); try (var r = q.executeQuery()) {
                if (!r.next() || !"InnoDB".equalsIgnoreCase(r.getString(1)) || !"BASE TABLE".equals(r.getString(2))) { throw new SQLException("protocol table requires InnoDB: " + table); }
            }
        }
        var indexes = new HashMap<String, SortedMap<Integer, String>>();
        try (var q = c.prepareStatement("select index_name,seq_in_index,column_name,sub_part,expression from information_schema.statistics where table_schema=database() and table_name=? and non_unique=0 order by index_name,seq_in_index")) {
            q.setString(1, table); try (var r = q.executeQuery()) {
                while (r.next()) {
                    // 前缀或函数唯一索引不能代替完整协议键，额外索引也不能改变合法写集合。
                    if (r.getObject(4) != null || r.getString(5) != null || r.getString(3) == null) { throw new SQLException("unsupported protocol unique index: " + table); }
                    indexes.computeIfAbsent(r.getString(1), ignored -> new TreeMap<>()).put(r.getInt(2), r.getString(3));
                }
            }
        }
        var actual = indexes.get("PRIMARY");
        if (actual == null || !expected.equals(List.copyOf(actual.values()))) { throw new SQLException("protocol primary key differs: " + table); }
        boolean originalWrite = false;
        for (var entry : indexes.entrySet()) {
            if (entry.getKey().equals("PRIMARY")) { continue; }
            List<String> columns = List.copyOf(entry.getValue().values());
            if (table.equals("pc_write") && columns.equals(List.of("user_key", "start_ts"))) { originalWrite = true; }
            else if (!columns.containsAll(expected)) { throw new SQLException("additional unique constraint changes protocol semantics: " + table); }
        }
        if (table.equals("pc_write") && !originalWrite) { throw new SQLException("Percolator original write uniqueness is missing"); }
        requireValueColumns(c, table, legacy);
    }

    /** 键必须按原始字节比较，载荷不能降为文本，时间戳也不能降为窄整数或无符号整数。 */
    private static void requireValueColumns(Connection c, String table, boolean legacy) throws SQLException {
        Map<String, Integer> binary = switch (table) {
            case "pc_guard", "pc_data", "pc_write", "pc_rollback" -> Map.of("user_key", 512);
            case "pc_lock" -> Map.of("user_key", 512, "primary_key", 512);
            case "omid_keys", "atlas_keys" -> Map.of("row_key", 512);
            case "omid_versions", "atlas_versions" -> Map.of("row_key", 512, "col_key", 256);
            default -> Map.of();
        };
        var missing = new HashSet<>(binary.keySet());
        missing.addAll(counterColumns(table));
        if (legacy) { missing.remove("finished_at"); }
        if (Set.of("pc_data", "omid_versions", "atlas_versions").contains(table)) { missing.add("payload"); }
        if (table.endsWith("_keys") || table.endsWith("_versions")) { missing.add("table_name"); }
        Set<String> counters = Set.of("start_ts", "commit_ts", "expires_at", "finished_at", "stamp", "shadow_ts", "cell_id", "seq", "current_value", "safe_point", "generation", "retired_before", "retention_ms", "retire_cursor", "pulse", "source_generation", "target_generation", "epoch", "updated_at");
        try (var q = c.prepareStatement("select column_name,data_type,column_type,character_maximum_length,is_nullable,collation_name,extra from information_schema.columns where table_schema=database() and table_name=?")) {
            q.setString(1, table); try (var r = q.executeQuery()) {
                while (r.next()) {
                    String name = r.getString(1), type = r.getString(2); boolean nullable = "YES".equals(r.getString(5));
                    if (r.getString(7).toUpperCase(Locale.ROOT).contains("GENERATED") && !r.getString(7).toUpperCase(Locale.ROOT).contains("DEFAULT_GENERATED")) { throw new SQLException("protocol columns cannot be generated: " + table); }
                    if (binary.containsKey(name) && (!type.equals("varbinary") || r.getLong(4) < binary.get(name) || nullable)) { throw new SQLException("protocol binary key layout differs: " + table + "." + name); }
                    if (name.equals("payload") && (!type.equals("longblob") || nullable != table.equals("pc_data"))) { throw new SQLException("protocol payload layout differs: " + table); }
                    if (name.equals("table_name") && (!type.equals("varchar") || r.getLong(4) < 100 || !"ascii_bin".equals(r.getString(6)) || nullable)) { throw new SQLException("protocol table key collation differs: " + table); }
                    if (counters.contains(name) && (!type.equals("bigint") || r.getString(3).toLowerCase(Locale.ROOT).contains("unsigned") || nullable != name.equals("shadow_ts"))) { throw new SQLException("protocol counter layout differs: " + table + "." + name); }
                    missing.remove(name);
                }
            }
        }
        if (!missing.isEmpty()) { throw new SQLException("protocol value columns missing: " + table); }
    }

    /** 布局必需的计数列不能因为整列缺失而绕过逐列类型检查。 */
    private static Set<String> counterColumns(String table) {
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

    /** 所有可验收布局均为固定白名单，不能让外部表名进入协议操作。 */
    private static List<String> primaryKey(String table) {
        if (table.equals("mvcc_tso")) { return List.of("sequence_name"); }
        if (!table.matches("(pc|omid|atlas)_[a-z_]+")) { throw new IllegalArgumentException("invalid protocol table"); }
        String prefix = table.substring(0, table.indexOf('_')); String name = table.substring(prefix.length() + 1);
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

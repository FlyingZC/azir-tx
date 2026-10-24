package mvcc.wal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 新模式专属表的结构检查，防止非事务引擎或错误主键破坏提交和快照语义。 */
final class WalMysqlSchema {
    /** 工具不允许实例化。 */
    private WalMysqlSchema() { }

    /** 对已经存在的两张协议表检查事务引擎，不能先迁移不受支持的用户表。 */
    static void engines(Connection connection) throws SQLException {
        var existing = new java.util.HashSet<String>();
        try (var query = connection.prepareStatement("select TABLE_NAME,ENGINE from information_schema.TABLES "
                + "where cast(TABLE_SCHEMA as binary)=cast(database() as binary) and cast(TABLE_NAME as binary) "
                + "in (cast('azir_wal_binding' as binary),cast('azir_wal_version' as binary))"); var rows = query.executeQuery()) {
            while (rows.next()) {
                existing.add(rows.getString(1));
                if (!"InnoDB".equalsIgnoreCase(rows.getString(2))) { throw new SQLException("WAL protocol tables require InnoDB"); }
            }
        }
        // 已发布绑定记录后，缺失版本表代表冷数据丢失；补建空表会掩盖已确认数据。
        if (existing.contains("azir_wal_binding") && !existing.contains("azir_wal_version")) {
            try (var query = connection.createStatement(); var rows = query.executeQuery("select 1 from azir_wal_binding limit 1")) {
                if (rows.next()) { throw new SQLException("initialized WAL backend version table missing; snapshot recovery required"); }
            }
        }
    }

    /** 已知旧表可原子增加成对游标字段，半套元数据不猜测修复。 */
    static void migrateGc(Connection connection) throws SQLException {
        var columns = columns(connection, "azir_wal_binding");
        boolean cursor = columns.containsKey("gc_cursor"); boolean repeat = columns.containsKey("gc_repeat");
        if (cursor != repeat) { throw new SQLException("partial GC cursor schema"); }
        if (!cursor) {
            try (var query = connection.createStatement()) {
                query.executeUpdate("alter table azir_wal_binding add column gc_cursor varbinary(512) null, add column gc_repeat int not null default 0");
            } catch (SQLException race) {
                var changed = columns(connection, "azir_wal_binding");
                if (race.getErrorCode() != 1060 || !changed.containsKey("gc_cursor") || !changed.containsKey("gc_repeat")) { throw race; }
            }
        }
    }

    /** 只接受已实现的字段、长度、空值语义与主键；额外唯一约束会改变版本写入语义。 */
    static void validate(Connection connection) throws SQLException {
        engines(connection);
        var binding = columns(connection, "azir_wal_binding"); var version = columns(connection, "azir_wal_version");
        if (!binding.keySet().equals(Set.of("id", "engine_id", "shard_id", "applied_seq", "digest", "restore_id", "restoring", "gc_cursor", "gc_repeat", "replica_owner"))
                || !version.keySet().equals(Set.of("user_key", "commit_seq", "payload"))) { throw new SQLException("unexpected WAL protocol columns"); }
        expect(binding, "id", "tinyint", 0, false); expect(binding, "engine_id", "char", 36, false);
        expect(binding, "shard_id", "varbinary", 128, false); expect(binding, "applied_seq", "bigint", 0, false);
        expect(binding, "digest", "char", 64, false); expect(binding, "restore_id", "char", 64, true);
        expect(binding, "restoring", "tinyint", 0, false); expect(binding, "gc_cursor", "varbinary", 512, true); expect(binding, "gc_repeat", "int", 0, false);
        expect(binding, "replica_owner", "char", 64, true);
        expect(version, "user_key", "varbinary", 512, false); expect(version, "commit_seq", "bigint", 0, false); expect(version, "payload", "longblob", 0, true);
        indexes(connection, "azir_wal_binding", List.of("id")); indexes(connection, "azir_wal_version", List.of("user_key", "commit_seq"));
    }

    /** 普通旧表保留空副本所有权，只有显式复制初始化才写入节点指纹。 */
    static void migrateReplica(Connection connection) throws SQLException {
        if (!columns(connection, "azir_wal_binding").containsKey("replica_owner")) {
            try (var query = connection.createStatement()) { query.executeUpdate("alter table azir_wal_binding add column replica_owner char(64) character set ascii null"); }
            catch (SQLException race) { if (race.getErrorCode() != 1060 || !columns(connection, "azir_wal_binding").containsKey("replica_owner")) { throw race; } }
        }
    }

    /** 固定字段描述，不读业务行内容。 */
    private record Column(String type, long length, boolean nullable, boolean unsigned) { }

    /** 使用精确表名查询元数据，不使用带下划线通配符的 JDBC 模式。 */
    private static Map<String, Column> columns(Connection connection, String table) throws SQLException {
        var result = new HashMap<String, Column>();
        try (var query = connection.prepareStatement("select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE,COLUMN_TYPE "
                + "from information_schema.COLUMNS where cast(TABLE_SCHEMA as binary)=cast(database() as binary) and cast(TABLE_NAME as binary)=cast(? as binary)")) {
            query.setString(1, table);
            try (var rows = query.executeQuery()) {
                while (rows.next()) { result.put(rows.getString(1), new Column(rows.getString(2), rows.getLong(3), "YES".equals(rows.getString(4)), rows.getString(5).contains("unsigned"))); }
            }
        }
        return result;
    }

    /** 数值必须为有符号整数，键必须保持二进制排序和完整长度。 */
    private static void expect(Map<String, Column> columns, String name, String type, long length, boolean nullable) throws SQLException {
        var value = columns.get(name);
        if (value == null || !value.type().equals(type) || length != 0 && value.length() != length
                || value.nullable() != nullable || value.unsigned()) { throw new SQLException("unsupported WAL protocol column: " + name); }
    }

    /** 校验主键顺序与完整索引列，拒绝前缀主键和未实现的额外唯一约束。 */
    private static void indexes(Connection connection, String table, List<String> expected) throws SQLException {
        var primary = new java.util.ArrayList<String>();
        try (var query = connection.prepareStatement("select INDEX_NAME,COLUMN_NAME,NON_UNIQUE,SUB_PART from information_schema.STATISTICS "
                + "where cast(TABLE_SCHEMA as binary)=cast(database() as binary) and cast(TABLE_NAME as binary)=cast(? as binary) order by INDEX_NAME,SEQ_IN_INDEX")) {
            query.setString(1, table);
            try (var rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getString(1).equals("PRIMARY")) {
                        if (rows.getObject(4) != null) { throw new SQLException("prefix primary key unsupported"); }
                        primary.add(rows.getString(2));
                    } else if (!rows.getBoolean(3)) { throw new SQLException("additional unique constraint unsupported"); }
                }
            }
        }
        if (!primary.equals(expected)) { throw new SQLException("WAL primary key mismatch"); }
    }
}

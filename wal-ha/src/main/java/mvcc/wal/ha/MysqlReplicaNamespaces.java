package mvcc.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.wal.WalReplicaIdentity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/** MySQL 空间的持久登记与受限创建，不清空已有库，不将就绪空间丢失当作新初始化。 */
final class MysqlReplicaNamespaces {
    private static final String CATALOG = "azir_wal_generation_catalog";
    private static final String BUDGET = "azir_wal_namespace_budget";
    private static final String MARKER = "azir_wal_generation";
    private final HikariDataSource control;
    private final MysqlWalReplicaBackendFactory.Options options;
    private final MysqlWalReplicaBackendFactory.Hook hook;

    /** 所有 SQL 只作用于明确控制库和本机派生的固定名字。 */
    MysqlReplicaNamespaces(HikariDataSource control, MysqlWalReplicaBackendFactory.Options options, MysqlWalReplicaBackendFactory.Hook hook) { this.control = control; this.options = options; this.hook = hook; }

    /** 登记先于数据库创建，创建中可幂等续做；READY 之后只核对，不重新创建丢失的库。 */
    void provision(WalReplicaIdentity identity, UUID generation, String shard, String schema) throws IOException {
        String owner = MysqlWalReplicaBackendFactory.owner(identity, generation, shard, schema);
        try (var connection = control.getConnection()) {
            connection.setAutoCommit(true); createControlTables(connection); int state = reserve(connection, identity, generation, shard, schema, owner); hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_RESERVATION);
            boolean exists = databaseExists(connection, schema);
            if (state == 1) {
                if (!exists) { throw new SQLException("ready generation database missing; snapshot recovery required"); }
                validateTarget(connection, schema, owner, false); return;
            }
            if (!exists) {
                try (var statement = connection.createStatement()) { statement.setQueryTimeout(5); statement.executeUpdate("create database `" + schema + "` character set utf8mb4 collate utf8mb4_bin"); }
                catch (SQLException race) { if (race.getErrorCode() != 1007 || !databaseExists(connection, schema)) { throw race; } }
            }
            hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_CREATE_DATABASE);
            Set<String> tables = tables(connection, schema);
            if (!tables.isEmpty() && !tables.equals(Set.of(MARKER))) { throw new SQLException("reserved generation contains unknown data"); }
            try (var ddl = connection.createStatement()) {
                ddl.setQueryTimeout(5); ddl.executeUpdate("create table if not exists `" + schema + "`." + MARKER
                        + " (id tinyint not null primary key,protocol_version int not null,owner char(64) character set ascii collate ascii_bin not null) engine=InnoDB");
            }
            validateMarkerTable(connection, schema);
            connection.setAutoCommit(false);
            try {
                try (var insert = connection.prepareStatement("insert ignore into `" + schema + "`." + MARKER + " (id,protocol_version,owner) values (1,1,?)")) { insert.setString(1, owner); insert.executeUpdate(); }
                marker(connection, schema, owner); connection.commit();
            } catch (Throwable failure) { connection.rollback(); throw failure; } finally { connection.setAutoCommit(true); }
            hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_MARKER); finish(connection, identity, generation, shard, schema, owner); hook.hit(MysqlWalReplicaBackendFactory.Stage.AFTER_READY);
        } catch (SQLException failure) { throw new IOException("provision MySQL generation namespace failed", failure); }
    }

    /** 固定控制表严格验证，错误引擎、列和唯一约束不能被 IF NOT EXISTS 掩盖。 */
    private void createControlTables(Connection connection) throws SQLException {
        try (var ddl = connection.createStatement()) {
            ddl.setQueryTimeout(5);
            ddl.executeUpdate("create table if not exists " + BUDGET + " (prefix varbinary(8) not null primary key,protocol_version int not null,max_slots int not null,used_slots int not null) engine=InnoDB");
            ddl.executeUpdate("create table if not exists " + CATALOG + " (schema_name varbinary(64) not null primary key,prefix varbinary(8) not null,"
                    + "node_owner char(64) character set ascii collate ascii_bin not null,generation char(36) character set ascii collate ascii_bin not null,"
                    + "shard_id varbinary(128) not null,owner char(64) character set ascii collate ascii_bin not null,state tinyint not null,"
                    + "key scope (prefix,node_owner,shard_id,state)) engine=InnoDB");
        }
        String schema = currentDatabase(connection);
        validateTable(connection, schema, BUDGET, Map.of("prefix", "varbinary:8", "protocol_version", "int", "max_slots", "int", "used_slots", "int"), List.of("prefix"));
        validateTable(connection, schema, CATALOG, Map.of("schema_name", "varbinary:64", "prefix", "varbinary:8", "node_owner", "char:64", "generation", "char:36", "shard_id", "varbinary:128", "owner", "char:64", "state", "tinyint"), List.of("schema_name"));
    }

    /** 控制库内串行预留额度，未登记的已有数据库即使为空也拒绝接管。 */
    private int reserve(Connection connection, WalReplicaIdentity identity, UUID generation, String shard, String schema, String owner) throws SQLException {
        connection.setAutoCommit(false);
        try {
            int used = lockBudget(connection); Integer existing = catalog(connection, identity, generation, shard, schema, owner);
            if (existing != null) { if (existing >= 2) { throw new SQLException("generation is retiring or retired"); } connection.commit(); return existing; }
            if (databaseExists(connection, schema)) { throw new SQLException("unregistered generation database already exists"); }
            try (var query = connection.prepareStatement("select count(*) from " + CATALOG + " where prefix=? and node_owner=? and shard_id=? and state<>3")) {
                query.setBytes(1, bytes(options.prefix())); query.setString(2, MysqlWalReplicaBackendFactory.nodeOwner(identity)); query.setBytes(3, bytes(shard));
                try (var row = query.executeQuery()) { row.next(); if (row.getInt(1) >= options.maxGenerationsPerShard()) { throw new SQLException("generation count budget exhausted"); } }
            }
            if (used >= options.maxSchemasPerControl() || catalogSize(connection) >= 65_536) { throw new SQLException("namespace schema or tombstone budget exhausted"); }
            try (var insert = connection.prepareStatement("insert into " + CATALOG + " (schema_name,prefix,node_owner,generation,shard_id,owner,state) values (?,?,?,?,?,?,0)")) {
                insert.setBytes(1, bytes(schema)); insert.setBytes(2, bytes(options.prefix())); insert.setString(3, MysqlWalReplicaBackendFactory.nodeOwner(identity)); insert.setString(4, generation.toString()); insert.setBytes(5, bytes(shard)); insert.setString(6, owner); insert.executeUpdate();
            }
            try (var update = connection.prepareStatement("update " + BUDGET + " set used_slots=used_slots+1 where prefix=?")) { update.setBytes(1, bytes(options.prefix())); if (update.executeUpdate() != 1) { throw new SQLException("namespace budget authority missing"); } }
            connection.commit(); return 0;
        } catch (Throwable failure) { connection.rollback(); throw failure; } finally { connection.setAutoCommit(true); }
    }

    /** 完成登记不会自动改变额度，恢复中断重复完成不会重复计数。 */
    private void finish(Connection connection, WalReplicaIdentity identity, UUID generation, String shard, String schema, String owner) throws SQLException {
        connection.setAutoCommit(false);
        try {
            lockBudget(connection); Integer state = catalog(connection, identity, generation, shard, schema, owner); if (state == null) { throw new SQLException("generation reservation missing"); }
            if (state >= 2) { throw new SQLException("cannot finish a retired reservation"); }
            if (state == 0) { try (var update = connection.prepareStatement("update " + CATALOG + " set state=1 where schema_name=? and owner=? and state=0")) {
                update.setBytes(1, bytes(schema)); update.setString(2, owner); if (update.executeUpdate() != 1) { throw new SQLException("generation reservation changed"); }
            } }
            connection.commit();
        } catch (Throwable failure) { connection.rollback(); throw failure; } finally { connection.setAutoCommit(true); }
    }

    /** 每个前缀预算行同时充当分配互斥，严格匹配配置版本和已用额度。 */
    private int lockBudget(Connection connection) throws SQLException {
        // 直接取得排他行锁，避免多个 INSERT IGNORE 的共享锁再升级造成死锁。
        try (var insert = connection.prepareStatement("insert into " + BUDGET + " (prefix,protocol_version,max_slots,used_slots) values (?,2,?,0) on duplicate key update prefix=prefix")) { insert.setBytes(1, bytes(options.prefix())); insert.setInt(2, options.maxSchemasPerControl()); insert.executeUpdate(); }
        int used; int version;
        try (var query = connection.prepareStatement("select protocol_version,max_slots,used_slots from " + BUDGET + " where prefix=? for update")) {
            query.setBytes(1, bytes(options.prefix())); try (var row = query.executeQuery()) {
                if (!row.next() || row.getInt(1) < 1 || row.getInt(1) > 2 || row.getInt(2) != options.maxSchemasPerControl() || row.getInt(3) < 0 || row.getInt(3) > row.getInt(2)) { throw new SQLException("namespace budget configuration mismatch"); }
                used = row.getInt(3); version = row.getInt(1);
            }
        }
        try (var query = connection.prepareStatement("select count(*) from " + CATALOG + " where prefix=? and state<>3")) {
            query.setBytes(1, bytes(options.prefix())); try (var row = query.executeQuery()) { row.next(); if (row.getInt(1) != used) { throw new SQLException("namespace budget count mismatch"); } }
        }
        if (version == 1) { try (var update = connection.prepareStatement("update " + BUDGET + " set protocol_version=2 where prefix=?")) { update.setBytes(1, bytes(options.prefix())); update.executeUpdate(); } }
        return used;
    }

    /** 短名字碰撞、错误节点、错误代次或状态异常都必须拒绝，不使用覆盖更新。 */
    private Integer catalog(Connection connection, WalReplicaIdentity identity, UUID generation, String shard, String schema, String owner) throws SQLException {
        try (var query = connection.prepareStatement("select prefix,node_owner,generation,shard_id,owner,state from " + CATALOG + " where schema_name=? for update")) {
            query.setBytes(1, bytes(schema)); try (var row = query.executeQuery()) {
                if (!row.next()) { return null; }
                if (!Arrays.equals(bytes(options.prefix()), row.getBytes(1)) || !MysqlWalReplicaBackendFactory.nodeOwner(identity).equals(row.getString(2))
                        || !generation.toString().equals(row.getString(3)) || !Arrays.equals(bytes(shard), row.getBytes(4)) || !owner.equals(row.getString(5)) || row.getInt(6) < 0 || row.getInt(6) > 3) { throw new SQLException("generation catalog identity mismatch"); }
                return row.getInt(6);
            }
        }
    }

    /** 退休先持久进入不可再打开状态，DDL 完成以后才原子释放额度，并永久保留身份墓碑。 */
    boolean retire(WalGenerationRetirement permit, String shard, String schema, MysqlWalReplicaBackendFactory.RetirementHook retirementHook) throws IOException {
        permit.verify(); var identity = permit.identity(); var generation = permit.generation(); String owner = MysqlWalReplicaBackendFactory.owner(identity, generation, shard, schema);
        try (var connection = control.getConnection()) {
            connection.setAutoCommit(true); createControlTables(connection); connection.setAutoCommit(false); Integer state;
            try {
                lockBudget(connection); state = catalog(connection, identity, generation, shard, schema, owner);
                if (state == null) {
                    if (databaseExists(connection, schema) || catalogSize(connection) >= 65_536) { throw new SQLException("unknown namespace or tombstone budget exhausted"); }
                    try (var insert = connection.prepareStatement("insert into " + CATALOG + " (schema_name,prefix,node_owner,generation,shard_id,owner,state) values (?,?,?,?,?,?,3)")) {
                        insert.setBytes(1, bytes(schema)); insert.setBytes(2, bytes(options.prefix())); insert.setString(3, MysqlWalReplicaBackendFactory.nodeOwner(identity)); insert.setString(4, generation.toString()); insert.setBytes(5, bytes(shard)); insert.setString(6, owner); insert.executeUpdate();
                    }
                    connection.commit(); return false;
                }
                if (state == 3) { if (databaseExists(connection, schema)) { throw new SQLException("retired namespace was externally recreated"); } connection.commit(); return false; }
                if (databaseExists(connection, schema)) { retirementOwnership(connection, schema, identity, shard, owner, state); }
                if (state < 2) { try (var update = connection.prepareStatement("update " + CATALOG + " set state=2 where schema_name=? and owner=?")) { update.setBytes(1, bytes(schema)); update.setString(2, owner); if (update.executeUpdate() != 1) { throw new SQLException("retirement authority changed"); } } }
                connection.commit();
            } catch (Throwable failure) { connection.rollback(); throw failure; } finally { connection.setAutoCommit(true); }
            retirementHook.hit(MysqlWalReplicaBackendFactory.RetirementStage.AFTER_RETIRING); permit.verify(); boolean dropped = databaseExists(connection, schema);
            if (dropped) {
                retirementOwnership(connection, schema, identity, shard, owner, 2); permit.verify();
                try (var ddl = connection.createStatement()) { ddl.setQueryTimeout(5); ddl.executeUpdate("drop database `" + schema + "`"); }
            }
            retirementHook.hit(MysqlWalReplicaBackendFactory.RetirementStage.AFTER_DATABASE_DROP); permit.verify(); connection.setAutoCommit(false);
            try {
                lockBudget(connection); state = catalog(connection, identity, generation, shard, schema, owner);
                if (state == null || state < 2 || databaseExists(connection, schema)) { throw new SQLException("namespace retirement is not complete"); }
                if (state == 2) {
                    try (var update = connection.prepareStatement("update " + CATALOG + " set state=3 where schema_name=? and owner=? and state=2")) { update.setBytes(1, bytes(schema)); update.setString(2, owner); if (update.executeUpdate() != 1) { throw new SQLException("retirement progress changed"); } }
                    try (var update = connection.prepareStatement("update " + BUDGET + " set used_slots=used_slots-1 where prefix=? and used_slots>0")) { update.setBytes(1, bytes(options.prefix())); if (update.executeUpdate() != 1) { throw new SQLException("retirement budget underflow"); } }
                }
                connection.commit();
            } catch (Throwable failure) { connection.rollback(); throw failure; } finally { connection.setAutoCommit(true); }
            retirementHook.hit(MysqlWalReplicaBackendFactory.RetirementStage.AFTER_RELEASE); return dropped;
        } catch (SQLException failure) { throw new IOException("retire MySQL namespace failed", failure); }
    }

    /** 删除前确认固定归属；已持久 RETIRING 后允许继续清理原 DROP 中断留下的协议表子集。 */
    private static void retirementOwnership(Connection connection, String schema, WalReplicaIdentity identity, String shard, String owner, int state) throws SQLException {
        Set<String> existing = tables(connection, schema);
        if (!Set.of(MARKER, "azir_wal_binding", "azir_wal_version").containsAll(existing)) { throw new SQLException("retirement target contains unknown tables"); }
        if (existing.contains(MARKER)) { validateMarkerTable(connection, schema); marker(connection, schema, owner); }
        else if (state == 1 || state == 0 && !existing.isEmpty()) { throw new SQLException("retirement target ownership marker missing"); }
        if (existing.contains("azir_wal_binding")) { try (var query = connection.createStatement(); var row = query.executeQuery("select engine_id,shard_id,replica_owner from `" + schema + "`.azir_wal_binding")) {
            if (row.next() && (!identity.engineId().toString().equals(row.getString(1)) || !Arrays.equals(bytes(shard), row.getBytes(2)) || !MysqlWalReplicaBackendFactory.nodeOwner(identity).equals(row.getString(3)) || row.next())) { throw new SQLException("retirement protocol binding mismatch"); }
        } }
    }

    /** 历史墓碑也有总量上限，不能靠释放数据库额度无限增加控制元数据。 */
    private int catalogSize(Connection connection) throws SQLException { try (var query = connection.prepareStatement("select count(*) from " + CATALOG + " where prefix=?")) { query.setBytes(1, bytes(options.prefix())); try (var row = query.executeQuery()) { row.next(); return row.getInt(1); } } }

    /** READY 空间只验证，不补建标记；已初始化的两个事务表可存在，其他表拒绝。 */
    private static void validateTarget(Connection connection, String schema, String owner, boolean reserved) throws SQLException {
        Set<String> tables = tables(connection, schema); Set<String> allowed = reserved ? Set.of(MARKER) : Set.of(MARKER, "azir_wal_binding", "azir_wal_version");
        if (!tables.contains(MARKER) || !allowed.containsAll(tables)) { throw new SQLException("generation marker missing or unknown data present"); }
        if (tables.contains("azir_wal_binding") != tables.contains("azir_wal_version")) {
            String remaining = tables.contains("azir_wal_binding") ? "azir_wal_binding" : "azir_wal_version";
            try (var query = connection.createStatement(); var row = query.executeQuery("select 1 from `" + schema + "`." + remaining + " limit 1")) {
                if (row.next()) { throw new SQLException("initialized generation protocol table pair incomplete"); }
            }
        }
        validateMarkerTable(connection, schema); marker(connection, schema, owner);
    }

    /** 池实际选择目标后再核对身份，防止 catalog 选择或池模板初始化 SQL 指向错误空间。 */
    static void verifyOpened(Connection connection, String schema, String owner) throws SQLException { validateTarget(connection, schema, owner, false); }

    /** 身份标记的结构必须完整且采用事务引擎。 */
    private static void validateMarkerTable(Connection connection, String schema) throws SQLException { validateTable(connection, schema, MARKER, Map.of("id", "tinyint", "protocol_version", "int", "owner", "char:64"), List.of("id")); }

    /** 标记只能有一行，原始身份一经发布不得替换。 */
    private static void marker(Connection connection, String schema, String owner) throws SQLException {
        try (var query = connection.createStatement(); var row = query.executeQuery("select id,protocol_version,owner from `" + schema + "`." + MARKER)) {
            if (!row.next() || row.getInt(1) != 1 || row.getInt(2) != 1 || !owner.equals(row.getString(3)) || row.next()) { throw new SQLException("generation database owner mismatch"); }
        }
    }

    /** 元数据按二进制精确匹配 schema，不能通过大小写或通配符误认另一个数据库。 */
    private static boolean databaseExists(Connection connection, String schema) throws SQLException {
        try (var query = connection.prepareStatement("select 1 from information_schema.schemata where cast(schema_name as binary)=?")) { query.setBytes(1, bytes(schema)); try (var row = query.executeQuery()) { return row.next(); } }
    }

    /** 有限列举本命名空间内对象，视图和未知对象也会被拒绝。 */
    private static Set<String> tables(Connection connection, String schema) throws SQLException {
        var result = new HashSet<String>();
        try (var query = connection.prepareStatement("select table_name from information_schema.tables where cast(table_schema as binary)=? limit 5")) {
            query.setBytes(1, bytes(schema)); try (var row = query.executeQuery()) { while (row.next()) { result.add(row.getString(1)); } }
        }
        return result;
    }

    /** 控制连接必须明确选择一个数据库，不能在默认空 catalog 上创建元数据。 */
    private static String currentDatabase(Connection connection) throws SQLException { try (var query = connection.createStatement(); var row = query.executeQuery("select database()")) { if (!row.next() || row.getString(1) == null) { throw new SQLException("namespace control database required"); } return row.getString(1); } }

    /** 对引擎、精确列、长度、字符集、空值和主键做严格启动校验。 */
    private static void validateTable(Connection connection, String schema, String table, Map<String, String> expected, List<String> primary) throws SQLException {
        try (var query = connection.prepareStatement("select engine,table_type from information_schema.tables where cast(table_schema as binary)=? and cast(table_name as binary)=?")) {
            query.setBytes(1, bytes(schema)); query.setBytes(2, bytes(table)); try (var row = query.executeQuery()) { if (!row.next() || !"InnoDB".equals(row.getString(1)) || !"BASE TABLE".equals(row.getString(2))) { throw new SQLException("namespace table must be InnoDB"); } }
        }
        var actual = new HashMap<String, String>();
        try (var query = connection.prepareStatement("select column_name,data_type,character_maximum_length,is_nullable,character_set_name,collation_name,extra,column_type from information_schema.columns where cast(table_schema as binary)=? and cast(table_name as binary)=?")) {
            query.setBytes(1, bytes(schema)); query.setBytes(2, bytes(table)); try (var row = query.executeQuery()) { while (row.next()) {
                String type = row.getString(2); if (!"NO".equals(row.getString(4)) || !row.getString(7).isEmpty() || row.getString(8).contains("unsigned")) { throw new SQLException("namespace columns must be non-null, signed and ordinary"); }
                if (type.equals("char")) { if (!"ascii".equals(row.getString(5)) || !"ascii_bin".equals(row.getString(6))) { throw new SQLException("namespace identity must use binary ASCII"); } }
                actual.put(row.getString(1), type + (type.equals("char") || type.equals("varbinary") ? ":" + row.getLong(3) : ""));
            } }
        }
        if (!actual.equals(expected)) { throw new SQLException("namespace table columns mismatch"); }
        var keys = new ArrayList<String>();
        try (var query = connection.prepareStatement("select index_name,column_name,non_unique,sub_part from information_schema.statistics where cast(table_schema as binary)=? and cast(table_name as binary)=? order by index_name,seq_in_index")) {
            query.setBytes(1, bytes(schema)); query.setBytes(2, bytes(table)); try (var row = query.executeQuery()) { while (row.next()) {
                if (row.getString(1).equals("PRIMARY")) { if (row.getObject(4) != null) { throw new SQLException("namespace prefix primary key unsupported"); } keys.add(row.getString(2)); } else if (!row.getBoolean(3)) { throw new SQLException("unexpected namespace unique constraint"); }
            } }
        }
        if (!keys.equals(primary)) { throw new SQLException("namespace primary key mismatch"); }
    }

    /** 所有动态元数据参数按固定 UTF-8 编码传入，SQL 标识符只使用工厂派生名字。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}

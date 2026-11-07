package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/** 新模式专属 MySQL 历史版本存储，只创建 azir_wal 前缀的两张表。 */
public final class MysqlWalMaterializedStore implements WalMaterializedStore {
    private final DataSource dataSource;
    private UUID engineId;
    private String shard;
    private long packetBudget;
    private String replicaOwner;

    /** 保存调用方管理的数据源，每个分片必须使用独立数据库。 */
    public MysqlWalMaterializedStore(DataSource dataSource) { this.dataSource = Objects.requireNonNull(dataSource); }

    /** 初始化专属表并永久绑定 WAL 身份，拒绝旧库或其他模式被隐式接管。 */
    @Override public synchronized void initialize(UUID engineId, String shard) {
        if (replicaOwner != null) { throw new IllegalStateException("replica backend requires explicit replica initialization"); }
        initializeBound(engineId, shard);
    }

    /** 调用方已经明确选择模式后核对数据库绑定，不自动转换已有模式。 */
    private void initializeBound(UUID engineId, String shard) {
        if (this.engineId != null && (!this.engineId.equals(engineId) || !this.shard.equals(shard))) {
            throw new IllegalStateException("materialized store already bound");
        }
        try (Connection connection = dataSource.getConnection()) {
            createTables(connection);
            connection.setAutoCommit(false);
            try {
                try (var query = connection.createStatement();
                     var row = query.executeQuery("select id from azir_wal_binding where id=1 for update")) {
                    if (!row.next()) {
                        try (var count = connection.createStatement();
                             var existing = count.executeQuery("select count(*) from azir_wal_version")) {
                            existing.next();
                            if (existing.getLong(1) != 0) { throw new SQLException("orphan materialized versions"); }
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
            } catch (Throwable failure) { connection.rollback(); throw failure; }
        } catch (SQLException ex) { throw storage("initialize materialized store", ex); }
    }

    /** 本地副本拥有独立逻辑数据库，不能将共享库误配置成三份数据副本。 */
    @Override public boolean supportsReplicaOwnership() { return true; }

    /** 只对全新存储绑定节点身份，不能从已初始化普通引擎隐式升级。 */
    @Override public synchronized void initializeReplica(WalReplicaIdentity identity, String shard) {
        String owner = identity.fingerprint();
        if (engineId != null && replicaOwner == null || replicaOwner != null && !replicaOwner.equals(owner)) { throw new IllegalStateException("backend already has another owner"); }
        replicaOwner = owner; initializeBound(identity.engineId(), shard);
    }

    /** 新建专属表，旧模式表不参与迁移；旧版本专属绑定表只增加恢复状态字段。 */
    private static void createTables(Connection connection) throws SQLException {
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
            } catch (SQLException race) {
                if (race.getErrorCode() != 1060 || restoreColumns(connection) != 2) { throw race; }
            }
        } else if (columns != 2) { throw new SQLException("partial restore metadata schema"); }
        WalMysqlSchema.migrateGc(connection); WalMysqlSchema.migrateReplica(connection); WalMysqlSchema.validate(connection);
    }

    /** 使用固定表元数据识别新模式的旧版表结构，不读取其他业务表。 */
    private static int restoreColumns(Connection connection) throws SQLException {
        int count = 0;
        try (var columns = connection.getMetaData().getColumns(connection.getCatalog(), null, "azir_wal_binding", null)) {
            while (columns.next()) {
                if (!columns.getString("TABLE_NAME").equals("azir_wal_binding")) { continue; }
                String name = columns.getString("COLUMN_NAME");
                if (name.equals("restore_id") || name.equals("restoring")) { count++; }
            }
        }
        return count;
    }

    /** 普通写入和恢复导入遵守同一后端单包限制。 */
    private void configurePacketBudget(Connection connection) throws SQLException {
        try (var query = connection.createStatement(); var row = query.executeQuery("select @@max_allowed_packet")) {
            row.next(); long serverLimit = row.getLong(1);
            long clientLimit = connection.unwrap(com.mysql.cj.jdbc.JdbcConnection.class).getPropertySet()
                    .getIntegerProperty(com.mysql.cj.conf.PropertyKey.maxAllowedPacket).getValue();
            packetBudget = Math.min(serverLimit, clientLimit) - 4096;
            if (packetBudget < 4096) { throw new SQLException("configured packet limit is too small"); }
        }
    }

    /** 保守计入文本转义开销，在事务确认前拒绝不能物化的单行。 */
    @Override public void validateMutation(WalMutation mutation) {
        if (packetBudget < 1 || estimatedPacketBytes(mutation) > packetBudget) {
            throw new WalMvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "mutation exceeds configured MySQL/JDBC packet budget", null, null);
        }
    }

    /** 对二进制及文本协议统一保守估算，包含键、载荷、参数和 SQL 开销。 */
    private static long estimatedPacketBytes(WalMutation mutation) {
        return 256L + 2L * (mutation.key().getBytes(StandardCharsets.UTF_8).length + mutation.valueLength());
    }

    /** 取得当前持久前缀，同时核对引擎和分片身份。 */
    @Override public WalCheckpoint checkpoint() {
        try (Connection connection = dataSource.getConnection()) {
            return verifyBinding(connection, engineId, shard, false, true);
        } catch (SQLException ex) { throw storage("read materialization checkpoint", ex); }
    }

    /** 在一个本地事务中批量写版本并推进前缀，确认丢失时可安全重试。 */
    @Override public WalCheckpoint apply(List<WalCommit> commits) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WalCheckpoint current = verifyBinding(connection, engineId, shard, true);
                long sequence = current.sequence();
                String digest = current.digest();
                try (var insert = connection.prepareStatement(
                        "insert into azir_wal_version (user_key,commit_seq,payload) values (?,?,?)")) {
                    long batchBytes = 0;
                    for (WalCommit commit : commits) {
                        if (commit.sequence() <= sequence) {
                            if (commit.sequence() == sequence && !commit.digest().equals(digest)) {
                                throw new SQLException("replay digest mismatch");
                            }
                            continue;
                        }
                        if (commit.sequence() != sequence + 1) { throw new SQLException("non-contiguous materialization"); }
                        for (WalMutation mutation : commit.mutations()) {
                            if (!mutation.shard().equals(shard)) { continue; }
                            validateMutation(mutation);
                            long bytes = estimatedPacketBytes(mutation);
                            if (batchBytes > 0 && batchBytes + bytes > packetBudget) {
                                insert.executeBatch(); insert.clearBatch(); batchBytes = 0;
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
                    if (batchBytes > 0) { insert.executeBatch(); }
                }
                if (sequence > current.sequence()) {
                    try (var update = connection.prepareStatement("update azir_wal_binding set applied_seq=?,digest=? "
                            + "where id=1 and engine_id=? and shard_id=?")) {
                        update.setLong(1, sequence);
                        update.setString(2, digest);
                        update.setString(3, engineId.toString());
                        update.setBytes(4, shard.getBytes(StandardCharsets.UTF_8));
                        if (update.executeUpdate() != 1) { throw new SQLException("materialization authority changed"); }
                    }
                }
                connection.commit();
                return new WalCheckpoint(sequence, digest);
            } catch (Throwable failure) { connection.rollback(); throw failure; }
        } catch (SQLException ex) { throw storage("apply materialized versions", ex); }
    }

    /** 按框架提交序号读取历史版本，不依赖 MySQL 本地 RR 定义全局快照。 */
    @Override public Optional<WalValue> get(String key, long readSequence) {
        if (replicaOwner != null) { checkpoint(); }
        try (Connection connection = dataSource.getConnection();
             var query = connection.prepareStatement("select commit_seq,payload from azir_wal_version "
                     + "where user_key=? and commit_seq<=? order by commit_seq desc limit 1")) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            query.setLong(2, readSequence);
            try (var row = query.executeQuery()) {
                return row.next() ? Optional.of(new WalValue(row.getLong(1), row.getBytes(2))) : Optional.empty();
            }
        } catch (SQLException ex) { throw storage("read materialized version", ex); }
    }

    /** 利用主键版本索引读取冲突栅栏，不把冷数据载荷带入提交路径。 */
    @Override public long latestSequence(String key) {
        if (replicaOwner != null) { checkpoint(); }
        try (Connection connection = dataSource.getConnection();
             var query = connection.prepareStatement("select commit_seq from azir_wal_version "
                     + "where user_key=? order by commit_seq desc limit 1")) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            try (var row = query.executeQuery()) { return row.next() ? row.getLong(1) : 0; }
        } catch (SQLException ex) { throw storage("read materialized conflict fence", ex); }
    }

    /** 从固定版本切面扫描候选，设置行数和字节上限避免无限物化查询结果。 */
    @Override public NavigableMap<String, WalValue> scan(String from, String to, long readSequence) {
        if (replicaOwner != null) { checkpoint(); }
        String range = (from == null ? "" : " and user_key>=?") + (to == null ? "" : " and user_key<?");
        String sql = "select v.user_key,v.commit_seq,v.payload from azir_wal_version v join "
                + "(select user_key,max(commit_seq) seq from azir_wal_version where commit_seq<=?"
                + range + " group by user_key) visible on v.user_key=visible.user_key and v.commit_seq=visible.seq "
                + "order by v.user_key limit 10001";
        try (Connection connection = dataSource.getConnection(); var query = connection.prepareStatement(sql)) {
            int parameter = 1;
            query.setLong(parameter++, readSequence);
            if (from != null) { query.setBytes(parameter++, from.getBytes(StandardCharsets.UTF_8)); }
            if (to != null) { query.setBytes(parameter, to.getBytes(StandardCharsets.UTF_8)); }
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
        } catch (SQLException ex) { throw storage("scan materialized versions", ex); }
    }

    /** 兼容手动回收入口，扫描键数和删除行数都受独立预算约束。 */
    @Override public int gc(long safePoint, int keyLimit) {
        return gcBounded(safePoint, keyLimit, 4096, 5000).removedRows();
    }

    /** 本实现提供持久游标和有限删除，允许托管维护。 */
    @Override public boolean supportsBoundedMaintenance() { return true; }

    /** 逐键主键 seek，不聚合全库；旧版本先于墓碑删除，部分批次不会复活旧值。 */
    @Override public GcProgress gcBounded(long safePoint, int keyLimit, int rowLimit, long budgetMillis) {
        if (safePoint < 0 || keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096 || budgetMillis < 1 || budgetMillis > 5000) {
            throw new IllegalArgumentException("invalid bounded GC limits");
        }
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(budgetMillis);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                WalCheckpoint applied = verifyBinding(connection, engineId, shard, true);
                byte[] cursor; int repeat;
                try (var query = connection.createStatement(); var row = query.executeQuery("select gc_cursor,gc_repeat from azir_wal_binding where id=1")) {
                    row.next(); cursor = row.getBytes(1); repeat = row.getInt(2);
                }
                if (repeat < 0 || repeat > 3) { throw new SQLException("invalid GC cursor repeat state"); }
                int inspected = 0; int removed = 0; boolean wrapped = false;
                while (inspected < keyLimit && removed < rowLimit && (inspected == 0 || System.nanoTime() - deadline < 0)) {
                    byte[] key;
                    String seek = "select user_key from azir_wal_version" + (cursor == null ? "" : " where user_key>?") + " order by user_key,commit_seq limit 1";
                    try (var query = connection.prepareStatement(seek)) {
                        query.setQueryTimeout(1); if (cursor != null) { query.setBytes(1, cursor); }
                        try (var row = query.executeQuery()) { key = row.next() ? row.getBytes(1) : null; }
                    }
                    if (key == null) { cursor = null; repeat = 0; wrapped = true; break; }
                    inspected++; long anchor = -1; boolean tombstone = false;
                    try (var query = connection.prepareStatement("select commit_seq,payload is null from azir_wal_version where user_key=? and commit_seq<=? order by commit_seq desc limit 1")) {
                        query.setQueryTimeout(1); query.setBytes(1, key); query.setLong(2, Math.min(safePoint, applied.sequence()));
                        try (var row = query.executeQuery()) { if (row.next()) { anchor = row.getLong(1); tombstone = row.getBoolean(2); } }
                    }
                    int deleted = 0; boolean remaining = false;
                    if (anchor >= 0) {
                        String bound = tombstone ? "<=?" : "<?";
                        try (var delete = connection.prepareStatement("delete from azir_wal_version where user_key=? and commit_seq" + bound + " order by commit_seq asc limit ?")) {
                            delete.setQueryTimeout(1); delete.setBytes(1, key); delete.setLong(2, anchor); delete.setInt(3, rowLimit - removed); deleted = delete.executeUpdate();
                        }
                        if (deleted == rowLimit - removed) {
                            try (var query = connection.prepareStatement("select commit_seq from azir_wal_version where user_key=? and commit_seq" + bound + " order by commit_seq limit 1")) {
                                query.setQueryTimeout(1); query.setBytes(1, key); query.setLong(2, anchor);
                                try (var row = query.executeQuery()) { remaining = row.next(); }
                            }
                        }
                    }
                    removed += deleted;
                    // 一个键最多连续占用三轮预算，随后轮转，避免热点历史永久饿死其他键。
                    if (remaining && repeat < 3) { repeat++; break; }
                    cursor = key; repeat = 0;
                }
                try (var update = connection.prepareStatement("update azir_wal_binding set gc_cursor=?,gc_repeat=? where id=1")) {
                    update.setBytes(1, cursor); update.setInt(2, repeat); update.executeUpdate();
                }
                connection.commit();
                return new GcProgress(inspected, removed, wrapped);
            } catch (Throwable failure) { connection.rollback(); throw failure; }
        } catch (SQLException ex) { throw storage("collect materialized history", ex); }
    }

    /** 托管入口要求可验证的借连接、连接和读取期限及可靠本地提交配置。 */
    @Override public void validateProduction() {
        try {
            var pool = dataSource.unwrap(com.zaxxer.hikari.HikariDataSource.class);
            if (pool.getConnectionTimeout() > 5000) { throw new SQLException("managed MySQL pool connectionTimeout must be at most 5000ms"); }
            try (var connection = dataSource.getConnection()) {
                var properties = connection.unwrap(com.mysql.cj.jdbc.JdbcConnection.class).getPropertySet();
                for (var property : List.of(com.mysql.cj.conf.PropertyKey.connectTimeout, com.mysql.cj.conf.PropertyKey.socketTimeout)) {
                    int value = properties.getIntegerProperty(property).getValue();
                    if (value < 1 || value > 5000) { throw new SQLException("managed MySQL " + property.getKeyName() + " must be 1..5000ms"); }
                }
                // 状态方法可能查询服务器，必须在确认驱动 I/O 已有期限之后调用。
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
        } catch (SQLException failure) { throw storage("validate production backend", failure); }
    }

    /** MySQL 实现完整快照流和恢复隔离协议，不能用普通有界扫描替代备份。 */
    @Override public boolean supportsBackup() { return true; }

    /** 副本恢复指纹与元数据同时绑定接收身份，完成后普通初始化仍拒绝接管。 */
    @Override public boolean supportsReplicaRestore() { return true; }

    /** MySQL 的 RR 事务在绑定查询时就固定一致性读视图。 */
    @Override public boolean supportsPinnedSnapshot() { return true; }

    /** 兼容原串行备份入口，复用同一个固定读视图协议。 */
    @Override public void exportSnapshot(WalCheckpoint boundary, BackupSink sink) throws java.io.IOException {
        try (var view = openSnapshot(boundary)) { view.export(sink); }
    }

    /** 准备阶段查到精确边界后保留事务，后续物化与 GC 使用其他连接继续推进。 */
    @Override public SnapshotView openSnapshot(WalCheckpoint boundary) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection(); connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ); connection.setReadOnly(true); connection.setAutoCommit(false);
            if (!boundary.equals(verifyBinding(connection, engineId, shard, false))) { throw new SQLException("backup boundary is not fully materialized"); }
            return new MysqlSnapshotView(connection, boundary);
        } catch (Throwable failure) {
            if (connection != null) { try { connection.close(); } catch (SQLException close) { failure.addSuppressed(close); } }
            if (failure instanceof Error error) { throw error; } if (failure instanceof RuntimeException runtime) { throw runtime; } throw storage("open materialized snapshot", (SQLException) failure);
        }
    }

    /** 本次导出独占一条连接，绑定查询和数据查询属于同一 RR 一致性视图。 */
    private final class MysqlSnapshotView implements SnapshotView {
        private final Connection connection; private final WalCheckpoint boundary; private boolean exported, closed;
        /** 事务读视图已经由打开阶段建立，不能在这里重开事务。 */
        MysqlSnapshotView(Connection connection, WalCheckpoint boundary) { this.connection = connection; this.boundary = boundary; }
        /** 后续提交可推进绑定表，本视图仍只能看到打开时的前缀。 */
        @Override public synchronized void export(BackupSink sink) throws java.io.IOException {
            if (closed || exported) { throw new java.io.IOException("materialized snapshot already consumed"); } exported = true;
            String sql = "select v.user_key,v.commit_seq,v.payload from azir_wal_version v join "
                    + "(select user_key,max(commit_seq) seq from azir_wal_version where commit_seq<=? group by user_key) a "
                    + "on v.user_key=a.user_key and v.commit_seq=a.seq order by v.user_key";
            try (var query = connection.prepareStatement(sql)) {
                query.setFetchSize(Integer.MIN_VALUE); query.setLong(1, boundary.sequence());
                try (var row = query.executeQuery()) { while (row.next()) { sink.accept(backupRow(row)); } }
            } catch (SQLException failure) { throw storage("export materialized backup", failure); }
        }
        /** 回滚只读事务结束读视图，连接关闭异常也不能继续使用此对象。 */
        @Override public synchronized void close() throws java.io.IOException {
            if (closed) { return; } closed = true; SQLException failure = null;
            try { connection.rollback(); } catch (SQLException rollback) { failure = rollback; }
            try { connection.close(); } catch (SQLException close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } }
            if (failure != null) { throw storage("close materialized snapshot", failure); }
        }
    }

    /** 只接受空数据库或同一备份的恢复状态，已初始化的普通数据库即使为空也不能接管。 */
    @Override public synchronized boolean beginRestore(WalRestoreSpec spec) {
        String owner = spec.replica() == null ? null : spec.replica().fingerprint();
        if (engineId != null && (!engineId.equals(spec.engineId()) || !shard.equals(spec.shard()) || !Objects.equals(replicaOwner, owner))) {
            throw new IllegalStateException("restore store already bound");
        }
        try (Connection connection = dataSource.getConnection()) {
            createTables(connection); configurePacketBudget(connection); connection.setAutoCommit(false);
            try {
                boolean exists;
                try (var query = connection.createStatement(); var row = query.executeQuery("select id from azir_wal_binding where id=1 for update")) {
                    exists = row.next();
                }
                if (!exists) {
                    try (var query = connection.createStatement(); var row = query.executeQuery("select count(*) from azir_wal_version")) {
                        row.next(); if (row.getLong(1) != 0) { throw new SQLException("restore target contains orphan data"); }
                    }
                    try (var insert = connection.prepareStatement("insert into azir_wal_binding "
                            + "(id,engine_id,shard_id,applied_seq,digest,restore_id,restoring,replica_owner) values (1,?,?,0,'',?,1,?)")) {
                        insert.setString(1, spec.engineId().toString()); insert.setBytes(2, spec.shard().getBytes(StandardCharsets.UTF_8));
                        insert.setString(3, restoreFingerprint(spec)); insert.setString(4, owner); insert.executeUpdate();
                    }
                }
                boolean importing = verifyRestoreBinding(connection, spec);
                connection.commit(); engineId = spec.engineId(); shard = spec.shard(); replicaOwner = owner; return importing;
            } catch (Throwable failure) { connection.rollback(); throw failure; }
        } catch (SQLException failure) { throw storage("begin backend restore", failure); }
    }

    /** 批量导入具有固定身份的最新版本，重试仅覆盖同一备份的未完成恢复空间。 */
    @Override public void restoreBatch(WalRestoreSpec spec, List<WalBackupRow> rows) {
        if (rows.isEmpty() || rows.size() > 1024) { throw new IllegalArgumentException("invalid restore batch size"); }
        long total = 0;
        for (var row : rows) {
            if (row.version().commitSequence() > spec.boundary().sequence()) { throw new IllegalArgumentException("restore version exceeds boundary"); }
            var mutation = new WalMutation(spec.shard(), row.key(), row.version().value()); validateMutation(mutation); total += mutation.retainedBytes();
        }
        if (total > 16L * 1024 * 1024) { throw new IllegalArgumentException("restore batch exceeds memory budget"); }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (!verifyRestoreBinding(connection, spec)) { connection.commit(); return; }
                try (var insert = connection.prepareStatement("insert into azir_wal_version (user_key,commit_seq,payload) values (?,?,?) "
                        + "as incoming on duplicate key update payload=incoming.payload")) {
                    long bytes = 0;
                    for (var row : rows) {
                        var mutation = new WalMutation(spec.shard(), row.key(), row.version().value()); long size = estimatedPacketBytes(mutation);
                        if (bytes > 0 && bytes + size > packetBudget) { insert.executeBatch(); insert.clearBatch(); bytes = 0; }
                        insert.setBytes(1, row.key().getBytes(StandardCharsets.UTF_8)); insert.setLong(2, row.version().commitSequence());
                        insert.setBytes(3, row.version().value()); insert.addBatch(); bytes += size;
                    }
                    insert.executeBatch();
                }
                connection.commit();
            } catch (Throwable failure) { connection.rollback(); throw failure; }
        } catch (SQLException failure) { throw storage("import backend restore batch", failure); }
    }

    /** 流式复算完整物理数据摘要，只有完全匹配时才发布应用前缀和清除恢复中状态。 */
    @Override public void finishRestore(WalRestoreSpec spec, long expectedRows, String expectedDataHash) {
        if (expectedRows < 0 || !expectedDataHash.matches("[a-f0-9]{64}")) { throw new IllegalArgumentException("invalid restore verification"); }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                boolean importing = verifyRestoreBinding(connection, spec);
                var digest = java.security.MessageDigest.getInstance("SHA-256");
                var output = new java.security.DigestOutputStream(java.io.OutputStream.nullOutputStream(), digest);
                var writer = new WalBackupData.Writer(output, new WalBackupData.Header(spec.engineId(), spec.shard(), spec.boundary()));
                try (var query = connection.prepareStatement("select user_key,commit_seq,payload from azir_wal_version order by user_key,commit_seq")) {
                    query.setFetchSize(Integer.MIN_VALUE);
                    try (var row = query.executeQuery()) { while (row.next()) { writer.accept(backupRow(row)); } }
                }
                long actualRows = writer.finish(); String actualHash = java.util.HexFormat.of().formatHex(digest.digest());
                if (actualRows != expectedRows || !actualHash.equals(expectedDataHash)) { throw new SQLException("restored backend data checksum mismatch"); }
                if (importing) { try (var update = connection.prepareStatement("update azir_wal_binding set applied_seq=?,digest=?,restoring=0 where id=1 and restore_id=?")) {
                    update.setLong(1, spec.boundary().sequence()); update.setString(2, spec.boundary().digest()); update.setString(3, restoreFingerprint(spec));
                    if (update.executeUpdate() != 1) { throw new SQLException("restore authority changed"); }
                } }
                connection.commit();
            } catch (Throwable failure) { connection.rollback(); throw failure; }
        } catch (SQLException | java.io.IOException | java.security.NoSuchAlgorithmException failure) {
            throw storage("verify and finish backend restore", new SQLException(failure));
        }
    }

    /** JDBC 流式行转换保留 UTF-8 键、版本和墓碑，非法内容由备份格式拒绝。 */
    private static WalBackupRow backupRow(ResultSet row) throws SQLException {
        return new WalBackupRow(decodeKey(row.getBytes(1)), new WalValue(row.getLong(2), row.getBytes(3)));
    }

    /** 非法后端字节不能被替换字符静默改成另一个业务键。 */
    private static String decodeKey(byte[] bytes) throws SQLException {
        try { return StandardCharsets.UTF_8.newDecoder().onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(bytes)).toString(); }
        catch (java.nio.charset.CharacterCodingException failure) { throw new SQLException("invalid UTF-8 in materialized key", failure); }
    }

    /** 恢复指纹覆盖所有身份字段，不能用相同备份号但不同提交边界续写。 */
    private static String restoreFingerprint(WalRestoreSpec spec) {
        String plain = String.join("\u0000", spec.backupId().toString(), spec.operationId().toString(), spec.engineId().toString(), spec.shard(),
                Long.toString(spec.boundary().sequence()), spec.boundary().digest());
        if (spec.replica() != null) { plain += "\u0000" + spec.replica().fingerprint(); }
        return WalCodec.digest(plain.getBytes(StandardCharsets.UTF_8));
    }

    /** 锁住恢复状态行并核对固定身份；完成后发生新提交的存储不能被旧备份回滚。 */
    private static boolean verifyRestoreBinding(Connection connection, WalRestoreSpec spec) throws SQLException {
        try (var query = connection.prepareStatement("select engine_id,shard_id,applied_seq,digest,restore_id,restoring,replica_owner from azir_wal_binding where id=1 for update");
             var row = query.executeQuery()) {
            if (!row.next() || !spec.engineId().toString().equals(row.getString(1))
                    || !spec.shard().equals(new String(row.getBytes(2), StandardCharsets.UTF_8)) || !restoreFingerprint(spec).equals(row.getString(5))
                    || !Objects.equals(spec.replica() == null ? null : spec.replica().fingerprint(), row.getString(7))) {
                throw new SQLException("restore target is occupied or belongs to another backup");
            }
            boolean importing = row.getBoolean(6); long sequence = row.getLong(3); String digest = row.getString(4);
            if (importing ? sequence != 0 || !digest.isEmpty()
                    : sequence != spec.boundary().sequence() || !digest.equals(spec.boundary().digest())) {
                throw new SQLException("restore progress or completed prefix changed");
            }
            return importing;
        }
    }

    /** 在当前连接检查固定存储身份，写入和回收时锁住元数据行。 */
    private WalCheckpoint verifyBinding(Connection connection, UUID engineId, String shard, boolean lock)
            throws SQLException {
        return verifyBinding(connection, engineId, shard, lock, false);
    }

    /** 前缀确认使用锁定提交读，连接即使配置为 RU 也不能看见未提交物化进度。 */
    private WalCheckpoint verifyBinding(Connection connection, UUID engineId, String shard, boolean lock, boolean committedRead)
            throws SQLException {
        if (engineId == null || shard == null) { throw new SQLException("store not initialized"); }
        try (PreparedStatement query = connection.prepareStatement("select engine_id,shard_id,applied_seq,digest,restoring,replica_owner "
                + "from azir_wal_binding where id=1" + (lock ? " for update" : committedRead ? " for share" : ""));
             ResultSet row = query.executeQuery()) {
            if (!row.next() || !engineId.toString().equals(row.getString(1))
                    || !shard.equals(new String(row.getBytes(2), StandardCharsets.UTF_8)) || row.getBoolean(5) || !Objects.equals(replicaOwner, row.getString(6))) {
                throw new SQLException("WAL identity or shard binding mismatch");
            }
            return new WalCheckpoint(row.getLong(3), row.getString(4));
        }
    }

    /** 将 JDBC 失败转换成新模式异常，不修改旧模式错误映射。 */
    private static WalMvccException storage(String operation, SQLException cause) {
        return new WalMvccException(ErrorCode.STORAGE_UNAVAILABLE, operation + " failed", null, cause);
    }
}

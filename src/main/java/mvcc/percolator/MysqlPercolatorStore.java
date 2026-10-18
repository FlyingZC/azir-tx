package mvcc.percolator;

import mvcc.common.ErrorCode;
import mvcc.common.MutationType;
import mvcc.common.MvccException;
import mvcc.storage.Mutation;
import mvcc.storage.KeyRange;
import mvcc.storage.Utf8KeyOrder;
import mvcc.storage.VersionedValue;
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

/** 在独立 MySQL 分片上用本地原子事务实现 Percolator data/lock/write 协议。 */
public final class MysqlPercolatorStore implements PercolatorStore {
    private final String shard;
    private final DataSource dataSource;
    private final mvcc.operations.MysqlProtocolState operations;

    /** 借用调用方连接池；存储对象不负责关闭连接池。 */
    public MysqlPercolatorStore(String shard, DataSource dataSource) {
        this(shard, dataSource, null);
    }
    /** 生产入口额外绑定运行代次，防止冻结后迟到 SQL 继续发布。 */
    public MysqlPercolatorStore(String shard, DataSource dataSource, mvcc.operations.MysqlProtocolState operations) {
        this.operations = operations;
        this.shard = new PercolatorKey(shard, "").shard();
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** 返回本地分片编号。 */
    @Override
    public String shardId() { return shard; }

    /** 创建隔离协议表；历史决议保留，载荷 GC 由持久读租约保护。 */
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
                """
        );
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            if (!connection.getAutoCommit()) {
                throw new IllegalArgumentException("Percolator requires an auto-commit pool");
            }
            for (String sql : ddl) { statement.execute(sql); }
            mvcc.operations.MysqlProtocolAdmin.ensureFinishedColumn(dataSource, "pc_write");
            mvcc.operations.MysqlProtocolAdmin.ensureFinishedColumn(dataSource, "pc_rollback");
        } catch (SQLException ex) {
            throw storageError("initialize Percolator schema", null, ex);
        }
    }

    /** 整批预写共享一个连接和本地事务，按键排序占位并严格拒绝重放更改值。 */
    @Override
    public void prewrite(long startTs, PercolatorKey primary, long ttlMillis, List<Mutation> mutations) {
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
            if (operations != null) { operations.lockData(connection, shard, startTs, false); }
            for (Mutation mutation : ordered) { guard(connection, mutation.key()); }
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
                try (var select = connection.prepareStatement(
                        "select commit_ts from pc_write where user_key=? order by commit_ts desc limit 1")) {
                    select.setBytes(1, bytes(key));
                    try (var result = select.executeQuery()) {
                        if (result.next() && result.getLong(1) >= startTs) {
                            throw error(ErrorCode.WRITE_CONFLICT, "newer write: " + key, true, startTs);
                        }
                    }
                }
                try (var data = connection.prepareStatement(
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

    /** 用一个 RR 一致性视图读取锁和版本；此本地快照只用于防止两次 SQL 撕裂。 */
    @Override
    public Snapshot snapshot(String key, long readTs) {
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
            } catch (SQLException | RuntimeException | Error ex) {
                rollback(connection, ex);
                throw ex;
            }
        } catch (SQLException ex) {
            throw storageError("read Percolator snapshot", null, ex);
        }
    }

    /** 有界枚举永久候选键；相关预写在 commitTs 分配前已经创建 guard，不会漏掉快照内提交。 */
    @Override
    public List<String> keys(KeyRange range, String afterKeyExclusive, int limit) {
        Objects.requireNonNull(range, "range");
        if (limit < 1 || limit > 4097) { throw new IllegalArgumentException("candidate limit must be in 1..4097"); }
        List<String> parameters = new ArrayList<>();
        StringBuilder sql = new StringBuilder("select user_key from pc_guard where 1=1");
        if (range.startKeyInclusive() != null) {
            sql.append(" and user_key>=?"); parameters.add(range.startKeyInclusive());
        }
        if (range.endKeyExclusive() != null) {
            sql.append(" and user_key<?"); parameters.add(range.endKeyExclusive());
        }
        if (afterKeyExclusive != null) { sql.append(" and user_key>?"); parameters.add(afterKeyExclusive); }
        sql.append(" order by user_key limit ?");
        // 范围边界可能是最大长度业务键再加排他后缀，不将边界误当作实际存储键截断。
        parameters.forEach(Utf8KeyOrder::encode);
        try (var connection = dataSource.getConnection(); var select = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (String parameter : parameters) { select.setBytes(index++, bytes(parameter)); }
            select.setInt(index, limit);
            try (var result = select.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (result.next()) { keys.add(text(result.getBytes(1))); }
                return List.copyOf(keys);
            }
        } catch (SQLException ex) { throw storageError("scan Percolator candidates", null, ex); }
    }

    /** 将 primary 的提交和回滚串行化；缺失 primary 必须先写栅栏再清理 secondary。 */
    @Override
    public Decision primary(PercolatorKey key, long startTs, Action action, long commitTs) {
        validateTimestamp(startTs);
        if (!key.shard().equals(shard) || action == null
                || (action == Action.COMMIT && commitTs <= startTs)) {
            throw new IllegalArgumentException("invalid primary operation");
        }
        return atomic(startTs, connection -> {
            // 终态永久不可变，历史凭据查询无需再修改永久候选行。
            Optional<Long> known = writeTimestamp(connection, key.key(), startTs);
            if (known.isPresent()) { return new Decision(State.COMMITTED, known.get()); }
            if (hasRollback(connection, key.key(), startTs)) { return new Decision(State.ROLLED_BACK, 0); }
            if (operations != null && startTs <= operations.lockData(connection, shard, 0, true)) {
                var stale = lock(connection, key.key());
                if (stale.isPresent() && stale.get().startTs() == startTs) {
                    guard(connection, key.key()); rollbackKey(connection, key.key(), startTs);
                    return new Decision(State.ROLLED_BACK, 0);
                }
                return new Decision(State.RETIRED, 0);
            }
            guard(connection, key.key());
            Optional<Long> committed = writeTimestamp(connection, key.key(), startTs);
            if (committed.isPresent()) { return new Decision(State.COMMITTED, committed.get()); }
            if (hasRollback(connection, key.key(), startTs)) { return new Decision(State.ROLLED_BACK, 0); }
            Optional<Lock> held = lock(connection, key.key());
            boolean owned = held.isPresent() && held.get().startTs() == startTs;
            if (owned && !held.get().primary().equals(key)) {
                throw error(ErrorCode.ILLEGAL_STATE, "address is not the primary", false, startTs);
            }
            if (!owned) {
                // 即便查询发生在首次预写之前，也必须持久阻止迟到的同 startTs 预写。
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

    /** 仅使用调用方已经确认的终态完成键；旧事务重放不能触碰新事务锁。 */
    @Override
    public void finish(long startTs, PercolatorKey primary, Decision decision, List<String> keys) {
        validateTimestamp(startTs);
        if ((decision.state() != State.COMMITTED && decision.state() != State.ROLLED_BACK) || (decision.state() == State.COMMITTED && decision.commitTs() <= startTs)) {
            throw new IllegalArgumentException("finish requires a valid terminal decision");
        }
        List<String> ordered = keys.stream().distinct().sorted(Utf8KeyOrder::compare).toList();
        for (String key : ordered) { new PercolatorKey(shard, key); }
        atomic(startTs, connection -> {
            for (String key : ordered) { guard(connection, key); }
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

    /** 每轮按原始事务顺序发现有限锁，避免启动时装载全部历史数据。 */
    @Override
    public List<Lock> pending(int limit) {
        if (limit < 1 || limit > 4096) { throw new IllegalArgumentException("limit must be in 1..4096"); }
        try (var connection = dataSource.getConnection(); var select = connection.prepareStatement(
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
        } catch (SQLException ex) {
            throw storageError("discover Percolator locks", null, ex);
        }
    }

    /** 统计持久锁，供运行状态诊断使用，不将全部锁载入堆。 */
    @Override public long pendingCount() {
        try (var c = dataSource.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select count(*) from pc_lock")) {
            r.next(); return r.getLong(1);
        } catch (SQLException failure) { throw storageError("count Percolator locks", null, failure); }
    }

    /** 每键保留安全点锚点，安全墓碑可退休；write 决议和 rollback 栅栏不删除。 */
    @Override public int collect(long safePoint, int limit) {
        if (safePoint < 0 || limit < 1 || limit > 4096) { throw new IllegalArgumentException("invalid GC budget"); }
        return atomic(0, connection -> {
            /** 可删除载荷的精确原始键，避免按历史数量做不安全裁剪。 */
            record Obsolete(String key, long start) { }
            List<Obsolete> old = new ArrayList<>();
            try (var q = connection.prepareStatement("select d.user_key,d.start_ts from pc_data d join pc_write w on w.user_key=d.user_key and w.start_ts=d.start_ts where w.commit_ts<=? and exists(select 1 from pc_write n where n.user_key=w.user_key and n.commit_ts>w.commit_ts and n.commit_ts<=?) order by d.user_key,d.start_ts limit ?")) {
                q.setLong(1, safePoint); q.setLong(2, safePoint); q.setInt(3, limit);
                try (var r = q.executeQuery()) { while (r.next()) { old.add(new Obsolete(text(r.getBytes(1)), r.getLong(2))); } }
            }
            int deleted = 0;
            for (var version : old) {
                mvcc.operations.MysqlProtocolState.checkInterrupted();
                guard(connection, version.key());
                try (var q = connection.prepareStatement("delete from pc_data where user_key=? and start_ts=?")) {
                    bindKey(q, version.key(), version.start()); deleted += q.executeUpdate();
                }
            }
            if (old.size() < limit) {
                List<Obsolete> tombstones = new ArrayList<>();
                try (var q = connection.prepareStatement("select d.user_key,d.start_ts from pc_data d join pc_write w on w.user_key=d.user_key and w.start_ts=d.start_ts where d.deleted=true and w.commit_ts<=? and not exists(select 1 from pc_write n where n.user_key=w.user_key and n.commit_ts>w.commit_ts) and not exists(select 1 from pc_data n where n.user_key=d.user_key and n.start_ts<>d.start_ts) and not exists(select 1 from pc_lock l where l.user_key=d.user_key) order by d.user_key limit ?")) {
                    q.setLong(1, safePoint); q.setInt(2, limit - old.size());
                    try (var r = q.executeQuery()) { while (r.next()) { tombstones.add(new Obsolete(text(r.getBytes(1)), r.getLong(2))); } }
                }
                for (var version : tombstones) {
                    guard(connection, version.key());
                    try (var q = connection.prepareStatement("delete from pc_data where user_key=? and start_ts=?")) {
                        bindKey(q, version.key(), version.start()); deleted += q.executeUpdate();
                    }
                }
            }
            return deleted;
        });
    }

    /** 跨表协议都先锁住永久 guard 行，避免不存在行、删锁和插锁之间的竞态。 */
    private void guard(Connection connection, String key) throws SQLException {
        try (var insert = connection.prepareStatement(
                "insert into pc_guard(user_key) values(?) on duplicate key update user_key=values(user_key)")) {
            insert.setBytes(1, bytes(key));
            insert.executeUpdate();
        }
    }

    /** 读取当前键锁；所有修改调用已经持有 guard 且使用 RC，读调用使用独立 RR 视图。 */
    private Optional<Lock> lock(Connection connection, String key) throws SQLException {
        try (var select = connection.prepareStatement(
                "select start_ts,primary_shard,primary_key from pc_lock where user_key=?")) {
            select.setBytes(1, bytes(key));
            try (var result = select.executeQuery()) {
                return result.next() ? Optional.of(new Lock(new PercolatorKey(shard, key), result.getLong(1),
                        new PercolatorKey(result.getString(2), text(result.getBytes(3))))) : Optional.empty();
            }
        }
    }

    /** 使用唯一反向索引按 startTs 查提交时间，避免扫描 primary 的全部历史。 */
    private Optional<Long> writeTimestamp(Connection connection, String key, long startTs) throws SQLException {
        try (var select = connection.prepareStatement("select commit_ts from pc_write where user_key=? and start_ts=?")) {
            bindKey(select, key, startTs);
            try (var result = select.executeQuery()) { return result.next() ? Optional.of(result.getLong(1)) : Optional.empty(); }
        }
    }

    /** 核对是否有永久回滚栅栏。 */
    private boolean hasRollback(Connection connection, String key, long startTs) throws SQLException {
        try (var select = connection.prepareStatement("select 1 from pc_rollback where user_key=? and start_ts=?")) {
            bindKey(select, key, startTs);
            try (var result = select.executeQuery()) { return result.next(); }
        }
    }

    /** 在 primary 数据库时钟上判断过期，不把逻辑 TSO 当作毫秒时钟。 */
    private boolean expired(Connection connection, String key) throws SQLException {
        try (var select = connection.prepareStatement("""
                select expires_at<=cast(unix_timestamp(current_timestamp(3))*1000 as signed)
                from pc_lock where user_key=?
                """)) {
            select.setBytes(1, bytes(key));
            try (var result = select.executeQuery()) { result.next(); return result.getBoolean(1); }
        }
    }

    /** 重复预写必须完全相同，不能修改已经用于分配 commitTs 的写集合。 */
    private void verifyReplay(Connection connection, long startTs, Mutation mutation) throws SQLException {
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

    /** 在持有 guard 的本地事务内发布 write 并移除原 startTs 的锁。 */
    private void commitKey(Connection connection, String key, long startTs, long commitTs) throws SQLException {
        try (var insert = connection.prepareStatement("insert into pc_write(user_key,commit_ts,start_ts) values(?,?,?)");
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

    /** 先记录不可复活的回滚标记，再删除原事务 data 和 lock。 */
    private void rollbackKey(Connection connection, String key, long startTs) throws SQLException {
        if (operations != null && startTs <= operations.lockData(connection, shard, 0, true) && !hasRollback(connection, key, startTs) && lock(connection, key).filter(held -> held.startTs() == startTs).isEmpty()) { throw mvcc.operations.MysqlProtocolState.expired(startTs); }
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

    /** 统一绑定二进制键和原始事务时间戳。 */
    private static void bindKey(PreparedStatement statement, String key, long startTs) throws SQLException {
        statement.setBytes(1, bytes(key));
        statement.setLong(2, startTs);
    }

    /** 对有副作用的本地事务统一回滚；提交响应丢失由运行时解析 primary。 */
    private <T> T atomic(long startTs, SqlWork<T> work) {
        try (var connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                if (operations != null) { operations.lockData(connection, shard, startTs, true); }
                T result = work.apply(connection);
                if (operations != null) { operations.requireDataPublication(); }
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error ex) {
                rollback(connection, ex);
                throw ex;
            }
        } catch (SQLException ex) {
            throw storageError("Percolator local transaction", startTs, ex);
        }
    }

    /** 回滚失败保留为原异常的附加证据。 */
    private static void rollback(Connection connection, Throwable original) {
        try { connection.rollback(); }
        catch (SQLException | RuntimeException | Error failure) {
            if (original != failure) { original.addSuppressed(failure); }
            // 不能让回滚未知的连接回到池中，后续自动提交或连接复用会破坏本地原子性。
            mvcc.control.MysqlNamedLock.discard(connection, original);
        }
    }

    /** 只接受正逻辑时间戳。 */
    private static void validateTimestamp(long timestamp) {
        if (timestamp <= 0) { throw new IllegalArgumentException("timestamp must be positive"); }
    }

    /** 使用现有二进制键编码规则。 */
    private static byte[] bytes(String key) { return Utf8KeyOrder.encode(key); }

    /** 从协议表还原经过校验的 UTF-8 键。 */
    private static String text(byte[] key) { return new String(key, java.nio.charset.StandardCharsets.UTF_8); }

    /** 构造带原始事务身份的协议异常。 */
    private static MvccException error(ErrorCode code, String message, boolean retryable, long startTs) {
        return new MvccException(code, message, retryable, startTs);
    }

    /** 明确回滚时允许业务重跑整个新事务。 */
    private static MvccException aborted(long startTs, String message) {
        return error(ErrorCode.TRANSACTION_ABORTED, message, true, startTs);
    }

    /** 保留数据库错误；该错误本身不能说明 primary 是否已经提交。 */
    private static MvccException storageError(String operation, Long startTs, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, operation + " failed", true, startTs, cause);
    }

    /** 在同一个本地事务中执行的 SQL 操作。 */
    @FunctionalInterface
    private interface SqlWork<T> {
        /** 执行协议原子操作。 */
        T apply(Connection connection) throws SQLException;
    }
}

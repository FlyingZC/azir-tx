package mvcc.percolator;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.control.MysqlDatabaseAuthority;
import mvcc.maintenance.MaintenanceResult;
import mvcc.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

/** MySQL Percolator 的持久读租约、路由绑定和安全点；提交权威仍保留在 primary。 */
public final class MysqlPercolatorLifecycle implements AutoCloseable {
    private final DataSource control;
    private final Map<String, DataSource> shards;
    private final MysqlTimestampOracle tso;
    private final ThreadLocal<Connection> bound = new ThreadLocal<>();
    private final String namespace;
    private final long leaseMillis;
    private final mvcc.operations.MysqlProtocolState operations;
    private volatile boolean closed;

    /** 初始化持久绑定；已有未绑定协议数据或丢失 TSO 时拒绝猜测恢复。 */
    public MysqlPercolatorLifecycle(DataSource control, Map<String, DataSource> sources, long leaseMillis) {
        this(control, sources, leaseMillis, false);
    }
    /** 只供显式离线升级使用，不能自动接管已经绑定的协议空间。 */
    public static MysqlPercolatorLifecycle adoptLegacyOffline(DataSource control, Map<String, DataSource> sources, long leaseMillis) {
        return new MysqlPercolatorLifecycle(control, sources, leaseMillis, true);
    }
    /** 统一装配并明确区分新空间初始化与离线旧数据接管。 */
    private MysqlPercolatorLifecycle(DataSource control, Map<String, DataSource> sources, long leaseMillis, boolean adopt) {
        this.control = Objects.requireNonNull(control, "control");
        this.shards = Map.copyOf(sources);
        if (shards.isEmpty() || leaseMillis < 1 || leaseMillis > 86_400_000) { throw new IllegalArgumentException("invalid lifecycle configuration"); }
        this.leaseMillis = leaseMillis;
        var routes = new TreeMap<String, String>();
        routes.put("control", MysqlDatabaseAuthority.inspect(control).toString());
        var unique = new HashSet<MysqlDatabaseAuthority.Identity>();
        var physical = new HashSet<MysqlDatabaseAuthority.Identity>();
        shards.forEach((shard, source) -> {
            new PercolatorKey(shard, "");
            var identity = MysqlDatabaseAuthority.inspect(source);
            if (!unique.add(identity)) { throw new IllegalArgumentException("Percolator shards alias the same database"); }
            try (var connection = source.getConnection()) {
                // 组数据源与直连数据源的稳定身份不同，但不能把同一物理库登记为两个业务分片。
                if (!physical.add(MysqlDatabaseAuthority.inspectConnection(connection))) { throw new IllegalArgumentException("Percolator shards currently alias the same physical database"); }
            } catch (SQLException failure) { throw storage(failure); }
            routes.put("shard:" + shard, identity.toString());
        });
        tso = new MysqlTimestampOracle(control);
        // 初始化锁与已有 UUID 无关，避免两个首次启动者创建不一致身份。
        namespace = initialize(routes.toString(), adopt);
        operations = new mvcc.operations.MysqlProtocolState("pc", namespace, control, shards);
    }
    /** 返回持久空间身份，必须随 primary 凭据一起保存。 */
    public String namespace() { return namespace; }
    /** 返回生产写入栅栏，提交权威仍由 primary 保存。 */
    public mvcc.operations.MysqlProtocolState operations() { return operations; }
    /** 在租约与安全点门禁内分配并登记起始快照。 */
    public long begin() {
        return gate(() -> {
            long start = tso.nextTimestamp();
            sql(c -> {
                try (var q = c.prepareStatement("insert into pc_readers(start_ts,expires_at) values(?,unix_timestamp(current_timestamp(3))*1000+?)")) {
                    q.setLong(1, start); q.setLong(2, leaseMillis); q.executeUpdate();
                }
                return null;
            });
            return start;
        });
    }
    /** 拒绝过期、关闭或已越过安全点的快照，在整个操作期间阻止 GC 交错。 */
    public <T> T live(long start, long read, Supplier<T> work) { return live(start, read, work, () -> false); }
    /** 内部提交阶段已经确认 primary 时保留终态，其余操作返回前必须仍有有效读保护。 */
    <T> T live(long start, long read, Supplier<T> work, java.util.function.BooleanSupplier terminal) {
        return gate(() -> {
            requireSnapshot(start, read); T result = work.get();
            try { verifyGate(); if (!terminal.getAsBoolean()) { requireSnapshot(start, read); } }
            catch (RuntimeException failure) { if (!terminal.getAsBoolean()) { throw failure; } }
            return result;
        });
    }
    /** 读租约与安全点在同一控制语句中检查，不能接受已经被新权威回收的快照。 */
    private void requireSnapshot(long start, long read) {
        boolean valid = sql(c -> {
            try (var q = c.prepareStatement("select r.expires_at>unix_timestamp(current_timestamp(3))*1000 and ? >= g.safe_point from pc_readers r join pc_control g on g.id=1 where r.start_ts=?")) {
                q.setLong(1, read); q.setLong(2, start); try (var r = q.executeQuery()) { return r.next() && r.getBoolean(1); }
            }
        });
        if (!valid) { throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD, "Percolator snapshot lease expired or fenced", false, start); }
    }
    /** 结束本地会话的读保护；原 primary 决议不受影响。 */
    public void release(long start) {
        gate(() -> sql(c -> {
            try (var q = c.prepareStatement("delete from pc_readers where start_ts=?")) { q.setLong(1, start); q.executeUpdate(); }
            return null;
        }));
    }

    /** 从控制库查询活跃快照及最老读者，残留锁数由各分片补充。 */
    public mvcc.maintenance.MaintenanceDiagnostics diagnostics(long pendingLocks) {
        return gate(() -> sql(c -> {
            long floor;
            try (var q = c.createStatement(); var r = q.executeQuery("select safe_point from pc_control where id=1")) {
                if (!r.next()) { throw new SQLException("Percolator durable control missing"); } floor = r.getLong(1);
            }
            try (var q = c.createStatement(); var r = q.executeQuery("select coalesce(min(case when expires_at>unix_timestamp(current_timestamp(3))*1000 then start_ts end),0),coalesce(sum(expires_at>unix_timestamp(current_timestamp(3))*1000),0),coalesce(sum(expires_at<=unix_timestamp(current_timestamp(3))*1000),0) from pc_readers")) {
                r.next(); return new mvcc.maintenance.MaintenanceDiagnostics(floor, r.getLong(1), r.getLong(2), r.getLong(3), pendingLocks);
            }
        }));
    }
    /** 有界清理失效租约，按全部活跃读者和未完成锁推进持久安全点。 */
    public MaintenanceResult collect(Map<String, PercolatorStore> stores, int limit, int recovered) {
        return gate(() -> {
            long candidate = tso.nextTimestamp() - 1;
            Long reader = sql(c -> {
                try (var q = c.prepareStatement("delete from pc_readers where expires_at<=unix_timestamp(current_timestamp(3))*1000 order by start_ts limit ?")) {
                    q.setInt(1, limit); q.executeUpdate();
                }
                try (var q = c.createStatement(); var r = q.executeQuery("select min(start_ts) from pc_readers where expires_at>unix_timestamp(current_timestamp(3))*1000")) {
                    r.next(); long value = r.getLong(1); return r.wasNull() ? null : value;
                }
            });
            if (reader != null) { candidate = Math.min(candidate, reader - 1); }
            for (var store : stores.values()) {
                for (var lock : store.pending(1)) { candidate = Math.min(candidate, lock.startTs() - 1); }
            }
            long proposed = candidate;
            long floor = sql(c -> {
                try (var q = c.prepareStatement("update pc_control set safe_point=greatest(safe_point,?) where id=1")) {
                    q.setLong(1, proposed); if (q.executeUpdate() != 1) { throw new SQLException("Percolator control state missing"); }
                }
                try (var q = c.createStatement(); var r = q.executeQuery("select safe_point from pc_control where id=1")) { r.next(); return r.getLong(1); }
            });
            int deleted = 0;
            for (var store : stores.values()) { deleted += store.collect(floor, limit); }
            new mvcc.operations.MysqlProtocolAdmin(operations, ignored -> 0).retireLocked(bound.get(), floor, limit);
            return new MaintenanceResult(recovered, deleted, floor);
        });
    }
    /** 同一命名空间跨进程共享门禁；丢失原会话后不能自动改借连接继续原操作。 */
    public <T> T gate(Supplier<T> work) {
        if (closed) { throw new IllegalStateException("Percolator runtime closed"); }
        if (bound.get() != null) { verifyGate(); return work.get(); }
        return mvcc.operations.MysqlProtocolState.admitted("azir:pc:" + namespace, 10, () -> {
        try (var c = control.getConnection()) {
            requireAutoCommit(c);
            String name = "azir:pc:" + namespace;
            try (var authority = mvcc.control.MysqlNamedLock.lock(c, name, 10)) {
                bound.set(c);
                try (var owner = operations.enterControl(c)) { T result = work.get(); verifyGate(); return result; }
                finally { bound.remove(); }
            }
        } catch (SQLException failure) { throw storage(failure); }
        });
    }
    /** 每次嵌套操作确认命名锁还属于原数据库会话。 */
    private void verifyGate() {
        sql(c -> {
            try (var q = c.prepareStatement("select is_used_lock(?)=connection_id()")) {
                q.setString(1, "azir:pc:" + namespace);
                try (var r = q.executeQuery()) { if (!r.next() || !r.getBoolean(1)) { throw new SQLException("Percolator lifecycle gate lost"); } }
            }
            return null;
        });
    }
    /** 控制 SQL 与运行代次行锁在同一短事务内提交，换代不能与迟到的读者清理交错。 */
    private <T> T sql(SqlWork<T> work) {
        Connection c = bound.get();
        if (c == null) { throw new IllegalStateException("Percolator lifecycle gate required"); }
        try {
            if (!c.getAutoCommit()) {
                try { return work.run(c); }
                catch (SQLException | RuntimeException | Error failure) { operations.invalidateControl(); throw failure; }
            }
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); c.setAutoCommit(false);
            boolean ended = false; Throwable original = null;
            try { operations.lockControl(c); T value = work.run(c); operations.verifyOwner(c); c.commit(); ended = true; return value; }
            catch (SQLException | RuntimeException | Error failure) {
                original = failure; operations.invalidateControl();
                try { c.rollback(); ended = true; }
                catch (SQLException | RuntimeException | Error rollback) { if (failure != rollback) { failure.addSuppressed(rollback); } mvcc.control.MysqlNamedLock.discard(c, failure); }
                throw failure;
            } finally {
                if (ended) {
                    try { c.setAutoCommit(true); }
                    catch (SQLException | RuntimeException | Error reset) {
                        operations.invalidateControl(); mvcc.control.MysqlNamedLock.discard(c, reset);
                        if (original == null) { throw reset; }
                        if (original != reset) { original.addSuppressed(reset); }
                    }
                }
            }
        } catch (SQLException failure) {
            operations.invalidateControl(); mvcc.control.MysqlNamedLock.discard(c, failure); throw storage(failure);
        } catch (RuntimeException | Error failure) {
            operations.invalidateControl(); mvcc.control.MysqlNamedLock.discard(c, failure); throw failure;
        }
    }
    /** 控制 SQL 回调只接收当前门禁持有的连接。 */
    @FunctionalInterface private interface SqlWork<T> {
        /** 执行一个有限控制步骤。 */
        T run(Connection connection) throws SQLException;
    }
    /** 串行首次装配，旧数据需要显式迁移，不能在线推断其时间线。 */
    private String initialize(String routes, boolean adopt) {
        String initName = "azir:pc:init:" + UUID.nameUUIDFromBytes(routes.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try (var c = control.getConnection()) {
            requireAutoCommit(c);
            try (var initialization = mvcc.control.MysqlNamedLock.lock(c, initName, 10)) {
                // 恢复日志不参与数据替换；未完成导入时不能由普通启动补写配置或 TSO。
                mvcc.operations.MysqlProtocolRestore.requireNoImport(control, "pc");
                try (var q = c.createStatement()) {
                    q.execute("create table if not exists pc_control(id int primary key,namespace varchar(36) not null,routes text not null,safe_point bigint not null) engine=InnoDB");
                    q.execute("create table if not exists pc_readers(start_ts bigint primary key,expires_at bigint not null,index expiry(expires_at)) engine=InnoDB");
                }
                String identity = null;
                try (var q = c.createStatement(); var r = q.executeQuery("select namespace,routes from pc_control where id=1")) {
                    if (r.next()) {
                        identity = r.getString(1);
                        if (!routes.equals(r.getString(2))) { throw new IllegalArgumentException("Percolator persistent routes or control authority changed"); }
                    }
                }
                if (identity != null) {
                    if (adopt) { throw new IllegalArgumentException("Percolator is already bound; refuse legacy adoption"); }
                    try (var q = c.prepareStatement("select current_value from mvcc_tso where sequence_name='global'" ); var r = q.executeQuery()) {
                        if (!r.next()) { throw new IllegalArgumentException("Percolator TSO missing; refuse reset"); }
                    }
                } else {
                    if (adopt) {
                        mvcc.operations.MysqlProtocolUpgrade.validateLegacy(control, shards);
                    } else {
                        for (var source : shards.values()) { requireEmptyUnbound(source); }
                        tso.initialize(100);
                    }
                    identity = UUID.randomUUID().toString();
                    try (var q = c.prepareStatement("insert into pc_control values(1,?,?,0)")) {
                        q.setString(1, identity); q.setString(2, routes); q.executeUpdate();
                    }
                }
                for (var entry : shards.entrySet()) { bind(entry.getValue(), entry.getKey(), identity); }
                return identity;
            }
        } catch (SQLException failure) { throw storage(failure); }
    }
    /** 无控制身份时只能绑定空协议空间，避免旧模型与新租约机制混用。 */
    private static void requireEmptyUnbound(DataSource source) throws SQLException {
        try (var c = source.getConnection(); var q = c.createStatement()) {
            requireAutoCommit(c);
            for (String table : List.of("pc_data", "pc_lock", "pc_write", "pc_rollback", "pc_guard")) {
                boolean exists;
                try (var check = c.prepareStatement("select count(*) from information_schema.tables where table_schema=database() and table_name=?")) {
                    check.setString(1, table); try (var r = check.executeQuery()) { r.next(); exists = r.getInt(1) != 0; }
                }
                if (exists) { try (var r = q.executeQuery("select exists(select 1 from " + table + " limit 1)")) { r.next(); if (r.getBoolean(1)) { throw new IllegalArgumentException("unbound Percolator data requires offline migration"); } } }
            }
        }
    }
    /** 分片绑定同时约束控制命名空间与逻辑分片名，拒绝控制库错配。 */
    private static void bind(DataSource source, String shard, String namespace) throws SQLException {
        try (var c = source.getConnection(); var q = c.createStatement()) {
            requireAutoCommit(c);
            q.execute("create table if not exists pc_binding(id int primary key,namespace varchar(36) not null,shard varchar(64) not null) engine=InnoDB");
            try (var insert = c.prepareStatement("insert into pc_binding values(1,?,?) on duplicate key update id=id")) {
                insert.setString(1, namespace); insert.setString(2, shard); insert.executeUpdate();
            }
            try (var r = q.executeQuery("select namespace,shard from pc_binding where id=1")) {
                if (!r.next() || !namespace.equals(r.getString(1)) || !shard.equals(r.getString(2))) { throw new IllegalArgumentException("Percolator shard/control binding mismatch"); }
            }
        }
    }
    /** 生命周期协议只允许借用默认自动提交的连接。 */
    private static void requireAutoCommit(Connection c) throws SQLException {
        if (!c.getAutoCommit()) { throw new IllegalArgumentException("Percolator requires auto-commit pools"); }
    }
    /** 保留数据库故障原因，由提交层判定结果是否未知。 */
    private static MvccException storage(SQLException failure) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "Percolator lifecycle JDBC operation failed", true, null, failure);
    }
    /** 阻止新操作，借用的池继续由应用管理。 */
    @Override public void close() { closed = true; }
}

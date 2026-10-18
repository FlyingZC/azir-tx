package mvcc.operations;

import mvcc.common.*;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** 持久运行代次与分片写入栅栏；控制会话失联后，迟到 SQL 仍受本地水位保护。 */
public final class MysqlProtocolState {
    private static final ReentrantLock[] ADMISSION = new ReentrantLock[256];
    private static final ThreadLocal<String> ADMIN = new ThreadLocal<>();
    static { Arrays.setAll(ADMISSION, ignored -> new ReentrantLock(true)); }
    private final String prefix;
    private final String namespace;
    private final long generation;
    private final DataSource control;
    private final Map<String, DataSource> shards;
    private final ThreadLocal<ControlOwner> controlOwner = new ThreadLocal<>();

    /** 当前执行流拥有的脉冲值，失败事务不能因回滚后的数值偶合重新夺取所有权。 */
    private static final class ControlOwner {
        private long pulse;
        private boolean invalid;
        private final Connection connection;
        /** 只保存已取得的控制脉冲，后续更新必须按预期值比较交换。 */
        private ControlOwner(long pulse, Connection connection) { this.pulse = pulse; this.connection = connection; }
    }
    /** 同一线程内的控制所有者作用域，关闭只移除本地令牌，不修改持久状态。 */
    public final class ControlScope implements AutoCloseable {
        private final ControlOwner owner;
        private final Thread thread = Thread.currentThread();
        private boolean closed;
        /** 不允许从外部伪造已经取得的控制所有权。 */
        private ControlScope(ControlOwner owner) { this.owner = owner; }
        /** 只在原执行线程退出作用域，避免误清除另一条执行流的所有权。 */
        @Override public void close() {
            if (Thread.currentThread() != thread) { throw new IllegalStateException("control scope belongs to another thread"); }
            if (!closed) {
                if (controlOwner.get() != owner) { throw new IllegalStateException("control scopes closed out of order"); }
                closed = true; controlOwner.remove();
            }
        }
    }
    /** 持久状态，冻结和交接状态都不允许普通客户端继续服务。 */
    public record Status(String namespace, long generation, String mode, long retiredBefore,
                         long receiptRetentionMillis, String checkpoint, String destination, boolean freezeReady) {
        /** 损坏的角色身份、水位或模式必须停止使用，不能继续套用默认代次和默认保留期。 */
        public Status {
            UUID.fromString(namespace); if (checkpoint != null) { UUID.fromString(checkpoint); }
            if (generation < 1 || retiredBefore < 0 || receiptRetentionMillis < 0 || receiptRetentionMillis > 3_153_600_000_000L
                    || !Set.of("ACTIVE", "FROZEN", "RESTORING", "STAGED", "HANDED_OFF").contains(mode)
                    || (!mode.equals("ACTIVE") && checkpoint == null)) { throw new IllegalArgumentException("corrupt protocol runtime state"); }
        }
    }

    /** 初始化版本二的运行栅栏；代次固定在当前对象，恢复后必须重新创建客户端。 */
    public MysqlProtocolState(String prefix, String namespace, DataSource control, Map<String, DataSource> shards) {
        this(prefix, namespace, control, shards, true);
    }
    /** 只读装载已有管理状态，允许工具修复局部代次切换，不执行初始化。 */
    public static MysqlProtocolState existing(String prefix, String namespace, DataSource control, Map<String, DataSource> shards) {
        return new MysqlProtocolState(prefix, namespace, control, shards, false);
    }
    /** 业务启动允许受控升级，运维诊断只装载原有栅栏。 */
    private MysqlProtocolState(String prefix, String namespace, DataSource control, Map<String, DataSource> shards, boolean initialize) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix)) { throw new IllegalArgumentException("invalid protocol"); }
        this.prefix = prefix; this.namespace = namespace; this.control = control; this.shards = Map.copyOf(shards);
        this.shards.keySet().forEach(id -> { if (!id.matches("[A-Za-z0-9_-]{1,64}")) { throw new IllegalArgumentException("invalid shard id"); } });
        String config = prefix.equals("pc") ? "pc_control" : prefix + "_config";
        boolean initialized = !initialize || operationsInitialized(config);
        if (initialize) { initialize(control, "control", initialized); }
        Status status = read(control, "control", false);
        generation = status.generation();
        if (initialize) { shards.forEach((id, source) -> initialize(source, "shard:" + id, initialized)); }
        for (var entry : shards.entrySet()) {
            Status data = read(entry.getValue(), "shard:" + entry.getKey(), false);
            if ((status.mode().equals("ACTIVE") && data.generation() != generation) || (!data.namespace().equals(namespace) && !namespaceRepairAllowed(data.namespace()))) { throw new IllegalStateException("incomplete protocol generation transition; use administration recovery"); }
        }
        if (initialize) {
            try (var c = control.getConnection(); var q = c.createStatement()) { q.executeUpdate("update " + config + " set operations_version=2 where id=1"); }
            catch (SQLException failure) { throw storage(failure); }
        }
    }
    /** 返回协议前缀。 */
    public String prefix() { return prefix; }
    /** 返回持久命名空间。 */
    public String namespace() { return namespace; }
    /** 返回创建客户端时固定的运行代次。 */
    public long generation() { return generation; }
    /** 返回本对象使用的受控连接池。 */
    public DataSource control() { return control; }
    /** 返回不可变分片路由。 */
    public Map<String, DataSource> shards() { return shards; }
    /** 返回与正常提交一致的数据库命名锁。 */
    public String gateName() { return "azir:" + prefix + ":" + namespace; }
    /** 取得组复制控制所有者令牌；普通固定实例依赖原命名锁，返回空资源。 */
    public ControlScope enterControl(Connection connection) throws SQLException {
        if (controlOwner.get() != null) { throw new IllegalStateException("nested control scope must reuse its owner"); }
        if (!connection.getAutoCommit()) { throw new SQLException("control owner requires an auto-commit authority connection"); }
        try (var q = connection.prepareStatement("select is_used_lock(?)=connection_id()")) {
            q.setString(1, gateName()); try (var r = q.executeQuery()) { if (!r.next() || !r.getBoolean(1)) { throw new SQLException("control owner requires the original named lock"); } }
        }
        requireControl(connection);
        if (!grouped(control)) { return null; }
        ControlOwner owner = new ControlOwner(lastPulse(connection), connection); ControlScope scope = new ControlScope(owner);
        try { controlOwner.set(owner); return scope; }
        catch (RuntimeException | Error failure) { controlOwner.remove(); throw failure; }
    }
    /** 回滚或提交未知后封闭原作用域，必须退出门禁再查询结果，不能重用暂存脉冲。 */
    public void invalidateControl() { ControlOwner owner = controlOwner.get(); if (owner != null) { owner.invalid = true; } }
    /** 在分片本地提交前重新确认原控制执行流，原会话回到主节点也不能夺回已被替代的所有权。 */
    public void requireDataPublication() throws SQLException {
        ControlOwner owner = controlOwner.get(); checkOwner(owner); if (owner == null) { return; }
        if (!owner.connection.getAutoCommit()) { throw new SQLException("data publication requires committed control metadata"); }
        requireControl(owner.connection);
    }
    /** 子本地事务内只读核对所有权，避免在另一连接已经持有控制行锁时再次写入。 */
    public void verifyOwner(Connection connection) throws SQLException {
        ControlOwner owner = controlOwner.get(); checkOwner(owner); if (owner == null) { return; }
        try (var q = connection.prepareStatement("select pulse from " + prefix + "_runtime where role='control' and namespace=? and generation=?")) {
            q.setString(1, namespace); q.setLong(2, generation);
            try (var r = q.executeQuery()) { if (!r.next() || r.getLong(1) != owner.pulse) { owner.invalid = true; throw new SQLException("control owner was replaced"); } }
        }
    }
    /** 在控制门禁内检查当前对象与所有者，旧节点重新当选也不能继续原执行流。 */
    public void requireControl(Connection c) throws SQLException {
        ControlOwner owner = controlOwner.get(); checkOwner(owner);
        Status current = read(c, "control", false); require(current);
        if (grouped(control)) {
            try (var q = c.createStatement(); var r = q.executeQuery("select member_state='ONLINE' and member_role='PRIMARY' from performance_schema.replication_group_members where member_id=@@server_uuid")) {
                if (!r.next() || !r.getBoolean(1)) { throw new SQLException("control group primary changed"); }
            }
            String compare = owner == null ? "" : " and pulse=?";
            try (var q = c.prepareStatement("update " + prefix + "_runtime set pulse=last_insert_id(pulse+1) where role='control' and namespace=? and generation=? and mode=? and pulse<9223372036854775807" + compare)) {
                q.setString(1, namespace); q.setLong(2, generation); q.setString(3, current.mode()); if (owner != null) { q.setLong(4, owner.pulse); }
                if (q.executeUpdate() != 1) { invalidateControl(); throw new SQLException("control owner changed or pulse exhausted"); }
            }
            if (owner != null) { owner.pulse = lastPulse(c); }
        }
    }
    /** 只读取当前会话自己写入的脉冲，不把另一执行流随后更新的计数当作本地所有权。 */
    private static long lastPulse(Connection connection) throws SQLException {
        try (var q = connection.createStatement(); var r = q.executeQuery("select last_insert_id()")) {
            if (!r.next() || r.getLong(1) <= 0) { throw new SQLException("control pulse result is invalid"); } return r.getLong(1);
        }
    }
    /** 失败作用域即使碰巧与别人的计数相同，也不能恢复使用。 */
    private static void checkOwner(ControlOwner owner) throws SQLException { if (owner != null && owner.invalid) { throw new SQLException("control owner is invalidated"); } }
    /** 显式组复制数据源及其受控包装器使用提交写集栅栏，普通固定实例保留本地共享行锁。 */
    static boolean grouped(DataSource source) {
        return source instanceof mvcc.control.MysqlGroupDataSource
                || (source instanceof mvcc.control.ProtocolMysqlDataSource protocol && protocol.groupAuthority() != null);
    }
    /** 控制元数据也持有本地运行栅栏，迟到的旧会话不能删改新命名空间中复用的读者编号。 */
    public void lockControl(Connection connection) throws SQLException {
        if (connection.getAutoCommit()) { throw new IllegalStateException("control fence requires a transaction"); }
        require(read(connection, "control", true)); requireControl(connection);
    }
    /** 本地事务先持有共享栅栏行锁，再写版本；退休和冻结使用排他锁等待所有旧 SQL 结束。 */
    public long lockData(Connection c, String shard, long start, boolean terminalReplay) throws SQLException {
        if (c.getAutoCommit()) { throw new IllegalStateException("local fence requires a transaction"); }
        Status status = read(c, "shard:" + shard, true);
        require(status);
        if (!terminalReplay && start <= status.retiredBefore()) { throw expired(start); }
        certify(c, "shard:" + shard, status);
        return status.retiredBefore();
    }
    /** 组复制的每个本地写批次都写入同一栅栏行，覆盖业务写、GC 和管理清理的认证顺序。 */
    void certify(Connection connection, String role, Status observed) throws SQLException {
        if (connection.getAutoCommit()) { throw new IllegalStateException("group fence certification requires a transaction"); }
        DataSource source = role.equals("control") ? control : role.startsWith("shard:") ? shards.get(role.substring(6)) : null;
        if (source == null) { throw new IllegalArgumentException("unconfigured fence role"); }
        if (!grouped(source)) { return; }
        ControlOwner owner = controlOwner.get(); checkOwner(owner);
        boolean ownControl = role.equals("control") && owner != null;
        String value = ownControl ? "last_insert_id(pulse+1)" : "pulse+1";
        String compare = ownControl ? " and pulse=?" : "";
        // 读锁不进入组复制认证写集，脉冲必须与版本或候选索引的变更一起提交。
        try (var q = connection.prepareStatement("update " + prefix + "_runtime set pulse=" + value + " where role=? and namespace=? and generation=? and mode=? and retired_before=? and pulse<9223372036854775807" + compare)) {
            q.setString(1, role); q.setString(2, observed.namespace()); q.setLong(3, observed.generation()); q.setString(4, observed.mode()); q.setLong(5, observed.retiredBefore()); if (ownControl) { q.setLong(6, owner.pulse); }
            if (q.executeUpdate() != 1) { invalidateControl(); throw new SQLException("group runtime fence changed or exhausted"); }
        }
        if (ownControl) { owner.pulse = lastPulse(connection); }
    }
    /** 查询当前持久状态，不把本地缓存当作运行权威。 */
    public Status status() { return read(control, "control", false); }
    /** 仅用于已确认的单项管理步骤，业务入口不能持有此作用域跨越代次切换。 */
    <T> T administrative(Supplier<T> work) {
        String before = ADMIN.get(); ADMIN.set(prefix + ":" + namespace + ":" + generation);
        try { return work.get(); } finally { if (before == null) { ADMIN.remove(); } else { ADMIN.set(before); } }
    }
    /** 与业务客户端共享进程内等待预算，避免排队者先占满连接池。 */
    public static <T> T admitted(String name, int seconds, Supplier<T> work) {
        ReentrantLock lock = ADMISSION[(name.hashCode() & Integer.MAX_VALUE) % ADMISSION.length];
        try {
            if (!lock.tryLock(seconds, TimeUnit.SECONDS)) { throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT, "protocol admission timed out", true, null); }
        } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException("protocol admission interrupted", failure); }
        try { return work.get(); } finally { lock.unlock(); }
    }
    /** 每轮后台任务在项边界响应关闭，不能开始下一项阻塞 SQL。 */
    public static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) { throw new IllegalStateException("protocol operation interrupted"); }
    }
    /** 已退休凭据的迟到写必须失败，不能自动恢复成新的 ACTIVE 事务。 */
    public static MvccException expired(long start) { return new MvccException(ErrorCode.SNAPSHOT_TOO_OLD, "transaction identity retired", false, start); }
    /** 构造并校验当前协议的本地持久栅栏表。 */
    private void initialize(DataSource source, String role, boolean alreadyInitialized) {
        try (var c = source.getConnection(); var q = c.createStatement()) {
            q.execute("create table if not exists " + prefix + "_runtime(role varchar(70) character set ascii collate ascii_bin primary key,namespace varchar(36) not null,generation bigint not null,mode varchar(20) not null,retired_before bigint not null,retention_ms bigint not null,checkpoint varchar(36) null,destination text null,retire_cursor bigint not null default 0,pulse bigint not null default 0,freeze_ready boolean not null default false) engine=InnoDB");
            try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='retire_cursor'")) {
                check.setString(1, prefix + "_runtime"); boolean present; try (var r = check.executeQuery()) { r.next(); present = r.getInt(1) > 0; }
                if (!present) { q.execute("alter table " + prefix + "_runtime add column retire_cursor bigint not null default 0"); }
            }
            try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='pulse'")) {
                check.setString(1, prefix + "_runtime"); boolean present; try (var r = check.executeQuery()) { r.next(); present = r.getInt(1) > 0; }
                if (!present) { q.execute("alter table " + prefix + "_runtime add column pulse bigint not null default 0"); }
            }
            try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='freeze_ready'")) {
                check.setString(1, prefix + "_runtime"); boolean present; try (var r = check.executeQuery()) { r.next(); present = r.getInt(1) > 0; }
                if (!present) { q.execute("alter table " + prefix + "_runtime add column freeze_ready boolean not null default false"); }
            }
            if (alreadyInitialized) {
                read(c, role, false);
            }
            try (var insert = c.prepareStatement("insert into " + prefix + "_runtime(role,namespace,generation,mode,retired_before,retention_ms,checkpoint,destination) values(?,?,1,'ACTIVE',0,604800000,null,null) on duplicate key update role=role")) {
                insert.setString(1, role); insert.setString(2, namespace); insert.executeUpdate();
            }
            Status found = read(c, role, false);
            if (!namespace.equals(found.namespace()) && !namespaceRepairAllowed(found.namespace())) { throw new IllegalArgumentException("protocol fence namespace mismatch"); }
        } catch (SQLException failure) { throw storage(failure); }
    }
    /** 首次版本升级建立标志；标志存在后，任何缺失的角色栅栏都不能重建为零。 */
    private boolean operationsInitialized(String config) {
        try (var c = control.getConnection(); var q = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='operations_version'")) {
            q.setString(1, config); boolean exists; try (var r = q.executeQuery()) { r.next(); exists = r.getInt(1) > 0; }
            if (!exists) { try (var ddl = c.createStatement()) { ddl.execute("alter table " + config + " add column operations_version int not null default 1"); } }
            try (var query = c.createStatement(); var r = query.executeQuery("select operations_version from " + config + " where id=1")) { if (!r.next()) { throw new SQLException("protocol configuration missing"); } return r.getInt(1) >= 2; }
        } catch (SQLException failure) { throw storage(failure); }
    }
    /** 仅允许已持久化的新命名空间激活计划修复局部完成，普通入口仍被 STAGED 阻止。 */
    private boolean namespaceRepairAllowed(String value) {
        Status current = read(control, "control", false); String plan = current.destination();
        if (!current.mode().equals("STAGED") || plan == null || !plan.startsWith("new-namespace:")) { return false; }
        String[] parts = plan.split(":");
        return parts.length == 4 && (namespace.equals(parts[1]) || namespace.equals(parts[2])) && (value.equals(parts[1]) || value.equals(parts[2]));
    }
    /** 统一检查代次与冻结状态，管理恢复也不能跨代次重放。 */
    private void require(Status status) {
        boolean administrative = (prefix + ":" + namespace + ":" + generation).equals(ADMIN.get());
        if (!namespace.equals(status.namespace()) || generation != status.generation()
                || (!status.mode().equals("ACTIVE") && !(administrative && status.mode().equals("FROZEN") && !MysqlProtocolAdmin.transitionPending(status)))) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "protocol frozen, staged, handed off or generation changed; reopen after administration", false, null);
        }
    }
    /** 在调用者本地事务中读取栅栏，共享锁与退休排他锁互斥。 */
    Status read(Connection c, String role, boolean lock) throws SQLException {
        try (var q = c.prepareStatement("select namespace,generation,mode,retired_before,retention_ms,checkpoint,destination,freeze_ready from " + prefix + "_runtime where role=?" + (lock ? " for share" : ""))) {
            q.setString(1, role);
            try (var r = q.executeQuery()) {
                if (!r.next()) { throw new SQLException("protocol fence missing; refuse reinitialization"); }
                return new Status(r.getString(1), r.getLong(2), r.getString(3), r.getLong(4), r.getLong(5), r.getString(6), r.getString(7), r.getBoolean(8));
            }
        }
    }
    /** 诊断或初始化时借用独立连接，不改变协议状态。 */
    Status read(DataSource source, String role, boolean lock) {
        try (var c = source.getConnection()) { return read(c, role, lock); }
        catch (SQLException failure) { throw storage(failure); }
    }
    /** 保留 SQL 失败，提交层另外判定原事务是否未知。 */
    public static MvccException storage(SQLException failure) { return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "protocol operations JDBC failure", true, null, failure); }
}

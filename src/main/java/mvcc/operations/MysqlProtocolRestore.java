package mvcc.operations;

import mvcc.control.MysqlDatabaseAuthority;
import mvcc.control.ProtocolMysqlDataSource;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import static mvcc.operations.MysqlProtocolState.storage;

/** 可重入的离线恢复任务；独立日志和分片执行栅栏不参与备份数据替换。 */
public final class MysqlProtocolRestore {
    /** 只包含运维身份和进度的持久任务，不记录业务内容或数据库凭据。 */
    public record Job(String sha256, String sourceNamespace, long sourceGeneration, String checkpoint,
                      String targetNamespace, long targetGeneration, long epoch, String state,
                      String role, String table, long updatedAt) {
        /** 损坏的管理日志必须拒绝使用，不能据此重新开放目标或猜测执行代次。 */
        public Job {
            UUID.fromString(sourceNamespace); UUID.fromString(targetNamespace); UUID.fromString(checkpoint);
            if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || sourceNamespace.equals(targetNamespace)
                    || sourceGeneration < 1 || targetGeneration <= sourceGeneration || epoch < 0 || updatedAt < 0
                    || !Set.of("IMPORTING", "STAGED").contains(state) || (state.equals("STAGED") && epoch == 0)
                    || (role == null) != (table == null)) { throw new IllegalArgumentException("invalid restore job"); }
        }
    }

    /** 禁止实例化恢复工具。 */
    private MysqlProtocolRestore() { }

    /** 正常启动在初始化任何协议表前拒绝尚未完成的恢复，防止补写初始化默认值。 */
    public static void requireNoImport(DataSource control, String prefix) {
        requirePrefix(prefix);
        try (var c = control.getConnection()) {
            if (!exists(c, prefix + "_restore")) { return; }
            Job job = read(c, prefix);
            if (job != null && job.state().equals("IMPORTING")) { throw new IllegalStateException("restore is incomplete; use resume-stage with the original archive"); }
        } catch (SQLException failure) { throw storage(failure); }
    }

    /** 即使运行栅栏正在被恢复，也能只读查询独立的恢复任务日志。 */
    public static Job status(DataSource control, String prefix) {
        requirePrefix(prefix);
        try (var c = control.getConnection()) { return exists(c, prefix + "_restore") ? read(c, prefix) : null; }
        catch (SQLException failure) { throw storage(failure); }
    }

    /** 创建任务之前仍由真实后端完成冻结和清理，之后才允许替换数据。 */
    static Session start(MysqlProtocolAdmin admin, MysqlProtocolBackup.Archive archive, Runnable requireEmpty) {
        var session = new Session(admin.state().prefix(), admin.state().control(), admin.state().shards());
        try {
            session.createJournal();
            if (read(session.gate, session.prefix) != null) { throw new IllegalStateException("restore job already exists; use resume-stage"); }
            requireEmpty.run();
            var checkpoint = admin.freeze();
            admin.locked(() -> {
                admin.requireFrozen(checkpoint); requireEmpty.run();
                try {
                    session.requireHeld();
                    transaction(session.control, c -> {
                        try (var q = c.prepareStatement("select namespace,generation,mode,checkpoint,freeze_ready from " + session.prefix + "_runtime where role='control' for update"); var r = q.executeQuery()) {
                            if (!r.next() || !checkpoint.namespace().equals(r.getString(1)) || checkpoint.generation() != r.getLong(2) || !r.getString(3).equals("FROZEN") || !checkpoint.id().equals(r.getString(4)) || !r.getBoolean(5)) { throw new SQLException("restore target is no longer a complete frozen checkpoint"); }
                        }
                        try (var q = c.prepareStatement("insert into " + session.prefix + "_restore(id,archive_sha,source_namespace,source_generation,checkpoint,target_namespace,target_generation,routes,epoch,state,progress_role,progress_table,updated_at) values(1,?,?,?,?,?,?,?,0,'IMPORTING',null,null,unix_timestamp(current_timestamp(3))*1000)")) {
                            q.setString(1, archive.sha256()); q.setString(2, archive.checkpoint().namespace()); q.setLong(3, archive.checkpoint().generation()); q.setString(4, archive.checkpoint().id());
                            q.setString(5, checkpoint.namespace()); q.setLong(6, Math.addExact(Math.max(checkpoint.generation(), archive.checkpoint().generation()), 1)); q.setString(7, session.routes); q.executeUpdate();
                        }
                        // 日志与控制停服状态原子发布，不能留下可被普通 resume 激活的空隙。
                        try (var q = c.createStatement()) { if (q.executeUpdate("update " + session.prefix + "_runtime set mode='RESTORING',freeze_ready=false where role='control'") != 1) { throw new SQLException("restore control fence missing"); } }
                    });
                    return null;
                } catch (SQLException failure) { throw storage(failure); }
            });
            session.claim(archive, () -> { }); return session;
        } catch (SQLException | RuntimeException | Error failure) { session.closeAfter(failure); if (failure instanceof SQLException sql) { throw storage(sql); } if (failure instanceof RuntimeException runtime) { throw runtime; } throw (Error) failure; }
    }

    /** 原目标重试只依赖任务日志，不依赖可能已被部分替换的配置和运行栅栏。 */
    static Session resume(String prefix, DataSource control, Map<String, DataSource> shards, MysqlProtocolBackup.Archive archive, Runnable preflight) {
        var session = new Session(prefix, control, shards);
        try { session.claim(archive, preflight); return session; }
        catch (SQLException | RuntimeException | Error failure) { session.closeAfter(failure); if (failure instanceof SQLException sql) { throw storage(sql); } if (failure instanceof RuntimeException runtime) { throw runtime; } throw (Error) failure; }
    }

    /** 固定协议名参与 SQL 标识符，不能接受外部任意前缀。 */
    private static void requirePrefix(String prefix) { if (!Set.of("pc", "omid", "atlas").contains(prefix)) { throw new IllegalArgumentException("invalid protocol"); } }

    /** 元数据存在性查询不创建表，避免只读状态命令改变目标。 */
    private static boolean exists(Connection c, String table) throws SQLException {
        try (var q = c.prepareStatement("select count(*) from information_schema.tables where table_schema=database() and table_name=?")) {
            q.setString(1, table); try (var r = q.executeQuery()) { r.next(); return r.getInt(1) != 0; }
        }
    }

    /** 固定单任务日志与备份表独立，任何导入都不能覆盖此处的执行代次。 */
    private static Job read(Connection c, String prefix) throws SQLException {
        try (var q = c.createStatement(); var r = q.executeQuery("select archive_sha,source_namespace,source_generation,checkpoint,target_namespace,target_generation,epoch,state,progress_role,progress_table,updated_at from " + prefix + "_restore where id=1")) {
            return r.next() ? new Job(r.getString(1), r.getString(2), r.getLong(3), r.getString(4), r.getString(5), r.getLong(6), r.getLong(7), r.getString(8), r.getString(9), r.getString(10), r.getLong(11)) : null;
        }
    }

    /** 一次有界执行的持锁会话，所有迟到导入批次都必须再次通过分片代次检查。 */
    static final class Session implements AutoCloseable {
        final String prefix;
        final DataSource control;
        final Map<String, DataSource> roles;
        final Map<String, DataSource> shards;
        final String routes;
        private final Connection gate;
        private final LinkedHashMap<String, mvcc.control.MysqlNamedLock.Lease> locks = new LinkedHashMap<>();
        private Job job;

        /** 以物理控制库绑定稳定恢复锁，命名空间被导入替换也不会改变锁名。 */
        Session(String prefix, DataSource control, Map<String, DataSource> shards) {
            requirePrefix(prefix); this.prefix = prefix;
            this.control = new ProtocolMysqlDataSource(control);
            var data = new TreeMap<String, DataSource>(); var identities = new TreeMap<String, String>();
            identities.put("control", MysqlDatabaseAuthority.inspect(this.control).toString());
            var unique = new HashSet<MysqlDatabaseAuthority.Identity>(); var physical = new HashSet<MysqlDatabaseAuthority.Identity>();
            shards.forEach((id, source) -> {
                if (!id.matches("[A-Za-z0-9_-]{1,64}")) { throw new IllegalArgumentException("invalid shard id"); }
                var safe = new ProtocolMysqlDataSource(source); var identity = MysqlDatabaseAuthority.inspect(safe);
                if (!unique.add(identity)) { throw new IllegalArgumentException("restore shards alias the same database"); }
                try (var connection = safe.getConnection()) {
                    if (!physical.add(MysqlDatabaseAuthority.inspectConnection(connection))) { throw new IllegalArgumentException("restore shards currently alias the same physical database"); }
                } catch (SQLException failure) { throw storage(failure); }
                data.put(id, safe); identities.put("shard:" + id, identity.toString());
            });
            if (data.isEmpty()) { throw new IllegalArgumentException("restore requires shards"); }
            this.shards = Collections.unmodifiableMap(data); routes = identities.toString();
            var ordered = new LinkedHashMap<String, DataSource>(); ordered.put("control", this.control); data.forEach((id, source) -> ordered.put("shard:" + id, source));
            roles = Collections.unmodifiableMap(ordered);
            Connection acquired = null;
            try {
                acquired = this.control.getConnection(); gate = acquired;
                acquire("azir:restore:" + prefix + ":" + UUID.nameUUIDFromBytes(identities.get("control").getBytes(StandardCharsets.UTF_8)));
            } catch (SQLException | RuntimeException | Error failure) {
                if (acquired != null) { try { acquired.abort(Runnable::run); } catch (SQLException abort) { failure.addSuppressed(abort); } try { acquired.close(); } catch (SQLException close) { failure.addSuppressed(close); } }
                if (failure instanceof SQLException sql) { throw storage(sql); } if (failure instanceof RuntimeException runtime) { throw runtime; } throw (Error) failure;
            }
        }

        /** 固定恢复日志包含原始空目标身份，不允许后续导入覆盖其路由或任务摘要。 */
        private void createJournal() throws SQLException {
            requireHeld();
            try (var q = gate.createStatement()) {
                q.execute("create table if not exists " + prefix + "_restore(id int primary key,archive_sha char(64) character set ascii collate ascii_bin not null,source_namespace varchar(36) not null,source_generation bigint not null,checkpoint varchar(36) not null,target_namespace varchar(36) not null,target_generation bigint not null,routes text not null,epoch bigint not null,state varchar(20) not null,progress_role varchar(70) null,progress_table varchar(64) null,updated_at bigint not null) engine=InnoDB");
            }
            MysqlProtocolSchema.requireTable(gate, prefix + "_restore");
        }

        /** 递增任务代次并先安装所有分片栅栏，旧执行者不能继续向已重放的表写入。 */
        private void claim(MysqlProtocolBackup.Archive archive, Runnable preflight) throws SQLException {
            requireHeld(); job = read(gate, prefix);
            MysqlProtocolSchema.requireTable(gate, prefix + "_restore");
            if (job == null || !archive.prefix().equals(prefix) || !job.sha256().equals(archive.sha256()) || !job.sourceNamespace().equals(archive.checkpoint().namespace()) || job.sourceGeneration() != archive.checkpoint().generation() || !job.checkpoint().equals(archive.checkpoint().id())) { throw new IllegalArgumentException("restore archive does not match the durable job"); }
            try (var q = gate.createStatement(); var r = q.executeQuery("select routes from " + prefix + "_restore where id=1")) {
                if (!r.next() || !routes.equals(r.getString(1))) { throw new IllegalArgumentException("restore physical routes changed"); }
            }
            acquire("azir:" + prefix + ":" + job.targetNamespace()); acquire("azir:" + prefix + ":" + job.sourceNamespace());
            if (job.state().equals("STAGED")) { return; }
            if (!job.state().equals("IMPORTING")) { throw new IllegalStateException("unknown restore state"); }
            preflight.run();
            // 已开始对外服务或交接的空间绝对不能通过恢复重试覆盖；运行行在每个本地事务内原子替换。
            for (var entry : roles.entrySet()) {
                MysqlProtocolSchema.requireRole(entry.getValue(), prefix, entry.getKey());
                requireNonServing(entry.getValue(), entry.getKey());
            }
            requireHeld();
            try (var q = gate.prepareStatement("update " + prefix + "_restore set epoch=epoch+1,updated_at=unix_timestamp(current_timestamp(3))*1000 where id=1 and epoch=? and state='IMPORTING' and epoch<9223372036854775807")) {
                q.setLong(1, job.epoch()); if (q.executeUpdate() != 1) { throw new SQLException("restore epoch changed"); }
            }
            job = read(gate, prefix);
            for (var entry : roles.entrySet()) { installFence(entry.getValue(), entry.getKey()); }
            for (var entry : roles.entrySet()) {
                local(entry.getValue(), entry.getKey(), c -> {
                    try (var q = c.prepareStatement("update " + prefix + "_runtime set mode='RESTORING',freeze_ready=false,pulse=pulse+1 where role=? and mode in ('FROZEN','RESTORING','STAGED')")) {
                        q.setString(1, entry.getKey()); if (q.executeUpdate() != 1) { throw new SQLException("restore runtime fence is unavailable"); }
                    }
                });
            }
        }

        /** 恢复结果已完成时重试只返回原凭据，不再触碰任何业务表。 */
        boolean complete() { return job.state().equals("STAGED"); }
        /** 返回执行任务的持久身份。 */
        Job job() { return job; }

        /** 运行栅栏存在且未服务才允许重放，缺失栅栏不能被恢复工具猜测补建。 */
        private void requireNonServing(DataSource source, String role) throws SQLException {
            try (var c = source.getConnection(); var q = c.prepareStatement("select namespace,mode from " + prefix + "_runtime where role=?")) {
                q.setString(1, role); try (var r = q.executeQuery()) {
                    if (!r.next() || !Set.of(job.targetNamespace(), job.sourceNamespace()).contains(r.getString(1)) || !Set.of("FROZEN", "RESTORING", "STAGED").contains(r.getString(2))) { throw new IllegalStateException("restore target is serving or its runtime fence is missing"); }
                }
            }
        }

        /** 排他安装新代次并等待旧批次结束；代次只增不减，失联旧进程不能覆盖新栅栏。 */
        private void installFence(DataSource source, String role) throws SQLException {
            requireHeld();
            try (var c = source.getConnection(); var q = c.createStatement()) {
                q.execute("create table if not exists " + prefix + "_restore_fence(role varchar(70) character set ascii collate ascii_bin primary key,archive_sha char(64) character set ascii collate ascii_bin not null,epoch bigint not null,pulse bigint not null default 0) engine=InnoDB");
                MysqlProtocolSchema.requireTable(c, prefix + "_restore_fence");
                try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='pulse'")) {
                    check.setString(1, prefix + "_restore_fence"); boolean present;
                    try (var r = check.executeQuery()) { r.next(); present = r.getInt(1) != 0; }
                    if (!present) { q.execute("alter table " + prefix + "_restore_fence add column pulse bigint not null default 0"); }
                }
            }
            transaction(source, c -> {
                try (var q = c.prepareStatement("insert into " + prefix + "_restore_fence(role,archive_sha,epoch) values(?,?,0) on duplicate key update role=role")) { q.setString(1, role); q.setString(2, job.sha256()); q.executeUpdate(); }
                try (var q = c.prepareStatement("select archive_sha,epoch from " + prefix + "_restore_fence where role=? for update")) {
                    q.setString(1, role); try (var r = q.executeQuery()) { if (!r.next() || !job.sha256().equals(r.getString(1)) || r.getLong(2) > job.epoch()) { throw new SQLException("restore executor was superseded"); } }
                }
                try (var q = c.prepareStatement("update " + prefix + "_restore_fence set epoch=? where role=?")) { q.setLong(1, job.epoch()); q.setString(2, role); q.executeUpdate(); }
            });
        }

        /** 每个导入批次先取得共享执行栅栏，接管者必须等待此批次提交或回滚。 */
        void fence(Connection c, String role) throws SQLException {
            if (c.getAutoCommit()) { throw new SQLException("restore fence requires a transaction"); }
            requireHeld();
            try (var q = c.prepareStatement("select archive_sha,epoch from " + prefix + "_restore_fence where role=? for share")) {
                q.setString(1, role); try (var r = q.executeQuery()) { if (!r.next() || !job.sha256().equals(r.getString(1)) || r.getLong(2) != job.epoch()) { throw new SQLException("restore batch belongs to an obsolete executor"); } }
            }
            if (MysqlProtocolState.grouped(roles.get(role))) {
                // 让批次写入与接管者的 epoch 更新在组复制认证中争用同一行，而不只依赖旧主本地锁。
                try (var q = c.prepareStatement("update " + prefix + "_restore_fence set pulse=pulse+1 where role=? and archive_sha=? and epoch=? and pulse<9223372036854775807")) {
                    q.setString(1, role); q.setString(2, job.sha256()); q.setLong(3, job.epoch()); if (q.executeUpdate() != 1) { throw new SQLException("group restore fence changed or exhausted"); }
                }
            }
        }

        /** 用短本地事务发布单步变化，不让失败连接归还池时意外自动提交。 */
        void local(DataSource source, String role, SqlAction action) throws SQLException {
            transaction(source, c -> { fence(c, role); action.run(c); });
        }

        /** 进度只作诊断，恢复总是从固定表序重新导入，不能依赖未完成回读的表标记。 */
        void progress(String role, String table) throws SQLException {
            requireHeld();
            try (var q = gate.prepareStatement("update " + prefix + "_restore set progress_role=?,progress_table=?,updated_at=unix_timestamp(current_timestamp(3))*1000 where id=1 and epoch=? and state='IMPORTING'")) {
                q.setString(1, role); q.setString(2, table); q.setLong(3, job.epoch()); if (q.executeUpdate() != 1) { throw new SQLException("restore task was superseded"); }
            }
        }

        /** 数据角色先暂存，控制角色与任务完成记录在同一事务内最后发布。 */
        void finish() throws SQLException {
            var ordered = new ArrayList<>(roles.entrySet()); ordered.sort(Comparator.comparing(e -> e.getKey().equals("control")));
            for (var entry : ordered) {
                local(entry.getValue(), entry.getKey(), c -> {
                    try (var q = c.prepareStatement("update " + prefix + "_runtime set generation=?,mode='STAGED',destination=?,freeze_ready=false where role=? and namespace=? and checkpoint=? and mode='RESTORING'")) {
                        q.setLong(1, job.targetGeneration()); q.setString(2, "archive:" + job.sha256()); q.setString(3, entry.getKey()); q.setString(4, job.sourceNamespace()); q.setString(5, job.checkpoint());
                        if (q.executeUpdate() != 1) { throw new SQLException("restored runtime identity inconsistent"); }
                    }
                    if (entry.getKey().equals("control")) {
                        try (var q = c.prepareStatement("update " + prefix + "_restore set state='STAGED',updated_at=unix_timestamp(current_timestamp(3))*1000 where id=1 and epoch=? and state='IMPORTING'")) { q.setLong(1, job.epoch()); if (q.executeUpdate() != 1) { throw new SQLException("restore completion was superseded"); } }
                    }
                });
            }
        }

        /** 同一控制会话必须仍持有所有锁，失联后不允许换连接继续发布结果。 */
        void requireHeld() throws SQLException {
            MysqlProtocolState.checkInterrupted();
            ((ProtocolMysqlDataSource) control).verifySession(gate);
            for (String name : locks.keySet()) {
                try (var q = gate.prepareStatement("select is_used_lock(?)=connection_id()")) { q.setString(1, name); try (var r = q.executeQuery()) { if (!r.next() || !r.getBoolean(1)) { throw new SQLException("restore control session lost"); } } }
            }
        }

        /** 命名锁取得失败不得继续导入，最长只等待十秒。 */
        private void acquire(String name) throws SQLException {
            if (locks.containsKey(name)) { return; }
            var lease = mvcc.control.MysqlNamedLock.lock(gate, name, 10);
            try { locks.put(name, lease); }
            catch (RuntimeException | Error failure) {
                try { lease.close(); } catch (SQLException close) { failure.addSuppressed(close); }
                mvcc.control.MysqlNamedLock.discard(gate, failure); throw failure;
            }
        }

        /** 已有主异常时保留关闭错误，不掩盖真正的导入失败。 */
        private void closeAfter(Throwable failure) { try { close(); } catch (RuntimeException | Error close) { if (failure != close) { failure.addSuppressed(close); } } }

        /** 逐个释放会话锁，任何异常都销毁会话，避免命名锁遗留在连接池。 */
        @Override public void close() {
            Throwable failure = null;
            for (var lease : locks.reversed().values()) {
                try { lease.close(); }
                catch (SQLException | RuntimeException | Error problem) { if (failure == null) { failure = problem; } else if (failure != problem) { failure.addSuppressed(problem); } }
            }
            if (failure != null) { mvcc.control.MysqlNamedLock.discard(gate, failure); }
            try { gate.close(); }
            catch (SQLException | RuntimeException | Error close) { if (failure == null) { failure = close; } else if (failure != close) { failure.addSuppressed(close); } }
            if (failure instanceof SQLException sql) { throw storage(sql); }
            if (failure instanceof RuntimeException runtime) { throw runtime; }
            if (failure instanceof Error fatal) { throw fatal; }
        }

    }

    /** 只执行已知的本地恢复 SQL，不接收文件中的 SQL 片段。 */
    @FunctionalInterface interface SqlAction { /** 执行一个本地恢复步骤。 */ void run(Connection c) throws SQLException; }

    /** 显式回滚所有失败，包括取消；回滚失败则销毁连接。 */
    private static void transaction(DataSource source, SqlAction action) throws SQLException {
        try (var c = source.getConnection()) {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); c.setAutoCommit(false);
            try { action.run(c); c.commit(); }
            catch (SQLException | RuntimeException | Error failure) {
                try { c.rollback(); } catch (SQLException | RuntimeException | Error rollback) { if (failure != rollback) { failure.addSuppressed(rollback); } mvcc.control.MysqlNamedLock.discard(c, failure); }
                throw failure;
            }
        }
    }
}

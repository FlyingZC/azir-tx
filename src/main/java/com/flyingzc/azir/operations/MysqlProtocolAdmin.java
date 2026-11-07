package com.flyingzc.azir.operations;

import javax.sql.DataSource;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/** 显式生产管理入口；冻结、恢复、退休和交接都留下持久栅栏，不依赖进程内停写约定。 */
public final class MysqlProtocolAdmin {
    private final MysqlProtocolState state;
    private final IntFunction<Integer> recovery;
    private final ThreadLocal<Connection> administrationGate = new ThreadLocal<>();
    private final String administrationName;
    private final boolean writable;
    private final FreezeOptions freezeOptions;

    /** 冻结的总时间预算与单个清理、恢复批次上限，适用于大事务积压的受控停服。 */
    public record FreezeOptions(long timeoutMillis, int batchSize) {
        /** 禁止无期限冻结、零预算忙循环或无限大的单次本地事务。 */
        public FreezeOptions {
            if (timeoutMillis < 1 || timeoutMillis > 3_600_000 || batchSize < 1 || batchSize > 4096) { throw new IllegalArgumentException("invalid freeze options"); }
        }
        /** 默认冻结预算六十秒，每个本地清理批次最多一千零二十四行。 */
        public static FreezeOptions defaults() { return new FreezeOptions(60_000, 1024); }
    }
    /** 冻结凭据同时绑定命名空间、运行代次和本次管理检查点。 */
    public record Checkpoint(String namespace, long generation, String id) {
        /** 管理凭据不能缺失或跨空间猜测。 */
        public Checkpoint {
            UUID.fromString(namespace); UUID.fromString(id);
            if (generation < 1) { throw new IllegalArgumentException("invalid generation"); }
        }
    }
    /** 由真实后端提供恢复回调，管理层不仿写提交协议。 */
    public MysqlProtocolAdmin(MysqlProtocolState state, IntFunction<Integer> recovery) { this(state, recovery, true); }
    /** 部分角色的凭据诊断只允许读，不能借此冻结或激活不完整的分片集合。 */
    private MysqlProtocolAdmin(MysqlProtocolState state, IntFunction<Integer> recovery, boolean writable) {
        this(state, recovery, writable, FreezeOptions.defaults());
    }
    /** 继承管理权限和真实恢复回调，只调整本次管理句柄使用的冻结预算。 */
    private MysqlProtocolAdmin(MysqlProtocolState state, IntFunction<Integer> recovery, boolean writable, FreezeOptions freezeOptions) {
        this.freezeOptions = Objects.requireNonNull(freezeOptions);
        this.writable = writable;
        if (writable && state.shards().isEmpty()) { throw new IllegalArgumentException("administration requires the full shard configuration"); }
        this.state = Objects.requireNonNull(state); this.recovery = Objects.requireNonNull(recovery);
        var authority = com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control());
        administrationName = "azir:admin:" + state.prefix() + ":" + UUID.nameUUIDFromBytes(authority.toString().getBytes(StandardCharsets.UTF_8));
    }
    /** 返回使用指定冻结预算的管理句柄，备份和首次暂存也会复用该预算。 */
    public MysqlProtocolAdmin withFreezeOptions(FreezeOptions options) { return new MysqlProtocolAdmin(state, recovery, writable, options); }
    /** 诊断和修复已有部署时不创建业务客户端，局部恢复也不能自动重置绑定或水位。 */
    public static MysqlProtocolAdmin openExisting(String prefix, DataSource control, Map<String, DataSource> shards) { return open(prefix, control, shards, true); }
    /** 只读凭据入口可只配置所需角色；返回对象不能执行任何冻结、切换或退休。 */
    public static MysqlProtocolAdmin openReceiptAuthority(String prefix, DataSource control, Map<String, DataSource> shards) { return open(prefix, control, shards, false); }
    /** 普通管理必须匹配完整持久路由，部分只读诊断仅装载明确提供的角色。 */
    private static MysqlProtocolAdmin open(String prefix, DataSource control, Map<String, DataSource> shards, boolean writable) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix)) { throw new IllegalArgumentException("invalid protocol"); }
        var safe = new com.flyingzc.azir.control.ProtocolMysqlDataSource(control); var data = new TreeMap<String, DataSource>();
        shards.forEach((id, source) -> data.put(id, new com.flyingzc.azir.control.ProtocolMysqlDataSource(source)));
        String config = prefix.equals("pc") ? "pc_control" : prefix + "_config";
        String namespace;
        try (var c = safe.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select namespace,operations_version,routes from " + config + " where id=1")) {
            if (!r.next() || r.getInt(2) != 2) { throw new SQLException("managed protocol schema not initialized"); } namespace = r.getString(1);
            if (writable) {
                var routes = new TreeMap<String, String>(); routes.put("control", safe.authorityIdentity().toString());
                data.forEach((id, source) -> routes.put("shard:" + id, com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source).toString()));
                if (!routes.toString().equals(r.getString(3))) { throw new IllegalArgumentException("administration requires the full original physical routes"); }
            }
        } catch (SQLException failure) { throw storage(failure); }
        if (writable) {
            MysqlProtocolSchema.requireRole(safe, prefix, "control");
            data.forEach((id, source) -> MysqlProtocolSchema.requireRole(source, prefix, "shard:" + id));
        }
        return new MysqlProtocolAdmin(MysqlProtocolState.existing(prefix, namespace, safe, data), ignored -> { throw new IllegalStateException("recovery needs the actual transaction backend"); }, writable);
    }
    /** 不完整角色集合只能核对终态，不能变更任何协议数据。 */
    void requireWritable() { if (!writable) { throw new IllegalStateException("receipt authority is read-only"); } }
    /** 返回协议栅栏，供备份格式读取受控布局。 */
    public MysqlProtocolState state() { return state; }
    /** 从持久恢复计划重建原始冻结凭据，支持部分恢复或回复丢失后找回旧代次。 */
    public Optional<Checkpoint> resumeCheckpoint() {
        var current = state.status(); String plan = current.destination();
        if (plan == null || !plan.startsWith("resume:") || !Set.of("FROZEN", "ACTIVE").contains(current.mode())) { return Optional.empty(); }
        try {
            String[] parts = plan.split(":", -1);
            if (parts.length != 4) { throw new IllegalArgumentException("invalid resume plan"); }
            long before = Long.parseLong(parts[1]), after = Long.parseLong(parts[2]);
            if (after != Math.addExact(before, 1) || (current.generation() != before && current.generation() != after) || !Objects.equals(current.checkpoint(), parts[3])) { throw new IllegalArgumentException("inconsistent resume plan"); }
            return Optional.of(new Checkpoint(current.namespace(), before, parts[3]));
        } catch (RuntimeException failure) { throw new IllegalStateException("durable resume plan is invalid", failure); }
    }
    /** 返回全部持久角色的状态，局部冻结或局部切换也可诊断。 */
    public Map<String, MysqlProtocolState.Status> status() {
        var result = new TreeMap<String, MysqlProtocolState.Status>();
        result.put("control", state.status());
        state.shards().forEach((id, source) -> result.put("shard:" + id, state.read(source, "shard:" + id, false)));
        return Collections.unmodifiableMap(result);
    }
    /** 冻结并中止尚未决定的业务事务，恢复所有真实终态后才确认检查点。 */
    public Checkpoint freeze() { return freeze(freezeOptions); }
    /** 时间预算覆盖等待、冻结、分批清理和恢复；SQL 只能在既有有限连接超时内协作取消。 */
    public Checkpoint freeze(FreezeOptions options) {
        requireWritable(); Objects.requireNonNull(options); long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(options.timeoutMillis());
        Checkpoint token = locked(() -> {
            checkFreezeBudget(deadline); var control = state.status();
            if (!control.namespace().equals(state.namespace()) || control.generation() != state.generation()) { throw new IllegalStateException("freeze requires administration reopened for the current generation"); }
            if (transitionPending(control)) { throw new IllegalStateException("administration transition pending; retry its original resume or handoff"); }
            if (!Set.of("ACTIVE", "FROZEN").contains(control.mode())) { throw new IllegalStateException("cannot freeze " + control.mode()); }
            if (status().values().stream().anyMatch(s -> s.generation() != control.generation())) { throw new IllegalStateException("incomplete generation transition; retry resume before freezing again"); }
            if (status().values().stream().anyMatch(s -> s.mode().equals("HANDED_OFF"))) { throw new IllegalStateException("handoff is irreversible"); }
            String checkpoint = control.mode().equals("FROZEN") ? control.checkpoint() : UUID.randomUUID().toString();
            var id = new Checkpoint(control.namespace(), control.generation(), checkpoint);
            String preserved = control.mode().equals("FROZEN") ? control.destination() : null;
            change(state.control(), "control", id.generation(), "FROZEN", checkpoint, preserved);
            mutateFrozen(state.control(), "control", id, "update " + state.prefix() + "_runtime set freeze_ready=false where role='control'", deadline);
            state.shards().forEach((shard, source) -> { checkFreezeBudget(deadline); change(source, "shard:" + shard, id.generation(), "FROZEN", checkpoint, preserved); });
            String limit = " limit " + options.batchSize();
            drainFrozen(state.control(), "control", id, state.prefix().equals("pc") ? "delete from pc_readers" + limit : "update " + state.prefix() + "_txn set status='ABORTED',finished_at=if(finished_at=0,unix_timestamp(current_timestamp(3))*1000,finished_at) where status='ACTIVE'" + limit, deadline);
            if (state.prefix().equals("pc")) { state.shards().forEach((shard, source) -> drainFrozen(source, "shard:" + shard, id, "update pc_lock set expires_at=0 where expires_at<>0" + limit, deadline)); }
            return id;
        });
        // 冻结状态已经持久，恢复回调另取普通控制门禁，不能在两个连接间自锁。
        state.administrative(() -> {
            while (hasPending()) { checkFreezeBudget(deadline); recovery.apply(options.batchSize()); }
            return null;
        });
        return locked(() -> {
            checkFreezeBudget(deadline); requireFrozenRoles(token);
            if (hasPending()) { throw new IllegalStateException("unstable frozen checkpoint"); }
            new MysqlProtocolValidator(this).verify();
            mutateFrozen(state.control(), "control", token, "update " + state.prefix() + "_runtime set freeze_ready=true where role='control'", deadline); return token;
        });
    }
    /** 重新启用原权威必须增加代次，冻结之前创建的所有客户端继续失效。 */
    public long resume(Checkpoint checkpoint) {
        requireWritable();
        return locked(() -> {
            requireToken(checkpoint);
            var current = state.status(); long next = Math.addExact(checkpoint.generation(), 1);
            String plan = "resume:" + checkpoint.generation() + ":" + next + ":" + checkpoint.id();
            if (current.mode().equals("ACTIVE") && current.generation() == next) {
                if ((current.destination() != null && !current.destination().equals(plan)) || status().values().stream().anyMatch(value -> !value.mode().equals("ACTIVE") || value.generation() != next || !value.namespace().equals(checkpoint.namespace()) || !Objects.equals(value.checkpoint(), checkpoint.id()) || !Objects.equals(value.destination(), current.destination()))) { throw new IllegalStateException("active control has an incomplete role transition"); }
                return current.generation();
            }
            if (!current.freezeReady() || hasPending()) { throw new IllegalStateException("freeze is incomplete; retry freeze recovery before resume"); }
            for (var entry : status().entrySet()) {
                var value = entry.getValue();
                boolean frozen = value.mode().equals("FROZEN") && (value.generation() == checkpoint.generation() || value.generation() == next);
                boolean partialActivation = !entry.getKey().equals("control") && current.mode().equals("FROZEN") && current.generation() == next && value.mode().equals("ACTIVE") && value.generation() == next;
                if (!value.namespace().equals(checkpoint.namespace()) || !Objects.equals(value.checkpoint(), checkpoint.id()) || !(frozen || partialActivation)
                        || (intent(value.destination()) && !plan.equals(value.destination()))) { throw new IllegalStateException("resume requires this frozen transition or its partially activated data roles"); }
            }
            change(state.control(), "control", current.generation(), "FROZEN", checkpoint.id(), plan);
            state.shards().forEach((id, source) -> change(source, "shard:" + id, next, "FROZEN", checkpoint.id(), plan));
            change(state.control(), "control", next, "FROZEN", checkpoint.id(), plan);
            state.shards().forEach((id, source) -> change(source, "shard:" + id, next, "ACTIVE", checkpoint.id(), plan));
            change(state.control(), "control", next, "ACTIVE", checkpoint.id(), plan);
            return next;
        });
    }
    /** 显式设置查询凭据保留时间；水位仍必须受快照和分片栅栏保护。 */
    public void receiptRetentionMillis(long millis) {
        requireWritable();
        if (millis < 0 || millis > 3_153_600_000_000L) { throw new IllegalArgumentException("invalid receipt retention"); }
        locked(() -> {
            if (!state.status().mode().equals("ACTIVE")) { throw new IllegalStateException("configure retention on an active deployment"); }
            return local(state.control(), c -> {
                exclusive(c, "control"); verifyGate(); var current = state.read(c, "control", false);
                if (!current.mode().equals("ACTIVE") || current.generation() != state.generation()) { throw new IllegalStateException("retention configuration belongs to an obsolete client"); }
                try (var q = c.prepareStatement("update " + state.prefix() + "_runtime set retention_ms=? where role='control'")) { q.setLong(1, millis); q.executeUpdate(); }
                verifyGate(); return null;
            });
        });
    }
    /** 自动维护已持有正常提交门禁；先在全部分片推进写栅栏，再退休无版本引用的身份。 */
    public int retireLocked(Connection gate, long floor, int limit) {
        requireWritable();
        try {
            if (!gate.getAutoCommit()) { throw new IllegalStateException("retirement requires a committed safe point"); }
            try (var q = gate.prepareStatement("select is_used_lock(?)=connection_id()")) { q.setString(1, state.gateName()); try (var r = q.executeQuery()) { if (!r.next() || !r.getBoolean(1)) { throw new SQLException("retirement gate not owned"); } } }
            String sql = state.prefix().equals("pc") ? "select safe_point from pc_control where id=1" : "select safe_point from " + state.prefix() + "_gc where id=1";
            try (var q = gate.createStatement(); var r = q.executeQuery(sql)) { if (!r.next() || floor > r.getLong(1)) { throw new IllegalArgumentException("retirement cannot force the safe point forward"); } }
        } catch (SQLException failure) { throw storage(failure); }
        if (limit < 1 || limit > 4096 || floor < 0) { throw new IllegalArgumentException("invalid retirement budget"); }
        var current = state.status();
        if (!current.mode().equals("ACTIVE") || current.generation() != state.generation()) { throw new IllegalStateException("retirement requires current active generation"); }
        String prefix = state.prefix();
        for (var entry : state.shards().entrySet()) {
            MysqlProtocolState.checkInterrupted();
            retireLocal(gate, entry.getValue(), "shard:" + entry.getKey(), c -> {
                try (var q = c.prepareStatement("update " + prefix + "_runtime set retired_before=greatest(retired_before,?) where role=?")) {
                    q.setLong(1, floor); q.setString(2, "shard:" + entry.getKey()); q.executeUpdate();
                }
                return null;
            });
        }
        retireLocal(gate, state.control(), "control", c -> {
            try (var q = c.prepareStatement("update " + prefix + "_runtime set retired_before=greatest(retired_before,?) where role='control'")) { q.setLong(1, floor); q.executeUpdate(); }
            return null;
        });
        int removed = 0;
        if (prefix.equals("pc")) {
            for (var entry : state.shards().entrySet()) {
                removed += retireLocal(gate, entry.getValue(), "shard:" + entry.getKey(), c -> {
                    int count = 0;
                    for (String table : List.of("pc_write", "pc_rollback")) {
                        /** 主键与原始身份，跨分片残留锁仍需要 primary 决议。 */
                        record Candidate(byte[] key, long start) { }
                        List<Candidate> candidates = new ArrayList<>();
                        try (var q = c.prepareStatement("select user_key,start_ts from " + table + " where start_ts<=? and finished_at<=unix_timestamp(current_timestamp(3))*1000-? and not exists(select 1 from pc_data d where d.user_key=" + table + ".user_key and d.start_ts=" + table + ".start_ts) order by user_key,start_ts limit ?")) {
                            q.setLong(1, floor); q.setLong(2, current.receiptRetentionMillis()); q.setInt(3, limit);
                            try (var r = q.executeQuery()) { while (r.next()) { candidates.add(new Candidate(r.getBytes(1), r.getLong(2))); } }
                        }
                        for (var candidate : candidates) {
                            boolean pending = false;
                            for (var shard : state.shards().values()) { if (scalar(shard, "select count(*) from pc_lock where start_ts=" + candidate.start()) > 0) { pending = true; break; } }
                            if (!pending) {
                                try (var q = c.prepareStatement("delete from " + table + " where user_key=? and start_ts=?")) { q.setBytes(1, candidate.key()); q.setLong(2, candidate.start()); count += q.executeUpdate(); }
                            }
                        }
                    }
                    try (var q = c.prepareStatement("delete from pc_guard where not exists(select 1 from pc_data d where d.user_key=pc_guard.user_key) and not exists(select 1 from pc_lock l where l.user_key=pc_guard.user_key) and not exists(select 1 from pc_write w where w.user_key=pc_guard.user_key) and not exists(select 1 from pc_rollback r where r.user_key=pc_guard.user_key) order by user_key limit ?")) { q.setInt(1, limit); q.executeUpdate(); }
                    return count;
                });
            }
        } else {
            List<Long> candidates = new ArrayList<>();
            long cursor = scalar(state.control(), "select retire_cursor from " + prefix + "_runtime where role='control'");
            try (var c = state.control().getConnection(); var q = c.prepareStatement("select start_ts from " + prefix + "_txn where start_ts<=? and start_ts>? and status<>'ACTIVE' and finalized=1 and finished_at>0 and finished_at<=unix_timestamp(current_timestamp(3))*1000-? order by start_ts limit ?")) {
                q.setLong(1, floor); q.setLong(2, cursor); q.setLong(3, current.receiptRetentionMillis()); q.setInt(4, limit);
                try (var r = q.executeQuery()) { while (r.next()) { candidates.add(r.getLong(1)); } }
            } catch (SQLException failure) { throw storage(failure); }
            for (long start : candidates) {
                MysqlProtocolState.checkInterrupted(); boolean referenced = false;
                for (var source : state.shards().values()) {
                    if (scalar(source, "select count(*) from " + prefix + "_versions where stamp=" + start) != 0) { referenced = true; break; }
                }
                if (!referenced) {
                    removed += retireLocal(gate, state.control(), "control", c -> {
                        try (var q = c.prepareStatement("delete from " + prefix + "_txn where start_ts=? and status<>'ACTIVE' and finalized=1")) { q.setLong(1, start); return q.executeUpdate(); }
                    });
                }
            }
            retireLocal(gate, state.control(), "control", c -> {
                try (var q = c.prepareStatement("update " + prefix + "_runtime set retire_cursor=? where role='control'")) { q.setLong(1, candidates.isEmpty() ? 0 : candidates.getLast()); q.executeUpdate(); }
                return null;
            });
            for (var entry : state.shards().entrySet()) {
                retireLocal(gate, entry.getValue(), "shard:" + entry.getKey(), c -> {
                    try (var q = c.prepareStatement("delete from " + prefix + "_keys where not exists(select 1 from " + prefix + "_versions v where v.table_name=" + prefix + "_keys.table_name and v.row_key=" + prefix + "_keys.row_key) order by table_name,row_key limit ?")) { q.setInt(1, limit); q.executeUpdate(); }
                    return null;
                });
            }
            retireLocal(gate, state.control(), "control", c -> {
                try (var q = c.prepareStatement("delete from " + prefix + "_conflicts where commit_ts<=? order by cell_id limit ?")) { q.setLong(1, floor); q.setInt(2, limit); q.executeUpdate(); }
                return null;
            });
        }
        return removed;
    }
    /** 退休元数据也必须等待本地旧写并复查原控制会话，冻结后不能继续删除备份证明。 */
    private <T> T retireLocal(Connection gate, DataSource source, String role, Work<T> work) {
        return local(source, c -> {
            exclusive(c, role); requireRetirementGate(gate); state.verifyOwner(gate); var current = state.read(c, role, false);
            if (!current.mode().equals("ACTIVE") || current.generation() != state.generation()) { throw new SQLException("retirement role is frozen or belongs to another generation"); }
            state.certify(c, role, current); T result = work.run(c); requireRetirementGate(gate); state.verifyOwner(role.equals("control") ? c : gate); return result;
        });
    }
    /** 用原始维护会话确认命名锁和权威，不在另一个连接持有 runtime 行锁时修改控制脉冲行。 */
    private void requireRetirementGate(Connection gate) throws SQLException {
        MysqlProtocolState.checkInterrupted();
        if (state.control() instanceof com.flyingzc.azir.control.ProtocolMysqlDataSource protocol) { protocol.verifySession(gate); }
        try (var q = gate.prepareStatement("select is_used_lock(?)=connection_id()")) { q.setString(1, state.gateName()); try (var r = q.executeQuery()) { if (!r.next() || !r.getBoolean(1)) { throw new SQLException("retirement control session lost"); } } }
        var current = state.read(gate, "control", false);
        if (!current.namespace().equals(state.namespace()) || current.generation() != state.generation() || !current.mode().equals("ACTIVE")) { throw new SQLException("retirement control generation is no longer active"); }
    }
    /** 将已校验的 STAGED 数据空间接到唯一原权威；原权威永久交出后才激活新权威。 */
    public void activateFrom(MysqlProtocolAdmin source, Checkpoint sourceCheckpoint, String archiveSha) {
        requireWritable(); source.requireWritable();
        if (!archiveSha.matches("[0-9a-f]{64}")) { throw new IllegalArgumentException("invalid archive digest"); }
        if (com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control()).serverUuid().equals(com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source.state.control()).serverUuid()) || routes().equals(source.routes()) || !state.namespace().equals(source.state.namespace())) { throw new IllegalArgumentException("handoff requires distinct routes and the same namespace"); }
        source.locked(() -> locked(() -> {
            source.requireToken(sourceCheckpoint);
            var original = source.state.status();
            if (!original.freezeReady() || original.generation() != sourceCheckpoint.generation()) { throw new IllegalStateException("source freeze has not completed at this generation"); }
            var target = state.status(); String destination = routes() + "@" + archiveSha;
            String handoffPlan = "handoff:" + destination;
            if (original.mode().equals("FROZEN") && intent(original.destination()) && !handoffPlan.equals(original.destination())) { throw new IllegalStateException("source has a different administration intent"); }
            if (!Objects.equals(target.checkpoint(), sourceCheckpoint.id()) || !Objects.equals(target.destination(), "archive:" + archiveSha)
                    || !Set.of("STAGED", "ACTIVE").contains(target.mode())) { throw new IllegalStateException("target is not this verified archive"); }
            for (var role : source.status().values()) {
                if (!role.namespace().equals(sourceCheckpoint.namespace()) || role.generation() != sourceCheckpoint.generation() || !Objects.equals(role.checkpoint(), sourceCheckpoint.id())
                        || !Set.of("FROZEN", "HANDED_OFF").contains(role.mode()) || (role.mode().equals("HANDED_OFF") && !destination.equals(role.destination()))) { throw new IllegalStateException("source already assigned to another destination"); }
            }
            for (var role : status().values()) {
                if (!Set.of("STAGED", "ACTIVE").contains(role.mode()) || role.generation() != target.generation()
                        || !Objects.equals(role.checkpoint(), sourceCheckpoint.id()) || !Objects.equals(role.destination(), "archive:" + archiveSha)) { throw new IllegalStateException("incomplete staged target"); }
            }
            if (original.mode().equals("FROZEN")) { source.change(source.state.control(), "control", sourceCheckpoint.generation(), "FROZEN", sourceCheckpoint.id(), handoffPlan); }
            source.state.shards().forEach((id, pool) -> source.change(pool, "shard:" + id, sourceCheckpoint.generation(), "HANDED_OFF", sourceCheckpoint.id(), destination));
            source.change(source.state.control(), "control", sourceCheckpoint.generation(), "HANDED_OFF", sourceCheckpoint.id(), destination);
            state.shards().forEach((id, pool) -> change(pool, "shard:" + id, target.generation(), "ACTIVE", target.checkpoint(), target.destination()));
            change(state.control(), "control", target.generation(), "ACTIVE", target.checkpoint(), target.destination());
            return null;
        }));
    }
    /** 返回物理路由指纹，不能把同一检查点交给第二组目标数据库。 */
    public String routes() {
        var values = new TreeMap<String, String>();
        values.put("control", com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control()).toString());
        state.shards().forEach((id, source) -> values.put("shard:" + id, com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source).toString()));
        return sha(values.toString().getBytes(StandardCharsets.UTF_8));
    }
    /** 构造与持久配置完全一致的路由字符串，用于显式空空间恢复。 */
    String routeConfiguration() {
        var values = new TreeMap<String, String>(); values.put("control", com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control()).toString());
        state.shards().forEach((id, source) -> values.put("shard:" + id, com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source).toString()));
        return values.toString();
    }
    /** 确认所有角色仍处于同一次完整冻结，备份不可从局部冻结状态生成。 */
    void requireFrozen(Checkpoint checkpoint) {
        var current = state.status();
        if (!current.freezeReady() || transitionPending(current)) { throw new IllegalStateException("checkpoint has not completed recovery or already has a transition intent"); }
        requireFrozenRoles(checkpoint);
    }
    /** 冻结恢复内部先检查角色一致，再完成校验并发布就绪标记。 */
    private void requireFrozenRoles(Checkpoint checkpoint) {
        requireToken(checkpoint);
        for (var value : status().values()) {
            if (!value.mode().equals("FROZEN") || value.generation() != checkpoint.generation()
                    || !checkpoint.id().equals(value.checkpoint())) { throw new IllegalStateException("not a complete frozen checkpoint"); }
        }
    }
    /** 管理操作也受正常控制面命名锁保护，不通过新连接绕过正在提交的事务。 */
    <T> T locked(Supplier<T> work) {
        return MysqlProtocolState.admitted(administrationName, 10, () -> MysqlProtocolState.admitted(state.gateName(), 10, () -> {
            // 物理管理锁先于 namespace 锁取得，释放异常由资源作用域附加到原操作失败。
            try (var c = state.control().getConnection();
                 var physical = com.flyingzc.azir.control.MysqlNamedLock.lock(c, administrationName, 10);
                 var logical = com.flyingzc.azir.control.MysqlNamedLock.lock(c, state.gateName(), 10)) {
                Connection before = administrationGate.get(); administrationGate.set(c);
                try { return work.get(); }
                finally { if (before == null) { administrationGate.remove(); } else { administrationGate.set(before); } }
            } catch (SQLException failure) { throw storage(failure); }
        }));
    }
    /** 长耗时管理步骤在发布文件或状态前检查原始持锁会话，不能仅用新连接读取冻结标志。 */
    void verifyGate() throws SQLException {
        MysqlProtocolState.checkInterrupted(); Connection c = administrationGate.get();
        if (c == null) { throw new SQLException("administration gate is not bound"); }
        if (state.control() instanceof com.flyingzc.azir.control.ProtocolMysqlDataSource protocol) { protocol.verifySession(c); }
        for (String name : List.of(administrationName, state.gateName())) {
            try (var q = c.prepareStatement("select is_used_lock(?)=connection_id()")) {
                q.setString(1, name); try (var r = q.executeQuery()) { if (!r.next() || !r.getBoolean(1)) { throw new SQLException("administration gate lost"); } }
            }
        }
    }

    /** 用局部排他行锁等待所有已进入本分片的旧写完成，再发布新代次或冻结状态。 */
    void change(DataSource source, String role, long generation, String mode, String checkpoint, String destination) {
        requireWritable();
        local(source, c -> {
            exclusive(c, role); verifyGate(); var current = state.read(c, role, false);
            if (generation < current.generation()) { throw new SQLException("administration cannot decrease runtime generation"); }
            requireTransition(role, current, generation, mode, checkpoint, destination);
            // resume 重试只能补齐未启用角色，不能把同一计划已 ACTIVE 的角色降回 FROZEN。
            if (mode.equals("FROZEN") && destination != null && destination.startsWith("resume:") && current.mode().equals("ACTIVE") && generation == current.generation() && Objects.equals(checkpoint, current.checkpoint())) { return null; }
            if (current.mode().equals("HANDED_OFF") && !(mode.equals("HANDED_OFF") && generation == current.generation() && Objects.equals(checkpoint, current.checkpoint()) && Objects.equals(destination, current.destination()))) { throw new SQLException("handoff is irreversible"); }
            if (mode.equals("FROZEN") && !Set.of("ACTIVE", "FROZEN").contains(current.mode())) { throw new SQLException("cannot refreeze restoring, staged or handed-off data"); }
            if (mode.equals("ACTIVE") && (generation != current.generation() || !Objects.equals(checkpoint, current.checkpoint()) || !Set.of("ACTIVE", "FROZEN", "STAGED").contains(current.mode()))) { throw new SQLException("activation must preserve the prepared generation and checkpoint"); }
            if (mode.equals("HANDED_OFF") && (generation != current.generation() || !Objects.equals(checkpoint, current.checkpoint()) || !Set.of("FROZEN", "HANDED_OFF").contains(current.mode()))) { throw new SQLException("handoff requires its original frozen generation"); }
            if (mode.equals("STAGED") && current.destination() != null && current.destination().startsWith("new-namespace:") && !Objects.equals(destination, current.destination())) { throw new SQLException("namespace activation plan already assigned"); }
            if (mode.equals("STAGED") && (!current.mode().equals("STAGED") || generation != current.generation() || !Objects.equals(checkpoint, current.checkpoint()))) { throw new SQLException("namespace activation plan requires its original staged generation"); }
            state.certify(c, role, current);
            boolean resetReady = mode.equals("ACTIVE") || (mode.equals("FROZEN") && (current.mode().equals("ACTIVE") || !Objects.equals(checkpoint, current.checkpoint())));
            try (var q = c.prepareStatement("update " + state.prefix() + "_runtime set generation=?,mode=?,checkpoint=?,destination=?" + (resetReady ? ",freeze_ready=false" : "") + " where role=?")) {
                q.setLong(1, generation); q.setString(2, mode); q.setString(3, checkpoint); q.setString(4, destination); q.setString(5, role); q.executeUpdate();
            }
            verifyGate(); return null;
        });
    }
    /** 管理意图与检查点一同校验，节点重新当选不能让保留旧锁的执行者回退新流程。 */
    private void requireTransition(String role, MysqlProtocolState.Status current, long generation,
                                   String mode, String checkpoint, String destination) throws SQLException {
        var authority = role.equals("control") ? current : state.read(administrationGate.get(), "control", false);
        if (!authority.namespace().equals(state.namespace())) { throw new SQLException("administration control namespace changed"); }
        if (destination != null && destination.startsWith("resume:")) {
            boolean sameIntent = destination.equals(authority.destination());
            boolean initial = role.equals("control") && authority.mode().equals("FROZEN") && authority.freezeReady() && !intent(authority.destination());
            if (!Objects.equals(checkpoint, authority.checkpoint()) || !Set.of("FROZEN", "ACTIVE").contains(authority.mode()) || !(sameIntent || initial)) { throw new SQLException("resume intent was superseded"); }
        } else if (destination != null && destination.startsWith("handoff:")) {
            if (!role.equals("control") || !mode.equals("FROZEN") || !authority.mode().equals("FROZEN") || !authority.freezeReady() || generation != authority.generation() || !Objects.equals(checkpoint, authority.checkpoint()) || (intent(authority.destination()) && !destination.equals(authority.destination()))) { throw new SQLException("handoff intent already assigned or superseded"); }
        } else if (mode.equals("HANDED_OFF")) {
            boolean prepared = authority.mode().equals("FROZEN") && Objects.equals(authority.destination(), "handoff:" + destination);
            boolean completed = authority.mode().equals("HANDED_OFF") && Objects.equals(authority.destination(), destination);
            if (generation != authority.generation() || !Objects.equals(checkpoint, authority.checkpoint()) || !(prepared || completed)) { throw new SQLException("handoff requires its exact persisted intent"); }
        } else if (mode.equals("FROZEN") && transitionPending(authority)) {
            throw new SQLException("freeze cannot replace an unfinished administration intent");
        }
    }
    /** 只识别内部恢复和交接意图，原有备份、旧版升级和命名空间计划保留各自语义。 */
    private static boolean intent(String destination) { return destination != null && (destination.startsWith("resume:") || destination.startsWith("handoff:")); }
    /** 持久冻结状态中的管理意图必须先完成，不能重新解释成另一次冻结。 */
    static boolean transitionPending(MysqlProtocolState.Status status) { return status.mode().equals("FROZEN") && intent(status.destination()); }
    /** 同一冻结检查点逐批清理，已提交批次保留，超时后后续重试继续处理剩余记录。 */
    private void drainFrozen(DataSource source, String role, Checkpoint token, String sql, long deadline) {
        while (true) { checkFreezeBudget(deadline); if (mutateFrozen(source, role, token, sql, deadline) == 0) { return; } }
    }
    /** 冻结清理同样锁定角色并复查原会话，迟到 SQL 不能中止恢复后新建的事务。 */
    private int mutateFrozen(DataSource source, String role, Checkpoint token, String sql, long deadline) {
        checkFreezeBudget(deadline);
        return local(source, c -> {
            exclusive(c, role); verifyGate(); checkFreezeBudget(deadline); var current = state.read(c, role, false);
            var authority = role.equals("control") ? current : state.read(administrationGate.get(), "control", false);
            if (transitionPending(authority)) { throw new SQLException("freeze cleanup cannot overwrite an administration intent"); }
            if (!current.mode().equals("FROZEN") || current.generation() != token.generation() || !Objects.equals(current.checkpoint(), token.id())) { throw new SQLException("frozen mutation belongs to an obsolete checkpoint"); }
            state.certify(c, role, current); int affected;
            try (var q = c.createStatement()) { q.setQueryTimeout(freezeSecondsRemaining(deadline)); affected = q.executeUpdate(sql); }
            verifyGate(); checkFreezeBudget(deadline); return affected;
        });
    }
    /** 使用单调时钟计时，不因系统校时而增加管理操作允许执行的时间。 */
    private static void checkFreezeBudget(long deadline) {
        MysqlProtocolState.checkInterrupted();
        if (deadline - System.nanoTime() <= 0) { throw new IllegalStateException("freeze budget exhausted; inspect status and retry freeze"); }
    }
    /** JDBC 以秒为单位取消 SQL，向上取整后仍须在提交前重新检查毫秒预算。 */
    private static int freezeSecondsRemaining(long deadline) {
        checkFreezeBudget(deadline); long remaining = deadline - System.nanoTime();
        return (int) Math.max(1, (remaining + 999_999_999L) / 1_000_000_000L);
    }
    /** 校验稳定检查点，不把任意字符串当作恢复授权。 */
    private void requireToken(Checkpoint token) {
        var value = state.status();
        if (!state.namespace().equals(token.namespace()) || !Objects.equals(value.checkpoint(), token.id())
                || (value.generation() != token.generation() && value.generation() != token.generation() + 1)) { throw new IllegalArgumentException("checkpoint mismatch"); }
    }
    /** 只判断是否仍有待恢复记录，不为管理循环累计整张表的精确数量。 */
    private boolean hasPending() {
        if (!state.prefix().equals("pc") && scalar(state.control(), "select exists(select 1 from " + state.prefix() + "_txn where status='ACTIVE' or finalized=0 limit 1)") != 0) { return true; }
        for (var source : state.shards().values()) {
            if (scalar(source, state.prefix().equals("pc") ? "select exists(select 1 from pc_lock limit 1)" : "select exists(select 1 from " + state.prefix() + "_versions where shadow_ts is null limit 1)") != 0) { return true; }
        }
        return false;
    }
    /** 取得本地栅栏排他锁，调用者必须已经开启短本地事务。 */
    private void exclusive(Connection c, String role) throws SQLException {
        try (var q = c.prepareStatement("select namespace from " + state.prefix() + "_runtime where role=? for update")) {
            q.setString(1, role); try (var r = q.executeQuery()) { if (!r.next() || !state.namespace().equals(r.getString(1))) { throw new SQLException("runtime fence missing or replaced"); } }
        }
    }
    /** 本地事务失败必须回滚，回滚未知时销毁连接而不是自动提交。 */
    <T> T local(DataSource source, Work<T> work) {
        MysqlProtocolState.checkInterrupted();
        try (var c = source.getConnection()) {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); c.setAutoCommit(false);
            try { T value = work.run(c); c.commit(); return value; }
            catch (SQLException | RuntimeException | Error failure) {
                state.invalidateControl();
                try { c.rollback(); } catch (SQLException | RuntimeException | Error rollback) { if (failure != rollback) { failure.addSuppressed(rollback); } com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure); }
                throw failure;
            }
        } catch (SQLException failure) { throw storage(failure); }
    }
    /** 只接受固定表布局中的 SQL 工作，不接收用户输入的 SQL。 */
    @FunctionalInterface interface Work<T> { /** 执行一个短本地事务。 */ T run(Connection c) throws SQLException; }
    /** 读取固定管理聚合值，不装载事务或版本全集。 */
    static long scalar(DataSource source, String sql) { try (var c = source.getConnection(); var q = c.createStatement(); var r = q.executeQuery(sql)) { r.next(); return r.getLong(1); } catch (SQLException failure) { throw storage(failure); } }
    /** 添加终态时间列，源码后端的旧记录由有界恢复逐批回填并开始计算保留期。 */
    public static void ensureFinishedColumn(DataSource source, String table) {
        if (!table.matches("(pc_(write|rollback)|(omid|atlas)_txn)")) { throw new IllegalArgumentException("invalid finished table"); }
        try (var c = source.getConnection(); var q = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='finished_at'")) {
            q.setString(1, table); boolean exists; try (var r = q.executeQuery()) { r.next(); exists = r.getInt(1) != 0; }
            if (!exists) { try (var ddl = c.createStatement()) { ddl.execute("alter table " + table + " add column finished_at bigint not null default " + (table.startsWith("pc_") ? "(cast(unix_timestamp(current_timestamp(3))*1000 as signed))" : "0")); } }
        } catch (SQLException failure) { throw storage(failure); }
    }
    /** 使用固定摘要表示文件和路由身份。 */
    static String sha(byte[] bytes) { try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); } }
}

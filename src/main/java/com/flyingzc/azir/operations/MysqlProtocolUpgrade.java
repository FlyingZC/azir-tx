package com.flyingzc.azir.operations;

import com.flyingzc.azir.percolator.*;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/** 旧版独立 Percolator 的显式离线升级；回退窗口在任何新业务发号之前结束。 */
public final class MysqlProtocolUpgrade {
    /** 禁止实例化静态升级工具。 */
    private MysqlProtocolUpgrade() { }
    /** 验证停写旧空间后建立绑定，保持所有业务值和原始时间戳，返回冻结凭据。 */
    public static MysqlProtocolAdmin.Checkpoint adoptOffline(DataSource control, Map<String, DataSource> shards, boolean offline) {
        if (!offline) { throw new IllegalArgumentException("legacy upgrade requires explicit offline mode"); }
        var safeControl = new com.flyingzc.azir.control.ProtocolMysqlDataSource(control);
        var safeShards = new TreeMap<String, DataSource>(); shards.forEach((id, source) -> safeShards.put(id, new com.flyingzc.azir.control.ProtocolMysqlDataSource(source)));
        var lifecycle = MysqlPercolatorLifecycle.adoptLegacyOffline(safeControl, safeShards, 60_000);
        var stores = new TreeMap<String, PercolatorStore>(); safeShards.forEach((id, source) -> {
            var store = new MysqlPercolatorStore(id, source, lifecycle.operations()); store.initialize(); stores.put(id, store);
        });
        try (var runtime = new PercolatorRuntime(new MysqlTimestampOracle(safeControl), stores, PercolatorRuntime.Options.defaults(), lifecycle)) {
            var admin = runtime.administration(); var token = admin.freeze();
            long sequence = MysqlProtocolAdmin.scalar(safeControl, "select current_value from mvcc_tso where sequence_name='global'");
            admin.locked(() -> {
                for (var role : MysqlProtocolBackup.roles(admin).entrySet()) { admin.change(role.getValue(), role.getKey(), token.generation(), "FROZEN", token.id(), "legacy-sequence:" + sequence); }
                return null;
            });
            return token;
        }
    }
    /** 未启用新事务、GC 或发号时允许撤销新增绑定；旧业务数据和 TSO 不删除。 */
    public static void rollbackOffline(MysqlProtocolAdmin admin, boolean offline) {
        admin.requireWritable();
        if (!offline || !admin.state().prefix().equals("pc")) { throw new IllegalArgumentException("offline Percolator rollback required"); }
        admin.locked(() -> {
            var status = admin.state().status(); String plan = status.destination();
            if (!status.mode().equals("FROZEN") || plan == null || !plan.startsWith("legacy-sequence:")) { throw new IllegalStateException("legacy rollback window closed"); }
            long expected = Long.parseLong(plan.substring("legacy-sequence:".length()));
            if (MysqlProtocolAdmin.scalar(admin.state().control(), "select current_value from mvcc_tso where sequence_name='global'") != expected) { throw new IllegalStateException("new timestamps allocated; restore a verified backup instead"); }
            for (var role : admin.status().values()) { if (!role.mode().equals("FROZEN") || role.retiredBefore() != 0 || !Objects.equals(role.destination(), plan)) { throw new IllegalStateException("not a pristine frozen upgrade"); } }
            for (var source : admin.state().shards().values()) {
                try (var c = source.getConnection(); var q = c.createStatement()) { q.execute("drop table if exists pc_binding"); q.execute("drop table if exists pc_runtime"); }
                catch (SQLException failure) { throw storage(failure); }
            }
            try (var c = admin.state().control().getConnection(); var q = c.createStatement()) { q.execute("drop table if exists pc_readers"); q.execute("drop table if exists pc_control"); q.execute("drop table if exists pc_runtime"); }
            catch (SQLException failure) { throw storage(failure); }
            return null;
        });
    }
    /** 离线接管必须没有残留锁、孤立载荷、已绑定身份或倒退的时间线。 */
    public static void validateLegacy(DataSource control, Map<String, DataSource> shards) {
        long sequence;
        try (var c = control.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select current_value from mvcc_tso where sequence_name='global'")) {
            MysqlProtocolSchema.requireLegacyTable(c, "mvcc_tso");
            if (!r.next() || r.getLong(1) <= 0) { throw new SQLException("legacy timestamp missing"); } sequence = r.getLong(1);
        } catch (SQLException failure) { throw storage(failure); }
        for (var source : shards.values()) {
            try (var connection = source.getConnection()) {
                for (String table : List.of("pc_guard", "pc_data", "pc_lock", "pc_write", "pc_rollback")) { MysqlProtocolSchema.requireLegacyTable(connection, table); }
            } catch (SQLException failure) { throw storage(failure); }
            if (exists(source, "pc_binding") && MysqlProtocolAdmin.scalar(source, "select count(*) from pc_binding") != 0) { throw new IllegalArgumentException("legacy data already bound"); }
            if (exists(source, "pc_runtime") && MysqlProtocolAdmin.scalar(source, "select count(*) from pc_runtime") != 0) { throw new IllegalArgumentException("legacy data has runtime fencing; refuse adoption"); }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_lock") != 0) { throw new IllegalArgumentException("drain legacy locks before upgrade"); }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_data d where not exists(select 1 from pc_write w where w.user_key=d.user_key and w.start_ts=d.start_ts)") != 0) { throw new IllegalArgumentException("orphan legacy prewrites"); }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_write where commit_ts<=start_ts or start_ts<=0") != 0) { throw new IllegalArgumentException("invalid legacy decision timestamps"); }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_data d where not exists(select 1 from pc_guard g where g.user_key=d.user_key)") != 0) { throw new IllegalArgumentException("legacy candidate index incomplete"); }
            for (String sql : List.of("select coalesce(max(commit_ts),0) from pc_write", "select coalesce(max(start_ts),0) from pc_data", "select coalesce(max(start_ts),0) from pc_rollback")) {
                if (MysqlProtocolAdmin.scalar(source, sql) > sequence) { throw new IllegalArgumentException("legacy TSO is behind persisted versions"); }
            }
        }
    }
    /** 只查询已知协议表名，不接受外部 SQL 片段。 */
    private static boolean exists(DataSource source, String table) {
        try (var c = source.getConnection(); var q = c.prepareStatement("select count(*) from information_schema.tables where table_schema=database() and table_name=?")) { q.setString(1, table); try (var r = q.executeQuery()) { r.next(); return r.getInt(1) > 0; } }
        catch (SQLException failure) { throw storage(failure); }
    }
}

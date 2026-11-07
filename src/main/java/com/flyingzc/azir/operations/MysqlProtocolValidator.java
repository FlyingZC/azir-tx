package com.flyingzc.azir.operations;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/** 冻结数据集的语义校验；文件摘要一致仍不能替代 TSO、决议和版本引用关系检查。 */
public final class MysqlProtocolValidator {
    private final MysqlProtocolAdmin admin;
    /** 绑定已经冻结或恢复中的管理空间。 */
    public MysqlProtocolValidator(MysqlProtocolAdmin admin) { this.admin = Objects.requireNonNull(admin); }
    /** 检查全部角色、时间线和跨分片决议引用，不允许对正在服务的数据集做不一致遍历。 */
    public void verify() {
        var state = admin.state(); String prefix = state.prefix(); var status = admin.status(); var control = status.get("control");
        for (var value : status.values()) {
            if (!Set.of("FROZEN", "RESTORING", "STAGED").contains(value.mode()) || value.generation() != control.generation()
                    || !value.namespace().equals(control.namespace()) || !Objects.equals(value.checkpoint(), control.checkpoint())) { throw new IllegalStateException("validation requires one non-serving checkpoint"); }
        }
        long sequence = scalar(state.control(), prefix.equals("pc") ? "select current_value from mvcc_tso where sequence_name='global'" : "select seq from " + prefix + "_sequence where id=1");
        if (sequence <= 0) { throw new IllegalStateException("timestamp sequence missing"); }
        long floor = scalar(state.control(), prefix.equals("pc") ? "select safe_point from pc_control where id=1" : "select safe_point from " + prefix + "_gc where id=1");
        if (floor < 0 || floor > sequence || status.values().stream().anyMatch(s -> s.retiredBefore() < 0 || s.retiredBefore() > floor)) { throw new IllegalStateException("safe point or retirement watermark exceeds timestamp sequence"); }
        if (prefix.equals("pc")) {
            zero(state.control(), "select count(*) from pc_readers");
            for (var source : state.shards().values()) {
                zero(source, "select count(*) from pc_lock");
                zero(source, "select count(*) from pc_write where start_ts<=0 or commit_ts<=start_ts or commit_ts>" + sequence);
                zero(source, "select count(*) from pc_data d where d.start_ts<=0 or d.start_ts>" + sequence + " or not exists(select 1 from pc_write w where w.user_key=d.user_key and w.start_ts=d.start_ts)");
                zero(source, "select count(*) from pc_rollback where start_ts<=0 or start_ts>" + sequence);
                zero(source, "select count(*) from pc_data d where not exists(select 1 from pc_guard g where g.user_key=d.user_key)");
                zero(source, "select count(*) from pc_rollback r join pc_write w on r.user_key=w.user_key and r.start_ts=w.start_ts");
            }
        } else {
            zero(state.control(), "select count(*) from " + prefix + "_txn where start_ts<=0 or start_ts>" + sequence + " or status not in ('COMMITTED','ABORTED') or finalized=0 or (status='COMMITTED' and (commit_ts<=start_ts or commit_ts>" + sequence + ")) or (status='ABORTED' and commit_ts<>0)");
            zero(state.control(), "select count(*) from " + prefix + "_conflicts where commit_ts>" + sequence);
            for (var source : state.shards().values()) {
                zero(source, "select count(*) from " + prefix + "_versions where stamp<=0 or stamp>" + sequence + " or shadow_ts is null or shadow_ts<=stamp or shadow_ts>" + sequence);
                zero(source, "select count(*) from " + prefix + "_versions v where not exists(select 1 from " + prefix + "_keys k where k.table_name=v.table_name and k.row_key=v.row_key)");
                try (var data = source.getConnection(); var q = data.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                     var authority = state.control().getConnection(); var lookup = authority.prepareStatement("select status,commit_ts from " + prefix + "_txn where start_ts=?")) {
                    q.setFetchSize(Integer.MIN_VALUE);
                    try (var rows = q.executeQuery("select distinct stamp,shadow_ts from " + prefix + "_versions order by stamp,shadow_ts")) {
                        while (rows.next()) {
                            MysqlProtocolState.checkInterrupted(); lookup.setLong(1, rows.getLong(1));
                            try (var decision = lookup.executeQuery()) { if (!decision.next() || !decision.getString(1).equals("COMMITTED") || decision.getLong(2) != rows.getLong(2)) { throw new IllegalStateException("version has no matching durable commit decision"); } }
                        }
                    }
                } catch (SQLException failure) { throw storage(failure); }
            }
        }
        for (var entry : state.shards().entrySet()) {
            try (var c = entry.getValue().getConnection(); var q = c.createStatement(); var rows = q.executeQuery("select namespace,shard from " + prefix + "_binding where id=1")) {
                if (!rows.next() || !control.namespace().equals(rows.getString(1)) || !entry.getKey().equals(rows.getString(2))) { throw new IllegalStateException("restored shard binding differs"); }
            } catch (SQLException failure) { throw storage(failure); }
        }
    }
    /** 对非空固定聚合结果进行校验，缺失 TSO 行不能当作零初始化。 */
    private static long scalar(DataSource source, String sql) {
        try (var c = source.getConnection(); var q = c.createStatement(); var r = q.executeQuery(sql)) { if (!r.next()) { throw new SQLException("required protocol row missing"); } return r.getLong(1); }
        catch (SQLException failure) { throw storage(failure); }
    }
    /** 固定不变量查询必须没有异常记录。 */
    private static void zero(DataSource source, String sql) { if (scalar(source, sql) != 0) { throw new IllegalStateException("protocol invariant violation during frozen validation"); } }
}

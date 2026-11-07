package com.flyingzc.azir.operations;

import com.flyingzc.azir.backend.BackendDecision;
import com.flyingzc.azir.backend.BackendId;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.sql.*;
import java.util.*;
import static com.flyingzc.azir.backend.BackendDecision.State.*;
import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/** 运维只读凭据查询，冻结或交接后仍可核对终态，不执行过期回滚或补写决议。 */
public final class MysqlProtocolReceipts {
    /** 禁止实例化凭据读取工具。 */
    private MysqlProtocolReceipts() { }

    /** 查询 Omid 或 AtlasDB 原控制权威，缺失记录只能返回未决或退休。 */
    public static BackendDecision lookup(MysqlProtocolAdmin admin, BackendId id) {
        Objects.requireNonNull(id); String prefix = admin.state().prefix();
        String implementation = switch (prefix) { case "omid" -> "omid"; case "atlas" -> "atlasdb"; default -> throw new IllegalArgumentException("source backend receipt required"); };
        if (!implementation.equals(id.implementation())) { throw new IllegalArgumentException("receipt implementation mismatch"); }
        return read(admin, id.namespace(), "control", id.startTs(), c -> {
            try (var q = c.prepareStatement("select status,commit_ts from " + prefix + "_txn where start_ts=?")) {
                q.setLong(1, id.startTs()); try (var r = q.executeQuery()) {
                    if (!r.next()) { return null; }
                    return switch (r.getString(1)) {
                        case "ACTIVE" -> { if (r.getLong(2) != 0) { throw new SQLException("pending receipt has commit timestamp"); } yield new BackendDecision(PENDING, 0); }
                        case "ABORTED" -> { if (r.getLong(2) != 0) { throw new SQLException("aborted receipt has commit timestamp"); } yield new BackendDecision(ABORTED, 0); }
                        case "COMMITTED" -> committed(id.startTs(), r.getLong(2));
                        default -> throw new SQLException("invalid durable transaction state");
                    };
                }
            }
        });
    }

    /** 查询 Percolator 原 primary，过期锁和缺失锁都不能由只读命令解释为回滚。 */
    public static BackendDecision lookup(MysqlProtocolAdmin admin, PercolatorRuntime.Handle handle) {
        Objects.requireNonNull(handle);
        if (!admin.state().prefix().equals("pc") || handle.namespace() == null) { throw new IllegalArgumentException("managed Percolator receipt required"); }
        byte[] key = Utf8KeyOrder.encode(handle.primary().key());
        return read(admin, handle.namespace(), "shard:" + handle.primary().shard(), handle.startTs(), c -> {
            try (var q = c.createStatement(); var r = q.executeQuery("select namespace,shard from pc_binding where id=1")) {
                if (!r.next() || !handle.namespace().equals(r.getString(1)) || !handle.primary().shard().equals(r.getString(2))) { throw new SQLException("receipt primary shard binding differs"); }
            }
            Long timestamp = null; boolean rollback;
            try (var q = c.prepareStatement("select commit_ts from pc_write where user_key=? and start_ts=?")) {
                q.setBytes(1, key); q.setLong(2, handle.startTs()); try (var r = q.executeQuery()) { if (r.next()) { timestamp = r.getLong(1); } }
            }
            try (var q = c.prepareStatement("select exists(select 1 from pc_rollback where user_key=? and start_ts=?)")) {
                q.setBytes(1, key); q.setLong(2, handle.startTs()); try (var r = q.executeQuery()) { r.next(); rollback = r.getBoolean(1); }
            }
            if (timestamp != null && rollback) { throw new SQLException("Percolator receipt has contradictory terminal proofs"); }
            if (timestamp != null) { return committed(handle.startTs(), timestamp); }
            return rollback ? new BackendDecision(ABORTED, 0) : null;
        });
    }

    /** 一个物理库内以同一本地快照读取水位与决议，命名锁丢失后禁止返回成功结果。 */
    private static BackendDecision read(MysqlProtocolAdmin admin, String namespace, String role, long startTs, Lookup lookup) {
        UUID.fromString(namespace); if (startTs <= 0) { throw new IllegalArgumentException("positive startTs required"); }
        if (!namespace.equals(admin.state().namespace())) { throw new IllegalArgumentException("receipt namespace differs from the opened authority"); }
        return admin.locked(() -> {
            try {
                admin.verifyGate(); requireAuthority(admin.state().status(), namespace);
                var source = role.equals("control") ? admin.state().control() : admin.state().shards().get(role.substring(6));
                if (source == null) { throw new IllegalArgumentException("receipt shard is not configured"); }
                BackendDecision result;
                try (var c = source.getConnection()) {
                    MysqlProtocolSchema.requireTable(c, admin.state().prefix() + "_runtime");
                    if (role.equals("control")) { MysqlProtocolSchema.requireTable(c, admin.state().prefix() + "_txn"); }
                    else {
                        for (String table : List.of("pc_binding", "pc_write", "pc_rollback")) { MysqlProtocolSchema.requireTable(c, table); }
                    }
                    c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ); c.setAutoCommit(false);
                    try {
                        var local = admin.state().read(c, role, false); requireAuthority(local, namespace);
                        var decision = lookup.read(c);
                        result = decision != null ? decision : new BackendDecision(startTs <= local.retiredBefore() ? RETIRED : PENDING, 0);
                        c.commit();
                    } catch (SQLException | RuntimeException | Error failure) {
                        try { c.rollback(); } catch (SQLException | RuntimeException | Error rollback) { if (failure != rollback) { failure.addSuppressed(rollback); } com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure); }
                        throw failure;
                    }
                }
                admin.verifyGate(); requireAuthority(admin.state().status(), namespace); return result;
            } catch (SQLException failure) { throw storage(failure); }
        });
    }

    /** 仅原权威及其冻结副本可供核对，暂存导入的数据不能假冒已接管的权威。 */
    private static void requireAuthority(MysqlProtocolState.Status status, String namespace) {
        if (!namespace.equals(status.namespace()) || !Set.of("ACTIVE", "FROZEN", "HANDED_OFF").contains(status.mode())) { throw new IllegalStateException("receipt authority is restoring, staged or belongs to another namespace"); }
    }

    /** 损坏的时间戳不能被包装为正常已提交凭据。 */
    private static BackendDecision committed(long startTs, long commitTs) throws SQLException {
        if (commitTs <= startTs) { throw new SQLException("receipt commit timestamp must exceed start timestamp"); }
        return new BackendDecision(COMMITTED, commitTs);
    }

    /** 只读取固定决议 SQL，不接受业务 SQL 或更改事务状态。 */
    @FunctionalInterface private interface Lookup { /** 同一本地快照内读取终态，缺失时返回空值。 */ BackendDecision read(Connection c) throws SQLException; }
}

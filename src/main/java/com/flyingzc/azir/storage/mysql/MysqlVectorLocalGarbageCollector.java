package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** 只回收正式向量本地旧行，混合键的跨片映射与已提交版本始终保留。 */
final class MysqlVectorLocalGarbageCollector {
    private final DataSource shard;
    private final String shardId;
    private final int stripes;
    private final MysqlVectorSnapshotLeaseStore protection;
    private volatile String expectedIdentity;

    /** 跨片映射绑定原保留序号、中心决议及真实版本载荷。 */
    private record CrossMapping(long txId, long sequence, long commitTs,
                                boolean deleted, byte[] payload) {
    }

    /** 标量和跨片数据行的持久身份用于识别无向量映射的旧协议记录。 */
    private record DataIdentity(long txId, long commitTs) {
    }

    /** 只接受已配置读者保护的同一分片布局。 */
    MysqlVectorLocalGarbageCollector(DataSource shard, String shardId, int stripes,
                                     MysqlVectorSnapshotLeaseStore protection) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.protection = Objects.requireNonNull(protection, "protection");
        if (shardId.isBlank() || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector local GC shard layout");
        }
        this.stripes = stripes;
    }

    /** 调度器只读此数据源身份以确认后台池没有指错物理分片。 */
    DataSource dataSource() {
        return shard;
    }

    /** 本地 GC 的分片编号必须等于发布者路由编号。 */
    String shardId() {
        return shardId;
    }

    /** 分条布局必须与发布者和中心读者租约相同。 */
    int stripeCount() {
        return stripes;
    }

    /** 持久游标只补缺失单例，重启不能清空已回收进度。 */
    void initializeProduct() {
        try (Connection connection = shard.getConnection(); Statement sql = connection.createStatement()) {
            String identity = MysqlVectorDatabaseIdentity.read(connection);
            if (expectedIdentity != null && !expectedIdentity.equals(identity)) {
                throw illegal("vector local GC shard identity changed during initialization");
            }
            sql.execute("""
                    create table if not exists mvcc_vector_local_gc_cursor_v1 (
                      singleton_id tinyint not null primary key,
                      after_key varbinary(256)
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_local_gc_cursor_v1(singleton_id, after_key)
                    values (1, null) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.executeUpdate();
            }
            try (PreparedStatement layout = connection.prepareStatement("""
                    select stripe_count from mvcc_vector_layout_v1 where singleton_id = 1
                    """); ResultSet row = layout.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw new SQLException("vector local GC stripe layout is missing or changed");
                }
            }
            expectedIdentity = identity;
        } catch (SQLException ex) {
            throw unavailable("initialize vector local GC failed", ex);
        }
    }

    /** 从中心持久安全向量取界限，再在分片键锁下有界删除更早本地版本。 */
    int collect(int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("vector local GC limit must be in 1..4096");
        }
        String expected = expectedIdentity;
        if (expected == null) {
            throw illegal("vector local GC was not initialized against a durable shard");
        }
        MysqlStripedSnapshotOracle.Snapshot floor = protection.currentSafeFloor();
        try {
            return MysqlConnectionContext.inTransaction(shard, connection -> {
                if (!expected.equals(MysqlVectorDatabaseIdentity.read(connection))) {
                    throw illegal("vector local GC shard identity changed before deletion");
                }
                byte[] after;
                try (PreparedStatement query = connection.prepareStatement("""
                        select after_key from mvcc_vector_local_gc_cursor_v1
                        where singleton_id = 1 for update
                        """); ResultSet row = query.executeQuery()) {
                    if (!row.next()) {
                        throw new SQLException("vector local GC cursor is missing");
                    }
                    after = row.getBytes(1);
                }
                int pageSize = Math.min(limit, 128);
                List<byte[]> keys = keysAfter(connection, after, pageSize);
                int reclaimed = 0;
                int scanned = 0;
                byte[] last = null;
                while (scanned < keys.size() && reclaimed < limit) {
                    byte[] key = keys.get(scanned++);
                    reclaimed += collectKey(connection, key, floor, limit - reclaimed);
                    last = key;
                }
                byte[] next = scanned == keys.size() && keys.size() < pageSize ? null : last;
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_vector_local_gc_cursor_v1
                        set after_key = ? where singleton_id = 1
                        """)) {
                    update.setBytes(1, next);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("vector local GC cursor update lost row");
                    }
                }
                return reclaimed;
            });
        } catch (SQLException ex) {
            throw unavailable("collect vector local versions failed", ex);
        }
    }

    /** 按二进制主键顺序从上轮持久游标读取有限候选。 */
    private static List<byte[]> keysAfter(Connection connection, byte[] after, int limit)
            throws SQLException {
        String sql = after == null ? """
                select distinct user_key from mvcc_vector_local_data_v1
                order by user_key limit ?
                """ : """
                select distinct user_key from mvcc_vector_local_data_v1
                where user_key > ? order by user_key limit ?
                """;
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = 1;
            if (after != null) {
                query.setBytes(parameter++, after);
            }
            query.setInt(parameter, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<byte[]> found = new ArrayList<>();
                while (rows.next()) {
                    found.add(rows.getBytes(1));
                }
                return found;
            }
        }
    }

    /** 同键稳定锁先于版本和意向检查，避免 GC 越过正在提交的本地或跨片写。 */
    private int collectKey(Connection connection, byte[] key,
                           MysqlStripedSnapshotOracle.Snapshot floor,
                           int budget) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("""
                insert into mvcc_key_lock(user_key) values (?)
                on duplicate key update user_key = user_key
                """)) {
            lock.setBytes(1, key);
            lock.executeUpdate();
        }
        for (String table : List.of("mvcc_intent", "mvcc_vector_intent_reservation_v1",
                "mvcc_rollback")) {
            if (hasKey(connection, table, key)) {
                return 0;
            }
        }
        if (hasUnmappedScalarData(connection, key)) {
            return 0;
        }
        int stripe;
        try (PreparedStatement latest = connection.prepareStatement("""
                select stripe_id from mvcc_vector_local_data_v1
                where user_key = ? order by local_seq desc limit 1 for update
                """)) {
            latest.setBytes(1, key);
            try (ResultSet row = latest.executeQuery()) {
                if (!row.next()) {
                    return 0;
                }
                stripe = row.getInt(1);
            }
        }
        if (stripe < 0 || stripe >= stripes) {
            throw illegal("vector local GC found an invalid data stripe");
        }
        try (PreparedStatement wrong = connection.prepareStatement("""
                select 1 from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id <> ? limit 1 for update
                """)) {
            wrong.setBytes(1, key);
            wrong.setInt(2, stripe);
            try (ResultSet row = wrong.executeQuery()) {
                if (row.next()) {
                    throw illegal("vector local GC found a key in multiple stripes");
                }
            }
        }
        long bound = floor.sequence(shardId, stripe);
        Long predecessor = null;
        try (PreparedStatement query = connection.prepareStatement("""
                select local_seq from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id = ? and local_seq <= ?
                order by local_seq desc limit 1 for update
                """)) {
            query.setBytes(1, key);
            query.setInt(2, stripe);
            query.setLong(3, bound);
            try (ResultSet row = query.executeQuery()) {
                if (row.next()) {
                    predecessor = row.getLong(1);
                }
            }
        }
        Long crossPredecessor = visibleCrossPredecessor(connection, key, stripe,
                bound, floor.decisionEpoch());
        if (crossPredecessor != null && (predecessor == null || crossPredecessor > predecessor)) {
            predecessor = crossPredecessor;
        }
        if (predecessor == null) {
            return 0;
        }
        try (PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id = ? and local_seq < ?
                order by local_seq limit ?
                """)) {
            delete.setBytes(1, key);
            delete.setInt(2, stripe);
            delete.setLong(3, predecessor);
            delete.setInt(4, budget);
            return delete.executeUpdate();
        }
    }

    /** 旧标量版本没有向量排序身份时保留整键历史，不能按本地序号推断它。 */
    private static boolean hasUnmappedScalarData(Connection connection, byte[] key)
            throws SQLException {
        List<DataIdentity> data = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                select tx_id, commit_ts from mvcc_data
                where user_key = ? for update
                """)) {
            query.setBytes(1, key);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    data.add(new DataIdentity(rows.getLong(1), rows.getLong(2)));
                }
            }
        }
        try (PreparedStatement mapping = connection.prepareStatement("""
                select 1 from mvcc_vector_committed_v1
                where user_key = ? and tx_id = ? and commit_ts = ?
                limit 1 for update
                """)) {
            for (DataIdentity version : data) {
                mapping.setBytes(1, key);
                mapping.setLong(2, version.txId());
                mapping.setLong(3, version.commitTs());
                try (ResultSet row = mapping.executeQuery()) {
                    if (!row.next()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** 决议时间已进入安全切面的跨片版本可作为本地旧行的稳定可见前驱。 */
    private static Long visibleCrossPredecessor(Connection connection, byte[] key,
                                                int stripe, long sequenceBound,
                                                long decisionBound) throws SQLException {
        List<CrossMapping> mappings = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                select tx_id, stripe_id, reserved_seq, commit_ts, deleted, payload
                from mvcc_vector_committed_v1 where user_key = ? for update
                """)) {
            query.setBytes(1, key);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getInt("stripe_id") != stripe) {
                        throw illegal("vector local GC found cross data in another stripe");
                    }
                    mappings.add(new CrossMapping(rows.getLong("tx_id"),
                            rows.getLong("reserved_seq"), rows.getLong("commit_ts"),
                            rows.getBoolean("deleted"), rows.getBytes("payload")));
                }
            }
        }
        Long predecessor = null;
        try (PreparedStatement version = connection.prepareStatement("""
                select deleted, value from mvcc_data
                where user_key = ? and tx_id = ? and commit_ts = ? for update
                """)) {
            for (CrossMapping mapping : mappings) {
                version.setBytes(1, key);
                version.setLong(2, mapping.txId());
                version.setLong(3, mapping.commitTs());
                try (ResultSet row = version.executeQuery()) {
                    if (!row.next() || row.getBoolean("deleted") != mapping.deleted()
                            || !Arrays.equals(row.getBytes("value"), mapping.payload())) {
                        throw illegal("vector local GC found an incomplete cross version proof");
                    }
                }
                if (mapping.sequence() <= sequenceBound && mapping.commitTs() <= decisionBound
                        && (predecessor == null || mapping.sequence() > predecessor)) {
                    predecessor = mapping.sequence();
                }
            }
        }
        return predecessor;
    }

    /** 未决意向与回滚证明尚不能由已发布下界安全回收。 */
    private static boolean hasKey(Connection connection, String table, byte[] key) throws SQLException {
        String sql = "select 1 from " + table + " where user_key = ? limit 1 for update";
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, key);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /** 发现跨分条身份破坏时停止 GC，而不是猜测应保留哪条版本链。 */
    private static MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /** 控制或分片数据库不可用时禁止按猜测下界继续回收。 */
    private static MvccException unavailable(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }
}

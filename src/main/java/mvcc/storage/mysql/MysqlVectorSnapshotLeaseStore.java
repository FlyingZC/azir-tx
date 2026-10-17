package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 进程级向量读者租约和 GC 安全下界共用持久行串行化的产品原型。 */
final class MysqlVectorSnapshotLeaseStore {
    private static final String FLOOR_TABLE = "mvcc_vector_gc_floor_v1";
    private static final String LEASE_TABLE = "mvcc_vector_reader_lease_v1";
    private final DataSource center;
    private final List<String> routes;
    private final int stripes;
    private final String layoutHash;
    private volatile String expectedIdentity;

    /** 持久 GC 下界的代数只在下界真正推进时递增。 */
    private record Floor(MysqlStripedSnapshotOracle.Snapshot snapshot, long generation) {
    }

    /** 固定路由和分条布局，不能用另一种拓扑解释租约向量。 */
    MysqlVectorSnapshotLeaseStore(DataSource center, List<String> routes, int stripes) {
        this.center = Objects.requireNonNull(center, "center");
        this.routes = routes.stream().sorted().toList();
        if (this.routes.isEmpty() || this.routes.stream().anyMatch(route -> route == null || route.isBlank())
                || this.routes.stream().distinct().count() != this.routes.size()
                || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector reader lease layout");
        }
        this.stripes = stripes;
        this.layoutHash = MysqlPublishedVectorCut.layoutFingerprint(this.routes, stripes);
    }

    /** GC 调度器在启动前核对控制库实际身份。 */
    DataSource centerDataSource() {
        return center;
    }

    /** 固定分片名与分条数的哈希是跨进程 GC 下界解释的前提。 */
    String layoutHash() {
        return layoutHash;
    }

    /** 初始化只补缺失零下界与租约表，重启不能清空旧读者或前进安全点。 */
    void initializeProduct() {
        try (Connection connection = center.getConnection(); Statement sql = connection.createStatement()) {
            String identity = MysqlVectorDatabaseIdentity.read(connection);
            if (expectedIdentity != null && !expectedIdentity.equals(identity)) {
                throw illegal("vector reader control identity changed during initialization");
            }
            sql.execute("""
                    create table if not exists mvcc_vector_gc_floor_v1 (
                      singleton_id tinyint not null primary key,
                      layout_hash char(64) not null,
                      decision_epoch bigint not null,
                      vector_bytes blob not null,
                      generation bigint not null
                    ) engine=InnoDB
                    """);
            sql.execute("""
                    create table if not exists mvcc_vector_reader_lease_v1 (
                      owner_id varchar(128) collate utf8mb4_0900_bin not null primary key,
                      layout_hash char(64) not null,
                      generation bigint not null,
                      decision_epoch bigint not null,
                      vector_bytes blob not null,
                      expires_at bigint not null,
                      index mvcc_vector_reader_expiry (expires_at, owner_id)
                    ) engine=InnoDB
                    """);
            MysqlStripedSnapshotOracle.Snapshot zero = zeroSnapshot();
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_gc_floor_v1
                      (singleton_id, layout_hash, decision_epoch, vector_bytes, generation)
                    values (1, ?, 0, ?, 1)
                    on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setString(1, layoutHash);
                insert.setBytes(2, encode(zero));
                insert.executeUpdate();
            }
            readFloor(connection, false);
            expectedIdentity = identity;
        } catch (SQLException ex) {
            throw unavailable("initialize vector snapshot protection failed", ex);
        }
    }

    /** 先锁 GC 下界再登记进程最老可读切面，旧切面低于已回收下界时失败。 */
    Guard acquire(String ownerId, MysqlStripedSnapshotOracle.Snapshot floor,
                  long leaseMillis) {
        requireOwnerAndLease(ownerId, leaseMillis);
        Objects.requireNonNull(floor, "floor");
        long requestStarted = System.nanoTime();
        try {
            long generation = MysqlConnectionContext.inTransaction(center, connection -> {
                Floor safe = readFloor(connection, true);
                requireAtLeast(floor, safe.snapshot());
                long now = databaseNow(connection);
                Long previous = null;
                Long expiry = null;
                try (PreparedStatement query = connection.prepareStatement("""
                        select generation, expires_at from mvcc_vector_reader_lease_v1
                        where owner_id = ? for update
                        """)) {
                    query.setString(1, ownerId);
                    try (ResultSet row = query.executeQuery()) {
                        if (row.next()) {
                            previous = row.getLong(1);
                            expiry = row.getLong(2);
                        }
                    }
                }
                if (expiry != null && expiry > now) {
                    throw illegal("vector reader owner already has an active lease");
                }
                if (previous != null && previous == Long.MAX_VALUE) {
                    throw illegal("vector reader lease generation exhausted");
                }
                long nextGeneration = previous == null ? 1 : previous + 1;
                try (PreparedStatement update = connection.prepareStatement("""
                        insert into mvcc_vector_reader_lease_v1
                          (owner_id, layout_hash, generation, decision_epoch,
                           vector_bytes, expires_at)
                        values (?, ?, ?, ?, ?, ?)
                        on duplicate key update
                          layout_hash = values(layout_hash),
                          generation = values(generation),
                          decision_epoch = values(decision_epoch),
                          vector_bytes = values(vector_bytes),
                          expires_at = values(expires_at)
                        """)) {
                    update.setString(1, ownerId);
                    update.setString(2, layoutHash);
                    update.setLong(3, nextGeneration);
                    update.setLong(4, floor.decisionEpoch());
                    update.setBytes(5, encode(floor));
                    update.setLong(6, Math.addExact(now, leaseMillis));
                    update.executeUpdate();
                }
                return nextGeneration;
            });
            return new Guard(ownerId, generation, leaseMillis,
                    conservativeDeadline(requestStarted, leaseMillis));
        } catch (SQLException ex) {
            throw unavailable("acquire vector reader lease failed", ex);
        }
    }

    /** GC 只推进至当前可信发布切面和全部未过期进程保护下界的逐分量最小值。 */
    MysqlStripedSnapshotOracle.Snapshot advanceSafeFloor(
            MysqlStripedSnapshotOracle.Snapshot published) {
        Objects.requireNonNull(published, "published");
        try {
            return MysqlConnectionContext.inTransaction(center, connection -> {
                Floor current = readFloor(connection, true);
                requireAtLeast(published, current.snapshot());
                long now = databaseNow(connection);
                MysqlStripedSnapshotOracle.Snapshot next = published;
                try (PreparedStatement query = connection.prepareStatement("""
                        select layout_hash, decision_epoch, vector_bytes
                        from mvcc_vector_reader_lease_v1
                        where expires_at > ? for share
                        """)) {
                    query.setLong(1, now);
                    try (ResultSet row = query.executeQuery()) {
                        while (row.next()) {
                            if (!layoutHash.equals(row.getString(1))) {
                                throw new SQLException("active vector reader has incompatible layout");
                            }
                            next = minimum(next, new MysqlStripedSnapshotOracle.Snapshot(
                                    row.getLong(2), decode(row.getBytes(3))));
                        }
                    }
                }
                requireAtLeast(next, current.snapshot());
                if (!next.equals(current.snapshot())) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_vector_gc_floor_v1
                            set decision_epoch = ?, vector_bytes = ?, generation = generation + 1
                            where singleton_id = 1 and generation = ? and layout_hash = ?
                            """)) {
                        update.setLong(1, next.decisionEpoch());
                        update.setBytes(2, encode(next));
                        update.setLong(3, current.generation());
                        update.setString(4, layoutHash);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("vector GC floor lost durable generation");
                        }
                    }
                }
                return next;
            });
        } catch (SQLException ex) {
            throw unavailable("advance vector GC floor failed", ex);
        }
    }

    /** 返回已持久发布的 GC 下界，不把本地缓存作为回收依据。 */
    MysqlStripedSnapshotOracle.Snapshot currentSafeFloor() {
        try (Connection connection = center.getConnection()) {
            return readFloor(connection, false).snapshot();
        } catch (SQLException ex) {
            throw unavailable("read vector GC floor failed", ex);
        }
    }

    /** 当前进程最老活跃切面只可单调上移，且不得越过持久 GC 下界反向续租。 */
    private void renew(Guard guard, MysqlStripedSnapshotOracle.Snapshot floor) {
        guard.requireActive();
        Objects.requireNonNull(floor, "floor");
        long requestStarted = System.nanoTime();
        try {
            MysqlConnectionContext.inTransaction(center, connection -> {
                Floor safe = readFloor(connection, true);
                requireAtLeast(floor, safe.snapshot());
                long now = databaseNow(connection);
                MysqlStripedSnapshotOracle.Snapshot previous;
                long expiry;
                try (PreparedStatement query = connection.prepareStatement("""
                        select layout_hash, generation, decision_epoch, vector_bytes, expires_at
                        from mvcc_vector_reader_lease_v1 where owner_id = ? for update
                        """)) {
                    query.setString(1, guard.ownerId);
                    try (ResultSet row = query.executeQuery()) {
                        if (!row.next() || !layoutHash.equals(row.getString(1))
                                || row.getLong(2) != guard.generation) {
                            throw illegal("vector reader lease was superseded");
                        }
                        previous = new MysqlStripedSnapshotOracle.Snapshot(
                                row.getLong(3), decode(row.getBytes(4)));
                        expiry = row.getLong(5);
                    }
                }
                if (expiry <= now) {
                    throw illegal("vector reader lease expired before renewal");
                }
                requireAtLeast(floor, previous);
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_vector_reader_lease_v1
                        set decision_epoch = ?, vector_bytes = ?, expires_at = ?
                        where owner_id = ? and generation = ? and layout_hash = ?
                        """)) {
                    update.setLong(1, floor.decisionEpoch());
                    update.setBytes(2, encode(floor));
                    update.setLong(3, Math.addExact(now, guard.leaseMillis));
                    update.setString(4, guard.ownerId);
                    update.setLong(5, guard.generation);
                    update.setString(6, layoutHash);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("vector reader lease renewal lost owner generation");
                    }
                }
                return null;
            });
            guard.validUntilNanos = conservativeDeadline(requestStarted, guard.leaseMillis);
        } catch (SQLException | RuntimeException ex) {
            guard.revoked.set(true);
            if (ex instanceof SQLException sql) {
                throw unavailable("renew vector reader lease failed", sql);
            }
            throw (RuntimeException) ex;
        }
    }

    /** 本地先撤销服务资格，再让同代数持久租约到期并保留接任代数。 */
    private void release(Guard guard) {
        guard.revoked.set(true);
        try {
            MysqlConnectionContext.inTransaction(center, connection -> {
                readFloor(connection, true);
                long now = databaseNow(connection);
                try (PreparedStatement expire = connection.prepareStatement("""
                        update mvcc_vector_reader_lease_v1
                        set expires_at = least(expires_at, ?)
                        where owner_id = ? and generation = ? and layout_hash = ?
                        """)) {
                    expire.setLong(1, now);
                    expire.setString(2, guard.ownerId);
                    expire.setLong(3, guard.generation);
                    expire.setString(4, layoutHash);
                    expire.executeUpdate();
                }
                return null;
            });
        } catch (SQLException ex) {
            throw unavailable("release vector reader lease failed", ex);
        }
    }

    /** 同一进程的本地租约守卫不允许其他代数续约或继续服务。 */
    final class Guard implements AutoCloseable {
        private final String ownerId;
        private final long generation;
        private final long leaseMillis;
        private final AtomicBoolean revoked = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();
        private volatile long validUntilNanos;

        /** 只能由成功落盘的 acquire 构造。 */
        private Guard(String ownerId, long generation, long leaseMillis, long validUntilNanos) {
            this.ownerId = ownerId;
            this.generation = generation;
            this.leaseMillis = leaseMillis;
            this.validUntilNanos = validUntilNanos;
        }

        /** 返回当前持久代数以识别故障后的旧进程。 */
        long generation() {
            return generation;
        }

        /** 本地截止早于中心租约，断联时立即停止读切面服务。 */
        void requireActive() {
            if (revoked.get() || System.nanoTime() - validUntilNanos >= 0) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "vector reader lease expired or was revoked", true, null);
            }
        }

        /** 续租必须携带此进程当前最老活跃切面。 */
        synchronized void renew(MysqlStripedSnapshotOracle.Snapshot floor) {
            MysqlVectorSnapshotLeaseStore.this.renew(this, floor);
        }

        /** 发布者或读者进程故障时立即拒绝本地后续读，中心租约自然到期。 */
        void revokeLocally() {
            revoked.set(true);
        }

        /** 先停止本地服务，再让 GC 可以不再保留本进程旧切面。 */
        @Override
        public synchronized void close() {
            revoked.set(true);
            if (released.compareAndSet(false, true)) {
                MysqlVectorSnapshotLeaseStore.this.release(this);
            }
        }
    }

    /** 读取中心单例行并验证布局、代数及非负向量。 */
    private Floor readFloor(Connection connection, boolean lock) throws SQLException {
        String expected = expectedIdentity;
        if (expected != null && !expected.equals(MysqlVectorDatabaseIdentity.read(connection))) {
            throw illegal("vector reader control identity changed before safe floor access");
        }
        String sql = "select layout_hash, decision_epoch, vector_bytes, generation from "
                + FLOOR_TABLE + " where singleton_id = 1" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql);
             ResultSet row = query.executeQuery()) {
            if (!row.next() || !layoutHash.equals(row.getString(1)) || row.getLong(4) < 1) {
                throw new SQLException("vector GC floor layout or generation is invalid");
            }
            return new Floor(new MysqlStripedSnapshotOracle.Snapshot(
                    row.getLong(2), decode(row.getBytes(3))), row.getLong(4));
        }
    }

    /** 每条路径只接受同一固定布局、且逐分量不早于已回收下界的切面。 */
    void requireAtLeast(MysqlStripedSnapshotOracle.Snapshot candidate,
                        MysqlStripedSnapshotOracle.Snapshot floor) {
        encode(candidate);
        if (candidate.decisionEpoch() < floor.decisionEpoch()) {
            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                    "vector decision cut precedes GC floor", false, null);
        }
        for (String route : routes) {
            for (int stripe = 0; stripe < stripes; stripe++) {
                if (candidate.sequence(route, stripe) < floor.sequence(route, stripe)) {
                    throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                            "vector data cut precedes GC floor", false, null);
                }
            }
        }
    }

    /** GC 对每个分量取当前发布上界与所有有效读者最老切面的最小值。 */
    MysqlStripedSnapshotOracle.Snapshot minimum(
            MysqlStripedSnapshotOracle.Snapshot left,
            MysqlStripedSnapshotOracle.Snapshot right) {
        Map<String, List<Long>> merged = new LinkedHashMap<>();
        for (String route : routes) {
            List<Long> components = new ArrayList<>(stripes);
            for (int stripe = 0; stripe < stripes; stripe++) {
                components.add(Math.min(left.sequence(route, stripe), right.sequence(route, stripe)));
            }
            merged.put(route, List.copyOf(components));
        }
        return new MysqlStripedSnapshotOracle.Snapshot(
                Math.min(left.decisionEpoch(), right.decisionEpoch()), merged);
    }

    /** 向量按布局顺序编码，不依赖 Map 遍历顺序。 */
    private byte[] encode(MysqlStripedSnapshotOracle.Snapshot snapshot) {
        ByteBuffer bytes = ByteBuffer.allocate(
                Math.multiplyExact(Math.multiplyExact(routes.size(), stripes), Long.BYTES));
        if (snapshot.localSequences().size() != routes.size()) {
            throw new IllegalArgumentException("vector reader snapshot route count differs from layout");
        }
        for (String route : routes) {
            List<Long> components = snapshot.localSequences().get(route);
            if (components == null || components.size() != stripes) {
                throw new IllegalArgumentException("vector reader snapshot has incomplete layout");
            }
            for (Long value : components) {
                if (value == null || value < 0) {
                    throw new IllegalArgumentException("vector reader snapshot has invalid sequence");
                }
                bytes.putLong(value);
            }
        }
        return bytes.array();
    }

    /** 解码前先验证字节长度和每一分量，损坏租约不能放宽安全下界。 */
    private Map<String, List<Long>> decode(byte[] encoded) throws SQLException {
        int expected = Math.multiplyExact(Math.multiplyExact(routes.size(), stripes), Long.BYTES);
        if (encoded == null || encoded.length != expected) {
            throw new SQLException("vector reader lease bytes differ from layout");
        }
        ByteBuffer bytes = ByteBuffer.wrap(encoded);
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (String route : routes) {
            List<Long> components = new ArrayList<>(stripes);
            for (int stripe = 0; stripe < stripes; stripe++) {
                long value = bytes.getLong();
                if (value < 0) {
                    throw new SQLException("vector reader lease has negative sequence");
                }
                components.add(value);
            }
            result.put(route, List.copyOf(components));
        }
        return Map.copyOf(result);
    }

    /** 初始 GC 下界为零，故障重建不能从当前最新切面跳过旧读者。 */
    private MysqlStripedSnapshotOracle.Snapshot zeroSnapshot() {
        Map<String, List<Long>> values = new LinkedHashMap<>();
        for (String route : routes) {
            values.put(route, Collections.nCopies(stripes, 0L));
        }
        return new MysqlStripedSnapshotOracle.Snapshot(0, values);
    }

    /** 数据库时间用于判断所有协调进程共有的持久租约期限。 */
    private static long databaseNow(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("vector reader database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 进程本地服务截止预留数据库租约的网络与时钟余量。 */
    private static long conservativeDeadline(long startedNanos, long leaseMillis) {
        long margin = Math.max(25, leaseMillis / 5);
        return Math.addExact(startedNanos,
                TimeUnit.MILLISECONDS.toNanos(leaseMillis - margin));
    }

    /** 进程身份和租期必须保持可索引与可保守续租。 */
    private static void requireOwnerAndLease(String ownerId, long leaseMillis) {
        if (ownerId == null || ownerId.isBlank() || ownerId.length() > 128
                || leaseMillis < 200 || leaseMillis > 60_000) {
            throw new IllegalArgumentException("invalid vector reader owner or lease duration");
        }
    }

    /** 协议不变量不成立时停止读和 GC，不把它当作可重试 SQL 错误。 */
    private static MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /** 数据库无法提供持久栅栏时失败关闭。 */
    private static MvccException unavailable(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }
}

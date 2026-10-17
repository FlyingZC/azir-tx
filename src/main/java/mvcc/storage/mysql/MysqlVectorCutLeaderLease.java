package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 内存向量发布者的中心租约实验，旧代数失效后禁止继续提供读切面与 ACK。 */
final class MysqlVectorCutLeaderLease {
    private final DataSource center;
    private final String layoutHash;
    private final String leaseTable;

    /** 客户端只可连接当前未过期代数对应的领导者端点。 */
    record LeaderEndpoint(String ownerId, long generation, long remainingMillis) {
        /** 排除空地址、未初始化代数和已过期的中心租约。 */
        LeaderEndpoint {
            if (ownerId == null || ownerId.isBlank() || generation < 1 || remainingMillis < 1) {
                throw new IllegalArgumentException("invalid vector leader endpoint");
            }
        }
    }

    /** 绑定控制库与固定路由布局，不让接任者用相同代数解释另一套分片。 */
    MysqlVectorCutLeaderLease(DataSource center, Map<String, DataSource> shards, int stripes) {
        this(center, shards, stripes, false);
    }

    /** 正式事务服务使用独立持久租约表，不和容量实验争夺领导者。 */
    static MysqlVectorCutLeaderLease forProduct(DataSource center,
                                                 Map<String, DataSource> shards, int stripes) {
        return new MysqlVectorCutLeaderLease(center, shards, stripes, true);
    }

    /** 两套固定表名共用同一代数和布局校验逻辑。 */
    private MysqlVectorCutLeaderLease(DataSource center, Map<String, DataSource> shards,
                                      int stripes, boolean product) {
        this.center = Objects.requireNonNull(center, "center");
        Objects.requireNonNull(shards, "shards");
        List<String> routes = shards.keySet().stream().sorted().toList();
        if (routes.isEmpty() || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector leader route layout");
        }
        this.layoutHash = MysqlPublishedVectorCut.layoutFingerprint(routes, stripes);
        this.leaseTable = product ? "mvcc_vector_leader_v1" : "bench_striped_vector_leader";
    }

    /** 运行时必须和远端客户端使用同一个持久租约命名空间。 */
    boolean productMode() {
        return "mvcc_vector_leader_v1".equals(leaseTable);
    }

    /** 只有显式隔离实验才重建租约表。 */
    void initializeIsolated() {
        if (!"bench_striped_vector_leader".equals(leaseTable)) {
            throw new IllegalStateException("product vector leader requires non-destructive initialization");
        }
        try (Connection connection = center.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bench_striped_vector_leader");
            statement.execute("""
                    create table bench_striped_vector_leader (
                      singleton_id tinyint not null primary key,
                      owner_id varchar(128),
                      layout_hash char(64) not null,
                      generation bigint not null,
                      expires_at bigint not null
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_striped_vector_leader
                      (singleton_id, owner_id, layout_hash, generation, expires_at)
                    values (1, null, ?, 0, 0)
                    """)) {
                insert.setString(1, layoutHash);
                if (insert.executeUpdate() != 1) {
                    throw new SQLException("initial vector leader row was not persisted");
                }
            }
        } catch (SQLException ex) {
            throw unavailable("initialize vector leader lease failed", ex);
        }
    }

    /** 正式租约初始化只补缺失单例并校验布局，不清除当前有效领导者。 */
    void initializeProduct() {
        if (!"mvcc_vector_leader_v1".equals(leaseTable)) {
            throw new IllegalStateException("benchmark vector leader requires isolated initialization");
        }
        try (Connection connection = center.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_vector_leader_v1 (
                      singleton_id tinyint not null primary key,
                      owner_id varchar(128),
                      layout_hash char(64) not null,
                      generation bigint not null,
                      expires_at bigint not null
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_leader_v1
                      (singleton_id, owner_id, layout_hash, generation, expires_at)
                    values (1, null, ?, 0, 0)
                    on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setString(1, layoutHash);
                insert.executeUpdate();
            }
            discover();
        } catch (SQLException ex) {
            throw unavailable("initialize product vector leader lease failed", ex);
        }
    }

    /** 租约仍有效时拒绝任何新领导者；过期接任原子增加代数。 */
    Optional<LeaseGuard> acquire(String ownerId, long leaseMillis) {
        requireIdentity(ownerId, leaseMillis);
        long requestStarted = System.nanoTime();
        try {
            Long generation = MysqlConnectionContext.inTransaction(center, connection -> {
                long previous;
                long expiry;
                long now;
                String durableLayout;
                String querySql = """
                        select layout_hash, generation, expires_at,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                        from %s where singleton_id = 1 for update
                        """.formatted(leaseTable);
                try (PreparedStatement query = connection.prepareStatement(querySql);
                     ResultSet row = query.executeQuery()) {
                    if (!row.next()) {
                        throw new SQLException("vector leader lease row is missing");
                    }
                    previous = row.getLong("generation");
                    expiry = row.getLong("expires_at");
                    now = row.getLong("now_millis");
                    durableLayout = row.getString("layout_hash");
                }
                if (!layoutHash.equals(durableLayout)) {
                    throw new SQLException("vector leader route layout differs from durable lease");
                }
                if (expiry > now) {
                    return null;
                }
                if (previous == Long.MAX_VALUE) {
                    throw new SQLException("vector leader generation exhausted");
                }
                String updateSql = """
                        update %s
                        set owner_id = ?, generation = ?, expires_at = ?
                        where singleton_id = 1 and layout_hash = ? and generation = ?
                        """.formatted(leaseTable);
                try (PreparedStatement update = connection.prepareStatement(updateSql)) {
                    update.setString(1, ownerId);
                    update.setLong(2, previous + 1);
                    update.setLong(3, Math.addExact(now, leaseMillis));
                    update.setString(4, layoutHash);
                    update.setLong(5, previous);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("vector leader generation changed during acquisition");
                    }
                }
                return previous + 1;
            });
            return generation == null ? Optional.empty()
                    : Optional.of(new LeaseGuard(center, leaseTable, ownerId, layoutHash,
                    generation, leaseMillis,
                    conservativeDeadline(requestStarted, leaseMillis)));
        } catch (SQLException ex) {
            throw unavailable("acquire vector leader lease failed", ex);
        }
    }

    /** 读侧只查询中心租约而不修改它，过期或路由布局错误时失败关闭。 */
    Optional<LeaderEndpoint> discover() {
        String querySql = """
                select owner_id, layout_hash, generation, expires_at,
                       cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                from %s where singleton_id = 1
                """.formatted(leaseTable);
        try (Connection connection = center.getConnection(); PreparedStatement query = connection.prepareStatement(querySql);
             ResultSet row = query.executeQuery()) {
            if (!row.next() || !layoutHash.equals(row.getString("layout_hash"))) {
                throw new SQLException("vector leader route layout is missing or changed");
            }
            String owner = row.getString("owner_id");
            long remaining = row.getLong("expires_at") - row.getLong("now_millis");
            if (owner == null || remaining <= 0) {
                return Optional.empty();
            }
            return Optional.of(new LeaderEndpoint(owner, row.getLong("generation"), remaining));
        } catch (SQLException ex) {
            throw unavailable("discover vector leader failed", ex);
        }
    }

    /** 限制租约身份、数据库期限和本地安全余量的取值。 */
    private static void requireIdentity(String ownerId, long leaseMillis) {
        if (ownerId == null || ownerId.isBlank() || ownerId.length() > 128
                || leaseMillis < 200 || leaseMillis > 60_000) {
            throw new IllegalArgumentException("invalid vector leader owner or lease duration");
        }
    }

    /** 本地截止从请求发送前计时并留出余量，先于数据库租约到期。 */
    private static long conservativeDeadline(long requestStarted, long leaseMillis) {
        long margin = Math.max(25, leaseMillis / 5);
        return Math.addExact(requestStarted, TimeUnit.MILLISECONDS.toNanos(leaseMillis - margin));
    }

    /** 控制面不可达时保持失败关闭，不能凭本地推测接任发布。 */
    private static MvccException unavailable(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /** 一个代数的本地截止仅由确认成功的中心租约结果延长。 */
    static final class LeaseGuard {
        private final DataSource center;
        private final String leaseTable;
        private final String ownerId;
        private final String layoutHash;
        private final long generation;
        private final long leaseMillis;
        private final AtomicBoolean revoked = new AtomicBoolean();
        private volatile long validUntilNanos;

        /** 只由中心成功接任创建，不接受外部伪造代数。 */
        private LeaseGuard(DataSource center, String leaseTable, String ownerId,
                           String layoutHash, long generation,
                           long leaseMillis, long validUntilNanos) {
            this.center = center;
            this.leaseTable = leaseTable;
            this.ownerId = ownerId;
            this.layoutHash = layoutHash;
            this.generation = generation;
            this.leaseMillis = leaseMillis;
            this.validUntilNanos = validUntilNanos;
        }

        /** 返回客户端响应须携带的领导者代数。 */
        long generation() {
            return generation;
        }

        /** 发布者只可解释取得租约时写入中心的同一分片布局。 */
        String layoutHash() {
            return layoutHash;
        }

        /** 切面来源必须与取得代数的中心租约表一致。 */
        boolean productMode() {
            return "mvcc_vector_leader_v1".equals(leaseTable);
        }

        /** 发布、读切面和确认前都检查本地保守截止。 */
        void requireActive() {
            if (revoked.get() || remainingNanos() <= 0) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "vector leader lease expired or was revoked", true, null);
            }
        }

        /** 等待中的 ACK 用此截止及时退出，而不等完整事务超时。 */
        long remainingNanos() {
            return revoked.get() ? 0 : validUntilNanos - System.nanoTime();
        }

        /** 只有同代数且未过期的中心行才能延长本地截止。 */
        void renew() {
            requireActive();
            long requestStarted = System.nanoTime();
            try {
                MysqlConnectionContext.inTransaction(center, connection -> {
                    String currentOwner;
                    String currentLayout;
                    long currentGeneration;
                    long expiry;
                    long now;
                    String querySql = """
                            select owner_id, layout_hash, generation, expires_at,
                                   cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                            from %s where singleton_id = 1 for update
                            """.formatted(leaseTable);
                    try (PreparedStatement query = connection.prepareStatement(querySql);
                         ResultSet row = query.executeQuery()) {
                        if (!row.next()) {
                            throw new SQLException("vector leader lease row is missing");
                        }
                        currentOwner = row.getString("owner_id");
                        currentLayout = row.getString("layout_hash");
                        currentGeneration = row.getLong("generation");
                        expiry = row.getLong("expires_at");
                        now = row.getLong("now_millis");
                    }
                    if (!ownerId.equals(currentOwner) || !layoutHash.equals(currentLayout)
                            || currentGeneration != generation
                            || expiry <= now) {
                        throw new SQLException("vector leader lease was superseded or expired");
                    }
                    String updateSql = """
                            update %s set expires_at = ?
                            where singleton_id = 1 and owner_id = ?
                              and layout_hash = ? and generation = ?
                            """.formatted(leaseTable);
                    try (PreparedStatement update = connection.prepareStatement(updateSql)) {
                        update.setLong(1, Math.addExact(now, leaseMillis));
                        update.setString(2, ownerId);
                        update.setString(3, layoutHash);
                        update.setLong(4, generation);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("vector leader lease renewal lost generation");
                        }
                    }
                    return null;
                });
                validUntilNanos = conservativeDeadline(requestStarted, leaseMillis);
            } catch (SQLException ex) {
                revoked.set(true);
                throw unavailable("renew vector leader lease failed", ex);
            }
        }

        /** 本进程自愿退出时立即拒绝后续读写，中心租约仍等到自然到期。 */
        void revokeLocally() {
            revoked.set(true);
        }
    }
}

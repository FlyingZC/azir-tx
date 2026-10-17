package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.HexFormat;

/** 按批发布持久跨分片向量切面，写入确认必须等待自身序号进入切面。 */
final class MysqlPublishedVectorCut implements AutoCloseable {
    private final DataSource center;
    private final MysqlStripedSnapshotOracle oracle;
    private final LongSupplier decisionEpoch;
    private final List<String> routes;
    private final int stripes;
    private final String layoutHash;
    private final boolean volatileExperiment;
    private final boolean productState;
    private final String cutTable;
    private final MysqlVectorCutLeaderLease.LeaseGuard leaseGuard;
    private final ReentrantLock waitLock = new ReentrantLock();
    private final Condition changed = waitLock.newCondition();
    private final LongAdder sampleNanos = new LongAdder();
    private final LongAdder centerNanos = new LongAdder();
    private final LongAdder samples = new LongAdder();
    private final LongAdder centerWrites = new LongAdder();
    private volatile Cut cached;

    /** 持久行同时保存中心决议、各分条上界和单调发布代数。 */
    private record Cut(MysqlStripedSnapshotOracle.Snapshot snapshot, long generation) {
    }

    /** 发布器必须绑定固定路由集合和分条数，防止故障切换后误解码旧切面。 */
    MysqlPublishedVectorCut(DataSource center, Map<String, DataSource> shards, int stripes,
                            LongSupplier decisionEpoch) {
        this(center, shards, stripes, decisionEpoch, false, null, false);
    }

    /** 正式分片水位使用独立持久切面表，不读取容量探针数据。 */
    static MysqlPublishedVectorCut productDurable(DataSource center,
                                                   Map<String, DataSource> shards, int stripes,
                                                   LongSupplier decisionEpoch) {
        return new MysqlPublishedVectorCut(center, shards, stripes, decisionEpoch,
                false, null, true);
    }

    /** 纯内存模式只用于验证无每批 fsync 的容量上界，生产调用方不得选择它。 */
    static MysqlPublishedVectorCut experimentalVolatile(DataSource center,
                                                         Map<String, DataSource> shards, int stripes,
                                                         LongSupplier decisionEpoch) {
        return new MysqlPublishedVectorCut(center, shards, stripes, decisionEpoch,
                true, null, false);
    }

    /** 领导者模式要求先取得持久租约，旧代数到期后不再服务客户端。 */
    static MysqlPublishedVectorCut leasedVolatile(DataSource center,
                                                   Map<String, DataSource> shards, int stripes,
                                                   LongSupplier decisionEpoch,
                                                   MysqlVectorCutLeaderLease.LeaseGuard guard) {
        Objects.requireNonNull(guard, "guard").requireActive();
        if (guard.productMode()) {
            throw new IllegalArgumentException("benchmark cut cannot use product vector lease");
        }
        String expectedLayout = layoutFingerprint(shards.keySet().stream().sorted().toList(), stripes);
        if (!expectedLayout.equals(guard.layoutHash())) {
            throw new IllegalArgumentException("vector leader lease route layout differs from publication cut");
        }
        return new MysqlPublishedVectorCut(center, shards, stripes, decisionEpoch,
                true, guard, false);
    }

    /** 正式参与者的持租约快切面从真实水位表重建，不写每批中心单例行。 */
    static MysqlPublishedVectorCut leasedProduct(DataSource center,
                                                  Map<String, DataSource> shards, int stripes,
                                                  LongSupplier decisionEpoch,
                                                  MysqlVectorCutLeaderLease.LeaseGuard guard) {
        Objects.requireNonNull(guard, "guard").requireActive();
        if (!guard.productMode()) {
            throw new IllegalArgumentException("product cut requires product vector lease");
        }
        String expectedLayout = layoutFingerprint(shards.keySet().stream().sorted().toList(), stripes);
        if (!expectedLayout.equals(guard.layoutHash())) {
            throw new IllegalArgumentException("product vector leader route layout differs from lease");
        }
        return new MysqlPublishedVectorCut(center, shards, stripes, decisionEpoch,
                true, guard, true);
    }

    /** 固定是否执行明确标记的非持久容量实验。 */
    private MysqlPublishedVectorCut(DataSource center, Map<String, DataSource> shards, int stripes,
                                    LongSupplier decisionEpoch, boolean volatileExperiment,
                                    MysqlVectorCutLeaderLease.LeaseGuard leaseGuard,
                                    boolean productState) {
        this.center = Objects.requireNonNull(center, "center");
        this.decisionEpoch = Objects.requireNonNull(decisionEpoch, "decisionEpoch");
        this.oracle = productState
                ? MysqlStripedSnapshotOracle.forProduct(shards, stripes, 1, true)
                : new MysqlStripedSnapshotOracle(shards, stripes, 1, true);
        this.routes = shards.keySet().stream().sorted().toList();
        this.stripes = stripes;
        this.layoutHash = layoutFingerprint(routes, stripes);
        this.volatileExperiment = volatileExperiment;
        this.leaseGuard = leaseGuard;
        this.productState = productState;
        this.cutTable = productState ? "mvcc_vector_published_cut_v1" : "bench_striped_published_cut";
    }

    /** 只在显式隔离控制库重建实验表，初始切面先读决议再读分片。 */
    void initializeIsolated() {
        if (volatileExperiment || productState) {
            throw new IllegalStateException("isolated benchmark cut cannot initialize this state source");
        }
        try (Connection connection = center.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bench_striped_published_cut");
            statement.execute("""
                    create table bench_striped_published_cut (
                      singleton_id tinyint not null primary key,
                      layout_hash char(64) not null,
                      decision_epoch bigint not null,
                      generation bigint not null,
                      vector_bytes blob not null
                    ) engine=InnoDB
                    """);
            long epoch = decisionEpoch.getAsLong();
            Map<String, List<Long>> vector = oracle.sampleForPublication();
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_striped_published_cut
                      (singleton_id, layout_hash, decision_epoch, generation, vector_bytes)
                    values (1, ?, ?, 1, ?)
                    """)) {
                insert.setString(1, layoutHash);
                insert.setLong(2, epoch);
                insert.setBytes(3, encode(vector));
                if (insert.executeUpdate() != 1) {
                    throw new SQLException("initial published vector was not persisted");
                }
            }
            installCache(new Cut(new MysqlStripedSnapshotOracle.Snapshot(epoch, vector), 1));
        } catch (SQLException ex) {
            throw unavailable("initialize published vector cut failed", ex);
        }
    }

    /** 正式控制库只创建或验证切面，不清空先前已确认的持久向量。 */
    void initializeProduct() {
        if (!productState || volatileExperiment) {
            throw new IllegalStateException("product durable cut requires product state source");
        }
        try (Connection connection = center.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_vector_published_cut_v1 (
                      singleton_id tinyint not null primary key,
                      layout_hash char(64) not null,
                      decision_epoch bigint not null,
                      generation bigint not null,
                      vector_bytes blob not null
                    ) engine=InnoDB
                    """);
            long epoch = decisionEpoch.getAsLong();
            Map<String, List<Long>> sampled = oracle.sampleForPublication();
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_published_cut_v1
                      (singleton_id, layout_hash, decision_epoch, generation, vector_bytes)
                    values (1, ?, ?, 1, ?)
                    on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setString(1, layoutHash);
                insert.setLong(2, epoch);
                insert.setBytes(3, encode(sampled));
                insert.executeUpdate();
            }
            Cut persisted = readCut(connection, false);
            if (epoch < persisted.snapshot().decisionEpoch()) {
                throw new SQLException("product decision epoch moved behind acknowledged cut");
            }
            for (String route : routes) {
                for (int stripe = 0; stripe < stripes; stripe++) {
                    if (sampled.get(route).get(stripe) < persisted.snapshot().sequence(route, stripe)) {
                        throw new SQLException("product stripe state moved behind acknowledged cut");
                    }
                }
            }
            installCache(persisted);
        } catch (SQLException ex) {
            throw unavailable("initialize product vector cut failed", ex);
        }
    }

    /** 崩溃重启从持久分片和中心决议重建切面；仅验证单领导者实验上界。 */
    void initializeVolatile() {
        if (!volatileExperiment) {
            throw new IllegalStateException("durable cut must initialize its center table");
        }
        requireLeader();
        long epoch = decisionEpoch.getAsLong();
        Map<String, List<Long>> vector = oracle.sampleForPublication();
        requireLeader();
        installCache(new Cut(new MysqlStripedSnapshotOracle.Snapshot(
                epoch, vector), 1));
    }

    /** 单轮先采中心决议再采各分片，控制库一次事务发布完整单调切面。 */
    MysqlStripedSnapshotOracle.Snapshot publishOnce() {
        requireLeader();
        long epoch = decisionEpoch.getAsLong();
        long sampleStarted = System.nanoTime();
        Map<String, List<Long>> sampled = oracle.sampleForPublication();
        sampleNanos.add(System.nanoTime() - sampleStarted);
        samples.increment();
        try {
            Cut old = cached;
            if (old == null) {
                try (Connection connection = center.getConnection()) {
                    old = readCut(connection, false);
                }
                installCache(old);
            }
            if (epoch < old.snapshot().decisionEpoch()) {
                throw new SQLException("center decision epoch moved behind published cut");
            }
            for (String route : routes) {
                for (int stripe = 0; stripe < stripes; stripe++) {
                    if (sampled.get(route).get(stripe) < old.snapshot().sequence(route, stripe)) {
                        throw new SQLException("shard watermark moved behind published cut");
                    }
                }
            }
            var next = new MysqlStripedSnapshotOracle.Snapshot(epoch, sampled);
            if (next.equals(old.snapshot())) {
                requireLeader();
                return next;
            }
            if (volatileExperiment) {
                requireLeader();
                installCache(new Cut(next, old.generation() + 1));
                return next;
            }
            long centerStarted = System.nanoTime();
            String updateSql = "update " + cutTable
                    + " set decision_epoch = ?, vector_bytes = ?, generation = generation + 1"
                    + " where singleton_id = 1 and layout_hash = ? and generation = ?";
            try (Connection connection = center.getConnection(); PreparedStatement update = connection.prepareStatement(updateSql)) {
                if (!connection.getAutoCommit()) {
                    throw new SQLException("published vector compare-and-swap requires auto-commit");
                }
                update.setLong(1, epoch);
                update.setBytes(2, encode(sampled));
                update.setString(3, layoutHash);
                update.setLong(4, old.generation());
                if (update.executeUpdate() != 1) {
                    throw new SQLException("published vector generation changed concurrently");
                }
            }
            centerNanos.add(System.nanoTime() - centerStarted);
            centerWrites.increment();
            // JDBC 自动提交成功后再唤醒等待者；响应未知绝不提前确认。
            installCache(new Cut(next, old.generation() + 1));
            return next;
        } catch (SQLException ex) {
            throw unavailable("publish vector cut failed", ex);
        }
    }

    /** RC 每条语句读取持久最新切面；RR 调用方保存第一次返回的不可变结果。 */
    MysqlStripedSnapshotOracle.Snapshot currentCut() {
        if (volatileExperiment) {
            requireLeader();
            Cut current = cached;
            if (current == null) {
                throw new IllegalStateException("volatile cut has not been reconstructed");
            }
            return current.snapshot();
        }
        try (Connection connection = center.getConnection()) {
            return readCut(connection, false).snapshot();
        } catch (SQLException ex) {
            throw unavailable("read published vector cut failed", ex);
        }
    }

    /** 多进程协调器可按批刷新本地确认缓存，而无需每笔写入查询中心。 */
    void refresh() {
        if (volatileExperiment) {
            publishOnce();
            return;
        }
        try (Connection connection = center.getConnection()) {
            installCache(readCut(connection, false));
        } catch (SQLException ex) {
            throw unavailable("refresh published vector cut failed", ex);
        }
    }

    /** 仅供容量探针读取并行分片单次采样的均值毫秒数。 */
    double sampleAverageMillis() {
        return samples.sum() == 0 ? Double.NaN : sampleNanos.sum() / (double) samples.sum() / 1_000_000.0;
    }

    /** 仅供容量探针读取中心持久单行更新的均值毫秒数。 */
    double centerWriteAverageMillis() {
        return centerWrites.sum() == 0 ? Double.NaN
                : centerNanos.sum() / (double) centerWrites.sum() / 1_000_000.0;
    }

    /** 仅在持久切面包含本地序号和必要中心决议后确认写事务。 */
    void awaitLocallyPublished(String shardId, int stripe, long sequence,
                               long minimumDecisionEpoch, long timeoutMillis) {
        if (sequence < 0 || minimumDecisionEpoch < 0 || timeoutMillis < 1) {
            throw new IllegalArgumentException("invalid published vector wait boundary");
        }
        long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        waitLock.lock();
        try {
            requireLeader();
            while (cached == null || cached.snapshot().sequence(shardId, stripe) < sequence
                    || cached.snapshot().decisionEpoch() < minimumDecisionEpoch) {
                if (remaining <= 0) {
                    throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                            "write was not included in a durable published vector", true, null);
                }
                try {
                    long leaseRemaining = leaseGuard == null ? Long.MAX_VALUE : leaseGuard.remainingNanos();
                    if (leaseRemaining <= 0) {
                        requireLeader();
                    }
                    long waitStarted = System.nanoTime();
                    changed.awaitNanos(Math.min(remaining, leaseRemaining));
                    remaining -= System.nanoTime() - waitStarted;
                    requireLeader();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                            "published vector wait interrupted", true, null, ex);
                }
            }
            requireLeader();
        } finally {
            waitLock.unlock();
        }
    }

    /** 读出中心单例行并验证固定路由布局，拒绝不兼容的故障恢复数据。 */
    private Cut readCut(Connection connection, boolean lock) throws SQLException {
        String sql = "select layout_hash, decision_epoch, generation, vector_bytes "
                + "from " + cutTable + " where singleton_id = 1"
                + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql);
             ResultSet row = query.executeQuery()) {
            if (!row.next() || !layoutHash.equals(row.getString("layout_hash"))) {
                throw new SQLException("published vector route layout is missing or changed");
            }
            long epoch = row.getLong("decision_epoch");
            long generation = row.getLong("generation");
            if (epoch < 0 || generation < 1) {
                throw new SQLException("published vector epoch or generation is invalid");
            }
            return new Cut(new MysqlStripedSnapshotOracle.Snapshot(
                    epoch, decode(row.getBytes("vector_bytes"))), generation);
        }
    }

    /** 路由按固定顺序编码为无符号语义的长整数序列。 */
    private byte[] encode(Map<String, List<Long>> values) {
        ByteBuffer bytes = ByteBuffer.allocate(Math.multiplyExact(Math.multiplyExact(routes.size(), stripes), Long.BYTES));
        for (String route : routes) {
            List<Long> sequences = values.get(route);
            if (sequences == null || sequences.size() != stripes) {
                throw new IllegalArgumentException("published vector has incomplete route layout");
            }
            for (Long sequence : sequences) {
                if (sequence == null || sequence < 0) {
                    throw new IllegalArgumentException("published vector contains invalid sequence");
                }
                bytes.putLong(sequence);
            }
        }
        return bytes.array();
    }

    /** 解码前验证总长度和每个分量，损坏行不能变成可用读快照。 */
    private Map<String, List<Long>> decode(byte[] encoded) throws SQLException {
        int expected = Math.multiplyExact(Math.multiplyExact(routes.size(), stripes), Long.BYTES);
        if (encoded == null || encoded.length != expected) {
            throw new SQLException("published vector binary length differs from route layout");
        }
        ByteBuffer bytes = ByteBuffer.wrap(encoded);
        Map<String, List<Long>> values = new LinkedHashMap<>();
        for (String route : routes) {
            Long[] sequences = new Long[stripes];
            for (int stripe = 0; stripe < stripes; stripe++) {
                long sequence = bytes.getLong();
                if (sequence < 0) {
                    throw new SQLException("published vector has negative sequence");
                }
                sequences[stripe] = sequence;
            }
            values.put(route, List.of(sequences));
        }
        return Map.copyOf(values);
    }

    /** 布局哈希把分片身份与分条数固化到持久行，避免相同长度的错误路由。 */
    static String layoutFingerprint(List<String> routes, int stripes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(stripes).array());
            for (String route : routes) {
                byte[] name = route.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(name.length).array());
                digest.update(name);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required for published vector layout", ex);
        }
    }

    /** 中心持久写成功后才唤醒本进程的提交等待者。 */
    private void installCache(Cut cut) {
        waitLock.lock();
        try {
            cached = cut;
            changed.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    /** 运行时撤销租约后立即唤醒正在等待发布确认的前台线程。 */
    void wakeWaiters() {
        waitLock.lock();
        try {
            changed.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    /** 仅租约型内存发布者需要验证本地保守截止。 */
    private void requireLeader() {
        if (leaseGuard != null) {
            leaseGuard.requireActive();
        }
    }

    /** SQL 故障使发布失败关闭，不得向客户端确认未入持久切面的写入。 */
    private static MvccException unavailable(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /** 释放后台并行采样线程。 */
    @Override
    public void close() {
        if (leaseGuard != null) {
            leaseGuard.revokeLocally();
            waitLock.lock();
            try {
                changed.signalAll();
            } finally {
                waitLock.unlock();
            }
        }
        oracle.close();
    }
}

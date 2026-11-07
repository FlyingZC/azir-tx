package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** 分条本地版本的隔离快照原型，两次采集验证跨分片水位形成同一稳定切面。 */
public final class MysqlStripedSnapshotOracle implements AutoCloseable {
    /** 实验水位与正式参与者水位使用不同表，查询语句固定不接受外部表名。 */
    private enum StateSource { BENCH, PRODUCT }

    private final Map<String, DataSource> shards;
    private final List<String> routes;
    private final int stripes;
    private final int maxRetries;
    private final ExecutorService readers;
    private final StateSource stateSource;
    private final AtomicLong collectionAttempts = new AtomicLong();

    /** 意向决议查询必须从持久中心返回已提交时间，ACTIVE 或 ABORTED 返回空。 */
    @FunctionalInterface
    public interface CrossDecisionResolver {
        /** 中心状态未知或不可达时必须抛错，不能把未知意向当作未提交。 */
        OptionalLong committedEpoch(long txId);
    }

    /** 固定控制决议水位与每个分片每条本地序号，不允许外部修改已捕获快照。 */
    public record Snapshot(long decisionEpoch, Map<String, List<Long>> localSequences) {
        /** 每个分量必须是不可变、非负的持久提交序号。 */
        public Snapshot {
            if (decisionEpoch < 0 || localSequences == null || localSequences.isEmpty()) {
                throw new IllegalArgumentException("invalid striped snapshot epoch or routes");
            }
            Map<String, List<Long>> copied = new LinkedHashMap<>();
            localSequences.forEach((route, sequences) -> {
                List<Long> fixed = List.copyOf(sequences);
                if (route == null || route.isBlank() || fixed.isEmpty()
                        || fixed.stream().anyMatch(value -> value == null || value < 0)) {
                    throw new IllegalArgumentException("invalid striped snapshot component");
                }
                copied.put(route, fixed);
            });
            localSequences = Map.copyOf(copied);
        }

        /** 返回目标分片某条水位在快照时已持久完成的最大序号。 */
        public long sequence(String shardId, int stripe) {
            List<Long> values = localSequences.get(shardId);
            if (values == null || stripe < 0 || stripe >= values.size()) {
                throw new IllegalArgumentException("striped snapshot has no such shard component");
            }
            return values.get(stripe);
        }
    }

    /** 每个被读分片都要有完整分条，失败不能猜测缺失分片的快照。 */
    public MysqlStripedSnapshotOracle(Map<String, DataSource> shards, int stripes, int maxRetries) {
        this(shards, stripes, maxRetries, false);
    }

    /** 可选并行采集每个分片，以缩短两次完整向量读取的竞争窗口。 */
    public MysqlStripedSnapshotOracle(Map<String, DataSource> shards, int stripes,
                                      int maxRetries, boolean parallelReads) {
        this(shards, stripes, maxRetries, parallelReads, StateSource.BENCH);
    }

    /** 正式参与者切面从真实 `mvcc_vector_stripe_state_v1` 读取，不能复用容量探针表。 */
    public static MysqlStripedSnapshotOracle forProduct(Map<String, DataSource> shards,
                                                         int stripes, int maxRetries,
                                                         boolean parallelReads) {
        return new MysqlStripedSnapshotOracle(shards, stripes, maxRetries,
                parallelReads, StateSource.PRODUCT);
    }

    /** 所有构造入口共用完整布局与轮次边界校验。 */
    private MysqlStripedSnapshotOracle(Map<String, DataSource> shards, int stripes,
                                       int maxRetries, boolean parallelReads,
                                       StateSource stateSource) {
        this.shards = Map.copyOf(shards);
        if (this.shards.isEmpty() || stripes < 1 || stripes > 256
                || maxRetries < 1 || maxRetries > 4096) {
            throw new IllegalArgumentException("invalid striped snapshot routes or limits");
        }
        this.routes = this.shards.keySet().stream().sorted().toList();
        this.stripes = stripes;
        this.maxRetries = maxRetries;
        this.readers = parallelReads ? Executors.newVirtualThreadPerTaskExecutor() : null;
        this.stateSource = stateSource;
    }

    /** 控制决议水位夹在两轮分片读取中；分量变化则重试而不返回割裂快照。 */
    public Snapshot capture(LongSupplier centerDecisionEpoch) {
        Objects.requireNonNull(centerDecisionEpoch, "centerDecisionEpoch");
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            collectionAttempts.incrementAndGet();
            Map<String, List<Long>> first = readAll();
            long epoch = centerDecisionEpoch.getAsLong();
            Map<String, List<Long>> second = readAll();
            if (first.equals(second)) {
                return new Snapshot(epoch, first);
            }
        }
        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "striped snapshot did not find a stable cross-shard cut", true, null);
    }

    /** 返回实际双采集轮数，便于识别高写入压力下隐藏的快照重试成本。 */
    public long collectionAttempts() {
        return collectionAttempts.get();
    }

    /** 仅供持久发布器采样；单轮原始向量不能直接充当客户端 RC/RR 快照。 */
    Map<String, List<Long>> sampleForPublication() {
        return readAll();
    }

    /** 按固定 RR 快照分量读取版本；RC 调用方每条语句重新 capture。 */
    public Optional<byte[]> read(Snapshot snapshot, String shardId, String key, int stripe) {
        requireBenchDataReader();
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(key, "key");
        DataSource dataSource = shards.get(shardId);
        if (dataSource == null || stripe < 0 || stripe >= stripes) {
            throw new IllegalArgumentException("striped snapshot read has no matching route");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select payload from bench_striped_mvcc_data
                where user_key = ? and stripe_id = ? and local_seq <= ?
                order by local_seq desc limit 1
                """)) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            query.setInt(2, stripe);
            query.setLong(3, snapshot.sequence(shardId, stripe));
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? Optional.of(row.getBytes(1)) : Optional.empty();
            }
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read striped MVCC prototype version failed", false, null, ex);
        }
    }

    /** 单条 MySQL 读视图合并本地版本、跨片意向及物化版本，避免物化瞬间漏读。 */
    public Optional<byte[]> readMixed(Snapshot snapshot, String shardId, String key,
                                      int stripe, CrossDecisionResolver decisions) {
        requireBenchDataReader();
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(decisions, "decisions");
        DataSource dataSource = shards.get(shardId);
        if (dataSource == null || stripe < 0 || stripe >= stripes) {
            throw new IllegalArgumentException("striped mixed read has no matching route");
        }
        long bound = snapshot.sequence(shardId, stripe);
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select local_seq as version_seq, payload, 0 as origin,
                       0 as tx_id, 0 as decision_epoch
                from bench_striped_mvcc_data
                where user_key = ? and stripe_id = ? and local_seq <= ?
                union all
                select reserved_seq as version_seq, payload, 1 as origin,
                       tx_id, decision_epoch
                from bench_striped_mvcc_cross_data
                where user_key = ? and stripe_id = ? and reserved_seq <= ?
                union all
                select reserved_seq as version_seq, payload, 2 as origin,
                       tx_id, 0 as decision_epoch
                from bench_striped_mvcc_cross_intent
                where user_key = ? and stripe_id = ? and reserved_seq <= ?
                order by version_seq desc
                """)) {
            byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
            for (int group = 0; group < 3; group++) {
                int offset = group * 3;
                query.setBytes(offset + 1, encodedKey);
                query.setInt(offset + 2, stripe);
                query.setLong(offset + 3, bound);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    int origin = rows.getInt("origin");
                    if (origin == 0 || (origin == 1
                            && rows.getLong("decision_epoch") <= snapshot.decisionEpoch())) {
                        return Optional.of(rows.getBytes("payload"));
                    }
                    if (origin == 2) {
                        OptionalLong committed = decisions.committedEpoch(rows.getLong("tx_id"));
                        if (committed.isPresent() && committed.getAsLong() <= snapshot.decisionEpoch()) {
                            return Optional.of(rows.getBytes("payload"));
                        }
                    }
                }
                return Optional.empty();
            }
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read striped mixed MVCC prototype version failed", false, null, ex);
        }
    }

    /** 依固定路由顺序采集所有分片，单个分片的一次查询使用同一 MySQL 读视图。 */
    private Map<String, List<Long>> readAll() {
        Map<String, List<Long>> found = new LinkedHashMap<>();
        if (readers != null) {
            Map<String, CompletableFuture<List<Long>>> pending = new LinkedHashMap<>();
            for (String shardId : routes) {
                pending.put(shardId, CompletableFuture.supplyAsync(() -> readOne(shardId), readers));
            }
            for (String shardId : routes) {
                try {
                    found.put(shardId, pending.get(shardId).join());
                } catch (CompletionException ex) {
                    if (ex.getCause() instanceof RuntimeException cause) {
                        throw cause;
                    }
                    throw ex;
                }
            }
            return Map.copyOf(found);
        }
        for (String shardId : routes) {
            found.put(shardId, readOne(shardId));
        }
        return Map.copyOf(found);
    }

    /** 一个分片的全部分条必须由同一 MySQL 语句视图原子读取。 */
    private List<Long> readOne(String shardId) {
        List<Long> values = new ArrayList<>(stripes);
        String querySql = stateSource == StateSource.PRODUCT
                ? "select stripe_id, committed_seq from mvcc_vector_stripe_state_v1 order by stripe_id"
                : "select stripe_id, committed_seq from bench_striped_mvcc_state order by stripe_id";
        try (Connection connection = shards.get(shardId).getConnection();
             PreparedStatement query = connection.prepareStatement(querySql);
             ResultSet row = query.executeQuery()) {
            while (row.next()) {
                if (row.getInt("stripe_id") != values.size()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "striped snapshot has missing or repeated local stripe", false, null);
                }
                values.add(row.getLong("committed_seq"));
            }
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read striped MVCC prototype watermark failed", false, null, ex);
        }
        if (values.size() != stripes) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "striped snapshot has incomplete shard watermark", false, null);
        }
        return List.copyOf(values);
    }

    /** 正式切面不允许误用实验版本表的单键读取入口。 */
    private void requireBenchDataReader() {
        if (stateSource != StateSource.BENCH) {
            throw new IllegalStateException("product vector versions require MysqlStorageAdapter vector read");
        }
    }

    /** 结束并行分片读取线程，避免容量探针退出后继续占用连接。 */
    @Override
    public void close() {
        if (readers != null) {
            readers.close();
        }
    }
}

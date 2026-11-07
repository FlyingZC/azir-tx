package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.metadata.mysql.MysqlLocalFirstMetadataView;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** 在独立跨片容量探针数据库中测量后继写入后的真实版本退休与后台积压。 */
public final class MysqlVectorCrossGcCapacityProbe {
    /** 跨片原始负载由前置容量探针生成，本入口只测恢复后退休阶段。 */
    private MysqlVectorCrossGcCapacityProbe() {
    }

    /** 只允许显式开启的隔离库运行，任何未最终化跨片事务都阻止退役测量。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_VECTOR_CROSS_GC_PROBE", "false"))) {
            throw new IllegalArgumentException("cross GC probe requires AZIR_TX_VECTOR_CROSS_GC_PROBE=true");
        }
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        int seconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "120"));
        int gcLimit = Integer.parseInt(env("AZIR_TX_VECTOR_CROSS_GC_BATCH_LIMIT", "64"));
        int parallelism = Integer.parseInt(env("AZIR_TX_VECTOR_CROSS_GC_PARALLELISM", "1"));
        int retireBatch = Integer.parseInt(env("AZIR_TX_VECTOR_CROSS_GC_RETIRE_BATCH", "1"));
        int localEveryRounds = Integer.parseInt(env("AZIR_TX_VECTOR_LOCAL_GC_EVERY_ROUNDS", "1"));
        long intervalMillis = Long.parseLong(env("AZIR_TX_VECTOR_GC_INTERVAL_MS", "20"));
        if (stripes < 1 || stripes > 256 || seconds < 1 || seconds > 600
                || gcLimit < 1 || gcLimit > 512 || parallelism < 1 || parallelism > 32
                || retireBatch < 1 || retireBatch > 32
                || localEveryRounds < 1 || localEveryRounds > 1_000
                || intervalMillis < 1 || intervalMillis > 60_000) {
            throw new IllegalArgumentException("invalid cross GC probe limits");
        }
        String user = env("AZIR_TX_USERNAME", "root");
        String password = env("AZIR_TX_PASSWORD", "root");
        String controlUrl = env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        String leftUrl = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23307/shard_1");
        String rightUrl = env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23308/shard_2");
        try (HikariDataSource center = pool(controlUrl, user, password);
             HikariDataSource leftData = pool(leftUrl, user, password);
             HikariDataSource rightData = pool(rightUrl, user, password)) {
            var ledger = new MysqlLocalFirstPublicationLedger(center);
            if (ledger.countUnfinalizedIntentDecisions() != 0) {
                throw new IllegalStateException("cross GC probe requires fully finalized center decisions");
            }
            var metadata = new MysqlLocalFirstMetadataView(ledger);
            var left = new MysqlStorageAdapter("s1", leftData, metadata);
            var right = new MysqlStorageAdapter("s2", rightData, metadata);
            left.initialize();
            right.initialize();
            left.initializeVectorProtocol(stripes);
            right.initializeVectorProtocol(stripes);
            long leftBefore = count(leftData, "mvcc_vector_committed_v1");
            long rightBefore = count(rightData, "mvcc_vector_committed_v1");
            if (leftBefore == 0 || leftBefore != rightBefore
                    || count(leftData, "mvcc_vector_retired_v1") != 0
                    || count(rightData, "mvcc_vector_retired_v1") != 0) {
                throw new IllegalStateException("cross GC probe requires untouched two-shard mapping history");
            }
            long successorStart = System.nanoTime();
            long leftSuccessors = addSuccessors(leftData, left);
            long rightSuccessors = addSuccessors(rightData, right);
            double successorSeconds = elapsed(successorStart);
            if (leftSuccessors != leftBefore || rightSuccessors != rightBefore) {
                throw new IllegalStateException("cross GC probe failed to create all local successors");
            }
            Map<String, DataSource> routes = Map.of("s1", leftData, "s2", rightData);
            var leaderLease = MysqlVectorCutLeaderLease.forProduct(center, routes, stripes);
            leaderLease.initializeProduct();
            try (HikariDataSource backgroundCenter = backgroundPool(controlUrl, user, password);
                 HikariDataSource backgroundLeft = backgroundPool(leftUrl, user, password);
                 HikariDataSource backgroundRight = backgroundPool(rightUrl, user, password);
                 var leader = MysqlVectorCutLeaderRuntime.forProduct(center, routes, stripes,
                         ledger::currentPublished, leaderLease, "cross-gc-capacity-leader", 5_000, 200)) {
                if (!leader.start()) {
                    throw new IllegalStateException("cross GC probe found another active cut leader");
                }
                var backgroundLeases = new MysqlVectorSnapshotLeaseStore(backgroundCenter,
                        List.of("s1", "s2"), stripes);
                backgroundLeases.initializeProduct();
                var leftLocal = new MysqlVectorLocalGarbageCollector(backgroundLeft,
                        "s1", stripes, backgroundLeases);
                var rightLocal = new MysqlVectorLocalGarbageCollector(backgroundRight,
                        "s2", stripes, backgroundLeases);
                leftLocal.initializeProduct();
                rightLocal.initializeProduct();
                var leftCross = new MysqlVectorCrossGarbageCollector(left, backgroundLeft,
                        "s1", stripes, backgroundLeases, ledger, parallelism, retireBatch);
                var rightCross = new MysqlVectorCrossGarbageCollector(right, backgroundRight,
                        "s2", stripes, backgroundLeases, ledger, parallelism, retireBatch);
                leftCross.initializeProduct();
                rightCross.initializeProduct();
                try (var scheduler = new MysqlVectorLocalGcScheduler(leader, backgroundLeases,
                        Map.of("s1", leftLocal, "s2", rightLocal),
                        Map.of("s1", leftCross, "s2", rightCross),
                        Math.min(256, gcLimit), gcLimit, localEveryRounds, intervalMillis)) {
                    long gcStarted = System.nanoTime();
                    scheduler.start();
                    long deadline = gcStarted + TimeUnit.SECONDS.toNanos(seconds);
                    long nextReport = gcStarted + TimeUnit.SECONDS.toNanos(5);
                    while (System.nanoTime() < deadline) {
                        if (scheduler.stats().failureCount() != 0) {
                            throw new IllegalStateException("cross GC scheduler failed", scheduler.lastFailure());
                        }
                        long leftRemaining = count(leftData, "mvcc_vector_committed_v1");
                        long rightRemaining = count(rightData, "mvcc_vector_committed_v1");
                        if (leftRemaining == 0 && rightRemaining == 0) {
                            break;
                        }
                        if (System.nanoTime() >= nextReport) {
                            System.out.printf("VECTOR_CROSS_GC_WINDOW elapsed_s=%.2f "
                                            + "left_remaining=%d right_remaining=%d retired=%d runs=%d%n",
                                    elapsed(gcStarted), leftRemaining, rightRemaining,
                                    scheduler.crossReclaimed(), scheduler.stats().runCount());
                            nextReport += TimeUnit.SECONDS.toNanos(5);
                        }
                        Thread.sleep(200);
                    }
                    double gcSeconds = elapsed(gcStarted);
                    scheduler.close();
                    long leftRemaining = count(leftData, "mvcc_vector_committed_v1");
                    long rightRemaining = count(rightData, "mvcc_vector_committed_v1");
                    long leftRetired = count(leftData, "mvcc_vector_retired_v1");
                    long rightRetired = count(rightData, "mvcc_vector_retired_v1");
                    long leftDataRows = count(leftData, "mvcc_data");
                    long rightDataRows = count(rightData, "mvcc_data");
                    if (leftRemaining + leftRetired != leftBefore
                            || rightRemaining + rightRetired != rightBefore
                            || leftDataRows != leftRemaining || rightDataRows != rightRemaining
                            || scheduler.stats().failureCount() != 0) {
                        throw new IllegalStateException("cross GC probe lost mapping, data, or receipt conservation");
                    }
                    System.out.printf("VECTOR_CROSS_GC_CAPACITY parallelism=%d retire_batch=%d local_every=%d initial_per_shard=%d "
                                    + "successors_per_shard=%d successor_s=%.2f "
                                    + "gc_s=%.2f retired=%d retired_per_s=%.2f "
                                    + "left_remaining=%d right_remaining=%d "
                                    + "left_receipts=%d right_receipts=%d "
                                    + "runs=%d failures=%d local_gc_s=%.2f cross_gc_s=%.2f%n",
                            parallelism, retireBatch, localEveryRounds,
                            leftBefore, leftSuccessors, successorSeconds,
                            gcSeconds, leftRetired + rightRetired,
                            (leftRetired + rightRetired) / gcSeconds,
                            leftRemaining, rightRemaining, leftRetired, rightRetired,
                            scheduler.stats().runCount(), scheduler.stats().failureCount(),
                            scheduler.localCollectNanos() / 1_000_000_000.0,
                            scheduler.crossCollectNanos() / 1_000_000_000.0);
                }
            }
        }
    }

    /** 与原跨片探针使用相同的前台池配置。 */
    private static HikariDataSource pool(String url, String user, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, user, password);
    }

    /** 后台扫描与物理删除使用独立小池以限制对业务连接的抢占。 */
    private static HikariDataSource backgroundPool(String url, String user, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, user, password);
    }

    /** 对每笔已最终化跨片版本写一个同键本地后继，使旧大载荷成为可退役候选。 */
    private static long addSuccessors(HikariDataSource dataSource, MysqlStorageAdapter adapter)
            throws SQLException {
        List<Mapping> mappings = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select user_key, tx_id, stripe_id, reserved_seq
                     from mvcc_vector_committed_v1 order by tx_id
                     """); ResultSet row = query.executeQuery()) {
            while (row.next()) {
                mappings.add(new Mapping(new String(row.getBytes("user_key"), StandardCharsets.UTF_8),
                        row.getLong("tx_id"), row.getInt("stripe_id"), row.getLong("reserved_seq")));
            }
        }
        for (int index = 0; index < mappings.size(); index++) {
            Mapping mapping = mappings.get(index);
            // 容量探针把本地后继放在靠近 -1 的独立负号段，不能取反跨片负事务号。
            adapter.directCommitVectorLocal(-1L - index,
                    Mutation.put(mapping.key(), new byte[]{42}),
                    mapping.stripe(), mapping.sequence());
        }
        return mappings.size();
    }

    /** 原映射只包含可复现的单键和其真实保留序号。 */
    private record Mapping(String key, long txId, int stripe, long sequence) {
    }

    /** 实际物理行数在 GC 前后用于核对凭据与大载荷守恒。 */
    private static long count(HikariDataSource dataSource, String table) throws SQLException {
        String sql = "select count(*) from " + table;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement(sql);
             ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("cross GC probe count returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 单调计时只用于本地容量记录，不解释为多机生产延迟。 */
    private static double elapsed(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    /** 只允许显式环境配置覆盖隔离容量探针参数。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

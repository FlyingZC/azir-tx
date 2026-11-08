/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * Measurs the real version retirement and background backlog after writing in an independent cross-shard capacity probe database.
 */
public final class MysqlVectorCrossGcCapacityProbe {

    /**
     * The original cross-shard load is generated from the pre-capacity probe and only the post- recovery retirement phase of this entry.
     */
    private MysqlVectorCrossGcCapacityProbe() {
    }

    /**
     * Only visible-opened silos are allowed to operate, and any unfinalized cross-shard transaction prevents decommissioning measurements.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
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
        try (
                HikariDataSource center = pool(controlUrl, user, password);
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
            try (
                    HikariDataSource backgroundCenter = backgroundPool(controlUrl, user, password);
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
                try (
                        var scheduler = new MysqlVectorLocalGcScheduler(leader, backgroundLeases,
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

    /**
     * The same foreground pool configuration as the original cross-shard probe.
     *
     * @param url url
     * @param user user
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource pool(final String url, final String user, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, user, password);
    }

    /**
     * Background Scan and Physical Delete Use of Independent Pools to limit capture of operations connection.
     *
     * @param url url
     * @param user user
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource backgroundPool(final String url, final String user, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.createBackground(url, user, password);
    }

    /**
     * Writes a single local follow-up to each final cross-shard version, making the old big payload eligible for retirement.
     *
     * @param dataSource database connection source
     * @param adapter adapter
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long addSuccessors(final HikariDataSource dataSource, final MysqlStorageAdapter adapter) throws SQLException {
        List<Mapping> mappings = new ArrayList<>();
        try (
                Connection connection = dataSource.getConnection();
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
            // Volume probe places the local follow-on to an independent negative timestamp range near -1, which cannot be countered by a negative cross-shard negative transaction ID.
            adapter.directCommitVectorLocal(-1L - index,
                    Mutation.put(mapping.key(), new byte[]{42}),
                    mapping.stripe(), mapping.sequence());
        }
        return mappings.size();
    }

    /**
     * The actual physical line numbers are used before and after GC to check receipt for consistency with large payload.
     *
     * @param dataSource database connection source
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long count(final HikariDataSource dataSource, final String table) throws SQLException {
        String sql = "select count(*) from " + table;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement(sql);
                ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("cross GC probe count returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * The single-module time is used only for local capacity records and is not interpreted as a delay in multi-machine production.
     *
     * @param startedNanos started nanos
     * @return the operation result
     */
    private static double elapsed(final long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    /**
     * Only visible environment configurations are allowed to cover the isolation capacity probe parameters.
     *
     * @param name name
     * @param fallback fallback
     * @return the operation result
     */
    private static String env(final String name, final String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    /**
     * The original map contains only a single key that can be repeated and its true retention serial number.
     *
     * @param key business key
     * @param txId transaction identifier
     * @param stripe stripe
     * @param sequence sequence
     */
    private record Mapping(String key, long txId, int stripe, long sequence) {
    }
}

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

/**
 * A separate snapshot prototype of local version with two collections of verify cross-shard watermark to form the same stable cut.
 */
public final class MysqlStripedSnapshotOracle implements AutoCloseable {

    private final Map<String, DataSource> shards;

    private final List<String> routes;

    private final int stripes;

    private final int maxRetries;

    private final ExecutorService readers;

    private final StateSource stateSource;

    private final AtomicLong collectionAttempts = new AtomicLong();

    /**
     * Each read shard must have a complete split, and failure cannot guess the missing shard snapshot.
     *
     * @param shards shards
     * @param stripes stripes
     * @param maxRetries max retries
     */
    public MysqlStripedSnapshotOracle(final Map<String, DataSource> shards, final int stripes, final int maxRetries) {
        this(shards, stripes, maxRetries, false);
    }

    /**
     * Optionally, you can collect each shard in parallel to shorten the two full vector-readable competition windows.
     *
     * @param shards shards
     * @param stripes stripes
     * @param maxRetries max retries
     * @param parallelReads parallel reads
     */
    public MysqlStripedSnapshotOracle(final Map<String, DataSource> shards, final int stripes,
                                      final int maxRetries, final boolean parallelReads) {
        this(shards, stripes, maxRetries, parallelReads, StateSource.BENCH);
    }

    /**
     * The official participant aspect is read from the real `mvcc_vector_stripe_state_v1 ' and cannot be reused in probe Table.
     *
     * @param shards shards
     * @param stripes stripes
     * @param maxRetries max retries
     * @param parallelReads parallel reads
     * @return the operation result
     */
    public static MysqlStripedSnapshotOracle forProduct(final Map<String, DataSource> shards,
                                                        final int stripes, final int maxRetries,
                                                        final boolean parallelReads) {
        return new MysqlStripedSnapshotOracle(shards, stripes, maxRetries,
                parallelReads, StateSource.PRODUCT);
    }

    /**
     * All tectonic entrances share the complete layout with the wheeled sub-border validate.
     *
     * @param shards shards
     * @param stripes stripes
     * @param maxRetries max retries
     * @param parallelReads parallel reads
     * @param stateSource state source
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private MysqlStripedSnapshotOracle(final Map<String, DataSource> shards, final int stripes,
                                       final int maxRetries, final boolean parallelReads,
                                       final StateSource stateSource) {
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

    /**
     * Control decision watermark in two-wheel shard readings; fraction changes retry without returning split snapshot.
     *
     * @param centerDecisionEpoch center decision epoch
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public Snapshot capture(final LongSupplier centerDecisionEpoch) {
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

    /**
     * Returns the number of actual double-collector wheels that allows you to identify snapshot retry costs hidden under high writing pressure.
     *
     * @return the operation result
     */
    public long collectionAttempts() {
        return collectionAttempts.get();
    }

    /**
     * Only for sample by durable publisher; single-wheel source vector cannot directly serve as the client RC/ RR snapshot.
     *
     * @return the operation result
     */
    Map<String, List<Long>> sampleForPublication() {
        return readAll();
    }

    /**
     * Read version by a fixed RR snapshot; RC caller recapture for each statement.
     *
     * @param snapshot snapshot
     * @param shardId logical shard identifier
     * @param key business key
     * @param stripe stripe
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public Optional<byte[]> read(final Snapshot snapshot, final String shardId, final String key, final int stripe) {
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
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read striped MVCC prototype version failed", false, null, ex);
        }
    }

    /**
     * A single MySQL reading view merges local version, cross-shard intent and materialization version, avoiding an instant missed materialization.
     *
     * @param snapshot snapshot
     * @param shardId logical shard identifier
     * @param key business key
     * @param stripe stripe
     * @param decisions decisions
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public Optional<byte[]> readMixed(final Snapshot snapshot, final String shardId, final String key,
                                      final int stripe, final CrossDecisionResolver decisions) {
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
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read striped mixed MVCC prototype version failed", false, null, ex);
        }
    }

    /**
     * Collects all shard s in fixed routing order, using the same MySQL reading view for a single shard query.
     *
     * @return the operation result
     */
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
                } catch (final CompletionException ex) {
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

    /**
     * All subsections of an shard must be read from the same MySQL statement view atomic.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private List<Long> readOne(final String shardId) {
        List<Long> values = new ArrayList<>(stripes);
        String querySql = stateSource == StateSource.PRODUCT
                ? "select stripe_id, committed_seq from mvcc_vector_stripe_state_v1 order by stripe_id"
                : "select stripe_id, committed_seq from bench_striped_mvcc_state order by stripe_id";
        try (
                Connection connection = shards.get(shardId).getConnection();
                PreparedStatement query = connection.prepareStatement(querySql);
                ResultSet row = query.executeQuery()) {
            while (row.next()) {
                if (row.getInt("stripe_id") != values.size()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "striped snapshot has missing or repeated local stripe", false, null);
                }
                values.add(row.getLong("committed_seq"));
            }
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read striped MVCC prototype watermark failed", false, null, ex);
        }
        if (values.size() != stripes) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "striped snapshot has incomplete shard watermark", false, null);
        }
        return List.copyOf(values);
    }

    /**
     * The official cut does not allow the error to read the entry with a single key of the version table.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    private void requireBenchDataReader() {
        if (stateSource != StateSource.BENCH) {
            throw new IllegalStateException("product vector versions require MysqlStorageAdapter vector read");
        }
    }

    /**
     * Ends the parallel shard reading thread and avoids taking connection after probe exits.
     */
    @Override
    public void close() {
        if (readers != null) {
            readers.close();
        }
    }

    /**
     * Experiment watermark uses a different form than the official participant watermark, and the query statement is fixed as not accepting external watch names.
     */
    private enum StateSource {
        BENCH, PRODUCT
    }

    /**
     * Intent decision Query must return from the durable center after commit time, ACTIVE or ABORTED returns empty.
     */
    @FunctionalInterface
    public interface CrossDecisionResolver {

        /**
         * Center status unknown or unattainable must be thrown in error. Unknown intent cannot be treated as missing commit.
         *
         * @param txId transaction identifier
         * @return the operation result
         */
        OptionalLong committedEpoch(long txId);
    }

    /**
     * Fixed control decision watermark with each shard local serial number does not allow external changes to capture snapshot.
     *
     * @param decisionEpoch decision epoch
     * @param localSequences local sequences
     */
    public record Snapshot(long decisionEpoch, Map<String, List<Long>> localSequences) {

        /**
         * Every component must be an immutable, nonnegative durable commit sequence.
         *
         * @param decisionEpoch decision epoch
         * @param localSequences local sequences
         * @throws IllegalArgumentException if the operation cannot complete
         */
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

        /**
         * Returns the maximum serial number of a target shard watermark completed by durable at snapshot.
         *
         * @param shardId logical shard identifier
         * @param stripe stripe
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public long sequence(final String shardId, final int stripe) {
            List<Long> values = localSequences.get(shardId);
            if (values == null || stripe < 0 || stripe >= values.size()) {
                throw new IllegalArgumentException("striped snapshot has no such shard component");
            }
            return values.get(stripe);
        }
    }
}

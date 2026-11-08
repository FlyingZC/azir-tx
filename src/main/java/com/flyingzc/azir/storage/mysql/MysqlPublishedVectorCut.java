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

/**
 * Releases durable cross-shard vector tangent by batch, writing confirmation must wait for your own serial number to enter the quadrant.
 */
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

    /**
     * The publisher shall bind the fixed route by a set and bar count to prevent any misunderstanding of the old cut-off after failover.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     */
    MysqlPublishedVectorCut(final DataSource center, final Map<String, DataSource> shards, final int stripes,
                            final LongSupplier decisionEpoch) {
        this(center, shards, stripes, decisionEpoch, false, null, false);
    }

    /**
     * Formal shard watermark uses a stand-alone durable surface table without reading the capacity probe data.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @return the operation result
     */
    static MysqlPublishedVectorCut productDurable(final DataSource center,
                                                  final Map<String, DataSource> shards, final int stripes,
                                                  final LongSupplier decisionEpoch) {
        return new MysqlPublishedVectorCut(center, shards, stripes, decisionEpoch,
                false, null, true);
    }

    /**
     * Pure memory mode is used only for the upper limit of verify without each fsync and cannot be selected by the producer caller.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @return the operation result
     */
    static MysqlPublishedVectorCut experimentalVolatile(final DataSource center,
                                                        final Map<String, DataSource> shards, final int stripes,
                                                        final LongSupplier decisionEpoch) {
        return new MysqlPublishedVectorCut(center, shards, stripes, decisionEpoch,
                true, null, false);
    }

    /**
     * The leader mode requires durable lease first, and no longer serves the client after the old algebra expires.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @param guard guard
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static MysqlPublishedVectorCut leasedVolatile(final DataSource center,
                                                  final Map<String, DataSource> shards, final int stripes,
                                                  final LongSupplier decisionEpoch,
                                                  final MysqlVectorCutLeaderLease.LeaseGuard guard) {
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

    /**
     * The official participant hold lease Quick-Tanger is rebuilt from the real watermark table and does not write a routine for each batch.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @param guard guard
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static MysqlPublishedVectorCut leasedProduct(final DataSource center,
                                                 final Map<String, DataSource> shards, final int stripes,
                                                 final LongSupplier decisionEpoch,
                                                 final MysqlVectorCutLeaderLease.LeaseGuard guard) {
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

    /**
     * Fixed whether or not to perform clearly marked non- durable capacity experiments.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param decisionEpoch decision epoch
     * @param volatileExperiment volatile experiment
     * @param leaseGuard lease guard
     * @param productState product state
     */
    private MysqlPublishedVectorCut(final DataSource center, final Map<String, DataSource> shards, final int stripes,
                                    final LongSupplier decisionEpoch, final boolean volatileExperiment,
                                    final MysqlVectorCutLeaderLease.LeaseGuard leaseGuard,
                                    final boolean productState) {
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

    /**
     * Rebuild test forms only for visible isolation control libraries, with decision read first on the initial cut-off and then shard.
     *
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
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
        } catch (final SQLException ex) {
            throw unavailable("initialize published vector cut failed", ex);
        }
    }

    /**
     * The official control library creates only or verify tangents and is not clear about emptying previously confirmed durable vectors.
     *
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
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
        } catch (final SQLException ex) {
            throw unavailable("initialize product vector cut failed", ex);
        }
    }

    /**
     * Collapse restarts re-rehabilitation from durable shard and Centre decision; only verify single leader experiment upper bounds.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
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

    /**
     * The single-wheel first extraction centre decision and then each shard and the control library transaction release a complete monotangular dimension.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
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
            // JDBC autocommit wakes up the waiter after success; response to unknown is never confirmed in advance.
            installCache(new Cut(next, old.generation() + 1));
            return next;
        } catch (final SQLException ex) {
            throw unavailable("publish vector cut failed", ex);
        }
    }

    /**
     * RC Each statement reads the latest durable tangent; RR caller saves the first returned immutable result.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
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
        } catch (final SQLException ex) {
            throw unavailable("read published vector cut failed", ex);
        }
    }

    /**
     * Multi-process coordinator can update the local cache by batch, without each writing to a query centre.
     */
    void refresh() {
        if (volatileExperiment) {
            publishOnce();
            return;
        }
        try (Connection connection = center.getConnection()) {
            installCache(readCut(connection, false));
        } catch (final SQLException ex) {
            throw unavailable("refresh published vector cut failed", ex);
        }
    }

    /**
     * Only the average number of milliseconds for capacity probe to read a parallel shard single-time sample.
     *
     * @return the operation result
     */
    double sampleAverageMillis() {
        return samples.sum() == 0 ? Double.NaN : sampleNanos.sum() / (double) samples.sum() / 1_000_000.0;
    }

    /**
     * Only the average number of milliseconds for capacity probe reading centres durable single-line updates.
     *
     * @return the operation result
     */
    double centerWriteAverageMillis() {
        return centerWrites.sum() == 0 ? Double.NaN
                : centerNanos.sum() / (double) centerWrites.sum() / 1_000_000.0;
    }

    /**
     * Only confirm transaction after the durable cut contains the local serial number and necessary centre decision.
     *
     * @param shardId logical shard identifier
     * @param stripe stripe
     * @param sequence sequence
     * @param minimumDecisionEpoch minimum decision epoch
     * @param timeoutMillis timeout millis
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    void awaitLocallyPublished(final String shardId, final int stripe, final long sequence,
                               final long minimumDecisionEpoch, final long timeoutMillis) {
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
                } catch (final InterruptedException ex) {
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

    /**
     * Reads the centre list routine and verify fixed route layout, rejecting incompatible failure recovery data.
     *
     * @param connection connection
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Cut readCut(final Connection connection, final boolean lock) throws SQLException {
        String sql = "select layout_hash, decision_epoch, generation, vector_bytes "
                + "from " + cutTable + " where singleton_id = 1"
                + (lock ? " for update" : "");
        try (
                PreparedStatement query = connection.prepareStatement(sql);
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

    /**
     * Paths are encoded in fixed order as long integer series with no symbol semantics.
     *
     * @param values values
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private byte[] encode(final Map<String, List<Long>> values) {
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

    /**
     * Before decoding, the total length and weight of verify cannot be converted to readable snapshot.
     *
     * @param encoded encoded
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Map<String, List<Long>> decode(final byte[] encoded) throws SQLException {
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

    /**
     * Layout Hash solidifies shard identity and the fraction to durable rows, avoiding an error path of the same length.
     *
     * @param routes routes
     * @param stripes stripes
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    static String layoutFingerprint(final List<String> routes, final int stripes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(stripes).array());
            for (String route : routes) {
                byte[] name = route.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(name.length).array());
                digest.update(name);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (final NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required for published vector layout", ex);
        }
    }

    /**
     * Center durable writes successfully to awaken the commit waiter for this process.
     *
     * @param cut cut
     */
    private void installCache(final Cut cut) {
        waitLock.lock();
        try {
            cached = cut;
            changed.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    /**
     * The foreground thread that is waiting to be confirmed is awakened as soon as the lease is aborted while running.
     */
    void wakeWaiters() {
        waitLock.lock();
        try {
            changed.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    /**
     * Only lease memory publisher needs verify local conservative deadline.
     */
    private void requireLeader() {
        if (leaseGuard != null) {
            leaseGuard.requireActive();
        }
    }

    /**
     * SQL failure shuts down the release and cannot confirm to the client that writing without an durable cut is not available.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unavailable(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * Release background parallel sampling threads.
     */
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

    /**
     * Line durable saves both the centre decision, the upper bounds of each strip and the single-tune release algebra.
     *
     * @param snapshot snapshot
     * @param generation generation
     */
    private record Cut(MysqlStripedSnapshotOracle.Snapshot snapshot, long generation) {
    }
}

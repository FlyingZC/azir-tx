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
import com.flyingzc.azir.control.MysqlConnectionContext;
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.Objects;

/**
 * Use global vector lower bounds and centre for the finalisation of receipt to retire a non-visible cross-shard large payload version.
 */
final class MysqlVectorCrossGarbageCollector {

    private final MysqlStorageAdapter foreground;

    private final DataSource shard;

    private final String shardId;

    private final int stripes;

    private final MysqlVectorSnapshotLeaseStore protection;

    private final MysqlLocalFirstPublicationLedger ledger;

    private final int maxInFlight;

    private final int retireBatchSize;

    private volatile String expectedShardIdentity;

    /**
     * A visible binding of real participant, centre finalisation authority and the same layout reader protection is required.
     *
     * @param foreground foreground
     * @param shard logical shard identifier
     * @param shardId logical shard identifier
     * @param stripes stripes
     * @param protection protection
     * @param ledger ledger
     */
    MysqlVectorCrossGarbageCollector(final MysqlStorageAdapter foreground, final DataSource shard,
                                     final String shardId, final int stripes,
                                     final MysqlVectorSnapshotLeaseStore protection,
                                     final MysqlLocalFirstPublicationLedger ledger) {
        this(foreground, shard, shardId, stripes, protection, ledger, 1);
    }

    /**
     * Independent candidates can retire in parallel within the visible concurrent budget and the same-key stabilization lock continues to clash with history.
     *
     * @param foreground foreground
     * @param shard logical shard identifier
     * @param shardId logical shard identifier
     * @param stripes stripes
     * @param protection protection
     * @param ledger ledger
     * @param maxInFlight max in flight
     */
    MysqlVectorCrossGarbageCollector(final MysqlStorageAdapter foreground, final DataSource shard,
                                     final String shardId, final int stripes,
                                     final MysqlVectorSnapshotLeaseStore protection,
                                     final MysqlLocalFirstPublicationLedger ledger, final int maxInFlight) {
        this(foreground, shard, shardId, stripes, protection, ledger, maxInFlight, 1);
    }

    /**
     * An bounded shard transaction can have more than retirement keys, and delete the whole rollback when there is an error receipt and all large payload s.
     *
     * @param foreground foreground
     * @param shard logical shard identifier
     * @param shardId logical shard identifier
     * @param stripes stripes
     * @param protection protection
     * @param ledger ledger
     * @param maxInFlight max in flight
     * @param retireBatchSize retire batch size
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorCrossGarbageCollector(final MysqlStorageAdapter foreground, final DataSource shard,
                                     final String shardId, final int stripes,
                                     final MysqlVectorSnapshotLeaseStore protection,
                                     final MysqlLocalFirstPublicationLedger ledger,
                                     final int maxInFlight, final int retireBatchSize) {
        this.foreground = Objects.requireNonNull(foreground, "foreground");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        if (shardId.isBlank() || !shardId.equals(foreground.shardId())
                || stripes < 1 || stripes > 256 || maxInFlight < 1 || maxInFlight > 32
                || retireBatchSize < 1 || retireBatchSize > 32) {
            throw new IllegalArgumentException("invalid vector cross GC shard layout");
        }
        this.stripes = stripes;
        this.maxInFlight = maxInFlight;
        this.retireBatchSize = retireBatchSize;
    }

    /**
     * Scheduler read-only real background pool to check with publisher shard authority.
     *
     * @return the operation result
     */
    DataSource dataSource() {
        return shard;
    }

    /**
     * Candidate scans must be executed by the logical shard route at the time of construction.
     *
     * @return the operation result
     */
    String shardId() {
        return shardId;
    }

    /**
     * The number of pieces of the security vector cannot drift with the participant layout.
     *
     * @return the operation result
     */
    int stripeCount() {
        return stripes;
    }

    /**
     * Initialize confirmation of shard real identity and durable layouts, restarting without deleting any original receipt.
     */
    void initializeProduct() {
        requireCenterAuthority();
        requireShardAuthority();
        requireSafeFloorLayout(protection.currentSafeFloor());
        try (
                Connection connection = shard.getConnection();
                PreparedStatement layout = connection.prepareStatement("""
                        select stripe_count from mvcc_vector_layout_v1 where singleton_id = 1
                        """)) {
            String identity = MysqlVectorDatabaseIdentity.read(connection);
            try (ResultSet row = layout.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw illegal("vector cross GC stripe layout is missing or changed");
                }
            }
            MysqlVectorRetiredVersionStore.ensure(connection);
            try (PreparedStatement create = connection.prepareStatement("""
                    create table if not exists mvcc_vector_cross_gc_cursor_v1 (
                      singleton_id tinyint not null primary key,
                      after_tx_id bigint not null
                    ) engine=InnoDB
                    """)) {
                create.execute();
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_cross_gc_cursor_v1(singleton_id, after_tx_id)
                    values (1, ?) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setLong(1, Long.MIN_VALUE);
                insert.executeUpdate();
            }
            expectedShardIdentity = identity;
        } catch (final SQLException ex) {
            throw unavailable("initialize vector cross GC failed", ex);
        }
    }

    /**
     * Bounded -wheeled cross-shard map per wheel, failed to keep durable receipt and next round retry.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    int collect(final int limit) {
        if (limit < 1 || limit > 512) {
            throw new IllegalArgumentException("vector cross GC scan limit must be in 1..512");
        }
        if (expectedShardIdentity == null) {
            throw illegal("vector cross GC was not initialized");
        }
        requireCenterAuthority();
        requireShardAuthority();
        MysqlStripedSnapshotOracle.Snapshot floor = protection.currentSafeFloor();
        requireSafeFloorLayout(floor);
        List<Candidate> candidates;
        try {
            candidates = nextCandidates(limit);
        } catch (final SQLException ex) {
            throw unavailable("scan vector cross GC candidates failed", ex);
        }
        Map<Long, MysqlLocalFirstPublicationLedger.FinalizedVectorProof> proofs =
                ledger.finalizedVectorProofs(candidates.stream().map(Candidate::txId).toList());
        var centerIdentity = MysqlDatabaseAuthority.inspect(protection.centerDataSource());
        List<List<Candidate>> batches = partitionCandidates(candidates);
        if (maxInFlight == 1 || batches.size() < 2) {
            int reclaimed = 0;
            for (List<Candidate> batch : batches) {
                reclaimed += retireBatch(batch, proofs, floor, centerIdentity);
            }
            return reclaimed;
        }
        Semaphore budget = new Semaphore(maxInFlight);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Integer>> tasks = new ArrayList<>(batches.size());
            for (List<Candidate> batch : batches) {
                tasks.add(CompletableFuture.supplyAsync(() -> {
                    budget.acquireUninterruptibly();
                    try {
                        return retireBatch(batch, proofs, floor, centerIdentity);
                    } finally {
                        budget.release();
                    }
                }, workers));
            }
            int reclaimed = 0;
            RuntimeException firstFailure = null;
            for (CompletableFuture<Integer> task : tasks) {
                try {
                    reclaimed += task.join();
                } catch (final CompletionException ex) {
                    if (firstFailure == null) {
                        firstFailure = ex.getCause() instanceof RuntimeException runtime
                                ? runtime
                                : new IllegalStateException("cross GC worker failed", ex.getCause());
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
            return reclaimed;
        }
    }

    /**
     * Lined with binary key for the same batch to remove locks and reduce the probability of dying locks with business writing and other decommissioning batch.
     *
     * @param candidates candidates
     * @return the operation result
     */
    private List<List<Candidate>> partitionCandidates(final List<Candidate> candidates) {
        if (candidates.isEmpty()) {
            return List.of();
        }
        List<Candidate> ordered = new ArrayList<>(candidates);
        if (retireBatchSize > 1) {
            ordered.sort(Comparator.comparing(Candidate::key, Arrays::compareUnsigned)
                    .thenComparingLong(Candidate::txId));
        }
        List<List<Candidate>> groups = new ArrayList<>();
        for (int offset = 0; offset < ordered.size(); offset += retireBatchSize) {
            groups.add(List.copyOf(ordered.subList(offset,
                    Math.min(ordered.size(), offset + retireBatchSize))));
        }
        return List.copyOf(groups);
    }

    /**
     * Cursor is in the same commit as the candidate for shard transaction and does not always rearrange the header after restarting.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private List<Candidate> nextCandidates(final int limit) throws SQLException {
        return MysqlConnectionContext.inTransaction(shard, connection -> {
            if (!expectedShardIdentity.equals(MysqlVectorDatabaseIdentity.read(connection))) {
                throw illegal("vector cross GC shard identity changed while scanning candidates");
            }
            long after;
            try (PreparedStatement cursor = connection.prepareStatement("""
                    select after_tx_id from mvcc_vector_cross_gc_cursor_v1
                    where singleton_id = 1 for update
                    """); ResultSet row = cursor.executeQuery()) {
                if (!row.next()) {
                    throw illegal("vector cross GC candidate cursor is missing");
                }
                after = row.getLong(1);
            }
            List<Candidate> found = new ArrayList<>(limit);
            try (PreparedStatement query = connection.prepareStatement("""
                    select tx_id, user_key from mvcc_vector_committed_v1
                    where tx_id > ? order by tx_id limit ?
                    """)) {
                query.setLong(1, after);
                query.setInt(2, limit);
                try (ResultSet row = query.executeQuery()) {
                    while (row.next()) {
                        found.add(new Candidate(row.getLong(1), row.getBytes(2)));
                    }
                }
            }
            long next = found.size() < limit ? Long.MIN_VALUE : found.getLast().txId();
            try (PreparedStatement update = connection.prepareStatement("""
                    update mvcc_vector_cross_gc_cursor_v1 set after_tx_id = ?
                    where singleton_id = 1
                    """)) {
                update.setLong(1, next);
                if (update.executeUpdate() != 1) {
                    throw illegal("vector cross GC candidate cursor update lost row");
                }
            }
            return List.copyOf(found);
        });
    }

    /**
     * Single retirement will only delete cross-shard version before the safe pre-discharge; commit receipt with two large payload rows atomic replacement.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    int retireOne(final long txId) {
        if (expectedShardIdentity == null || txId == 0) {
            throw illegal("vector cross GC is uninitialized or has invalid transaction");
        }
        requireCenterAuthority();
        requireShardAuthority();
        MysqlStripedSnapshotOracle.Snapshot floor = protection.currentSafeFloor();
        requireSafeFloorLayout(floor);
        var proof = ledger.finalizedVectorProof(txId).orElse(null);
        try {
            Mapping discovered;
            try (Connection lookup = shard.getConnection()) {
                discovered = mapping(lookup, txId, false);
            }
            if (discovered == null) {
                return 0;
            }
            return retireCandidate(new Candidate(txId, discovered.key()), proof, floor,
                    MysqlDatabaseAuthority.inspect(protection.centerDataSource()));
        } catch (final SQLException ex) {
            throw unavailable("retire vector cross version failed", ex);
        }
    }

    /**
     * Batch Reuse Center certifies the security cut-off, really deletes the still-stabilized key lock and local atomic receipt.
     *
     * @param candidate candidate
     * @param proof proof
     * @param floor floor
     * @param centerIdentity center identity
     * @return the operation result
     */
    private int retireCandidate(final Candidate candidate,
                                final MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof,
                                final MysqlStripedSnapshotOracle.Snapshot floor,
                                final MysqlDatabaseAuthority.Identity centerIdentity) {
        return retireBatch(List.of(candidate), proof == null ? Map.of() : Map.of(candidate.txId(), proof),
                floor, centerIdentity);
    }

    /**
     * The group of candidates in the same key order shares shard commit once, and any anomalies make the whole group rollback.
     *
     * @param batch batch
     * @param proofs proofs
     * @param floor floor
     * @param centerIdentity center identity
     * @return the operation result
     */
    private int retireBatch(final List<Candidate> batch,
                            final Map<Long, MysqlLocalFirstPublicationLedger.FinalizedVectorProof> proofs,
                            final MysqlStripedSnapshotOracle.Snapshot floor,
                            final MysqlDatabaseAuthority.Identity centerIdentity) {
        String expected = expectedShardIdentity;
        if (expected == null) {
            return 0;
        }
        List<Candidate> eligible = new ArrayList<>(batch.size());
        for (Candidate candidate : batch) {
            var proof = proofs.get(candidate.txId());
            if (proof == null || !proof.belongsTo(ledger) || proof.txId() != candidate.txId()
                    || !proof.includes(shardId) || proof.commitTs() > floor.decisionEpoch()) {
                continue;
            }
            if (!proof.authorityIdentity().equals(centerIdentity)) {
                throw illegal("vector cross GC finalized proof came from another control database");
            }
            eligible.add(candidate);
        }
        if (eligible.isEmpty()) {
            return 0;
        }
        try {
            return MysqlConnectionContext.inTransaction(shard, connection -> {
                if (!expected.equals(MysqlVectorDatabaseIdentity.read(connection))) {
                    throw illegal("vector cross GC shard identity changed before deletion");
                }
                int reclaimed = 0;
                for (Candidate candidate : eligible) {
                    reclaimed += retireInTransaction(connection, candidate,
                            proofs.get(candidate.txId()), floor);
                }
                return reclaimed;
            });
        } catch (final SQLException ex) {
            throw unavailable("retire vector cross version failed", ex);
        }
    }

    /**
     * Each candidate still holds a stabilization key lock, participant terminal state lock and complete version validate.
     *
     * @param connection connection
     * @param candidate candidate
     * @param proof proof
     * @param floor floor
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int retireInTransaction(final Connection connection, final Candidate candidate,
                                    final MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof,
                                    final MysqlStripedSnapshotOracle.Snapshot floor) throws SQLException {
        lockKey(connection, candidate.key());
        requireParticipant(connection, proof);
        Mapping current = mapping(connection, candidate.txId(), true);
        if (current == null) {
            if (retiredReceiptExists(connection, proof)) {
                return 0;
            }
            throw illegal("vector cross GC lost mapping without retirement receipt");
        }
        if (!Arrays.equals(current.key(), candidate.key())
                || current.commitTs() != proof.commitTs()
                || current.stripe() < 0 || current.stripe() >= stripes
                || current.sequence() <= current.startSeq()) {
            throw illegal("vector cross GC mapping differs from finalized proof");
        }
        long bound = floor.sequence(shardId, current.stripe());
        if (current.sequence() > bound || hasBlockingHistory(connection, current)) {
            return 0;
        }
        Long predecessor = visiblePredecessor(connection, current, bound,
                floor.decisionEpoch());
        if (predecessor == null || predecessor <= current.sequence()) {
            return 0;
        }
        requireOnlyVersionForTransaction(connection, current);
        requireMatchingData(connection, current);
        writeReceipt(connection, current, proof.startTs());
        deleteLargeRows(connection, current);
        return 1;
    }

    /**
     * Concurrent background allows the current batch safe skip when atomic retirement is the same candidate.
     *
     * @param connection connection
     * @param proof proof
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean retiredReceiptExists(final Connection connection,
                                                final MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from mvcc_vector_retired_v1
                where tx_id = ? and start_ts = ? and commit_ts = ? for update
                """)) {
            query.setLong(1, proof.txId());
            query.setLong(2, proof.startTs());
            query.setLong(3, proof.commitTs());
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * Control lease and finalization records must come from the same real MySQL server and database.
     */
    private void requireCenterAuthority() {
        if (!ledger.databaseIdentity().equals(MysqlDatabaseAuthority.inspect(protection.centerDataSource()))) {
            throw illegal("vector cross GC center proof differs from reader safe floor authority");
        }
    }

    /**
     * Background connection shall always point to the same physical shard of the foreground adaptor.
     */
    private void requireShardAuthority() {
        if (!MysqlDatabaseAuthority.inspect(foreground.vectorDataSource())
                .equals(MysqlDatabaseAuthority.inspect(shard))) {
            throw illegal("vector cross GC background shard differs from foreground route");
        }
    }

    /**
     * The security vector of the other route or a different fraction cannot be interpreted as this shard to remove the boundary.
     *
     * @param floor floor
     */
    private void requireSafeFloorLayout(final MysqlStripedSnapshotOracle.Snapshot floor) {
        var components = floor.localSequences().get(shardId);
        if (components == null || components.size() != stripes) {
            throw illegal("vector cross GC safe floor differs from shard stripe layout");
        }
    }

    /**
     * Unlocked reading is used only for discovery keys, and must be rechecked under a stable key lock before eventually being deleted.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Mapping mapping(final Connection connection, final long txId, final boolean lock) throws SQLException {
        String sql = "select user_key, tx_id, stripe_id, start_seq, reserved_seq, commit_ts, deleted, payload "
                + "from mvcc_vector_committed_v1 where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                Mapping result = new Mapping(row.getBytes("user_key"), row.getLong("tx_id"),
                        row.getInt("stripe_id"), row.getLong("start_seq"),
                        row.getLong("reserved_seq"), row.getLong("commit_ts"),
                        row.getBoolean("deleted"), row.getBytes("payload"));
                if (row.next()) {
                    throw illegal("vector cross GC found multiple keys for one transaction");
                }
                return result;
            }
        }
    }

    /**
     * Stable key lock prevents concurrent local writing or new cross-shard prewrite to cross GC judgement.
     *
     * @param connection connection
     * @param key business key
     * @throws SQLException if the operation cannot complete
     */
    private static void lockKey(final Connection connection, final byte[] key) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("""
                insert into mvcc_key_lock(user_key) values (?)
                on duplicate key update user_key = user_key
                """)) {
            lock.setBytes(1, key);
            lock.executeUpdate();
        }
    }

    /**
     * Shard terminal state is the durable anchor for replay receipt and cannot accept missing or different local decision.
     *
     * @param connection connection
     * @param proof proof
     * @throws SQLException if the operation cannot complete
     */
    private static void requireParticipant(final Connection connection,
                                           final MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select start_ts, status, commit_ts from mvcc_participant_txn
                where tx_id = ? for update
                """)) {
            query.setLong(1, proof.txId());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong("start_ts") != proof.startTs()
                        || !"COMMITTED".equals(row.getString("status"))
                        || row.getLong("commit_ts") != proof.commitTs()) {
                    throw illegal("vector cross GC lost its committed participant terminal");
                }
            }
        }
    }

    /**
     * Active intent, rollback tags, cross-discretion and old scale lines failed to shut down the whole text.
     *
     * @param connection connection
     * @param current current
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean hasBlockingHistory(final Connection connection, final Mapping current) throws SQLException {
        byte[] key = current.key();
        int stripe = current.stripe();
        for (String table : java.util.List.of("mvcc_intent", "mvcc_vector_intent_reservation_v1",
                "mvcc_rollback")) {
            if (exists(connection, "select 1 from " + table + " where user_key = ? limit 1 for update",
                    key, null)) {
                return true;
            }
        }
        for (String table : java.util.List.of("mvcc_vector_local_data_v1",
                "mvcc_vector_committed_v1")) {
            if (exists(connection, "select 1 from " + table
                    + " where user_key = ? and stripe_id <> ? limit 1 for update", key, stripe)) {
                throw illegal("vector cross GC found a key in multiple stripes");
            }
        }
        return exists(connection, """
                select 1 from mvcc_data d where d.user_key = ?
                  and not exists(select 1 from mvcc_vector_committed_v1 c
                    where c.user_key = d.user_key and c.tx_id = d.tx_id
                      and c.commit_ts = d.commit_ts)
                limit 1 for update
                """, key, null);
    }

    /**
     * Reads the existence of the current shard lock protection, and the second parameter is used only for strip-discretion queries.
     *
     * @param connection connection
     * @param sql sql
     * @param key business key
     * @param stripe stripe
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean exists(final Connection connection, final String sql, final byte[] key, final Integer stripe) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, key);
            if (stripe != null) {
                query.setInt(2, stripe);
            }
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * Visible front-drive by the local in a secure cut with the maximum serial number of decision version already taken by the center.
     *
     * @param connection connection
     * @param current current
     * @param sequenceBound sequence bound
     * @param decisionBound decision bound
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Long visiblePredecessor(final Connection connection, final Mapping current,
                                           final long sequenceBound, final long decisionBound) throws SQLException {
        Long local = latestSequence(connection, """
                select local_seq from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id = ? and local_seq <= ?
                order by local_seq desc limit 1 for update
                """, current.key(), current.stripe(), sequenceBound, null);
        Long cross = latestSequence(connection, """
                select reserved_seq from mvcc_vector_committed_v1
                where user_key = ? and stripe_id = ? and reserved_seq <= ?
                  and commit_ts <= ?
                order by reserved_seq desc limit 1 for update
                """, current.key(), current.stripe(), sequenceBound, decisionBound);
        if (cross != null && (local == null || cross > local)) {
            Mapping successor = mappingByKeySequence(connection, current.key(), cross);
            if (successor == null || successor.stripe() != current.stripe()) {
                throw illegal("vector cross GC lost its visible successor mapping");
            }
            requireMatchingData(connection, successor);
        }
        if (local == null) {
            return cross;
        }
        return cross == null ? local : Math.max(local, cross);
    }

    /**
     * The cross-shard front drive used to mask old version must still be fully real payload.
     *
     * @param connection connection
     * @param key business key
     * @param sequence sequence
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Mapping mappingByKeySequence(final Connection connection, final byte[] key, final long sequence) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select user_key, tx_id, stripe_id, start_seq, reserved_seq,
                       commit_ts, deleted, payload
                from mvcc_vector_committed_v1
                where user_key = ? and reserved_seq = ? for update
                """)) {
            query.setBytes(1, key);
            query.setLong(2, sequence);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                return new Mapping(row.getBytes("user_key"), row.getLong("tx_id"),
                        row.getInt("stripe_id"), row.getLong("start_seq"),
                        row.getLong("reserved_seq"), row.getLong("commit_ts"),
                        row.getBoolean("deleted"), row.getBytes("payload"));
            }
        }
    }

    /**
     * The current reading takes only one line of secure forwards and avoids using the old RR view as a basis for GC judgement.
     *
     * @param connection connection
     * @param sql sql
     * @param key business key
     * @param stripe stripe
     * @param bound bound
     * @param decisionBound decision bound
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Long latestSequence(final Connection connection, final String sql, final byte[] key,
                                       final int stripe, final long bound, final Long decisionBound) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, key);
            query.setInt(2, stripe);
            query.setLong(3, bound);
            if (decisionBound != null) {
                query.setLong(4, decisionBound);
            }
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? row.getLong(1) : null;
            }
        }
    }

    /**
     * A formal cross-shard writing of only one version per piece cannot create a false replay receipt.
     *
     * @param connection connection
     * @param current current
     * @throws SQLException if the operation cannot complete
     */
    private static void requireOnlyVersionForTransaction(final Connection connection, final Mapping current) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select user_key, commit_ts from mvcc_data where tx_id = ? for update
                """)) {
            query.setLong(1, current.txId());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !Arrays.equals(row.getBytes(1), current.key())
                        || row.getLong(2) != current.commitTs() || row.next()) {
                    throw illegal("vector cross GC found a non-single-key transaction version");
                }
            }
        }
    }

    /**
     * The real version payload must match the durable map by bytes and cannot be issued for damage to history receipt.
     *
     * @param connection connection
     * @param current current
     * @throws SQLException if the operation cannot complete
     */
    private static void requireMatchingData(final Connection connection, final Mapping current) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select deleted, value from mvcc_data
                where user_key = ? and tx_id = ? and commit_ts = ? for update
                """)) {
            query.setBytes(1, current.key());
            query.setLong(2, current.txId());
            query.setLong(3, current.commitTs());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getBoolean("deleted") != current.deleted()
                        || !Arrays.equals(row.getBytes("value"), current.payload()) || row.next()) {
                    throw illegal("vector cross GC found an incomplete version payload");
                }
            }
        }
    }

    /**
     * Small receipt first writes the same transaction and then deletes two large payload, and one rollback when commit fails.
     *
     * @param connection connection
     * @param current current
     * @param startTs transaction start timestamp
     * @throws SQLException if the operation cannot complete
     */
    private static void writeReceipt(final Connection connection, final Mapping current, final long startTs) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_vector_retired_v1
                  (tx_id, user_key, stripe_id, start_ts, start_seq, reserved_seq,
                   commit_ts, deleted, payload_sha)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            insert.setLong(1, current.txId());
            insert.setBytes(2, current.key());
            insert.setInt(3, current.stripe());
            insert.setLong(4, startTs);
            insert.setLong(5, current.startSeq());
            insert.setLong(6, current.sequence());
            insert.setLong(7, current.commitTs());
            insert.setBoolean(8, current.deleted());
            insert.setBytes(9, MysqlVectorRetiredVersionStore.payloadDigest(
                    current.deleted(), current.payload()));
            if (insert.executeUpdate() != 1) {
                throw illegal("vector cross GC failed to persist retirement receipt");
            }
        }
    }

    /**
     * Receipt with Large payload Delete completed with transaction, and none of the rows does not match rollback.
     *
     * @param connection connection
     * @param current current
     * @throws SQLException if the operation cannot complete
     */
    private static void deleteLargeRows(final Connection connection, final Mapping current) throws SQLException {
        try (PreparedStatement data = connection.prepareStatement("""
                delete from mvcc_data where user_key = ? and tx_id = ? and commit_ts = ?
                """); PreparedStatement mapping = connection.prepareStatement("""
                delete from mvcc_vector_committed_v1
                where user_key = ? and tx_id = ? and reserved_seq = ? and commit_ts = ?
                """)) {
            data.setBytes(1, current.key());
            data.setLong(2, current.txId());
            data.setLong(3, current.commitTs());
            mapping.setBytes(1, current.key());
            mapping.setLong(2, current.txId());
            mapping.setLong(3, current.sequence());
            mapping.setLong(4, current.commitTs());
            if (data.executeUpdate() != 1 || mapping.executeUpdate() != 1) {
                throw illegal("vector cross GC lost an atomically retired large row");
            }
        }
    }

    /**
     * Durable map or database identity mismatch must stop deleting.
     *
     * @param message message
     * @return the operation result
     */
    private static MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * Connection or transaction may be commit receipt after an anomaly, and the caller must confirm it with durable status retry.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unavailable(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, message, true, null, cause);
    }

    /**
     * Single key cross-shard map identity read again after the key lock and the participant lock.
     *
     * @param key business key
     * @param txId transaction identifier
     * @param stripe stripe
     * @param startSeq start seq
     * @param sequence sequence
     * @param commitTs durable commit timestamp
     * @param deleted deleted
     * @param payload payload
     */
    private record Mapping(byte[] key, long txId, int stripe, long startSeq, long sequence,
            long commitTs, boolean deleted, byte[] payload) {
    }

    /**
     * Shard cursor carries the Stable Business Key in a query and rechecks the real map under key lock until retirement.
     *
     * @param txId transaction identifier
     * @param key business key
     */
    private record Candidate(long txId, byte[] key) {
    }
}

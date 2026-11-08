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
import com.flyingzc.azir.common.TxStatus;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Primary shard decision vector and participant local serial numbers determine the cross-shard version Visibility Separator Prototype.
 */
final class MysqlPrimaryVectorReadPrototype implements AutoCloseable {

    private final String primaryShard;

    private final String secondaryShard;

    private final Map<String, DataSource> shards;

    private final MysqlPrimaryDecisionPrototype decisions;

    private final MysqlStripedSnapshotOracle dataOracle;

    /**
     * After both shards finish prewriting, the primary shard is the sole authority for the terminal decision.
     *
     * @param primaryShard primary shard
     * @param primary original Percolator primary identity
     * @param secondaryShard secondary shard
     * @param secondary secondary
     * @param stripes stripes
     * @param decisions decisions
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlPrimaryVectorReadPrototype(final String primaryShard, final DataSource primary,
                                    final String secondaryShard, final DataSource secondary,
                                    final int stripes, final MysqlPrimaryDecisionPrototype decisions) {
        if (primaryShard == null || primaryShard.isBlank() || secondaryShard == null
                || secondaryShard.isBlank() || primaryShard.equals(secondaryShard)) {
            throw new IllegalArgumentException("invalid primary and secondary routes");
        }
        this.primaryShard = primaryShard;
        this.secondaryShard = secondaryShard;
        this.shards = Map.of(primaryShard, Objects.requireNonNull(primary, "primary"),
                secondaryShard, Objects.requireNonNull(secondary, "secondary"));
        this.decisions = Objects.requireNonNull(decisions, "decisions");
        this.dataOracle = new MysqlStripedSnapshotOracle(shards, stripes, 1);
    }

    /**
     * First master decision and then take all the data bars. The commit already taken must have a serial number covering two pieces of intent.
     *
     * @return the operation result
     */
    Snapshot captureUnpublished() {
        List<Long> decision = decisions.decisionVector();
        Map<String, List<Long>> data = dataOracle.sampleForPublication();
        return new Snapshot(decision, data);
    }

    /**
     * Old cutters filter local serial numbers first, then intent and materialization version with primary shard decision.
     *
     * @param snapshot snapshot
     * @param shard logical shard identifier
     * @param key business key
     * @param stripe stripe
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    Optional<byte[]> read(final Snapshot snapshot, final String shard, final String key, final int stripe) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(key, "key");
        DataSource dataSource = shards.get(shard);
        if (dataSource == null) {
            throw new IllegalArgumentException("unknown primary vector read route");
        }
        long dataBound = snapshot.dataSequence(shard, stripe);
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
            byte[] encoded = key.getBytes(StandardCharsets.UTF_8);
            for (int group = 0; group < 3; group++) {
                int offset = group * 3;
                query.setBytes(offset + 1, encoded);
                query.setInt(offset + 2, stripe);
                query.setLong(offset + 3, dataBound);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getInt("origin") == 0) {
                        return Optional.of(rows.getBytes("payload"));
                    }
                    long txId = rows.getLong("tx_id");
                    long reservedSequence = rows.getLong("version_seq");
                    var decision = decisions.participantDecision(txId,
                            shard.equals(primaryShard), key, stripe, reservedSequence);
                    if (decision.status() != TxStatus.COMMITTED) {
                        continue;
                    }
                    if (rows.getInt("origin") == 1
                            && rows.getLong("decision_epoch") != decision.decisionSequence()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "materialized version has a different primary decision", false, txId);
                    }
                    if (decision.decisionSequence() <= snapshot.decisionSequence(decision.decisionStripe())) {
                        return Optional.of(rows.getBytes("payload"));
                    }
                }
                return Optional.empty();
            }
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read primary vector prototype version failed", true, null, ex);
        }
    }

    /**
     * Closes the reading thread resources that are used only for data vector collection.
     */
    @Override
    public void close() {
        dataOracle.close();
    }

    /**
     * Fixed main shard decision vector and data vectors, RR can hold the old example unchanged.
     *
     * @param decisionSequences decision sequences
     * @param dataSequences data sequences
     */
    record Snapshot(List<Long> decisionSequences, Map<String, List<Long>> dataSequences) {

        /**
         * The client cannot change the decision or data mass already captured.
         *
         * @param decisionSequences decision sequences
         * @param dataSequences data sequences
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Snapshot {
            decisionSequences = List.copyOf(decisionSequences);
            dataSequences = Map.copyOf(dataSequences);
            if (decisionSequences.isEmpty() || dataSequences.isEmpty()
                    || decisionSequences.stream().anyMatch(value -> value == null || value < 0)) {
                throw new IllegalArgumentException("invalid primary vector snapshot");
            }
        }

        /**
         * Returns the maximum serial number of the target primary shard decision that has been issued on this side.
         *
         * @param stripe stripe
         * @return the operation result
         */
        long decisionSequence(final int stripe) {
            return decisionSequences.get(stripe);
        }

        /**
         * Returns the maximum serial number of the local data bar that has been completed by durable on this side.
         *
         * @param shard logical shard identifier
         * @param stripe stripe
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        long dataSequence(final String shard, final int stripe) {
            List<Long> values = dataSequences.get(shard);
            if (values == null || stripe < 0 || stripe >= values.size()) {
                throw new IllegalArgumentException("snapshot has no target data stripe");
            }
            return values.get(stripe);
        }
    }
}

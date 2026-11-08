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
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.ScanPage;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Merge version vector with intent in a real shard RR reading view.
 */
final class MysqlProductVectorSnapshotReader {

    private static final int MAX_INTERNAL_PAGE = 128;

    /**
     * Only adaptor shard connection can be called when the adapter holds the product.
     */
    private MysqlProductVectorSnapshotReader() {
    }

    /**
     * Version screening by local serial number has been published, and cross-shard visibility is judged by centre decision time.
     *
     * @param connection connection
     * @param snapshot snapshot
     * @param shardId logical shard identifier
     * @param key business key
     * @param stripe stripe
     * @param requesterTxId requester tx id
     * @param metadata metadata
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static Optional<VersionedValue> get(final Connection connection,
                                        final MysqlStripedSnapshotOracle.Snapshot snapshot,
                                        final String shardId, final String key, final int stripe, final long requesterTxId,
                                        final TransactionMetadataStore metadata) throws SQLException {
        long bound = snapshot.sequence(shardId, stripe);
        byte[] encodedKey = Utf8KeyOrder.encode(key);
        rejectUnmappedScalarRows(connection, encodedKey, stripe, requesterTxId);
        try (PreparedStatement own = connection.prepareStatement("""
                select i.start_ts, i.deleted, i.value
                from mvcc_intent i
                join mvcc_vector_intent_reservation_v1 r
                  on r.user_key = i.user_key and r.tx_id = i.tx_id
                where i.user_key = ? and i.tx_id = ?
                """)) {
            own.setBytes(1, encodedKey);
            own.setLong(2, requesterTxId);
            try (ResultSet row = own.executeQuery()) {
                if (row.next()) {
                    return Optional.of(new VersionedValue(key, row.getBytes("value"),
                            row.getBoolean("deleted"), row.getLong("start_ts"), true));
                }
            }
        }
        try (PreparedStatement query = connection.prepareStatement("""
                select c.reserved_seq as local_seq, c.commit_ts as decision_ts,
                       d.deleted, d.value, 0 as origin, c.tx_id,
                       0 as intent_start, d.tx_id is null as missing_version
                from mvcc_vector_committed_v1 c
                left join mvcc_data d on d.user_key = c.user_key
                  and d.tx_id = c.tx_id and d.commit_ts = c.commit_ts
                where c.user_key = ? and c.stripe_id = ? and c.reserved_seq <= ?
                union all
                select r.reserved_seq as local_seq, 0 as decision_ts,
                       i.deleted, i.value, 1 as origin, r.tx_id,
                       i.start_ts as intent_start, i.tx_id is null as missing_version
                from mvcc_vector_intent_reservation_v1 r
                left join mvcc_intent i on i.user_key = r.user_key and i.tx_id = r.tx_id
                where r.user_key = ? and r.stripe_id = ? and r.reserved_seq <= ?
                union all
                select l.local_seq, l.local_seq as decision_ts,
                       l.deleted, l.payload as value, 2 as origin, l.tx_id,
                       0 as intent_start, 0 as missing_version
                from mvcc_vector_local_data_v1 l
                where l.user_key = ? and l.stripe_id = ? and l.local_seq <= ?
                order by local_seq desc
                """)) {
            for (int group = 0; group < 3; group++) {
                int offset = group * 3;
                query.setBytes(offset + 1, encodedKey);
                query.setInt(offset + 2, stripe);
                query.setLong(offset + 3, bound);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getBoolean("missing_version")) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector mapping lost its product intent or committed version", false, requesterTxId);
                    }
                    long visibleTs;
                    int origin = rows.getInt("origin");
                    if (origin == 2) {
                        // The striped upper bound proves visibility of local 1PC without relying on the central decision timestamp.
                        return Optional.of(new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), rows.getLong("local_seq"), false));
                    }
                    if (origin == 0) {
                        visibleTs = rows.getLong("decision_ts");
                    } else {
                        long intentTxId = rows.getLong("tx_id");
                        TxRecord decision = metadata.get(intentTxId)
                                .orElseThrow(() -> new MysqlSnapshotReader.MissingDecisionException(intentTxId));
                        if (decision.status() != TxStatus.COMMITTED || decision.commitTs() == null
                                || decision.startTs() != rows.getLong("intent_start")) {
                            continue;
                        }
                        visibleTs = decision.commitTs();
                    }
                    if (visibleTs <= snapshot.decisionEpoch()) {
                        return Optional.of(new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), visibleTs, false));
                    }
                }
                return Optional.empty();
            }
        }
    }

    /**
     * All keys in one page use the same shard RR view and the same global vector, cursor from the original key.
     *
     * @param connection connection
     * @param snapshot snapshot
     * @param shardId logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @param requesterTxId requester tx id
     * @param metadata metadata
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static ScanPage page(final Connection connection, final MysqlStripedSnapshotOracle.Snapshot snapshot,
                         final String shardId, final KeyRange range, final String afterKeyExclusive, final int limit,
                         final long requesterTxId, final TransactionMetadataStore metadata) throws SQLException {
        int pageLimit = Math.min(limit, MAX_INTERNAL_PAGE);
        List<String> candidates = candidateKeys(connection, range, afterKeyExclusive, pageLimit + 1);
        boolean more = candidates.size() > pageLimit;
        List<String> keys = more ? candidates.subList(0, pageLimit) : candidates;
        Map<String, Integer> stripes = keyStripes(connection, keys, requesterTxId);
        rejectUnmappedScalarRows(connection, keys, requesterTxId);
        Map<String, VersionedValue> visible = visiblePageValues(connection, snapshot,
                shardId, keys, stripes, requesterTxId, metadata);
        List<VersionedValue> values = new ArrayList<>(keys.size());
        for (String key : keys) {
            VersionedValue value = visible.get(key);
            if (value != null && !value.deleted()) {
                values.add(value);
            }
        }
        return new ScanPage(values, more ? keys.getLast() : null);
    }

    /**
     * Five real key sources were involved in the selection and it was found that the old target number of orphans could not be lost silently in a range scan.
     *
     * @param connection connection
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static List<String> candidateKeys(final Connection connection, final KeyRange range,
                                              final String afterKeyExclusive, final int limit) throws SQLException {
        List<byte[]> bounds = new ArrayList<>();
        StringBuilder predicate = new StringBuilder(" where 1 = 1");
        if (range.startKeyInclusive() != null) {
            predicate.append(" and user_key >= ?");
            bounds.add(Utf8KeyOrder.encode(range.startKeyInclusive()));
        }
        if (range.endKeyExclusive() != null) {
            predicate.append(" and user_key < ?");
            bounds.add(Utf8KeyOrder.encode(range.endKeyExclusive()));
        }
        if (afterKeyExclusive != null) {
            predicate.append(" and user_key > ?");
            bounds.add(Utf8KeyOrder.encode(afterKeyExclusive));
        }
        List<String> sources = List.of("mvcc_vector_local_data_v1", "mvcc_vector_committed_v1",
                "mvcc_vector_intent_reservation_v1", "mvcc_data", "mvcc_intent");
        StringBuilder sql = new StringBuilder("select user_key from (");
        for (int index = 0; index < sources.size(); index++) {
            if (index != 0) {
                sql.append(" union ");
            }
            sql.append("(select distinct user_key from ").append(sources.get(index))
                    .append(predicate).append(" order by user_key limit ?)");
        }
        sql.append(") page_keys order by user_key limit ?");
        try (PreparedStatement query = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            for (int ignored = 0; ignored < sources.size(); ignored++) {
                for (byte[] bound : bounds) {
                    query.setBytes(parameter++, bound);
                }
                query.setInt(parameter++, limit);
            }
            query.setInt(parameter, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (rows.next()) {
                    byte[] bytes = rows.getBytes(1);
                    String key = new String(bytes, StandardCharsets.UTF_8);
                    if (!java.util.Arrays.equals(bytes, Utf8KeyOrder.encode(key))) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector scan found malformed UTF-8 key", false, null);
                    }
                    keys.add(key);
                }
                return keys;
            }
        }
    }

    /**
     * Three vector rows determine the only physical subsection for each candidate in a query.
     *
     * @param connection connection
     * @param keys keys
     * @param requesterTxId requester tx id
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static Map<String, Integer> keyStripes(final Connection connection, final List<String> keys,
                                                   final long requesterTxId) throws SQLException {
        Map<String, Integer> stripes = new HashMap<>();
        if (keys.isEmpty()) {
            return stripes;
        }
        String in = placeholders(keys.size());
        String sql = "select user_key, stripe_id from mvcc_vector_local_data_v1 where user_key in (" + in
                + ") union select user_key, stripe_id from mvcc_vector_committed_v1 where user_key in (" + in
                + ") union select user_key, stripe_id from mvcc_vector_intent_reservation_v1 where user_key in ("
                + in + ")";
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (int source = 0; source < 3; source++) {
                parameter = bindKeys(query, keys, parameter);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String key = decodeKey(rows.getBytes("user_key"), requesterTxId);
                    int stripe = rows.getInt("stripe_id");
                    Integer previous = stripes.putIfAbsent(key, stripe);
                    if (previous != null && previous != stripe) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector scan found a key in multiple stripes", false, requesterTxId);
                    }
                }
            }
        }
        if (stripes.size() != keys.size()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector scan found a key without vector ordering proof", false, requesterTxId);
        }
        return stripes;
    }

    /**
     * The history of mixed agreements cannot be ignored because vector lines exist.
     *
     * @param connection connection
     * @param keys keys
     * @param requesterTxId requester tx id
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static void rejectUnmappedScalarRows(final Connection connection, final List<String> keys,
                                                 final long requesterTxId) throws SQLException {
        if (keys.isEmpty()) {
            return;
        }
        String in = placeholders(keys.size());
        String sql = """
                select 1 from mvcc_data d where d.user_key in (%s)
                  and not exists(select 1 from mvcc_vector_committed_v1 v
                    where v.user_key = d.user_key and v.tx_id = d.tx_id and v.commit_ts = d.commit_ts)
                union all
                select 1 from mvcc_intent i where i.user_key in (%s)
                  and not exists(select 1 from mvcc_vector_intent_reservation_v1 v
                    where v.user_key = i.user_key and v.tx_id = i.tx_id)
                limit 1
                """.formatted(in, in);
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = bindKeys(query, keys, 1);
            bindKeys(query, keys, parameter);
            try (ResultSet row = query.executeQuery()) {
                if (row.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector scan found an unmapped scalar version or intent", false, requesterTxId);
                }
            }
        }
    }

    /**
     * The split upper bounds and candidate keys are passed to database at a time, and the first visible entry is selected after combining three types of version with some reading rule.
     *
     * @param connection connection
     * @param snapshot snapshot
     * @param shardId logical shard identifier
     * @param keys keys
     * @param stripes stripes
     * @param requesterTxId requester tx id
     * @param metadata metadata
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static Map<String, VersionedValue> visiblePageValues(final Connection connection,
                                                                 final MysqlStripedSnapshotOracle.Snapshot snapshot, final String shardId, final List<String> keys,
                                                                 final Map<String, Integer> stripes, final long requesterTxId,
                                                                 final TransactionMetadataStore metadata) throws SQLException {
        Map<String, VersionedValue> visible = new HashMap<>();
        if (keys.isEmpty()) {
            return visible;
        }
        String keyRows = IntStream.range(0, keys.size())
                .mapToObj(index -> "select ? as user_key, ? as stripe_id, ? as bound")
                .collect(Collectors.joining(" union all "));
        String sql = """
                with scan_keys as (%s)
                select user_key, local_seq, decision_ts, deleted, value, origin, tx_id,
                       intent_start, missing_version
                from (
                  select c.user_key, c.reserved_seq as local_seq, c.commit_ts as decision_ts,
                         d.deleted, d.value, 0 as origin, c.tx_id,
                         0 as intent_start, d.tx_id is null as missing_version
                  from mvcc_vector_committed_v1 c
                  join scan_keys k on k.user_key = c.user_key and k.stripe_id = c.stripe_id
                    and c.reserved_seq <= k.bound
                  left join mvcc_data d on d.user_key = c.user_key
                    and d.tx_id = c.tx_id and d.commit_ts = c.commit_ts
                  union all
                  select r.user_key, r.reserved_seq as local_seq, 0 as decision_ts,
                         i.deleted, i.value, 1 as origin, r.tx_id,
                         i.start_ts as intent_start, i.tx_id is null as missing_version
                  from mvcc_vector_intent_reservation_v1 r
                  join scan_keys k on k.user_key = r.user_key and k.stripe_id = r.stripe_id
                    and (r.reserved_seq <= k.bound or r.tx_id = ?)
                  left join mvcc_intent i on i.user_key = r.user_key and i.tx_id = r.tx_id
                  union all
                  select l.user_key, l.local_seq, l.local_seq as decision_ts,
                         l.deleted, l.payload as value, 2 as origin, l.tx_id,
                         0 as intent_start, 0 as missing_version
                  from mvcc_vector_local_data_v1 l
                  join scan_keys k on k.user_key = l.user_key and k.stripe_id = l.stripe_id
                    and l.local_seq <= k.bound
                ) candidates
                order by user_key, case when origin = 1 and tx_id = ? then 1 else 0 end desc,
                         local_seq desc
                """.formatted(keyRows);
        Map<Long, TxRecord> decisions = new HashMap<>();
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (String key : keys) {
                int stripe = stripes.get(key);
                query.setBytes(parameter++, Utf8KeyOrder.encode(key));
                query.setInt(parameter++, stripe);
                query.setLong(parameter++, snapshot.sequence(shardId, stripe));
            }
            query.setLong(parameter++, requesterTxId);
            query.setLong(parameter, requesterTxId);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String key = decodeKey(rows.getBytes("user_key"), requesterTxId);
                    if (visible.containsKey(key)) {
                        continue;
                    }
                    if (rows.getBoolean("missing_version")) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector mapping lost its product intent or committed version", false, requesterTxId);
                    }
                    int origin = rows.getInt("origin");
                    long sequence = rows.getLong("local_seq");
                    if (origin == 1 && rows.getLong("tx_id") == requesterTxId) {
                        visible.put(key, new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), rows.getLong("intent_start"), true));
                        continue;
                    }
                    if (origin == 2) {
                        visible.put(key, new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), sequence, false));
                        continue;
                    }
                    long visibleTs;
                    if (origin == 0) {
                        visibleTs = rows.getLong("decision_ts");
                    } else {
                        long txId = rows.getLong("tx_id");
                        TxRecord decision = decisions.computeIfAbsent(txId, ignored -> metadata.get(txId)
                                .orElseThrow(() -> new MysqlSnapshotReader.MissingDecisionException(txId)));
                        if (decision.status() != TxStatus.COMMITTED || decision.commitTs() == null
                                || decision.startTs() != rows.getLong("intent_start")) {
                            continue;
                        }
                        visibleTs = decision.commitTs();
                    }
                    if (visibleTs <= snapshot.decisionEpoch()) {
                        visible.put(key, new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), visibleTs, false));
                    }
                }
            }
        }
        return visible;
    }

    /**
     * Ties the batch binary key and returns the next parameter position.
     *
     * @param query query
     * @param keys keys
     * @param parameter parameter
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static int bindKeys(final PreparedStatement query, final List<String> keys, int parameter) throws SQLException {
        for (String key : keys) {
            query.setBytes(parameter++, Utf8KeyOrder.encode(key));
        }
        return parameter;
    }

    /**
     * The list of parameters per page is generated only by the number of bounded candidates.
     *
     * @param count count
     * @return the operation result
     */
    private static String placeholders(final int count) {
        return IntStream.range(0, count).mapToObj(ignored -> "?").collect(Collectors.joining(", "));
    }

    /**
     * Database keys shall be able to move to and from UTF-8 without loss, preventing Java string aliases.
     *
     * @param bytes bytes
     * @param requesterTxId requester tx id
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private static String decodeKey(final byte[] bytes, final long requesterTxId) {
        String key = new String(bytes, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(bytes, Utf8KeyOrder.encode(key))) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector scan found malformed UTF-8 key", false, requesterTxId);
        }
        return key;
    }

    /**
     * Disables guessing cross-arranged order when mixing old version or intent without vector map.
     *
     * @param connection connection
     * @param key business key
     * @param stripe stripe
     * @param requesterTxId requester tx id
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static void rejectUnmappedScalarRows(final Connection connection, final byte[] key,
                                                 final int stripe, final long requesterTxId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select
                  exists(select 1 from mvcc_data d where d.user_key = ?
                    and not exists(select 1 from mvcc_vector_committed_v1 v
                                   where v.user_key = d.user_key and v.tx_id = d.tx_id
                                     and v.commit_ts = d.commit_ts)) as scalar_data,
                  exists(select 1 from mvcc_intent i where i.user_key = ?
                    and not exists(select 1 from mvcc_vector_intent_reservation_v1 v
                                   where v.user_key = i.user_key and v.tx_id = i.tx_id)) as scalar_intent,
                  exists(select 1 from mvcc_vector_committed_v1 v
                         where v.user_key = ? and v.stripe_id <> ?) as wrong_committed_stripe,
                  exists(select 1 from mvcc_vector_intent_reservation_v1 v
                         where v.user_key = ? and v.stripe_id <> ?) as wrong_intent_stripe,
                  exists(select 1 from mvcc_vector_local_data_v1 v
                         where v.user_key = ? and v.stripe_id <> ?) as wrong_local_stripe
                """)) {
            query.setBytes(1, key);
            query.setBytes(2, key);
            query.setBytes(3, key);
            query.setInt(4, stripe);
            query.setBytes(5, key);
            query.setInt(6, stripe);
            query.setBytes(7, key);
            query.setInt(8, stripe);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getBoolean("scalar_data") || row.getBoolean("scalar_intent")
                        || row.getBoolean("wrong_committed_stripe")
                        || row.getBoolean("wrong_intent_stripe")
                        || row.getBoolean("wrong_local_stripe")) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector read found a version without vector ordering proof", false, requesterTxId);
                }
            }
        }
    }
}

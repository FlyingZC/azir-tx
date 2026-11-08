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

import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.storage.VersionedValue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Batch reads version and intent in the same local snapshot held by the caller, without holding a cross-page JDBC cursor.
 */
final class MysqlSnapshotReader {

    /**
     * Stops the exemplifier snapshot reading tool.
     */
    private MysqlSnapshotReader() {
    }

    /**
     * Read the original bounded key page and superimpose a copy of transaction written and other transaction intent published.
     *
     * @param connection connection
     * @param context context
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @param metadata metadata
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static ScanPage page(final Connection connection, final ReadContext context, final KeyRange range,
                         final String afterKeyExclusive, final int limit, final TransactionMetadataStore metadata) throws SQLException {
        List<String> collected = collectKeys(connection, range, afterKeyExclusive, limit + 1);
        boolean hasMore = collected.size() > limit;
        List<String> keys = hasMore ? collected.subList(0, limit) : collected;
        Map<String, VersionedValue> visible = visibleValues(connection, context, keys, metadata);
        List<VersionedValue> values = new ArrayList<>(keys.size());
        for (String key : keys) {
            VersionedValue value = visible.get(key);
            if (value != null && !value.deleted()) {
                values.add(value);
            }
        }
        // Cursor must come from a scanned original key and delete or not see intent does not make the page break stagnate.
        return new ScanPage(values, hasMore ? keys.getLast() : null);
    }

    /**
     * Read the latest visible version and all intent keys with two batch queries.
     *
     * @param connection connection
     * @param context context
     * @param keys keys
     * @param metadata metadata
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static Map<String, VersionedValue> visibleValues(final Connection connection, final ReadContext context,
                                                     final List<String> keys, final TransactionMetadataStore metadata) throws SQLException {
        Map<String, VersionedValue> values = new HashMap<>();
        if (keys.isEmpty()) {
            return values;
        }
        String versionSql = """
                select d.user_key, d.commit_ts, d.deleted, d.value
                from mvcc_data d
                join (select user_key, max(commit_ts) as commit_ts from mvcc_data
                      where user_key in (%s) and commit_ts <= ? group by user_key) latest
                  on latest.user_key = d.user_key and latest.commit_ts = d.commit_ts
                """.formatted(placeholders(keys.size()));
        try (PreparedStatement statement = connection.prepareStatement(versionSql)) {
            bindKeys(statement, keys);
            statement.setLong(keys.size() + 1, context.readTs());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String key = decode(rows.getBytes("user_key"));
                    values.put(key, new VersionedValue(key, rows.getBytes("value"), rows.getBoolean("deleted"),
                            rows.getLong("commit_ts"), false));
                }
            }
        }
        List<ReadIntent> intents = new ArrayList<>();
        Set<Long> foreignTransactions = new LinkedHashSet<>();
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select user_key, tx_id, start_ts, deleted, value from mvcc_intent where user_key in ("
                                + placeholders(keys.size()) + ")")) {
            bindKeys(statement, keys);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    ReadIntent intent = new ReadIntent(decode(rows.getBytes("user_key")), rows.getLong("tx_id"),
                            rows.getLong("start_ts"), rows.getBoolean("deleted"), rows.getBytes("value"));
                    intents.add(intent);
                    if (intent.txId() != context.txId()) {
                        foreignTransactions.add(intent.txId());
                    }
                }
            }
        }
        // Each page is resolved, not terminal state without cache; the next page allows for a new view of the newly released decision.
        Map<Long, TxRecord> terminal = new HashMap<>();
        if (metadata != null) {
            for (long txId : foreignTransactions) {
                // The old RR view may still see materialization, but the center decision has already been GC; failure cannot be judged as missing commit.
                TxRecord record = metadata.get(txId).orElseThrow(() -> new MissingDecisionException(txId));
                if (record.status() == TxStatus.COMMITTED || record.status() == TxStatus.ABORTED) {
                    terminal.put(txId, record);
                }
            }
        }
        for (ReadIntent intent : intents) {
            if (intent.txId() == context.txId()) {
                values.put(intent.key(), new VersionedValue(intent.key(), intent.value(), intent.deleted(), intent.startTs(), true));
                continue;
            }
            TxRecord decision = terminal.get(intent.txId());
            if (decision == null || decision.status() != TxStatus.COMMITTED || decision.commitTs() == null
                    || decision.startTs() != intent.startTs() || decision.commitTs() > context.readTs()) {
                continue;
            }
            VersionedValue current = values.get(intent.key());
            if (current == null || current.versionTs() < decision.commitTs()) {
                values.put(intent.key(), new VersionedValue(intent.key(), intent.value(), intent.deleted(), decision.commitTs(), false));
            }
        }
        return values;
    }

    /**
     * Limit each source to a one-page candidate key, avoiding UNION loading the full range of keys in front of the page break.
     *
     * @param connection connection
     * @param range range
     * @param after after
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static List<String> collectKeys(final Connection connection, final KeyRange range, final String after, final int limit) throws SQLException {
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
        if (after != null) {
            predicate.append(" and user_key > ?");
            bounds.add(Utf8KeyOrder.encode(after));
        }
        String sql = "select user_key from ((select distinct user_key from mvcc_data" + predicate
                + " order by user_key limit ?) union (select user_key from mvcc_intent" + predicate
                + " order by user_key limit ?)) page_keys order by user_key limit ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (int source = 0; source < 2; source++) {
                for (byte[] bound : bounds) {
                    statement.setBytes(parameter++, bound);
                }
                statement.setInt(parameter++, limit);
            }
            statement.setInt(parameter, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (rows.next()) {
                    keys.add(decode(rows.getBytes("user_key")));
                }
                return keys;
            }
        }
    }

    /**
     * Ties a page of UTF-8 binary key parameters.
     *
     * @param statement statement
     * @param keys keys
     * @throws SQLException if the operation cannot complete
     */
    private static void bindKeys(final PreparedStatement statement, final List<String> keys) throws SQLException {
        for (int index = 0; index < keys.size(); index++) {
            statement.setBytes(index + 1, Utf8KeyOrder.encode(keys.get(index)));
        }
    }

    /**
     * Creates a list of bounded parameters, and the key always passes through JDBC parameters.
     *
     * @param count count
     * @return the operation result
     */
    private static String placeholders(final int count) {
        return IntStream.range(0, count).mapToObj(ignored -> "?").collect(Collectors.joining(", "));
    }

    /**
     * Decoding UTF-8 business keys in database that pass validate.
     *
     * @param key business key
     * @return the operation result
     */
    private static String decode(final byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    /**
     * Notify caller to close the old local snapshot and read it again, without leaking or zooming in to the upper layer.
     */
    static final class MissingDecisionException extends RuntimeException {

        /**
         * Retain transaction identifier of missing centre decision, report undecidable intent when rereading failed.
         *
         * @param txId transaction identifier
         */
        MissingDecisionException(final long txId) {
            super("metadata decision missing for intent transaction " + txId);
        }
    }

    /**
     * The intent value of the current page is retained only during this reading.
     *
     * @param key business key
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deleted deleted
     * @param value value to write
     */
    private record ReadIntent(String key, long txId, long startTs, boolean deleted, byte[] value) {
    }
}

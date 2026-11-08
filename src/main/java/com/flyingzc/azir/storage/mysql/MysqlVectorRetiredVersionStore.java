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
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.Utf8KeyOrder;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;

/**
 * Cross-shard old version retains durable replay receipt without business payload after decommissioning.
 */
final class MysqlVectorRetiredVersionStore {

    /**
     * Allows only the initialisation of the official vector layout to create a compact receipt table without modifying an existing commit version.
     */
    private MysqlVectorRetiredVersionStore() {
    }

    /**
     * Receipt identity binding transaction, keys, original retention numbers and payload digests, primary key preventing transaction ID reuse.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void ensure(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_vector_retired_v1 (
                      tx_id bigint not null primary key,
                      user_key varbinary(256) not null,
                      stripe_id smallint not null,
                      start_ts bigint not null,
                      start_seq bigint not null,
                      reserved_seq bigint not null,
                      commit_ts bigint not null,
                      deleted tinyint not null,
                      payload_sha binary(32) not null,
                      unique key mvcc_vector_retired_key_seq (user_key, reserved_seq)
                    ) engine=InnoDB
                    """);
        }
    }

    /**
     * The old materialization call is considered successful only when the real terminal state matches the full of decommissioned receipt and large payload rows have disappeared.
     *
     * @param dataSource database connection source
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static boolean matches(final DataSource dataSource, final long txId, final long startTs, final long commitTs) throws SQLException {
        Objects.requireNonNull(dataSource, "dataSource");
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement("""
                        select r.tx_id from mvcc_vector_retired_v1 r
                        join mvcc_participant_txn p on p.tx_id = r.tx_id
                        where r.tx_id = ? and r.start_ts = ? and r.commit_ts = ?
                          and p.start_ts = r.start_ts and p.commit_ts = r.commit_ts
                          and p.status = 'COMMITTED'
                          and not exists(select 1 from mvcc_vector_committed_v1 c
                                         where c.tx_id = r.tx_id)
                          and not exists(select 1 from mvcc_data d where d.tx_id = r.tx_id)
                        """)) {
            query.setLong(1, txId);
            query.setLong(2, startTs);
            query.setLong(3, commitTs);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * Foreground Synonym replay also needs to accurately match the original key, tombstone and payload digests, which cannot be successful only with commit time.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param expected expected
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static boolean matchesExpected(final Connection connection, final long txId, final long startTs,
                                   final long commitTs, final Mutation expected) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select user_key, start_ts, commit_ts, deleted, payload_sha
                from mvcc_vector_retired_v1 where tx_id = ? for update
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return false;
                }
                boolean deleted = expected.type() == MutationType.DELETE;
                if (row.getLong("start_ts") != startTs || row.getLong("commit_ts") != commitTs
                        || !Arrays.equals(row.getBytes("user_key"), Utf8KeyOrder.encode(expected.key()))
                        || row.getBoolean("deleted") != deleted
                        || !Arrays.equals(row.getBytes("payload_sha"), payloadDigest(deleted, expected.value()))) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector retired replay differs from committed mutation", false, txId);
                }
                try (PreparedStatement rows = connection.prepareStatement("""
                        select exists(select 1 from mvcc_vector_committed_v1 where tx_id = ?)
                            or exists(select 1 from mvcc_data where tx_id = ?)
                        """)) {
                    rows.setLong(1, txId);
                    rows.setLong(2, txId);
                    try (ResultSet present = rows.executeQuery()) {
                        if (!present.next() || present.getBoolean(1)) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "vector retired receipt overlaps a live version", false, txId);
                        }
                    }
                }
                return true;
            }
        }
    }

    /**
     * Summary distinguishes between tombstone, SQL NULL and empty bytes payload, receipt does not retain business values per se.
     *
     * @param deleted deleted
     * @param payload payload
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    static byte[] payloadDigest(final boolean deleted, final byte[] payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((byte) (deleted ? 1 : 0));
            digest.update((byte) (payload == null ? 0 : 1));
            if (payload != null) {
                digest.update(payload);
            }
            return digest.digest();
        } catch (final NoSuchAlgorithmException ex) {
            throw new IllegalStateException("JDK SHA-256 provider is unavailable", ex);
        }
    }
}

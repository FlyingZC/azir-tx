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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Only the collection official vector local old line, with a mixed-key cross-shard map and already commit version always maintained.
 */
final class MysqlVectorLocalGarbageCollector {

    private final DataSource shard;

    private final String shardId;

    private final int stripes;

    private final MysqlVectorSnapshotLeaseStore protection;

    private volatile String expectedIdentity;

    /**
     * Accepts only the same shard layout that has been configured for reader protection.
     *
     * @param shard logical shard identifier
     * @param shardId logical shard identifier
     * @param stripes stripes
     * @param protection protection
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorLocalGarbageCollector(final DataSource shard, final String shardId, final int stripes,
                                     final MysqlVectorSnapshotLeaseStore protection) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.protection = Objects.requireNonNull(protection, "protection");
        if (shardId.isBlank() || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector local GC shard layout");
        }
        this.stripes = stripes;
    }

    /**
     * Scheduler read-only This data source identity confirms that the background pool did not misdirect physical shard.
     *
     * @return the operation result
     */
    DataSource dataSource() {
        return shard;
    }

    /**
     * The local GC shard number shall be equal to the issuer 's route number.
     *
     * @return the operation result
     */
    String shardId() {
        return shardId;
    }

    /**
     * The partition must be the same as the publisher and centre reader lease.
     *
     * @return the operation result
     */
    int stripeCount() {
        return stripes;
    }

    /**
     * Durable cursor fills only a single case of missing items, restarting cannot empty the collection progress.
     *
     * @throws SQLException if the operation cannot complete
     */
    void initializeProduct() {
        try (Connection connection = shard.getConnection(); Statement sql = connection.createStatement()) {
            String identity = MysqlVectorDatabaseIdentity.read(connection);
            if (expectedIdentity != null && !expectedIdentity.equals(identity)) {
                throw illegal("vector local GC shard identity changed during initialization");
            }
            sql.execute("""
                    create table if not exists mvcc_vector_local_gc_cursor_v1 (
                      singleton_id tinyint not null primary key,
                      after_key varbinary(256)
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_local_gc_cursor_v1(singleton_id, after_key)
                    values (1, null) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.executeUpdate();
            }
            try (PreparedStatement layout = connection.prepareStatement("""
                    select stripe_count from mvcc_vector_layout_v1 where singleton_id = 1
                    """); ResultSet row = layout.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw new SQLException("vector local GC stripe layout is missing or changed");
                }
            }
            expectedIdentity = identity;
        } catch (final SQLException ex) {
            throw unavailable("initialize vector local GC failed", ex);
        }
    }

    /**
     * Take the limit from the center durable security vector, then lock the shard key and remove the bounded earlier local version.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    int collect(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("vector local GC limit must be in 1..4096");
        }
        String expected = expectedIdentity;
        if (expected == null) {
            throw illegal("vector local GC was not initialized against a durable shard");
        }
        MysqlStripedSnapshotOracle.Snapshot floor = protection.currentSafeFloor();
        try {
            return MysqlConnectionContext.inTransaction(shard, connection -> {
                if (!expected.equals(MysqlVectorDatabaseIdentity.read(connection))) {
                    throw illegal("vector local GC shard identity changed before deletion");
                }
                byte[] after;
                try (PreparedStatement query = connection.prepareStatement("""
                        select after_key from mvcc_vector_local_gc_cursor_v1
                        where singleton_id = 1 for update
                        """); ResultSet row = query.executeQuery()) {
                    if (!row.next()) {
                        throw new SQLException("vector local GC cursor is missing");
                    }
                    after = row.getBytes(1);
                }
                int pageSize = Math.min(limit, 128);
                List<byte[]> keys = keysAfter(connection, after, pageSize);
                int reclaimed = 0;
                int scanned = 0;
                byte[] last = null;
                while (scanned < keys.size() && reclaimed < limit) {
                    byte[] key = keys.get(scanned++);
                    reclaimed += collectKey(connection, key, floor, limit - reclaimed);
                    last = key;
                }
                byte[] next = scanned == keys.size() && keys.size() < pageSize ? null : last;
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_vector_local_gc_cursor_v1
                        set after_key = ? where singleton_id = 1
                        """)) {
                    update.setBytes(1, next);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("vector local GC cursor update lost row");
                    }
                }
                return reclaimed;
            });
        } catch (final SQLException ex) {
            throw unavailable("collect vector local versions failed", ex);
        }
    }

    /**
     * Reads a limited candidate from the upper wheel durable cursor in binary main key order.
     *
     * @param connection connection
     * @param after after
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static List<byte[]> keysAfter(final Connection connection, final byte[] after, final int limit) throws SQLException {
        String sql = after == null ? """
                select distinct user_key from mvcc_vector_local_data_v1
                order by user_key limit ?
                """ : """
                select distinct user_key from mvcc_vector_local_data_v1
                where user_key > ? order by user_key limit ?
                """;
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = 1;
            if (after != null) {
                query.setBytes(parameter++, after);
            }
            query.setInt(parameter, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<byte[]> found = new ArrayList<>();
                while (rows.next()) {
                    found.add(rows.getBytes(1));
                }
                return found;
            }
        }
    }

    /**
     * The same key stabilizer lock pre-checks version and intent to avoid GC crossing local or cross-shard writing in commit.
     *
     * @param connection connection
     * @param key business key
     * @param floor floor
     * @param budget budget
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int collectKey(final Connection connection, final byte[] key,
                           final MysqlStripedSnapshotOracle.Snapshot floor,
                           final int budget) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("""
                insert into mvcc_key_lock(user_key) values (?)
                on duplicate key update user_key = user_key
                """)) {
            lock.setBytes(1, key);
            lock.executeUpdate();
        }
        for (String table : List.of("mvcc_intent", "mvcc_vector_intent_reservation_v1",
                "mvcc_rollback")) {
            if (hasKey(connection, table, key)) {
                return 0;
            }
        }
        if (hasUnmappedScalarData(connection, key)) {
            return 0;
        }
        int stripe;
        try (PreparedStatement latest = connection.prepareStatement("""
                select stripe_id from mvcc_vector_local_data_v1
                where user_key = ? order by local_seq desc limit 1 for update
                """)) {
            latest.setBytes(1, key);
            try (ResultSet row = latest.executeQuery()) {
                if (!row.next()) {
                    return 0;
                }
                stripe = row.getInt(1);
            }
        }
        if (stripe < 0 || stripe >= stripes) {
            throw illegal("vector local GC found an invalid data stripe");
        }
        try (PreparedStatement wrong = connection.prepareStatement("""
                select 1 from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id <> ? limit 1 for update
                """)) {
            wrong.setBytes(1, key);
            wrong.setInt(2, stripe);
            try (ResultSet row = wrong.executeQuery()) {
                if (row.next()) {
                    throw illegal("vector local GC found a key in multiple stripes");
                }
            }
        }
        long bound = floor.sequence(shardId, stripe);
        Long predecessor = null;
        try (PreparedStatement query = connection.prepareStatement("""
                select local_seq from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id = ? and local_seq <= ?
                order by local_seq desc limit 1 for update
                """)) {
            query.setBytes(1, key);
            query.setInt(2, stripe);
            query.setLong(3, bound);
            try (ResultSet row = query.executeQuery()) {
                if (row.next()) {
                    predecessor = row.getLong(1);
                }
            }
        }
        Long crossPredecessor = visibleCrossPredecessor(connection, key, stripe,
                bound, floor.decisionEpoch());
        if (crossPredecessor != null && (predecessor == null || crossPredecessor > predecessor)) {
            predecessor = crossPredecessor;
        }
        if (predecessor == null) {
            return 0;
        }
        try (PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id = ? and local_seq < ?
                order by local_seq limit ?
                """)) {
            delete.setBytes(1, key);
            delete.setInt(2, stripe);
            delete.setLong(3, predecessor);
            delete.setInt(4, budget);
            return delete.executeUpdate();
        }
    }

    /**
     * The old cursor version retains the whole key history when there is no vector sorting identity and cannot be extrapolated by an local serial number.
     *
     * @param connection connection
     * @param key business key
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean hasUnmappedScalarData(final Connection connection, final byte[] key) throws SQLException {
        List<DataIdentity> data = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                select tx_id, commit_ts from mvcc_data
                where user_key = ? for update
                """)) {
            query.setBytes(1, key);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    data.add(new DataIdentity(rows.getLong(1), rows.getLong(2)));
                }
            }
        }
        try (PreparedStatement mapping = connection.prepareStatement("""
                select 1 from mvcc_vector_committed_v1
                where user_key = ? and tx_id = ? and commit_ts = ?
                limit 1 for update
                """)) {
            for (DataIdentity version : data) {
                mapping.setBytes(1, key);
                mapping.setLong(2, version.txId());
                mapping.setLong(3, version.commitTs());
                try (ResultSet row = mapping.executeQuery()) {
                    if (!row.next()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Decision cross-shard version timed to enter a secure cut-off can be used as a stable front for the old line local.
     *
     * @param connection connection
     * @param key business key
     * @param stripe stripe
     * @param sequenceBound sequence bound
     * @param decisionBound decision bound
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Long visibleCrossPredecessor(final Connection connection, final byte[] key,
                                                final int stripe, final long sequenceBound,
                                                final long decisionBound) throws SQLException {
        List<CrossMapping> mappings = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                select tx_id, stripe_id, reserved_seq, commit_ts, deleted, payload
                from mvcc_vector_committed_v1 where user_key = ? for update
                """)) {
            query.setBytes(1, key);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getInt("stripe_id") != stripe) {
                        throw illegal("vector local GC found cross data in another stripe");
                    }
                    mappings.add(new CrossMapping(rows.getLong("tx_id"),
                            rows.getLong("reserved_seq"), rows.getLong("commit_ts"),
                            rows.getBoolean("deleted"), rows.getBytes("payload")));
                }
            }
        }
        Long predecessor = null;
        try (PreparedStatement version = connection.prepareStatement("""
                select deleted, value from mvcc_data
                where user_key = ? and tx_id = ? and commit_ts = ? for update
                """)) {
            for (CrossMapping mapping : mappings) {
                version.setBytes(1, key);
                version.setLong(2, mapping.txId());
                version.setLong(3, mapping.commitTs());
                try (ResultSet row = version.executeQuery()) {
                    if (!row.next() || row.getBoolean("deleted") != mapping.deleted()
                            || !Arrays.equals(row.getBytes("value"), mapping.payload())) {
                        throw illegal("vector local GC found an incomplete cross version proof");
                    }
                }
                if (mapping.sequence() <= sequenceBound && mapping.commitTs() <= decisionBound
                        && (predecessor == null || mapping.sequence() > predecessor)) {
                    predecessor = mapping.sequence();
                }
            }
        }
        return predecessor;
    }

    /**
     * Pending intent and rollback certificates are not yet safe from the released lower bounds collection.
     *
     * @param connection connection
     * @param table table
     * @param key business key
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean hasKey(final Connection connection, final String table, final byte[] key) throws SQLException {
        String sql = "select 1 from " + table + " where user_key = ? limit 1 for update";
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, key);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * Stop GC when cross-section identity damage is discovered, instead of speculating which version chain should be retained.
     *
     * @param message message
     * @return the operation result
     */
    private static MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * Controls or shard database do not allow the continuation of collection as assumed when it is not available.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unavailable(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * Cross-shard Maps original retention number, centre decision and real version payload.
     *
     * @param txId transaction identifier
     * @param sequence sequence
     * @param commitTs durable commit timestamp
     * @param deleted deleted
     * @param payload payload
     */
    private record CrossMapping(long txId, long sequence, long commitTs,
            boolean deleted, byte[] payload) {
    }

    /**
     * Durable identity of the cursor and cross-shard data row is used to identify old protocol records without vector mapping.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    private record DataIdentity(long txId, long commitTs) {
    }
}

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

package com.flyingzc.azir.publication.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Use the old status table to archive as a physical fence and toggle the durable protocol of the phased recovery sub-book.
 */
final class MysqlSegmentProtocolActivator {

    private static final String OLD_STATE = "mvcc_publication_state_v1";

    private static final String ARCHIVED_STATE = "mvcc_publication_state_v1_legacy_archived";

    private final DataSource dataSource;

    private final MysqlSegmentPublicationLedger segments;

    /**
     * Saves a control library and an initialization portal for partition status.
     *
     * @param dataSource database connection source
     * @param segments segments
     */
    MysqlSegmentProtocolActivator(final DataSource dataSource, final MysqlSegmentPublicationLedger segments) {
        this.dataSource = dataSource;
        this.segments = segments;
    }

    /**
     * Archives the old status table after you have been disabled and continues to switch from durable stage and the archive form when you collapse.
     */
    void activate() {
        MysqlConnectionContext.withPublicationLock(dataSource, 10, () -> {
            try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
                MysqlProtocolEpoch.Mode mode = MysqlProtocolEpoch.current(connection);
                if (mode == MysqlProtocolEpoch.Mode.SEGMENT) {
                    assertActivated(connection);
                    return null;
                }
                if (mode != MysqlProtocolEpoch.Mode.LEDGER
                        && mode != MysqlProtocolEpoch.Mode.SEGMENT_PREPARE) {
                    throw illegal("only an active old ledger can enter segmented mode");
                }
                boolean archived = tableExists(connection, ARCHIVED_STATE);
                if (!archived) {
                    if (mode != MysqlProtocolEpoch.Mode.LEDGER || !tableExists(connection, OLD_STATE)) {
                        throw illegal("old publication state cannot be archived or resumed");
                    }
                    requireDrained(connection, OLD_STATE);
                    // RENAME waits for the current transaction on the old status table, after which the old process can no longer update the original table.
                    try (Statement rename = connection.createStatement()) {
                        rename.execute("rename table " + OLD_STATE + " to " + ARCHIVED_STATE);
                    }
                }
                removeEmptyRecreatedOldState(connection);
                LegacyWatermark previous = requireDrained(connection, ARCHIVED_STATE);
                if (mode == MysqlProtocolEpoch.Mode.LEDGER) {
                    MysqlConnectionContext.inTransaction(dataSource, activation -> {
                        if (MysqlProtocolEpoch.lock(activation) != MysqlProtocolEpoch.Mode.LEDGER) {
                            throw illegal("old ledger mode changed after physical fence");
                        }
                        MysqlProtocolEpoch.transition(activation, MysqlProtocolEpoch.Mode.LEDGER,
                                MysqlProtocolEpoch.Mode.SEGMENT_PREPARE);
                        return null;
                    });
                }
                segments.initializeIsolated(previous.upper());
                MysqlConnectionContext.inTransaction(dataSource, activation -> {
                    if (MysqlProtocolEpoch.lock(activation) != MysqlProtocolEpoch.Mode.SEGMENT_PREPARE) {
                        throw illegal("segment activation phase changed before final transition");
                    }
                    try (
                            PreparedStatement state = activation.prepareStatement("""
                                    select base_commit_ts, next_commit_ts, published_ts, safe_point
                                    from mvcc_segment_publication_state_v1 where singleton_id = 1 for update
                                    """);
                            ResultSet row = state.executeQuery()) {
                        if (!row.next() || row.getLong(1) != previous.upper()
                                || row.getLong(2) != previous.upper()
                                || row.getLong(3) != previous.upper()
                                || row.getLong(4) > previous.upper()) {
                            throw illegal("segment timeline differs from archived publication boundary");
                        }
                    }
                    try (PreparedStatement safe = activation.prepareStatement("""
                            update mvcc_segment_publication_state_v1
                            set safe_point = greatest(safe_point, ?) where singleton_id = 1
                            """)) {
                        safe.setLong(1, previous.safePoint());
                        safe.executeUpdate();
                    }
                    MysqlProtocolEpoch.transition(activation, MysqlProtocolEpoch.Mode.SEGMENT_PREPARE,
                            MysqlProtocolEpoch.Mode.SEGMENT);
                    return null;
                });
                assertActivated(connection);
            } catch (final SQLException ex) {
                throw storageError("activate segmented publication mode failed", ex);
            }
            return null;
        });
    }

    /**
     * Runs at the same time checking mode lines, archive borders and isolated old status tables.
     */
    void assertActivated() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
            assertActivated(connection);
        } catch (final SQLException ex) {
            throw storageError("inspect segmented publication activation failed", ex);
        }
    }

    /**
     * The new verify model in the same connection cannot be falsified by missing filing forms or re-establishing old lines.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void assertActivated(final Connection connection) throws SQLException {
        MysqlProtocolEpoch.requireSegment(connection);
        if (!tableExists(connection, ARCHIVED_STATE)
                || !tableExists(connection, "mvcc_segment_publication_state_v1")
                || !tableExists(connection, "mvcc_publication_segment_v1")
                || !tableExists(connection, "mvcc_publication_segment_slot_v1")) {
            throw illegal("segmented mode is missing durable tables or old fence");
        }
        if (tableExists(connection, OLD_STATE)) {
            throw illegal("old publication state table reappeared after physical fencing");
        }
        segments.assertSchemaReady(connection);
        LegacyWatermark previous = readWatermark(connection, ARCHIVED_STATE);
        try (
                PreparedStatement state = connection.prepareStatement("""
                        select base_commit_ts, next_commit_ts, published_ts, safe_point
                        from mvcc_segment_publication_state_v1 where singleton_id = 1
                        """);
                ResultSet row = state.executeQuery()) {
            if (!row.next() || row.getLong(1) != previous.upper()
                    || row.getLong(2) < row.getLong(3)
                    || row.getLong(3) < previous.upper()
                    || row.getLong(4) < previous.safePoint()
                    || row.getLong(4) > row.getLong(3)) {
                throw illegal("segmented timeline does not continue archived watermark");
            }
        }
    }

    /**
     * The old initializer may recreate an empty table after archiving, allowing only the missing feature with no time line data to be removed.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void removeEmptyRecreatedOldState(final Connection connection) throws SQLException {
        if (!tableExists(connection, OLD_STATE)) {
            return;
        }
        if (rowCount(connection, OLD_STATE) != 0) {
            throw illegal("old publication state was repopulated after archive");
        }
        try (Statement drop = connection.createStatement()) {
            drop.execute("drop table " + OLD_STATE);
        }
    }

    /**
     * All old decision, reader and participant are required to be empty before and after the migration.
     *
     * @param connection connection
     * @param stateTable state table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private LegacyWatermark requireDrained(final Connection connection, final String stateTable) throws SQLException {
        LegacyWatermark watermark = readWatermark(connection, stateTable);
        try (
                PreparedStatement query = connection.prepareStatement("""
                        select (select count(*) from mvcc_publication_entry_v1 where status = 'PENDING')
                             + (select count(*) from mvcc_snapshot_lease_v2
                                 where expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed))
                             + (select count(*) from mvcc_coordinator_snapshot_lease_v1
                                 where expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed))
                             + (select count(*) from mvcc_txn t
                                 left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                                 where f.tx_id is null)
                        """);
                ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getLong(1) != 0) {
                throw illegal("old ledger still has pending decisions, readers or unfinalized participants");
            }
        }
        return watermark;
    }

    /**
     * The old book must have no unpublished slot, and the archive line retains its safe point.
     *
     * @param connection connection
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private LegacyWatermark readWatermark(final Connection connection, final String table) throws SQLException {
        try (
                PreparedStatement query = connection.prepareStatement("""
                        select next_commit_ts, published_ts, safe_point from %s where singleton_id = 1
                        """.formatted(table));
                ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getLong(1) != row.getLong(2)
                    || row.getLong(3) > row.getLong(2) || row.getLong(2) < 0) {
                throw illegal("old ledger has an unpublished or invalid boundary");
            }
            return new LegacyWatermark(row.getLong(2), row.getLong(3));
        }
    }

    /**
     * Checks whether the fixed protocol table name is the real table in the current database.
     *
     * @param connection connection
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private boolean tableExists(final Connection connection, final String table) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.tables
                where table_schema = database() and table_name = ? and table_type = 'BASE TABLE'
                limit 1
                """)) {
            query.setString(1, table);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * Read-only takes the number of rows with a fixed name to reject the re-emergence of the old time line.
     *
     * @param connection connection
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private long rowCount(final Connection connection, final String table) throws SQLException {
        try (
                PreparedStatement query = connection.prepareStatement("select count(*) from " + table);
                ResultSet row = query.executeQuery()) {
            row.next();
            return row.getLong(1);
        }
    }

    /**
     * The migration of non-variable states will not be satisfied by failure to close.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * Keep database abnormally available to distinguish DDL from connection failure.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private MvccException storageError(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, false, null, cause);
    }

    /**
     * Keep archive time upper bounds and safe point.
     *
     * @param upper upper
     * @param safePoint oldest retained snapshot timestamp
     */
    private record LegacyWatermark(long upper, long safePoint) {
    }
}

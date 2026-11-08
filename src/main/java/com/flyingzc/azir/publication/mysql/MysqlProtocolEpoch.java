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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Separates old and new release protocols by durable mode rows with the old lease external key.
 */
final class MysqlProtocolEpoch {

    /**
     * Only the protocol mode operation in MySQL connection is available.
     */
    private MysqlProtocolEpoch() {
    }

    /**
     * Installs the mode line and the old lease external key to repeat the initialize the existing protocol stage.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void install(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_protocol_epoch_v1 (
                      singleton_id tinyint not null primary key,
                      protocol_mode varchar(16) not null,
                      generation bigint not null
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("""
                    insert ignore into mvcc_protocol_epoch_v1(singleton_id, protocol_mode, generation)
                    values (1, 'LEGACY', 0)
                    """);
            statement.execute("""
                    create table if not exists mvcc_legacy_lease_gate_v1 (
                      mode_id tinyint not null primary key
                    ) engine=InnoDB
                    """);
        }
        Mode mode = current(connection);
        if (mode == Mode.LEGACY) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("insert ignore into mvcc_legacy_lease_gate_v1(mode_id) values (0)");
            }
            requireGateRow(connection, 0);
        } else {
            requireGateRow(connection, 1);
        }
        if (!schemaObjectExists(connection, "information_schema.columns",
                "table_name = 'mvcc_snapshot_lease' and column_name = 'protocol_epoch'")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        alter table mvcc_snapshot_lease
                        add column protocol_epoch tinyint not null default 0
                        """);
            }
        }
        if (!schemaObjectExists(connection, "information_schema.table_constraints",
                "table_name = 'mvcc_snapshot_lease' and constraint_name = 'mvcc_legacy_lease_epoch_fk' "
                        + "and constraint_type = 'FOREIGN KEY'")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        alter table mvcc_snapshot_lease
                        add constraint mvcc_legacy_lease_epoch_fk
                        foreign key (protocol_epoch) references mvcc_legacy_lease_gate_v1(mode_id)
                        on update restrict on delete restrict
                        """);
            }
        }
    }

    /**
     * Modeline must be aligned with the only valid parent key to prevent the old lease from being restarted.
     *
     * @param connection connection
     * @param expected expected
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static void requireGateRow(final Connection connection, final int expected) throws SQLException {
        try (
                PreparedStatement query = connection.prepareStatement("select mode_id from mvcc_legacy_lease_gate_v1");
                ResultSet result = query.executeQuery()) {
            if (!result.next() || result.getInt(1) != expected || result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "legacy snapshot gate differs from publication mode", false, null);
            }
        }
    }

    /**
     * Ask if the version mode structure has been installed.
     *
     * @param connection connection
     * @param table table
     * @param conditions conditions
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean schemaObjectExists(final Connection connection, final String table, final String conditions) throws SQLException {
        String sql = "select 1 from " + table + " where table_schema = database() and " + conditions + " limit 1";
        try (
                PreparedStatement query = connection.prepareStatement(sql);
                ResultSet result = query.executeQuery()) {
            return result.next();
        }
    }

    /**
     * Querying the current durable mode, failed to close when no or unknown mode exists.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static Mode current(final Connection connection) throws SQLException {
        return readMode(connection, false);
    }

    /**
     * Unlock locking mode lines, toggle checking and updating must be done in the same database transaction.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static Mode lock(final Connection connection) throws SQLException {
        return readMode(connection, true);
    }

    /**
     * Advance the protocol phase and add algebras to the expected old model atomic.
     *
     * @param connection connection
     * @param expected expected
     * @param next next
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static void transition(final Connection connection, final Mode expected, final Mode next) throws SQLException {
        if (!(expected == Mode.LEGACY && next == Mode.ACTIVATING
                || expected == Mode.ACTIVATING && next == Mode.LEDGER
                || expected == Mode.LEDGER && next == Mode.SEGMENT_PREPARE
                || expected == Mode.SEGMENT_PREPARE && next == Mode.SEGMENT)) {
            throw new IllegalArgumentException("unsupported publication protocol transition");
        }
        if (expected == Mode.LEGACY && next == Mode.ACTIVATING) {
            // Parent-key updates are the same as mode line updates transaction; old lease is either first commit and stops switching, or waits for later failure.
            try (PreparedStatement gate = connection.prepareStatement("""
                    update mvcc_legacy_lease_gate_v1 set mode_id = 1 where mode_id = 0
                    """)) {
                if (gate.executeUpdate() != 1) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "legacy snapshot gate cannot enter activating mode", false, null);
                }
            }
        }
        try (PreparedStatement update = connection.prepareStatement("""
                update mvcc_protocol_epoch_v1
                set protocol_mode = ?, generation = generation + 1
                where singleton_id = 1 and protocol_mode = ?
                """)) {
            update.setString(1, next.name());
            update.setString(2, expected.name());
            if (update.executeUpdate() != 1) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication protocol mode changed during activation", false, null);
            }
        }
    }

    /**
     * New snapshot can only start after the account book mode has been activated by durable.
     *
     * @param connection connection
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static void requireLedger(final Connection connection) throws SQLException {
        if (readMode(connection, false) != Mode.LEDGER) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication ledger protocol is not active", false, null);
        }
    }

    /**
     * The branch entry can be activated only after the old status table is archived and the durable mode switch has been completed.
     *
     * @param connection connection
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static void requireSegment(final Connection connection) throws SQLException {
        if (readMode(connection, false) != Mode.SEGMENT) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented publication protocol is not active", false, null);
        }
    }

    /**
     * Rejects the status of missing and unrecognized, depending on whether or not you are exclusive in reading mode.
     *
     * @param connection connection
     * @param exclusive exclusive
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static Mode readMode(final Connection connection, final boolean exclusive) throws SQLException {
        String sql = "select protocol_mode from mvcc_protocol_epoch_v1 where singleton_id = 1"
                + (exclusive ? " for update" : " for share");
        try (
                PreparedStatement query = connection.prepareStatement(sql);
                ResultSet result = query.executeQuery()) {
            if (!result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication protocol mode row missing", false, null);
            }
            try {
                return Mode.valueOf(result.getString(1));
            } catch (final IllegalArgumentException ex) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "unknown publication protocol mode", false, null, ex);
            }
        }
    }

    /**
     * Durable phase of an old agreement, account book and branch book switch.
     */
    enum Mode {
        LEGACY, ACTIVATING, LEDGER, SEGMENT_PREPARE, SEGMENT
    }
}

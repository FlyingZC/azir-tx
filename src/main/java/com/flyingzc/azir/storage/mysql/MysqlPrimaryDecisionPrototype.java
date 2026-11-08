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
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Main participation shard replaces the isolated prototype transaction decision with a durable status line and subsection decision serial numbers.
 */
final class MysqlPrimaryDecisionPrototype {

    private final DataSource primary;

    private final DataSource secondary;

    private final int stripes;

    /**
     * Main transaction rows with decision count only primary shard and repeat shard provide only durable prewrite certificates.
     *
     * @param primary original Percolator primary identity
     * @param secondary secondary
     * @param stripes stripes
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlPrimaryDecisionPrototype(final DataSource primary, final DataSource secondary, final int stripes) {
        this.primary = Objects.requireNonNull(primary, "primary");
        this.secondary = Objects.requireNonNull(secondary, "secondary");
        if (stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid primary decision stripes");
        }
        this.stripes = stripes;
    }

    /**
     * Creates a separate main transaction row and split count, and repeats the call to keep decision data.
     *
     * @throws SQLException if the operation cannot complete
     */
    void initializeIsolated() throws SQLException {
        try (Connection connection = primary.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_primary_layout_v1 (
                      singleton_id tinyint not null primary key,
                      stripe_count smallint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_primary_decision_state_v1 (
                      stripe_id smallint not null primary key,
                      committed_seq bigint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_primary_txn_v2 (
                      tx_id bigint not null primary key,
                      primary_key varbinary(256) not null,
                      primary_value blob not null,
                      secondary_key varbinary(256) not null,
                      secondary_value blob not null,
                      primary_stripe smallint not null,
                      secondary_stripe smallint not null,
                      primary_start_seq bigint not null,
                      secondary_start_seq bigint not null,
                      deadline_millis bigint not null,
                      decision_stripe smallint not null,
                      status varchar(16) not null,
                      decision_seq bigint,
                      primary_reserved_seq bigint,
                      secondary_reserved_seq bigint,
                      index bench_primary_unfinished (status, deadline_millis, tx_id)
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_primary_layout_v1(singleton_id, stripe_count)
                    values (1, ?) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setInt(1, stripes);
                insert.executeUpdate();
            }
            try (PreparedStatement query = connection.prepareStatement("""
                    select stripe_count from bench_primary_layout_v1 where singleton_id = 1
                    """); ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw new SQLException("primary decision stripe layout changed");
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_primary_decision_state_v1(stripe_id, committed_seq)
                    values (?, 0) on duplicate key update stripe_id = stripe_id
                    """)) {
                for (int stripe = 0; stripe < stripes; stripe++) {
                    insert.setInt(1, stripe);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            MysqlPrimaryCombinedPrewriteProcedure.ensure(connection);
        }
    }

    /**
     * Test only visible calls to clear the isolation, not for product recovery.
     *
     * @throws SQLException if the operation cannot complete
     */
    void resetIsolated() throws SQLException {
        try (Connection connection = primary.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from bench_primary_txn_v2");
            statement.executeUpdate("update bench_primary_decision_state_v1 set committed_seq = 0");
        }
    }

    /**
     * The main ACTIVE identity starts with durable storage; the same sign replay can only use exactly the same key values and duration.
     *
     * @param registration registration
     * @return the operation result
     */
    Decision register(final Registration registration) {
        requireStripe(registration.decisionStripe());
        requireStripe(registration.primaryStripe());
        requireStripe(registration.secondaryStripe());
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into bench_primary_txn_v2
                          (tx_id, primary_key, primary_value, secondary_key, secondary_value,
                           primary_stripe, secondary_stripe,
                           primary_start_seq, secondary_start_seq, deadline_millis,
                           decision_stripe, status)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')
                        on duplicate key update tx_id = tx_id
                        """)) {
                    insert.setLong(1, registration.txId());
                    insert.setBytes(2, encode(registration.primaryKey()));
                    insert.setBytes(3, registration.primaryValue());
                    insert.setBytes(4, encode(registration.secondaryKey()));
                    insert.setBytes(5, registration.secondaryValue());
                    insert.setInt(6, registration.primaryStripe());
                    insert.setInt(7, registration.secondaryStripe());
                    insert.setLong(8, registration.primaryStartSequence());
                    insert.setLong(9, registration.secondaryStartSequence());
                    insert.setLong(10, registration.deadlineMillis());
                    insert.setInt(11, registration.decisionStripe());
                    insert.executeUpdate();
                }
                PrimaryRow row = readPrimary(connection, registration.txId(), true);
                requireSameRegistration(row, registration);
                if (row.status() == TxStatus.ABORTED) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "primary transaction was already aborted", false, registration.txId());
                }
                return row.decision();
            });
        } catch (final SQLException ex) {
            throw unknown(registration.txId(), "register primary transaction failed", ex);
        }
    }

    /**
     * The main ACTIVE registration with the main intent prewrite is stored by a storage process atomic commit and returns the retention number when successful.
     *
     * @param registration registration
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    long registerAndPrewritePrimary(final Registration registration) {
        Objects.requireNonNull(registration, "registration");
        requireStripe(registration.primaryStripe());
        requireStripe(registration.secondaryStripe());
        requireStripe(registration.decisionStripe());
        try (Connection connection = primary.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_primary_register_prewrite_v3(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            call.setLong(1, registration.txId());
            call.setBytes(2, encode(registration.primaryKey()));
            call.setBytes(3, registration.primaryValue());
            call.setBytes(4, encode(registration.secondaryKey()));
            call.setBytes(5, registration.secondaryValue());
            call.setInt(6, registration.primaryStripe());
            call.setInt(7, registration.secondaryStripe());
            call.setLong(8, registration.primaryStartSequence());
            call.setLong(9, registration.secondaryStartSequence());
            call.setLong(10, registration.deadlineMillis());
            call.setInt(11, registration.decisionStripe());
            try (ResultSet result = call.executeQuery()) {
                if (!result.next() || result.getLong(1) <= registration.primaryStartSequence()) {
                    throw new SQLException("combined primary prewrite did not return a valid sequence");
                }
                return result.getLong(1);
            }
        } catch (final SQLException ex) {
            ErrorCode code = switch (ex.getErrorCode()) {
                case 36002 -> ErrorCode.LOCK_CONFLICT;
                case 36003 -> ErrorCode.WRITE_CONFLICT;
                case 36006, 36016 -> ErrorCode.TRANSACTION_ABORTED;
                case 36017 -> ErrorCode.TRANSACTION_TIMEOUT;
                case 36004, 36005, 36015 -> ErrorCode.ILLEGAL_STATE;
                default -> ErrorCode.TRANSACTION_STATUS_UNKNOWN;
            };
            throw new MvccException(code, "combined primary prewrite failed",
                    code == ErrorCode.TRANSACTION_STATUS_UNKNOWN, registration.txId(), ex);
        }
    }

    /**
     * Primary shard Reads two pieces of durable prewrite, locks within and suspends competition and allocates decision serial numbers to atomic.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    Decision commit(final long txId) {
        PrimaryRow identity = requirePrimary(txId);
        if (identity.status() == TxStatus.COMMITTED) {
            return identity.decision();
        }
        if (identity.status() == TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "primary transaction was aborted", false, txId);
        }
        ParticipantProof primaryProof = verifyPrewrite(primary, txId, identity.primaryKey(),
                identity.primaryValue(), identity.primaryStripe(), identity.primaryStartSequence());
        ParticipantProof secondaryProof = verifyPrewrite(secondary, txId, identity.secondaryKey(),
                identity.secondaryValue(), identity.secondaryStripe(), identity.secondaryStartSequence());
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                PrimaryRow row = readPrimary(connection, txId, true);
                requireSameRegistration(row, identity.registration());
                return decideCommitted(connection, row, primaryProof, secondaryProof);
            });
        } catch (final SQLException ex) {
            throw unknown(txId, "commit primary decision requires durable lookup", ex);
        }
    }

    /**
     * Foreground holds a full registration identity without additional queries outside primary shard transaction and rechecks identity within the line lock.
     *
     * @param registration registration
     * @return the operation result
     */
    Decision commit(final Registration registration) {
        Objects.requireNonNull(registration, "registration");
        ParticipantProof secondaryProof = verifyPrewrite(secondary, registration.txId(),
                registration.secondaryKey(), registration.secondaryValue(),
                registration.secondaryStripe(), registration.secondaryStartSequence());
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                PrimaryRow row = readPrimary(connection, registration.txId(), true);
                requireSameRegistration(row, registration);
                if (row.status() == TxStatus.COMMITTED) {
                    return row.decision();
                }
                ParticipantProof primaryProof = verifyPrewrite(connection,
                        registration.txId(), registration.primaryKey(),
                        registration.primaryValue(), registration.primaryStripe(),
                        registration.primaryStartSequence());
                return decideCommitted(connection, row, primaryProof, secondaryProof);
            });
        } catch (final SQLException ex) {
            throw unknown(registration.txId(), "commit primary decision requires durable lookup", ex);
        }
    }

    /**
     * In the main line lock, the decision serial number is aligned with the terminal state transaction commit and the expired and suspended state is not allowed to reverse.
     *
     * @param connection connection
     * @param row row
     * @param primaryProof primary proof
     * @param secondaryProof secondary proof
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private Decision decideCommitted(final Connection connection, final PrimaryRow row,
                                     final ParticipantProof primaryProof,
                                     final ParticipantProof secondaryProof) throws SQLException {
        long txId = row.txId();
        if (row.status() == TxStatus.COMMITTED) {
            return row.decision();
        }
        if (row.status() != TxStatus.ACTIVE || row.deadlineMillis() <= databaseNow(connection)) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "primary decision lost ACTIVE lease", false, txId);
        }
        try (PreparedStatement advance = connection.prepareStatement("""
                update bench_primary_decision_state_v1
                set committed_seq = last_insert_id(committed_seq + 1)
                where stripe_id = ?
                """)) {
            advance.setInt(1, row.decisionStripe());
            if (advance.executeUpdate() != 1) {
                throw new SQLException("primary decision stripe is missing");
            }
        }
        long decisionSequence;
        try (
                PreparedStatement sequence = connection.prepareStatement("select last_insert_id()");
                ResultSet result = sequence.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("primary decision did not return a sequence");
            }
            decisionSequence = result.getLong(1);
        }
        try (PreparedStatement terminal = connection.prepareStatement("""
                update bench_primary_txn_v2
                set status = 'COMMITTED', decision_seq = ?,
                    primary_reserved_seq = ?, secondary_reserved_seq = ?
                where tx_id = ? and status = 'ACTIVE'
                """)) {
            terminal.setLong(1, decisionSequence);
            terminal.setLong(2, primaryProof.reservedSequence());
            terminal.setLong(3, secondaryProof.reservedSequence());
            terminal.setLong(4, txId);
            if (terminal.executeUpdate() != 1) {
                throw new SQLException("primary decision lost transaction row lock");
            }
        }
        return new Decision(txId, TxStatus.COMMITTED, row.decisionStripe(),
                decisionSequence, primaryProof.reservedSequence(),
                secondaryProof.reservedSequence());
    }

    /**
     * Commit Before registering identity from primary shard durable row recovery and when missing, decision based on caller information is prohibited.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private PrimaryRow requirePrimary(final long txId) {
        try (Connection connection = primary.getConnection()) {
            PrimaryRow row = readPrimary(connection, txId, false);
            if (row == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "primary commit has no durable ACTIVE identity", false, txId);
            }
            return row;
        } catch (final SQLException ex) {
            throw unknown(txId, "read primary transaction identity failed", ex);
        }
    }

    /**
     * Same main transaction lockstring aborted with commit, commit terminal state cannot reverse rollback.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    Decision abort(final long txId) {
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                PrimaryRow row = readPrimary(connection, txId, true);
                if (row == null) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "primary abort has no durable ACTIVE identity", false, txId);
                }
                if (row.status() == TxStatus.COMMITTED) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "primary abort raced a committed decision", false, txId);
                }
                if (row.status() == TxStatus.ACTIVE) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update bench_primary_txn_v2 set status = 'ABORTED'
                            where tx_id = ? and status = 'ACTIVE'
                            """)) {
                        update.setLong(1, txId);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("primary abort lost ACTIVE row lock");
                        }
                    }
                }
                return new Decision(txId, TxStatus.ABORTED, row.decisionStripe(), null, null, null);
            });
        } catch (final SQLException ex) {
            throw unknown(txId, "abort primary decision requires durable lookup", ex);
        }
    }

    /**
     * Replaying and reading sides believe only real primary shard transaction rows, do not cache ACTIVE or missing terminal state.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    Optional<Decision> decision(final long txId) {
        try (Connection connection = primary.getConnection()) {
            PrimaryRow row = readPrimary(connection, txId, false);
            return row == null ? Optional.empty() : Optional.of(row.decision());
        } catch (final SQLException ex) {
            throw unknown(txId, "read primary decision failed", ex);
        }
    }

    /**
     * Read side check transaction ID for primary shard durable identity to prevent extra intent counterfeit commit transaction.
     *
     * @param txId transaction identifier
     * @param primaryShard primary shard
     * @param key business key
     * @param stripe stripe
     * @param reservedSequence reserved sequence
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    Decision participantDecision(final long txId, final boolean primaryShard, final String key,
                                 final int stripe, final long reservedSequence) {
        try (Connection connection = primary.getConnection()) {
            PrimaryRow row = readPrimary(connection, txId, false);
            if (row == null) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "participant intent has no durable primary identity", true, txId);
            }
            if (primaryShard ? !row.primaryKey().equals(key) : !row.secondaryKey().equals(key)) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "participant intent key differs from primary identity", false, txId);
            }
            if (primaryShard ? row.primaryStripe() != stripe : row.secondaryStripe() != stripe) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "participant intent stripe differs from primary identity", false, txId);
            }
            if (row.status() == TxStatus.COMMITTED) {
                Long expected = primaryShard ? row.primarySequence() : row.secondarySequence();
                if (expected == null || expected != reservedSequence) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "participant intent sequence differs from primary decision", false, txId);
                }
            }
            return row.decision();
        } catch (final SQLException ex) {
            throw unknown(txId, "read participant primary decision failed", ex);
        }
    }

    /**
     * A MySQL statement gets a full piece decision watermark for first sample by the cutface publisher.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    List<Long> decisionVector() {
        try (Connection connection = primary.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select stripe_id, committed_seq from bench_primary_decision_state_v1 order by stripe_id
                """); ResultSet row = query.executeQuery()) {
            List<Long> values = new ArrayList<>(stripes);
            while (row.next()) {
                if (row.getInt(1) != values.size() || row.getLong(2) < 0) {
                    throw new SQLException("primary decision vector is incomplete");
                }
                values.add(row.getLong(2));
            }
            if (values.size() != stripes) {
                throw new SQLException("primary decision vector lost a stripe");
            }
            return List.copyOf(values);
        } catch (final SQLException ex) {
            throw unknown(null, "read primary decision vector failed", ex);
        }
    }

    /**
     * All participant proofs must be read from durable intent for shard or rereaded by materialization version.
     *
     * @param shard logical shard identifier
     * @param txId transaction identifier
     * @param key business key
     * @param value value to write
     * @param stripe stripe
     * @param startSequence start sequence
     * @return the operation result
     */
    private ParticipantProof verifyPrewrite(final DataSource shard, final long txId, final String key,
                                            final byte[] value, final int stripe, final long startSequence) {
        try (Connection connection = shard.getConnection()) {
            return verifyPrewrite(connection, txId, key, value, stripe, startSequence);
        } catch (final SQLException ex) {
            throw unknown(txId, "read participant prewrite proof failed", ex);
        }
    }

    /**
     * Read prewrite proof in the maincard line lock transaction, responding to replay accept materialization version for the same identity.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param key business key
     * @param value value to write
     * @param stripe stripe
     * @param startSequence start sequence
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private ParticipantProof verifyPrewrite(final Connection connection, final long txId, final String key,
                                            final byte[] value, final int stripe, final long startSequence) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select stripe_id, start_seq, reserved_seq, payload
                from bench_striped_mvcc_cross_intent where user_key = ? and tx_id = ?
                union all
                select stripe_id, start_seq, reserved_seq, payload
                from bench_striped_mvcc_cross_data where user_key = ? and tx_id = ?
                """)) {
            byte[] encoded = encode(key);
            query.setBytes(1, encoded);
            query.setLong(2, txId);
            query.setBytes(3, encoded);
            query.setLong(4, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getInt("stripe_id") != stripe
                        || row.getLong("start_seq") != startSequence
                        || row.getLong("reserved_seq") <= startSequence
                        || !Arrays.equals(row.getBytes("payload"), value)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "primary commit lacks matching durable participant prewrite", false, txId);
                }
                return new ParticipantProof(row.getInt("stripe_id"), startSequence,
                        row.getLong("reserved_seq"));
            }
        }
    }

    /**
     * The change key, value, snapshot limit, duration or decision subsection is prohibited after the fixed centre identity.
     *
     * @param row row
     * @param expected expected
     * @throws MvccException if the operation cannot complete
     */
    private void requireSameRegistration(final PrimaryRow row, final Registration expected) {
        if (row == null || row.txId() != expected.txId()
                || !row.primaryKey().equals(expected.primaryKey())
                || !row.secondaryKey().equals(expected.secondaryKey())
                || !Arrays.equals(row.primaryValue(), expected.primaryValue())
                || !Arrays.equals(row.secondaryValue(), expected.secondaryValue())
                || row.primaryStartSequence() != expected.primaryStartSequence()
                || row.secondaryStartSequence() != expected.secondaryStartSequence()
                || row.primaryStripe() != expected.primaryStripe()
                || row.secondaryStripe() != expected.secondaryStripe()
                || row.deadlineMillis() != expected.deadlineMillis()
                || row.decisionStripe() != expected.decisionStripe()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "primary transaction replay changed durable identity", false, expected.txId());
        }
    }

    /**
     * Accurate reading of main transaction rows, decision and aborting the use of an exclusionary lock.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private PrimaryRow readPrimary(final Connection connection, final long txId, final boolean lock) throws SQLException {
        String sql = "select tx_id, primary_key, primary_value, secondary_key, secondary_value,"
                + " primary_stripe, secondary_stripe, primary_start_seq, secondary_start_seq,"
                + " deadline_millis, decision_stripe,"
                + " status, decision_seq, primary_reserved_seq, secondary_reserved_seq"
                + " from bench_primary_txn_v2 where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                return new PrimaryRow(row.getLong("tx_id"),
                        decode(row.getBytes("primary_key")), row.getBytes("primary_value"),
                        decode(row.getBytes("secondary_key")), row.getBytes("secondary_value"),
                        row.getInt("primary_stripe"), row.getInt("secondary_stripe"),
                        row.getLong("primary_start_seq"), row.getLong("secondary_start_seq"),
                        row.getLong("deadline_millis"), row.getInt("decision_stripe"),
                        TxStatus.valueOf(row.getString("status")),
                        nullableLong(row, "decision_seq"),
                        nullableLong(row, "primary_reserved_seq"),
                        nullableLong(row, "secondary_reserved_seq"));
            }
        }
    }

    /**
     * Formal key constraints before JDBC executes verify, avoiding VARBINARY cut-off.
     *
     * @param key business key
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static byte[] encode(final String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > 256
                || !key.equals(new String(bytes, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("invalid primary transaction key");
        }
        return bytes;
    }

    /**
     * Primary shard registered binary key recovery is the original UTF-8 string.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String decode(final byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * But empty decision column cannot misidentify SQL NULL as serial number zero.
     *
     * @param row row
     * @param column column
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Long nullableLong(final ResultSet row, final String column) throws SQLException {
        long value = row.getLong(column);
        return row.wasNull() ? null : value;
    }

    /**
     * The duration is determined by an primary shard database clock, which does not trust the coordinator wall.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long databaseNow(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("primary database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Transaction ID is a zero or decision subsection that refuses access when it exceeds the layout.
     *
     * @param stripe stripe
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void requireStripe(final int stripe) {
        if (stripe < 0 || stripe >= stripes) {
            throw new IllegalArgumentException("invalid primary decision stripe");
        }
    }

    /**
     * Only primary shard durable line judgements are allowed to be read when physical results are uncertain.
     *
     * @param txId transaction identifier
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unknown(final Long txId, final String message, final SQLException cause) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, message, true, txId, cause);
    }

    /**
     * Register identity to fix two key values, the initial line boundary and the main decision.
     *
     * @param txId transaction identifier
     * @param primaryKey primary key
     * @param primaryValue primary value
     * @param secondaryKey secondary key
     * @param secondaryValue secondary value
     * @param primaryStripe primary stripe
     * @param secondaryStripe secondary stripe
     * @param primaryStartSequence primary start sequence
     * @param secondaryStartSequence secondary start sequence
     * @param deadlineMillis deadline millis
     * @param decisionStripe decision stripe
     */
    record Registration(long txId, String primaryKey, byte[] primaryValue,
            String secondaryKey, byte[] secondaryValue,
            int primaryStripe, int secondaryStripe,
            long primaryStartSequence, long secondaryStartSequence,
            long deadlineMillis, int decisionStripe) {

        /**
         * The business bytes are copied when you enter the queue and durable decision identity cannot be modified after the caller.
         *
         * @param txId transaction identifier
         * @param primaryKey primary key
         * @param primaryValue primary value
         * @param secondaryKey secondary key
         * @param secondaryValue secondary value
         * @param primaryStripe primary stripe
         * @param secondaryStripe secondary stripe
         * @param primaryStartSequence primary start sequence
         * @param secondaryStartSequence secondary start sequence
         * @param deadlineMillis deadline millis
         * @param decisionStripe decision stripe
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Registration {
            Objects.requireNonNull(primaryKey, "primaryKey");
            Objects.requireNonNull(secondaryKey, "secondaryKey");
            Objects.requireNonNull(primaryValue, "primaryValue");
            Objects.requireNonNull(secondaryValue, "secondaryValue");
            primaryValue = primaryValue.clone();
            secondaryValue = secondaryValue.clone();
            if (txId == 0 || primaryKey.isBlank() || secondaryKey.isBlank()
                    || primaryStripe < 0 || secondaryStripe < 0
                    || primaryStartSequence < 0 || secondaryStartSequence < 0
                    || deadlineMillis < 1 || decisionStripe < 0) {
                throw new IllegalArgumentException("invalid primary transaction registration");
            }
        }

        /**
         * External reading primary shard payload can only get replica.
         *
         * @return the operation result
         */
        @Override
        public byte[] primaryValue() {
            return primaryValue.clone();
        }

        /**
         * External reading shard payload can only get replica.
         *
         * @return the operation result
         */
        @Override
        public byte[] secondaryValue() {
            return secondaryValue.clone();
        }
    }

    /**
     * Primary shard durable decision with two serial numbers of the irreversible result.
     *
     * @param txId transaction identifier
     * @param status status
     * @param decisionStripe decision stripe
     * @param decisionSequence decision sequence
     * @param primarySequence primary sequence
     * @param secondarySequence secondary sequence
     */
    record Decision(long txId, TxStatus status, int decisionStripe,
            Long decisionSequence, Long primarySequence, Long secondarySequence) {

        /**
         * Only COMMITTED can carry three complete serial numbers at the same time.
         *
         * @param txId transaction identifier
         * @param status status
         * @param decisionStripe decision stripe
         * @param decisionSequence decision sequence
         * @param primarySequence primary sequence
         * @param secondarySequence secondary sequence
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Decision {
            Objects.requireNonNull(status, "status");
            if (txId == 0 || decisionStripe < 0
                    || (status == TxStatus.COMMITTED) != (decisionSequence != null
                            && primarySequence != null && secondarySequence != null)) {
                throw new IllegalArgumentException("invalid primary decision identity");
            }
        }
    }

    /**
     * Each durable participant certificate binds a sub-bar, the threshold and the prewrite serial number.
     *
     * @param stripe stripe
     * @param startSequence start sequence
     * @param reservedSequence reserved sequence
     */
    private record ParticipantProof(int stripe, long startSequence, long reservedSequence) {
    }

    /**
     * Main transaction durable row can be restored to registered identity and terminal state already commit.
     *
     * @param txId transaction identifier
     * @param primaryKey primary key
     * @param primaryValue primary value
     * @param secondaryKey secondary key
     * @param secondaryValue secondary value
     * @param primaryStripe primary stripe
     * @param secondaryStripe secondary stripe
     * @param primaryStartSequence primary start sequence
     * @param secondaryStartSequence secondary start sequence
     * @param deadlineMillis deadline millis
     * @param decisionStripe decision stripe
     * @param status status
     * @param decisionSequence decision sequence
     * @param primarySequence primary sequence
     * @param secondarySequence secondary sequence
     */
    private record PrimaryRow(long txId, String primaryKey, byte[] primaryValue,
            String secondaryKey, byte[] secondaryValue,
            int primaryStripe, int secondaryStripe,
            long primaryStartSequence, long secondaryStartSequence,
            long deadlineMillis, int decisionStripe, TxStatus status,
            Long decisionSequence, Long primarySequence, Long secondarySequence) {

        /**
         * Rereading within the follow-up line lock is still checked against the original registration.
         *
         * @return the operation result
         */
        private Registration registration() {
            return new Registration(txId, primaryKey, primaryValue, secondaryKey,
                    secondaryValue, primaryStripe, secondaryStripe,
                    primaryStartSequence, secondaryStartSequence,
                    deadlineMillis, decisionStripe);
        }

        /**
         * ACTIVE and ABORTED do not have a visible decision serial number.
         *
         * @return the operation result
         */
        private Decision decision() {
            return new Decision(txId, status, decisionStripe,
                    decisionSequence, primarySequence, secondarySequence);
        }
    }
}

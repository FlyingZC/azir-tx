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

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.TxStatus;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * Cross-shard intent presets the local serial number and follows the prototype of the isolation protocol for that serial number when materialization.
 */
final class MysqlStripedCrossVersionPrototype {

    /**
     * The prototype only handles the isolated database, which is visible, and does not save the global status.
     */
    private MysqlStripedCrossVersionPrototype() {
    }

    /**
     * Creates a subsection local version, key lock and cross-shard intent and materialization version sheet.
     *
     * @param dataSource database connection source
     * @param stripes stripes
     * @throws Exception if the operation cannot complete
     */
    static void initializeIsolated(final HikariDataSource dataSource, final int stripes) throws Exception {
        MysqlStripedMvccCapacityProbe.initializeCheckedIsolated(dataSource, stripes);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bench_striped_mvcc_cross_intent");
            statement.execute("""
                    create table if not exists bench_striped_mvcc_cross_intent (
                      user_key varbinary(256) not null primary key,
                      tx_id bigint not null,
                      stripe_id smallint not null,
                      start_seq bigint not null,
                      reserved_seq bigint not null,
                      payload blob not null,
                      unique key cross_intent_tx_key (tx_id, user_key)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_striped_mvcc_cross_abort (
                      user_key varbinary(256) not null,
                      tx_id bigint not null,
                      primary key (user_key, tx_id)
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_striped_mvcc_cross_abort");
            ensurePrewriteProcedure(connection);
            ensureMaterializeProcedure(connection);
            ensureAbortProcedure(connection);
        }
    }

    /**
     * The real shard transaction keeps the serial number in the same key lock and drops the intent with the reserved atomic number.
     *
     * @param dataSource database connection source
     * @param key business key
     * @param value value to write
     * @param stripe stripe
     * @param txId transaction identifier
     * @param startSeq start seq
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static long prewrite(final HikariDataSource dataSource, final String key, final byte[] value,
                         final int stripe, final long txId, final long startSeq) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_prewrite_cross_v3(?, ?, ?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setBytes(2, value);
            call.setInt(3, stripe);
            call.setLong(4, txId);
            call.setLong(5, startSeq);
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("cross prewrite returned no reserved sequence");
                }
                return row.getLong(1);
            }
        }
    }

    /**
     * Local commit also passes the same durable key lock to ensure that intent materialization cannot be written over cross-shard before it is passed.
     *
     * @param dataSource database connection source
     * @param key business key
     * @param value value to write
     * @param stripe stripe
     * @param startSeq start seq
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    static long localCommit(final HikariDataSource dataSource, final String key, final byte[] value,
                            final int stripe, final long startSeq) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_commit_checked_v2(?, ?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setBytes(2, value);
            call.setInt(3, stripe);
            call.setLong(4, startSeq);
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("local commit returned no sequence");
                }
                return row.getLong(1);
            }
        }
    }

    /**
     * Center is not allowed to call until durable decision, materialization follows the prewrite number and releases the same key intent.
     *
     * @param dataSource database connection source
     * @param key business key
     * @param txId transaction identifier
     * @param decisionEpoch decision epoch
     * @param decisions decisions
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static void materialize(final HikariDataSource dataSource, final String key, final long txId,
                            final long decisionEpoch,
                            final MysqlStripedSnapshotOracle.CrossDecisionResolver decisions) throws SQLException {
        Objects.requireNonNull(decisions, "decisions");
        var committed = decisions.committedEpoch(txId);
        if (committed.isEmpty() || committed.getAsLong() != decisionEpoch) {
            throw new IllegalStateException("cross materialization has no matching durable center decision");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_materialize_cross_v2(?, ?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setLong(2, txId);
            call.setLong(3, decisionEpoch);
            call.execute();
        }
    }

    /**
     * Center atomic clears intent after ABORTED decision and leaves tombstone behind, late for prewrite cannot revive transaction.
     *
     * @param dataSource database connection source
     * @param key business key
     * @param txId transaction identifier
     * @param statuses statuses
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static void abort(final HikariDataSource dataSource, final String key, final long txId,
                      final CrossStatusResolver statuses) throws SQLException {
        Objects.requireNonNull(statuses, "statuses");
        if (statuses.status(txId) != TxStatus.ABORTED) {
            throw new IllegalStateException("cross abort has no durable center aborted decision");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_striped_mvcc_abort_cross_v1(?, ?)
                """)) {
            call.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            call.setLong(2, txId);
            call.execute();
        }
    }

    /**
     * Cross-shard prewrite must lock the key first to avoid local commit crossing intent for pending.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private static void ensurePrewriteProcedure(final Connection connection) throws SQLException {
        if (routineExists(connection, "bench_striped_mvcc_prewrite_cross_v3")) {
            return;
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure bench_striped_mvcc_prewrite_cross_v3(
                    in p_key varbinary(256), in p_payload blob, in p_stripe smallint,
                    in p_tx_id bigint, in p_start_seq bigint)
                begin
                  declare v_seq bigint default null;
                  declare v_conflict int default 0;
                  declare v_tx_id bigint default null;
                  declare v_stripe smallint default null;
                  declare v_start_seq bigint default null;
                  declare v_payload blob;
                  declare v_owner bigint default null;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  set transaction isolation level read committed;
                  start transaction;
                  insert into bench_striped_mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select exists(select 1 from bench_striped_mvcc_cross_abort
                                where user_key = p_key and tx_id = p_tx_id)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 36006,
                      message_text = 'STRIPED_MVCC_ABORTED_REPLAY';
                  end if;
                  select tx_id, stripe_id, start_seq, reserved_seq, payload
                    into v_tx_id, v_stripe, v_start_seq, v_seq, v_payload
                    from bench_striped_mvcc_cross_intent
                    where user_key = p_key for update;
                  if v_tx_id is not null then
                    select owner_tx_id into v_owner from bench_striped_mvcc_intent
                      where user_key = p_key;
                    if v_owner is null or v_owner <> v_tx_id then
                      signal sqlstate '45000' set mysql_errno = 36004,
                        message_text = 'STRIPED_MVCC_CROSS_PROOF_MISSING';
                    end if;
                  end if;
                  if v_tx_id is null then
                    select tx_id, stripe_id, start_seq, reserved_seq, payload
                      into v_tx_id, v_stripe, v_start_seq, v_seq, v_payload
                      from bench_striped_mvcc_cross_data
                      where user_key = p_key and tx_id = p_tx_id;
                  end if;
                  if v_tx_id is not null then
                    if v_tx_id <> p_tx_id or v_stripe <> p_stripe
                        or v_start_seq <> p_start_seq or not (v_payload <=> p_payload) then
                      signal sqlstate '45000' set mysql_errno = 36005,
                        message_text = 'STRIPED_MVCC_REPLAY_IDENTITY';
                    end if;
                  else
                    select exists(select 1 from bench_striped_mvcc_intent where user_key = p_key)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 36002,
                        message_text = 'STRIPED_MVCC_INTENT_CONFLICT';
                    end if;
                    select exists(select 1 from bench_striped_mvcc_data
                                  where user_key = p_key and local_seq > p_start_seq)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 36003,
                        message_text = 'STRIPED_MVCC_WRITE_CONFLICT';
                    end if;
                    select exists(select 1 from bench_striped_mvcc_cross_data
                                  where user_key = p_key and reserved_seq > p_start_seq)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 36003,
                        message_text = 'STRIPED_MVCC_WRITE_CONFLICT';
                    end if;
                    update bench_striped_mvcc_state
                      set committed_seq = last_insert_id(committed_seq + 1)
                      where stripe_id = p_stripe;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 36001,
                        message_text = 'STRIPED_MVCC_STATE_MISSING';
                    end if;
                    set v_seq = last_insert_id();
                    insert into bench_striped_mvcc_intent(user_key, owner_tx_id)
                      values (p_key, p_tx_id);
                    insert into bench_striped_mvcc_cross_intent
                      (user_key, tx_id, stripe_id, start_seq, reserved_seq, payload)
                      values (p_key, p_tx_id, p_stripe, p_start_seq, v_seq, p_payload);
                  end if;
                  commit;
                  select v_seq as reserved_seq;
                end
                """)) {
            create.execute();
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /**
     * Materialization transaction locks first, then inserts the original intent serial number into version chain and removes two intent anchor points.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private static void ensureMaterializeProcedure(final Connection connection) throws SQLException {
        if (routineExists(connection, "bench_striped_mvcc_materialize_cross_v2")) {
            return;
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure bench_striped_mvcc_materialize_cross_v2(
                    in p_key varbinary(256), in p_tx_id bigint, in p_decision_epoch bigint)
                begin
                  declare v_seq bigint default null;
                  declare v_stripe smallint default null;
                  declare v_start_seq bigint default null;
                  declare v_payload blob;
                  declare v_owner bigint default null;
                  declare v_epoch bigint default null;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  insert into bench_striped_mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select decision_epoch into v_epoch from bench_striped_mvcc_cross_data
                    where user_key = p_key and tx_id = p_tx_id;
                  if v_epoch is not null then
                    if v_epoch <> p_decision_epoch then
                      signal sqlstate '45000' set mysql_errno = 36005,
                        message_text = 'STRIPED_MVCC_REPLAY_IDENTITY';
                    end if;
                  else
                    select owner_tx_id into v_owner from bench_striped_mvcc_intent
                      where user_key = p_key for update;
                    select reserved_seq, stripe_id, start_seq, payload
                      into v_seq, v_stripe, v_start_seq, v_payload
                      from bench_striped_mvcc_cross_intent
                      where user_key = p_key and tx_id = p_tx_id for update;
                    if v_owner is null or v_owner <> p_tx_id or v_seq is null
                        or p_decision_epoch < 1 then
                      signal sqlstate '45000' set mysql_errno = 36004,
                        message_text = 'STRIPED_MVCC_CROSS_PROOF_MISSING';
                    end if;
                    insert into bench_striped_mvcc_cross_data
                      (user_key, reserved_seq, tx_id, stripe_id, start_seq, decision_epoch, payload)
                      values (p_key, v_seq, p_tx_id, v_stripe, v_start_seq,
                              p_decision_epoch, v_payload);
                    delete from bench_striped_mvcc_cross_intent
                      where user_key = p_key and tx_id = p_tx_id;
                    delete from bench_striped_mvcc_intent
                      where user_key = p_key and owner_tx_id = p_tx_id;
                  end if;
                  commit;
                end
                """)) {
            create.execute();
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /**
     * Durable Abort tombstone and intent clean-up must be in one shard transaction, repeat recovery safe replay.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private static void ensureAbortProcedure(final Connection connection) throws SQLException {
        if (routineExists(connection, "bench_striped_mvcc_abort_cross_v1")) {
            return;
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure bench_striped_mvcc_abort_cross_v1(
                    in p_key varbinary(256), in p_tx_id bigint)
                begin
                  declare v_owner bigint default null;
                  declare v_committed int default 0;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  insert into bench_striped_mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select exists(select 1 from bench_striped_mvcc_cross_data
                                where user_key = p_key and tx_id = p_tx_id)
                    into v_committed;
                  if v_committed = 1 then
                    signal sqlstate '45000' set mysql_errno = 36007,
                      message_text = 'STRIPED_MVCC_COMMITTED_CANNOT_ABORT';
                  end if;
                  select owner_tx_id into v_owner from bench_striped_mvcc_intent
                    where user_key = p_key for update;
                  delete from bench_striped_mvcc_cross_intent
                    where user_key = p_key and tx_id = p_tx_id;
                  if v_owner = p_tx_id then
                    delete from bench_striped_mvcc_intent
                      where user_key = p_key and owner_tx_id = p_tx_id;
                  end if;
                  insert into bench_striped_mvcc_cross_abort(user_key, tx_id)
                    values (p_key, p_tx_id)
                    on duplicate key update tx_id = tx_id;
                  commit;
                end
                """)) {
            create.execute();
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /**
     * Check if the version routine has been installed only in isolation database.
     *
     * @param connection connection
     * @param name name
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean routineExists(final Connection connection, final String name) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = ? limit 1
                """)) {
            query.setString(1, name);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /**
     * Recovery clean-up can only be based on centre durable terminal state and must be thrown if it is not readable.
     */
    @FunctionalInterface
    interface CrossStatusResolver {

        /**
         * Returns the center terminal state of target cross-shard transaction or is still pending status.
         *
         * @param txId transaction identifier
         * @return the operation result
         */
        TxStatus status(long txId);
    }
}

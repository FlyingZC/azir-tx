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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Center has a single key vector intent for COMMITTED at once to call atomic materialization as the real version and keep the serial number.
 */
final class MysqlVectorCrossMaterializeProcedure {

    private final DataSource dataSource;

    private volatile boolean ready;

    /**
     * Use only the same shard DataSource in the bound product participant table.
     *
     * @param dataSource database connection source
     */
    MysqlVectorCrossMaterializeProcedure(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Version was created when the routine is missing and local terminal state can still be numbered replay after restarting.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    void ensure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_vector_commit_intent_one_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                ready = true;
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_vector_commit_intent_one_v1(
                    in p_tx_id bigint, in p_start bigint, in p_commit bigint)
                begin
                  declare v_safe bigint default null;
                  declare v_start bigint default null;
                  declare v_status varchar(16) default null;
                  declare v_commit bigint default null;
                  declare v_intent_count int default 0;
                  declare v_reservation_count int default 0;
                  declare v_committed_count int default 0;
                  declare v_key varbinary(256) default null;
                  declare v_intent_start bigint default null;
                  declare v_deleted tinyint default null;
                  declare v_value blob;
                  declare v_intent_tx bigint default null;
                  declare v_reservation_tx bigint default null;
                  declare v_stripe smallint default null;
                  declare v_start_seq bigint default null;
                  declare v_reserved_seq bigint default null;
                  declare v_reservation_deleted tinyint default null;
                  declare v_reservation_value blob;
                  declare v_map_commit bigint default null;
                  declare v_map_deleted tinyint default null;
                  declare v_map_value blob;
                  declare v_version_tx bigint default null;
                  declare v_version_deleted tinyint default null;
                  declare v_version_value blob;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select start_ts, status, commit_ts into v_start, v_status, v_commit
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_safe is null or v_status is null or v_start <> p_start
                      or p_commit <= p_start then
                    signal sqlstate '45000' set mysql_errno = 39001,
                      message_text = 'VECTOR_MATERIALIZE_PARTICIPANT';
                  end if;
                  if v_status = 'COMMITTED' then
                    if v_commit is null or v_commit <> p_commit then
                      signal sqlstate '45000' set mysql_errno = 39003,
                        message_text = 'VECTOR_MATERIALIZE_REPLAY_TS';
                    end if;
                    select count(*) into v_committed_count from mvcc_vector_committed_v1
                      where tx_id = p_tx_id;
                    select user_key, commit_ts, deleted, payload
                      into v_key, v_map_commit, v_map_deleted, v_map_value
                      from mvcc_vector_committed_v1 where tx_id = p_tx_id limit 1;
                    if v_committed_count <> 1 or v_key is null or v_map_commit <> p_commit then
                      signal sqlstate '45000' set mysql_errno = 39003,
                        message_text = 'VECTOR_MATERIALIZE_REPLAY_MAP';
                    end if;
                    select tx_id, deleted, value
                      into v_version_tx, v_version_deleted, v_version_value
                      from mvcc_data where user_key = v_key and commit_ts = p_commit;
                    if v_version_tx is null or v_version_tx <> p_tx_id
                        or v_version_deleted <> v_map_deleted
                        or not (v_version_value <=> v_map_value) then
                      signal sqlstate '45000' set mysql_errno = 39003,
                        message_text = 'VECTOR_MATERIALIZE_REPLAY_VERSION';
                    end if;
                  elseif v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 39004,
                      message_text = 'VECTOR_MATERIALIZE_ABORTED';
                  elseif v_status in ('ACTIVE', 'PREPARING') and v_commit is null then
                    select count(*) into v_intent_count from mvcc_intent where tx_id = p_tx_id;
                    select user_key into v_key from mvcc_intent where tx_id = p_tx_id limit 1;
                    if v_intent_count <> 1 or v_key is null then
                      signal sqlstate '45000' set mysql_errno = 39002,
                        message_text = 'VECTOR_MATERIALIZE_INTENT_MISSING';
                    end if;
                    -- 真实意向只作键发现；加锁验证使用当前读视图。
                    insert into mvcc_key_lock(user_key) values (v_key)
                      on duplicate key update user_key = user_key;
                    select tx_id, start_ts, deleted, value
                      into v_intent_tx, v_intent_start, v_deleted, v_value
                      from mvcc_intent where user_key = v_key for update;
                    select count(*) into v_reservation_count
                      from mvcc_vector_intent_reservation_v1 where tx_id = p_tx_id;
                    select tx_id, stripe_id, start_seq, reserved_seq, deleted, payload
                      into v_reservation_tx, v_stripe, v_start_seq, v_reserved_seq,
                           v_reservation_deleted, v_reservation_value
                      from mvcc_vector_intent_reservation_v1
                      where user_key = v_key for update;
                    if v_intent_tx is null or v_intent_tx <> p_tx_id
                        or v_intent_start <> p_start or v_reservation_count <> 1
                        or v_reservation_tx is null or v_reservation_tx <> p_tx_id
                        or v_reserved_seq <= v_start_seq
                        or v_deleted <> v_reservation_deleted
                        or not (v_value <=> v_reservation_value) then
                      signal sqlstate '45000' set mysql_errno = 39002,
                        message_text = 'VECTOR_MATERIALIZE_PROOF_MISMATCH';
                    end if;
                    insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                      values (v_key, p_commit, p_tx_id, v_deleted, v_value);
                    insert into mvcc_vector_committed_v1
                      (user_key, reserved_seq, tx_id, stripe_id, start_seq, commit_ts, deleted, payload)
                      values (v_key, v_reserved_seq, p_tx_id, v_stripe,
                              v_start_seq, p_commit, v_deleted, v_value);
                    update mvcc_participant_txn set status = 'COMMITTED', commit_ts = p_commit,
                      updated_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                      where tx_id = p_tx_id and start_ts = p_start
                        and status in ('ACTIVE', 'PREPARING') and commit_ts is null;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 39001,
                        message_text = 'VECTOR_MATERIALIZE_TERMINAL_LOST';
                    end if;
                    delete from mvcc_intent where user_key = v_key and tx_id = p_tx_id;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 39002,
                        message_text = 'VECTOR_MATERIALIZE_INTENT_LOST';
                    end if;
                    delete from mvcc_vector_intent_reservation_v1
                      where user_key = v_key and tx_id = p_tx_id;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 39002,
                        message_text = 'VECTOR_MATERIALIZE_RESERVATION_LOST';
                    end if;
                  else
                    signal sqlstate '45000' set mysql_errno = 39001,
                      message_text = 'VECTOR_MATERIALIZE_STATUS_INVALID';
                  end if;
                  commit;
                end
                """)) {
            create.execute();
            ready = true;
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
            ready = true;
        }
    }

    /**
     * Only Centre commit proof already verify calls the server atomic materialization routine.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    void materialize(final long txId, final long startTs, final long commitTs) throws SQLException {
        if (!ready) {
            throw new IllegalStateException("vector materialize procedure is not initialized");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call mvcc_vector_commit_intent_one_v1(?, ?, ?)
                """)) {
            if (!connection.getAutoCommit()) {
                throw new SQLException("vector materialization requires auto-commit connection");
            }
            call.setLong(1, txId);
            call.setLong(2, startTs);
            call.setLong(3, commitTs);
            call.execute();
        } catch (final SQLException ex) {
            // The old routine still requires large payload version; only compact replay receipt saved by shard atomic will be recognized after decommissioning.
            if (ex.getErrorCode() == 39003
                    && MysqlVectorRetiredVersionStore.matches(dataSource, txId, startTs, commitTs)) {
                return;
            }
            throw ex;
        }
    }
}

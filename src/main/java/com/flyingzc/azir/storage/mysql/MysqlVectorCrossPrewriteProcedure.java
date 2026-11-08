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

import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Formal cross-shard single key prewrite saves real intent and strip serial numbers in atomic on a single server call.
 */
final class MysqlVectorCrossPrewriteProcedure {

    private final DataSource dataSource;

    private volatile boolean ready;

    /**
     * The prewrite routine must operate the product participant in the same physical database as the vector table.
     *
     * @param dataSource database connection source
     */
    MysqlVectorCrossPrewriteProcedure(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Install version routines when they are missing, restart the original durable intent and book watermark.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    void ensure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_vector_prewrite_one_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                ready = true;
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_vector_prewrite_one_v1(
                    in p_tx_id bigint, in p_start bigint, in p_key varbinary(256),
                    in p_deleted tinyint, in p_value blob, in p_expire_at bigint,
                    in p_stripe smallint, in p_start_seq bigint)
                begin
                  declare v_safe bigint default null;
                  declare v_stripes smallint default null;
                  declare v_record_start bigint default null;
                  declare v_record_status varchar(16) default null;
                  declare v_newer_scalar int default 0;
                  declare v_newer_local int default 0;
                  declare v_newer_cross int default 0;
                  declare v_cross_drift int default 0;
                  declare v_local_drift int default 0;
                  declare v_intent_tx bigint default null;
                  declare v_intent_start bigint default null;
                  declare v_intent_deleted tinyint default null;
                  declare v_intent_value blob;
                  declare v_reservation_tx bigint default null;
                  declare v_reservation_stripe smallint default null;
                  declare v_reservation_start bigint default null;
                  declare v_reservation_seq bigint default null;
                  declare v_reservation_deleted tinyint default null;
                  declare v_reservation_value blob;
                  declare v_rolled int default 0;
                  declare v_seq bigint default null;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select stripe_count into v_stripes from mvcc_vector_layout_v1
                    where singleton_id = 1 for share;
                  if v_safe is null or v_stripes is null or p_tx_id = 0
                      or p_start < 0 or p_key is null or length(p_key) < 1
                      or length(p_key) > 256 or p_stripe < 0 or p_stripe >= v_stripes
                      or p_start_seq < 0 or p_expire_at < 1 then
                    signal sqlstate '45000' set mysql_errno = 38001,
                      message_text = 'VECTOR_PREWRITE_IDENTITY';
                  end if;
                  insert into mvcc_participant_txn(tx_id, start_ts, status, commit_ts, updated_at)
                    values (p_tx_id, p_start, 'ACTIVE', null,
                            cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                    on duplicate key update tx_id = tx_id;
                  select start_ts, status into v_record_start, v_record_status
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_record_status <> 'ACTIVE' then
                    signal sqlstate '45000' set mysql_errno = 38002,
                      message_text = 'VECTOR_PREWRITE_PARTICIPANT_TERMINAL';
                  end if;
                  if v_record_start <> p_start then
                    signal sqlstate '45000' set mysql_errno = 38003,
                      message_text = 'VECTOR_PREWRITE_START_MISMATCH';
                  end if;
                  if p_start < v_safe then
                    signal sqlstate '45000' set mysql_errno = 38008,
                      message_text = 'VECTOR_PREWRITE_SNAPSHOT_TOO_OLD';
                  end if;
                  -- 先锁稳定键行，后续普通读视图不能早于同键前驱的物理提交。
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select
                    exists(select 1 from mvcc_data
                           where user_key = p_key and commit_ts > p_start),
                    exists(select 1 from mvcc_vector_local_data_v1
                           where user_key = p_key and local_seq > p_start_seq),
                    exists(select 1 from mvcc_vector_committed_v1
                           where user_key = p_key and reserved_seq > p_start_seq),
                    exists(select 1 from mvcc_vector_committed_v1
                           where user_key = p_key and stripe_id <> p_stripe),
                    exists(select 1 from mvcc_vector_local_data_v1
                           where user_key = p_key and stripe_id <> p_stripe)
                    into v_newer_scalar, v_newer_local, v_newer_cross,
                         v_cross_drift, v_local_drift;
                  if v_cross_drift = 1 or v_local_drift = 1 then
                    signal sqlstate '45000' set mysql_errno = 38004,
                      message_text = 'VECTOR_PREWRITE_STRIPE_DRIFT';
                  end if;
                  if v_newer_scalar = 1 or v_newer_local = 1 or v_newer_cross = 1 then
                    signal sqlstate '45000' set mysql_errno = 38005,
                      message_text = 'VECTOR_PREWRITE_NEWER_VERSION';
                  end if;
                  select tx_id, start_ts, deleted, value
                    into v_intent_tx, v_intent_start, v_intent_deleted, v_intent_value
                    from mvcc_intent where user_key = p_key;
                  if v_intent_tx is not null and v_intent_tx <> p_tx_id then
                    signal sqlstate '45000' set mysql_errno = 38006,
                      message_text = 'VECTOR_PREWRITE_FOREIGN_INTENT';
                  end if;
                  if v_intent_tx is not null
                      and (v_intent_start <> p_start or v_intent_deleted <> p_deleted
                           or not (v_intent_value <=> p_value)) then
                    signal sqlstate '45000' set mysql_errno = 38003,
                      message_text = 'VECTOR_PREWRITE_INTENT_REPLAY';
                  end if;
                  select exists(select 1 from mvcc_rollback
                                where user_key = p_key and tx_id = p_tx_id)
                    into v_rolled;
                  if v_rolled = 1 then
                    signal sqlstate '45000' set mysql_errno = 38007,
                      message_text = 'VECTOR_PREWRITE_ROLLBACK_MARKER';
                  end if;
                  select tx_id, stripe_id, start_seq, reserved_seq, deleted, payload
                    into v_reservation_tx, v_reservation_stripe, v_reservation_start,
                         v_reservation_seq, v_reservation_deleted, v_reservation_value
                    from mvcc_vector_intent_reservation_v1 where user_key = p_key;
                  if v_reservation_tx is not null then
                    if v_reservation_tx <> p_tx_id or v_reservation_stripe <> p_stripe
                        or v_reservation_start <> p_start_seq
                        or v_reservation_deleted <> p_deleted
                        or not (v_reservation_value <=> p_value)
                        or v_intent_tx is null then
                      signal sqlstate '45000' set mysql_errno = 38003,
                        message_text = 'VECTOR_PREWRITE_RESERVATION_REPLAY';
                    end if;
                    set v_seq = v_reservation_seq;
                  else
                    update mvcc_vector_stripe_state_v1
                      set committed_seq = last_insert_id(committed_seq + 1)
                      where stripe_id = p_stripe;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 38001,
                        message_text = 'VECTOR_PREWRITE_STRIPE_MISSING';
                    end if;
                    set v_seq = last_insert_id();
                    if v_seq <= p_start_seq then
                      signal sqlstate '45000' set mysql_errno = 38005,
                        message_text = 'VECTOR_PREWRITE_START_AHEAD';
                    end if;
                    insert into mvcc_vector_intent_reservation_v1
                      (user_key, tx_id, stripe_id, start_seq, reserved_seq, deleted, payload)
                    values (p_key, p_tx_id, p_stripe, p_start_seq, v_seq, p_deleted, p_value);
                  end if;
                  insert into mvcc_intent(user_key, tx_id, start_ts, deleted, value, expire_at)
                    values (p_key, p_tx_id, p_start, p_deleted, p_value, p_expire_at)
                    on duplicate key update expire_at = greatest(expire_at, values(expire_at));
                  commit;
                  select v_seq as reserved_seq;
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
     * JDBC autocommit connection returns only the real local serial number after durable prewrite.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     * @param expireAtMillis expire at millis
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    long prewrite(final long txId, final long startTs, final Mutation mutation, final int stripe,
                  final long startSequence, final long expireAtMillis) throws SQLException {
        if (!ready) {
            throw new IllegalStateException("vector cross prewrite procedure is not initialized");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call mvcc_vector_prewrite_one_v1(?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            if (!connection.getAutoCommit()) {
                throw new SQLException("vector cross prewrite requires auto-commit connection");
            }
            call.setLong(1, txId);
            call.setLong(2, startTs);
            call.setBytes(3, mutation.key().getBytes(StandardCharsets.UTF_8));
            call.setBoolean(4, mutation.type() == MutationType.DELETE);
            call.setBytes(5, mutation.value());
            call.setLong(6, expireAtMillis);
            call.setInt(7, stripe);
            call.setLong(8, startSequence);
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("vector cross prewrite returned no reserved sequence");
                }
                return row.getLong(1);
            }
        }
    }
}

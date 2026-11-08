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
import com.flyingzc.azir.storage.Utf8KeyOrder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Multiple separate cross-shard transaction atomic in one shard transaction writing real intent and retention serial numbers.
 */
final class MysqlVectorCrossPrewriteBatchProcedure {

    private final DataSource dataSource;

    private volatile boolean ready;

    /**
     * The same batch is only within a single physical shard database.
     *
     * @param dataSource database connection source
     */
    MysqlVectorCrossPrewriteBatchProcedure(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Only install version routines, restarting will not clear existing intent or watermark.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    void ensure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_vector_prewrite_batch_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                ready = true;
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_vector_prewrite_batch_v1(in p_batch json)
                begin
                  declare v_safe bigint default null;
                  declare v_stripes smallint default null;
                  declare v_count int default null;
                  declare v_index int default 0;
                  declare v_path varchar(32);
                  declare v_tx_id bigint default null;
                  declare v_start bigint default null;
                  declare v_expire bigint default null;
                  declare v_key_raw blob;
                  declare v_key varbinary(256);
                  declare v_payload_raw longblob;
                  declare v_payload blob;
                  declare v_deleted tinyint default null;
                  declare v_stripe smallint default null;
                  declare v_start_seq bigint default null;
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
                  declare v_result json default json_array();
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  set v_count = json_length(p_batch);
                  if v_count is null or v_count < 1 or v_count > 64 then
                    signal sqlstate '45000' set mysql_errno = 38001,
                      message_text = 'VECTOR_PREWRITE_BATCH_SIZE';
                  end if;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select stripe_count into v_stripes from mvcc_vector_layout_v1
                    where singleton_id = 1 for share;
                  if v_safe is null or v_stripes is null then
                    signal sqlstate '45000' set mysql_errno = 38001,
                      message_text = 'VECTOR_PREWRITE_BATCH_LAYOUT';
                  end if;
                  while v_index < v_count do
                    set v_path = concat('$[', v_index, ']');
                    set v_tx_id = cast(json_unquote(json_extract(p_batch, concat(v_path, '.tx'))) as signed);
                    set v_start = cast(json_unquote(json_extract(p_batch, concat(v_path, '.startTs'))) as signed);
                    set v_expire = cast(json_unquote(json_extract(p_batch, concat(v_path, '.expire'))) as signed);
                    set v_key_raw = from_base64(json_unquote(json_extract(p_batch, concat(v_path, '.key'))));
                    set v_deleted = cast(json_unquote(json_extract(p_batch, concat(v_path, '.deleted'))) as signed);
                    set v_stripe = cast(json_unquote(json_extract(p_batch, concat(v_path, '.stripe'))) as signed);
                    set v_start_seq = cast(json_unquote(json_extract(p_batch, concat(v_path, '.startSeq'))) as signed);
                    set v_payload_raw = from_base64(json_unquote(json_extract(p_batch, concat(v_path, '.value'))));
                    if v_deleted = 1
                        or json_type(json_extract(p_batch, concat(v_path, '.value'))) = 'NULL' then
                      set v_payload = null;
                    else
                      set v_payload = v_payload_raw;
                    end if;
                    if v_tx_id is null or v_tx_id = 0 or v_start is null or v_start < 0
                        or v_expire is null or v_expire < 1
                        or v_key_raw is null or length(v_key_raw) < 1 or length(v_key_raw) > 256
                        or v_deleted not in (0, 1) or v_stripe is null
                        or v_stripe < 0 or v_stripe >= v_stripes
                        or v_start_seq is null or v_start_seq < 0
                        or json_contains_path(p_batch, 'one', concat(v_path, '.value')) <> 1
                        or (v_payload_raw is not null and length(v_payload_raw) > 65535) then
                      signal sqlstate '45000' set mysql_errno = 38001,
                        message_text = 'VECTOR_PREWRITE_BATCH_IDENTITY';
                    end if;
                    set v_key = v_key_raw;
                    insert into mvcc_participant_txn(tx_id, start_ts, status, commit_ts, updated_at)
                      values (v_tx_id, v_start, 'ACTIVE', null,
                              cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                      on duplicate key update tx_id = tx_id;
                    select start_ts, status into v_record_start, v_record_status
                      from mvcc_participant_txn where tx_id = v_tx_id for update;
                    if v_record_status <> 'ACTIVE' then
                      signal sqlstate '45000' set mysql_errno = 38002,
                        message_text = 'VECTOR_PREWRITE_BATCH_TERMINAL';
                    end if;
                    if v_record_start <> v_start then
                      signal sqlstate '45000' set mysql_errno = 38003,
                        message_text = 'VECTOR_PREWRITE_BATCH_START';
                    end if;
                    if v_start < v_safe then
                      signal sqlstate '45000' set mysql_errno = 38008,
                        message_text = 'VECTOR_PREWRITE_BATCH_SNAPSHOT_OLD';
                    end if;
                    -- 输入先按分条再按二进制键排序，稳定键锁早于普通冲突读取。
                    insert into mvcc_key_lock(user_key) values (v_key)
                      on duplicate key update user_key = user_key;
                    select
                      exists(select 1 from mvcc_data
                             where user_key = v_key and commit_ts > v_start),
                      exists(select 1 from mvcc_vector_local_data_v1
                             where user_key = v_key and local_seq > v_start_seq),
                      exists(select 1 from mvcc_vector_committed_v1
                             where user_key = v_key and reserved_seq > v_start_seq),
                      exists(select 1 from mvcc_vector_committed_v1
                             where user_key = v_key and stripe_id <> v_stripe),
                      exists(select 1 from mvcc_vector_local_data_v1
                             where user_key = v_key and stripe_id <> v_stripe)
                      into v_newer_scalar, v_newer_local, v_newer_cross,
                           v_cross_drift, v_local_drift;
                    if v_cross_drift = 1 or v_local_drift = 1 then
                      signal sqlstate '45000' set mysql_errno = 38004,
                        message_text = 'VECTOR_PREWRITE_BATCH_DRIFT';
                    end if;
                    if v_newer_scalar = 1 or v_newer_local = 1 or v_newer_cross = 1 then
                      signal sqlstate '45000' set mysql_errno = 38005,
                        message_text = 'VECTOR_PREWRITE_BATCH_NEWER';
                    end if;
                    set v_intent_tx = null;
                    set v_reservation_tx = null;
                    select tx_id, start_ts, deleted, value
                      into v_intent_tx, v_intent_start, v_intent_deleted, v_intent_value
                      from mvcc_intent where user_key = v_key;
                    if v_intent_tx is not null and v_intent_tx <> v_tx_id then
                      signal sqlstate '45000' set mysql_errno = 38006,
                        message_text = 'VECTOR_PREWRITE_BATCH_FOREIGN_INTENT';
                    end if;
                    if v_intent_tx is not null
                        and (v_intent_start <> v_start or v_intent_deleted <> v_deleted
                             or not (v_intent_value <=> v_payload)) then
                      signal sqlstate '45000' set mysql_errno = 38003,
                        message_text = 'VECTOR_PREWRITE_BATCH_INTENT_REPLAY';
                    end if;
                    select exists(select 1 from mvcc_rollback
                                  where user_key = v_key and tx_id = v_tx_id)
                      into v_rolled;
                    if v_rolled = 1 then
                      signal sqlstate '45000' set mysql_errno = 38007,
                        message_text = 'VECTOR_PREWRITE_BATCH_ROLLBACK';
                    end if;
                    select tx_id, stripe_id, start_seq, reserved_seq, deleted, payload
                      into v_reservation_tx, v_reservation_stripe, v_reservation_start,
                           v_reservation_seq, v_reservation_deleted, v_reservation_value
                      from mvcc_vector_intent_reservation_v1 where user_key = v_key;
                    if v_reservation_tx is not null then
                      if v_reservation_tx <> v_tx_id or v_reservation_stripe <> v_stripe
                          or v_reservation_start <> v_start_seq
                          or v_reservation_deleted <> v_deleted
                          or not (v_reservation_value <=> v_payload)
                          or v_intent_tx is null then
                        signal sqlstate '45000' set mysql_errno = 38003,
                          message_text = 'VECTOR_PREWRITE_BATCH_RESERVATION_REPLAY';
                      end if;
                      set v_seq = v_reservation_seq;
                    else
                      update mvcc_vector_stripe_state_v1
                        set committed_seq = last_insert_id(committed_seq + 1)
                        where stripe_id = v_stripe;
                      if row_count() <> 1 then
                        signal sqlstate '45000' set mysql_errno = 38001,
                          message_text = 'VECTOR_PREWRITE_BATCH_STRIPE';
                      end if;
                      set v_seq = last_insert_id();
                      if v_seq <= v_start_seq then
                        signal sqlstate '45000' set mysql_errno = 38005,
                          message_text = 'VECTOR_PREWRITE_BATCH_START_AHEAD';
                      end if;
                      insert into mvcc_vector_intent_reservation_v1
                        (user_key, tx_id, stripe_id, start_seq, reserved_seq, deleted, payload)
                      values (v_key, v_tx_id, v_stripe, v_start_seq, v_seq, v_deleted, v_payload);
                    end if;
                    insert into mvcc_intent(user_key, tx_id, start_ts, deleted, value, expire_at)
                      values (v_key, v_tx_id, v_start, v_deleted, v_payload, v_expire)
                      on duplicate key update expire_at = greatest(expire_at, values(expire_at));
                    set v_result = json_array_append(v_result, '$', v_seq);
                    set v_index = v_index + 1;
                  end while;
                  commit;
                  select v_result as reserved_sequences;
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
     * The fixed locking order of the rows and keys does not change the caller 's request order and the verb identity.
     *
     * @param requests requests
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws NumberFormatException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    List<Long> prewrite(final List<MysqlStorageAdapter.VectorPrewriteRequest> requests) throws SQLException {
        if (!ready || requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid vector cross prewrite batch");
        }
        List<IndexedRequest> ordered = IntStream.range(0, requests.size())
                .mapToObj(index -> new IndexedRequest(index, requests.get(index)))
                .sorted(Comparator.comparingInt((IndexedRequest item) -> item.request().stripe())
                        .thenComparing(item -> item.request().mutation().key(), Utf8KeyOrder::compare))
                .toList();
        StringBuilder json = new StringBuilder(ordered.size() * 150);
        Base64.Encoder base64 = Base64.getEncoder();
        json.append('[');
        for (IndexedRequest item : ordered) {
            if (json.length() > 1) {
                json.append(',');
            }
            var request = item.request();
            json.append("{\"tx\":").append(request.txId())
                    .append(",\"startTs\":").append(request.startTs())
                    .append(",\"expire\":").append(request.expireAtMillis())
                    .append(",\"key\":\"")
                    .append(base64.encodeToString(Utf8KeyOrder.encode(request.mutation().key())))
                    .append("\",\"value\":");
            if (request.mutation().value() == null) {
                json.append("null");
            } else {
                json.append('"').append(base64.encodeToString(request.mutation().value())).append('"');
            }
            json.append(",\"deleted\":")
                    .append(request.mutation().type() == MutationType.DELETE ? 1 : 0)
                    .append(",\"stripe\":").append(request.stripe())
                    .append(",\"startSeq\":").append(request.startSequence()).append('}');
        }
        json.append(']');
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call mvcc_vector_prewrite_batch_v1(?)
                """)) {
            if (!connection.getAutoCommit()) {
                throw new SQLException("vector cross prewrite batch requires auto-commit connection");
            }
            call.setString(1, json.toString());
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("vector cross prewrite batch returned no sequences");
                }
                String encoded = row.getString(1);
                if (encoded == null || encoded.length() < 3 || encoded.charAt(0) != '['
                        || encoded.charAt(encoded.length() - 1) != ']') {
                    throw new SQLException("vector cross prewrite batch returned malformed sequences");
                }
                String[] parts = encoded.substring(1, encoded.length() - 1).split(",");
                if (parts.length != ordered.size()) {
                    throw new SQLException("vector cross prewrite batch returned incomplete results");
                }
                Long[] original = new Long[ordered.size()];
                try {
                    for (int index = 0; index < ordered.size(); index++) {
                        long sequence = Long.parseLong(parts[index].trim());
                        if (sequence < 1) {
                            throw new NumberFormatException("nonpositive prewrite sequence");
                        }
                        original[ordered.get(index).index()] = sequence;
                    }
                } catch (final NumberFormatException ex) {
                    throw new SQLException("vector cross prewrite batch returned invalid sequence", ex);
                }
                return List.of(original);
            }
        }
    }

    /**
     * Keep the original position after sorting to ensure that the centre receipt corresponds to the original transaction ID.
     *
     * @param index index
     * @param request request
     */
    private record IndexedRequest(int index, MysqlStorageAdapter.VectorPrewriteRequest request) {
    }
}

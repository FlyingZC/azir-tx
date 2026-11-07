package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.storage.Utf8KeyOrder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/** 多笔独立本地逻辑事务在一个 MySQL 事务内完成校验、取号和持久落盘。 */
final class MysqlVectorLocalBatchProcedure {
    private final DataSource dataSource;
    private volatile boolean ready;

    /** 后台组批与参与者版本必须使用同一物理数据库。 */
    MysqlVectorLocalBatchProcedure(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** 版本化例程只在缺失时创建，重复初始化保留已提交本地版本。 */
    void ensure(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_vector_local_commit_batch_v2' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                ready = true;
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_vector_local_commit_batch_v2(in p_batch json)
                proc: begin
                  declare v_safe bigint default null;
                  declare v_stripes smallint default null;
                  declare v_count int default null;
                  declare v_index int default 0;
                  declare v_path varchar(32);
                  declare v_tx_id bigint default null;
                  declare v_key varbinary(256) default null;
                  declare v_key_raw blob;
                  declare v_payload blob;
                  declare v_payload_raw longblob;
                  declare v_deleted tinyint default null;
                  declare v_stripe smallint default null;
                  declare v_start_seq bigint default null;
                  declare v_existing_key varbinary(256) default null;
                  declare v_existing_stripe smallint default null;
                  declare v_existing_start bigint default null;
                  declare v_existing_seq bigint default null;
                  declare v_existing_deleted tinyint default null;
                  declare v_existing_payload blob;
                  declare v_intent int default 0;
                  declare v_reservation int default 0;
                  declare v_cross_drift int default 0;
                  declare v_local_drift int default 0;
                  declare v_newer_cross int default 0;
                  declare v_newer_local int default 0;
                  declare v_scalar int default 0;
                  declare v_rollback int default 0;
                  declare v_seq bigint default null;
                  declare v_result json default json_array();
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  set v_count = json_length(p_batch);
                  if v_count is null or v_count < 1 or v_count > 64 then
                    signal sqlstate '45000' set mysql_errno = 37001,
                      message_text = 'VECTOR_LOCAL_BATCH_SIZE';
                  end if;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select stripe_count into v_stripes from mvcc_vector_layout_v1
                    where singleton_id = 1 for share;
                  if v_safe is null or v_stripes is null then
                    signal sqlstate '45000' set mysql_errno = 37001,
                      message_text = 'VECTOR_LOCAL_LAYOUT_MISSING';
                  end if;
                  while v_index < v_count do
                    set v_path = concat('$[', v_index, ']');
                    set v_tx_id = cast(json_unquote(json_extract(p_batch, concat(v_path, '.tx'))) as signed);
                    set v_key_raw = from_base64(json_unquote(json_extract(p_batch, concat(v_path, '.key'))));
                    set v_deleted = cast(json_unquote(json_extract(p_batch, concat(v_path, '.deleted'))) as signed);
                    set v_stripe = cast(json_unquote(json_extract(p_batch, concat(v_path, '.stripe'))) as signed);
                    set v_start_seq = cast(json_unquote(json_extract(p_batch, concat(v_path, '.start'))) as signed);
                    set v_payload_raw = from_base64(json_unquote(json_extract(p_batch, concat(v_path, '.value'))));
                    if v_deleted = 1
                        or json_type(json_extract(p_batch, concat(v_path, '.value'))) = 'NULL' then
                      set v_payload = null;
                    else
                      set v_payload = v_payload_raw;
                    end if;
                    if v_tx_id is null or v_tx_id >= 0 or v_key_raw is null
                        or length(v_key_raw) < 1 or length(v_key_raw) > 256
                        or v_deleted not in (0, 1) or v_stripe is null
                        or v_stripe < 0 or v_stripe >= v_stripes
                        or v_start_seq is null or v_start_seq < 0
                        or json_contains_path(p_batch, 'one', concat(v_path, '.value')) <> 1
                        or (v_deleted = 0 and v_payload_raw is null
                            and json_type(json_extract(p_batch, concat(v_path, '.value'))) <> 'NULL')
                        or (v_payload_raw is not null and length(v_payload_raw) > 65535) then
                      signal sqlstate '45000' set mysql_errno = 37001,
                        message_text = 'VECTOR_LOCAL_BATCH_IDENTITY';
                    end if;
                    set v_key = v_key_raw;
                    -- 调用者按分条及二进制键排序，先锁稳定键行再作普通一致读。
                    insert into mvcc_key_lock(user_key) values (v_key)
                      on duplicate key update user_key = user_key;
                    set v_existing_key = null;
                    set v_existing_seq = null;
                    select user_key, stripe_id, start_seq, local_seq, deleted, payload
                      into v_existing_key, v_existing_stripe, v_existing_start,
                           v_existing_seq, v_existing_deleted, v_existing_payload
                      from mvcc_vector_local_data_v1 where tx_id = v_tx_id;
                    if v_existing_key is not null then
                      if not (v_existing_key <=> v_key) or v_existing_stripe <> v_stripe
                          or v_existing_start <> v_start_seq
                          or v_existing_deleted <> v_deleted
                          or not (v_existing_payload <=> v_payload) then
                        signal sqlstate '45000' set mysql_errno = 37004,
                          message_text = 'VECTOR_LOCAL_BATCH_REPLAY';
                      end if;
                      set v_seq = v_existing_seq;
                    else
                      select
                        exists(select 1 from mvcc_intent where user_key = v_key),
                        exists(select 1 from mvcc_vector_intent_reservation_v1 where user_key = v_key),
                        exists(select 1 from mvcc_vector_committed_v1
                               where user_key = v_key and stripe_id <> v_stripe),
                        exists(select 1 from mvcc_vector_local_data_v1
                               where user_key = v_key and stripe_id <> v_stripe),
                        exists(select 1 from mvcc_vector_committed_v1
                               where user_key = v_key and reserved_seq > v_start_seq),
                        exists(select 1 from mvcc_vector_local_data_v1
                               where user_key = v_key and local_seq > v_start_seq),
                        exists(select 1 from mvcc_data d where d.user_key = v_key
                               and not exists(select 1 from mvcc_vector_committed_v1 c
                                              where c.user_key = d.user_key and c.tx_id = d.tx_id
                                                and c.commit_ts = d.commit_ts)),
                        exists(select 1 from mvcc_rollback
                               where user_key = v_key and tx_id = v_tx_id)
                        into v_intent, v_reservation, v_cross_drift, v_local_drift,
                             v_newer_cross, v_newer_local, v_scalar, v_rollback;
                      if v_cross_drift = 1 or v_local_drift = 1 or v_scalar = 1 then
                        signal sqlstate '45000' set mysql_errno = 37004,
                          message_text = 'VECTOR_LOCAL_BATCH_HISTORY';
                      end if;
                      if v_intent = 1 or v_reservation = 1 then
                        signal sqlstate '45000' set mysql_errno = 37002,
                          message_text = 'VECTOR_LOCAL_BATCH_INTENT';
                      end if;
                      if v_newer_cross = 1 or v_newer_local = 1 then
                        signal sqlstate '45000' set mysql_errno = 37003,
                          message_text = 'VECTOR_LOCAL_BATCH_NEWER';
                      end if;
                      if v_rollback = 1 then
                        signal sqlstate '45000' set mysql_errno = 37005,
                          message_text = 'VECTOR_LOCAL_BATCH_ROLLBACK';
                      end if;
                      update mvcc_vector_stripe_state_v1
                        set committed_seq = last_insert_id(committed_seq + 1)
                        where stripe_id = v_stripe;
                      if row_count() <> 1 then
                        signal sqlstate '45000' set mysql_errno = 37001,
                          message_text = 'VECTOR_LOCAL_BATCH_STRIPE';
                      end if;
                      set v_seq = last_insert_id();
                      if v_seq <= v_start_seq then
                        signal sqlstate '45000' set mysql_errno = 37003,
                          message_text = 'VECTOR_LOCAL_BATCH_START_AHEAD';
                      end if;
                      insert into mvcc_vector_local_data_v1
                        (user_key, local_seq, tx_id, stripe_id, start_seq, deleted, payload)
                        values (v_key, v_seq, v_tx_id, v_stripe, v_start_seq, v_deleted, v_payload);
                    end if;
                    set v_result = json_array_append(v_result, '$', v_seq);
                    set v_index = v_index + 1;
                  end while;
                  commit;
                  select v_result as local_sequences;
                end
                """)) {
            create.execute();
            ready = true;
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
            ready = true;
        }
    }

    /** 先按分条再按二进制键排序，统一分条行与同键锁的跨批次获取顺序。 */
    List<Long> commit(List<MysqlStorageAdapter.LocalVectorCommitRequest> requests) throws SQLException {
        if (!ready || requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid vector local server batch");
        }
        List<IndexedRequest> ordered = IntStream.range(0, requests.size())
                .mapToObj(index -> new IndexedRequest(index, requests.get(index)))
                .sorted(Comparator.comparingInt((IndexedRequest item) -> item.request().stripe())
                        .thenComparing(item -> item.request().mutation().key(), Utf8KeyOrder::compare))
                .toList();
        StringBuilder json = new StringBuilder(ordered.size() * 128);
        json.append('[');
        Base64.Encoder base64 = Base64.getEncoder();
        for (IndexedRequest item : ordered) {
            if (json.length() > 1) {
                json.append(',');
            }
            var request = item.request();
            byte[] key = Utf8KeyOrder.encode(request.mutation().key());
            json.append("{\"tx\":").append(request.txId())
                    .append(",\"key\":\"").append(base64.encodeToString(key))
                    .append("\",\"value\":");
            if (request.mutation().value() == null) {
                json.append("null");
            } else {
                json.append('"').append(base64.encodeToString(request.mutation().value())).append('"');
            }
            json.append(",\"deleted\":")
                    .append(request.mutation().type() == MutationType.DELETE ? 1 : 0)
                    .append(",\"stripe\":").append(request.stripe())
                    .append(",\"start\":").append(request.startSequence()).append('}');
        }
        json.append(']');
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call mvcc_vector_local_commit_batch_v2(?)
                """)) {
            if (!connection.getAutoCommit()) {
                throw new SQLException("vector local batch procedure requires auto-commit connection");
            }
            call.setString(1, json.toString());
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("vector local batch returned no sequences");
                }
                String encoded = row.getString(1);
                if (encoded == null || encoded.length() < 3 || encoded.charAt(0) != '['
                        || encoded.charAt(encoded.length() - 1) != ']') {
                    throw new SQLException("vector local batch returned malformed sequences");
                }
                String[] parts = encoded.substring(1, encoded.length() - 1).split(",");
                if (parts.length != ordered.size()) {
                    throw new SQLException("vector local batch returned a different result count");
                }
                Long[] originalOrder = new Long[ordered.size()];
                try {
                    for (int index = 0; index < ordered.size(); index++) {
                        long value = Long.parseLong(parts[index].trim());
                        if (value < 1) {
                            throw new NumberFormatException("nonpositive sequence");
                        }
                        originalOrder[ordered.get(index).index()] = value;
                    }
                } catch (NumberFormatException ex) {
                    throw new SQLException("vector local batch returned malformed sequence", ex);
                }
                return List.of(originalOrder);
            }
        }
    }

    /** 排序后的请求保留原始位置，用于按调用方顺序返回提交序号。 */
    private record IndexedRequest(int index, MysqlStorageAdapter.LocalVectorCommitRequest request) {
    }
}

package mvcc.storage.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** 主事务 ACTIVE 行与主参与者意向在一次 InnoDB 提交中持久化的隔离存储过程。 */
final class MysqlPrimaryCombinedPrewriteProcedure {
    /** 存储过程不保存 Java 进程内的事务状态。 */
    private MysqlPrimaryCombinedPrewriteProcedure() {
    }

    /** 在当前隔离主分片创建单次提交例程，已存在时保持数据库状态不变。 */
    static void ensure(Connection connection) throws SQLException {
        try (PreparedStatement check = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'bench_primary_register_prewrite_v3' limit 1
                """); ResultSet row = check.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure bench_primary_register_prewrite_v3(
                    in p_tx_id bigint, in p_primary_key varbinary(256),
                    in p_primary_value blob, in p_secondary_key varbinary(256),
                    in p_secondary_value blob, in p_primary_stripe smallint,
                    in p_secondary_stripe smallint, in p_primary_start_seq bigint,
                    in p_secondary_start_seq bigint, in p_deadline_millis bigint,
                    in p_decision_stripe smallint)
                begin
                  declare v_status varchar(16) default null;
                  declare v_primary_key varbinary(256);
                  declare v_primary_value blob;
                  declare v_secondary_key varbinary(256);
                  declare v_secondary_value blob;
                  declare v_primary_stripe smallint;
                  declare v_secondary_stripe smallint;
                  declare v_primary_start_seq bigint;
                  declare v_secondary_start_seq bigint;
                  declare v_deadline_millis bigint;
                  declare v_decision_stripe smallint;
                  declare v_owner bigint default null;
                  declare v_stripe smallint default null;
                  declare v_start_seq bigint default null;
                  declare v_reserved_seq bigint default null;
                  declare v_payload blob;
                  declare v_lock_owner bigint default null;
                  declare v_conflict int default 0;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  set transaction isolation level read committed;
                  start transaction;
                  insert into bench_primary_txn_v2
                    (tx_id, primary_key, primary_value, secondary_key, secondary_value,
                     primary_stripe, secondary_stripe, primary_start_seq, secondary_start_seq,
                     deadline_millis, decision_stripe, status)
                  values
                    (p_tx_id, p_primary_key, p_primary_value, p_secondary_key,
                     p_secondary_value, p_primary_stripe, p_secondary_stripe,
                     p_primary_start_seq, p_secondary_start_seq, p_deadline_millis,
                     p_decision_stripe, 'ACTIVE')
                  on duplicate key update tx_id = tx_id;
                  select primary_key, primary_value, secondary_key, secondary_value,
                         primary_stripe, secondary_stripe, primary_start_seq,
                         secondary_start_seq, deadline_millis, decision_stripe, status
                    into v_primary_key, v_primary_value, v_secondary_key, v_secondary_value,
                         v_primary_stripe, v_secondary_stripe, v_primary_start_seq,
                         v_secondary_start_seq, v_deadline_millis, v_decision_stripe, v_status
                    from bench_primary_txn_v2 where tx_id = p_tx_id for update;
                  if not (v_primary_key <=> p_primary_key)
                      or not (v_primary_value <=> p_primary_value)
                      or not (v_secondary_key <=> p_secondary_key)
                      or not (v_secondary_value <=> p_secondary_value)
                      or v_primary_stripe <> p_primary_stripe
                      or v_secondary_stripe <> p_secondary_stripe
                      or v_primary_start_seq <> p_primary_start_seq
                      or v_secondary_start_seq <> p_secondary_start_seq
                      or v_deadline_millis <> p_deadline_millis
                      or v_decision_stripe <> p_decision_stripe then
                    signal sqlstate '45000' set mysql_errno = 36015,
                      message_text = 'PRIMARY_REGISTRATION_IDENTITY';
                  end if;
                  if v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 36016,
                      message_text = 'PRIMARY_TRANSACTION_ABORTED';
                  end if;
                  if v_status = 'ACTIVE' and p_deadline_millis <=
                      cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) then
                    signal sqlstate '45000' set mysql_errno = 36017,
                      message_text = 'PRIMARY_REGISTRATION_EXPIRED';
                  end if;
                  insert into bench_striped_mvcc_key_lock(user_key) values (p_primary_key)
                    on duplicate key update user_key = user_key;
                  select exists(select 1 from bench_striped_mvcc_cross_abort
                                where user_key = p_primary_key and tx_id = p_tx_id)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 36006,
                      message_text = 'STRIPED_MVCC_ABORTED_REPLAY';
                  end if;
                  select tx_id, stripe_id, start_seq, reserved_seq, payload
                    into v_owner, v_stripe, v_start_seq, v_reserved_seq, v_payload
                    from bench_striped_mvcc_cross_intent
                    where user_key = p_primary_key for update;
                  if v_owner is not null then
                    select owner_tx_id into v_lock_owner from bench_striped_mvcc_intent
                      where user_key = p_primary_key;
                    if v_lock_owner is null or v_lock_owner <> v_owner then
                      signal sqlstate '45000' set mysql_errno = 36004,
                        message_text = 'STRIPED_MVCC_CROSS_PROOF_MISSING';
                    end if;
                  end if;
                  if v_owner is null then
                    select tx_id, stripe_id, start_seq, reserved_seq, payload
                      into v_owner, v_stripe, v_start_seq, v_reserved_seq, v_payload
                      from bench_striped_mvcc_cross_data
                      where user_key = p_primary_key and tx_id = p_tx_id;
                  end if;
                  if v_owner is not null then
                    if v_owner <> p_tx_id then
                      signal sqlstate '45000' set mysql_errno = 36002,
                        message_text = 'STRIPED_MVCC_INTENT_CONFLICT';
                    end if;
                    if v_stripe <> p_primary_stripe
                        or v_start_seq <> p_primary_start_seq
                        or not (v_payload <=> p_primary_value) then
                      signal sqlstate '45000' set mysql_errno = 36005,
                        message_text = 'STRIPED_MVCC_REPLAY_IDENTITY';
                    end if;
                  else
                    if v_status = 'COMMITTED' then
                      signal sqlstate '45000' set mysql_errno = 36004,
                        message_text = 'STRIPED_MVCC_CROSS_PROOF_MISSING';
                    end if;
                    select exists(select 1 from bench_striped_mvcc_intent
                                  where user_key = p_primary_key) into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 36002,
                        message_text = 'STRIPED_MVCC_INTENT_CONFLICT';
                    end if;
                    select exists(select 1 from bench_striped_mvcc_data
                                  where user_key = p_primary_key
                                    and local_seq > p_primary_start_seq)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 36003,
                        message_text = 'STRIPED_MVCC_WRITE_CONFLICT';
                    end if;
                    select exists(select 1 from bench_striped_mvcc_cross_data
                                  where user_key = p_primary_key
                                    and reserved_seq > p_primary_start_seq)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 36003,
                        message_text = 'STRIPED_MVCC_WRITE_CONFLICT';
                    end if;
                    update bench_striped_mvcc_state
                      set committed_seq = last_insert_id(committed_seq + 1)
                      where stripe_id = p_primary_stripe;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 36001,
                        message_text = 'STRIPED_MVCC_STATE_MISSING';
                    end if;
                    set v_reserved_seq = last_insert_id();
                    insert into bench_striped_mvcc_intent(user_key, owner_tx_id)
                      values (p_primary_key, p_tx_id);
                    insert into bench_striped_mvcc_cross_intent
                      (user_key, tx_id, stripe_id, start_seq, reserved_seq, payload)
                      values (p_primary_key, p_tx_id, p_primary_stripe,
                              p_primary_start_seq, v_reserved_seq, p_primary_value);
                  end if;
                  commit;
                  select v_reserved_seq as reserved_seq;
                end
                """)) {
            create.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }
}

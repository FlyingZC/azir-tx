package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.storage.Mutation;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** 正式参与者预写事务内的分条序号保留账本，与 `mvcc_intent` 共用物理提交。 */
final class MysqlVectorReservationStore {
    private final DataSource dataSource;
    private final int stripes;
    private final MysqlVectorLocalBatchProcedure batchProcedure;
    private final MysqlVectorCrossPrewriteProcedure crossPrewriteProcedure;
    private final MysqlVectorCrossPrewriteBatchProcedure crossPrewriteBatchProcedure;
    private final MysqlVectorCrossMaterializeProcedure crossMaterializeProcedure;
    private volatile boolean localFastProcedureReady;

    /** 向中心暴露的保留身份不包含可变载荷引用。 */
    record Reservation(long txId, String key, int stripe, long startSequence, long reservedSequence) {
        /** 每个保留序号必须晚于起始分条快照。 */
        Reservation {
            Objects.requireNonNull(key, "key");
            if (txId == 0 || key.isBlank() || stripe < 0 || startSequence < 0
                    || reservedSequence <= startSequence) {
                throw new IllegalArgumentException("invalid vector prewrite reservation");
            }
        }
    }

    /** 已物化映射沿用预写序号，中心决议时间仅用于跨片可见性判断。 */
    record CommittedReservation(Reservation reservation, long commitTs) {
        /** 跨片已提交版本必须拥有正决议时间。 */
        CommittedReservation {
            Objects.requireNonNull(reservation, "reservation");
            if (commitTs < 1) {
                throw new IllegalArgumentException("invalid vector committed reservation");
            }
        }
    }

    /** 绑定现有参与者的同一 DataSource，禁止跨库保留无法原子证明的序号。 */
    MysqlVectorReservationStore(DataSource dataSource, int stripes) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector stripe count");
        }
        this.stripes = stripes;
        this.batchProcedure = new MysqlVectorLocalBatchProcedure(dataSource);
        this.crossPrewriteProcedure = new MysqlVectorCrossPrewriteProcedure(dataSource);
        this.crossPrewriteBatchProcedure = new MysqlVectorCrossPrewriteBatchProcedure(dataSource);
        this.crossMaterializeProcedure = new MysqlVectorCrossMaterializeProcedure(dataSource);
    }

    /** 持久布局只允许首次建立或精确重入，不清除已有版本与意向证明。 */
    void initialize() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_vector_layout_v1 (
                      singleton_id tinyint not null primary key,
                      stripe_count smallint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_vector_stripe_state_v1 (
                      stripe_id smallint not null primary key,
                      committed_seq bigint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_vector_intent_reservation_v1 (
                      user_key varbinary(256) not null primary key,
                      tx_id bigint not null,
                      stripe_id smallint not null,
                      start_seq bigint not null,
                      reserved_seq bigint not null,
                      deleted tinyint not null,
                      payload blob,
                      unique key mvcc_vector_reservation_tx_key (tx_id, user_key)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_vector_committed_v1 (
                      user_key varbinary(256) not null,
                      reserved_seq bigint not null,
                      tx_id bigint not null,
                      stripe_id smallint not null,
                      start_seq bigint not null,
                      commit_ts bigint not null,
                      deleted tinyint not null,
                      payload blob,
                      primary key (user_key, reserved_seq),
                      unique key mvcc_vector_committed_tx_key (tx_id, user_key)
                    ) engine=InnoDB
                    """);
            MysqlVectorRetiredVersionStore.ensure(connection);
            statement.execute("""
                    create table if not exists mvcc_vector_local_data_v1 (
                      user_key varbinary(256) not null,
                      local_seq bigint not null,
                      tx_id bigint not null,
                      stripe_id smallint not null,
                      start_seq bigint not null,
                      deleted tinyint not null,
                      payload blob,
                      primary key (user_key, local_seq),
                      unique key mvcc_vector_local_tx_id (tx_id)
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_layout_v1(singleton_id, stripe_count)
                    values (1, ?) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setInt(1, stripes);
                insert.executeUpdate();
            }
            try (PreparedStatement query = connection.prepareStatement("""
                    select stripe_count from mvcc_vector_layout_v1 where singleton_id = 1
                    """); ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw new SQLException("vector stripe layout differs from durable database");
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_stripe_state_v1(stripe_id, committed_seq)
                    values (?, 0) on duplicate key update stripe_id = stripe_id
                    """)) {
                for (int stripe = 0; stripe < stripes; stripe++) {
                    insert.setInt(1, stripe);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement query = connection.prepareStatement("""
                    select count(*), min(stripe_id), max(stripe_id)
                    from mvcc_vector_stripe_state_v1
                    """); ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong(1) != stripes || row.getInt(2) != 0
                        || row.getInt(3) != stripes - 1) {
                    throw new SQLException("vector stripe state has an incompatible layout");
                }
            }
            ensureLocalCommitProcedure(connection);
            batchProcedure.ensure(connection);
            crossPrewriteProcedure.ensure(connection);
            crossPrewriteBatchProcedure.ensure(connection);
            crossMaterializeProcedure.ensure(connection);
            localFastProcedureReady = true;
        }
    }

    /** 只创建一次版本化单键例程，重启后仍核对其持久存在。 */
    private void ensureLocalCommitProcedure(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_vector_local_commit_one_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_vector_local_commit_one_v1(
                    in p_tx_id bigint, in p_key varbinary(256), in p_payload blob,
                    in p_deleted tinyint, in p_stripe smallint, in p_start_seq bigint)
                proc: begin
                  declare v_safe bigint default null;
                  declare v_stripes smallint default null;
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
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select stripe_count into v_stripes from mvcc_vector_layout_v1
                    where singleton_id = 1 for share;
                  if v_safe is null or v_stripes is null or p_tx_id >= 0
                      or p_stripe < 0 or p_stripe >= v_stripes or p_start_seq < 0
                      or p_key is null or length(p_key) < 1 or length(p_key) > 256 then
                    signal sqlstate '45000' set mysql_errno = 37001,
                      message_text = 'VECTOR_LOCAL_IDENTITY_INVALID';
                  end if;
                  -- 稳定键锁先于普通版本读取，避免 RR 旧读视图漏掉前驱提交。
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select user_key, stripe_id, start_seq, local_seq, deleted, payload
                    into v_existing_key, v_existing_stripe, v_existing_start,
                         v_existing_seq, v_existing_deleted, v_existing_payload
                    from mvcc_vector_local_data_v1 where tx_id = p_tx_id;
                  if v_existing_key is not null then
                    if not (v_existing_key <=> p_key) or v_existing_stripe <> p_stripe
                        or v_existing_start <> p_start_seq
                        or v_existing_deleted <> p_deleted
                        or not (v_existing_payload <=> p_payload) then
                      signal sqlstate '45000' set mysql_errno = 37004,
                        message_text = 'VECTOR_LOCAL_REPLAY_IDENTITY';
                    end if;
                    commit;
                    select v_existing_seq as local_seq;
                    leave proc;
                  end if;
                  select
                    exists(select 1 from mvcc_intent where user_key = p_key),
                    exists(select 1 from mvcc_vector_intent_reservation_v1 where user_key = p_key),
                    exists(select 1 from mvcc_vector_committed_v1
                           where user_key = p_key and stripe_id <> p_stripe),
                    exists(select 1 from mvcc_vector_local_data_v1
                           where user_key = p_key and stripe_id <> p_stripe),
                    exists(select 1 from mvcc_vector_committed_v1
                           where user_key = p_key and reserved_seq > p_start_seq),
                    exists(select 1 from mvcc_vector_local_data_v1
                           where user_key = p_key and local_seq > p_start_seq),
                    exists(select 1 from mvcc_data d where d.user_key = p_key
                           and not exists(select 1 from mvcc_vector_committed_v1 c
                                          where c.user_key = d.user_key and c.tx_id = d.tx_id
                                            and c.commit_ts = d.commit_ts)),
                    exists(select 1 from mvcc_rollback
                           where user_key = p_key and tx_id = p_tx_id)
                    into v_intent, v_reservation, v_cross_drift, v_local_drift,
                         v_newer_cross, v_newer_local, v_scalar, v_rollback;
                  if v_cross_drift = 1 or v_local_drift = 1 or v_scalar = 1 then
                    signal sqlstate '45000' set mysql_errno = 37004,
                      message_text = 'VECTOR_LOCAL_INCOMPATIBLE_HISTORY';
                  end if;
                  if v_intent = 1 or v_reservation = 1 then
                    signal sqlstate '45000' set mysql_errno = 37002,
                      message_text = 'VECTOR_LOCAL_INTENT_CONFLICT';
                  end if;
                  if v_newer_cross = 1 or v_newer_local = 1 then
                    signal sqlstate '45000' set mysql_errno = 37003,
                      message_text = 'VECTOR_LOCAL_NEWER_VERSION';
                  end if;
                  if v_rollback = 1 then
                    signal sqlstate '45000' set mysql_errno = 37005,
                      message_text = 'VECTOR_LOCAL_ROLLBACK_MARKER';
                  end if;
                  update mvcc_vector_stripe_state_v1
                    set committed_seq = last_insert_id(committed_seq + 1)
                    where stripe_id = p_stripe;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 37001,
                      message_text = 'VECTOR_LOCAL_STRIPE_MISSING';
                  end if;
                  set v_seq = last_insert_id();
                  if v_seq <= p_start_seq then
                    signal sqlstate '45000' set mysql_errno = 37003,
                      message_text = 'VECTOR_LOCAL_START_AHEAD';
                  end if;
                  insert into mvcc_vector_local_data_v1
                    (user_key, local_seq, tx_id, stripe_id, start_seq, deleted, payload)
                    values (p_key, v_seq, p_tx_id, p_stripe, p_start_seq, p_deleted, p_payload);
                  commit;
                  select v_seq as local_seq;
                end
                """)) {
            create.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 单次服务器调用执行正式本地 1PC，异常由适配器映射为可重试或结果未知。 */
    long commitLocalFast(long txId, Mutation mutation, int stripe, long startSequence) throws SQLException {
        if (!localFastProcedureReady) {
            throw new IllegalStateException("vector local commit procedure is not initialized");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call mvcc_vector_local_commit_one_v1(?, ?, ?, ?, ?, ?)
                """)) {
            if (!connection.getAutoCommit()) {
                throw new SQLException("vector local commit requires auto-commit connection");
            }
            call.setLong(1, txId);
            call.setBytes(2, mutation.key().getBytes(StandardCharsets.UTF_8));
            call.setBytes(3, mutation.value());
            call.setBoolean(4, mutation.type() == MutationType.DELETE);
            call.setInt(5, stripe);
            call.setLong(6, startSequence);
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("vector local commit procedure returned no sequence");
                }
                return row.getLong(1);
            }
        }
    }

    /** 批量例程只执行一次物理提交，成员序号按原调用顺序返回。 */
    List<Long> commitLocalBatchFast(List<MysqlStorageAdapter.LocalVectorCommitRequest> requests)
            throws SQLException {
        return batchProcedure.commit(requests);
    }

    /** 正式意向与序号保留在单次服务器调用中提交，保留同一重放身份。 */
    long reserveFast(long txId, long startTs, Mutation mutation, int stripe,
                     long startSequence, long expireAtMillis) throws SQLException {
        if (txId == 0 || startTs < 0 || stripe < 0 || stripe >= stripes
                || startSequence < 0 || expireAtMillis < 1) {
            throw new IllegalArgumentException("invalid vector prewrite identity or stripe");
        }
        return crossPrewriteProcedure.prewrite(txId, startTs, mutation,
                stripe, startSequence, expireAtMillis);
    }

    /** 同批跨片事务的每个序号仍绑定原始请求位置和事务身份。 */
    List<Long> reserveBatchFast(List<MysqlStorageAdapter.VectorPrewriteRequest> requests)
            throws SQLException {
        return crossPrewriteBatchProcedure.prewrite(requests);
    }

    /** 中心提交已确定后，单次物理调用搬迁真实版本和向量序号。 */
    void materializeFast(long txId, long startTs, long commitTs) throws SQLException {
        crossMaterializeProcedure.materialize(txId, startTs, commitTs);
    }

    /** 必须由适配器在稳定键锁内调用，复用同一连接与 `mvcc_intent` 原子提交。 */
    Reservation reserve(Connection connection, long txId, long startTs,
                        Mutation mutation, int stripe, long startSequence) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(mutation, "mutation");
        if (txId == 0 || startTs < 0 || stripe < 0 || stripe >= stripes || startSequence < 0) {
            throw new IllegalArgumentException("invalid vector prewrite identity");
        }
        byte[] key = mutation.key().getBytes(StandardCharsets.UTF_8);
        if (key.length < 1 || key.length > 256) {
            throw new IllegalArgumentException("vector prewrite key exceeds storage limit");
        }
        try (PreparedStatement existing = connection.prepareStatement("""
                select tx_id, stripe_id, start_seq, reserved_seq, deleted, payload
                from mvcc_vector_intent_reservation_v1 where user_key = ? for update
                """)) {
            existing.setBytes(1, key);
            try (ResultSet row = existing.executeQuery()) {
                if (row.next()) {
                    if (row.getLong("tx_id") != txId || row.getInt("stripe_id") != stripe
                            || row.getLong("start_seq") != startSequence
                            || row.getBoolean("deleted") != (mutation.type() == MutationType.DELETE)
                            || !Arrays.equals(row.getBytes("payload"), mutation.value())) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector prewrite replay changed durable reservation identity", false, txId);
                    }
                    return new Reservation(txId, mutation.key(), stripe,
                            startSequence, row.getLong("reserved_seq"));
                }
            }
        }
        try (PreparedStatement prior = connection.prepareStatement("""
                select
                  exists(select 1 from mvcc_vector_committed_v1
                         where user_key = ? and stripe_id <> ?) as cross_drift,
                  exists(select 1 from mvcc_vector_local_data_v1
                         where user_key = ? and stripe_id <> ?) as local_drift,
                  exists(select 1 from mvcc_vector_local_data_v1
                         where user_key = ? and local_seq > ?) as newer_local
                """)) {
            prior.setBytes(1, key);
            prior.setInt(2, stripe);
            prior.setBytes(3, key);
            prior.setInt(4, stripe);
            prior.setBytes(5, key);
            prior.setLong(6, startSequence);
            try (ResultSet row = prior.executeQuery()) {
                if (!row.next() || row.getBoolean("cross_drift") || row.getBoolean("local_drift")) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "same vector key cannot move to another stripe", false, txId);
                }
                if (row.getBoolean("newer_local")) {
                    throw new MvccException(ErrorCode.WRITE_CONFLICT,
                            "vector prewrite has a newer local version", true, txId);
                }
            }
        }
        try (PreparedStatement advance = connection.prepareStatement("""
                update mvcc_vector_stripe_state_v1
                set committed_seq = last_insert_id(committed_seq + 1)
                where stripe_id = ?
                """)) {
            advance.setInt(1, stripe);
            if (advance.executeUpdate() != 1) {
                throw new SQLException("vector stripe state is missing");
            }
        }
        long reserved;
        try (PreparedStatement query = connection.prepareStatement("select last_insert_id()");
             ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("vector stripe did not return a reserved sequence");
            }
            reserved = row.getLong(1);
        }
        if (reserved <= startSequence) {
            throw new MvccException(ErrorCode.WRITE_CONFLICT,
                    "vector prewrite start sequence is ahead of physical reservation", true, txId);
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_vector_intent_reservation_v1
                  (user_key, tx_id, stripe_id, start_seq, reserved_seq, deleted, payload)
                values (?, ?, ?, ?, ?, ?, ?)
                """)) {
            insert.setBytes(1, key);
            insert.setLong(2, txId);
            insert.setInt(3, stripe);
            insert.setLong(4, startSequence);
            insert.setLong(5, reserved);
            insert.setBoolean(6, mutation.type() == MutationType.DELETE);
            insert.setBytes(7, mutation.value());
            if (insert.executeUpdate() != 1) {
                throw new SQLException("vector reservation was not persisted");
            }
        }
        return new Reservation(txId, mutation.key(), stripe, startSequence, reserved);
    }

    /** 本地单键 1PC 在同一事务完成幂等、同键冲突、分条取号及版本落盘。 */
    long commitLocal(Connection connection, long txId, Mutation mutation,
                     int stripe, long startSequence) throws SQLException {
        if (txId >= 0 || stripe < 0 || stripe >= stripes || startSequence < 0) {
            throw new IllegalArgumentException("local vector transaction requires a negative id and valid stripe");
        }
        byte[] key = mutation.key().getBytes(StandardCharsets.UTF_8);
        boolean deleted = mutation.type() == MutationType.DELETE;
        try (PreparedStatement replay = connection.prepareStatement("""
                select user_key, stripe_id, start_seq, local_seq, deleted, payload
                from mvcc_vector_local_data_v1 where tx_id = ?
                """)) {
            replay.setLong(1, txId);
            try (ResultSet row = replay.executeQuery()) {
                if (row.next()) {
                    if (!Arrays.equals(row.getBytes("user_key"), key)
                            || row.getInt("stripe_id") != stripe
                            || row.getLong("start_seq") != startSequence
                            || row.getBoolean("deleted") != deleted
                            || !Arrays.equals(row.getBytes("payload"), mutation.value())) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local vector replay changed durable transaction identity", false, txId);
                    }
                    return row.getLong("local_seq");
                }
            }
        }
        try (PreparedStatement check = connection.prepareStatement("""
                select
                  exists(select 1 from mvcc_intent where user_key = ?) as active_intent,
                  exists(select 1 from mvcc_vector_intent_reservation_v1 where user_key = ?) as active_reservation,
                  exists(select 1 from mvcc_vector_committed_v1
                         where user_key = ? and stripe_id <> ?) as cross_drift,
                  exists(select 1 from mvcc_vector_local_data_v1
                         where user_key = ? and stripe_id <> ?) as local_drift,
                  exists(select 1 from mvcc_vector_committed_v1
                         where user_key = ? and reserved_seq > ?) as newer_cross,
                  exists(select 1 from mvcc_vector_local_data_v1
                         where user_key = ? and local_seq > ?) as newer_local,
                  exists(select 1 from mvcc_data d where d.user_key = ?
                         and not exists(select 1 from mvcc_vector_committed_v1 c
                                        where c.user_key = d.user_key and c.tx_id = d.tx_id
                                          and c.commit_ts = d.commit_ts)) as scalar_data,
                  exists(select 1 from mvcc_rollback where user_key = ? and tx_id = ?) as rollback_mark
                """)) {
            check.setBytes(1, key);
            check.setBytes(2, key);
            check.setBytes(3, key);
            check.setInt(4, stripe);
            check.setBytes(5, key);
            check.setInt(6, stripe);
            check.setBytes(7, key);
            check.setLong(8, startSequence);
            check.setBytes(9, key);
            check.setLong(10, startSequence);
            check.setBytes(11, key);
            check.setBytes(12, key);
            check.setLong(13, txId);
            try (ResultSet row = check.executeQuery()) {
                if (!row.next() || row.getBoolean("cross_drift") || row.getBoolean("local_drift")
                        || row.getBoolean("scalar_data")) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local vector write has incompatible key history", false, txId);
                }
                if (row.getBoolean("active_intent") || row.getBoolean("active_reservation")) {
                    throw new MvccException(ErrorCode.LOCK_CONFLICT,
                            "local vector write conflicts with a durable intent", true, txId);
                }
                if (row.getBoolean("newer_cross") || row.getBoolean("newer_local")) {
                    throw new MvccException(ErrorCode.WRITE_CONFLICT,
                            "local vector write conflicts with a newer version", true, txId);
                }
                if (row.getBoolean("rollback_mark")) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "local vector transaction was already rolled back", false, txId);
                }
            }
        }
        try (PreparedStatement advance = connection.prepareStatement("""
                update mvcc_vector_stripe_state_v1
                set committed_seq = last_insert_id(committed_seq + 1)
                where stripe_id = ?
                """)) {
            advance.setInt(1, stripe);
            if (advance.executeUpdate() != 1) {
                throw new SQLException("local vector stripe state is missing");
            }
        }
        long localSequence;
        try (PreparedStatement query = connection.prepareStatement("select last_insert_id()");
             ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("local vector commit did not return a sequence");
            }
            localSequence = row.getLong(1);
        }
        if (localSequence <= startSequence) {
            throw new MvccException(ErrorCode.WRITE_CONFLICT,
                    "local vector commit start sequence is ahead of physical state", true, txId);
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_vector_local_data_v1
                  (user_key, local_seq, tx_id, stripe_id, start_seq, deleted, payload)
                values (?, ?, ?, ?, ?, ?, ?)
                """)) {
            insert.setBytes(1, key);
            insert.setLong(2, localSequence);
            insert.setLong(3, txId);
            insert.setInt(4, stripe);
            insert.setLong(5, startSequence);
            insert.setBoolean(6, deleted);
            insert.setBytes(7, mutation.value());
            if (insert.executeUpdate() != 1) {
                throw new SQLException("local vector version was not persisted");
            }
        }
        return localSequence;
    }

    /** 提交响应未知时按事务号核对持久键、分条、起始界限和载荷。 */
    OptionalLong verifyLocalCommit(MysqlStorageAdapter.LocalVectorCommitRequest request) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select user_key, stripe_id, start_seq, local_seq, deleted, payload
                from mvcc_vector_local_data_v1 where tx_id = ?
                """)) {
            query.setLong(1, request.txId());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return OptionalLong.empty();
                }
                Mutation mutation = request.mutation();
                if (!Arrays.equals(row.getBytes("user_key"), mutation.key().getBytes(StandardCharsets.UTF_8))
                        || row.getInt("stripe_id") != request.stripe()
                        || row.getLong("start_seq") != request.startSequence()
                        || row.getBoolean("deleted") != (mutation.type() == MutationType.DELETE)
                        || !Arrays.equals(row.getBytes("payload"), mutation.value())) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local vector proof differs from requested transaction", false, request.txId());
                }
                return OptionalLong.of(row.getLong("local_seq"));
            }
        }
    }

    /** 与真实 mvcc_data 插入处于同一事务，映射从意向转为已提交且序号不变。 */
    void materialize(Connection connection, long txId, String key, boolean deleted,
                     byte[] value, long commitTs) throws SQLException {
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        try (PreparedStatement existing = connection.prepareStatement("""
                select reserved_seq, commit_ts, deleted, payload
                from mvcc_vector_committed_v1 where user_key = ? and tx_id = ?
                """)) {
            existing.setBytes(1, encodedKey);
            existing.setLong(2, txId);
            try (ResultSet row = existing.executeQuery()) {
                if (row.next()) {
                    if (row.getLong("commit_ts") != commitTs || row.getBoolean("deleted") != deleted
                            || !Arrays.equals(row.getBytes("payload"), value)) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector committed replay changed durable identity", false, txId);
                    }
                    return;
                }
            }
        }
        long sequence;
        int stripe;
        long startSequence;
        try (PreparedStatement intent = connection.prepareStatement("""
                select tx_id, stripe_id, start_seq, reserved_seq, deleted, payload
                from mvcc_vector_intent_reservation_v1 where user_key = ? for update
                """)) {
            intent.setBytes(1, encodedKey);
            try (ResultSet row = intent.executeQuery()) {
                if (!row.next() || row.getLong("tx_id") != txId
                        || row.getBoolean("deleted") != deleted
                        || !Arrays.equals(row.getBytes("payload"), value)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector materialization has no matching durable intent reservation", false, txId);
                }
                stripe = row.getInt("stripe_id");
                startSequence = row.getLong("start_seq");
                sequence = row.getLong("reserved_seq");
            }
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_vector_committed_v1
                  (user_key, reserved_seq, tx_id, stripe_id, start_seq, commit_ts, deleted, payload)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            insert.setBytes(1, encodedKey);
            insert.setLong(2, sequence);
            insert.setLong(3, txId);
            insert.setInt(4, stripe);
            insert.setLong(5, startSequence);
            insert.setLong(6, commitTs);
            insert.setBoolean(7, deleted);
            insert.setBytes(8, value);
            if (insert.executeUpdate() != 1) {
                throw new SQLException("vector committed reservation was not persisted");
            }
        }
        try (PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_vector_intent_reservation_v1 where user_key = ? and tx_id = ?
                """)) {
            delete.setBytes(1, encodedKey);
            delete.setLong(2, txId);
            if (delete.executeUpdate() != 1) {
                throw new SQLException("vector intent reservation disappeared during materialization");
            }
        }
    }

    /** 中心 ABORTED 后产品回滚标记负责阻止重放，分条预留行同事务清除。 */
    void abort(Connection connection, long txId, String key) throws SQLException {
        try (PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_vector_intent_reservation_v1 where user_key = ? and tx_id = ?
                """)) {
            delete.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            delete.setLong(2, txId);
            delete.executeUpdate();
        }
    }

    /** 响应丢失后要求物化映射与真实已提交版本同在，缺失则拒绝确认。 */
    void requireCommitted(Connection connection, long txId, String key, long commitTs) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select commit_ts from mvcc_vector_committed_v1
                where user_key = ? and tx_id = ?
                """)) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            query.setLong(2, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong(1) != commitTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "committed participant lacks matching vector version", false, txId);
                }
            }
        }
    }

    /** 恢复器按事务和键读取已经从意向转入版本链的持久序号。 */
    Optional<CommittedReservation> findCommitted(long txId, String key) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select stripe_id, start_seq, reserved_seq, commit_ts
                from mvcc_vector_committed_v1 where user_key = ? and tx_id = ?
                """)) {
            query.setBytes(1, key.getBytes(StandardCharsets.UTF_8));
            query.setLong(2, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                return Optional.of(new CommittedReservation(new Reservation(txId, key,
                        row.getInt("stripe_id"), row.getLong("start_seq"),
                        row.getLong("reserved_seq")), row.getLong("commit_ts")));
            }
        }
    }

    /** 重启或响应丢失后按真实物理行复核保留序号。 */
    Optional<Reservation> find(long txId, String key) throws SQLException {
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select stripe_id, start_seq, reserved_seq
                from mvcc_vector_intent_reservation_v1 where user_key = ? and tx_id = ?
                """)) {
            query.setBytes(1, encodedKey);
            query.setLong(2, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? Optional.of(new Reservation(txId, key, row.getInt("stripe_id"),
                        row.getLong("start_seq"), row.getLong("reserved_seq"))) : Optional.empty();
            }
        }
    }

    /** 查询当前持久分条上界，供集成测试核对重放没有再次取号。 */
    long sequence(int stripe) throws SQLException {
        if (stripe < 0 || stripe >= stripes) {
            throw new IllegalArgumentException("invalid vector stripe");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select committed_seq from mvcc_vector_stripe_state_v1 where stripe_id = ?
                """)) {
            query.setInt(1, stripe);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("vector stripe state is missing");
                }
                return row.getLong(1);
            }
        }
    }
}

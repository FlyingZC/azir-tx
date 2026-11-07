package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.control.MysqlConnectionContext;
import com.flyingzc.azir.metadata.TransactionMetadataValidation;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.FinalizedCandidateProof;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.FinalizedSegmentProof;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.FinalizedFenceGcProof;
import com.flyingzc.azir.storage.Mutation;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** 单分片本地先提交协议的持久 owner 栅栏；中心接管必须先完成物理 fencing。 */
public final class MysqlShardSegmentFence {
    private final String shardId;
    private final DataSource dataSource;
    private volatile boolean localSingleKeyProcedureReady;
    private volatile boolean localSingleKeyProcedureV3Ready;
    private volatile boolean useLocalSingleKeyProcedure;
    private volatile boolean useLocalSingleKeyProcedureV3;

    /** 返回分片数据库时钟下的本地租约，而不复用控制库绝对时间。 */
    public record FenceSnapshot(long segmentStart, long generation, long expiresAtMillis, boolean active) {
        /** 拒绝没有持久代数或截止时间的本地栅栏视图。 */
        public FenceSnapshot {
            if (segmentStart < 1 || generation < 1 || expiresAtMillis < 1) {
                throw new IllegalArgumentException("invalid local segment fence snapshot");
            }
        }
    }

    /** 与分片版本及参与者 COMMITTED 终态同事务持久化的本地候选证明。 */
    public record CandidateProof(long segmentStart, long txId, long startTs, long commitTs,
                                 long deadlineMillis, IsolationLevel isolationLevel, long ownerGeneration) {
        /** 候选必须属于正代数且提交时间晚于其读快照。 */
        public CandidateProof {
            Objects.requireNonNull(isolationLevel, "isolationLevel");
            if (segmentStart < 1 || txId == 0 || startTs < 0 || commitTs <= startTs
                    || deadlineMillis < 1 || ownerGeneration < 1) {
                throw new IllegalArgumentException("invalid local segment candidate proof");
            }
        }
    }

    /** 将 shard fence 绑定到一个确定的分片路由和物理连接池。 */
    public MysqlShardSegmentFence(String shardId, DataSource dataSource) {
        TransactionMetadataValidation.validateParticipant(0, shardId);
        if (shardId.length() > 128) {
            throw new IllegalArgumentException("local segment shard identity is too long");
        }
        this.shardId = shardId;
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** 返回未来控制段必须精确绑定的目标分片。 */
    public String shardId() {
        return shardId;
    }

    /** 分片版本和 fence 必须在同一物理数据库事务中写入。 */
    boolean usesDataSource(DataSource candidate) {
        return dataSource == candidate;
    }

    /** 只显式创建独立版本表，默认 MySQL 参与者不会隐式启用本地先提交协议。 */
    public void initializeIsolated() {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_local_segment_state_v1 (
                      singleton_id tinyint not null primary key,
                      schema_version smallint not null,
                      retired_through_ts bigint not null,
                      constraint mvcc_local_segment_state_version_check check (
                        schema_version = 2 and retired_through_ts >= 0)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_local_segment_fence_v1 (
                      segment_start bigint not null primary key,
                      segment_end bigint not null,
                      owner_id varchar(128) collate utf8mb4_0900_bin not null,
                      generation bigint not null,
                      expires_at bigint not null,
                      status varchar(16) not null,
                      index mvcc_local_segment_expiry (status, expires_at, segment_start),
                      constraint mvcc_local_segment_range_check check (
                        segment_end >= segment_start and generation > 0 and expires_at > 0
                        and status in ('ACTIVE', 'FENCED'))
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_local_segment_candidate_v1 (
                      commit_ts bigint not null primary key,
                      segment_start bigint not null,
                      tx_id bigint not null unique,
                      start_ts bigint not null,
                      deadline_millis bigint not null,
                      isolation_level varchar(20) not null,
                      owner_generation bigint not null,
                      created_at bigint not null,
                      index mvcc_local_candidate_range (segment_start, commit_ts),
                      constraint mvcc_local_candidate_values_check check (
                        tx_id <> 0 and commit_ts > start_ts and deadline_millis > 0
                        and owner_generation > 0
                        and isolation_level in ('READ_COMMITTED', 'REPEATABLE_READ')),
                      constraint mvcc_local_candidate_fence_fk foreign key (segment_start)
                        references mvcc_local_segment_fence_v1(segment_start)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    insert into mvcc_local_segment_state_v1
                      (singleton_id, schema_version, retired_through_ts)
                    values (1, 2, 0) on duplicate key update singleton_id = singleton_id
                    """);
            requireSchemaVersion(connection);
            ensureSingleKeyCommitProcedure(connection);
            localSingleKeyProcedureReady = true;
            ensureSingleKeyCommitProcedureV3(connection);
            localSingleKeyProcedureV3Ready = true;
        } catch (SQLException ex) {
            throw storageError("initialize local segment fence failed", ex);
        }
    }

    /** 重启后验证版本化本地例程存在，缺失时仍可保留 Java 多语句回退路径。 */
    public void requireInitializedFastPath() {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select routine_name from information_schema.routines
                where routine_schema = database()
                  and routine_name in ('mvcc_local_first_commit_one_v1',
                                       'mvcc_local_first_commit_one_v3')
                """); ResultSet row = query.executeQuery()) {
            boolean v1 = false;
            boolean v3 = false;
            while (row.next()) {
                if ("mvcc_local_first_commit_one_v1".equalsIgnoreCase(row.getString(1))) {
                    v1 = true;
                } else if ("mvcc_local_first_commit_one_v3".equalsIgnoreCase(row.getString(1))) {
                    v3 = true;
                }
            }
            if (!v1) {
                throw illegal("local-first single-key procedure is not initialized");
            }
            localSingleKeyProcedureReady = true;
            localSingleKeyProcedureV3Ready = v3;
        } catch (SQLException ex) {
            throw storageError("inspect local-first single-key procedure failed", ex);
        }
    }

    /** 第二条路径须显式启用，默认继续使用已验证的 Java 本地事务。 */
    public void enableSingleKeyProcedure() {
        if (!localSingleKeyProcedureReady) {
            throw illegal("local-first single-key procedure is not ready");
        }
        useLocalSingleKeyProcedure = true;
        useLocalSingleKeyProcedureV3 = false;
    }

    /** 仅显式容量实验选择避免早建读快照的第三版直接终态过程。 */
    public void enableSingleKeyProcedureV3() {
        if (!localSingleKeyProcedureV3Ready) {
            throw illegal("local-first single-key v3 procedure is not ready");
        }
        useLocalSingleKeyProcedure = true;
        useLocalSingleKeyProcedureV3 = true;
    }

    /** 分片适配器只在明确启用且已探测例程时选择一次服务器调用。 */
    boolean singleKeyProcedureEnabled() {
        return useLocalSingleKeyProcedure && localSingleKeyProcedureReady;
    }

    /** 单键物理提交在一次服务器调用内完成 fence、冲突、版本、终态和候选证明。 */
    void commitSingleKey(PublicationSegmentLease lease, long txId, long startTs, long commitTs,
                         long deadlineMillis, IsolationLevel isolationLevel, Mutation mutation) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        Objects.requireNonNull(mutation, "mutation");
        validateLease(lease, 1);
        if (!singleKeyProcedureEnabled() || commitTs < lease.firstTs() || commitTs > lease.lastTs()) {
            throw illegal("local-first single-key procedure is unavailable for this segment");
        }
        String routine = useLocalSingleKeyProcedureV3
                ? "mvcc_local_first_commit_one_v3" : "mvcc_local_first_commit_one_v1";
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement(
                "call " + routine + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            if (!connection.getAutoCommit()) {
                throw illegal("local-first single-key procedure requires auto-commit connection");
            }
            call.setLong(1, lease.firstTs());
            call.setLong(2, lease.lastTs());
            call.setString(3, lease.ownerId());
            call.setLong(4, lease.generation());
            call.setLong(5, txId);
            call.setLong(6, startTs);
            call.setLong(7, commitTs);
            call.setLong(8, deadlineMillis);
            call.setString(9, isolationLevel.name());
            call.setBytes(10, mutation.key().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            call.setBoolean(11, mutation.type() == MutationType.DELETE);
            if (mutation.value() == null) {
                call.setNull(12, java.sql.Types.BLOB);
            } else {
                call.setBytes(12, mutation.value());
            }
            call.execute();
            if (!connection.getAutoCommit()) {
                throw new SQLException("local-first single-key procedure left a transaction open");
            }
        } catch (SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 33002, 33003, 33004, 33005 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local-first single-key identity differs from durable proof", false, txId, ex);
                case 33006, 33007 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "local-first single-key participant or owner was fenced", false, txId, ex);
                case 33008 -> new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                        "local-first single-key snapshot is older than shard safe point", false, txId, ex);
                case 33009 -> new MvccException(ErrorCode.WRITE_CONFLICT,
                        "local-first single-key newer version exists", true, txId, ex);
                case 33010 -> new MvccException(ErrorCode.LOCK_CONFLICT,
                        "local-first single-key conflicts with a durable intent", true, txId, ex);
                default -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local-first single-key commit outcome requires local proof lookup", false, txId, ex);
            };
        }
    }

    /** 版本化例程不修改旧存储过程，安装响应未知只重用原例程。 */
    private void ensureSingleKeyCommitProcedure(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_local_first_commit_one_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_local_first_commit_one_v1(
                    in p_segment_start bigint, in p_segment_end bigint,
                    in p_owner_id varchar(128), in p_generation bigint,
                    in p_tx_id bigint, in p_start bigint, in p_commit bigint,
                    in p_deadline bigint, in p_isolation varchar(20),
                    in p_key varbinary(256), in p_deleted tinyint, in p_value blob)
                begin
                  declare v_safe bigint default null;
                  declare v_segment_end bigint default null;
                  declare v_owner_id varchar(128) default null;
                  declare v_generation bigint default null;
                  declare v_expiry bigint default null;
                  declare v_fence_status varchar(16) default null;
                  declare v_now bigint default null;
                  declare v_start bigint default null;
                  declare v_status varchar(16) default null;
                  declare v_commit bigint default null;
                  declare v_count int default 0;
                  declare v_conflict int default 0;
                  declare v_version_tx bigint default null;
                  declare v_version_deleted tinyint default null;
                  declare v_version_value blob;
                  declare v_candidate_segment bigint default null;
                  declare v_candidate_start bigint default null;
                  declare v_candidate_commit bigint default null;
                  declare v_candidate_deadline bigint default null;
                  declare v_candidate_isolation varchar(20) default null;
                  declare v_candidate_generation bigint default null;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select segment_end, owner_id, generation, expires_at, status,
                         cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                    into v_segment_end, v_owner_id, v_generation, v_expiry, v_fence_status, v_now
                    from mvcc_local_segment_fence_v1
                    where segment_start = p_segment_start for share;
                  if v_safe is null or v_segment_end is null or v_segment_end <> p_segment_end
                      or binary v_owner_id <> binary p_owner_id
                      or p_commit < p_segment_start or p_commit > p_segment_end
                      or p_commit <= p_start or p_deadline < 1
                      or p_isolation not in ('READ_COMMITTED', 'REPEATABLE_READ') then
                    signal sqlstate '45000' set mysql_errno = 33002,
                      message_text = 'LOCAL_FIRST_SEGMENT_IDENTITY';
                  end if;
                  insert into mvcc_participant_txn(tx_id, start_ts, status, commit_ts, updated_at)
                    values (p_tx_id, p_start, 'ACTIVE', null, v_now)
                    on duplicate key update tx_id = tx_id;
                  select start_ts, status, commit_ts into v_start, v_status, v_commit
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_start is null or v_start <> p_start then
                    signal sqlstate '45000' set mysql_errno = 33003,
                      message_text = 'LOCAL_FIRST_PARTICIPANT_START';
                  end if;
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  if v_status = 'COMMITTED' then
                    if v_commit is null or v_commit <> p_commit then
                      signal sqlstate '45000' set mysql_errno = 33004,
                        message_text = 'LOCAL_FIRST_REPLAY_TIMESTAMP';
                    end if;
                    select count(*) into v_count from mvcc_data
                      where tx_id = p_tx_id and commit_ts = p_commit;
                    select tx_id, deleted, value into v_version_tx, v_version_deleted, v_version_value
                      from mvcc_data where user_key = p_key and commit_ts = p_commit;
                    select segment_start, start_ts, commit_ts, deadline_millis,
                           isolation_level, owner_generation
                      into v_candidate_segment, v_candidate_start, v_candidate_commit,
                           v_candidate_deadline, v_candidate_isolation, v_candidate_generation
                      from mvcc_local_segment_candidate_v1 where tx_id = p_tx_id for update;
                    if v_count <> 1 or v_version_tx is null or v_version_tx <> p_tx_id
                        or v_version_deleted <> p_deleted or not (v_version_value <=> p_value)
                        or v_candidate_segment is null or v_candidate_segment <> p_segment_start
                        or v_candidate_start <> p_start or v_candidate_commit <> p_commit
                        or v_candidate_deadline <> p_deadline
                        or binary v_candidate_isolation <> binary p_isolation
                        or v_candidate_generation <> p_generation then
                      signal sqlstate '45000' set mysql_errno = 33005,
                        message_text = 'LOCAL_FIRST_REPLAY_PROOF';
                    end if;
                  elseif v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 33006,
                      message_text = 'LOCAL_FIRST_PARTICIPANT_ABORTED';
                  elseif v_status = 'ACTIVE' and v_commit is null then
                    if v_fence_status <> 'ACTIVE' or v_generation <> p_generation
                        or v_expiry <= v_now then
                      signal sqlstate '45000' set mysql_errno = 33007,
                        message_text = 'LOCAL_FIRST_OWNER_FENCED';
                    end if;
                    if p_start < v_safe then
                      signal sqlstate '45000' set mysql_errno = 33008,
                        message_text = 'LOCAL_FIRST_SNAPSHOT_TOO_OLD';
                    end if;
                    select exists(select 1 from mvcc_data
                                  where user_key = p_key and commit_ts > p_start)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 33009,
                        message_text = 'LOCAL_FIRST_NEWER_VERSION';
                    end if;
                    select exists(select 1 from mvcc_intent
                                  where user_key = p_key or tx_id = p_tx_id)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 33010,
                        message_text = 'LOCAL_FIRST_EXISTING_INTENT';
                    end if;
                    select exists(select 1 from mvcc_rollback
                                  where user_key = p_key and tx_id = p_tx_id)
                      into v_conflict;
                    if v_conflict = 1 then
                      signal sqlstate '45000' set mysql_errno = 33006,
                        message_text = 'LOCAL_FIRST_ROLLBACK_MARKER';
                    end if;
                    insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                      values (p_key, p_commit, p_tx_id, p_deleted, p_value);
                    update mvcc_participant_txn set status = 'COMMITTED', commit_ts = p_commit,
                      updated_at = v_now where tx_id = p_tx_id and start_ts = p_start
                        and status = 'ACTIVE' and commit_ts is null;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 33003,
                        message_text = 'LOCAL_FIRST_PARTICIPANT_LOST';
                    end if;
                    insert into mvcc_local_segment_candidate_v1
                      (commit_ts, segment_start, tx_id, start_ts, deadline_millis,
                       isolation_level, owner_generation, created_at)
                      values (p_commit, p_segment_start, p_tx_id, p_start, p_deadline,
                              p_isolation, p_generation, v_now);
                  else
                    signal sqlstate '45000' set mysql_errno = 33003,
                      message_text = 'LOCAL_FIRST_PARTICIPANT_STATUS';
                  end if;
                  commit;
                end
                """)) {
            create.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 第三版先以唯一参与者插入仲裁身份，避免键锁前的一致性读快照。 */
    private void ensureSingleKeyCommitProcedureV3(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_local_first_commit_one_v3' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_local_first_commit_one_v3(
                    in p_segment_start bigint, in p_segment_end bigint,
                    in p_owner_id varchar(128), in p_generation bigint,
                    in p_tx_id bigint, in p_start bigint, in p_commit bigint,
                    in p_deadline bigint, in p_isolation varchar(20),
                    in p_key varbinary(256), in p_deleted tinyint, in p_value blob)
                proc: begin
                  declare v_safe bigint default null;
                  declare v_segment_end bigint default null;
                  declare v_owner_id varchar(128) default null;
                  declare v_generation bigint default null;
                  declare v_expiry bigint default null;
                  declare v_fence_status varchar(16) default null;
                  declare v_now bigint default null;
                  declare v_existing int default 0;
                  declare v_conflict int default 0;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select segment_end, owner_id, generation, expires_at, status,
                         cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                    into v_segment_end, v_owner_id, v_generation, v_expiry, v_fence_status, v_now
                    from mvcc_local_segment_fence_v1
                    where segment_start = p_segment_start for share;
                  if v_safe is null or v_segment_end is null or v_segment_end <> p_segment_end
                      or binary v_owner_id <> binary p_owner_id
                      or p_commit < p_segment_start or p_commit > p_segment_end
                      or p_commit <= p_start or p_deadline < 1
                      or p_isolation not in ('READ_COMMITTED', 'REPEATABLE_READ') then
                    signal sqlstate '45000' set mysql_errno = 33002,
                      message_text = 'LOCAL_FIRST_SEGMENT_IDENTITY';
                  end if;
                  begin
                    declare continue handler for 1062 set v_existing = 1;
                    insert into mvcc_participant_txn
                      (tx_id, start_ts, status, commit_ts, updated_at)
                      values (p_tx_id, p_start, 'COMMITTED', p_commit, v_now);
                  end;
                  if v_existing = 1 then
                    rollback;
                    call mvcc_local_first_commit_one_v1(
                      p_segment_start, p_segment_end, p_owner_id, p_generation,
                      p_tx_id, p_start, p_commit, p_deadline, p_isolation,
                      p_key, p_deleted, p_value);
                    leave proc;
                  end if;
                  if v_fence_status <> 'ACTIVE' or v_generation <> p_generation
                      or v_expiry <= v_now then
                    signal sqlstate '45000' set mysql_errno = 33007,
                      message_text = 'LOCAL_FIRST_OWNER_FENCED';
                  end if;
                  if p_start < v_safe then
                    signal sqlstate '45000' set mysql_errno = 33008,
                      message_text = 'LOCAL_FIRST_SNAPSHOT_TOO_OLD';
                  end if;
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select exists(select 1 from mvcc_data
                                where user_key = p_key and commit_ts > p_start)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 33009,
                      message_text = 'LOCAL_FIRST_NEWER_VERSION';
                  end if;
                  select exists(select 1 from mvcc_intent
                                where user_key = p_key or tx_id = p_tx_id)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 33010,
                      message_text = 'LOCAL_FIRST_EXISTING_INTENT';
                  end if;
                  select exists(select 1 from mvcc_rollback
                                where user_key = p_key and tx_id = p_tx_id)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 33006,
                      message_text = 'LOCAL_FIRST_ROLLBACK_MARKER';
                  end if;
                  insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                    values (p_key, p_commit, p_tx_id, p_deleted, p_value);
                  insert into mvcc_local_segment_candidate_v1
                    (commit_ts, segment_start, tx_id, start_ts, deadline_millis,
                     isolation_level, owner_generation, created_at)
                    values (p_commit, p_segment_start, p_tx_id, p_start, p_deadline,
                            p_isolation, p_generation, v_now);
                  commit;
                end
                """)) {
            create.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 每次跨中心预留的新段先安装到目标分片；完全相同的响应丢失重放不续命。 */
    public FenceSnapshot install(PublicationSegmentLease lease, long leaseMillis) {
        validateLease(lease, leaseMillis);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                requireSchemaVersion(connection);
                // 串行化安装，防止不同段范围交叠而共享同一提交时间戳。
                long retiredThrough = lockInstallState(connection);
                FenceRow existing = lockFence(connection, lease.firstTs(), true);
                if (existing != null) {
                    requireSameLease(lease, existing);
                    if (!"ACTIVE".equals(existing.status())) {
                        throw aborted("fenced local segment cannot be installed again");
                    }
                    return new FenceSnapshot(lease.firstTs(), existing.generation(),
                            existing.expiresAtMillis(), true);
                }
                if (lease.firstTs() <= retiredThrough) {
                    throw aborted("retired local segment cannot be installed again");
                }
                requireNoOverlap(connection, lease);
                long deadline = Math.addExact(databaseNow(connection), leaseMillis);
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_local_segment_fence_v1
                          (segment_start, segment_end, owner_id, generation, expires_at, status)
                        values (?, ?, ?, ?, ?, 'ACTIVE')
                        """)) {
                    insert.setLong(1, lease.firstTs());
                    insert.setLong(2, lease.lastTs());
                    insert.setString(3, lease.ownerId());
                    insert.setLong(4, lease.generation());
                    insert.setLong(5, deadline);
                    if (insert.executeUpdate() != 1) {
                        throw illegal("local segment fence installation was incomplete");
                    }
                }
                return new FenceSnapshot(lease.firstTs(), lease.generation(), deadline, true);
            });
        } catch (SQLException ex) {
            throw storageError("install local segment fence failed", ex);
        }
    }

    /** 只在当前分片代数仍有效时按分片数据库时钟续租。 */
    public FenceSnapshot renew(PublicationSegmentLease lease, long leaseMillis) {
        validateLease(lease, leaseMillis);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                FenceRow current = lockFence(connection, lease.firstTs(), true);
                requireSameLease(lease, current);
                long now = databaseNow(connection);
                if (!"ACTIVE".equals(current.status()) || current.expiresAtMillis() <= now) {
                    throw aborted("local segment owner expired or was fenced");
                }
                long deadline = Math.addExact(now, leaseMillis);
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_segment_fence_v1 set expires_at = ?
                        where segment_start = ? and generation = ? and status = 'ACTIVE'
                        """)) {
                    update.setLong(1, deadline);
                    update.setLong(2, lease.firstTs());
                    update.setLong(3, lease.generation());
                    if (update.executeUpdate() != 1) {
                        throw aborted("local segment renewal lost owner generation");
                    }
                }
                return new FenceSnapshot(lease.firstTs(), lease.generation(), deadline, true);
            });
        } catch (SQLException ex) {
            throw storageError("renew local segment fence failed", ex);
        }
    }

    /** 排他 fencing 等待持共享锁的本地提交事务结束，然后拒绝旧代数新提交。 */
    public boolean fence(PublicationSegmentLease lease) {
        validateLease(lease, 1);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                requireSchemaVersion(connection);
                // 安装与缺行 fencing 共用状态行；先封存会留下持久墓碑阻止迟到安装。
                long retiredThrough = lockInstallState(connection);
                FenceRow current = lockFence(connection, lease.firstTs(), true);
                if (current == null) {
                    if (lease.lastTs() <= retiredThrough) {
                        return false;
                    }
                    if (lease.firstTs() <= retiredThrough) {
                        throw illegal("local segment range crosses physical retirement boundary");
                    }
                    if (lease.generation() == Long.MAX_VALUE) {
                        throw aborted("local segment generation cannot be fenced again");
                    }
                    requireNoOverlap(connection, lease);
                    try (PreparedStatement tombstone = connection.prepareStatement("""
                            insert into mvcc_local_segment_fence_v1
                              (segment_start, segment_end, owner_id, generation, expires_at, status)
                            values (?, ?, ?, ?, ?, 'FENCED')
                            """)) {
                        tombstone.setLong(1, lease.firstTs());
                        tombstone.setLong(2, lease.lastTs());
                        tombstone.setString(3, lease.ownerId());
                        tombstone.setLong(4, lease.generation() + 1);
                        tombstone.setLong(5, databaseNow(connection));
                        if (tombstone.executeUpdate() != 1) {
                            throw illegal("local segment fencing tombstone was not persisted");
                        }
                    }
                    return true;
                }
                if (current.segmentEnd() != lease.lastTs()
                        || !current.ownerId().equals(lease.ownerId())) {
                    throw illegal("local segment fence identity differs from control owner");
                }
                if ("FENCED".equals(current.status()) && lease.generation() < Long.MAX_VALUE
                        && current.generation() == lease.generation() + 1) {
                    return false;
                }
                if (!"ACTIVE".equals(current.status()) || current.generation() != lease.generation()
                        || current.generation() == Long.MAX_VALUE) {
                    throw aborted("local segment owner generation was already fenced");
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_segment_fence_v1
                        set status = 'FENCED', generation = generation + 1
                        where segment_start = ? and generation = ? and status = 'ACTIVE'
                        """)) {
                    update.setLong(1, lease.firstTs());
                    update.setLong(2, lease.generation());
                    if (update.executeUpdate() != 1) {
                        throw aborted("local segment fencing lost owner generation");
                    }
                }
                return true;
            });
        } catch (SQLException ex) {
            throw storageError("fence local segment failed", ex);
        }
    }

    /** 分片提交事务在同一 JDBC 事务中调用并持有共享锁直至本地终态落盘。 */
    boolean lockForCommit(Connection connection, PublicationSegmentLease lease, long commitTs) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        validateLease(lease, 1);
        if (commitTs < lease.firstTs() || commitTs > lease.lastTs()) {
            throw illegal("local commit timestamp is outside owner range");
        }
        try (PreparedStatement query = connection.prepareStatement("""
                select segment_end, owner_id, generation, expires_at, status,
                       cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                from mvcc_local_segment_fence_v1 where segment_start = ? for share
                """)) {
            query.setLong(1, lease.firstTs());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong("segment_end") != lease.lastTs()
                        || !lease.ownerId().equals(row.getString("owner_id"))) {
                    throw illegal("local commit has no matching segment fence");
                }
                long generation = row.getLong("generation");
                String status = row.getString("status");
                if ("FENCED".equals(status) && lease.generation() < Long.MAX_VALUE
                        && generation == lease.generation() + 1) {
                    return false;
                }
                if (!"ACTIVE".equals(status) || generation != lease.generation()
                        || row.getLong("expires_at") <= row.getLong("now_millis")) {
                    throw aborted("local commit owner lease expired or was fenced");
                }
                return true;
            }
        }
    }

    /** 由分片适配器在版本及本地终态所在的同一事务中插入恢复证明。 */
    void insertCandidate(Connection connection, PublicationSegmentLease lease, CandidateProof proof)
            throws SQLException {
        requireCandidateIdentity(lease, proof);
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_local_segment_candidate_v1
                  (commit_ts, segment_start, tx_id, start_ts, deadline_millis,
                   isolation_level, owner_generation, created_at)
                values (?, ?, ?, ?, ?, ?, ?,
                        cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                """)) {
            insert.setLong(1, proof.commitTs());
            insert.setLong(2, proof.segmentStart());
            insert.setLong(3, proof.txId());
            insert.setLong(4, proof.startTs());
            insert.setLong(5, proof.deadlineMillis());
            insert.setString(6, proof.isolationLevel().name());
            insert.setLong(7, proof.ownerGeneration());
            if (insert.executeUpdate() != 1) {
                throw illegal("local segment candidate insertion was incomplete");
            }
        }
    }

    /** 本地提交响应丢失后的重放必须核对同一段、事务、时间戳与隔离级别。 */
    void requireCandidateReplay(Connection connection, PublicationSegmentLease lease, CandidateProof expected)
            throws SQLException {
        requireCandidateIdentity(lease, expected);
        CandidateProof actual = readCandidate(connection, expected.txId(), true).orElseThrow(() ->
                illegal("committed local participant has no segment candidate proof"));
        if (!actual.equals(expected)) {
            throw illegal("local segment candidate replay changed durable identity");
        }
    }

    /** 按事务编号读取已落分片的候选，响应未知时不依赖协调者内存。 */
    public Optional<CandidateProof> candidate(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            return readCandidate(connection, txId, false);
        } catch (SQLException ex) {
            throw storageError("read local segment candidate failed", ex);
        }
    }

    /** 中心补决议前同时核对分片候选、本地 COMMITTED 终态和已落版本。 */
    public Optional<CandidateProof> verifiedCandidate(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<CandidateProof> proof = readCandidate(connection, txId, false);
            if (proof.isPresent()) {
                verifyCandidate(connection, proof.get());
            }
            return proof;
        } catch (SQLException ex) {
            throw storageError("verify local segment candidate failed", ex);
        }
    }

    /** 一次分片查询核对同批候选、参与者终态和版本证据，缺一笔即整批拒绝。 */
    public List<CandidateProof> verifiedCandidates(List<Long> txIds) {
        if (txIds == null || txIds.isEmpty() || txIds.size() > 64
                || new HashSet<>(txIds).size() != txIds.size()) {
            throw new IllegalArgumentException("invalid local candidate verification batch");
        }
        String placeholders = String.join(",", Collections.nCopies(txIds.size(), "?"));
        String sql = "select c.segment_start, c.tx_id, c.start_ts, c.commit_ts,"
                + " c.deadline_millis, c.isolation_level, c.owner_generation,"
                + " p.start_ts as participant_start, p.status as participant_status,"
                + " p.commit_ts as participant_commit,"
                + " exists(select 1 from mvcc_data d where d.tx_id = c.tx_id"
                + " and d.commit_ts = c.commit_ts limit 1) as has_version"
                + " from mvcc_local_segment_candidate_v1 c"
                + " left join mvcc_participant_txn p on p.tx_id = c.tx_id"
                + " where c.tx_id in (" + placeholders + ")";
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement(sql)) {
            for (int index = 0; index < txIds.size(); index++) {
                query.setLong(index + 1, txIds.get(index));
            }
            Map<Long, CandidateProof> found = new HashMap<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    CandidateProof proof = mapCandidate(row);
                    if (row.getObject("participant_start") == null
                            || row.getLong("participant_start") != proof.startTs()
                            || !"COMMITTED".equals(row.getString("participant_status"))
                            || row.getObject("participant_commit") == null
                            || row.getLong("participant_commit") != proof.commitTs()
                            || !row.getBoolean("has_version")
                            || found.put(proof.txId(), proof) != null) {
                        throw illegal("local candidate batch lacks matching committed shard evidence");
                    }
                }
            }
            if (found.size() != txIds.size()) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local candidate batch has missing durable shard proof", false, null);
            }
            return txIds.stream().map(found::get).toList();
        } catch (SQLException ex) {
            throw storageError("verify local candidate batch failed", ex);
        }
    }

    /** 只有物理 fencing 成功后才能把本段候选集合当作稳定恢复输入。 */
    public List<CandidateProof> scanFenced(PublicationSegmentLease lease, long afterCommitTs, int limit) {
        validateLease(lease, 1);
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local segment candidate scan limit");
        }
        try (Connection connection = dataSource.getConnection()) {
            FenceRow owner = lockFence(connection, lease.firstTs(), false);
            if (owner == null) {
                if (readRetiredThrough(connection) >= lease.lastTs()) {
                    return List.of();
                }
                throw aborted("local candidate scan has no physical fence or retirement proof");
            }
            if (owner.segmentEnd() != lease.lastTs()
                    || !owner.ownerId().equals(lease.ownerId())
                    || !"FENCED".equals(owner.status())
                    || lease.generation() == Long.MAX_VALUE
                    || owner.generation() != lease.generation() + 1) {
                throw aborted("local candidate scan requires physical owner fencing");
            }
            try (PreparedStatement query = connection.prepareStatement("""
                    select segment_start, tx_id, start_ts, commit_ts, deadline_millis,
                           isolation_level, owner_generation
                    from mvcc_local_segment_candidate_v1
                    where segment_start = ? and commit_ts > ? and commit_ts <= ?
                    order by commit_ts limit ?
                    """)) {
                query.setLong(1, lease.firstTs());
                query.setLong(2, afterCommitTs);
                query.setLong(3, lease.lastTs());
                query.setInt(4, limit);
                List<CandidateProof> found = new ArrayList<>();
                try (ResultSet rows = query.executeQuery()) {
                    while (rows.next()) {
                        CandidateProof proof = mapCandidate(rows);
                        requireCandidateIdentity(lease, proof);
                        verifyCandidate(connection, proof);
                        found.add(proof);
                    }
                }
                return List.copyOf(found);
            }
        } catch (SQLException ex) {
            throw storageError("scan fenced local candidates failed", ex);
        }
    }

    /** 物理行已删除时仍可凭持久高水位确认旧段不能重新安装。 */
    public boolean isFenceRetired(PublicationSegmentLease lease) {
        validateLease(lease, 1);
        try (Connection connection = dataSource.getConnection()) {
            return readRetiredThrough(connection) >= lease.lastTs()
                    && lockFence(connection, lease.firstTs(), false) == null;
        } catch (SQLException ex) {
            throw storageError("check local segment physical retirement failed", ex);
        }
    }

    /** 中心已准许退役时，分片原子推进拒绝旧安装的高水位并删除 FENCED 行。 */
    public boolean purgeFence(FinalizedFenceGcProof proof) {
        Objects.requireNonNull(proof, "proof");
        if (!shardId.equals(proof.shardId())) {
            throw illegal("local fence GC proof targets another shard");
        }
        PublicationSegmentLease lease = proof.physicalLease();
        validateLease(lease, 1);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long retiredThrough = lockInstallState(connection);
                FenceRow current = lockFence(connection, lease.firstTs(), true);
                if (current == null) {
                    if (retiredThrough >= lease.lastTs()) {
                        return false;
                    }
                    throw illegal("local fence was removed without durable retirement waterline");
                }
                if (lease.generation() == Long.MAX_VALUE
                        || current.segmentEnd() != lease.lastTs()
                        || !current.ownerId().equals(lease.ownerId())
                        || !"FENCED".equals(current.status())
                        || current.generation() != lease.generation() + 1) {
                    throw aborted("local fence GC proof differs from physical owner generation");
                }
                try (PreparedStatement candidates = connection.prepareStatement("""
                        select 1 from mvcc_local_segment_candidate_v1
                        where segment_start = ? limit 1
                        """)) {
                    candidates.setLong(1, lease.firstTs());
                    try (ResultSet row = candidates.executeQuery()) {
                        if (row.next()) {
                            throw illegal("local fence still owns unrecovered candidates");
                        }
                    }
                }
                try (PreparedStatement advance = connection.prepareStatement("""
                        update mvcc_local_segment_state_v1
                        set retired_through_ts = greatest(retired_through_ts, ?)
                        where singleton_id = 1 and schema_version = 2
                        """); PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_local_segment_fence_v1
                        where segment_start = ? and generation = ? and status = 'FENCED'
                        """)) {
                    advance.setLong(1, lease.lastTs());
                    if (advance.executeUpdate() != 1) {
                        throw illegal("local fence retirement state was not persisted");
                    }
                    delete.setLong(1, lease.firstTs());
                    delete.setLong(2, current.generation());
                    if (delete.executeUpdate() != 1) {
                        throw illegal("local FENCED row disappeared before retirement committed");
                    }
                }
                return true;
            });
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local fence retirement requires durable state lookup", false, null, ex);
        }
    }

    /** 中心已封段并持久发布后，幂等删除这笔分片候选以解除本地 GC 下界。 */
    public boolean purgeCandidate(FinalizedCandidateProof proof) {
        Objects.requireNonNull(proof, "proof");
        if (!shardId.equals(proof.shardId())) {
            throw illegal("local candidate GC proof targets another shard");
        }
        CandidateProof expected = proof.candidate();
        PublicationSegmentLease lease = proof.physicalLease();
        requireCandidateIdentity(lease, expected);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                if (lockForCommit(connection, lease, expected.commitTs())) {
                    throw aborted("local candidate cannot be purged before physical fencing");
                }
                Optional<CandidateProof> current = readCandidate(connection, expected.txId(), true);
                if (current.isEmpty()) {
                    return false;
                }
                if (!current.get().equals(expected)) {
                    throw illegal("local candidate GC proof changed durable shard identity");
                }
                verifyCandidate(connection, current.get());
                try (PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_local_segment_candidate_v1
                        where tx_id = ? and commit_ts = ? and segment_start = ?
                        """)) {
                    delete.setLong(1, expected.txId());
                    delete.setLong(2, expected.commitTs());
                    delete.setLong(3, expected.segmentStart());
                    if (delete.executeUpdate() != 1) {
                        throw illegal("local candidate disappeared during finalized GC");
                    }
                }
                return true;
            });
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local candidate GC result requires durable lookup", false, expected.txId(), ex);
        }
    }

    /** 全段中心终态已发布后，在物理 FENCED 代数内一次删除全部候选。 */
    public int purgeFencedSegmentCandidates(FinalizedSegmentProof proof) {
        Objects.requireNonNull(proof, "proof");
        if (!shardId.equals(proof.shardId())) {
            throw illegal("local-first segment GC proof targets another shard");
        }
        PublicationSegmentLease lease = proof.physicalLease();
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                if (lockForCommit(connection, lease, lease.firstTs())) {
                    throw aborted("local-first segment cannot purge candidates before physical fencing");
                }
                try (PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_local_segment_candidate_v1
                        where segment_start = ?
                        """)) {
                    delete.setLong(1, lease.firstTs());
                    return delete.executeUpdate();
                }
            });
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first segment candidate purge requires durable lookup", false, null, ex);
        }
    }

    /** 已物理封存的段按主键统计尚存证明，供中心批量 GC 标记核实。 */
    public int countCandidates(long segmentStart) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select count(*) from mvcc_local_segment_candidate_v1 where segment_start = ?
                """)) {
            query.setLong(1, segmentStart);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("local-first candidate count returned no row");
                }
                return row.getInt(1);
            }
        } catch (SQLException ex) {
            throw storageError("count local-first segment candidates failed", ex);
        }
    }

    /** 后台 GC 先读尚存候选的最老快照，允许分片安全点保守落后控制库。 */
    public OptionalLong oldestCandidateStartTs() {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select min(start_ts) from mvcc_local_segment_candidate_v1
                """); ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getObject(1) == null) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(row.getLong(1));
        } catch (SQLException ex) {
            throw storageError("read local-first oldest candidate snapshot failed", ex);
        }
    }

    /** 分片候选必须落在同一中心预留范围和 owner 代数内。 */
    private void requireCandidateIdentity(PublicationSegmentLease lease, CandidateProof proof) {
        Objects.requireNonNull(proof, "proof");
        if (proof.segmentStart() != lease.firstTs() || proof.commitTs() < lease.firstTs()
                || proof.commitTs() > lease.lastTs() || proof.ownerGeneration() != lease.generation()) {
            throw illegal("local candidate differs from segment owner range");
        }
    }

    /** 事务编号索引精确查询本地证明，重放时额外取得排他行锁。 */
    private Optional<CandidateProof> readCandidate(Connection connection, long txId, boolean lock)
            throws SQLException {
        String sql = "select segment_start, tx_id, start_ts, commit_ts, deadline_millis,"
                + " isolation_level, owner_generation from mvcc_local_segment_candidate_v1"
                + " where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? Optional.of(mapCandidate(row)) : Optional.empty();
            }
        }
    }

    /** 将数据库中的持久枚举和时间戳转换为不可变本地证明。 */
    private CandidateProof mapCandidate(ResultSet row) throws SQLException {
        return new CandidateProof(row.getLong("segment_start"), row.getLong("tx_id"),
                row.getLong("start_ts"), row.getLong("commit_ts"), row.getLong("deadline_millis"),
                IsolationLevel.valueOf(row.getString("isolation_level")), row.getLong("owner_generation"));
    }

    /** 单分片版本链至少保留一条与候选时间戳相同的本地持久提交记录。 */
    private void verifyCandidate(Connection connection, CandidateProof proof) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select p.start_ts, p.status, p.commit_ts,
                       exists(select 1 from mvcc_data d
                              where d.tx_id = p.tx_id and d.commit_ts = ? limit 1) as has_version
                from mvcc_participant_txn p where p.tx_id = ?
                """)) {
            query.setLong(1, proof.commitTs());
            query.setLong(2, proof.txId());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong("start_ts") != proof.startTs()
                        || !"COMMITTED".equals(row.getString("status"))
                        || row.getObject("commit_ts") == null
                        || row.getLong("commit_ts") != proof.commitTs()
                        || !row.getBoolean("has_version")) {
                    throw illegal("local candidate lacks matching committed participant and version");
                }
            }
        }
    }

    /** 只读取持久化分片栅栏状态，供失联接管重复确认物理 fencing。 */
    public FenceSnapshot snapshot(long segmentStart) {
        try (Connection connection = dataSource.getConnection()) {
            FenceRow row = lockFence(connection, segmentStart, false);
            if (row == null) {
                throw illegal("local segment fence missing");
            }
            return new FenceSnapshot(segmentStart, row.generation(), row.expiresAtMillis(),
                    "ACTIVE".equals(row.status()));
        } catch (SQLException ex) {
            throw storageError("read local segment fence failed", ex);
        }
    }

    /** 只在短事务内部保存完整 owner 身份。 */
    private record FenceRow(long segmentEnd, String ownerId, long generation,
                            long expiresAtMillis, String status) {
    }

    /** 共享或排他锁定一条分片 fence 行，调用方决定是否允许重放。 */
    private FenceRow lockFence(Connection connection, long segmentStart, boolean exclusive) throws SQLException {
        String sql = "select segment_end, owner_id, generation, expires_at, status"
                + " from mvcc_local_segment_fence_v1 where segment_start = ?"
                + (exclusive ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, segmentStart);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? new FenceRow(row.getLong(1), row.getString(2),
                        row.getLong(3), row.getLong(4), row.getString(5)) : null;
            }
        }
    }

    /** 新段安装与缺行 tombstone 写入共享一条串行栅栏。 */
    private long lockInstallState(Connection connection) throws SQLException {
        try (PreparedStatement state = connection.prepareStatement("""
                select retired_through_ts from mvcc_local_segment_state_v1
                where singleton_id = 1 for update
                """); ResultSet row = state.executeQuery()) {
            if (!row.next()) {
                throw illegal("local segment install state missing");
            }
            return row.getLong(1);
        }
    }

    /** 非锁定读取只用于不可逆退役的重放证明，安装与删除仍使用状态行排他锁。 */
    private long readRetiredThrough(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select retired_through_ts from mvcc_local_segment_state_v1
                where singleton_id = 1
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw illegal("local segment retirement state missing");
            }
            return row.getLong(1);
        }
    }

    /** 分片段范围重叠会破坏时间槽的唯一归属，安装和缺行 fencing 都须拒绝。 */
    private void requireNoOverlap(Connection connection, PublicationSegmentLease lease) throws SQLException {
        try (PreparedStatement overlap = connection.prepareStatement("""
                select segment_start from mvcc_local_segment_fence_v1
                where segment_start <= ? and segment_end >= ? limit 1
                """)) {
            overlap.setLong(1, lease.lastTs());
            overlap.setLong(2, lease.firstTs());
            try (ResultSet row = overlap.executeQuery()) {
                if (row.next()) {
                    throw illegal("local segment range overlaps another owner");
                }
            }
        }
    }

    /** 不允许另一个 owner、范围或代数复用已安装的 shard fence。 */
    private void requireSameLease(PublicationSegmentLease expected, FenceRow current) {
        if (current == null || current.segmentEnd() != expected.lastTs()
                || !current.ownerId().equals(expected.ownerId())) {
            throw illegal("local segment owner identity differs from reserved range");
        }
        if (current.generation() != expected.generation()) {
            if ("FENCED".equals(current.status()) && expected.generation() < Long.MAX_VALUE
                    && current.generation() == expected.generation() + 1) {
                throw aborted("old local segment generation was fenced");
            }
            throw illegal("local segment generation differs from reserved range");
        }
    }

    /** 初始化后的 schema 版本须精确匹配，旧表不能静默作为新协议运行。 */
    private void requireSchemaVersion(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select schema_version, retired_through_ts
                from mvcc_local_segment_state_v1 where singleton_id = 1
                """); ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getInt("schema_version") != 2
                    || row.getLong("retired_through_ts") < 0) {
                throw illegal("local segment fence schema version differs");
            }
        }
    }

    /** 分片数据库时钟只判定本地租约，控制库时钟由中心单独处理。 */
    private long databaseNow(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("local segment database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 拒绝不可信 owner 身份及无界本地租约。 */
    private void validateLease(PublicationSegmentLease lease, long leaseMillis) {
        Objects.requireNonNull(lease, "lease");
        if (!lease.ownerId().matches("[A-Za-z0-9._:-]{1,128}")
                || leaseMillis < 1 || leaseMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid local segment owner or lease duration");
        }
    }

    /** 本地持久身份不匹配时禁止继续恢复或安装。 */
    private MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /** 已失去旧代数时只允许读取已有证明，不能创建新候选。 */
    private MvccException aborted(String message) {
        return new MvccException(ErrorCode.TRANSACTION_ABORTED, message, false, null);
    }

    /** 数据库失败保留异常链，由上层决定结果未知或可重试接管。 */
    private MvccException storageError(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }
}

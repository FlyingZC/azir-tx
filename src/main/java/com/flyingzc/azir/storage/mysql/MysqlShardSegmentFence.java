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

/**
 * Durable owner fence for single shard local; the centre must finish physics before taking over.
 */
public final class MysqlShardSegmentFence {

    private final String shardId;

    private final DataSource dataSource;

    private volatile boolean localSingleKeyProcedureReady;

    private volatile boolean localSingleKeyProcedureV3Ready;

    private volatile boolean useLocalSingleKeyProcedure;

    private volatile boolean useLocalSingleKeyProcedureV3;

    /**
     * Staples the shard fence to a determined shard route and physics connection pool.
     *
     * @param shardId logical shard identifier
     * @param dataSource database connection source
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MysqlShardSegmentFence(final String shardId, final DataSource dataSource) {
        TransactionMetadataValidation.validateParticipant(0, shardId);
        if (shardId.length() > 128) {
            throw new IllegalArgumentException("local segment shard identity is too long");
        }
        this.shardId = shardId;
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Returns the target shard which must be precisely bound to the future control segment.
     *
     * @return the operation result
     */
    public String shardId() {
        return shardId;
    }

    /**
     * Shard version and fence must be written in the same physical database transaction.
     *
     * @param candidate candidate
     * @return the operation result
     */
    boolean usesDataSource(final DataSource candidate) {
        return dataSource == candidate;
    }

    /**
     * Creates a stand-alone version table only visible, the default MySQL participant does not implicitly enable local first commit protocols.
     */
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
        } catch (final SQLException ex) {
            throw storageError("initialize local segment fence failed", ex);
        }
    }

    /**
     * The verify version local routine exists after restart, and Java Multi-Speech Back path is retained when missing.
     */
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
        } catch (final SQLException ex) {
            throw storageError("inspect local-first single-key procedure failed", ex);
        }
    }

    /**
     * The second path must be visiblely enabled and the verify Java local transaction default continues.
     */
    public void enableSingleKeyProcedure() {
        if (!localSingleKeyProcedureReady) {
            throw illegal("local-first single-key procedure is not ready");
        }
        useLocalSingleKeyProcedure = true;
        useLocalSingleKeyProcedureV3 = false;
    }

    /**
     * Only the visible capacity test selects to avoid a direct terminal state process for pre-reading snapshot.
     */
    public void enableSingleKeyProcedureV3() {
        if (!localSingleKeyProcedureV3Ready) {
            throw illegal("local-first single-key v3 procedure is not ready");
        }
        useLocalSingleKeyProcedure = true;
        useLocalSingleKeyProcedureV3 = true;
    }

    /**
     * The shard adaptor selects a server call only once when it is clearly activated and the routine has been detected.
     *
     * @return the operation result
     */
    boolean singleKeyProcedureEnabled() {
        return useLocalSingleKeyProcedure && localSingleKeyProcedureReady;
    }

    /**
     * Single-key physics commit completes fence, conflict, version, terminal state and candidate certificates in a single server call.
     *
     * @param lease lease
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param mutation mutation
     * @throws SQLException if the operation cannot complete
     */
    void commitSingleKey(final PublicationSegmentLease lease, final long txId, final long startTs, final long commitTs,
                         final long deadlineMillis, final IsolationLevel isolationLevel, final Mutation mutation) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        Objects.requireNonNull(mutation, "mutation");
        validateLease(lease, 1);
        if (!singleKeyProcedureEnabled() || commitTs < lease.firstTs() || commitTs > lease.lastTs()) {
            throw illegal("local-first single-key procedure is unavailable for this segment");
        }
        String routine = useLocalSingleKeyProcedureV3
                ? "mvcc_local_first_commit_one_v3"
                : "mvcc_local_first_commit_one_v1";
        try (
                Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement(
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
        } catch (final SQLException ex) {
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

    /**
     * The version routine does not modify the old storage process and the installation response is unknown only reused.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureSingleKeyCommitProcedure(final Connection connection) throws SQLException {
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
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /**
     * Release 3 first inserts an arbitration identity with the only participant to avoid a consistent reading of snapshot before the key lock.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureSingleKeyCommitProcedureV3(final Connection connection) throws SQLException {
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
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /**
     * A new section is installed to target shard for each cross-centre; the exact same response is lost replay and not renewed.
     *
     * @param lease lease
     * @param leaseMillis lease millis
     * @return the operation result
     */
    public FenceSnapshot install(final PublicationSegmentLease lease, final long leaseMillis) {
        validateLease(lease, leaseMillis);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                requireSchemaVersion(connection);
                // Serialized installation to prevent the overlap of different band ranges and share the same commit timestamp.
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
        } catch (final SQLException ex) {
            throw storageError("install local segment fence failed", ex);
        }
    }

    /**
     * Re-letting shard database hours only if the current shard algebra is still valid.
     *
     * @param lease lease
     * @param leaseMillis lease millis
     * @return the operation result
     */
    public FenceSnapshot renew(final PublicationSegmentLease lease, final long leaseMillis) {
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
        } catch (final SQLException ex) {
            throw storageError("renew local segment fence failed", ex);
        }
    }

    /**
     * Freezing to wait for the end of local commit transaction with a shared lock and then rejecting the old algebra for new commit.
     *
     * @param lease lease
     * @return the operation result
     */
    public boolean fence(final PublicationSegmentLease lease) {
        validateLease(lease, 1);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                requireSchemaVersion(connection);
                // Installing and missing lines Fencing state line; first sealing will leave durable tombstone to prevent late installation.
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
        } catch (final SQLException ex) {
            throw storageError("fence local segment failed", ex);
        }
    }

    /**
     * Shard commit transaction is called in the same JDBC transaction and holds a shared lock until local terminal state drops.
     *
     * @param connection connection
     * @param lease lease
     * @param commitTs durable commit timestamp
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    boolean lockForCommit(final Connection connection, final PublicationSegmentLease lease, final long commitTs) throws SQLException {
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

    /**
     * Inserts recovery proof from the same transaction where version and local terminal state are found by an shard adaptor.
     *
     * @param connection connection
     * @param lease lease
     * @param proof proof
     * @throws SQLException if the operation cannot complete
     */
    void insertCandidate(final Connection connection, final PublicationSegmentLease lease, final CandidateProof proof) throws SQLException {
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

    /**
     * Local commit Response replay after loss must check the same paragraph, transaction, timestamp and isolation levels.
     *
     * @param connection connection
     * @param lease lease
     * @param expected expected
     * @throws SQLException if the operation cannot complete
     */
    void requireCandidateReplay(final Connection connection, final PublicationSegmentLease lease, final CandidateProof expected) throws SQLException {
        requireCandidateIdentity(lease, expected);
        CandidateProof actual = readCandidate(connection, expected.txId(), true).orElseThrow(() -> illegal("committed local participant has no segment candidate proof"));
        if (!actual.equals(expected)) {
            throw illegal("local segment candidate replay changed durable identity");
        }
    }

    /**
     * Press transaction identifier to read the dropped shard candidate and do not rely on the coordinator memory when responding to unknown.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<CandidateProof> candidate(final long txId) {
        try (Connection connection = dataSource.getConnection()) {
            return readCandidate(connection, txId, false);
        } catch (final SQLException ex) {
            throw storageError("read local segment candidate failed", ex);
        }
    }

    /**
     * Check shard candidate, local COMMITTED terminal state and dropped version before filling decision.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<CandidateProof> verifiedCandidate(final long txId) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<CandidateProof> proof = readCandidate(connection, txId, false);
            if (proof.isPresent()) {
                verifyCandidate(connection, proof.get());
            }
            return proof;
        } catch (final SQLException ex) {
            throw storageError("verify local segment candidate failed", ex);
        }
    }

    /**
     * A single shard query cross-checks the evidence of candidates, participant terminal state and version and rejects it as a whole without one item.
     *
     * @param txIds tx ids
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public List<CandidateProof> verifiedCandidates(final List<Long> txIds) {
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
        } catch (final SQLException ex) {
            throw storageError("verify local candidate batch failed", ex);
        }
    }

    /**
     * Only when the physics fencing is successful can this section be used as a stable recovery input.
     *
     * @param lease lease
     * @param afterCommitTs after commit ts
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<CandidateProof> scanFenced(final PublicationSegmentLease lease, final long afterCommitTs, final int limit) {
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
        } catch (final SQLException ex) {
            throw storageError("scan fenced local candidates failed", ex);
        }
    }

    /**
     * Physical lines can still be deleted with durable -high watermark confirmation that old paragraphs cannot be re-installed.
     *
     * @param lease lease
     * @return the operation result
     */
    public boolean isFenceRetired(final PublicationSegmentLease lease) {
        validateLease(lease, 1);
        try (Connection connection = dataSource.getConnection()) {
            return readRetiredThrough(connection) >= lease.lastTs()
                    && lockFence(connection, lease.firstTs(), false) == null;
        } catch (final SQLException ex) {
            throw storageError("check local segment physical retirement failed", ex);
        }
    }

    /**
     * Shard atomic boosts the high watermark rejection of old installation and deletes FENCED rows when the centre has authorized retirement.
     *
     * @param proof proof
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean purgeFence(final FinalizedFenceGcProof proof) {
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
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local fence retirement requires durable state lookup", false, null, ex);
        }
    }

    /**
     * Idempotent removes this shard candidate after the center is closed and durable has been released to remove the local GC lower boundary.
     *
     * @param proof proof
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean purgeCandidate(final FinalizedCandidateProof proof) {
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
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local candidate GC result requires durable lookup", false, expected.txId(), ex);
        }
    }

    /**
     * After the entire section terminal state has been released, all candidates are deleted at a time within the physical FENCED algebra.
     *
     * @param proof proof
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public int purgeFencedSegmentCandidates(final FinalizedSegmentProof proof) {
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
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first segment candidate purge requires durable lookup", false, null, ex);
        }
    }

    /**
     * Physically sealed segments still have a key-based statistical certificate to verify the centre batch GC tag.
     *
     * @param segmentStart segment start
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    public int countCandidates(final long segmentStart) {
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
        } catch (final SQLException ex) {
            throw storageError("count local-first segment candidates failed", ex);
        }
    }

    /**
     * Background GC first reads the oldest remaining candidate snapshot and allows shard safe point to maintain a backward control library.
     *
     * @return the operation result
     */
    public OptionalLong oldestCandidateStartTs() {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select min(start_ts) from mvcc_local_segment_candidate_v1
                """); ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getObject(1) == null) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(row.getLong(1));
        } catch (final SQLException ex) {
            throw storageError("read local-first oldest candidate snapshot failed", ex);
        }
    }

    /**
     * Shard Candidates must be within the reserved range of the same centre and the owner algebra.
     *
     * @param lease lease
     * @param proof proof
     */
    private void requireCandidateIdentity(final PublicationSegmentLease lease, final CandidateProof proof) {
        Objects.requireNonNull(proof, "proof");
        if (proof.segmentStart() != lease.firstTs() || proof.commitTs() < lease.firstTs()
                || proof.commitTs() > lease.lastTs() || proof.ownerGeneration() != lease.generation()) {
            throw illegal("local candidate differs from segment owner range");
        }
    }

    /**
     * Transaction identifier Index is accurately searched for local proof, and extra-exclusive locks are obtained on replay.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Optional<CandidateProof> readCandidate(final Connection connection, final long txId, final boolean lock) throws SQLException {
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

    /**
     * Converts the durable count and timestamp in database to immutable local certificate.
     *
     * @param row row
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private CandidateProof mapCandidate(final ResultSet row) throws SQLException {
        return new CandidateProof(row.getLong("segment_start"), row.getLong("tx_id"),
                row.getLong("start_ts"), row.getLong("commit_ts"), row.getLong("deadline_millis"),
                IsolationLevel.valueOf(row.getString("isolation_level")), row.getLong("owner_generation"));
    }

    /**
     * Single shard version chain keeps at least one local durable commit record identical to the candidate timestamp.
     *
     * @param connection connection
     * @param proof proof
     * @throws SQLException if the operation cannot complete
     */
    private void verifyCandidate(final Connection connection, final CandidateProof proof) throws SQLException {
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

    /**
     * Read-only takes the durable storage shard fence state for loss of connection to take over a duplicate confirmation of physics.
     *
     * @param segmentStart segment start
     * @return the operation result
     */
    public FenceSnapshot snapshot(final long segmentStart) {
        try (Connection connection = dataSource.getConnection()) {
            FenceRow row = lockFence(connection, segmentStart, false);
            if (row == null) {
                throw illegal("local segment fence missing");
            }
            return new FenceSnapshot(segmentStart, row.generation(), row.expiresAtMillis(),
                    "ACTIVE".equals(row.status()));
        } catch (final SQLException ex) {
            throw storageError("read local segment fence failed", ex);
        }
    }

    /**
     * Share or separate a shard fence line and caller to decide whether replay is allowed.
     *
     * @param connection connection
     * @param segmentStart segment start
     * @param exclusive exclusive
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private FenceRow lockFence(final Connection connection, final long segmentStart, final boolean exclusive) throws SQLException {
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

    /**
     * New paragraph installation and line absence tombstone write to share a string fence.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private long lockInstallState(final Connection connection) throws SQLException {
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

    /**
     * The replay certificate, which is not locked to read only for the decommissioning of irreversible, installs and removes a still-used state line exclusion lock.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private long readRetiredThrough(final Connection connection) throws SQLException {
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

    /**
     * The overlap of the shard segment will destroy the single ownership time slot, and installation and absences are subject to rejection.
     *
     * @param connection connection
     * @param lease lease
     * @throws SQLException if the operation cannot complete
     */
    private void requireNoOverlap(final Connection connection, final PublicationSegmentLease lease) throws SQLException {
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

    /**
     * Another homeer, range or algebra is not allowed to reuse the installed shell fence.
     *
     * @param expected expected
     * @param current current
     */
    private void requireSameLease(final PublicationSegmentLease expected, final FenceRow current) {
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

    /**
     * An initialized schema version must be accurately matched and the old table cannot run silently as a new protocol.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void requireSchemaVersion(final Connection connection) throws SQLException {
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

    /**
     * Shard database clock only determines local lease and the control library clock is handled separately by the centre.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private long databaseNow(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("local segment database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Deny untrustworthy own identity and unbounded local lease.
     *
     * @param lease lease
     * @param leaseMillis lease millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void validateLease(final PublicationSegmentLease lease, final long leaseMillis) {
        Objects.requireNonNull(lease, "lease");
        if (!lease.ownerId().matches("[A-Za-z0-9._:-]{1,128}")
                || leaseMillis < 1 || leaseMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid local segment owner or lease duration");
        }
    }

    /**
     * The continuation of recovery or installation is prohibited if local durable identity does not match.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * The old algebra is only allowed to read the certificates and cannot create new candidates when it has been lost.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException aborted(final String message) {
        return new MvccException(ErrorCode.TRANSACTION_ABORTED, message, false, null);
    }

    /**
     * Database failed to maintain the anomaly chain, which is taken over by a top-level decision unknown outcome or retry.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private MvccException storageError(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * Returns local lease under shard database clock without reverting to the control library absolute time.
     *
     * @param segmentStart segment start
     * @param generation generation
     * @param expiresAtMillis expires at millis
     * @param active active
     */
    public record FenceSnapshot(long segmentStart, long generation, long expiresAtMillis, boolean active) {

        /**
         * Rejects the local fence view with no durable algebra or deadline.
         *
         * @param segmentStart segment start
         * @param generation generation
         * @param expiresAtMillis expires at millis
         * @param active active
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public FenceSnapshot {
            if (segmentStart < 1 || generation < 1 || expiresAtMillis < 1) {
                throw new IllegalArgumentException("invalid local segment fence snapshot");
            }
        }
    }

    /**
     * Local with shard version and participant COMMITTED terminal state.
     *
     * @param segmentStart segment start
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param ownerGeneration owner generation
     */
    public record CandidateProof(long segmentStart, long txId, long startTs, long commitTs,
            long deadlineMillis, IsolationLevel isolationLevel, long ownerGeneration) {

        /**
         * The candidate must have a positive algebra and commit time is later than it reads snapshot.
         *
         * @param segmentStart segment start
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param deadlineMillis deadline millis
         * @param isolationLevel global transaction isolation level
         * @param ownerGeneration owner generation
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public CandidateProof {
            Objects.requireNonNull(isolationLevel, "isolationLevel");
            if (segmentStart < 1 || txId == 0 || startTs < 0 || commitTs <= startTs
                    || deadlineMillis < 1 || ownerGeneration < 1) {
                throw new IllegalArgumentException("invalid local segment candidate proof");
            }
        }
    }

    /**
     * Only save full owner identity in short transaction.
     *
     * @param segmentEnd segment end
     * @param ownerId owner id
     * @param generation generation
     * @param expiresAtMillis expires at millis
     * @param status status
     */
    private record FenceRow(long segmentEnd, String ownerId, long generation,
            long expiresAtMillis, String status) {
    }
}

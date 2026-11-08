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
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.mysql.MysqlLocalFirstMetadataView;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger.CommittedProof;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.CommittedIntentProof;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.storage.VersionedValue;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Stores transaction intents, rollback markers and committed versions in a MySQL shard.
 */
public class MysqlStorageAdapter implements StorageAdapter {

    private static final int BATCH_SIZE = 512;

    private static final int MAX_PAGE_SIZE = 4096;

    private static final long TERMINAL_RETENTION_MS = 60_000L;

    private final String shardId;

    private final DataSource dataSource;

    private volatile TransactionMetadataStore metadataStore;

    private volatile boolean singleKeyPrewriteReady;

    private volatile boolean singleKeyCommitReady;

    private volatile boolean singleKeyIntentCommitReady;

    private volatile boolean singleKeyCommitV2Ready;

    private volatile boolean useSingleKeyCommitV2;

    private volatile MysqlVectorReservationStore vectorReservations;

    private long pendingScanAfter = Long.MIN_VALUE;

    private long terminalGcAfter = Long.MIN_VALUE;

    private boolean terminalGcTurn;

    /**
     * Vector background Clean read-only This real foreground data source to check physical shard authority.
     *
     * @return the operation result
     */
    DataSource vectorDataSource() {
        return dataSource;
    }

    /**
     * Constructs the adaptor for later binding of centre authority; the injected data source should configure its own conectTimeout and socketTimeout.
     *
     * @param shardId logical shard identifier
     * @param dataSource database connection source
     */
    public MysqlStorageAdapter(final String shardId, final DataSource dataSource) {
        this(shardId, dataSource, null);
    }

    /**
     * Construct an adaptor that can interpret centre decision; the driver of external data sources timeout cannot be replaced by cooperative recovery budget.
     *
     * @param shardId logical shard identifier
     * @param dataSource database connection source
     * @param metadataStore metadata store
     */
    public MysqlStorageAdapter(final String shardId, final DataSource dataSource, final TransactionMetadataStore metadataStore) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.metadataStore = metadataStore;
    }

    /**
     * Returns the participating shard number.
     *
     * @return the operation result
     */
    @Override
    public String shardId() {
        return shardId;
    }

    /**
     * Only the first binding or repeated binding of the same example is allowed to avoid silent replacement centre decision authority.
     *
     * @param store store
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public synchronized void configureMetadataStore(final TransactionMetadataStore store) {
        Objects.requireNonNull(store, "store");
        if (metadataStore != null && metadataStore != store) {
            throw new IllegalStateException("adapter already belongs to another metadata authority");
        }
        metadataStore = store;
    }

    /**
     * Is that version and participant commit records can be dropped on atomic in the same local transaction.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsAtomicDirectCommit() {
        return true;
    }

    /**
     * Intent published but not yet materialization will be parsed only after binding the centre authority.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsCommittedIntentResolution() {
        return metadataStore != null;
    }

    /**
     * Initialize protocol table and query index without extrapolating history of missing old data transaction decision.
     */
    @Override
    public void initialize() {
        List<String> statements = List.of("""
                create table if not exists mvcc_data (
                  user_key varbinary(256) not null, commit_ts bigint not null, tx_id bigint not null,
                  deleted tinyint not null default 0, value blob, primary key (user_key, commit_ts)
                ) engine = InnoDB
                """, """
                create table if not exists mvcc_intent (
                  user_key varbinary(256) not null, tx_id bigint not null, start_ts bigint not null,
                  deleted tinyint not null default 0, value blob, expire_at bigint not null,
                  primary key (user_key)
                ) engine = InnoDB
                """, """
                create table if not exists mvcc_rollback (
                  user_key varbinary(256) not null, tx_id bigint not null, start_ts bigint not null,
                  created_at bigint not null, primary key (user_key, tx_id)
                ) engine = InnoDB
                """, """
                create table if not exists mvcc_participant_txn (
                  tx_id bigint not null, start_ts bigint not null, status varchar(16) not null,
                  commit_ts bigint, updated_at bigint not null, primary key (tx_id)
                ) engine = InnoDB
                """, """
                create table if not exists mvcc_key_lock (
                  user_key varbinary(256) not null, primary key (user_key)
                ) engine = InnoDB
                """, """
                create table if not exists mvcc_local_safe_point (
                  singleton_id tinyint not null, safe_point bigint not null, gc_after_key varbinary(256),
                  primary key (singleton_id)
                ) engine = InnoDB
                """, """
                insert into mvcc_local_safe_point(singleton_id, safe_point) values (1, 0)
                on duplicate key update singleton_id = singleton_id
                """);
        try (Connection connection = dataSource.getConnection()) {
            for (String sql : statements) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.execute();
                }
            }
            ensureIndex(connection, "mvcc_intent", "idx_mvcc_intent_tx_id_user_key", "tx_id, user_key");
            ensureIndex(connection, "mvcc_rollback", "idx_mvcc_rollback_tx_id_user_key", "tx_id, user_key");
            ensureIndex(connection, "mvcc_data", "idx_mvcc_data_tx_id_user_key", "tx_id, user_key, commit_ts");
            ensureIndex(connection, "mvcc_participant_txn", "idx_mvcc_participant_terminal", "status, start_ts, updated_at");
            ensureGcCursorColumn(connection);
            ensureSingleKeyPrewriteProcedure(connection);
            singleKeyPrewriteReady = true;
            ensureSingleKeyCommitProcedure(connection);
            singleKeyCommitReady = true;
            ensureSingleKeyIntentCommitProcedure(connection);
            singleKeyIntentCommitReady = true;
            ensureSingleKeyCommitProcedureV2(connection);
            singleKeyCommitV2Ready = true;
        } catch (final SQLException ex) {
            throw storageError(null, "initialize schema failed", ex);
        }
    }

    /**
     * Visiblely enable the layout of durable sub-column numbers without affecting the default participant table and the old transaction entry.
     *
     * @param stripes stripes
     */
    public synchronized void initializeVectorProtocol(final int stripes) {
        MysqlVectorReservationStore store = new MysqlVectorReservationStore(dataSource, stripes);
        try {
            store.initialize();
            vectorReservations = store;
        } catch (final SQLException ex) {
            throw storageError(null, "initialize vector reservation protocol failed", ex);
        }
    }

    /**
     * Resuscitation only detects a single-key fast-track routine and, when missing, refuses to run high performance books in uninitialised storage.
     */
    public void requireInitializedFastPath() {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement("""
                        select routine_name from information_schema.routines
                        where routine_schema = database()
                          and routine_name in ('mvcc_prewrite_one_v2', 'mvcc_commit_one_v1', 'mvcc_commit_one_v2')
                        """)) {
            boolean prewriteReady = false;
            boolean commitReady = false;
            boolean commitV2Ready = false;
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if ("mvcc_prewrite_one_v2".equalsIgnoreCase(rows.getString(1))) {
                        prewriteReady = true;
                    } else if ("mvcc_commit_one_v1".equalsIgnoreCase(rows.getString(1))) {
                        commitReady = true;
                    } else if ("mvcc_commit_one_v2".equalsIgnoreCase(rows.getString(1))) {
                        commitV2Ready = true;
                    }
                }
            }
            if (!prewriteReady || !commitReady && !commitV2Ready) {
                throw illegal(0, "single-key participant routines are not initialized");
            }
            singleKeyPrewriteReady = true;
            singleKeyCommitReady = true;
            singleKeyCommitV2Ready = commitV2Ready;
        } catch (final SQLException ex) {
            throw storageError(null, "inspect participant routines failed", ex);
        }
    }

    /**
     * Only when the second version of the routine is detected will it be visible, and the old version remains the default materialization path.
     */
    public void enableSingleKeyCommitV2() {
        if (!singleKeyCommitV2Ready) {
            throw illegal(0, "single-key commit v2 routine is not initialized");
        }
        useSingleKeyCommitV2 = true;
    }

    /**
     * Install a single key prewrite routine, and version is fixed to avoid rewrite the database code being implemented when it is upgraded.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureSingleKeyPrewriteProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_prewrite_one_v2' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_prewrite_one_v2(in p_tx_id bigint, in p_start bigint,
                                                      in p_key varbinary(256), in p_deleted tinyint,
                                                      in p_value blob, in p_expire_at bigint)
                begin
                  declare v_safe bigint default null;
                  declare v_record_start bigint default null;
                  declare v_record_status varchar(16) default null;
                  declare v_newer int default 0;
                  declare v_intent_tx bigint default null;
                  declare v_intent_start bigint default null;
                  declare v_rolled int default 0;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  if v_safe is null then
                    signal sqlstate '45000' set mysql_errno = 31001,
                      message_text = 'MVCC_LOCAL_SAFE_POINT_MISSING';
                  end if;
                  insert into mvcc_participant_txn(tx_id, start_ts, status, commit_ts, updated_at)
                    values (p_tx_id, p_start, 'ACTIVE', null,
                            cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                    on duplicate key update tx_id = tx_id;
                  select start_ts, status into v_record_start, v_record_status
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_record_status <> 'ACTIVE' then
                    signal sqlstate '45000' set mysql_errno = 31002,
                      message_text = 'MVCC_PARTICIPANT_NOT_ACTIVE';
                  end if;
                  if v_record_start <> p_start then
                    signal sqlstate '45000' set mysql_errno = 31003,
                      message_text = 'MVCC_PARTICIPANT_START_MISMATCH';
                  end if;
                  if p_start < v_safe then
                    signal sqlstate '45000' set mysql_errno = 31004,
                      message_text = 'MVCC_SNAPSHOT_TOO_OLD';
                  end if;
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select exists(select 1 from mvcc_data
                                where user_key = p_key and commit_ts > p_start)
                    into v_newer;
                  if v_newer = 1 then
                    signal sqlstate '45000' set mysql_errno = 31005,
                      message_text = 'MVCC_NEWER_COMMITTED_VERSION';
                  end if;
                  -- 稳定键行已串行化同键写者；普通读避免对不存在意向获取间隙锁。
                  select tx_id, start_ts into v_intent_tx, v_intent_start
                    from mvcc_intent where user_key = p_key;
                  if v_intent_tx is not null and v_intent_tx <> p_tx_id then
                    signal sqlstate '45000' set mysql_errno = 31006,
                      message_text = 'MVCC_FOREIGN_INTENT';
                  end if;
                  if v_intent_tx is not null and v_intent_start <> p_start then
                    signal sqlstate '45000' set mysql_errno = 31003,
                      message_text = 'MVCC_INTENT_START_MISMATCH';
                  end if;
                  select exists(select 1 from mvcc_rollback
                                where user_key = p_key and tx_id = p_tx_id)
                    into v_rolled;
                  if v_rolled = 1 then
                    signal sqlstate '45000' set mysql_errno = 31007,
                      message_text = 'MVCC_ROLLBACK_MARKER';
                  end if;
                  insert into mvcc_intent(user_key, tx_id, start_ts, deleted, value, expire_at)
                    values (p_key, p_tx_id, p_start, p_deleted, p_value, p_expire_at)
                    on duplicate key update deleted = values(deleted), value = values(value),
                                            expire_at = values(expire_at);
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
     * Install single-key intent materialization routines to clean version, participant terminal state and optional intent in the same local transaction.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureSingleKeyCommitProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_commit_one_v1' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_commit_one_v1(in p_tx_id bigint, in p_start bigint,
                                                    in p_commit bigint, in p_key varbinary(256),
                                                    in p_deleted tinyint, in p_value blob,
                                                    in p_cleanup boolean)
                begin
                  declare v_safe bigint default null;
                  declare v_start bigint default null;
                  declare v_status varchar(16) default null;
                  declare v_commit bigint default null;
                  declare v_count int default 0;
                  declare v_intent_tx bigint default null;
                  declare v_intent_start bigint default null;
                  declare v_intent_deleted tinyint default null;
                  declare v_intent_value blob;
                  declare v_version_tx bigint default null;
                  declare v_version_deleted tinyint default null;
                  declare v_version_value blob;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  -- 与常规路径保持安全点、参与者、稳定键的加锁顺序。
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select start_ts, status, commit_ts into v_start, v_status, v_commit
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_safe is null or v_status is null or v_start <> p_start or p_commit <= v_start then
                    signal sqlstate '45000' set mysql_errno = 32001,
                      message_text = 'MVCC_COMMIT_PARTICIPANT_INVALID';
                  end if;
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  if v_status = 'COMMITTED' then
                    -- 终态重放仍要核对完整单键版本，禁止用相同事务号伪造不同值。
                    if v_commit is null or v_commit <> p_commit then
                      signal sqlstate '45000' set mysql_errno = 32002,
                        message_text = 'MVCC_COMMIT_REPLAY_TIMESTAMP';
                    end if;
                    select count(*) into v_count from mvcc_data
                      where tx_id = p_tx_id and commit_ts = p_commit;
                    select tx_id, deleted, value
                      into v_version_tx, v_version_deleted, v_version_value
                      from mvcc_data where user_key = p_key and commit_ts = p_commit;
                    if v_count <> 1 or v_version_tx is null or v_version_tx <> p_tx_id
                        or v_version_deleted <> p_deleted or not (v_version_value <=> p_value) then
                      signal sqlstate '45000' set mysql_errno = 32003,
                        message_text = 'MVCC_COMMIT_REPLAY_VALUE';
                    end if;
                  elseif v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 32004,
                      message_text = 'MVCC_COMMIT_PARTICIPANT_ABORTED';
                  elseif v_status in ('ACTIVE', 'PREPARING') and v_commit is null then
                    select count(*) into v_count from mvcc_intent where tx_id = p_tx_id;
                    select tx_id, start_ts, deleted, value
                      into v_intent_tx, v_intent_start, v_intent_deleted, v_intent_value
                      from mvcc_intent where user_key = p_key;
                    if v_count <> 1 or v_intent_tx is null or v_intent_tx <> p_tx_id
                        or v_intent_start <> p_start or v_intent_deleted <> p_deleted
                        or not (v_intent_value <=> p_value) then
                      signal sqlstate '45000' set mysql_errno = 32005,
                        message_text = 'MVCC_COMMIT_INTENT_MISMATCH';
                    end if;
                    insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                      values (p_key, p_commit, p_tx_id, p_deleted, p_value);
                    update mvcc_participant_txn set status = 'COMMITTED', commit_ts = p_commit,
                      updated_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                      where tx_id = p_tx_id;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32001,
                        message_text = 'MVCC_COMMIT_PARTICIPANT_LOST';
                    end if;
                  else
                    signal sqlstate '45000' set mysql_errno = 32001,
                      message_text = 'MVCC_COMMIT_PARTICIPANT_INVALID';
                  end if;
                  if p_cleanup then
                    select count(*) into v_count from mvcc_intent where tx_id = p_tx_id;
                    if v_count > 1 then
                      signal sqlstate '45000' set mysql_errno = 32005,
                        message_text = 'MVCC_COMMIT_EXTRA_INTENT';
                    end if;
                    if v_count = 1 then
                      set v_intent_tx = null;
                      select tx_id, start_ts, deleted, value
                        into v_intent_tx, v_intent_start, v_intent_deleted, v_intent_value
                        from mvcc_intent where user_key = p_key;
                      if v_intent_tx is null or v_intent_tx <> p_tx_id
                          or v_intent_start <> p_start or v_intent_deleted <> p_deleted
                          or not (v_intent_value <=> p_value) then
                        signal sqlstate '45000' set mysql_errno = 32005,
                          message_text = 'MVCC_COMMIT_CLEANUP_MISMATCH';
                      end if;
                      delete from mvcc_intent where user_key = p_key and tx_id = p_tx_id;
                      if row_count() <> 1 then
                        signal sqlstate '45000' set mysql_errno = 32005,
                          message_text = 'MVCC_COMMIT_CLEANUP_LOST';
                      end if;
                    end if;
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
     * Install a single-key intent recovery routine that does not require foreground write set, read and materialization local intent in shard transaction.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureSingleKeyIntentCommitProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_commit_intent_one_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_commit_intent_one_v1(in p_tx_id bigint, in p_start bigint,
                                                          in p_commit bigint)
                begin
                  declare v_safe bigint default null;
                  declare v_start bigint default null;
                  declare v_status varchar(16) default null;
                  declare v_commit bigint default null;
                  declare v_count int default 0;
                  declare v_key varbinary(256) default null;
                  declare v_intent_start bigint default null;
                  declare v_deleted tinyint default null;
                  declare v_value blob;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select start_ts, status, commit_ts into v_start, v_status, v_commit
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_safe is null or v_status is null or v_start <> p_start
                      or p_commit <= p_start then
                    signal sqlstate '45000' set mysql_errno = 32101,
                      message_text = 'MVCC_INTENT_COMMIT_PARTICIPANT_INVALID';
                  end if;
                  if v_status = 'COMMITTED' then
                    if v_commit is null or v_commit <> p_commit then
                      signal sqlstate '45000' set mysql_errno = 32102,
                        message_text = 'MVCC_INTENT_COMMIT_REPLAY_TIMESTAMP';
                    end if;
                    select count(*) into v_count from mvcc_data
                      where tx_id = p_tx_id and commit_ts = p_commit;
                    if v_count > 1 then
                      signal sqlstate '45000' set mysql_errno = 32106,
                        message_text = 'MVCC_INTENT_COMMIT_MULTI_KEY';
                    end if;
                    if v_count <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32103,
                        message_text = 'MVCC_INTENT_COMMIT_MISSING_VERSION';
                    end if;
                    delete from mvcc_intent where tx_id = p_tx_id;
                  elseif v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 32104,
                      message_text = 'MVCC_INTENT_COMMIT_ABORTED';
                  elseif v_status in ('ACTIVE', 'PREPARING') and v_commit is null then
                    if p_start < v_safe then
                      signal sqlstate '45000' set mysql_errno = 32105,
                        message_text = 'MVCC_INTENT_COMMIT_SNAPSHOT_TOO_OLD';
                    end if;
                    select count(*) into v_count from mvcc_intent where tx_id = p_tx_id;
                    if v_count > 1 then
                      signal sqlstate '45000' set mysql_errno = 32106,
                        message_text = 'MVCC_INTENT_COMMIT_MULTI_KEY';
                    end if;
                    if v_count <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32103,
                        message_text = 'MVCC_INTENT_COMMIT_MISSING_INTENT';
                    end if;
                    select user_key, start_ts, deleted, value
                      into v_key, v_intent_start, v_deleted, v_value
                      from mvcc_intent where tx_id = p_tx_id for update;
                    if v_key is null or v_intent_start <> p_start then
                      signal sqlstate '45000' set mysql_errno = 32103,
                        message_text = 'MVCC_INTENT_COMMIT_IDENTITY';
                    end if;
                    insert into mvcc_key_lock(user_key) values (v_key)
                      on duplicate key update user_key = user_key;
                    insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                      values (v_key, p_commit, p_tx_id, v_deleted, v_value);
                    update mvcc_participant_txn set status = 'COMMITTED', commit_ts = p_commit,
                      updated_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                      where tx_id = p_tx_id and start_ts = p_start
                        and status in ('ACTIVE', 'PREPARING') and commit_ts is null;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32101,
                        message_text = 'MVCC_INTENT_COMMIT_PARTICIPANT_LOST';
                    end if;
                    delete from mvcc_intent where user_key = v_key and tx_id = p_tx_id;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32103,
                        message_text = 'MVCC_INTENT_COMMIT_CLEANUP_LOST';
                    end if;
                  else
                    signal sqlstate '45000' set mysql_errno = 32101,
                      message_text = 'MVCC_INTENT_COMMIT_PARTICIPANT_STATUS';
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
     * Installs single key materialization version 2, which combines a common first commit intent check with version for writing in a statement.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureSingleKeyCommitProcedureV2(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_commit_one_v2' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_commit_one_v2(in p_tx_id bigint, in p_start bigint,
                                                    in p_commit bigint, in p_key varbinary(256),
                                                    in p_deleted tinyint, in p_value blob,
                                                    in p_cleanup boolean)
                begin
                  declare v_safe bigint default null;
                  declare v_start bigint default null;
                  declare v_status varchar(16) default null;
                  declare v_commit bigint default null;
                  declare v_count int default 0;
                  declare v_version_tx bigint default null;
                  declare v_version_deleted tinyint default null;
                  declare v_version_value blob;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select safe_point into v_safe from mvcc_local_safe_point
                    where singleton_id = 1 for share;
                  select start_ts, status, commit_ts into v_start, v_status, v_commit
                    from mvcc_participant_txn where tx_id = p_tx_id for update;
                  if v_safe is null or v_status is null or v_start <> p_start or p_commit <= v_start then
                    signal sqlstate '45000' set mysql_errno = 32001,
                      message_text = 'MVCC_COMMIT_PARTICIPANT_INVALID';
                  end if;
                  insert into mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  if v_status = 'COMMITTED' then
                    -- 重放仍核对唯一版本及完整字节值，禁止改写原提交。
                    if v_commit is null or v_commit <> p_commit then
                      signal sqlstate '45000' set mysql_errno = 32002,
                        message_text = 'MVCC_COMMIT_REPLAY_TIMESTAMP';
                    end if;
                    select count(*) into v_count from mvcc_data
                      where tx_id = p_tx_id and commit_ts = p_commit;
                    select tx_id, deleted, value
                      into v_version_tx, v_version_deleted, v_version_value
                      from mvcc_data where user_key = p_key and commit_ts = p_commit;
                    if v_count <> 1 or v_version_tx is null or v_version_tx <> p_tx_id
                        or v_version_deleted <> p_deleted or not (v_version_value <=> p_value) then
                      signal sqlstate '45000' set mysql_errno = 32003,
                        message_text = 'MVCC_COMMIT_REPLAY_VALUE';
                    end if;
                    if p_cleanup then
                      select count(*) into v_count from mvcc_intent where tx_id = p_tx_id;
                      if v_count > 1 then
                        signal sqlstate '45000' set mysql_errno = 32005,
                          message_text = 'MVCC_COMMIT_EXTRA_INTENT';
                      end if;
                      if v_count = 1 then
                        delete from mvcc_intent where user_key = p_key and tx_id = p_tx_id
                          and start_ts = p_start and deleted = p_deleted and value <=> p_value;
                        if row_count() <> 1 then
                          signal sqlstate '45000' set mysql_errno = 32005,
                            message_text = 'MVCC_COMMIT_CLEANUP_MISMATCH';
                        end if;
                      end if;
                    end if;
                  elseif v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 32004,
                      message_text = 'MVCC_COMMIT_PARTICIPANT_ABORTED';
                  elseif v_status in ('ACTIVE', 'PREPARING') and v_commit is null then
                    -- 事务行锁串行化同一事务的预写，稳定键锁串行化同键写者。
                    insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                      select i.user_key, p_commit, p_tx_id, i.deleted, i.value
                      from mvcc_intent i
                      where i.user_key = p_key and i.tx_id = p_tx_id and i.start_ts = p_start
                        and i.deleted = p_deleted and i.value <=> p_value
                        and (select count(*) from mvcc_intent own where own.tx_id = p_tx_id) = 1;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32005,
                        message_text = 'MVCC_COMMIT_INTENT_MISMATCH';
                    end if;
                    update mvcc_participant_txn set status = 'COMMITTED', commit_ts = p_commit,
                      updated_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                      where tx_id = p_tx_id and start_ts = p_start
                        and status in ('ACTIVE', 'PREPARING') and commit_ts is null;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 32001,
                        message_text = 'MVCC_COMMIT_PARTICIPANT_LOST';
                    end if;
                    if p_cleanup then
                      delete from mvcc_intent where user_key = p_key and tx_id = p_tx_id
                        and start_ts = p_start and deleted = p_deleted and value <=> p_value;
                      if row_count() <> 1 then
                        signal sqlstate '45000' set mysql_errno = 32005,
                          message_text = 'MVCC_COMMIT_CLEANUP_LOST';
                      end if;
                    end if;
                  else
                    signal sqlstate '45000' set mysql_errno = 32001,
                      message_text = 'MVCC_COMMIT_PARTICIPANT_INVALID';
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
     * Querys the real durable storage state, does not cache ACTIVE or missing records.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public Optional<ParticipantRecord> participantRecord(final long txId) {
        try (Connection connection = dataSource.getConnection()) {
            return selectParticipant(connection, txId, false);
        } catch (final SQLException ex) {
            throw storageError(txId, "read participant record failed", ex);
        }
    }

    /**
     * Read version and intent in the same local snapshot and keep point reading tombstone.
     *
     * @param context context
     * @param key business key
     * @return the operation result
     */
    @Override
    public Optional<VersionedValue> get(final ReadContext context, final String key) {
        encodeKey(key);
        return readSnapshot(context, connection -> Optional.ofNullable(MysqlSnapshotReader.visibleValues(connection, context, List.of(key), metadataStore).get(key)));
    }

    /**
     * Internally protected reading portals can only be read with a fixed vector point, and external access to reader lease cannot be bypassed.
     *
     * @param snapshot snapshot
     * @param requesterTxId requester tx id
     * @param key business key
     * @param stripe stripe
     * @return the operation result
     */
    Optional<VersionedValue> getVector(final MysqlStripedSnapshotOracle.Snapshot snapshot,
                                       final long requesterTxId, final String key, final int stripe) {
        encodeKey(key);
        if (vectorReservations == null || !(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(requesterTxId, "vector read requires initialized stripe layout and center authority");
        }
        for (int attempt = 0;; attempt++) {
            try {
                return transaction(requesterTxId, true, connection -> {
                    requireFresh(requesterTxId, snapshot.decisionEpoch(), readWatermark(connection));
                    return MysqlProductVectorSnapshotReader.get(connection, snapshot,
                            shardId, key, stripe, requesterTxId, metadataStore);
                });
            } catch (final MysqlSnapshotReader.MissingDecisionException missing) {
                // When the old local RR view sees cleared intent, close connection and reread it again to the original global vector.
                if (attempt == 1) {
                    throw uncertain(requesterTxId,
                            "vector intent decision still missing after rebuilding local snapshot", missing);
                }
            }
        }
    }

    /**
     * The fixed vector range is read only for inside access to the package holding GC protected handles.
     *
     * @param snapshot snapshot
     * @param requesterTxId requester tx id
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    ScanPage scanVectorPage(final MysqlStripedSnapshotOracle.Snapshot snapshot, final long requesterTxId,
                            final KeyRange range, final String afterKeyExclusive, final int limit) {
        Objects.requireNonNull(range, "range");
        checkLimit(limit);
        if (range.startKeyInclusive() != null)
            encodeKey(range.startKeyInclusive());
        if (range.endKeyExclusive() != null)
            encodeKey(range.endKeyExclusive());
        if (afterKeyExclusive != null)
            encodeKey(afterKeyExclusive);
        if (vectorReservations == null || !(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(requesterTxId, "vector scan requires initialized stripe layout and center authority");
        }
        for (int attempt = 0;; attempt++) {
            try {
                return transaction(requesterTxId, true, connection -> {
                    requireFresh(requesterTxId, snapshot.decisionEpoch(), readWatermark(connection));
                    return MysqlProductVectorSnapshotReader.page(connection, snapshot,
                            shardId, range, afterKeyExclusive, limit, requesterTxId, metadataStore);
                });
            } catch (final MysqlSnapshotReader.MissingDecisionException missing) {
                if (attempt == 1) {
                    throw uncertain(requesterTxId,
                            "vector scan decision still missing after rebuilding local snapshot", missing);
                }
            }
        }
    }

    /**
     * Cycle bounded page combination. The caller that requires current consumption should use the page break interface.
     *
     * @param context context
     * @param keyRange key range
     * @return the operation result
     */
    @Override
    public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
        List<VersionedValue> values = new ArrayList<>();
        String after = null;
        do {
            ScanPage page = scanPage(context, keyRange, after, BATCH_SIZE);
            values.addAll(page.values());
            after = page.nextKey();
        } while (after != null);
        return values;
    }

    /**
     * Batch Reads one page original key, latest visible version and intent, closes all JDBC resources before returning.
     *
     * @param context context
     * @param keyRange key range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public ScanPage scanPage(final ReadContext context, final KeyRange keyRange, final String afterKeyExclusive, final int limit) {
        checkLimit(limit);
        Objects.requireNonNull(keyRange, "keyRange");
        return readSnapshot(context, connection -> MysqlSnapshotReader.page(connection, context, keyRange, afterKeyExclusive, limit, metadataStore));
    }

    /**
     * Decision recreates local RR snapshot only once when it is missing, and the original global readTs, key range and page break parameters remain unchanged.
     *
     * @param <T> value type
     * @param context context
     * @param read read
     * @return the operation result
     */
    private <T> T readSnapshot(final ReadContext context, final SqlWork<T> read) {
        for (int attempt = 0;; attempt++) {
            try {
                return transaction(context.txId(), true, connection -> {
                    requireFresh(context.txId(), context.readTs(), readWatermark(connection));
                    return read.execute(connection);
                });
            } catch (final MysqlSnapshotReader.MissingDecisionException missing) {
                // Transaction is rollback and closes the old connection; returning the old or empty cache value or page is prohibited when it is still missing.
                if (attempt == 1) {
                    throw uncertain(context.txId(), "intent decision still missing after rebuilding local snapshot", missing);
                }
            }
        }
    }

    /**
     * First protect transaction terminal state and stabilize key locks, then press bounded batch to detect conflict and atomic write the whole intent.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutations transaction write set
     */
    @Override
    public void prewrite(final long txId, final long startTs, final List<Mutation> mutations) {
        List<Mutation> writes = normalize(mutations);
        prewriteNormalized(txId, startTs, writes);
    }

    /**
     * The new cross-shard protocol only returns an unincorporated durable prewrite certificate to the non-empty complete write set.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutations transaction write set
     * @return the operation result
     */
    public PrewriteProof prewriteWithProof(final long txId, final long startTs, final List<Mutation> mutations) {
        List<Mutation> writes = normalize(mutations);
        TransactionMetadataStore authority = metadataStore;
        if (writes.isEmpty() || !(authority instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "cross-shard prewrite proof requires nonempty writes and local-first authority");
        }
        prewriteNormalized(txId, startTs, writes);
        return new PrewriteProof(txId, startTs, shardId, writes.size(), authority);
    }

    /**
     * Cross-shard prewrite issues a certificate after landing in the same shard transaction with the real intent number.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     * @return the operation result
     */
    public PrewriteProof prewriteWithVectorProof(final long txId, final long startTs, final Mutation mutation,
                                                 final int stripe, final long startSequence) {
        List<Mutation> writes = normalize(List.of(Objects.requireNonNull(mutation, "mutation")));
        TransactionMetadataStore authority = metadataStore;
        MysqlVectorReservationStore reservations = vectorReservations;
        if (writes.size() != 1 || reservations == null
                || !(authority instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "vector prewrite requires initialized stripes and local-first authority");
        }
        try {
            long sequence = reservations.reserveFast(txId, startTs, writes.getFirst(), stripe,
                    startSequence, System.currentTimeMillis() + TERMINAL_RETENTION_MS);
            return new PrewriteProof(txId, startTs, shardId, 1, authority,
                    new VectorReservation(mutation.key(), stripe, startSequence, sequence));
        } catch (final SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 38001, 38003, 38004 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "vector prewrite has incompatible durable identity", false, txId, ex);
                case 38002, 38007 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "vector prewrite belongs to a terminal participant", false, txId, ex);
                case 38005 -> new MvccException(ErrorCode.WRITE_CONFLICT,
                        "vector prewrite found a newer version", true, txId, ex);
                case 38006 -> new MvccException(ErrorCode.LOCK_CONFLICT,
                        "vector prewrite conflicts with another intent", true, txId, ex);
                case 38008 -> new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                        "vector prewrite snapshot is older than shard safe point", false, txId, ex);
                default -> uncertain(txId, "vector prewrite outcome requires durable lookup", ex);
            };
        }
    }

    /**
     * Only prewrite of the previous JDBC SQL vectors retained against the capacity.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     * @return the operation result
     */
    PrewriteProof prewriteWithVectorProofJavaForProbe(final long txId, final long startTs, final Mutation mutation,
                                                      final int stripe, final long startSequence) {
        List<Mutation> writes = normalize(List.of(Objects.requireNonNull(mutation, "mutation")));
        TransactionMetadataStore authority = metadataStore;
        MysqlVectorReservationStore reservations = vectorReservations;
        if (writes.size() != 1 || reservations == null
                || !(authority instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "vector prewrite requires initialized stripes and local-first authority");
        }
        MysqlVectorReservationStore.Reservation reserved = transaction(txId, false, connection -> {
            long safePoint = lockWatermark(connection, false);
            ParticipantRecord record = lockParticipant(connection, txId, startTs);
            requireNonTerminal(record);
            if (record.status() != TxStatus.ACTIVE || record.startTs() != startTs) {
                throw illegal(txId, "vector prewrite requires matching ACTIVE participant");
            }
            requireFresh(txId, startTs, safePoint);
            lockKeys(connection, List.of(mutation.key()));
            checkWriteConflicts(connection, txId, startTs, writes);
            writeIntents(connection, txId, startTs, writes);
            // Keeps the serial number and real mvcc_intent to share the current JDBC transaction with the same stable key lock.
            return reservations.reserve(connection, txId, startTs, mutation, stripe, startSequence);
        });
        return new PrewriteProof(txId, startTs, shardId, 1, authority,
                new VectorReservation(mutation.key(), reserved.stripe(),
                        reserved.startSequence(), reserved.reservedSequence()));
    }

    /**
     * The service batch prewrite physical transaction at a time, returns independent real shard certificates after each successful entry.
     *
     * @param requests requests
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<PrewriteProof> prewriteVectorBatchWithProof(final List<VectorPrewriteRequest> requests) {
        MysqlVectorReservationStore reservations = vectorReservations;
        TransactionMetadataStore authority = metadataStore;
        if (reservations == null || !(authority instanceof MysqlLocalFirstMetadataView)
                || requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid vector cross batch or authority");
        }
        List<VectorPrewriteRequest> fixed = new ArrayList<>(requests.size());
        for (VectorPrewriteRequest request : requests) {
            Objects.requireNonNull(request, "request");
            Mutation mutation = normalize(List.of(request.mutation())).getFirst();
            fixed.add(new VectorPrewriteRequest(request.txId(), request.startTs(),
                    request.expireAtMillis(), mutation, request.stripe(), request.startSequence()));
        }
        if (fixed.stream().map(VectorPrewriteRequest::txId).distinct().count() != fixed.size()
                || fixed.stream().map(request -> request.mutation().key()).distinct().count() != fixed.size()) {
            throw new IllegalArgumentException("vector cross batch repeats a transaction or business key");
        }
        try {
            List<Long> sequences = reservations.reserveBatchFast(List.copyOf(fixed));
            List<PrewriteProof> proofs = new ArrayList<>(fixed.size());
            for (int index = 0; index < fixed.size(); index++) {
                VectorPrewriteRequest request = fixed.get(index);
                proofs.add(new PrewriteProof(request.txId(), request.startTs(), shardId, 1,
                        authority, new VectorReservation(request.mutation().key(),
                                request.stripe(), request.startSequence(), sequences.get(index))));
            }
            return List.copyOf(proofs);
        } catch (final SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 38001, 38003, 38004 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "vector cross batch has incompatible durable identity", false, null, ex);
                case 38002, 38007 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "vector cross batch contains a terminal participant", false, null, ex);
                case 38005 -> new MvccException(ErrorCode.WRITE_CONFLICT,
                        "vector cross batch has a newer version", true, null, ex);
                case 38006 -> new MvccException(ErrorCode.LOCK_CONFLICT,
                        "vector cross batch conflicts with another intent", true, null, ex);
                case 38008 -> new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                        "vector cross batch snapshot is older than safe point", false, null, ex);
                default -> uncertain(null, "vector cross batch outcome requires member lookup", ex);
            };
        }
    }

    /**
     * Reads the real prewrite serial number from durable shard line after responding to loss or restart.
     *
     * @param txId transaction identifier
     * @param key business key
     * @return the operation result
     */
    public Optional<VectorReservation> vectorReservation(final long txId, final String key) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "vector reservation protocol is not initialized");
        }
        try {
            return reservations.find(txId, key).map(found -> new VectorReservation(found.key(), found.stripe(),
                    found.startSequence(), found.reservedSequence()));
        } catch (final SQLException ex) {
            throw storageError(txId, "read vector reservation failed", ex);
        }
    }

    /**
     * The recovery reader reads the version map with the actual mvcc_data atomic materialization.
     *
     * @param txId transaction identifier
     * @param key business key
     * @return the operation result
     */
    public Optional<VectorReservation> committedVectorReservation(final long txId, final String key) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "vector reservation protocol is not initialized");
        }
        try {
            return reservations.findCommitted(txId, key).map(found -> new VectorReservation(found.reservation().key(), found.reservation().stripe(),
                    found.reservation().startSequence(), found.reservation().reservedSequence()));
        } catch (final SQLException ex) {
            throw storageError(txId, "read committed vector reservation failed", ex);
        }
    }

    /**
     * Reads the real participant sub-section watermark to prove that replay does not take a secondary number.
     *
     * @param stripe stripe
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public long vectorSequence(final int stripe) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector reservation protocol is not initialized", false, null);
        }
        try {
            return reservations.sequence(stripe);
        } catch (final SQLException ex) {
            throw storageError(null, "read vector stripe sequence failed", ex);
        }
    }

    /**
     * Single shard single key 1PC does not write central metadata. Only atomic saves sub-point version after the local lock key.
     *
     * @param txId transaction identifier
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     * @return the operation result
     */
    public long directCommitVectorLocal(final long txId, final Mutation mutation, final int stripe,
                                        final long startSequence) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "local vector commit requires initialized stripe layout");
        }
        List<Mutation> writes = normalize(List.of(Objects.requireNonNull(mutation, "mutation")));
        try {
            return reservations.commitLocalFast(txId, writes.getFirst(), stripe, startSequence);
        } catch (final SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 37001, 37004 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local vector commit has incompatible durable identity", false, txId, ex);
                case 37002 -> new MvccException(ErrorCode.LOCK_CONFLICT,
                        "local vector commit conflicts with an intent", true, txId, ex);
                case 37003 -> new MvccException(ErrorCode.WRITE_CONFLICT,
                        "local vector commit has a newer version", true, txId, ex);
                case 37005 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "local vector transaction has a rollback marker", false, txId, ex);
                default -> uncertain(txId, "local vector commit outcome requires durable lookup", ex);
            };
        }
    }

    /**
     * The capacity is achieved by retaining the original Java multi-speaker and using a single call from the server end at the production entrance.
     *
     * @param txId transaction identifier
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     * @return the operation result
     */
    long directCommitVectorLocalJavaForProbe(final long txId, final Mutation mutation, final int stripe,
                                             final long startSequence) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "local vector commit requires initialized stripe layout");
        }
        List<Mutation> writes = normalize(List.of(Objects.requireNonNull(mutation, "mutation")));
        return transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            lockKeys(connection, List.of(writes.getFirst().key()));
            return reservations.commitLocal(connection, txId, writes.getFirst(), stripe, startSequence);
        });
    }

    /**
     * Independent logic is written to a common InnoDB commit, where any conflict allows the whole batch of rollback s without identifying some members.
     *
     * @param requests requests
     * @return the operation result
     */
    public List<Long> directCommitVectorLocalBatch(final List<LocalVectorCommitRequest> requests) {
        MysqlVectorReservationStore reservations = vectorReservations;
        List<LocalVectorCommitRequest> fixed = validateLocalVectorBatch(requests, reservations);
        try {
            return reservations.commitLocalBatchFast(fixed);
        } catch (final SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 37001, 37004 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local vector batch has incompatible durable identity", false, null, ex);
                case 37002 -> new MvccException(ErrorCode.LOCK_CONFLICT,
                        "local vector batch conflicts with an intent", true, null, ex);
                case 37003 -> new MvccException(ErrorCode.WRITE_CONFLICT,
                        "local vector batch has a newer version", true, null, ex);
                case 37005 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "local vector batch has a rollback marker", false, null, ex);
                default -> uncertain(null, "local vector batch outcome requires member proof lookup", ex);
            };
        }
    }

    /**
     * The capacity will be achieved by retaining the old Java group commit, and whether the verify service end-end batch actually reduces fixed costs.
     *
     * @param requests requests
     * @return the operation result
     */
    List<Long> directCommitVectorLocalBatchJavaForProbe(final List<LocalVectorCommitRequest> requests) {
        MysqlVectorReservationStore reservations = vectorReservations;
        List<LocalVectorCommitRequest> fixed = validateLocalVectorBatch(requests, reservations);
        return transaction(null, false, connection -> {
            lockWatermark(connection, false);
            lockKeys(connection, fixed.stream().map(request -> request.mutation().key()).toList());
            List<Integer> ordered = IntStream.range(0, fixed.size()).boxed()
                    .sorted(java.util.Comparator.comparingInt((Integer index) -> fixed.get(index).stripe())
                            .thenComparing(index -> fixed.get(index).mutation().key(), Utf8KeyOrder::compare))
                    .toList();
            Long[] sequences = new Long[fixed.size()];
            for (int index : ordered) {
                LocalVectorCommitRequest request = fixed.get(index);
                sequences[index] = reservations.commitLocal(connection, request.txId(), request.mutation(),
                        request.stripe(), request.startSequence());
            }
            return List.of(sequences);
        });
    }

    /**
     * Sets the order of calls and refuses to repeat transaction or key before the batch, avoiding that a key is competing with two members.
     *
     * @param requests requests
     * @param reservations reservations
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private List<LocalVectorCommitRequest> validateLocalVectorBatch(
                                                                    final List<LocalVectorCommitRequest> requests, final MysqlVectorReservationStore reservations) {
        if (reservations == null || requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid local vector commit batch or stripe layout");
        }
        List<LocalVectorCommitRequest> fixed = new ArrayList<>(requests.size());
        for (LocalVectorCommitRequest request : requests) {
            Objects.requireNonNull(request, "request");
            Mutation mutation = normalize(List.of(request.mutation())).getFirst();
            fixed.add(new LocalVectorCommitRequest(request.txId(), mutation,
                    request.stripe(), request.startSequence()));
        }
        if (fixed.stream().map(LocalVectorCommitRequest::txId).distinct().count() != fixed.size()
                || fixed.stream().map(request -> request.mutation().key()).distinct().count() != fixed.size()) {
            throw new IllegalArgumentException("local vector commit batch repeats a transaction or business key");
        }
        return List.copyOf(fixed);
    }

    /**
     * COMMIT checked identity on durable single key when responding to unknown. Missing lines indicate that commit is not yet proven.
     *
     * @param request request
     * @return the operation result
     */
    public OptionalLong verifyLocalVectorCommit(final LocalVectorCommitRequest request) {
        Objects.requireNonNull(request, "request");
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(request.txId(), "local vector proof requires initialized stripe layout");
        }
        try {
            return reservations.verifyLocalCommit(request);
        } catch (final SQLException ex) {
            throw storageError(request.txId(), "verify local vector commit failed", ex);
        }
    }

    /**
     * Single-key routines and multiple-key transaction returns the caller after local commit success.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param writes writes
     */
    private void prewriteNormalized(final long txId, final long startTs, final List<Mutation> writes) {
        if (writes.size() == 1 && singleKeyPrewriteReady) {
            prewriteSingleKey(txId, startTs, writes.getFirst());
            return;
        }
        transaction(txId, false, connection -> {
            long safePoint = lockWatermark(connection, false);
            ParticipantRecord record = lockParticipant(connection, txId, startTs);
            requireNonTerminal(record);
            if (record.status() != TxStatus.ACTIVE || record.startTs() != startTs) {
                throw illegal(txId, "prewrite requires matching ACTIVE participant");
            }
            requireFresh(txId, startTs, safePoint);
            lockKeys(connection, writes.stream().map(Mutation::key).toList());
            checkWriteConflicts(connection, txId, startTs, writes);
            writeIntents(connection, txId, startTs, writes);
            return null;
        });
    }

    /**
     * Single prewrite completes the stabilization key lock with one call from the server, conflicts validate with intent.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param mutation mutation
     * @throws MvccException if the operation cannot complete
     */
    private void prewriteSingleKey(final long txId, final long startTs, final Mutation mutation) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement call = connection.prepareStatement("call mvcc_prewrite_one_v2(?, ?, ?, ?, ?, ?)")) {
            if (!connection.getAutoCommit()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "single-key prewrite requires auto-commit connection", false, txId);
            }
            call.setLong(1, txId);
            call.setLong(2, startTs);
            call.setBytes(3, encodeKey(mutation.key()));
            call.setBoolean(4, mutation.type() == MutationType.DELETE);
            bindValue(call, 5, mutation.value());
            call.setLong(6, System.currentTimeMillis() + TERMINAL_RETENTION_MS);
            call.execute();
        } catch (final SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 31001, 31003 -> illegal(txId, "single-key prewrite protocol state mismatch");
                case 31002, 31007 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "single-key prewrite rejected by terminal participant", false, txId, ex);
                case 31004 -> new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                        "single-key prewrite is older than local safe point", false, txId, ex);
                case 31005 -> new MvccException(ErrorCode.WRITE_CONFLICT,
                        "newer committed version exists", true, txId, ex);
                case 31006 -> new MvccException(ErrorCode.LOCK_CONFLICT,
                        "key is owned by another intent", true, txId, ex);
                default -> uncertain(txId, "single-key prewrite outcome is unknown", ex);
            };
        }
    }

    /**
     * Check durable intent and freeze the prewrite collection, repeat to keep idempotent.
     *
     * @param txId transaction identifier
     */
    @Override
    public void prepare(final long txId) {
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            ParticipantRecord record = requireParticipant(connection, txId);
            requireNonTerminal(record);
            List<IntentRow> rows = loadIntents(connection, txId);
            if (rows.isEmpty()) {
                throw illegal(txId, "no intent to prepare");
            }
            lockKeys(connection, rows.stream().map(IntentRow::key).toList());
            if (record.status() != TxStatus.PREPARING) {
                updateParticipant(connection, txId, TxStatus.PREPARING, null);
            }
            return null;
        });
    }

    /**
     * All commit decision materialization by centre durable, replay accepts only the same commit timestamp.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    @Override
    public void commit(final long txId, final long commitTs) {
        commitTransaction(txId, commitTs, null, true);
    }

    /**
     * Recovery materialization returned a non-falsible finalization certificate to the centre after success and review of local terminal state.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    public IntentTerminalProof commitIntentWithProof(final long txId, final long commitTs) {
        if (!(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "cross-shard terminal proof requires local-first authority");
        }
        Optional<Mutation> singleIntent = singleIntendedMutation(txId);
        if (singleIntent.isPresent()) {
            // Single-key recovery repeats the shard storage process; regular processes will again check complete durable intent with centre decision.
            commit(txId, commitTs, List.of(singleIntent.get()));
        } else {
            commit(txId, commitTs);
        }
        ParticipantRecord record = participantRecord(txId).orElseThrow(() -> illegal(txId, "committed intent has no durable participant terminal"));
        if (record.status() != TxStatus.COMMITTED || !Objects.equals(record.commitTs(), commitTs)) {
            throw illegal(txId, "committed intent terminal differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /**
     * Vector cross-shard materialization writes real version in the same transaction, retains serial number maps and participant terminal state.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param expected expected
     * @return the operation result
     */
    public IntentTerminalProof commitVectorIntentWithProof(final long txId, final long commitTs, final Mutation expected) {
        if (!(metadataStore instanceof MysqlLocalFirstMetadataView) || vectorReservations == null) {
            throw illegal(txId, "vector materialization requires local-first authority and stripe layout");
        }
        commitTransaction(txId, commitTs,
                normalize(List.of(Objects.requireNonNull(expected, "expected"))), true, null, true);
        ParticipantRecord record = participantRecord(txId).orElseThrow(() -> illegal(txId, "vector materialization has no durable participant terminal"));
        if (record.status() != TxStatus.COMMITTED || !Objects.equals(record.commitTs(), commitTs)) {
            throw illegal(txId, "vector materialization terminal differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /**
     * The recovery device is written from the durable intent or has materialization version returned single-key, then the atomic terminal state amount.
     *
     * @param proof proof
     * @return the operation result
     */
    public IntentTerminalProof commitVectorIntentWithProof(final CommittedIntentProof proof) {
        Objects.requireNonNull(proof, "proof");
        TxRecord decision = proof.record();
        if (!proof.belongsTo(metadataStore) || decision.status() != TxStatus.COMMITTED
                || decision.commitTs() == null || !decision.participants().contains(shardId)) {
            throw illegal(decision.txId(), "vector recovery has no matching center commit proof");
        }
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(decision.txId(), "vector recovery has no initialized stripe layout");
        }
        try {
            reservations.materializeFast(decision.txId(), decision.startTs(), decision.commitTs());
            return new IntentTerminalProof(shardId,
                    new ParticipantRecord(decision.txId(), decision.startTs(),
                            TxStatus.COMMITTED, decision.commitTs()),
                    metadataStore);
        } catch (final SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 39001, 39002, 39003 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "vector recovery lacks matching durable participant proof", false, decision.txId(), ex);
                case 39004 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "vector recovery encountered an aborted participant", false, decision.txId(), ex);
                default -> uncertain(decision.txId(), "vector recovery outcome requires durable lookup", ex);
            };
        }
    }

    /**
     * Multiple shard duplicates the same centre decision certificate, and single-key path saves every shard repeat query center.
     *
     * @param proof proof
     * @return the operation result
     */
    public IntentTerminalProof commitIntentWithProof(final CommittedIntentProof proof) {
        Objects.requireNonNull(proof, "proof");
        TxRecord decision = proof.record();
        if (!proof.belongsTo(metadataStore) || decision.status() != TxStatus.COMMITTED
                || decision.commitTs() == null || !decision.participants().contains(shardId)) {
            throw illegal(decision.txId(), "cross-shard committed proof has wrong authority or participant");
        }
        if (singleKeyIntentCommitReady && commitSingleIntended(decision)) {
            // Local version and participant terminal state are already atomic durable storage after the routine is completed, and can issue a certificate directly.
            return new IntentTerminalProof(shardId,
                    new ParticipantRecord(decision.txId(), decision.startTs(),
                            TxStatus.COMMITTED, decision.commitTs()),
                    metadataStore);
        }
        commit(decision.txId(), decision.commitTs());
        ParticipantRecord record = participantRecord(decision.txId()).orElseThrow(() -> illegal(decision.txId(), "committed intent has no durable participant terminal"));
        if (record.startTs() != decision.startTs() || record.status() != TxStatus.COMMITTED
                || !Objects.equals(record.commitTs(), decision.commitTs())) {
            throw illegal(decision.txId(), "committed intent terminal differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /**
     * Single-key intent read-only two-line decision, restarting recovery will also securely select the atomic materialization routine for the server.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private Optional<Mutation> singleIntendedMutation(final long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select user_key, deleted, value from mvcc_intent where tx_id = ? limit 2
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                String key = decodeKey(row.getBytes("user_key"));
                Mutation mutation = row.getBoolean("deleted")
                        ? Mutation.delete(key)
                        : Mutation.put(key, row.getBytes("value"));
                return row.next() ? Optional.empty() : Optional.of(mutation);
            }
        } catch (final SQLException ex) {
            throw storageError(txId, "read single cross-shard intent for recovery failed", ex);
        }
    }

    /**
     * Validate Full known write set corresponds to durable intent and clears intent.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     */
    @Override
    public void commit(final long txId, final long commitTs, final List<Mutation> mutations) {
        commit(txId, commitTs, mutations, true);
    }

    /**
     * Validate real intent ownership and value after materialization, allowing the retention of commit intent for background cleanup.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     * @param cleanupIntents cleanup intents
     */
    @Override
    public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
        commitTransaction(txId, commitTs, normalize(mutations), cleanupIntents);
    }

    /**
     * Only receipt generated after the successful closing of the batch book is accepted, and each shard repeat query centre decision is saved.
     *
     * @param proof proof
     * @param mutations transaction write set
     * @param cleanupIntents cleanup intents
     * @throws MvccException if the operation cannot complete
     */
    public void commitDecided(final CommittedProof proof, final List<Mutation> mutations, final boolean cleanupIntents) {
        Objects.requireNonNull(proof, "proof");
        if (!proof.belongsTo(metadataStore) || !proof.participants().contains(shardId)
                || proof.startTs() >= proof.commitTs()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "committed proof does not authorize this participant", false, proof.txId());
        }
        commitTransaction(proof.txId(), proof.commitTs(), normalize(mutations), cleanupIntents, proof);
    }

    /**
     * Validate Center decision, freeze write set and atomic commit version and terminal state in local transaction.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param expected expected
     * @param cleanup cleanup
     */
    private void commitTransaction(final long txId, final long commitTs, final List<Mutation> expected, final boolean cleanup) {
        commitTransaction(txId, commitTs, expected, cleanup, null);
    }

    /**
     * Normal recovery reads durable decision; foreground confirmed centre commit uses account book receipt.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param expected expected
     * @param cleanup cleanup
     * @param proof proof
     */
    private void commitTransaction(final long txId, final long commitTs, final List<Mutation> expected,
                                   final boolean cleanup, final CommittedProof proof) {
        commitTransaction(txId, commitTs, expected, cleanup, proof, false);
    }

    /**
     * Vector path disables the old single-key routine and requires version to be kept with the serial number durable commit.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param expected expected
     * @param cleanup cleanup
     * @param proof proof
     * @param vectorRequired vector required
     */
    private void commitTransaction(final long txId, final long commitTs, final List<Mutation> expected,
                                   final boolean cleanup, final CommittedProof proof, final boolean vectorRequired) {
        Optional<TxRecord> central = proof == null ? decision(txId) : Optional.empty();
        if (!vectorRequired && singleKeyCommitReady && expected != null && expected.size() == 1
                && (proof != null || central.isPresent() && central.get().status() == TxStatus.COMMITTED
                        && central.get().txId() == txId && central.get().participants().contains(shardId)
                        && Objects.equals(central.get().commitTs(), commitTs))) {
            long startTs = proof == null ? central.orElseThrow().startTs() : proof.startTs();
            commitSingleKey(txId, startTs, commitTs, expected.getFirst(), cleanup);
            return;
        }
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            ParticipantRecord record = requireParticipant(connection, txId);
            validateCommitTimestamp(record, commitTs);
            if (record.status() == TxStatus.COMMITTED) {
                if (vectorRequired && expected != null && expected.size() == 1
                        && MysqlVectorRetiredVersionStore.matchesExpected(connection, txId,
                                record.startTs(), commitTs, expected.getFirst())) {
                    return null;
                }
                if (expected != null) {
                    verifyCommittedWrites(connection, record, expected);
                }
                if (vectorRequired) {
                    vectorReservations.requireCommitted(connection, txId, expected.getFirst().key(), commitTs);
                }
                if (cleanup) {
                    cleanupCommittedIntents(connection, record);
                }
                return null;
            }
            requireNonTerminal(record);
            requireCommitDecision(record, commitTs, central, proof);
            List<IntentRow> rows = loadIntents(connection, txId);
            if (rows.isEmpty()) {
                throw illegal(txId, "missing durable intents for uncommitted participant");
            }
            lockKeys(connection, rows.stream().map(IntentRow::key).toList());
            verifyIntentStart(record, rows);
            if (expected != null) {
                verifyMutations(txId, rows, expected);
            }
            if (vectorRequired && (rows.size() != 1 || expected == null || expected.size() != 1)) {
                throw illegal(txId, "vector materialization requires one complete durable intent");
            }
            insertVersions(connection, txId, commitTs, rows);
            if (vectorRequired) {
                IntentRow row = rows.getFirst();
                vectorReservations.materialize(connection, txId, row.key(), row.deleted(), row.value(), commitTs);
            }
            updateParticipant(connection, txId, TxStatus.COMMITTED, commitTs);
            if (cleanup) {
                deleteIntents(connection, txId);
            }
            return null;
        });
    }

    /**
     * Once the center decision has been confirmed, local calls materialization single keys version and participant terminal state once.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param expected expected
     * @param cleanup cleanup
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void commitSingleKey(final long txId, final long startTs, final long commitTs, final Mutation expected, final boolean cleanup) {
        String routine = useSingleKeyCommitV2 ? "mvcc_commit_one_v2" : "mvcc_commit_one_v1";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement call = connection.prepareStatement(
                        "call " + routine + "(?, ?, ?, ?, ?, ?, ?)")) {
            call.setLong(1, txId);
            call.setLong(2, startTs);
            call.setLong(3, commitTs);
            call.setBytes(4, encodeKey(expected.key()));
            call.setBoolean(5, expected.type() == MutationType.DELETE);
            bindValue(call, 6, expected.value());
            call.setBoolean(7, cleanup);
            call.execute();
            if (!connection.getAutoCommit()) {
                throw new SQLException("single-key commit procedure left a local transaction open");
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 32004) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "participant is ABORTED", false, txId, ex);
            }
            if (ex.getErrorCode() >= 32001 && ex.getErrorCode() <= 32005) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "single-key materialization rejected durable participant evidence", false, txId, ex);
            }
            throw uncertain(txId, "single-key materialization outcome is unknown", ex);
        }
    }

    /**
     * A single server call from local durable single key intent to version and terminal state, multiple keys return the common path.
     *
     * @param decision decision
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private boolean commitSingleIntended(final TxRecord decision) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement call = connection.prepareStatement("call mvcc_commit_intent_one_v1(?, ?, ?)")) {
            if (!connection.getAutoCommit()) {
                throw illegal(decision.txId(), "single-key intent recovery requires auto-commit connection");
            }
            call.setLong(1, decision.txId());
            call.setLong(2, decision.startTs());
            call.setLong(3, decision.commitTs());
            call.execute();
            if (!connection.getAutoCommit()) {
                throw new SQLException("single-key intent recovery left a local transaction open");
            }
            return true;
        } catch (final SQLException ex) {
            return switch (ex.getErrorCode()) {
                case 32106 -> false;
                case 32104 -> throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "single-key intent participant is ABORTED", false, decision.txId(), ex);
                case 32105 -> throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                        "single-key intent recovery crossed shard safe point", false, decision.txId(), ex);
                case 32101, 32102, 32103 -> throw illegal(decision.txId(),
                        "single-key intent recovery lacks matching durable participant evidence");
                default -> throw uncertain(decision.txId(),
                        "single-key intent recovery outcome requires durable lookup", ex);
            };
        }
    }

    /**
     * Atomic save sheets shard version and decision; callers must first publish PREPARING candidate times and hold release barrier.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param mutations transaction write set
     */
    @Override
    public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
        List<Mutation> writes = normalize(mutations);
        Optional<TxRecord> central = decision(txId);
        transaction(txId, false, connection -> {
            long safePoint = lockWatermark(connection, false);
            ParticipantRecord record = lockParticipant(connection, txId, startTs);
            validateCommitTimestamp(record, commitTs);
            if (record.startTs() != startTs) {
                throw illegal(txId, "startTs differs from participant record");
            }
            if (record.status() == TxStatus.COMMITTED) {
                verifyCommittedWrites(connection, record, writes);
                cleanupCommittedIntents(connection, record);
                return null;
            }
            requireNonTerminal(record);
            requireFresh(txId, startTs, safePoint);
            if (metadataStore != null && (central.isEmpty()
                    || central.get().status() != TxStatus.PREPARING && central.get().status() != TxStatus.COMMITTED
                    || central.get().startTs() != startTs
                    || !Objects.equals(central.get().commitTs(), commitTs)
                    || !central.get().participants().equals(Set.of(shardId)))) {
                throw illegal(txId, "direct commit requires matching single-participant metadata publication");
            }
            List<IntentRow> ownIntents = loadIntents(connection, txId);
            if (!ownIntents.isEmpty()) {
                verifyIntentStart(record, ownIntents);
                verifyMutations(txId, ownIntents, writes);
            }
            lockKeys(connection, writes.stream().map(Mutation::key).toList());
            checkWriteConflicts(connection, txId, startTs, writes);
            List<IntentRow> rows = writes.stream().map(mutation -> new IntentRow(mutation.key(), txId, startTs,
                    mutation.type() == MutationType.DELETE, mutation.value())).toList();
            insertVersions(connection, txId, commitTs, rows);
            updateParticipant(connection, txId, TxStatus.COMMITTED, commitTs);
            deleteIntents(connection, txId);
            return null;
        });
    }

    /**
     * Local First commit Compatible Entry, ignores successful foreground certificates.
     *
     * @param fence fence
     * @param lease lease
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param mutations transaction write set
     */
    void directCommitFenced(final MysqlShardSegmentFence fence, final PublicationSegmentLease lease,
                            final long txId, final long startTs, final long commitTs, final long deadlineMillis,
                            final IsolationLevel isolationLevel, final List<Mutation> mutations) {
        directCommitFencedWithProof(fence, lease, txId, startTs, commitTs,
                deadlineMillis, isolationLevel, mutations);
    }

    /**
     * Only foreground candidates that are not externally constructed return after shard transaction is durable commit.
     *
     * @param fence fence
     * @param lease lease
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param mutations transaction write set
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    LocalCommitProof directCommitFencedWithProof(final MysqlShardSegmentFence fence, final PublicationSegmentLease lease,
                                                 final long txId, final long startTs, final long commitTs, final long deadlineMillis,
                                                 final IsolationLevel isolationLevel, final List<Mutation> mutations) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        if (!fence.usesDataSource(dataSource) || !fence.shardId().equals(shardId)) {
            throw illegal(txId, "local segment fence must share the participant database");
        }
        List<Mutation> writes = normalize(mutations);
        if (writes.isEmpty()) {
            throw new IllegalArgumentException("fenced direct commit requires a nonempty write set");
        }
        var proof = new MysqlShardSegmentFence.CandidateProof(lease.firstTs(), txId,
                startTs, commitTs, deadlineMillis, isolationLevel, lease.generation());
        if (writes.size() == 1 && fence.singleKeyProcedureEnabled()) {
            fence.commitSingleKey(lease, txId, startTs, commitTs, deadlineMillis,
                    isolationLevel, writes.getFirst());
            return new LocalCommitProof(fence, proof);
        }
        transaction(txId, false, connection -> {
            long safePoint = lockWatermark(connection, false);
            boolean active = fence.lockForCommit(connection, lease, commitTs);
            ParticipantRecord record = lockParticipant(connection, txId, startTs);
            validateCommitTimestamp(record, commitTs);
            if (record.startTs() != startTs) {
                throw illegal(txId, "fenced commit start differs from local participant");
            }
            if (record.status() == TxStatus.COMMITTED) {
                verifyCommittedWrites(connection, record, writes);
                fence.requireCandidateReplay(connection, lease, proof);
                return null;
            }
            if (!active) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "local segment owner was fenced before direct commit", false, txId);
            }
            requireNonTerminal(record);
            requireFresh(txId, startTs, safePoint);
            List<IntentRow> ownIntents = loadIntents(connection, txId);
            if (!ownIntents.isEmpty()) {
                verifyIntentStart(record, ownIntents);
                verifyMutations(txId, ownIntents, writes);
            }
            lockKeys(connection, writes.stream().map(Mutation::key).toList());
            checkWriteConflicts(connection, txId, startTs, writes);
            List<IntentRow> rows = writes.stream().map(mutation -> new IntentRow(mutation.key(), txId, startTs,
                    mutation.type() == MutationType.DELETE, mutation.value())).toList();
            insertVersions(connection, txId, commitTs, rows);
            updateParticipant(connection, txId, TxStatus.COMMITTED, commitTs);
            fence.insertCandidate(connection, lease, proof);
            if (!ownIntents.isEmpty()) {
                deleteIntents(connection, txId);
            }
            return null;
        });
        return new LocalCommitProof(fence, proof);
    }

    /**
     * Press full durable storage evidence to clean intent, without relying on the caller memory list.
     *
     * @param txId transaction identifier
     * @param mutations transaction write set
     */
    @Override
    public void cleanupIntents(final long txId, final List<Mutation> mutations) {
        cleanupIntents(txId);
    }

    /**
     * Only the local commit already has intent matching version, and it is forbidden to delete pending for writing.
     *
     * @param txId transaction identifier
     */
    @Override
    public void cleanupIntents(final long txId) {
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            Optional<ParticipantRecord> record = selectParticipant(connection, txId, true);
            if (record.isEmpty() && !hasIntents(connection, txId)) {
                return null;
            }
            if (record.isEmpty() || record.get().status() != TxStatus.COMMITTED) {
                throw illegal(txId, "intent cleanup requires local COMMITTED decision");
            }
            cleanupCommittedIntents(connection, record.get());
            return null;
        });
    }

    /**
     * Atomic rollback and quarantined late request, commit transaction has been maintained as decision and idempotent returned successfully.
     *
     * @param txId transaction identifier
     */
    @Override
    public void abort(final long txId) {
        if (metadataStore instanceof MysqlLocalFirstMetadataView localFirst) {
            // Center Expiry with decision transaction Locked Line to prevent foreground prewrite certificate of advance physical rollback.
            localFirst.assertIntentAbortAllowed(txId);
        }
        Optional<TxRecord> central = decision(txId);
        long startTs = central.map(TxRecord::startTs).orElse(txId);
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            // The txId of the new protocol can be independent of snapshot time; no intent rollback still needs to keep an accurate start-time certificate.
            ParticipantRecord record = lockParticipant(connection, txId, startTs);
            if (record.status() == TxStatus.COMMITTED) {
                return null;
            }
            if (central.isPresent() && central.get().status() == TxStatus.COMMITTED) {
                throw illegal(txId, "cannot abort a globally committed participant");
            }
            List<IntentRow> rows = loadIntents(connection, txId);
            lockKeys(connection, rows.stream().map(IntentRow::key).toList());
            writeRollbacks(connection, txId, rows);
            MysqlVectorReservationStore reservations = vectorReservations;
            if (reservations != null) {
                for (IntentRow row : rows) {
                    reservations.abort(connection, txId, row.key());
                }
            }
            if (record.status() != TxStatus.ABORTED) {
                updateParticipant(connection, txId, TxStatus.ABORTED, null);
            }
            deleteIntents(connection, txId);
            return null;
        });
    }

    /**
     * Centre Physical rollback under ABORTED decision is issued with an shard finalisation certificate upon completion.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public IntentTerminalProof abortIntentWithProof(final long txId) {
        if (!(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "cross-shard abort proof requires local-first authority");
        }
        abort(txId);
        ParticipantRecord record = participantRecord(txId).orElseThrow(() -> illegal(txId, "aborted intent has no durable participant terminal"));
        if (record.status() != TxStatus.ABORTED || record.commitTs() != null) {
            throw illegal(txId, "aborted intent participant differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /**
     * Search for residual intent and local not released decision, keep scan progress to avoid the old terminal state full of all recovery batch.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public synchronized Set<Long> findPendingTransactionIds(final int limit) {
        checkLimit(limit);
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("""
                select tx_id from (select tx_id from mvcc_intent union select tx_id from mvcc_participant_txn) pending
                where tx_id > ? order by tx_id limit ?
                """)) {
            Set<Long> result = new LinkedHashSet<>();
            statement.setLong(1, pendingScanAfter);
            statement.setInt(2, limit);
            List<Long> candidates = new ArrayList<>();
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    candidates.add(rows.getLong("tx_id"));
                }
            }
            for (long txId : candidates) {
                Optional<ParticipantRecord> local = selectParticipant(connection, txId, false);
                if (hasIntents(connection, txId) || local.isPresent() && !hasMatchingTerminalDecision(local.get())) {
                    result.add(txId);
                }
            }
            pendingScanAfter = candidates.size() < limit ? Long.MIN_VALUE : candidates.getLast();
            return result;
        } catch (final SQLException ex) {
            throw storageError(null, "scan pending participants failed", ex);
        }
    }

    /**
     * Monochrome bounded clean-up history version, tombstone chains and local terminal state with secure collection evidence.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public synchronized int gc(final long safePoint, final int limit) {
        if (vectorReservations != null || durableVectorLayoutExists()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "scalar GC cannot reclaim an initialized vector version chain", false, null);
        }
        checkLimit(limit);
        if (safePoint < 0) {
            throw new IllegalArgumentException("safePoint must be nonnegative");
        }
        List<ParticipantRecord> candidates = gcCandidates(safePoint, limit);
        boolean terminalFirst = terminalGcTurn;
        terminalGcTurn = !terminalGcTurn;
        return transaction(null, false, connection -> {
            long previous = lockWatermark(connection, true);
            Long candidateFloor = localCandidateFloor(connection);
            if (candidateFloor != null && safePoint > candidateFloor) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local segment candidate still protects a snapshot below requested safe point",
                        false, null);
            }
            long effective = Math.max(previous, safePoint);
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "update mvcc_local_safe_point set safe_point = ? where singleton_id = 1")) {
                statement.setLong(1, effective);
                statement.executeUpdate();
            }
            int reclaimed = terminalFirst ? gcTerminalRecords(connection, candidates, Math.max(1, limit / 2)) : 0;
            if (reclaimed < limit) {
                int versionBudget = terminalFirst ? limit - reclaimed : Math.max(1, limit / 2);
                reclaimed += gcVersions(connection, effective, versionBudget);
            }
            if (!terminalFirst && reclaimed < limit) {
                reclaimed += gcTerminalRecords(connection, candidates, limit - reclaimed);
            }
            return reclaimed;
        });
    }

    /**
     * Rebooting adapters must also reject the old standard GC based on shard durable layout, and cannot read only the internal markings of the process.
     *
     * @return the operation result
     */
    private boolean durableVectorLayoutExists() {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement("""
                        select 1 from information_schema.tables
                        where table_schema = database()
                          and table_name = 'mvcc_vector_layout_v1'
                          and table_type = 'BASE TABLE'
                        limit 1
                        """); ResultSet row = query.executeQuery()) {
            return row.next();
        } catch (final SQLException ex) {
            throw storageError(null, "inspect durable vector GC layout failed", ex);
        }
    }

    /**
     * Unfinished local candidate fixed the lower bounds of shard GC even if transaction lease is no longer valid.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Long localCandidateFloor(final Connection connection) throws SQLException {
        try (PreparedStatement table = connection.prepareStatement("""
                select 1 from information_schema.tables
                where table_schema = database()
                  and table_name = 'mvcc_local_segment_candidate_v1' limit 1
                """); ResultSet exists = table.executeQuery()) {
            if (!exists.next()) {
                return null;
            }
        }
        try (PreparedStatement floor = connection.prepareStatement("""
                select min(start_ts) from mvcc_local_segment_candidate_v1
                """); ResultSet row = floor.executeQuery()) {
            if (!row.next() || row.getObject(1) == null) {
                return null;
            }
            return row.getLong(1);
        }
    }

    /**
     * Visible settings and recovery connection isolation levels, commit response anomalies are always treated by unknown outcome.
     *
     * @param <T> value type
     * @param txId transaction identifier
     * @param snapshot snapshot
     * @param work work
     * @return the operation result
     */
    private <T> T transaction(final Long txId, final boolean snapshot, final SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            int originalIsolation = connection.getTransactionIsolation();
            boolean originalAutoCommit = connection.getAutoCommit();
            boolean commitStarted = false;
            boolean ended = false;
            Throwable failure = null;
            try {
                connection.setTransactionIsolation(snapshot ? Connection.TRANSACTION_REPEATABLE_READ : Connection.TRANSACTION_READ_COMMITTED);
                connection.setAutoCommit(false);
                T result = work.execute(connection);
                commitStarted = true;
                connection.commit();
                ended = true;
                return result;
            } catch (final SQLException | RuntimeException | Error ex) {
                failure = ex;
                try {
                    connection.rollback();
                    ended = true;
                } catch (final SQLException rollbackFailure) {
                    ex.addSuppressed(rollbackFailure);
                }
                // Entering COMMIT does not prove that the previous COMMIT was in force, even if subsequent ROLLBACK returns successfully.
                if (commitStarted || !ended) {
                    MvccException uncertain = uncertain(txId, "local transaction outcome is unknown", ex);
                    failure = uncertain;
                    throw uncertain;
                }
                if (ex instanceof SQLException sql) {
                    MvccException mapped = storageError(txId, "local transaction rolled back", sql);
                    failure = mapped;
                    throw mapped;
                }
                throw ex;
            } finally {
                if (ended) {
                    try {
                        connection.setAutoCommit(originalAutoCommit);
                        connection.setTransactionIsolation(originalIsolation);
                    } catch (final SQLException restoreFailure) {
                        if (failure != null) {
                            failure.addSuppressed(restoreFailure);
                        } else {
                            throw uncertain(txId, "connection restore failed after transaction", restoreFailure);
                        }
                    }
                }
            }
        } catch (final SQLException ex) {
            throw uncertain(txId, "connection acquisition or close failed", ex);
        }
    }

    /**
     * Create RR snapshot, followed by version and intent with normal reading, is protected by the same read view and InnoDB undo.
     *
     * @param connection connection
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private long readWatermark(final Connection connection) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select safe_point from mvcc_local_safe_point where singleton_id = 1");
                ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) {
                throw new IllegalStateException("local safe point has not been initialized");
            }
            return rows.getLong(1);
        }
    }

    /**
     * Write to hold shared watermark lock first, GC holds an exclusive lock to prevent the interlocking of check and cleanup.
     *
     * @param connection connection
     * @param exclusive exclusive
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private long lockWatermark(final Connection connection, final boolean exclusive) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select safe_point from mvcc_local_safe_point where singleton_id = 1 " + (exclusive ? "for update" : "for share"));
                ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) {
                throw new IllegalStateException("local safe point has not been initialized");
            }
            return rows.getLong(1);
        }
    }

    /**
     * Create or lock transaction rows, repeat requests not to modify the start time and terminal state.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private ParticipantRecord lockParticipant(final Connection connection, final long txId, final long startTs) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into mvcc_participant_txn(tx_id, start_ts, status, commit_ts, updated_at)
                values (?, ?, 'ACTIVE', null, ?) on duplicate key update tx_id = tx_id
                """)) {
            statement.setLong(1, txId);
            statement.setLong(2, startTs);
            statement.setLong(3, System.currentTimeMillis());
            statement.executeUpdate();
        }
        return requireParticipant(connection, txId);
    }

    /**
     * Lock transaction records, which must already exist. commit is not allowed to live without evidence collection transaction.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private ParticipantRecord requireParticipant(final Connection connection, final long txId) throws SQLException {
        return selectParticipant(connection, txId, true)
                .orElseThrow(() -> illegal(txId, "participant record missing or reclaimed"));
    }

    /**
     * Query participant records, select locking lines to string foreground and recovery requests.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Optional<ParticipantRecord> selectParticipant(final Connection connection, final long txId, final boolean lock) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select tx_id, start_ts, status, commit_ts from mvcc_participant_txn where tx_id = ?" + (lock ? " for update" : ""))) {
            statement.setLong(1, txId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(participant(rows)) : Optional.empty();
            }
        }
    }

    /**
     * Construct to keep an empty local record of commit timestamp.
     *
     * @param rows rows
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private ParticipantRecord participant(final ResultSet rows) throws SQLException {
        long commitTs = rows.getLong("commit_ts");
        Long timestamp = rows.wasNull() ? null : commitTs;
        return new ParticipantRecord(rows.getLong("tx_id"), rows.getLong("start_ts"),
                TxStatus.valueOf(rows.getString("status")), timestamp);
    }

    /**
     * Update participant status when transaction line lock is already in place and validate past terminal state.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param status status
     * @param commitTs durable commit timestamp
     * @throws SQLException if the operation cannot complete
     */
    private void updateParticipant(final Connection connection, final long txId, final TxStatus status, final Long commitTs) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "update mvcc_participant_txn set status = ?, commit_ts = ?, updated_at = ? where tx_id = ?")) {
            statement.setString(1, status.name());
            if (commitTs == null) {
                statement.setNull(2, Types.BIGINT);
            } else {
                statement.setLong(2, commitTs);
            }
            statement.setLong(3, System.currentTimeMillis());
            statement.setLong(4, txId);
            if (statement.executeUpdate() != 1) {
                throw illegal(txId, "participant record disappeared while locked");
            }
        }
    }

    /**
     * Press unsigned UTF-8 to get an durable -barlock, with the same protection for empty keys as those never written.
     *
     * @param connection connection
     * @param keys keys
     * @throws SQLException if the operation cannot complete
     */
    private void lockKeys(final Connection connection, final List<String> keys) throws SQLException {
        List<String> ordered = keys.stream().distinct().sorted(Utf8KeyOrder::compare).toList();
        for (int offset = 0; offset < ordered.size(); offset += BATCH_SIZE) {
            List<String> chunk = ordered.subList(offset, Math.min(offset + BATCH_SIZE, ordered.size()));
            String sql = "insert into mvcc_key_lock(user_key) values "
                    + chunk.stream().map(ignored -> "(?)").collect(Collectors.joining(", "))
                    + " on duplicate key update user_key = user_key";
            // INSERT locks the row in the binary order of VALUES, and cannot rely on an existing intent line or gap lock.
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int index = 0; index < chunk.size(); index++) {
                    statement.setBytes(index + 1, encodeKey(chunk.get(index)));
                }
                statement.executeUpdate();
            }
        }
    }

    /**
     * Checks new version, external intent and existing rollback tags within the stability key lock.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param writes writes
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void checkWriteConflicts(final Connection connection, final long txId, final long startTs, final List<Mutation> writes) throws SQLException {
        for (int offset = 0; offset < writes.size(); offset += BATCH_SIZE) {
            List<Mutation> chunk = writes.subList(offset, Math.min(offset + BATCH_SIZE, writes.size()));
            try (PreparedStatement statement = connection.prepareStatement(directCommitConflictSql(chunk.size()))) {
                bindMutationKeys(statement, chunk);
                statement.setLong(chunk.size() + 1, startTs);
                try (ResultSet rows = statement.executeQuery()) {
                    if (rows.next()) {
                        throw new MvccException(ErrorCode.WRITE_CONFLICT, "newer committed version exists", true, txId);
                    }
                }
            }
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "select tx_id, start_ts from mvcc_intent where user_key in (" + placeholders(chunk.size()) + ")")) {
                bindMutationKeys(statement, chunk);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        if (rows.getLong("tx_id") != txId) {
                            throw new MvccException(ErrorCode.LOCK_CONFLICT, "key is owned by another intent", true, txId);
                        }
                        if (rows.getLong("start_ts") != startTs) {
                            throw illegal(txId, "existing intent has a different startTs");
                        }
                    }
                }
            }
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "select 1 from mvcc_rollback where user_key in (" + placeholders(chunk.size()) + ") and tx_id = ? limit 1")) {
                bindMutationKeys(statement, chunk);
                statement.setLong(chunk.size() + 1, txId);
                try (ResultSet rows = statement.executeQuery()) {
                    if (rows.next()) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "rollback marker rejects late write", false, txId);
                    }
                }
            }
        }
    }

    /**
     * Batch writes intent for ownership already verified, and the whole write set shares one local transaction.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param writes writes
     * @throws SQLException if the operation cannot complete
     */
    private void writeIntents(final Connection connection, final long txId, final long startTs, final List<Mutation> writes) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into mvcc_intent(user_key, tx_id, start_ts, deleted, value, expire_at) values (?, ?, ?, ?, ?, ?)
                on duplicate key update deleted = values(deleted), value = values(value), expire_at = values(expire_at)
                """)) {
            int pending = 0;
            for (Mutation mutation : writes) {
                statement.setBytes(1, encodeKey(mutation.key()));
                statement.setLong(2, txId);
                statement.setLong(3, startTs);
                statement.setBoolean(4, mutation.type() == MutationType.DELETE);
                bindValue(statement, 5, mutation.value());
                statement.setLong(6, System.currentTimeMillis() + TERMINAL_RETENTION_MS);
                statement.addBatch();
                if (++pending == BATCH_SIZE) {
                    statement.executeBatch();
                    statement.clearBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                statement.executeBatch();
            }
        }
    }

    /**
     * The full intent for loading transaction actual durable storage in the order determined by the VARBINARY index.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private List<IntentRow> loadIntents(final Connection connection, final long txId) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select user_key, tx_id, start_ts, deleted, value from mvcc_intent where tx_id = ? order by user_key")) {
            statement.setLong(1, txId);
            try (ResultSet rows = statement.executeQuery()) {
                List<IntentRow> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(new IntentRow(decodeKey(rows.getBytes("user_key")), rows.getLong("tx_id"),
                            rows.getLong("start_ts"), rows.getBoolean("deleted"), rows.getBytes("value")));
                }
                return result;
            }
        }
    }

    /**
     * Verify transaction and start time does indeed have all to be materialization intent.
     *
     * @param record record
     * @param rows rows
     */
    private void verifyIntentStart(final ParticipantRecord record, final List<IntentRow> rows) {
        for (IntentRow row : rows) {
            if (row.txId() != record.txId() || row.startTs() != record.startTs()) {
                throw illegal(record.txId(), "intent ownership differs from participant record");
            }
        }
    }

    /**
     * Press the full set comparison key, remove tags and byte values to disable memory write set for missing intent.
     *
     * @param txId transaction identifier
     * @param rows rows
     * @param expected expected
     */
    private void verifyMutations(final long txId, final List<IntentRow> rows, final List<Mutation> expected) {
        if (rows.size() != expected.size()) {
            throw illegal(txId, "known write set does not match all durable intents or versions");
        }
        for (int index = 0; index < rows.size(); index++) {
            IntentRow row = rows.get(index);
            Mutation mutation = expected.get(index);
            if (!row.key().equals(mutation.key()) || row.deleted() != (mutation.type() == MutationType.DELETE)
                    || !Arrays.equals(row.value(), mutation.value())) {
                throw illegal(txId, "known mutation differs from durable value for key " + mutation.key());
            }
        }
    }

    /**
     * Only immutable version, primary key conflict allows the whole local transaction rollback and no other version is overwritten.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param rows rows
     * @throws SQLException if the operation cannot complete
     */
    private void insertVersions(final Connection connection, final long txId, final long commitTs, final List<IntentRow> rows) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value) values (?, ?, ?, ?, ?)")) {
            int pending = 0;
            for (IntentRow row : rows) {
                statement.setBytes(1, encodeKey(row.key()));
                statement.setLong(2, commitTs);
                statement.setLong(3, txId);
                statement.setBoolean(4, row.deleted());
                bindValue(statement, 5, row.value());
                statement.addBatch();
                if (++pending == BATCH_SIZE) {
                    statement.executeBatch();
                    statement.clearBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                statement.executeBatch();
            }
        }
    }

    /**
     * Check all durable version under the original transaction timestamp for the known write set commit replay.
     *
     * @param connection connection
     * @param record record
     * @param expected expected
     * @throws SQLException if the operation cannot complete
     */
    private void verifyCommittedWrites(final Connection connection, final ParticipantRecord record, final List<Mutation> expected) throws SQLException {
        lockKeys(connection, expected.stream().map(Mutation::key).toList());
        verifyMutations(record.txId(), loadCommittedRows(connection, record), expected);
    }

    /**
     * Query version of original commit timestamp, other transaction or other timestamp is not considered to be equivalent replay.
     *
     * @param connection connection
     * @param record record
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private List<IntentRow> loadCommittedRows(final Connection connection, final ParticipantRecord record) throws SQLException {
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select user_key, deleted, value from mvcc_data where tx_id = ? and commit_ts = ? order by user_key")) {
            statement.setLong(1, record.txId());
            statement.setLong(2, record.commitTs());
            try (ResultSet rows = statement.executeQuery()) {
                List<IntentRow> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(new IntentRow(decodeKey(rows.getBytes("user_key")), record.txId(), record.startTs(),
                            rows.getBoolean("deleted"), rows.getBytes("value")));
                }
                return result;
            }
        }
    }

    /**
     * Proves that all intent residues have version of the same value as transaction before cleanup, and retains intent missing materialization evidence.
     *
     * @param connection connection
     * @param record record
     * @throws SQLException if the operation cannot complete
     */
    private void cleanupCommittedIntents(final Connection connection, final ParticipantRecord record) throws SQLException {
        List<IntentRow> intents = loadIntents(connection, record.txId());
        if (intents.isEmpty()) {
            return;
        }
        lockKeys(connection, intents.stream().map(IntentRow::key).toList());
        verifyIntentStart(record, intents);
        List<Mutation> expected = intents.stream().map(row -> row.deleted() ? Mutation.delete(row.key()) : Mutation.put(row.key(), row.value())).toList();
        verifyMutations(record.txId(), loadCommittedRows(connection, record), expected);
        deleteIntents(connection, record.txId());
    }

    /**
     * Convert intent to idempotent rollback, and replay does not refresh the retention time.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param rows rows
     * @throws SQLException if the operation cannot complete
     */
    private void writeRollbacks(final Connection connection, final long txId, final List<IntentRow> rows) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into mvcc_rollback(user_key, tx_id, start_ts, created_at) values (?, ?, ?, ?)
                on duplicate key update tx_id = tx_id
                """)) {
            int pending = 0;
            for (IntentRow row : rows) {
                statement.setBytes(1, encodeKey(row.key()));
                statement.setLong(2, txId);
                statement.setLong(3, row.startTs());
                statement.setLong(4, System.currentTimeMillis());
                statement.addBatch();
                if (++pending == BATCH_SIZE) {
                    statement.executeBatch();
                    statement.clearBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                statement.executeBatch();
            }
        }
    }

    /**
     * Remove intent of this transaction when holding transaction and all key locks.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @throws SQLException if the operation cannot complete
     */
    private void deleteIntents(final Connection connection, final long txId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("delete from mvcc_intent where tx_id = ?")) {
            statement.setLong(1, txId);
            statement.executeUpdate();
        }
    }

    /**
     * Determines whether transaction is still to be processed for intent.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private boolean hasIntents(final Connection connection, final long txId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select 1 from mvcc_intent where tx_id = ? limit 1")) {
            statement.setLong(1, txId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    /**
     * Press durable cursor for bounded original key page, all delete shared budget and advance cursor without collection.
     *
     * @param connection connection
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int gcVersions(final Connection connection, final long safePoint, final int limit) throws SQLException {
        byte[] after;
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select gc_after_key from mvcc_local_safe_point where singleton_id = 1");
                ResultSet rows = statement.executeQuery()) {
            rows.next();
            after = rows.getBytes(1);
        }
        int pageSize = Math.min(limit, BATCH_SIZE);
        List<byte[]> keys = collectGcKeys(connection, after, pageSize + 1);
        int reclaimed = 0;
        int processed = 0;
        byte[] last = null;
        while (processed < Math.min(pageSize, keys.size()) && reclaimed < limit) {
            byte[] key = keys.get(processed++);
            reclaimed += gcKey(connection, key, safePoint, limit - reclaimed);
            last = key;
        }
        // Long version chain also moves after the current round has run out of budget, and the next tour continues to clean up, avoiding a heat key dying of hunger.
        byte[] next = processed == keys.size() ? null : last;
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "update mvcc_local_safe_point set gc_after_key = ? where singleton_id = 1")) {
            statement.setBytes(1, next);
            statement.executeUpdate();
        }
        return reclaimed;
    }

    /**
     * Each source read-only takes a page keyset candidate, which is not cut after aggregation on the full table version chain.
     *
     * @param connection connection
     * @param after after
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private List<byte[]> collectGcKeys(final Connection connection, final byte[] after, final int limit) throws SQLException {
        String predicate = after == null ? "" : " where user_key > ?";
        String sql = "select user_key from ((select distinct user_key from mvcc_data" + predicate
                + " order by user_key limit ?) union (select user_key from mvcc_key_lock" + predicate
                + " order by user_key limit ?)) gc_keys order by user_key limit ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (int source = 0; source < 2; source++) {
                if (after != null) {
                    statement.setBytes(parameter++, after);
                }
                statement.setInt(parameter++, limit);
            }
            statement.setInt(parameter, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<byte[]> keys = new ArrayList<>();
                while (rows.next()) {
                    keys.add(rows.getBytes(1));
                }
                return keys;
            }
        }
    }

    /**
     * Clears the version or tombstone chain of a key and maintains an intent key to avoid loss of materialization validate evidence.
     *
     * @param connection connection
     * @param key business key
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int gcKey(final Connection connection, final byte[] key, final long safePoint, final int limit) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select 1 from mvcc_intent where user_key = ?")) {
            statement.setBytes(1, key);
            try (ResultSet rows = statement.executeQuery()) {
                if (rows.next()) {
                    return 0;
                }
            }
        }
        Long latest = null;
        boolean tombstone = false;
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "select commit_ts, deleted from mvcc_data where user_key = ? order by commit_ts desc limit 1")) {
            statement.setBytes(1, key);
            try (ResultSet rows = statement.executeQuery()) {
                if (rows.next()) {
                    latest = rows.getLong("commit_ts");
                    tombstone = rows.getBoolean("deleted") && latest <= safePoint;
                }
            }
        }
        Long boundary = tombstone ? latest : null;
        if (latest != null && !tombstone) {
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "select commit_ts from mvcc_data where user_key = ? and commit_ts <= ? order by commit_ts desc limit 1")) {
                statement.setBytes(1, key);
                statement.setLong(2, safePoint);
                try (ResultSet rows = statement.executeQuery()) {
                    if (rows.next()) {
                        boundary = rows.getLong(1);
                    }
                }
            }
        }
        int reclaimed = 0;
        if (boundary != null) {
            String operator = tombstone ? " <= ?" : " < ?";
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "delete from mvcc_data where user_key = ? and commit_ts" + operator + " order by commit_ts limit ?")) {
                statement.setBytes(1, key);
                statement.setLong(2, boundary);
                statement.setInt(3, limit);
                // The ascending deletion ensures that the tombstone is last removed and that the old values during the page cleaning are not rediscovered.
                reclaimed = statement.executeUpdate();
            }
        }
        if (reclaimed < limit) {
            // The watermark exclusionary lock prevents any writer from acquiring or re-establishing this keylock, and securely collection has an empty business key steady lock line.
            try (PreparedStatement statement = connection.prepareStatement("""
                    delete from mvcc_key_lock where user_key = ?
                      and not exists (select 1 from mvcc_data where user_key = ?)
                      and not exists (select 1 from mvcc_intent where user_key = ?)
                    """)) {
                statement.setBytes(1, key);
                statement.setBytes(2, key);
                statement.setBytes(3, key);
                reclaimed += statement.executeUpdate();
            }
        }
        return reclaimed;
    }

    /**
     * Query the centre to complete the evidence before you can access the shard lock and avoid a reverse waiting for the shard lock and release of barrier.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<ParticipantRecord> gcCandidates(final long safePoint, final int limit) {
        TransactionMetadataStore authority = metadataStore;
        if (authority == null) {
            return List.of();
        }
        // The centre lower bounds contain all terminal state s that have not been Finalized, below which the deletion of local recovery evidence is permitted.
        long finalizedBoundary = authority.oldestActiveStartTs().orElse(Long.MAX_VALUE);
        long boundary = Math.min(safePoint, finalizedBoundary);
        List<ParticipantRecord> records = new ArrayList<>();
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("""
                select p.tx_id, p.start_ts, p.status, p.commit_ts from mvcc_participant_txn p
                where p.tx_id > ? and p.status in ('COMMITTED', 'ABORTED') and p.start_ts > 0 and p.start_ts < ?
                  and (p.commit_ts is null or p.commit_ts < ?) and p.updated_at < ?
                  and not exists (select 1 from mvcc_intent i where i.tx_id = p.tx_id)
                order by p.tx_id limit ?
                """)) {
            statement.setLong(1, terminalGcAfter);
            statement.setLong(2, boundary);
            statement.setLong(3, safePoint);
            statement.setLong(4, System.currentTimeMillis() - TERMINAL_RETENTION_MS);
            statement.setInt(5, limit);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    records.add(participant(rows));
                }
            }
        } catch (final SQLException ex) {
            throw storageError(null, "read terminal GC candidates failed", ex);
        }
        terminalGcAfter = records.size() < limit ? Long.MIN_VALUE : records.getLast().txId();
        // Center has deleted only those that may occur after the finished; local watermark before deleting records atomic durable storage to block old requests.
        return records.stream().filter(record -> {
            Optional<TxRecord> central = decision(record.txId());
            return central.isEmpty() || matchesTerminalDecision(record, central.get());
        }).toList();
    }

    /**
     * Recheck candidate records in exclusive watermark, clear rollback tags and local terminal state by sharing rows.
     *
     * @param connection connection
     * @param records records
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private int gcTerminalRecords(final Connection connection, final List<ParticipantRecord> records, final int limit) throws SQLException {
        int reclaimed = 0;
        for (ParticipantRecord record : records) {
            Optional<ParticipantRecord> current = selectParticipant(connection, record.txId(), true);
            if (current.isEmpty() || !current.get().equals(record) || hasIntents(connection, record.txId())) {
                continue;
            }
            if (record.status() == TxStatus.ABORTED) {
                try (PreparedStatement statement = connection.prepareStatement("""
                        delete from mvcc_rollback where tx_id = ? and start_ts = ? and created_at < ?
                        order by user_key limit ?
                        """)) {
                    statement.setLong(1, record.txId());
                    statement.setLong(2, record.startTs());
                    statement.setLong(3, System.currentTimeMillis() - TERMINAL_RETENTION_MS);
                    statement.setInt(4, limit - reclaimed);
                    reclaimed += statement.executeUpdate();
                }
            }
            if (reclaimed == limit) {
                break;
            }
            try (PreparedStatement statement = connection.prepareStatement("""
                    delete from mvcc_participant_txn where tx_id = ?
                      and not exists (select 1 from mvcc_rollback r where r.tx_id = ?)
                    """)) {
                statement.setLong(1, record.txId());
                statement.setLong(2, record.txId());
                reclaimed += statement.executeUpdate();
            }
            if (reclaimed == limit) {
                break;
            }
        }
        return reclaimed;
    }

    /**
     * The centre is required to maintain the same terminal state and start time, and local records are maintained in the absence of evidence.
     *
     * @param local local
     * @return the operation result
     */
    private boolean hasMatchingTerminalDecision(final ParticipantRecord local) {
        if (local.status() != TxStatus.COMMITTED && local.status() != TxStatus.ABORTED) {
            return false;
        }
        return decision(local.txId()).filter(record -> matchesTerminalDecision(local, record)).isPresent();
    }

    /**
     * The check centre is fully consistent with local terminal state and collection local evidence is prohibited for any mismatch.
     *
     * @param local local
     * @param central central
     * @return the operation result
     */
    private boolean matchesTerminalDecision(final ParticipantRecord local, final TxRecord central) {
        return central.startTs() == local.startTs() && central.status() == local.status()
                && Objects.equals(central.commitTs(), local.commitTs());
    }

    /**
     * Query the current binding authority, no request cache does not exist or is not terminal state decision.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private Optional<TxRecord> decision(final long txId) {
        TransactionMetadataStore store = metadataStore;
        try {
            return store == null ? Optional.empty() : store.get(txId);
        } catch (final RuntimeException ex) {
            throw uncertain(txId, "metadata decision lookup failed", ex);
        }
    }

    /**
     * Intent materialization must refer to COMMITTED decision, which cannot replace the release protocol with memory write set.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param central central
     * @param proof proof
     * @throws MvccException if the operation cannot complete
     */
    private void requireCommitDecision(final ParticipantRecord local, final long commitTs,
                                       final Optional<TxRecord> central, final CommittedProof proof) {
        boolean confirmed = proof == null ? central.isPresent()
                && central.get().txId() == local.txId()
                && central.get().status() == TxStatus.COMMITTED
                && Objects.equals(central.get().commitTs(), commitTs)
                && central.get().startTs() == local.startTs()
                && central.get().participants().contains(shardId)
                : proof.txId() == local.txId() && proof.startTs() == local.startTs()
                        && proof.commitTs() == commitTs && proof.participants().contains(shardId);
        if (!confirmed) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "materialization requires matching metadata COMMITTED decision", false, local.txId());
        }
    }

    /**
     * Check transaction terminal state irreversible, late request after rollback cannot be revived intent.
     *
     * @param record record
     * @throws MvccException if the operation cannot complete
     */
    private void requireNonTerminal(final ParticipantRecord record) {
        if (record.status() == TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "participant is ABORTED", false, record.txId());
        }
        if (record.status() == TxStatus.COMMITTED) {
            throw illegal(record.txId(), "participant is already COMMITTED");
        }
    }

    /**
     * Commit is strictly later than the start time and replay does not change original commit.
     *
     * @param record record
     * @param commitTs durable commit timestamp
     */
    private void validateCommitTimestamp(final ParticipantRecord record, final long commitTs) {
        if (commitTs <= record.startTs()
                || record.status() == TxStatus.COMMITTED && !Objects.equals(record.commitTs(), commitTs)) {
            throw illegal(record.txId(), "commitTs is invalid or differs from committed participant");
        }
    }

    /**
     * The watermark itself is a legitimate visible border for refusing requests prior to the security of watermark.
     *
     * @param txId transaction identifier
     * @param timestamp timestamp
     * @param safePoint oldest retained snapshot timestamp
     * @throws MvccException if the operation cannot complete
     */
    private void requireFresh(final long txId, final long timestamp, final long safePoint) {
        if (timestamp < safePoint) {
            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD, "timestamp precedes local safe point " + safePoint, false, txId);
        }
    }

    /**
     * Fixed write set bytes and reweighted in binary order, which is the last change.
     *
     * @param mutations transaction write set
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private List<Mutation> normalize(final List<Mutation> mutations) {
        TreeMap<String, Mutation> ordered = new TreeMap<>(Utf8KeyOrder::compare);
        for (Mutation mutation : Objects.requireNonNull(mutations, "mutations")) {
            Objects.requireNonNull(mutation, "mutation");
            encodeKey(mutation.key());
            Objects.requireNonNull(mutation.type(), "mutation type");
            byte[] value = mutation.value();
            if (mutation.type() == MutationType.DELETE && value != null) {
                throw new IllegalArgumentException("DELETE must not contain a value");
            }
            if (value != null && value.length > 65535) {
                throw new IllegalArgumentException("value exceeds MySQL BLOB capacity");
            }
            ordered.put(mutation.key(), new Mutation(mutation.key(), mutation.type(), value == null ? null : value.clone()));
        }
        return List.copyOf(ordered.values());
    }

    /**
     * Construct to compatible with the already called batch version conflict query.
     *
     * @param keyCount key count
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static String directCommitConflictSql(final int keyCount) {
        if (keyCount < 1) {
            throw new IllegalArgumentException("keyCount must be >= 1");
        }
        return "select user_key from mvcc_data where user_key in (" + placeholders(keyCount) + ") and commit_ts > ? limit 1";
    }

    /**
     * Returns the bounded conflict check number required for an existing call.
     *
     * @param keyCount key count
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static int directCommitConflictChunkCount(final int keyCount) {
        if (keyCount < 1) {
            throw new IllegalArgumentException("keyCount must be >= 1");
        }
        return 1 + (keyCount - 1) / BATCH_SIZE;
    }

    /**
     * Generates a JDBC parameter placeholder, without inserting business text.
     *
     * @param count count
     * @return the operation result
     */
    private static String placeholders(final int count) {
        return IntStream.range(0, count).mapToObj(ignored -> "?").collect(Collectors.joining(", "));
    }

    /**
     * Ties a binary key parameter to write batch.
     *
     * @param statement statement
     * @param writes writes
     * @throws SQLException if the operation cannot complete
     */
    private void bindMutationKeys(final PreparedStatement statement, final List<Mutation> writes) throws SQLException {
        for (int index = 0; index < writes.size(); index++) {
            statement.setBytes(index + 1, encodeKey(writes.get(index).key()));
        }
    }

    /**
     * A visible distinction between SQL Null and binary empty values.
     *
     * @param statement statement
     * @param parameter parameter
     * @param value value to write
     * @throws SQLException if the operation cannot complete
     */
    private void bindValue(final PreparedStatement statement, final int parameter, final byte[] value) throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.BLOB);
        } else {
            statement.setBytes(parameter, value);
        }
    }

    /**
     * A single bounded parameter for validate page breaks and batch cleanup.
     *
     * @param limit maximum number of items
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void checkLimit(final int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("limit must be in 1..4096");
        }
    }

    /**
     * Encoding database VARBINARY, which refuses to cut or cause an illegal character-induced key collision.
     *
     * @param key business key
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static byte[] encodeKey(final String key) {
        byte[] encoded = Utf8KeyOrder.encode(key);
        if (encoded.length > 256) {
            throw new IllegalArgumentException("UTF-8 key exceeds 256 bytes");
        }
        return encoded;
    }

    /**
     * Database binary key recovery is the business string.
     *
     * @param key business key
     * @return the operation result
     */
    private static String decodeKey(final byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    /**
     * Error to construct protocol status other than retry.
     *
     * @param txId transaction identifier
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final long txId, final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, txId);
    }

    /**
     * Visible tag commit or connection life cycle is unknown and it is prohibited to misperceive it as a conflict that certainly does not commit.
     *
     * @param txId transaction identifier
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private MvccException uncertain(final Long txId, final String message, final Throwable cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, shardId + ": " + message, false, txId, cause);
    }

    /**
     * Retains SQL status and bottom cause, and the caller only uses a conflict map when rollback is confirmed.
     *
     * @param txId transaction identifier
     * @param message message
     * @param ex ex
     * @return the operation result
     */
    private MvccException storageError(final Long txId, final String message, final SQLException ex) {
        return MysqlStorageExceptionMapper.toMvccException(shardId, txId, message, ex);
    }

    /**
     * The index is supplemented by a search for existing tables and allows multiple initialifiers to add the same index simultaneously.
     *
     * @param connection connection
     * @param table table
     * @param name name
     * @param columns columns
     * @throws SQLException if the operation cannot complete
     */
    private void ensureIndex(final Connection connection, final String table, final String name, final String columns) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select 1 from information_schema.statistics
                where table_schema = database() and table_name = ? and index_name = ? limit 1
                """)) {
            statement.setString(1, table);
            statement.setString(2, name);
            try (ResultSet rows = statement.executeQuery()) {
                if (rows.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement("create index " + name + " on " + table + "(" + columns + ")")) {
            statement.execute();
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1061) {
                throw ex;
            }
        }
    }

    /**
     * Fills the existing local watermark table with durable scan cursor and allows for duplicate initialization and concurrent tabulation.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureGcCursorColumn(final Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select 1 from information_schema.columns
                where table_schema = database() and table_name = 'mvcc_local_safe_point' and column_name = 'gc_after_key'
                """); ResultSet rows = statement.executeQuery()) {
            if (rows.next()) {
                return;
            }
        }
        try (
                PreparedStatement statement = connection.prepareStatement(
                        "alter table mvcc_local_safe_point add column gc_after_key varbinary(256) null")) {
            statement.execute();
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1060) {
                throw ex;
            }
        }
    }

    /**
     * The centre created only after the full intent local transaction successful commit cross-shard decision receipt.
     */
    public static final class PrewriteProof {

        private final long txId;

        private final long startTs;

        private final String shardId;

        private final int mutationCount;

        private final TransactionMetadataStore authority;

        private final VectorReservation vectorReservation;

        /**
         * Construct the entrance to be called only by the durable prewrite successful path of the same adapter.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param shardId logical shard identifier
         * @param mutationCount mutation count
         * @param authority authority
         */
        private PrewriteProof(final long txId, final long startTs, final String shardId, final int mutationCount,
                              final TransactionMetadataStore authority) {
            this(txId, startTs, shardId, mutationCount, authority, null);
        }

        /**
         * The new agreement proves that the same physical prewrite transaction retained sub-column number is additionally bound.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param shardId logical shard identifier
         * @param mutationCount mutation count
         * @param authority authority
         * @param vectorReservation vector reservation
         */
        private PrewriteProof(final long txId, final long startTs, final String shardId, final int mutationCount,
                              final TransactionMetadataStore authority, final VectorReservation vectorReservation) {
            this.txId = txId;
            this.startTs = startTs;
            this.shardId = shardId;
            this.mutationCount = mutationCount;
            this.authority = authority;
            this.vectorReservation = vectorReservation;
        }

        /**
         * Returns global transaction identifier belonging to local intent.
         *
         * @return the operation result
         */
        public long txId() {
            return txId;
        }

        /**
         * Returns shard transaction Read snapshot after validate.
         *
         * @return the operation result
         */
        public long startTs() {
            return startTs;
        }

        /**
         * Returns the only target shard route.
         *
         * @return the operation result
         */
        public String shardId() {
            return shardId;
        }

        /**
         * Returns the number of non-empty write set durable prewrite bars.
         *
         * @return the operation result
         */
        public int mutationCount() {
            return mutationCount;
        }

        /**
         * Only vector prewrite path carries serial number certificates from the local transaction atomic landing disc.
         *
         * @return the operation result
         */
        public Optional<VectorReservation> vectorReservation() {
            return Optional.ofNullable(vectorReservation);
        }

        /**
         * The prewrite result of another control book example is rejected as the basis for this time line decision.
         *
         * @param ledger ledger
         * @return the operation result
         */
        public boolean belongsTo(final MysqlLocalFirstPublicationLedger ledger) {
            return authority instanceof MysqlLocalFirstMetadataView view && view.usesLedger(ledger);
        }
    }

    /**
     * The Centre accepts only the durable subsections packaged by the Private PrewriteProof.
     *
     * @param key business key
     * @param stripe stripe
     * @param startSequence start sequence
     * @param reservedSequence reserved sequence
     */
    public record VectorReservation(String key, int stripe, long startSequence, long reservedSequence) {

        /**
         * The initial subsection snapshot with a serial number must be kept later than transaction.
         *
         * @param key business key
         * @param stripe stripe
         * @param startSequence start sequence
         * @param reservedSequence reserved sequence
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public VectorReservation {
            Objects.requireNonNull(key, "key");
            if (key.isBlank() || stripe < 0 || startSequence < 0
                    || reservedSequence <= startSequence) {
                throw new IllegalArgumentException("invalid vector reservation proof");
            }
        }
    }

    /**
     * Multiple separate single-key logic transaction can be shared once with shard Physics commit, but each keeps replay identity.
     *
     * @param txId transaction identifier
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     */
    public record LocalVectorCommitRequest(long txId, Mutation mutation,
            int stripe, long startSequence) {

        /**
         * Each transaction in the same batch must carry a negative transaction ID and a valid initial line border.
         *
         * @param txId transaction identifier
         * @param mutation mutation
         * @param stripe stripe
         * @param startSequence start sequence
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public LocalVectorCommitRequest {
            Objects.requireNonNull(mutation, "mutation");
            if (txId >= 0 || stripe < 0 || startSequence < 0) {
                throw new IllegalArgumentException("invalid local vector group commit request");
            }
        }
    }

    /**
     * Several separate cross-shard transaction participating in the same shard batch prewrite still issue centre certificates on a case by case basis.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param expireAtMillis expire at millis
     * @param mutation mutation
     * @param stripe stripe
     * @param startSequence start sequence
     */
    public record VectorPrewriteRequest(long txId, long startTs, long expireAtMillis,
            Mutation mutation, int stripe, long startSequence) {

        /**
         * Batch prewrite must have an durable identity period of validity and a non-negative subbar starting point.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param expireAtMillis expire at millis
         * @param mutation mutation
         * @param stripe stripe
         * @param startSequence start sequence
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public VectorPrewriteRequest {
            Objects.requireNonNull(mutation, "mutation");
            if (txId == 0 || startTs < 0 || expireAtMillis < 1
                    || stripe < 0 || startSequence < 0) {
                throw new IllegalArgumentException("invalid vector cross prewrite request");
            }
        }
    }

    /**
     * Only the cross-shard finalisation certificate issued after the real shard terminal state has been dropped and checked.
     */
    public static final class IntentTerminalProof {

        private final String shardId;

        private final ParticipantRecord record;

        private final TransactionMetadataStore authority;

        /**
         * The construction entrance is open only after completion of the same adaptor commit or rollback.
         *
         * @param shardId logical shard identifier
         * @param record record
         * @param authority authority
         */
        private IntentTerminalProof(final String shardId, final ParticipantRecord record,
                                    final TransactionMetadataStore authority) {
            this.shardId = shardId;
            this.record = record;
            this.authority = authority;
        }

        /**
         * Returns the physical shard route of terminal state completed by durable.
         *
         * @return the operation result
         */
        public String shardId() {
            return shardId;
        }

        /**
         * Returns verified participant transaction identifier, snapshot and terminal state.
         *
         * @return the operation result
         */
        public ParticipantRecord record() {
            return record;
        }

        /**
         * The Centre accepts only certificates issued by adapters under the same durable control book.
         *
         * @param ledger ledger
         * @return the operation result
         */
        public boolean belongsTo(final MysqlLocalFirstPublicationLedger ledger) {
            return authority instanceof MysqlLocalFirstMetadataView view && view.usesLedger(ledger);
        }
    }

    /**
     * Foreground local commit was successful before proof of binding the shard instance was produced to avoid rereading the shard when the centre is finished.
     */
    public static final class LocalCommitProof {

        private final MysqlShardSegmentFence fence;

        private final MysqlShardSegmentFence.CandidateProof candidate;

        /**
         * The private construction sure caller cannot impersonate a successful commit with a self-created candidate record.
         *
         * @param fence fence
         * @param candidate candidate
         */
        private LocalCommitProof(final MysqlShardSegmentFence fence,
                                 final MysqlShardSegmentFence.CandidateProof candidate) {
            this.fence = fence;
            this.candidate = candidate;
        }

        /**
         * The center accepts only the same commit receipt as this physical shard instance.
         *
         * @param expected expected
         * @return the operation result
         */
        public boolean belongsTo(final MysqlShardSegmentFence expected) {
            return fence == expected;
        }

        /**
         * Returns a candidate identity for the local transaction atomic saves for central checker slot and lease.
         *
         * @return the operation result
         */
        public MysqlShardSegmentFence.CandidateProof candidate() {
            return candidate;
        }
    }

    /**
     * JDBC work unit managed by local transaction packaging.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    private interface SqlWork<T> {

        /**
         * Executes in transaction connection by caller.
         *
         * @param connection connection
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        T execute(Connection connection) throws SQLException;
    }

    /**
     * Intent or version validate rows with real ownership and full byte values.
     *
     * @param key business key
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deleted deleted
     * @param value value to write
     */
    private record IntentRow(String key, long txId, long startTs, boolean deleted, byte[] value) {
    }
}

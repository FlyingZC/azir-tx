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

package com.flyingzc.azir.publication.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.control.MysqlConnectionContext;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.TransactionMetadataValidation;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.publication.PublicationEntryStatus;
import com.flyingzc.azir.publication.PublicationEntry;
import com.flyingzc.azir.publication.PublicationCommitResult;
import com.flyingzc.azir.publication.PublicationIntentCommit;
import com.flyingzc.azir.publication.PublicationLocalCommitRequest;
import com.flyingzc.azir.publication.PublicationLocalCommitProof;
import com.flyingzc.azir.publication.CoordinatorSnapshotLease;
import com.flyingzc.azir.publication.CoordinatorSnapshotView;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampAuthority;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalLong;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL durable storage commit prototype, using a stand-alone version table to avoid mixing with the current release protocol.
 */
public final class MysqlPublicationLedger {

    private final DataSource dataSource;

    private volatile boolean atomicIntentProcedureReady;

    private volatile boolean beginProcedureReady;

    private volatile boolean localReserveProcedureReady;

    private volatile boolean localCompleteProcedureReady;

    private final LongAdder batchValidationNanos = new LongAdder();

    private final LongAdder batchStateWaitNanos = new LongAdder();

    private final LongAdder batchWriteNanos = new LongAdder();

    private final LongAdder batchFinishNanos = new LongAdder();

    private final LongAdder batchCalls = new LongAdder();

    /**
     * Returns the time-consuming cumulative phase of batch control transaction.
     *
     * @return the operation result
     */
    public BatchTiming batchTiming() {
        return new BatchTiming(batchCalls.sum(), batchValidationNanos.sum(), batchStateWaitNanos.sum(),
                batchWriteNanos.sum(), batchFinishNanos.sum());
    }

    /**
     * Bind the same MySQL control database.
     *
     * @param dataSource database connection source
     */
    public MysqlPublicationLedger(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Rejects mixing of account time lines with transaction metadata in another control library.
     *
     * @param metadataStore metadata store
     * @return the operation result
     */
    public boolean sharesMetadataAuthority(final TransactionMetadataStore metadataStore) {
        return metadataStore instanceof MysqlTransactionMetadataStore mysql
                && mysql.usesDataSource(dataSource);
    }

    /**
     * The old controls that still exist during the transition period can only be called by TSO using the same MySQL data source.
     *
     * @param timestampOracle timestamp oracle
     * @return the operation result
     */
    public boolean sharesTimestampAuthority(final TimestampOracle timestampOracle) {
        return timestampOracle instanceof MysqlTimestampAuthority authority
                && authority.usesDataSource(dataSource);
    }

    /**
     * The new coordinator configuration confirms that the cross-process mode has been switched to an account book by durable.
     *
     * @throws MvccException if the operation cannot complete
     */
    public void assertActivated() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement routines = connection.prepareStatement("""
                        select routine_name from information_schema.routines
                        where routine_schema = database()
                          and routine_name in ('mvcc_ledger_begin_v2', 'mvcc_ledger_intent_commit_v1',
                                               'mvcc_ledger_local_reserve_v1', 'mvcc_ledger_local_complete_v2')
                        """)) {
            MysqlProtocolEpoch.requireLedger(connection);
            boolean beginReady = false;
            boolean commitReady = false;
            boolean reserveReady = false;
            boolean completeReady = false;
            try (ResultSet rows = routines.executeQuery()) {
                while (rows.next()) {
                    if ("mvcc_ledger_begin_v2".equalsIgnoreCase(rows.getString(1))) {
                        beginReady = true;
                    } else if ("mvcc_ledger_intent_commit_v1".equalsIgnoreCase(rows.getString(1))) {
                        commitReady = true;
                    } else if ("mvcc_ledger_local_reserve_v1".equalsIgnoreCase(rows.getString(1))) {
                        reserveReady = true;
                    } else if ("mvcc_ledger_local_complete_v2".equalsIgnoreCase(rows.getString(1))) {
                        completeReady = true;
                    }
                }
            }
            if (!beginReady || !commitReady || !reserveReady || !completeReady) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "activated publication ledger is missing required routines", false, null);
            }
            beginProcedureReady = true;
            atomicIntentProcedureReady = true;
            localReserveProcedureReady = true;
            localCompleteProcedureReady = true;
        } catch (final SQLException ex) {
            throw storageError("inspect publication protocol mode failed", null, ex);
        }
    }

    /**
     * Creates an old protocol fence in the inter-process release lock, and activates a new time line from the old TSO upper horizon.
     */
    public void initialize() {
        MysqlConnectionContext.withPublicationLock(dataSource, 10, () -> {
            initializeUnderPublicationLock();
            return null;
        });
    }

    /**
     * Push ACTIVATING, atomic commit account state and LEDGER mode after the old snapshot key fence has been installed.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void initializeUnderPublicationLock() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                Statement statement = connection.createStatement()) {
            // Late to an old process must recognize the transitioned segment mode before any DDL of install.
            try {
                MysqlProtocolEpoch.Mode installed = MysqlProtocolEpoch.current(connection);
                if (installed == MysqlProtocolEpoch.Mode.SEGMENT_PREPARE
                        || installed == MysqlProtocolEpoch.Mode.SEGMENT) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "old publication ledger cannot initialize after segmented fencing", false, null);
                }
            } catch (final SQLException ex) {
                if (ex.getErrorCode() != 1146) {
                    throw ex;
                }
            }
            MysqlProtocolEpoch.install(connection);
            MysqlProtocolEpoch.Mode currentMode = MysqlProtocolEpoch.current(connection);
            if (currentMode == MysqlProtocolEpoch.Mode.SEGMENT_PREPARE
                    || currentMode == MysqlProtocolEpoch.Mode.SEGMENT) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "old publication ledger cannot initialize after segmented fencing", false, null);
            }
            statement.execute("""
                    create table if not exists mvcc_publication_state_v1 (
                      singleton_id tinyint not null primary key,
                      next_commit_ts bigint not null,
                      published_ts bigint not null,
                      safe_point bigint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_publication_entry_v1 (
                      commit_ts bigint not null primary key,
                      tx_id bigint not null unique,
                      status varchar(16) not null,
                      expires_at bigint not null,
                      index mvcc_publication_entry_status (status, expires_at, commit_ts)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_snapshot_lease_v2 (
                      tx_id bigint not null auto_increment primary key,
                      read_ts bigint not null,
                      expires_at bigint not null,
                      index mvcc_snapshot_v2_expiry (expires_at, tx_id),
                      index mvcc_snapshot_v2_read (read_ts)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_coordinator_snapshot_lease_v1 (
                      coordinator_id bigint not null auto_increment primary key,
                      read_ts bigint not null,
                      expires_at bigint not null,
                      generation bigint not null,
                      index mvcc_coordinator_lease_expiry (expires_at, coordinator_id),
                      index mvcc_coordinator_lease_read (read_ts)
                    ) engine=InnoDB
                    """);
            ensureAtomicIntentProcedure(connection);
            atomicIntentProcedureReady = true;
            ensureLocalReserveProcedure(connection);
            localReserveProcedureReady = true;
            ensureLocalCompleteProcedure(connection);
            localCompleteProcedureReady = true;
            ensureBeginProcedure(connection);
            beginProcedureReady = true;
            try (
                    ResultSet existing = statement.executeQuery(
                            "select next_commit_ts, published_ts, safe_point from mvcc_publication_state_v1 where singleton_id = 1")) {
                if (existing.next()) {
                    long next = existing.getLong(1);
                    long published = existing.getLong(2);
                    long safe = existing.getLong(3);
                    if (safe > published || published > next) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "existing publication state violates watermark order", false, null);
                    }
                    if (MysqlProtocolEpoch.current(connection) != MysqlProtocolEpoch.Mode.LEDGER) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state exists before protocol activation completed", false, null);
                    }
                    return;
                }
            }
            try (ResultSet orphaned = statement.executeQuery("""
                    select (select count(*) from mvcc_publication_entry_v1)
                         + (select count(*) from mvcc_snapshot_lease_v2)
                         + (select count(*) from mvcc_coordinator_snapshot_lease_v1)
                    """)) {
                if (!orphaned.next() || orphaned.getLong(1) != 0) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "publication state missing while ledger rows remain", false, null);
                }
            }
            // Known old time-line errors are rejected first, avoiding unnecessary placement of the used agreements in ACTIVATING.
            long checkedUpper = legacyTimestampUpper(connection);
            requireLegacyTimestampBound(connection, checkedUpper);
            requireValidLegacyUpper(checkedUpper);
            MysqlConnectionContext.inTransaction(dataSource, activation -> {
                MysqlProtocolEpoch.Mode mode = MysqlProtocolEpoch.lock(activation);
                if (mode == MysqlProtocolEpoch.Mode.LEDGER) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "active ledger mode has no publication state", false, null);
                }
                requireLegacyDrained(activation);
                if (mode == MysqlProtocolEpoch.Mode.LEGACY) {
                    MysqlProtocolEpoch.transition(activation, mode, MysqlProtocolEpoch.Mode.ACTIVATING);
                }
                return null;
            });
            long oldUpper = legacyTimestampUpper(connection);
            requireLegacyTimestampBound(connection, oldUpper);
            requireValidLegacyUpper(oldUpper);
            statement.execute("alter table mvcc_snapshot_lease_v2 auto_increment = " + (oldUpper + 1));
            MysqlConnectionContext.inTransaction(dataSource, activation -> {
                if (MysqlProtocolEpoch.lock(activation) != MysqlProtocolEpoch.Mode.ACTIVATING) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "publication mode changed before ledger state creation", false, null);
                }
                try (PreparedStatement insert = activation.prepareStatement("""
                        insert into mvcc_publication_state_v1(singleton_id, next_commit_ts, published_ts, safe_point)
                        values (1, ?, ?, 0)
                        """)) {
                    insert.setLong(1, oldUpper);
                    insert.setLong(2, oldUpper);
                    insert.executeUpdate();
                }
                MysqlProtocolEpoch.transition(activation, MysqlProtocolEpoch.Mode.ACTIVATING,
                        MysqlProtocolEpoch.Mode.LEDGER);
                return null;
            });
        } catch (final SQLException ex) {
            throw storageError("initialize publication ledger failed", null, ex);
        }
    }

    /**
     * Verify The old TSO upper border is secure as the initial boundary of a new book and lease number.
     *
     * @param oldUpper old upper
     * @throws MvccException if the operation cannot complete
     */
    private void requireValidLegacyUpper(final long oldUpper) {
        if (oldUpper < 0 || oldUpper == Long.MAX_VALUE) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "invalid old timestamp upper bound for publication migration", false, null);
        }
    }

    /**
     * Install a single-time server to call completed version intent commit routine and maintain block control transaction rollback if it fails.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureAtomicIntentProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_ledger_intent_commit_v1' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_ledger_intent_commit_v1(in p_tx_id bigint,
                                                                in p_expires_at bigint,
                                                                in p_participants text)
                begin
                  declare v_next bigint;
                  declare v_published bigint;
                  declare v_existing bigint default null;
                  declare v_existing_status varchar(16) default null;
                  declare v_existing_deadline bigint default null;
                  declare v_start bigint default null;
                  declare v_lease_deadline bigint default null;
                  declare v_record_start bigint default null;
                  declare v_record_status varchar(32) default null;
                  declare v_record_commit bigint default null;
                  declare v_record_participants text default null;
                  declare v_commit bigint;
                  declare v_new_published bigint;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select next_commit_ts, published_ts into v_next, v_published
                    from mvcc_publication_state_v1 where singleton_id = 1 for update;
                  if v_next is null then
                    signal sqlstate '45000' set mysql_errno = 30101,
                      message_text = 'LEDGER_STATE_MISSING';
                  end if;
                  select commit_ts, status, expires_at
                    into v_existing, v_existing_status, v_existing_deadline
                    from mvcc_publication_entry_v1 where tx_id = p_tx_id for update;
                  if v_existing is not null then
                    if v_existing_deadline <> p_expires_at or v_existing_status <> 'COMMITTED' then
                      signal sqlstate '45000' set mysql_errno = 30102,
                        message_text = 'LEDGER_EXISTING_RESERVATION';
                    end if;
                    select status, commit_ts into v_record_status, v_record_commit
                      from mvcc_txn where tx_id = p_tx_id for update;
                    if v_record_status <> 'COMMITTED' or v_record_commit <> v_existing then
                      signal sqlstate '45000' set mysql_errno = 30103,
                        message_text = 'LEDGER_REPLAY_MISMATCH';
                    end if;
                    commit;
                    select v_existing as commit_ts, (v_published >= v_existing) as published_immediately;
                  else
                    select read_ts, expires_at into v_start, v_lease_deadline
                      from mvcc_snapshot_lease_v2
                      where tx_id = p_tx_id
                        and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed);
                    if v_start is null or v_lease_deadline <> p_expires_at then
                      signal sqlstate '45000' set mysql_errno = 30104,
                        message_text = 'LEDGER_LEASE_EXPIRED';
                    end if;
                    select start_ts, status, commit_ts, participants
                      into v_record_start, v_record_status, v_record_commit, v_record_participants
                      from mvcc_txn where tx_id = p_tx_id for update;
                    if v_record_status is null or v_record_start <> v_start
                        or v_record_commit is not null
                        or v_record_status not in ('ACTIVE', 'PREPARING')
                        or v_record_participants is null
                        or binary v_record_participants <> binary p_participants then
                      signal sqlstate '45000' set mysql_errno = 30105,
                        message_text = 'LEDGER_PARTICIPANTS_MISMATCH';
                    end if;
                    if v_next = 9223372036854775807 or v_next + 1 <= v_start then
                      signal sqlstate '45000' set mysql_errno = 30106,
                        message_text = 'LEDGER_TSO_EXHAUSTED';
                    end if;
                    set v_commit = v_next + 1;
                    set v_new_published = if(v_published = v_next, v_commit, v_published);
                    update mvcc_publication_state_v1
                      set next_commit_ts = v_commit, published_ts = v_new_published
                      where singleton_id = 1;
                    insert into mvcc_publication_entry_v1(commit_ts, tx_id, status, expires_at)
                      values (v_commit, p_tx_id, 'COMMITTED', p_expires_at);
                    update mvcc_txn set status = 'COMMITTED', commit_ts = v_commit
                      where tx_id = p_tx_id;
                    commit;
                    select v_commit as commit_ts, (v_new_published = v_commit) as published_immediately;
                  end if;
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
     * Install a single call to complete the version routine for local direct commit reservation and centre candidate decision.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureLocalReserveProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_ledger_local_reserve_v1' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_ledger_local_reserve_v1(in p_tx_id bigint,
                                                               in p_expires_at bigint,
                                                               in p_participant text)
                begin
                  declare v_next bigint default null;
                  declare v_existing bigint default null;
                  declare v_existing_status varchar(16) default null;
                  declare v_existing_deadline bigint default null;
                  declare v_start bigint default null;
                  declare v_lease_deadline bigint default null;
                  declare v_record_start bigint default null;
                  declare v_record_status varchar(32) default null;
                  declare v_record_commit bigint default null;
                  declare v_record_parts text default null;
                  declare v_commit bigint;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select next_commit_ts into v_next from mvcc_publication_state_v1
                    where singleton_id = 1 for update;
                  if v_next is null then
                    signal sqlstate '45000' set mysql_errno = 30401,
                      message_text = 'LEDGER_LOCAL_STATE_MISSING';
                  end if;
                  select commit_ts, status, expires_at
                    into v_existing, v_existing_status, v_existing_deadline
                    from mvcc_publication_entry_v1 where tx_id = p_tx_id for update;
                  if v_existing is not null then
                    select status, commit_ts, participants
                      into v_record_status, v_record_commit, v_record_parts
                      from mvcc_txn where tx_id = p_tx_id for update;
                    if v_existing_deadline <> p_expires_at
                        or v_record_commit is null or v_record_commit <> v_existing
                        or binary v_record_parts <> binary p_participant
                        or not (v_existing_status = 'PENDING' and v_record_status = 'PREPARING'
                                or v_existing_status = 'COMMITTED' and v_record_status = 'COMMITTED') then
                      signal sqlstate '45000' set mysql_errno = 30402,
                        message_text = 'LEDGER_LOCAL_REPLAY_MISMATCH';
                    end if;
                    commit;
                    select v_existing as commit_ts;
                  else
                    select read_ts, expires_at into v_start, v_lease_deadline
                      from mvcc_snapshot_lease_v2
                      where tx_id = p_tx_id
                        and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed);
                    if v_start is null or v_lease_deadline <> p_expires_at then
                      signal sqlstate '45000' set mysql_errno = 30403,
                        message_text = 'LEDGER_LOCAL_LEASE_EXPIRED';
                    end if;
                    select start_ts, status, commit_ts, participants
                      into v_record_start, v_record_status, v_record_commit, v_record_parts
                      from mvcc_txn where tx_id = p_tx_id for update;
                    if v_record_status <> 'ACTIVE' or v_record_commit is not null
                        or v_record_start <> v_start
                        or v_record_parts is null or binary v_record_parts <> binary p_participant then
                      signal sqlstate '45000' set mysql_errno = 30404,
                        message_text = 'LEDGER_LOCAL_PARTICIPANT_MISMATCH';
                    end if;
                    if v_next = 9223372036854775807 or v_next + 1 <= v_start then
                      signal sqlstate '45000' set mysql_errno = 30405,
                        message_text = 'LEDGER_LOCAL_CLOCK_INVALID';
                    end if;
                    set v_commit = v_next + 1;
                    update mvcc_publication_state_v1 set next_commit_ts = v_commit
                      where singleton_id = 1;
                    insert into mvcc_publication_entry_v1(commit_ts, tx_id, status, expires_at)
                      values (v_commit, p_tx_id, 'PENDING', p_expires_at);
                    update mvcc_txn set status = 'PREPARING', commit_ts = v_commit
                      where tx_id = p_tx_id and status = 'ACTIVE' and commit_ts is null;
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 30404,
                        message_text = 'LEDGER_LOCAL_CANDIDATE_LOST';
                    end if;
                    commit;
                    select v_commit as commit_ts;
                  end if;
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
     * Install local has completed the version routines issued by Centre decision and Immediate watermark.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureLocalCompleteProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_ledger_local_complete_v2' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_ledger_local_complete_v2(in p_tx_id bigint, in p_start bigint,
                                                                in p_commit bigint, in p_participant text)
                begin
                  declare v_published bigint default null;
                  declare v_before bigint default null;
                  declare v_upper bigint default null;
                  declare v_cursor bigint default null;
                  declare v_seen_status varchar(16) default null;
                  declare v_scanned int default 0;
                  declare v_entry_tx bigint default null;
                  declare v_entry_status varchar(16) default null;
                  declare v_record_start bigint default null;
                  declare v_record_status varchar(32) default null;
                  declare v_record_commit bigint default null;
                  declare v_record_parts text default null;
                  declare v_immediate boolean default false;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select published_ts, next_commit_ts into v_published, v_upper from mvcc_publication_state_v1
                    where singleton_id = 1 for update;
                  if v_published is null then
                    signal sqlstate '45000' set mysql_errno = 30501,
                      message_text = 'LEDGER_LOCAL_PUBLISH_STATE_MISSING';
                  end if;
                  set v_before = v_published;
                  select tx_id, status into v_entry_tx, v_entry_status
                    from mvcc_publication_entry_v1 where commit_ts = p_commit for update;
                  if v_entry_tx is null or v_entry_tx <> p_tx_id or v_entry_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 30502,
                      message_text = 'LEDGER_LOCAL_ENTRY_MISMATCH';
                  end if;
                  select start_ts, status, commit_ts, participants
                    into v_record_start, v_record_status, v_record_commit, v_record_parts
                    from mvcc_txn where tx_id = p_tx_id for update;
                  if v_record_start <> p_start or v_record_commit is null
                      or v_record_commit <> p_commit
                      or v_record_parts is null or binary v_record_parts <> binary p_participant then
                    signal sqlstate '45000' set mysql_errno = 30503,
                      message_text = 'LEDGER_LOCAL_RECORD_MISMATCH';
                  end if;
                  if v_entry_status = 'COMMITTED' then
                    if v_record_status <> 'COMMITTED' then
                      signal sqlstate '45000' set mysql_errno = 30503,
                        message_text = 'LEDGER_LOCAL_REPLAY_MISMATCH';
                    end if;
                  else
                    if v_entry_status <> 'PENDING' or v_record_status <> 'PREPARING'
                        or v_published >= p_commit then
                      signal sqlstate '45000' set mysql_errno = 30503,
                        message_text = 'LEDGER_LOCAL_PENDING_MISMATCH';
                    end if;
                    update mvcc_txn set status = 'COMMITTED' where tx_id = p_tx_id;
                    update mvcc_publication_entry_v1 set status = 'COMMITTED'
                      where commit_ts = p_commit and tx_id = p_tx_id and status = 'PENDING';
                    if row_count() <> 1 then
                      signal sqlstate '45000' set mysql_errno = 30502,
                        message_text = 'LEDGER_LOCAL_ENTRY_LOST';
                    end if;
                  end if;
                  -- 最后一个较早槽完成时，一次推进此前已决议的连续后缀。
                  publication_loop: while v_published < v_upper and v_scanned < 4096 do
                    set v_cursor = v_published + 1;
                    set v_seen_status = null;
                    select status into v_seen_status from mvcc_publication_entry_v1
                      where commit_ts = v_cursor;
                    if v_seen_status is null or v_seen_status = 'PENDING' then
                      leave publication_loop;
                    end if;
                    if v_seen_status not in ('COMMITTED', 'ABORTED') then
                      signal sqlstate '45000' set mysql_errno = 30504,
                        message_text = 'LEDGER_LOCAL_UNKNOWN_ENTRY_STATUS';
                    end if;
                    set v_published = v_cursor;
                    set v_scanned = v_scanned + 1;
                  end while;
                  if v_published > v_before then
                    update mvcc_publication_state_v1 set published_ts = v_published
                      where singleton_id = 1;
                  end if;
                  set v_immediate = v_published >= p_commit;
                  commit;
                  select v_immediate as published_immediately;
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
     * Installs an version begin routine for reading watermark and registering lease under a shared line lock.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureBeginProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_ledger_begin_v2' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_ledger_begin_v2(in p_isolation varchar(32), in p_timeout bigint,
                                                        in p_persist_metadata boolean)
                begin
                  declare v_mode varchar(16) default null;
                  declare v_read_ts bigint default null;
                  declare v_safe_point bigint default null;
                  declare v_deadline bigint;
                  declare v_tx_id bigint;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  select protocol_mode into v_mode from mvcc_protocol_epoch_v1
                    where singleton_id = 1 for share;
                  if v_mode is null or v_mode <> 'LEDGER' then
                    signal sqlstate '45000' set mysql_errno = 30203,
                      message_text = 'LEDGER_PROTOCOL_NOT_ACTIVE';
                  end if;
                  select published_ts, safe_point,
                         cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + p_timeout
                    into v_read_ts, v_safe_point, v_deadline
                    from mvcc_publication_state_v1 where singleton_id = 1 for share;
                  if v_read_ts is null then
                    signal sqlstate '45000' set mysql_errno = 30201,
                      message_text = 'LEDGER_BEGIN_STATE_MISSING';
                  end if;
                  if v_read_ts < v_safe_point then
                    signal sqlstate '45000' set mysql_errno = 30202,
                      message_text = 'LEDGER_BEGIN_SNAPSHOT_TOO_OLD';
                  end if;
                  insert into mvcc_snapshot_lease_v2(read_ts, expires_at)
                    values (v_read_ts, v_deadline);
                  set v_tx_id = last_insert_id();
                  if p_persist_metadata then
                    insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                      values (v_tx_id, v_read_ts, p_isolation, 'ACTIVE', null, '');
                  end if;
                  commit;
                  select v_tx_id as tx_id, v_read_ts as read_ts, v_deadline as expires_at;
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
     * The old protocol still refuses to start a new time line when lease or transaction of materialization is not finished.
     *
     * @param connection connection
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void requireLegacyDrained(final Connection connection) throws SQLException {
        try (
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("""
                        select (select count(*) from mvcc_txn t
                                  left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                                  where f.tx_id is null)
                             + (select count(*) from mvcc_snapshot_lease)
                        """)) {
            if (!result.next() || result.getLong(1) != 0) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "old publication protocol must drain before migration", false, null);
            }
        }
    }

    /**
     * Reads the assigned upper boundary from the old global TSO and avoids renumbering the old transaction ID with the new lease.
     *
     * @param connection connection
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private long legacyTimestampUpper(final Connection connection) throws SQLException {
        try (
                PreparedStatement query = connection.prepareStatement(
                        "select current_value from mvcc_tso where sequence_name = 'global'");
                ResultSet result = query.executeQuery()) {
            if (!result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "old TSO row missing", false, null);
            }
            return result.getLong(1);
        }
    }

    /**
     * Both transaction identifier and commit version of the old metadata must be located within the upper boundary of the old TSO already durable storage.
     *
     * @param connection connection
     * @param upper upper
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void requireLegacyTimestampBound(final Connection connection, final long upper) throws SQLException {
        try (
                PreparedStatement query = connection.prepareStatement("""
                        select coalesce(max(tx_id), 0), coalesce(max(commit_ts), 0) from mvcc_txn
                        """);
                ResultSet result = query.executeQuery()) {
            if (!result.next() || result.getLong(1) > upper || result.getLong(2) > upper) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "old transaction metadata exceeds durable TSO upper bound", false, null);
            }
        }
    }

    /**
     * The shared control line lock secures the released snapshot and independent transaction ID, before returning durable storage lease.
     *
     * @param isolationLevel global transaction isolation level
     * @param timeoutMillis timeout millis
     * @param persistMetadata persist metadata
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BeginReservation begin(final IsolationLevel isolationLevel, final long timeoutMillis, final boolean persistMetadata) {
        if (timeoutMillis < 1 || timeoutMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid snapshot lease timeout");
        }
        if (beginProcedureReady) {
            return callBeginProcedure(isolationLevel, timeoutMillis, persistMetadata);
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireLedger(connection);
                long readTs;
                long deadline;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                        from mvcc_publication_state_v1 where singleton_id = 1 for share
                        """)) {
                    state.setLong(1, timeoutMillis);
                    try (ResultSet result = state.executeQuery()) {
                        if (!result.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "publication state missing", false, null);
                        }
                        readTs = result.getLong("published_ts");
                        if (readTs < result.getLong("safe_point")) {
                            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                                    "published snapshot is older than safe point", false, null);
                        }
                        deadline = result.getLong("deadline");
                    }
                }
                long txId;
                try (PreparedStatement lease = connection.prepareStatement("""
                        insert into mvcc_snapshot_lease_v2(read_ts, expires_at) values (?, ?)
                        """, Statement.RETURN_GENERATED_KEYS)) {
                    lease.setLong(1, readTs);
                    lease.setLong(2, deadline);
                    lease.executeUpdate();
                    try (ResultSet keys = lease.getGeneratedKeys()) {
                        if (!keys.next()) {
                            throw new SQLException("snapshot lease did not return tx id");
                        }
                        txId = keys.getLong(1);
                    }
                }
                if (persistMetadata) {
                    try (PreparedStatement record = connection.prepareStatement("""
                            insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                            values (?, ?, ?, 'ACTIVE', null, '')
                            """)) {
                        record.setLong(1, txId);
                        record.setLong(2, readTs);
                        record.setString(3, isolationLevel.name());
                        record.executeUpdate();
                    }
                }
                return new BeginReservation(txId, readTs, deadline, persistMetadata);
            });
        } catch (final SQLException ex) {
            throw storageError("begin publication snapshot failed", null, ex);
        }
    }

    /**
     * Register more than one independent lease under shared watermark snapshot and return transaction identifier in the requested order.
     *
     * @param count count
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<BeginReservation> beginBatch(final int count, final long timeoutMillis) {
        if (count < 1 || count > 64 || timeoutMillis < 1 || timeoutMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid publication begin batch size or timeout");
        }
        if (count == 1) {
            return List.of(begin(IsolationLevel.REPEATABLE_READ, timeoutMillis, false));
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireLedger(connection);
                long readTs;
                long deadline;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                        from mvcc_publication_state_v1 where singleton_id = 1 for share
                        """)) {
                    state.setLong(1, timeoutMillis);
                    try (ResultSet row = state.executeQuery()) {
                        if (!row.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "publication state missing for batch begin", false, null);
                        }
                        readTs = row.getLong("published_ts");
                        if (readTs < row.getLong("safe_point")) {
                            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                                    "batch begin snapshot is older than safe point", false, null);
                        }
                        deadline = row.getLong("deadline");
                    }
                }
                String values = String.join(",", Collections.nCopies(count, "(?, ?)"));
                String insertSql = "insert into mvcc_snapshot_lease_v2(read_ts, expires_at) values " + values;
                try (PreparedStatement insert = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                    for (int index = 0; index < count; index++) {
                        insert.setLong(index * 2 + 1, readTs);
                        insert.setLong(index * 2 + 2, deadline);
                    }
                    if (insert.executeUpdate() != count) {
                        throw new SQLException("batch snapshot lease insert returned an incomplete row count");
                    }
                    List<BeginReservation> reservations = new ArrayList<>(count);
                    long previousTxId = 0;
                    try (ResultSet keys = insert.getGeneratedKeys()) {
                        while (keys.next()) {
                            long txId = keys.getLong(1);
                            if (txId <= previousTxId || reservations.size() == count) {
                                throw new SQLException("batch snapshot lease returned invalid generated transaction ids");
                            }
                            reservations.add(new BeginReservation(txId, readTs, deadline, false));
                            previousTxId = txId;
                        }
                    }
                    if (reservations.size() != count) {
                        throw new SQLException("batch snapshot lease did not return every transaction id");
                    }
                    return List.copyOf(reservations);
                }
            });
        } catch (final SQLException ex) {
            throw storageError("begin publication snapshot batch failed", null, ex);
        }
    }

    /**
     * Retrieving shared fence snapshot and independent transaction identifier with one call from a server.
     *
     * @param isolationLevel global transaction isolation level
     * @param timeoutMillis timeout millis
     * @param persistMetadata persist metadata
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private BeginReservation callBeginProcedure(final IsolationLevel isolationLevel, final long timeoutMillis,
                                                final boolean persistMetadata) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement call = connection.prepareStatement("call mvcc_ledger_begin_v2(?, ?, ?)")) {
            call.setString(1, isolationLevel.name());
            call.setLong(2, timeoutMillis);
            call.setBoolean(3, persistMetadata);
            boolean hasResult = call.execute();
            while (!hasResult && call.getUpdateCount() != -1) {
                hasResult = call.getMoreResults();
            }
            if (!hasResult) {
                throw new SQLException("ledger begin procedure returned no snapshot");
            }
            try (ResultSet result = call.getResultSet()) {
                if (!result.next()) {
                    throw new SQLException("ledger begin procedure returned no transaction");
                }
                return new BeginReservation(result.getLong("tx_id"), result.getLong("read_ts"),
                        result.getLong("expires_at"), persistMetadata);
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 30201) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication state missing", false, null, ex);
            }
            if (ex.getErrorCode() == 30202) {
                throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                        "published snapshot older than safe point", false, null, ex);
            }
            if (ex.getErrorCode() == 30203) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication ledger protocol is not active", false, null, ex);
            }
            throw storageError("ledger begin procedure failed", null, ex);
        }
    }

    /**
     * In short transaction, atomic sets the commit time in line PENDING, repeats a request to return the original reservation.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @return the operation result
     */
    public long reserveCommit(final long txId, final long expiresAtMillis) {
        return reserveCommit(txId, expiresAtMillis, null);
    }

    /**
     * Single shard local directly before commit, atomic sets the time slot and center PREPARING candidate decision.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participant participant
     * @return the operation result
     */
    public long reserveLocalCommit(final long txId, final long expiresAtMillis, final String participant) {
        TransactionMetadataValidation.validateParticipant(txId, participant);
        if (localReserveProcedureReady) {
            return callLocalReserveProcedure(txId, expiresAtMillis, participant);
        }
        return reserveCommit(txId, expiresAtMillis, participant);
    }

    /**
     * Multiple local direct commit shares an durable timeline preset, and the whole batch does not create an timestamp gap when it fails.
     *
     * @param requests requests
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> reserveLocalCommitBatch(final List<PublicationLocalCommitRequest> requests) {
        if (requests.isEmpty()) {
            return List.of();
        }
        if (requests.size() == 1) {
            PublicationLocalCommitRequest request = requests.getFirst();
            return List.of(reserveLocalCommit(request.txId(), request.expiresAtMillis(), request.participant()));
        }
        if (requests.size() > 64) {
            throw new IllegalArgumentException("local reservation batch exceeds 64 transactions");
        }
        List<IndexedLocalCommit> sorted = new ArrayList<>(requests.size());
        for (int index = 0; index < requests.size(); index++) {
            sorted.add(new IndexedLocalCommit(index, Objects.requireNonNull(requests.get(index), "request")));
        }
        sorted.sort(Comparator.comparingLong(item -> item.request().txId()));
        for (int index = 1; index < sorted.size(); index++) {
            if (sorted.get(index - 1).request().txId() == sorted.get(index).request().txId()) {
                throw new IllegalArgumentException("local reservation batch contains duplicate transaction id");
            }
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                String placeholders = String.join(",", Collections.nCopies(sorted.size(), "?"));
                String leaseSql = """
                        select tx_id, read_ts, expires_at from mvcc_snapshot_lease_v2
                        where tx_id in (%s)
                          and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """.formatted(placeholders);
                Map<Long, SnapshotLease> leases = new HashMap<>(sorted.size());
                try (PreparedStatement query = connection.prepareStatement(leaseSql)) {
                    for (int index = 0; index < sorted.size(); index++) {
                        query.setLong(index + 1, sorted.get(index).request().txId());
                    }
                    try (ResultSet rows = query.executeQuery()) {
                        while (rows.next()) {
                            leases.put(rows.getLong("tx_id"),
                                    new SnapshotLease(rows.getLong("read_ts"), rows.getLong("expires_at")));
                        }
                    }
                }
                long newestStart = Long.MIN_VALUE;
                long earliestDeadline = Long.MAX_VALUE;
                try (PreparedStatement metadata = connection.prepareStatement("""
                        select start_ts, status, commit_ts, participants from mvcc_txn
                        where tx_id = ? for update
                        """)) {
                    // Precise main key line lock to txId in order to avoid the IN range lock blocking other transaction registrations.
                    for (IndexedLocalCommit item : sorted) {
                        PublicationLocalCommitRequest request = item.request();
                        SnapshotLease lease = leases.get(request.txId());
                        if (lease == null || lease.expiresAtMillis() != request.expiresAtMillis()) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "local batch has no active matching snapshot lease", false, request.txId());
                        }
                        metadata.setLong(1, request.txId());
                        try (ResultSet row = metadata.executeQuery()) {
                            if (!row.next() || row.getLong("start_ts") != lease.readTs()
                                    || !"ACTIVE".equals(row.getString("status"))
                                    || row.getObject("commit_ts") != null
                                    || !request.participant().equals(row.getString("participants"))) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "local batch differs from durable participant metadata", false, request.txId());
                            }
                        }
                        newestStart = Math.max(newestStart, lease.readTs());
                        earliestDeadline = Math.min(earliestDeadline, request.expiresAtMillis());
                    }
                }
                long nextTs;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select next_commit_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing for local reservation batch", false, null);
                    }
                    nextTs = row.getLong(1);
                }
                try (
                        PreparedStatement clock = connection.prepareStatement("""
                                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                                """);
                        ResultSet now = clock.executeQuery()) {
                    if (!now.next() || earliestDeadline <= now.getLong(1)) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                "local batch snapshot lease expired before reservation", false, null);
                    }
                }
                if (nextTs > Long.MAX_VALUE - sorted.size() || nextTs + 1 <= newestStart) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local batch commit timestamp cannot pass snapshot", false, null);
                }
                long upper = nextTs + sorted.size();
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                update mvcc_publication_state_v1 set next_commit_ts = ? where singleton_id = 1
                                """);
                        PreparedStatement entry = connection.prepareStatement("""
                                insert into mvcc_publication_entry_v1(commit_ts, tx_id, status, expires_at)
                                values (?, ?, 'PENDING', ?)
                                """);
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'PREPARING', commit_ts = ?
                                where tx_id = ? and status = 'ACTIVE' and commit_ts is null
                                  and binary participants = binary ?
                                """)) {
                    state.setLong(1, upper);
                    if (state.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local batch lost publication state", false, null);
                    }
                    Long[] results = new Long[requests.size()];
                    for (int index = 0; index < sorted.size(); index++) {
                        IndexedLocalCommit item = sorted.get(index);
                        long commitTs = nextTs + index + 1;
                        entry.setLong(1, commitTs);
                        entry.setLong(2, item.request().txId());
                        entry.setLong(3, item.request().expiresAtMillis());
                        entry.addBatch();
                        metadata.setLong(1, commitTs);
                        metadata.setLong(2, item.request().txId());
                        metadata.setString(3, item.request().participant());
                        metadata.addBatch();
                        results[item.index()] = commitTs;
                    }
                    entry.executeBatch();
                    for (int updated : metadata.executeBatch()) {
                        if (updated == 0) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local batch lost a candidate metadata row", false, null);
                        }
                    }
                    return List.copyOf(Arrays.asList(results));
                }
            });
        } catch (final SQLException ex) {
            throw storageError("batch local commit reservation failed", null, ex);
        }
    }

    /**
     * Call a single service-end to complete the single shard slot and decision candidate atomic reservation.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participant participant
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private long callLocalReserveProcedure(final long txId, final long expiresAtMillis, final String participant) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement call = connection.prepareStatement("call mvcc_ledger_local_reserve_v1(?, ?, ?)")) {
            call.setLong(1, txId);
            call.setLong(2, expiresAtMillis);
            call.setString(3, participant);
            boolean hasResult = call.execute();
            while (!hasResult && call.getUpdateCount() != -1) {
                hasResult = call.getMoreResults();
            }
            if (!hasResult) {
                throw new SQLException("local reservation procedure returned no timestamp");
            }
            try (ResultSet row = call.getResultSet()) {
                if (!row.next()) {
                    throw new SQLException("local reservation procedure returned no transaction");
                }
                return row.getLong("commit_ts");
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 30402) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local reservation replay differs from durable candidate", false, txId, ex);
            }
            if (ex.getErrorCode() == 30403) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "local reservation snapshot lease expired", false, txId, ex);
            }
            if (ex.getErrorCode() >= 30401 && ex.getErrorCode() <= 30405) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local reservation rejected durable metadata", false, txId, ex);
            }
            throw storageError("server-side local commit reservation failed", txId, ex);
        }
    }

    /**
     * Optionally place single shard candidate time in the same control transaction as PENDING row to avoid an empty window in recovery.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param localParticipant local participant
     * @return the operation result
     */
    private long reserveCommit(final long txId, final long expiresAtMillis, final String localParticipant) {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long nextTs;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select next_commit_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet result = state.executeQuery()) {
                    if (!result.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing", false, txId);
                    }
                    nextTs = result.getLong(1);
                }
                try (
                        PreparedStatement existing = connection.prepareStatement(
                                "select commit_ts, status, expires_at from mvcc_publication_entry_v1 where tx_id = ?")) {
                    existing.setLong(1, txId);
                    try (ResultSet result = existing.executeQuery()) {
                        if (result.next()) {
                            long reserved = result.getLong("commit_ts");
                            if (result.getLong("expires_at") != expiresAtMillis) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "commit reservation deadline mismatch", false, txId);
                            }
                            if (localParticipant != null) {
                                String entryStatus = result.getString("status");
                                try (PreparedStatement metadata = connection.prepareStatement("""
                                        select status, commit_ts, participants from mvcc_txn
                                        where tx_id = ? for update
                                        """)) {
                                    metadata.setLong(1, txId);
                                    try (ResultSet record = metadata.executeQuery()) {
                                        if (!record.next() || record.getObject("commit_ts") == null
                                                || record.getLong("commit_ts") != reserved
                                                || !localParticipant.equals(record.getString("participants"))
                                                || !("PENDING".equals(entryStatus)
                                                        && "PREPARING".equals(record.getString("status"))
                                                        || "COMMITTED".equals(entryStatus)
                                                                && "COMMITTED".equals(record.getString("status")))) {
                                            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                                    "local reservation replay has different durable decision", false, txId);
                                        }
                                    }
                                }
                            }
                            return reserved;
                        }
                    }
                }
                long startTs;
                try (
                        PreparedStatement lease = connection.prepareStatement(
                                """
                                        select read_ts, expires_at from mvcc_snapshot_lease_v2
                                        where tx_id = ? and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                                        """)) {
                    lease.setLong(1, txId);
                    try (ResultSet result = lease.executeQuery()) {
                        if (!result.next()) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "snapshot lease missing for commit reservation", false, txId);
                        }
                        startTs = result.getLong(1);
                        if (result.getLong(2) != expiresAtMillis) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "commit reservation cannot change snapshot deadline", false, txId);
                        }
                    }
                }
                try (PreparedStatement metadata = connection.prepareStatement("""
                        select start_ts, status, commit_ts, participants from mvcc_txn
                        where tx_id = ? for update
                        """)) {
                    metadata.setLong(1, txId);
                    try (ResultSet record = metadata.executeQuery()) {
                        if (!record.next() || record.getLong("start_ts") != startTs
                                || record.getObject("commit_ts") != null
                                || !Set.of("ACTIVE", "PREPARING").contains(record.getString("status"))
                                || record.getString("participants") == null
                                || record.getString("participants").isBlank()
                                || localParticipant != null && (!"ACTIVE".equals(record.getString("status"))
                                        || !localParticipant.equals(record.getString("participants")))) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "commit reservation requires durable undecided participants", false, txId);
                        }
                    }
                }
                if (nextTs == Long.MAX_VALUE || nextTs + 1 <= startTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "commit timestamp cannot advance past snapshot", false, txId);
                }
                long commitTs = nextTs + 1;
                try (
                        PreparedStatement update = connection.prepareStatement("""
                                update mvcc_publication_state_v1 set next_commit_ts = ? where singleton_id = 1
                                """);
                        PreparedStatement insert = connection.prepareStatement("""
                                insert into mvcc_publication_entry_v1(commit_ts, tx_id, status, expires_at)
                                values (?, ?, 'PENDING', ?)
                                """);
                        PreparedStatement candidate = localParticipant == null ? null : connection.prepareStatement("""
                                update mvcc_txn set status = 'PREPARING', commit_ts = ?
                                where tx_id = ? and status = 'ACTIVE' and commit_ts is null
                                  and binary participants = binary ?
                                """)) {
                    update.setLong(1, commitTs);
                    if (update.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local reservation lost publication state", false, txId);
                    }
                    insert.setLong(1, commitTs);
                    insert.setLong(2, txId);
                    insert.setLong(3, expiresAtMillis);
                    insert.executeUpdate();
                    if (candidate != null) {
                        candidate.setLong(1, commitTs);
                        candidate.setLong(2, txId);
                        candidate.setString(3, localParticipant);
                        if (candidate.executeUpdate() != 1) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local reservation lost matching participant metadata", false, txId);
                        }
                    }
                }
                return commitTs;
            });
        } catch (final SQLException ex) {
            throw storageError("reserve commit timestamp failed", txId, ex);
        }
    }

    /**
     * Transaction completed timestamp distribution at one time, centre decision and terminal state ledgers.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public PublicationCommitResult commitPreparedIntents(final long txId, final long expiresAtMillis, final Set<String> participants) {
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (participants.isEmpty()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "intent commit requires durable participants", false, txId);
        }
        String encoded = participants.stream().sorted().collect(Collectors.joining(","));
        if (atomicIntentProcedureReady) {
            return callAtomicIntentProcedure(txId, expiresAtMillis, encoded);
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long nextTs;
                long publishedTs;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select next_commit_ts, published_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet result = state.executeQuery()) {
                    if (!result.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing", false, txId);
                    }
                    nextTs = result.getLong(1);
                    publishedTs = result.getLong(2);
                }
                try (PreparedStatement existing = connection.prepareStatement("""
                        select commit_ts, status, expires_at from mvcc_publication_entry_v1
                        where tx_id = ? for update
                        """)) {
                    existing.setLong(1, txId);
                    try (ResultSet result = existing.executeQuery()) {
                        if (result.next()) {
                            if (result.getLong("expires_at") != expiresAtMillis
                                    || !"COMMITTED".equals(result.getString("status"))) {
                                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                        "existing publication reservation needs recovery", false, txId);
                            }
                            long commitTs = result.getLong("commit_ts");
                            verifyCommittedRecord(connection, txId, commitTs);
                            return new PublicationCommitResult(commitTs, publishedTs >= commitTs);
                        }
                    }
                }
                long startTs;
                try (PreparedStatement lease = connection.prepareStatement("""
                        select read_ts, expires_at from mvcc_snapshot_lease_v2
                        where tx_id = ? and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """)) {
                    lease.setLong(1, txId);
                    try (ResultSet result = lease.executeQuery()) {
                        if (!result.next() || result.getLong("expires_at") != expiresAtMillis) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "intent commit has no matching active snapshot lease", false, txId);
                        }
                        startTs = result.getLong("read_ts");
                    }
                }
                if (nextTs == Long.MAX_VALUE || nextTs + 1 <= startTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "intent commit timestamp cannot pass snapshot", false, txId);
                }
                try (PreparedStatement metadata = connection.prepareStatement("""
                        select start_ts, status, commit_ts, participants from mvcc_txn
                        where tx_id = ? for update
                        """)) {
                    metadata.setLong(1, txId);
                    try (ResultSet record = metadata.executeQuery()) {
                        if (!record.next() || record.getLong("start_ts") != startTs
                                || record.getObject("commit_ts") != null
                                || !Set.of("ACTIVE", "PREPARING").contains(record.getString("status"))
                                || !encoded.equals(record.getString("participants"))) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "intent commit differs from durable participant metadata", false, txId);
                        }
                    }
                }
                long commitTs = nextTs + 1;
                // When no early pending slot is available, decision and watermark are published in the same MySQL transaction commit.
                long nextPublished = publishedTs == nextTs ? commitTs : publishedTs;
                try (
                        PreparedStatement clock = connection.prepareStatement("""
                                update mvcc_publication_state_v1
                                set next_commit_ts = ?, published_ts = ? where singleton_id = 1
                                """);
                        PreparedStatement entry = connection.prepareStatement("""
                                insert into mvcc_publication_entry_v1(commit_ts, tx_id, status, expires_at)
                                values (?, ?, 'COMMITTED', ?)
                                """);
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'COMMITTED', commit_ts = ? where tx_id = ?
                                """)) {
                    clock.setLong(1, commitTs);
                    clock.setLong(2, nextPublished);
                    clock.executeUpdate();
                    entry.setLong(1, commitTs);
                    entry.setLong(2, txId);
                    entry.setLong(3, expiresAtMillis);
                    entry.executeUpdate();
                    metadata.setLong(1, commitTs);
                    metadata.setLong(2, txId);
                    metadata.executeUpdate();
                }
                return new PublicationCommitResult(commitTs, nextPublished == commitTs);
            });
        } catch (final SQLException ex) {
            throw storageError("atomic intent commit failed", txId, ex);
        }
    }

    /**
     * Commit bounded batch in a control database transaction and returns each commit time in the call order.
     *
     * @param requests requests
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<PublicationCommitResult> commitPreparedIntentBatch(final List<PublicationIntentCommit> requests) {
        if (requests.isEmpty()) {
            return List.of();
        }
        if (requests.size() == 1) {
            PublicationIntentCommit request = requests.getFirst();
            return List.of(commitPreparedIntents(request.txId(), request.expiresAtMillis(), request.participants()));
        }
        if (requests.size() > 64) {
            throw new IllegalArgumentException("publication commit batch exceeds 64 transactions");
        }
        List<IndexedIntentCommit> sorted = new ArrayList<>(requests.size());
        for (int index = 0; index < requests.size(); index++) {
            PublicationIntentCommit request = Objects.requireNonNull(requests.get(index), "request");
            String participants = request.participants().stream().sorted().collect(Collectors.joining(","));
            sorted.add(new IndexedIntentCommit(index, request, participants));
        }
        sorted.sort(Comparator.comparingLong(value -> value.request().txId()));
        for (int index = 1; index < sorted.size(); index++) {
            if (sorted.get(index - 1).request().txId() == sorted.get(index).request().txId()) {
                throw new IllegalArgumentException("publication commit batch contains duplicate transaction id");
            }
        }
        String placeholders = String.join(",", Collections.nCopies(sorted.size(), "?"));
        String metadataUpdateSql = "update mvcc_txn set status = 'COMMITTED', commit_ts = case tx_id "
                + "when ? then ? ".repeat(sorted.size())
                + "end where tx_id in (" + placeholders + ")"
                + " and status in ('ACTIVE', 'PREPARING') and commit_ts is null";
        try {
            long[] timings = new long[3];
            long entered = System.nanoTime();
            List<PublicationCommitResult> committed = MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long newestStart = Long.MIN_VALUE;
                long earliestDeadline = Long.MAX_VALUE;
                String leasesSql = """
                        select tx_id, read_ts, expires_at from mvcc_snapshot_lease_v2
                        where tx_id in (%s)
                          and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """.formatted(placeholders);
                Map<Long, SnapshotLease> leases = new HashMap<>(sorted.size());
                try (PreparedStatement lease = connection.prepareStatement(leasesSql)) {
                    for (int index = 0; index < sorted.size(); index++) {
                        lease.setLong(index + 1, sorted.get(index).request().txId());
                    }
                    try (ResultSet rows = lease.executeQuery()) {
                        while (rows.next()) {
                            leases.put(rows.getLong("tx_id"),
                                    new SnapshotLease(rows.getLong("read_ts"), rows.getLong("expires_at")));
                        }
                    }
                }
                String metadataSql = """
                        select tx_id, start_ts, status, commit_ts, participants from mvcc_txn force index(primary)
                        where tx_id in (%s) order by tx_id for update
                        """.formatted(placeholders);
                Map<Long, BatchIntentMetadata> lockedMetadata = new HashMap<>(sorted.size());
                try (PreparedStatement metadata = connection.prepareStatement(metadataSql)) {
                    for (int index = 0; index < sorted.size(); index++) {
                        metadata.setLong(index + 1, sorted.get(index).request().txId());
                    }
                    // The primary key sequence is locked and then the verify is written to avoid a batch of commit s producing multiple control library round-trip.
                    try (ResultSet rows = metadata.executeQuery()) {
                        while (rows.next()) {
                            long txId = rows.getLong("tx_id");
                            Object commit = rows.getObject("commit_ts");
                            lockedMetadata.put(txId, new BatchIntentMetadata(rows.getLong("start_ts"),
                                    rows.getString("status"), commit == null ? null : rows.getLong("commit_ts"),
                                    rows.getString("participants")));
                        }
                    }
                }
                for (IndexedIntentCommit item : sorted) {
                    PublicationIntentCommit request = item.request();
                    SnapshotLease current = leases.get(request.txId());
                    if (current == null || current.expiresAtMillis() != request.expiresAtMillis()) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                "batch intent commit has no active snapshot lease", false, request.txId());
                    }
                    long startTs = current.readTs();
                    BatchIntentMetadata durable = lockedMetadata.get(request.txId());
                    if (durable == null || durable.startTs() != startTs || durable.commitTs() != null
                            || !Set.of("ACTIVE", "PREPARING").contains(durable.status())
                            || !item.participants().equals(durable.participants())) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "batch intent commit differs from durable metadata", false, request.txId());
                    }
                    newestStart = Math.max(newestStart, startTs);
                    earliestDeadline = Math.min(earliestDeadline, request.expiresAtMillis());
                }
                timings[0] = System.nanoTime() - entered;
                long nextTs;
                long publishedTs;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select next_commit_ts, published_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing for batch commit", false, null);
                    }
                    nextTs = row.getLong(1);
                    publishedTs = row.getLong(2);
                }
                // Reread database clocks after obtaining state line lock. lease commit is not allowed while waiting for timeout.
                try (
                        PreparedStatement clock = connection.prepareStatement("""
                                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                                """);
                        ResultSet now = clock.executeQuery()) {
                    if (!now.next() || earliestDeadline <= now.getLong(1)) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                "batch intent snapshot lease expired before decision", false, null);
                    }
                }
                timings[1] = System.nanoTime() - entered;
                if (nextTs > Long.MAX_VALUE - sorted.size() || nextTs + 1 <= newestStart) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "batch commit timestamp cannot advance past snapshot", false, null);
                }
                long upper = nextTs + sorted.size();
                long nextPublished = publishedTs == nextTs ? upper : publishedTs;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                update mvcc_publication_state_v1
                                set next_commit_ts = ?, published_ts = ? where singleton_id = 1
                                """);
                        PreparedStatement entry = connection.prepareStatement("""
                                insert into mvcc_publication_entry_v1(commit_ts, tx_id, status, expires_at)
                                values (?, ?, 'COMMITTED', ?)
                                """);
                        PreparedStatement metadata = connection.prepareStatement(metadataUpdateSql)) {
                    state.setLong(1, upper);
                    state.setLong(2, nextPublished);
                    if (state.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "batch commit lost publication state", false, null);
                    }
                    PublicationCommitResult[] results = new PublicationCommitResult[requests.size()];
                    for (int index = 0; index < sorted.size(); index++) {
                        IndexedIntentCommit item = sorted.get(index);
                        long commitTs = nextTs + index + 1;
                        entry.setLong(1, commitTs);
                        entry.setLong(2, item.request().txId());
                        entry.setLong(3, item.request().expiresAtMillis());
                        entry.addBatch();
                        results[item.index()] = new PublicationCommitResult(commitTs, nextPublished >= commitTs);
                    }
                    entry.executeBatch();
                    int parameter = 1;
                    for (int index = 0; index < sorted.size(); index++) {
                        metadata.setLong(parameter++, sorted.get(index).request().txId());
                        metadata.setLong(parameter++, nextTs + index + 1);
                    }
                    for (IndexedIntentCommit item : sorted) {
                        metadata.setLong(parameter++, item.request().txId());
                    }
                    // All locked metadata must be converted to COMMITTED, otherwise the whole batch decision and the time slot rollback.
                    if (metadata.executeUpdate() != sorted.size()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "batch commit lost transaction metadata", false, null);
                    }
                    timings[2] = System.nanoTime() - entered;
                    return List.copyOf(Arrays.asList(results));
                }
            });
            long finished = System.nanoTime() - entered;
            batchCalls.increment();
            batchValidationNanos.add(timings[0]);
            batchStateWaitNanos.add(timings[1] - timings[0]);
            batchWriteNanos.add(timings[2] - timings[1]);
            batchFinishNanos.add(finished - timings[2]);
            return committed;
        } catch (final SQLException ex) {
            throw storageError("batch intent commit failed", null, ex);
        }
    }

    /**
     * Call a single server to complete the atomic commit and instant release of normal intent transaction.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private PublicationCommitResult callAtomicIntentProcedure(final long txId, final long expiresAtMillis,
                                                              final String participants) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement call = connection.prepareStatement("call mvcc_ledger_intent_commit_v1(?, ?, ?)")) {
            call.setLong(1, txId);
            call.setLong(2, expiresAtMillis);
            call.setString(3, participants);
            boolean hasResult = call.execute();
            while (!hasResult && call.getUpdateCount() != -1) {
                hasResult = call.getMoreResults();
            }
            if (!hasResult) {
                throw new SQLException("atomic intent procedure returned no decision");
            }
            try (ResultSet result = call.getResultSet()) {
                if (!result.next()) {
                    throw new SQLException("atomic intent procedure returned no commit timestamp");
                }
                return new PublicationCommitResult(result.getLong("commit_ts"),
                        result.getBoolean("published_immediately"));
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 30102) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "existing reservation needs recovery", false, txId, ex);
            }
            if (ex.getErrorCode() == 30104) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "snapshot lease expired before intent commit", false, txId, ex);
            }
            if (ex.getErrorCode() == 30103 || ex.getErrorCode() == 30105 || ex.getErrorCode() == 30106) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "atomic intent commit rejected", false, txId, ex);
            }
            throw storageError("atomic intent procedure failed", txId, ex);
        }
    }

    /**
     * Atomic Writes to COMMITTED decision and the book terminal state, repeats that commit accepts only the same timestamp.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param participants participants
     */
    public void decideCommitted(final long txId, final long commitTs, final Set<String> participants) {
        TransactionMetadataValidation.validateParticipants(txId, participants);
        String encoded = participants.stream().sorted().collect(Collectors.joining(","));
        try {
            MysqlConnectionContext.inTransaction(dataSource, connection -> {
                PublicationEntryStatus entry = lockEntry(connection, txId, commitTs);
                if (entry == PublicationEntryStatus.COMMITTED) {
                    verifyCommittedRecord(connection, txId, commitTs);
                    return null;
                }
                if (entry != PublicationEntryStatus.PENDING) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "publication entry already aborted", false, txId);
                }
                try (PreparedStatement record = connection.prepareStatement("""
                        select start_ts, status, commit_ts, participants from mvcc_txn
                        where tx_id = ? for update
                        """)) {
                    record.setLong(1, txId);
                    try (ResultSet current = record.executeQuery()) {
                        if (!current.next() || current.getLong("start_ts") >= commitTs
                                || current.getObject("commit_ts") != null
                                || !Set.of("ACTIVE", "PREPARING").contains(current.getString("status"))
                                || !encoded.equals(current.getString("participants"))) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "commit decision does not match durable transaction", false, txId);
                        }
                    }
                }
                try (PreparedStatement lease = connection.prepareStatement("""
                        select 1 from mvcc_snapshot_lease_v2
                        where tx_id = ? and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """)) {
                    lease.setLong(1, txId);
                    try (ResultSet valid = lease.executeQuery()) {
                        if (!valid.next()) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "snapshot lease expired before commit decision", false, txId);
                        }
                    }
                }
                try (
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'COMMITTED', commit_ts = ? where tx_id = ?
                                """);
                        PreparedStatement ledger = connection.prepareStatement("""
                                update mvcc_publication_entry_v1 set status = 'COMMITTED'
                                where commit_ts = ? and tx_id = ? and status = 'PENDING'
                                """)) {
                    metadata.setLong(1, commitTs);
                    metadata.setLong(2, txId);
                    metadata.executeUpdate();
                    ledger.setLong(1, commitTs);
                    ledger.setLong(2, txId);
                    if (ledger.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication decision lost its pending entry", false, txId);
                    }
                }
                return null;
            });
        } catch (final SQLException ex) {
            throw storageError("persist publication decision failed", txId, ex);
        }
    }

    /**
     * Lock transaction identity in current database transaction and reserve validate rows.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private PublicationEntryStatus lockEntry(final Connection connection, final long txId, final long commitTs) throws SQLException {
        try (PreparedStatement entry = connection.prepareStatement("""
                select tx_id, status from mvcc_publication_entry_v1
                where commit_ts = ? for update
                """)) {
            entry.setLong(1, commitTs);
            try (ResultSet result = entry.executeQuery()) {
                if (!result.next() || result.getLong("tx_id") != txId) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "publication reservation mismatch", false, txId);
                }
                return PublicationEntryStatus.valueOf(result.getString("status"));
            }
        }
    }

    /**
     * Idempotent commit accepts only COMMITTED timestamp from the original center.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void verifyCommittedRecord(final Connection connection, final long txId, final long commitTs) throws SQLException {
        try (PreparedStatement record = connection.prepareStatement("""
                select status, commit_ts from mvcc_txn where tx_id = ? for update
                """)) {
            record.setLong(1, txId);
            try (ResultSet result = record.executeQuery()) {
                if (!result.next() || !"COMMITTED".equals(result.getString("status"))
                        || result.getObject("commit_ts") == null || result.getLong("commit_ts") != commitTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "publication replay differs from durable commit", false, txId);
                }
            }
        }
    }

    /**
     * Participant closing allows the PENDING preset to be marked ABORTED only after it has been completed.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    public void decideAbortedAfterFencing(final long txId, final long commitTs) {
        try {
            MysqlConnectionContext.inTransaction(dataSource, connection -> {
                PublicationEntryStatus entry = lockEntry(connection, txId, commitTs);
                if (entry == PublicationEntryStatus.ABORTED) {
                    verifyAbortedRecord(connection, txId);
                    return null;
                }
                if (entry == PublicationEntryStatus.COMMITTED) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "cannot abort committed publication", false, txId);
                }
                boolean alreadyAborted;
                try (PreparedStatement record = connection.prepareStatement("""
                        select status, commit_ts from mvcc_txn where tx_id = ? for update
                        """)) {
                    record.setLong(1, txId);
                    try (ResultSet current = record.executeQuery()) {
                        if (!current.next() || current.getObject("commit_ts") != null) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "abort cannot cross a committed or local pending decision", false, txId);
                        }
                        String status = current.getString("status");
                        alreadyAborted = "ABORTED".equals(status);
                        if (!alreadyAborted && !Set.of("ACTIVE", "PREPARING").contains(status)) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "abort decision has invalid metadata state", false, txId);
                        }
                    }
                }
                if (!alreadyAborted) {
                    try (PreparedStatement metadata = connection.prepareStatement("""
                            update mvcc_txn set status = 'ABORTED', commit_ts = null
                            where tx_id = ? and status in ('ACTIVE', 'PREPARING') and commit_ts is null
                            """)) {
                        metadata.setLong(1, txId);
                        if (metadata.executeUpdate() != 1) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "cannot abort transaction without undecided metadata", false, txId);
                        }
                    }
                }
                try (PreparedStatement ledger = connection.prepareStatement("""
                        update mvcc_publication_entry_v1 set status = 'ABORTED'
                        where commit_ts = ? and tx_id = ? and status = 'PENDING'
                        """)) {
                    ledger.setLong(1, commitTs);
                    ledger.setLong(2, txId);
                    if (ledger.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "abort decision lost its pending entry", false, txId);
                    }
                }
                return null;
            });
        } catch (final SQLException ex) {
            throw storageError("abort publication reservation failed", txId, ex);
        }
    }

    /**
     * Idempotent rollback must be consistent with ABORTED terminal state at the centre of durable storage.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void verifyAbortedRecord(final Connection connection, final long txId) throws SQLException {
        try (PreparedStatement record = connection.prepareStatement("""
                select status from mvcc_txn where tx_id = ? for update
                """)) {
            record.setLong(1, txId);
            try (ResultSet result = record.executeQuery()) {
                if (!result.next() || !"ABORTED".equals(result.getString(1))) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "publication replay differs from durable abort", false, txId);
                }
            }
        }
    }

    /**
     * When local commit proof is established, atomic completes the decision and releases it as soon as there is no front-runner pending slot.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean completeLocalCommitAfterFencing(final ParticipantRecord local, final long commitTs, final String participant) {
        long txId = local.txId();
        TransactionMetadataValidation.validateParticipant(txId, participant);
        if (local.status() != TxStatus.COMMITTED || local.commitTs() == null
                || local.commitTs() != commitTs) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local participant proof does not match reserved commit", false, txId);
        }
        if (localCompleteProcedureReady) {
            return callLocalCompleteProcedure(local, commitTs, participant);
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long publishedTs;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing during local commit resolution", false, txId);
                    }
                    publishedTs = row.getLong(1);
                }
                PublicationEntryStatus entry = lockEntry(connection, txId, commitTs);
                if (entry == PublicationEntryStatus.COMMITTED) {
                    verifyCommittedRecord(connection, txId, commitTs);
                    return publishedTs >= commitTs;
                }
                if (entry != PublicationEntryStatus.PENDING) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local committed proof conflicts with aborted publication", false, txId);
                }
                try (PreparedStatement record = connection.prepareStatement("""
                        select start_ts, status, commit_ts, participants from mvcc_txn
                        where tx_id = ? for update
                        """)) {
                    record.setLong(1, txId);
                    try (ResultSet current = record.executeQuery()) {
                        if (!current.next() || current.getLong("start_ts") != local.startTs()
                                || !"PREPARING".equals(current.getString("status"))
                                || current.getObject("commit_ts") == null
                                || current.getLong("commit_ts") != commitTs
                                || !participant.equals(current.getString("participants"))) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local participant proof differs from control reservation", false, txId);
                        }
                    }
                }
                try (
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'COMMITTED' where tx_id = ?
                                """);
                        PreparedStatement ledger = connection.prepareStatement("""
                                update mvcc_publication_entry_v1 set status = 'COMMITTED'
                                where commit_ts = ? and tx_id = ? and status = 'PENDING'
                                """)) {
                    metadata.setLong(1, txId);
                    if (metadata.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local commit recovery lost central metadata", false, txId);
                    }
                    ledger.setLong(1, commitTs);
                    ledger.setLong(2, txId);
                    if (ledger.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local commit recovery lost pending publication", false, txId);
                    }
                }
                // Terminal state and watermark have the same MySQL transaction external effect when a continuous prefix has reached its previous slot.
                if (publishedTs == commitTs - 1) {
                    try (PreparedStatement publish = connection.prepareStatement("""
                            update mvcc_publication_state_v1 set published_ts = ?
                            where singleton_id = 1 and published_ts = ?
                            """)) {
                        publish.setLong(1, commitTs);
                        publish.setLong(2, publishedTs);
                        if (publish.executeUpdate() != 1) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local commit lost contiguous publication state", false, txId);
                        }
                    }
                    return true;
                }
                return false;
            });
        } catch (final SQLException ex) {
            throw storageError("complete local participant publication failed", txId, ex);
        }
    }

    /**
     * Use a single serviceend to call atomic to complete the centre local commit and instantaneously release decision.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private boolean callLocalCompleteProcedure(final ParticipantRecord local, final long commitTs, final String participant) {
        long txId = local.txId();
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement call = connection.prepareStatement("call mvcc_ledger_local_complete_v2(?, ?, ?, ?)")) {
            call.setLong(1, txId);
            call.setLong(2, local.startTs());
            call.setLong(3, commitTs);
            call.setString(4, participant);
            boolean hasResult = call.execute();
            while (!hasResult && call.getUpdateCount() != -1) {
                hasResult = call.getMoreResults();
            }
            if (!hasResult) {
                throw new SQLException("local completion procedure returned no publication result");
            }
            try (ResultSet row = call.getResultSet()) {
                if (!row.next()) {
                    throw new SQLException("local completion procedure returned no decision");
                }
                return row.getBoolean("published_immediately");
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() >= 30501 && ex.getErrorCode() <= 30504) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local completion rejected durable participant proof", false, txId, ex);
            }
            throw storageError("server-side local commit completion failed", txId, ex);
        }
    }

    /**
     * A control transaction check multiple local COMMITTED certificates and advance a continuous terminal state prefix.
     *
     * @param proofs proofs
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Boolean> completeLocalCommitBatch(final List<PublicationLocalCommitProof> proofs) {
        if (proofs.isEmpty()) {
            return List.of();
        }
        if (proofs.size() == 1) {
            PublicationLocalCommitProof proof = proofs.getFirst();
            return List.of(completeLocalCommitAfterFencing(proof.local(), proof.commitTs(), proof.participant()));
        }
        if (proofs.size() > 64) {
            throw new IllegalArgumentException("local completion batch exceeds 64 transactions");
        }
        List<IndexedLocalProof> sorted = new ArrayList<>(proofs.size());
        for (int index = 0; index < proofs.size(); index++) {
            sorted.add(new IndexedLocalProof(index, Objects.requireNonNull(proofs.get(index), "proof")));
        }
        sorted.sort(Comparator.comparingLong(item -> item.proof().commitTs()));
        for (int index = 1; index < sorted.size(); index++) {
            if (sorted.get(index - 1).proof().commitTs() == sorted.get(index).proof().commitTs()
                    || sorted.get(index - 1).proof().local().txId() == sorted.get(index).proof().local().txId()) {
                throw new IllegalArgumentException("local completion batch contains duplicate decision");
            }
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing for local completion batch", false, null);
                    }
                    published = row.getLong(1);
                }
                try (
                        PreparedStatement decision = connection.prepareStatement("""
                                select e.tx_id, e.status as entry_status,
                                       t.start_ts, t.status as record_status, t.commit_ts, t.participants
                                from mvcc_publication_entry_v1 e
                                straight_join mvcc_txn t on t.tx_id = e.tx_id
                                where e.commit_ts = ? for update
                                """);
                        PreparedStatement commitMetadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'COMMITTED'
                                where tx_id = ? and status = 'PREPARING' and commit_ts = ?
                                """);
                        PreparedStatement commitEntry = connection.prepareStatement("""
                                update mvcc_publication_entry_v1 set status = 'COMMITTED'
                                where commit_ts = ? and tx_id = ? and status = 'PENDING'
                                """)) {
                    for (IndexedLocalProof item : sorted) {
                        PublicationLocalCommitProof proof = item.proof();
                        long txId = proof.local().txId();
                        decision.setLong(1, proof.commitTs());
                        PublicationEntryStatus entry;
                        try (ResultSet row = decision.executeQuery()) {
                            if (!row.next() || row.getLong("tx_id") != txId) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "local completion has no matching ledger reservation", false, txId);
                            }
                            entry = PublicationEntryStatus.valueOf(row.getString("entry_status"));
                            String status = row.getString("record_status");
                            if (entry == PublicationEntryStatus.ABORTED
                                    || entry == PublicationEntryStatus.PENDING && published >= proof.commitTs()
                                    || row.getLong("start_ts") != proof.local().startTs()
                                    || row.getObject("commit_ts") == null
                                    || row.getLong("commit_ts") != proof.commitTs()
                                    || !proof.participant().equals(row.getString("participants"))
                                    || entry == PublicationEntryStatus.PENDING && !"PREPARING".equals(status)
                                    || entry == PublicationEntryStatus.COMMITTED && !"COMMITTED".equals(status)) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "local completion differs from durable candidate metadata", false, txId);
                            }
                        }
                        if (entry == PublicationEntryStatus.PENDING) {
                            commitMetadata.setLong(1, txId);
                            commitMetadata.setLong(2, proof.commitTs());
                            commitMetadata.addBatch();
                            commitEntry.setLong(1, proof.commitTs());
                            commitEntry.setLong(2, txId);
                            commitEntry.addBatch();
                        }
                    }
                    commitMetadata.executeBatch();
                    commitEntry.executeBatch();
                }
                // The number of article-by-article returns when Contractor/J rewrites batch UPDATE is unstable and real terminal state in direct validate transaction.
                String placeholders = String.join(",", Collections.nCopies(sorted.size(), "?"));
                try (
                        PreparedStatement metadata = connection.prepareStatement("""
                                select count(*) from mvcc_txn
                                where tx_id in (%s) and status = 'COMMITTED'
                                """.formatted(placeholders));
                        PreparedStatement entries = connection.prepareStatement("""
                                select count(*) from mvcc_publication_entry_v1
                                where commit_ts in (%s) and status = 'COMMITTED'
                                """.formatted(placeholders))) {
                    for (int index = 0; index < sorted.size(); index++) {
                        metadata.setLong(index + 1, sorted.get(index).proof().local().txId());
                        entries.setLong(index + 1, sorted.get(index).proof().commitTs());
                    }
                    try (ResultSet rows = metadata.executeQuery()) {
                        if (!rows.next() || rows.getInt(1) != sorted.size()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local completion batch lost metadata candidate", false, null);
                        }
                    }
                    try (ResultSet rows = entries.executeQuery()) {
                        if (!rows.next() || rows.getInt(1) != sorted.size()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local completion batch lost ledger reservation", false, null);
                        }
                    }
                }
                long nextPublished = published;
                try (PreparedStatement entries = connection.prepareStatement("""
                        select commit_ts, status from mvcc_publication_entry_v1
                        where commit_ts > ? order by commit_ts limit 4096
                        """)) {
                    entries.setLong(1, published);
                    try (ResultSet rows = entries.executeQuery()) {
                        while (rows.next()) {
                            long candidate = rows.getLong("commit_ts");
                            if (candidate != nextPublished + 1
                                    || PublicationEntryStatus.valueOf(rows.getString("status")) == PublicationEntryStatus.PENDING) {
                                break;
                            }
                            nextPublished = candidate;
                        }
                    }
                }
                if (nextPublished != published) {
                    try (PreparedStatement state = connection.prepareStatement("""
                            update mvcc_publication_state_v1 set published_ts = ? where singleton_id = 1
                            """)) {
                        state.setLong(1, nextPublished);
                        if (state.executeUpdate() != 1) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local completion batch lost publication state", false, null);
                        }
                    }
                }
                Boolean[] results = new Boolean[proofs.size()];
                for (IndexedLocalProof item : sorted) {
                    results[item.index()] = nextPublished >= item.proof().commitTs();
                }
                return List.copyOf(Arrays.asList(results));
            });
        } catch (final SQLException ex) {
            throw storageError("batch local commit completion failed", null, ex);
        }
    }

    /**
     * Only the continuous terminal state prefix will be advanced after an exclusive locking control line, and a single scan will remain bounded.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public long advancePublished(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("publication batch limit must be between 1 and 4096");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                long upper;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts, next_commit_ts from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet result = state.executeQuery()) {
                    if (!result.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing", false, null);
                    }
                    published = result.getLong(1);
                    upper = result.getLong(2);
                }
                long next = published;
                try (PreparedStatement entries = connection.prepareStatement("""
                        select commit_ts, status from mvcc_publication_entry_v1
                        where commit_ts > ? order by commit_ts limit ?
                        """)) {
                    entries.setLong(1, published);
                    entries.setInt(2, limit);
                    try (ResultSet result = entries.executeQuery()) {
                        while (result.next()) {
                            long candidate = result.getLong(1);
                            if (candidate != next + 1
                                    || PublicationEntryStatus.valueOf(result.getString(2)) == PublicationEntryStatus.PENDING) {
                                break;
                            }
                            next = candidate;
                        }
                    }
                }
                if (next > upper) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "published watermark exceeds reserved sequence", false, null);
                }
                if (next != published) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_publication_state_v1 set published_ts = ? where singleton_id = 1
                            """)) {
                        update.setLong(1, next);
                        update.executeUpdate();
                    }
                }
                return next;
            });
        } catch (final SQLException ex) {
            throw storageError("advance published watermark failed", null, ex);
        }
    }

    /**
     * Query has been issued commit watermark, and each RC statement should read the value again.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public long currentPublished() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement state = connection.prepareStatement("""
                        select published_ts from mvcc_publication_state_v1 where singleton_id = 1
                        """);
                ResultSet result = state.executeQuery()) {
            if (!result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication state missing", false, null);
            }
            return result.getLong(1);
        } catch (final SQLException ex) {
            throw storageError("read published watermark failed", null, ex);
        }
    }

    /**
     * Query the timestamp preset and terminal state specified for transaction to respond to unknown and recovery process locator authority records.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<PublicationEntry> findByTxId(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select tx_id, commit_ts, status, expires_at
                        from mvcc_publication_entry_v1 where tx_id = ?
                        """)) {
            query.setLong(1, txId);
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? Optional.of(mapEntry(result)) : Optional.empty();
            }
        } catch (final SQLException ex) {
            throw storageError("load publication entry failed", txId, ex);
        }
    }

    /**
     * Returns PENDING batch, which is due; the receiver still has to recheck lease and participant terminal state.
     *
     * @param nowMillis now millis
     * @param afterCommitTs after commit ts
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<PublicationEntry> findExpiredPending(final long nowMillis, final long afterCommitTs, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("pending scan limit must be between 1 and 4096");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select tx_id, commit_ts, status, expires_at
                        from mvcc_publication_entry_v1
                        where status = 'PENDING' and expires_at <= ? and commit_ts > ?
                        order by commit_ts limit ?
                        """)) {
            query.setLong(1, nowMillis);
            query.setLong(2, afterCommitTs);
            query.setInt(3, limit);
            try (ResultSet result = query.executeQuery()) {
                List<PublicationEntry> entries = new ArrayList<>();
                while (result.next()) {
                    entries.add(mapEntry(result));
                }
                return List.copyOf(entries);
            }
        } catch (final SQLException ex) {
            throw storageError("scan expired publication entries failed", null, ex);
        }
    }

    /**
     * Finds unset and lease invalid pending or aborted transaction to overwrite the failure of participant after aborting rollback.
     *
     * @param nowMillis now millis
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> findExpiredUnreserved(final long nowMillis, final long afterTxId, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("unreserved scan limit must be between 1 and 4096");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select t.tx_id from mvcc_txn t
                        left join mvcc_snapshot_lease_v2 s on s.tx_id = t.tx_id
                        left join mvcc_publication_entry_v1 e on e.tx_id = t.tx_id
                        left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                        where t.tx_id > ? and (s.tx_id is null or s.expires_at <= ?)
                          and e.tx_id is null and f.tx_id is null
                          and t.status in ('ACTIVE', 'PREPARING', 'ABORTED') and t.commit_ts is null
                        order by t.tx_id limit ?
                        """)) {
            query.setLong(1, afterTxId);
            query.setLong(2, nowMillis);
            query.setInt(3, limit);
            try (ResultSet result = query.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (result.next()) {
                    ids.add(result.getLong(1));
                }
                return List.copyOf(ids);
            }
        } catch (final SQLException ex) {
            throw storageError("scan expired unreserved transactions failed", null, ex);
        }
    }

    /**
     * Only scan terminal state of new book but transaction of participant materialization is not completed, excluding old protocol records before moving.
     *
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> findUnfinalizedTerminal(final long afterTxId, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("terminal scan limit must be between 1 and 4096");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select e.tx_id from mvcc_publication_entry_v1 e
                        left join mvcc_txn_finalized f on f.tx_id = e.tx_id
                        where e.tx_id > ? and e.status in ('COMMITTED', 'ABORTED') and f.tx_id is null
                        order by e.tx_id limit ?
                        """)) {
            query.setLong(1, afterTxId);
            query.setInt(2, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (rows.next()) {
                    ids.add(rows.getLong(1));
                }
                return List.copyOf(ids);
            }
        } catch (final SQLException ex) {
            throw storageError("scan unfinalized publication decisions failed", null, ex);
        }
    }

    /**
     * Decoding SQL lines to immutable transaction release record.
     *
     * @param result result
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private PublicationEntry mapEntry(final ResultSet result) throws SQLException {
        return new PublicationEntry(result.getLong("tx_id"), result.getLong("commit_ts"),
                PublicationEntryStatus.valueOf(result.getString("status")), result.getLong("expires_at"));
    }

    /**
     * Commit decision can be confirmed to business only after the release of watermark, pending an unknown return status when budget is exhausted.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param timeoutMillis timeout millis
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public void awaitPublished(final long txId, final long commitTs, final long timeoutMillis) {
        if (timeoutMillis < 1) {
            throw new IllegalArgumentException("publication wait timeout must be positive");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            PublicationObservation observation = observePublication(txId);
            if (observation.commitTs() != commitTs || observation.status() == PublicationEntryStatus.ABORTED) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "publication decision differs from expected commit", false, txId);
            }
            if (observation.status() == PublicationEntryStatus.COMMITTED
                    && observation.publishedTs() >= commitTs) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "committed transaction is waiting for an earlier publication slot", false, txId);
            }
            try {
                Thread.sleep(1);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "publication wait interrupted after commit decision", false, txId, ex);
            }
        }
    }

    /**
     * The same MySQL statement snapshot reads decision and watermark, avoiding waiting for a loop to repeat connection.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private PublicationObservation observePublication(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select s.published_ts, e.commit_ts, e.status
                        from mvcc_publication_state_v1 s
                        left join mvcc_publication_entry_v1 e on e.tx_id = ?
                        where s.singleton_id = 1
                        """)) {
            query.setLong(1, txId);
            try (ResultSet result = query.executeQuery()) {
                if (!result.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "publication state missing while waiting for commit", false, txId);
                }
                if (result.getObject("commit_ts") == null || result.getString("status") == null) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "publication reservation disappeared", false, txId);
                }
                return new PublicationObservation(result.getLong("published_ts"),
                        result.getLong("commit_ts"), PublicationEntryStatus.valueOf(result.getString("status")));
            }
        } catch (final SQLException ex) {
            throw storageError("observe publication decision failed", txId, ex);
        }
    }

    /**
     * Returns safe point in the ledger, reading access and GC must use the same control status.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public long currentSafePoint() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement state = connection.prepareStatement("""
                        select safe_point from mvcc_publication_state_v1 where singleton_id = 1
                        """);
                ResultSet result = state.executeQuery()) {
            if (!result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication state missing", false, null);
            }
            return result.getLong(1);
        } catch (final SQLException ex) {
            throw storageError("read publication safe point failed", null, ex);
        }
    }

    /**
     * The original deadline for searching for new protocol lease, the transaction entrance should be reviewed before each storage operation.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public OptionalLong snapshotExpiry(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement lease = connection.prepareStatement("""
                        select expires_at from mvcc_snapshot_lease_v2 where tx_id = ?
                        """)) {
            lease.setLong(1, txId);
            try (ResultSet result = lease.executeQuery()) {
                return result.next() ? OptionalLong.of(result.getLong(1)) : OptionalLong.empty();
            }
        } catch (final SQLException ex) {
            throw storageError("read publication snapshot expiry failed", txId, ex);
        }
    }

    /**
     * Share the release watermark lock to register coordinator below durable snapshot and fix lease.
     *
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public CoordinatorSnapshotLease acquireCoordinatorLease(final long timeoutMillis) {
        if (timeoutMillis < 1 || timeoutMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid coordinator snapshot lease timeout");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireLedger(connection);
                long readTs;
                long deadline;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                        from mvcc_publication_state_v1 where singleton_id = 1 for share
                        """)) {
                    state.setLong(1, timeoutMillis);
                    try (ResultSet row = state.executeQuery()) {
                        if (!row.next() || row.getLong("published_ts") < row.getLong("safe_point")) {
                            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                                    "coordinator lease cannot protect published snapshot", false, null);
                        }
                        readTs = row.getLong("published_ts");
                        deadline = row.getLong("deadline");
                    }
                }
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_coordinator_snapshot_lease_v1(read_ts, expires_at, generation)
                        values (?, ?, 1)
                        """, Statement.RETURN_GENERATED_KEYS)) {
                    insert.setLong(1, readTs);
                    insert.setLong(2, deadline);
                    if (insert.executeUpdate() != 1) {
                        throw new SQLException("coordinator lease insert did not persist one row");
                    }
                    try (ResultSet keys = insert.getGeneratedKeys()) {
                        if (!keys.next()) {
                            throw new SQLException("coordinator lease did not return id");
                        }
                        return new CoordinatorSnapshotLease(keys.getLong(1), readTs, deadline, 1);
                    }
                }
            });
        } catch (final SQLException ex) {
            throw storageError("acquire coordinator snapshot lease failed", null, ex);
        }
    }

    /**
     * Do not write each transaction lease and consistently read the published watermark request that coordinator lease overwrite the whole transaction period.
     *
     * @param coordinatorId coordinator id
     * @param generation generation
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public CoordinatorSnapshotView snapshotUnderCoordinatorLease(final long coordinatorId, final long generation,
                                                                 final long timeoutMillis) {
        if (coordinatorId < 1 || generation < 1 || timeoutMillis < 1 || timeoutMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid coordinator protected begin");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select s.published_ts, s.safe_point, c.read_ts, c.expires_at, c.generation,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                        from mvcc_publication_state_v1 s
                        join mvcc_protocol_epoch_v1 p on p.singleton_id = s.singleton_id
                          and p.protocol_mode = 'LEDGER'
                        join mvcc_coordinator_snapshot_lease_v1 c on c.coordinator_id = ?
                        where s.singleton_id = 1
                        """)) {
            query.setLong(1, timeoutMillis);
            query.setLong(2, coordinatorId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "coordinator snapshot lease or ledger mode is unavailable", false, null);
                }
                long published = row.getLong("published_ts");
                long safePoint = row.getLong("safe_point");
                long leaseRead = row.getLong("read_ts");
                long leaseExpiry = row.getLong("expires_at");
                long deadline = row.getLong("deadline");
                if (row.getLong("generation") != generation || leaseExpiry < deadline) {
                    throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                            "coordinator snapshot lease does not cover transaction deadline", false, null);
                }
                if (published < safePoint || leaseRead > published || leaseRead < safePoint) {
                    throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                            "coordinator snapshot is outside published safe boundary", false, null);
                }
                return new CoordinatorSnapshotView(published, deadline, leaseRead, leaseExpiry);
            }
        } catch (final SQLException ex) {
            throw storageError("read coordinator protected snapshot failed", null, ex);
        }
    }

    /**
     * A database reading of simultaneous validate durable algebras, snapshot lower bounds and database hours to reduce the controlled face-to-face movement of readable statements.
     *
     * @param coordinatorId coordinator id
     * @param generation generation
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param txId transaction identifier
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public void assertCoordinatorSnapshot(final long coordinatorId, final long generation, final long startTs,
                                          final long deadlineMillis, final long txId) {
        if (coordinatorId < 1 || generation < 1 || startTs < 0 || deadlineMillis < 1 || txId >= 0) {
            throw new IllegalArgumentException("invalid coordinator snapshot verification");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select read_ts, expires_at, generation,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as database_now
                        from mvcc_coordinator_snapshot_lease_v1 where coordinator_id = ?
                        """)) {
            query.setLong(1, coordinatorId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong("generation") != generation
                        || row.getLong("read_ts") > startTs
                        || row.getLong("expires_at") < deadlineMillis
                        || row.getLong("expires_at") <= row.getLong("database_now")) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "coordinator lease no longer protects transaction", false, txId);
                }
                if (row.getLong("database_now") >= deadlineMillis) {
                    throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                            "transaction lease expired", false, txId);
                }
            }
        } catch (final SQLException ex) {
            throw storageError("verify coordinator snapshot failed", txId, ex);
        }
    }

    /**
     * Raise local transaction ID to independent durable lease before initial writing, avoiding the loss of an recovery anchor after coordinator lease has been raised.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param coordinatorId coordinator id
     * @param generation generation
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void promoteCoordinatorSnapshot(final long txId, final long startTs, final long deadlineMillis,
                                           final long coordinatorId, final long generation) {
        if (txId >= 0 || startTs < 0 || deadlineMillis < 1 || coordinatorId < 1 || generation < 1) {
            throw new IllegalArgumentException("invalid coordinator transaction promotion");
        }
        try {
            MysqlConnectionContext.inTransaction(dataSource, connection -> {
                promoteCoordinatorSnapshotInTransaction(connection, txId, startTs, deadlineMillis,
                        coordinatorId, generation);
                return null;
            });
        } catch (final SQLException ex) {
            throw storageError("promote coordinator snapshot failed", txId, ex);
        }
    }

    /**
     * Verify coordinator protects and establishes an independent snapshot lease in the same control transaction.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param coordinatorId coordinator id
     * @param generation generation
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void promoteCoordinatorSnapshotInTransaction(final Connection connection, final long txId, final long startTs,
                                                         final long deadlineMillis, final long coordinatorId,
                                                         final long generation) throws SQLException {
        MysqlProtocolEpoch.requireLedger(connection);
        long published;
        long safePoint;
        try (
                PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point from mvcc_publication_state_v1
                        where singleton_id = 1 for share
                        """);
                ResultSet row = state.executeQuery()) {
            if (!row.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication state missing before coordinator promotion", false, txId);
            }
            published = row.getLong("published_ts");
            safePoint = row.getLong("safe_point");
        }
        long leaseRead;
        long leaseExpiry;
        long actualGeneration;
        try (PreparedStatement lease = connection.prepareStatement("""
                select read_ts, expires_at, generation
                from mvcc_coordinator_snapshot_lease_v1
                where coordinator_id = ? for share
                """)) {
            lease.setLong(1, coordinatorId);
            try (ResultSet row = lease.executeQuery()) {
                if (!row.next()) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "coordinator lease missing during write promotion", false, txId);
                }
                leaseRead = row.getLong("read_ts");
                leaseExpiry = row.getLong("expires_at");
                actualGeneration = row.getLong("generation");
            }
        }
        long now;
        try (
                PreparedStatement clock = connection.prepareStatement("""
                        select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """);
                ResultSet row = clock.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("database clock returned no promotion time");
            }
            now = row.getLong(1);
        }
        if (actualGeneration != generation || leaseExpiry <= now || deadlineMillis <= now
                || deadlineMillis > leaseExpiry) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "coordinator lease expired before write promotion", false, txId);
        }
        if (startTs < safePoint || startTs < leaseRead || startTs > published) {
            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                    "write promotion snapshot is outside coordinator protection", false, txId);
        }
        boolean replay = false;
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_snapshot_lease_v2(tx_id, read_ts, expires_at)
                values (?, ?, ?)
                """)) {
            insert.setLong(1, txId);
            insert.setLong(2, startTs);
            insert.setLong(3, deadlineMillis);
            try {
                insert.executeUpdate();
            } catch (final SQLException ex) {
                if (ex.getErrorCode() != 1062) {
                    throw ex;
                }
                replay = true;
            }
        }
        if (replay) {
            try (PreparedStatement verify = connection.prepareStatement("""
                    select read_ts, expires_at from mvcc_snapshot_lease_v2
                    where tx_id = ? for update
                    """)) {
                verify.setLong(1, txId);
                try (ResultSet row = verify.executeQuery()) {
                    if (!row.next() || row.getLong("read_ts") != startTs
                            || row.getLong("expires_at") != deadlineMillis) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "promoted snapshot conflicts with another transaction", false, txId);
                    }
                }
            }
        }
    }

    /**
     * The independent lease that will be written for the first time with complete participant metadata is commit, avoiding the middle state and a second connection round trip.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param coordinatorId coordinator id
     * @param generation generation
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public TxRecord promoteAndRegisterCoordinatorSnapshot(final long txId, final long startTs,
                                                          final long deadlineMillis, final long coordinatorId,
                                                          final long generation, final IsolationLevel isolationLevel,
                                                          final Set<String> participants) {
        if (txId >= 0 || startTs < 0 || deadlineMillis < 1 || coordinatorId < 1 || generation < 1) {
            throw new IllegalArgumentException("invalid coordinator transaction registration");
        }
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        TransactionMetadataValidation.validateParticipants(txId, participants);
        String encoded = participants.stream().sorted().collect(Collectors.joining(","));
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                promoteCoordinatorSnapshotInTransaction(connection, txId, startTs, deadlineMillis,
                        coordinatorId, generation);
                boolean replay = false;
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                        values (?, ?, ?, 'ACTIVE', null, ?)
                        """)) {
                    insert.setLong(1, txId);
                    insert.setLong(2, startTs);
                    insert.setString(3, isolationLevel.name());
                    insert.setString(4, encoded);
                    try {
                        insert.executeUpdate();
                    } catch (final SQLException ex) {
                        if (ex.getErrorCode() != 1062) {
                            throw ex;
                        }
                        replay = true;
                    }
                }
                if (replay) {
                    try (PreparedStatement verify = connection.prepareStatement("""
                            select start_ts, isolation_level, status, commit_ts, participants
                            from mvcc_txn where tx_id = ? for update
                            """)) {
                        verify.setLong(1, txId);
                        try (ResultSet row = verify.executeQuery()) {
                            if (!row.next() || row.getLong("start_ts") != startTs
                                    || !isolationLevel.name().equals(row.getString("isolation_level"))
                                    || !encoded.equals(row.getString("participants"))
                                    || row.getObject("commit_ts") != null
                                    || !("ACTIVE".equals(row.getString("status"))
                                            || "PREPARING".equals(row.getString("status")))) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "coordinator write registration conflicts with durable transaction", false, txId);
                            }
                            return new TxRecord(txId, startTs, isolationLevel, participants,
                                    TxStatus.valueOf(row.getString("status")), null);
                        }
                    }
                }
                return new TxRecord(txId, startTs, isolationLevel, participants, TxStatus.ACTIVE, null);
            });
        } catch (final SQLException ex) {
            throw storageError("register coordinator write transaction failed", txId, ex);
        }
    }

    /**
     * The algebra fencing lease is renewed and only the oldest snapshot lower bounds are allowed.
     *
     * @param coordinatorId coordinator id
     * @param expectedGeneration expected generation
     * @param nextReadTs next read ts
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public CoordinatorSnapshotLease renewCoordinatorLease(final long coordinatorId, final long expectedGeneration,
                                                          final long nextReadTs, final long timeoutMillis) {
        if (coordinatorId < 1 || expectedGeneration < 1 || nextReadTs < 0
                || timeoutMillis < 1 || timeoutMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid coordinator lease renewal");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                long safePoint;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts, safe_point from mvcc_publication_state_v1
                                where singleton_id = 1 for share
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing during coordinator renewal", false, null);
                    }
                    published = row.getLong(1);
                    safePoint = row.getLong(2);
                }
                long currentRead;
                long currentExpiry;
                long generation;
                try (PreparedStatement lease = connection.prepareStatement("""
                        select read_ts, expires_at, generation
                        from mvcc_coordinator_snapshot_lease_v1
                        where coordinator_id = ? for update
                        """)) {
                    lease.setLong(1, coordinatorId);
                    try (ResultSet row = lease.executeQuery()) {
                        if (!row.next()) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "coordinator lease was removed", false, null);
                        }
                        currentRead = row.getLong("read_ts");
                        currentExpiry = row.getLong("expires_at");
                        generation = row.getLong("generation");
                    }
                }
                long now;
                try (
                        PreparedStatement clock = connection.prepareStatement("""
                                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                                """);
                        ResultSet row = clock.executeQuery()) {
                    if (!row.next()) {
                        throw new SQLException("database clock returned no coordinator lease time");
                    }
                    now = row.getLong(1);
                }
                if (generation != expectedGeneration || currentExpiry <= now) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "coordinator lease generation or expiry changed", false, null);
                }
                if (generation == Long.MAX_VALUE || nextReadTs < currentRead
                        || nextReadTs < safePoint || nextReadTs > published) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "coordinator lease cannot lower protection or pass published watermark", false, null);
                }
                long nextExpiry = Math.max(currentExpiry, Math.addExact(now, timeoutMillis));
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_coordinator_snapshot_lease_v1
                        set read_ts = ?, expires_at = ?, generation = ?
                        where coordinator_id = ? and generation = ?
                        """)) {
                    update.setLong(1, nextReadTs);
                    update.setLong(2, nextExpiry);
                    update.setLong(3, generation + 1);
                    update.setLong(4, coordinatorId);
                    update.setLong(5, generation);
                    if (update.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                "coordinator lease lost its current generation", false, null);
                    }
                }
                return new CoordinatorSnapshotLease(coordinatorId, nextReadTs, nextExpiry, generation + 1);
            });
        } catch (final SQLException ex) {
            throw storageError("renew coordinator snapshot lease failed", null, ex);
        }
    }

    /**
     * Ask for the current algebra of coordinator lease and stop service by the caller when it is missing.
     *
     * @param coordinatorId coordinator id
     * @return the operation result
     */
    public Optional<CoordinatorSnapshotLease> coordinatorLease(final long coordinatorId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select coordinator_id, read_ts, expires_at, generation
                        from mvcc_coordinator_snapshot_lease_v1 where coordinator_id = ?
                        """)) {
            query.setLong(1, coordinatorId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? Optional.of(new CoordinatorSnapshotLease(row.getLong("coordinator_id"),
                        row.getLong("read_ts"), row.getLong("expires_at"), row.getLong("generation")))
                        : Optional.empty();
            }
        } catch (final SQLException ex) {
            throw storageError("read coordinator snapshot lease failed", null, ex);
        }
    }

    /**
     * Only the coordinator of the current algebra can release lease, with no side effects on deleted lines.
     *
     * @param coordinatorId coordinator id
     * @param generation generation
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean releaseCoordinatorLease(final long coordinatorId, final long generation) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_coordinator_snapshot_lease_v1
                        where coordinator_id = ? and generation = ?
                        """)) {
            delete.setLong(1, coordinatorId);
            delete.setLong(2, generation);
            if (delete.executeUpdate() == 1) {
                return true;
            }
            if (coordinatorLease(coordinatorId).isPresent()) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "stale coordinator lease generation cannot release current owner", false, null);
            }
            return false;
        } catch (final SQLException ex) {
            throw storageError("release coordinator snapshot lease failed", null, ex);
        }
    }

    /**
     * After transaction new release idempotently protocol lease, GC concurrent to more conservatively delay this round collection.
     *
     * @param txId transaction identifier
     */
    public void releaseSnapshot(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_snapshot_lease_v2 where tx_id = ?
                        """)) {
            delete.setLong(1, txId);
            delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("release publication snapshot failed", txId, ex);
        }
    }

    /**
     * Share fence control line with begin and advance safe point, keeping the oldest reader and unx3 decision.
     *
     * @return the operation result
     */
    public long advanceSafePoint() {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                long current;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts, safe_point from mvcc_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet result = state.executeQuery()) {
                    if (!result.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "publication state missing", false, null);
                    }
                    published = result.getLong(1);
                    current = result.getLong(2);
                }
                if (current > published) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "safe point already exceeds published watermark", false, null);
                }
                Long oldest = null;
                try (
                        PreparedStatement active = connection.prepareStatement("""
                                select min(read_ts) from (
                                  select read_ts from mvcc_snapshot_lease_v2
                                    where expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                                  union all
                                  select read_ts from mvcc_coordinator_snapshot_lease_v1
                                    where expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                                  union all
                                  select t.start_ts as read_ts from mvcc_txn t
                                    left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                                    where f.tx_id is null
                                ) protected_snapshots
                                """);
                        ResultSet result = active.executeQuery()) {
                    if (result.next() && result.getObject(1) != null) {
                        oldest = result.getLong(1);
                    }
                }
                long candidate = oldest == null ? published
                        : Math.min(published, oldest > 0 ? oldest - 1 : 0);
                long next = Math.max(current, candidate);
                if (next != current) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_publication_state_v1 set safe_point = ? where singleton_id = 1
                            """)) {
                        update.setLong(1, next);
                        update.executeUpdate();
                    }
                }
                return next;
            });
        } catch (final SQLException ex) {
            throw storageError("advance publication safe point failed", null, ex);
        }
    }

    /**
     * Press database clock bounded to remove expired lease, pending metadata and books of accounts are still independently protected recovery.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    public int expireSnapshots(final int limit) {
        checkGcLimit(limit);
        return expireLeasesByPrimaryKey("mvcc_snapshot_lease_v2", "tx_id", limit);
    }

    /**
     * Press database clock bounded to clear the missing coordinator lease and remove old algebras from lease.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    public int expireCoordinatorLeases(final int limit) {
        checkGcLimit(limit);
        return expireLeasesByPrimaryKey("mvcc_coordinator_snapshot_lease_v1", "coordinator_id", limit);
    }

    /**
     * Selecting the due number without lock, then pressing the main key to delete and unify the index lock order of foreground release with background GC.
     *
     * @param table table
     * @param idColumn id column
     * @param limit maximum number of items
     * @return the operation result
     */
    private int expireLeasesByPrimaryKey(final String table, final String idColumn, final int limit) {
        String candidatesSql = "select " + idColumn + " from " + table
                + " where expires_at <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)"
                + " order by expires_at, " + idColumn + " limit ?";
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement candidates = connection.prepareStatement(candidatesSql)) {
            candidates.setInt(1, limit);
            List<Long> ids = new ArrayList<>(limit);
            try (ResultSet rows = candidates.executeQuery()) {
                while (rows.next()) {
                    ids.add(rows.getLong(1));
                }
            }
            if (ids.isEmpty()) {
                return 0;
            }
            ids.sort(Long::compare);
            String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
            String deleteSql = "delete lease from " + table + " lease force index(primary)"
                    + " where lease." + idColumn + " in (" + placeholders + ")"
                    + " and lease.expires_at <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)";
            try (PreparedStatement delete = connection.prepareStatement(deleteSql)) {
                for (int index = 0; index < ids.size(); index++) {
                    delete.setLong(index + 1, ids.get(index));
                }
                return delete.executeUpdate();
            }
        } catch (final SQLException ex) {
            throw storageError("expire publication leases by primary key failed", null, ex);
        }
    }

    /**
     * Only after the central metadata has been cleared, terminal state is earlier than safe point and the collection account book has been kept.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param expiredBeforeMillis expired before millis
     * @param limit maximum number of items
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public int purgeTerminal(final long safePoint, final long expiredBeforeMillis, final int limit) {
        checkGcLimit(limit);
        if (safePoint > currentSafePoint()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication purge cannot exceed durable safe point", false, null);
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_publication_entry_v1
                        where commit_ts < ? and status in ('COMMITTED', 'ABORTED')
                          and expires_at < ?
                          and not exists (select 1 from mvcc_txn
                                          where mvcc_txn.tx_id = mvcc_publication_entry_v1.tx_id)
                        order by commit_ts limit ?
                        """)) {
            delete.setLong(1, safePoint);
            delete.setLong(2, expiredBeforeMillis);
            delete.setInt(3, limit);
            return delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("purge terminal publication entries failed", null, ex);
        }
    }

    /**
     * Limit the single load of the control sheet per round GC.
     *
     * @param limit maximum number of items
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void checkGcLimit(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("publication GC limit must be between 1 and 4096");
        }
    }

    /**
     * Harmonizes to convert database anomalies to non-usable storage and keeps the transaction context.
     *
     * @param message message
     * @param txId transaction identifier
     * @param ex ex
     * @return the operation result
     */
    private MvccException storageError(final String message, final Long txId, final SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }

    /**
     * A consistent reading captures the specified decision and current release watermark.
     *
     * @param publishedTs published ts
     * @param commitTs durable commit timestamp
     * @param status status
     */
    private record PublicationObservation(long publishedTs, long commitTs, PublicationEntryStatus status) {
    }

    /**
     * Keeps the call order and pre-codes participant to avoid formatting strings when holding a time line lock.
     *
     * @param index index
     * @param request request
     * @param participants participants
     */
    private record IndexedIntentCommit(int index, PublicationIntentCommit request, String participants) {
    }

    /**
     * Save list shard pre-altered call order, only immutable requests of validate are processed when locking.
     *
     * @param index index
     * @param request request
     */
    private record IndexedLocalCommit(int index, PublicationLocalCommitRequest request) {
    }

    /**
     * Keep local commit proof call order and secure the account book lock by commit time.
     *
     * @param index index
     * @param proof proof
     */
    private record IndexedLocalProof(int index, PublicationLocalCommitProof proof) {
    }

    /**
     * Saves the start snapshot and cut-off of a lockless lease batch reading.
     *
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     */
    private record SnapshotLease(long readTs, long expiresAtMillis) {
    }

    /**
     * Saves a lock reading result for batch commit to press all centre metadata for transaction identifier verify.
     *
     * @param startTs transaction start timestamp
     * @param status status
     * @param commitTs durable commit timestamp
     * @param participants participants
     */
    private record BatchIntentMetadata(long startTs, String status, Long commitTs, String participants) {
    }

    /**
     * Reporting batch Control transaction Stages Accumulated time-consuming costs for segregating bits.
     *
     * @param calls calls
     * @param validationNanos validation nanos
     * @param stateWaitNanos state wait nanos
     * @param writeNanos write nanos
     * @param finishNanos finish nanos
     */
    public record BatchTiming(long calls, long validationNanos, long stateWaitNanos,
            long writeNanos, long finishNanos) {
    }
}

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

package com.flyingzc.azir.metadata.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.control.MysqlConnectionContext;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.FastTransactionBegin;
import com.flyingzc.azir.metadata.FastTransactionCommit;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampAuthority;

import javax.sql.DataSource;
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
import java.util.function.Supplier;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateCommitTs;
import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipant;
import static com.flyingzc.azir.metadata.TransactionMetadataValidation.validateParticipants;

/**
 * Persists the default coordinator's transaction decisions and publication metadata in MySQL.
 */
public final class MysqlTransactionMetadataStore
        implements
            TransactionMetadataStore,
            FastTransactionBegin,
            FastTransactionCommit {

    private final DataSource dataSource;

    private final int publicationLockTimeoutSeconds;

    private volatile boolean beginProcedureReady;

    private volatile boolean beginProcedureV2Ready;

    private volatile boolean commitProcedureReady;

    private volatile boolean commitProcedureV2Ready;

    private volatile boolean proceduresChecked;

    /**
     * Construct a MysqlTransactionMetadataStore instance.
     *
     * @param dataSource database connection source
     */
    public MysqlTransactionMetadataStore(final DataSource dataSource) {
        this(dataSource, 10);
    }

    /**
     * Configure the release lock waiting seconds; master not to lose confirmed control script is a prerequisite for deployment and this category does not meet HA.
     *
     * @param dataSource database connection source
     * @param publicationLockTimeoutSeconds publication lock timeout seconds
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MysqlTransactionMetadataStore(final DataSource dataSource, final int publicationLockTimeoutSeconds) {
        if (publicationLockTimeoutSeconds < 0) {
            throw new IllegalArgumentException("publicationLockTimeoutSeconds must be >= 0");
        }
        this.dataSource = dataSource;
        this.publicationLockTimeoutSeconds = publicationLockTimeoutSeconds;
    }

    /**
     * Transaction authority can only be shared if the old and new control component is identified as an example of the same data source.
     *
     * @param candidate candidate
     * @return the operation result
     */
    public boolean usesDataSource(final DataSource candidate) {
        return dataSource == candidate;
    }

    /**
     * Confirms that the control library is not entered in book or segment mode when the old transaction manager starts.
     *
     * @throws MvccException if the operation cannot complete
     */
    public void assertLegacyProtocolActive() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement tables = connection.prepareStatement("""
                        select table_name from information_schema.tables
                        where table_schema = database()
                          and table_name in ('mvcc_protocol_epoch_v1', 'mvcc_legacy_lease_gate_v1')
                        """)) {
            boolean epochExists = false;
            boolean gateExists = false;
            try (ResultSet rows = tables.executeQuery()) {
                while (rows.next()) {
                    if ("mvcc_protocol_epoch_v1".equalsIgnoreCase(rows.getString(1))) {
                        epochExists = true;
                    } else if ("mvcc_legacy_lease_gate_v1".equalsIgnoreCase(rows.getString(1))) {
                        gateExists = true;
                    }
                }
            }
            if (!epochExists && !gateExists) {
                return;
            }
            if (!epochExists || !gateExists) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "legacy publication protocol has incomplete durable fencing", false, null);
            }
            try (PreparedStatement mode = connection.prepareStatement("""
                    select protocol_mode from mvcc_protocol_epoch_v1 where singleton_id = 1
                    """); ResultSet row = mode.executeQuery()) {
                if (!row.next() || !"LEGACY".equals(row.getString(1)) || row.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "legacy transaction manager cannot use activated publication mode", false, null);
                }
            }
            try (PreparedStatement gate = connection.prepareStatement("""
                    select mode_id from mvcc_legacy_lease_gate_v1
                    """); ResultSet row = gate.executeQuery()) {
                if (!row.next() || row.getInt(1) != 0 || row.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "legacy snapshot lease gate is not active", false, null);
                }
            }
        } catch (final SQLException ex) {
            throw storageError(null, "inspect legacy publication mode failed", ex);
        }
    }

    /**
     * Press database -string to release, embezzled to share locked connection.
     *
     * @param <T> value type
     * @param action action
     * @return the operation result
     */
    @Override
    public <T> T withPublicationLock(final Supplier<T> action) {
        return MysqlConnectionContext.withPublicationLock(dataSource, publicationLockTimeoutSeconds, action);
    }

    /**
     * The lease and recovery determinations of the different coordination nodes are aligned with the database millisecond wall clock.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    @Override
    public long currentTimeMillis() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement(
                        "select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)");
                ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("database clock returned no row");
            }
            return result.getLong(1);
        } catch (final SQLException ex) {
            throw storageError(null, "load database clock failed", ex);
        }
    }

    /**
     * The durable storage position before the remote list shard commit still prevents new snapshot s from being sent to other nodes when GET_LOCK is lost.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    @Override
    public void markLocalCommitPending(final long txId, final long commitTs) {
        withPublicationLock(() -> {
            TxRecord record = get(txId).orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId));
            validateCommitTs(txId, record.startTs(), commitTs);
            if (record.participants().size() != 1 || record.status() == TxStatus.ABORTED
                    || record.status() == TxStatus.COMMITTED
                    || record.commitTs() != null && record.commitTs() != commitTs) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "invalid local publication state", false, txId);
            }
            return executeUpdate("""
                    update mvcc_txn set status = 'PREPARING', commit_ts = ?
                    where tx_id = ? and status in ('ACTIVE', 'PREPARING')
                    """, commitTs, txId);
        });
    }

    /**
     * The release of an unknown local commit slot is refused when it exists, and the unmarkable retry does not allow business to be automatically restarted.
     *
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void assertNoPendingPublications() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement(
                        "select tx_id from mvcc_txn where status = 'PREPARING' and commit_ts is not null limit 1");
                ResultSet result = statement.executeQuery()) {
            if (result.next()) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local publication requires recovery", false, result.getLong(1));
            }
        } catch (final SQLException ex) {
            throw storageError(null, "inspect pending publications failed", ex);
        }
    }

    /**
     * Initialize the required database tables or runtime environment.
     */
    public void initialize() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        create table if not exists mvcc_txn (
                          tx_id bigint not null primary key,
                          start_ts bigint not null,
                          isolation_level varchar(32) not null,
                          status varchar(32) not null,
                          commit_ts bigint null,
                          participants text not null,
                          index mvcc_txn_status_commit (status, commit_ts, tx_id),
                          index mvcc_txn_status_start (status, start_ts, tx_id)
                        ) engine=InnoDB
                        """)) {
            statement.execute();
            try (
                    PreparedStatement snapshots = connection.prepareStatement("""
                            create table if not exists mvcc_snapshot_lease (
                              tx_id bigint not null primary key,
                              read_ts bigint not null,
                              expires_at bigint not null,
                              index mvcc_snapshot_expiry (expires_at, tx_id),
                              index mvcc_snapshot_read (read_ts)
                            ) engine=InnoDB
                            """);
                    PreparedStatement finalized = connection.prepareStatement("""
                            create table if not exists mvcc_txn_finalized (
                              tx_id bigint not null primary key,
                              finalized_at bigint not null,
                              index mvcc_finalized_time (finalized_at, tx_id),
                              constraint mvcc_finalized_tx_fk foreign key (tx_id) references mvcc_txn(tx_id) on delete cascade
                            ) engine=InnoDB
                            """)) {
                snapshots.execute();
                finalized.execute();
            }
            ensureMetadataIndexes(connection);
            ensureBeginProcedure(connection);
            ensureBeginProcedureV2(connection);
            ensureCommitProcedure(connection);
            ensureCommitProcedureV2(connection);
            beginProcedureReady = true;
            beginProcedureV2Ready = true;
            commitProcedureReady = true;
            commitProcedureV2Ready = true;
            proceduresChecked = true;
        } catch (final SQLException ex) {
            throw storageError(null, "initialize metadata schema failed", ex);
        }
    }

    /**
     * The newly created control side example after restart only detects the existing routine once and does not need to repeat the initialization of the implementation mode.
     */
    private synchronized void detectInstalledProcedures() {
        if (proceduresChecked) {
            return;
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select routine_name from information_schema.routines
                        where routine_schema = database()
                          and routine_name in ('mvcc_begin_v1', 'mvcc_begin_v2',
                                               'mvcc_commit_v1', 'mvcc_commit_v2')
                        """);
                ResultSet result = query.executeQuery()) {
            while (result.next()) {
                if ("mvcc_begin_v1".equalsIgnoreCase(result.getString(1))) {
                    beginProcedureReady = true;
                } else if ("mvcc_begin_v2".equalsIgnoreCase(result.getString(1))) {
                    beginProcedureV2Ready = true;
                } else if ("mvcc_commit_v1".equalsIgnoreCase(result.getString(1))) {
                    commitProcedureReady = true;
                } else if ("mvcc_commit_v2".equalsIgnoreCase(result.getString(1))) {
                    commitProcedureV2Ready = true;
                }
            }
            proceduresChecked = true;
        } catch (final SQLException ex) {
            throw storageError(null, "inspect control procedures failed", ex);
        }
    }

    /**
     * Install MySQL transaction to start the routine, and accept the created result when a stand-alone instance concurrent is initiated.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureBeginProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_begin_v1' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_begin_v1(in p_isolation varchar(32), in p_timeout bigint,
                                                in p_persist_metadata boolean)
                begin
                  declare v_pending bigint default null;
                  declare v_ts bigint;
                  declare v_deadline bigint;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  select tx_id into v_pending from mvcc_txn
                    where status = 'PREPARING' and commit_ts is not null limit 1;
                  if v_pending is not null then
                    signal sqlstate '45000' set mysql_errno = 30001,
                      message_text = 'MVCC_PENDING_PUBLICATION';
                  end if;
                  start transaction;
                  update mvcc_tso set current_value = last_insert_id(current_value + 1)
                    where sequence_name = 'global' and current_value < 9223372036854775807;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30002,
                      message_text = 'MVCC_TSO_EXHAUSTED';
                  end if;
                  set v_ts = last_insert_id();
                  set v_deadline = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + p_timeout;
                  insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at)
                    values (v_ts, v_ts, v_deadline);
                  if p_persist_metadata then
                    insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                      values (v_ts, v_ts, p_isolation, 'ACTIVE', null, '');
                  end if;
                  commit;
                  select v_ts as tx_id, v_deadline as expires_at;
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
     * A new version of begin takes the same name release lock at the end of the server to avoid the client's article-by-article locking and ownership detection.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureBeginProcedureV2(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_begin_v2' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_begin_v2(in p_isolation varchar(32), in p_timeout bigint,
                                                in p_persist_metadata boolean, in p_lock_timeout int)
                begin
                  declare v_pending bigint default null;
                  declare v_ts bigint;
                  declare v_deadline bigint;
                  declare v_lock_name varchar(64);
                  declare v_got_lock int default 0;
                  declare v_released int;
                  declare exit handler for sqlexception begin
                    rollback;
                    if v_got_lock = 1 then do release_lock(v_lock_name); end if;
                    resignal;
                  end;
                  set v_lock_name = concat('mvcc:pub:', substring(sha2(database(), 256), 1, 48));
                  select get_lock(v_lock_name, p_lock_timeout) into v_got_lock;
                  if v_got_lock is null or v_got_lock <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30006,
                      message_text = 'MVCC_PUBLICATION_LOCK_TIMEOUT';
                  end if;
                  select tx_id into v_pending from mvcc_txn
                    where status = 'PREPARING' and commit_ts is not null limit 1;
                  if v_pending is not null then
                    signal sqlstate '45000' set mysql_errno = 30001,
                      message_text = 'MVCC_PENDING_PUBLICATION';
                  end if;
                  start transaction;
                  update mvcc_tso set current_value = last_insert_id(current_value + 1)
                    where sequence_name = 'global' and current_value < 9223372036854775807;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30002,
                      message_text = 'MVCC_TSO_EXHAUSTED';
                  end if;
                  set v_ts = last_insert_id();
                  set v_deadline = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + p_timeout;
                  insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at)
                    values (v_ts, v_ts, v_deadline);
                  if p_persist_metadata then
                    insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                      values (v_ts, v_ts, p_isolation, 'ACTIVE', null, '');
                  end if;
                  commit;
                  select release_lock(v_lock_name) into v_released;
                  if v_released is null or v_released <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30007,
                      message_text = 'MVCC_PUBLICATION_LOCK_RELEASE_FAILED';
                  end if;
                  set v_got_lock = 0;
                  select v_ts as tx_id, v_deadline as expires_at;
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
     * Install commit decision routines; old version can be added to the numbering changes for rolling upgrade.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureCommitProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_commit_v1' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_commit_v1(in p_tx_id bigint, in p_participants text)
                begin
                  declare v_start bigint;
                  declare v_status varchar(32) default null;
                  declare v_existing bigint default null;
                  declare v_participants text;
                  declare v_ts bigint;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  update mvcc_tso set current_value = last_insert_id(current_value + 1)
                    where sequence_name = 'global' and current_value < 9223372036854775807;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30002,
                      message_text = 'MVCC_TSO_EXHAUSTED';
                  end if;
                  set v_ts = last_insert_id();
                  select start_ts, status, commit_ts, participants
                    into v_start, v_status, v_existing, v_participants
                    from mvcc_txn where tx_id = p_tx_id for update;
                  if v_status is null then
                    signal sqlstate '45000' set mysql_errno = 30003,
                      message_text = 'MVCC_TX_MISSING';
                  end if;
                  if v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 30004,
                      message_text = 'MVCC_TX_ABORTED';
                  end if;
                  if v_status = 'COMMITTED' then
                    if v_existing is null then
                      signal sqlstate '45000' set mysql_errno = 30005,
                        message_text = 'MVCC_COMMIT_MISMATCH';
                    end if;
                    commit;
                    select v_existing as commit_ts;
                  else
                    if v_status not in ('ACTIVE', 'PREPARING') or v_existing is not null
                        or binary v_participants <> binary p_participants
                        or v_ts <= v_start then
                      signal sqlstate '45000' set mysql_errno = 30005,
                        message_text = 'MVCC_COMMIT_MISMATCH';
                    end if;
                    update mvcc_txn set status = 'COMMITTED', commit_ts = v_ts
                      where tx_id = p_tx_id;
                    commit;
                    select v_ts as commit_ts;
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
     * A new version of commit completes the name lock, timestamp, decision and lock release at the server end.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureCommitProcedureV2(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'mvcc_commit_v2' limit 1
                """)) {
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_commit_v2(in p_tx_id bigint, in p_participants text,
                                                 in p_lock_timeout int)
                begin
                  declare v_start bigint;
                  declare v_status varchar(32) default null;
                  declare v_existing bigint default null;
                  declare v_participants text;
                  declare v_ts bigint;
                  declare v_result bigint;
                  declare v_lock_name varchar(64);
                  declare v_got_lock int default 0;
                  declare v_released int;
                  declare exit handler for sqlexception begin
                    rollback;
                    if v_got_lock = 1 then do release_lock(v_lock_name); end if;
                    resignal;
                  end;
                  set v_lock_name = concat('mvcc:pub:', substring(sha2(database(), 256), 1, 48));
                  select get_lock(v_lock_name, p_lock_timeout) into v_got_lock;
                  if v_got_lock is null or v_got_lock <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30006,
                      message_text = 'MVCC_PUBLICATION_LOCK_TIMEOUT';
                  end if;
                  start transaction;
                  update mvcc_tso set current_value = last_insert_id(current_value + 1)
                    where sequence_name = 'global' and current_value < 9223372036854775807;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30002,
                      message_text = 'MVCC_TSO_EXHAUSTED';
                  end if;
                  set v_ts = last_insert_id();
                  select start_ts, status, commit_ts, participants
                    into v_start, v_status, v_existing, v_participants
                    from mvcc_txn where tx_id = p_tx_id for update;
                  if v_status is null then
                    signal sqlstate '45000' set mysql_errno = 30003,
                      message_text = 'MVCC_TX_MISSING';
                  end if;
                  if v_status = 'ABORTED' then
                    signal sqlstate '45000' set mysql_errno = 30004,
                      message_text = 'MVCC_TX_ABORTED';
                  end if;
                  if v_status = 'COMMITTED' then
                    if v_existing is null then
                      signal sqlstate '45000' set mysql_errno = 30005,
                        message_text = 'MVCC_COMMIT_MISMATCH';
                    end if;
                    set v_result = v_existing;
                  else
                    if v_status not in ('ACTIVE', 'PREPARING') or v_existing is not null
                        or binary v_participants <> binary p_participants
                        or v_ts <= v_start then
                      signal sqlstate '45000' set mysql_errno = 30005,
                        message_text = 'MVCC_COMMIT_MISMATCH';
                    end if;
                    update mvcc_txn set status = 'COMMITTED', commit_ts = v_ts
                      where tx_id = p_tx_id;
                    set v_result = v_ts;
                  end if;
                  commit;
                  select release_lock(v_lock_name) into v_released;
                  if v_released is null or v_released <> 1 then
                    signal sqlstate '45000' set mysql_errno = 30007,
                      message_text = 'MVCC_PUBLICATION_LOCK_RELEASE_FAILED';
                  end if;
                  set v_got_lock = 0;
                  select v_result as commit_ts;
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
     * Use server atomic only when TSO shares the same data source with metadata and the routine is initiated.
     *
     * @param timestampOracle timestamp oracle
     * @param isolationLevel global transaction isolation level
     * @param timeoutMillis timeout millis
     * @param persistMetadata persist metadata
     * @param beforePublish before publish
     * @return the operation result
     */
    @Override
    public Optional<BeginReservation> tryBegin(final TimestampOracle timestampOracle, final IsolationLevel isolationLevel,
                                               final long timeoutMillis, final boolean persistMetadata, final Runnable beforePublish) {
        if (!(timestampOracle instanceof MysqlTimestampAuthority authority)
                || !authority.usesDataSource(dataSource)) {
            return Optional.empty();
        }
        detectInstalledProcedures();
        if (!beginProcedureReady) {
            return Optional.empty();
        }
        if (beginProcedureV2Ready && !MysqlConnectionContext.hasPublicationScope(dataSource)) {
            try {
                return Optional.of(MysqlConnectionContext.withPublicationProcedure(dataSource,
                        publicationLockTimeoutSeconds, connection -> {
                            beforePublish.run();
                            return callBeginProcedure(connection, isolationLevel, timeoutMillis,
                                    persistMetadata, true);
                        }));
            } catch (final SQLException ex) {
                throw storageError(null, "server-side publication connection failed", ex);
            }
        }
        return Optional.of(withPublicationLock(() -> {
            beforePublish.run();
            try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
                return callBeginProcedure(connection, isolationLevel, timeoutMillis, persistMetadata, false);
            } catch (final SQLException ex) {
                throw storageError(null, "server-side transaction begin failed", ex);
            }
        }));
    }

    /**
     * Run a control operation on the locked connection, only lease and optional metadata commit return transaction identifier.
     *
     * @param connection connection
     * @param isolationLevel global transaction isolation level
     * @param timeoutMillis timeout millis
     * @param persistMetadata persist metadata
     * @param serverLock server lock
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private BeginReservation callBeginProcedure(final Connection connection, final IsolationLevel isolationLevel,
                                                final long timeoutMillis, final boolean persistMetadata, final boolean serverLock) {
        String sql = serverLock ? "call mvcc_begin_v2(?, ?, ?, ?)" : "call mvcc_begin_v1(?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, isolationLevel.name());
            statement.setLong(2, timeoutMillis);
            statement.setBoolean(3, persistMetadata);
            if (serverLock) {
                statement.setInt(4, publicationLockTimeoutSeconds);
            }
            boolean hasResult = statement.execute();
            while (!hasResult && statement.getUpdateCount() != -1) {
                hasResult = statement.getMoreResults();
            }
            if (!hasResult) {
                throw new SQLException("begin procedure returned no reservation");
            }
            try (ResultSet result = statement.getResultSet()) {
                if (!result.next()) {
                    throw new SQLException("begin procedure returned no transaction");
                }
                long txId = result.getLong("tx_id");
                return new BeginReservation(txId, txId, result.getLong("expires_at"), persistMetadata);
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 30001) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local publication requires recovery", false, null, ex);
            }
            if (ex.getErrorCode() == 30002) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "timestamp sequence exhausted", false, null, ex);
            }
            if (ex.getErrorCode() == 30006) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                        "server-side publication lock timed out", true, null, ex);
            }
            throw storageError(null, "server-side transaction begin failed", ex);
        }
    }

    /**
     * Same authority cross-shard commit logs only decision via server-end transaction atomic.
     *
     * @param timestampOracle timestamp oracle
     * @param txId transaction identifier
     * @param participants participants
     * @param beforePublish before publish
     * @param afterCommitted after committed
     * @return the operation result
     */
    @Override
    public OptionalLong tryCommit(final TimestampOracle timestampOracle, final long txId, final Set<String> participants,
                                  final Runnable beforePublish, final LongConsumer afterCommitted) {
        if (!(timestampOracle instanceof MysqlTimestampAuthority authority)
                || !authority.usesDataSource(dataSource)) {
            return OptionalLong.empty();
        }
        detectInstalledProcedures();
        if (!commitProcedureReady) {
            return OptionalLong.empty();
        }
        validateParticipants(txId, participants);
        String encodedParticipants = encodeParticipants(txId, participants);
        if (commitProcedureV2Ready && !MysqlConnectionContext.hasPublicationScope(dataSource)) {
            try {
                return OptionalLong.of(MysqlConnectionContext.withPublicationProcedure(dataSource,
                        publicationLockTimeoutSeconds, connection -> {
                            beforePublish.run();
                            long timestamp = callCommitProcedure(connection, txId, encodedParticipants, true);
                            afterCommitted.accept(timestamp);
                            return timestamp;
                        }));
            } catch (final SQLException ex) {
                throw storageError(txId, "server-side commit connection failed", ex);
            }
        }
        long commitTs = withPublicationLock(() -> {
            beforePublish.run();
            try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
                long timestamp = callCommitProcedure(connection, txId, encodedParticipants, false);
                afterCommitted.accept(timestamp);
                return timestamp;
            } catch (final SQLException ex) {
                throw storageError(txId, "server-side commit connection failed", ex);
            }
        });
        return OptionalLong.of(commitTs);
    }

    /**
     * Read commit timestamp returned from storage. The caller must look for durable decision when the response is lost.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param participants participants
     * @param serverLock server lock
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private long callCommitProcedure(final Connection connection, final long txId, final String participants, final boolean serverLock) {
        String sql = serverLock ? "call mvcc_commit_v2(?, ?, ?)" : "call mvcc_commit_v1(?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, txId);
            statement.setString(2, participants);
            if (serverLock) {
                statement.setInt(3, publicationLockTimeoutSeconds);
            }
            boolean hasResult = statement.execute();
            while (!hasResult && statement.getUpdateCount() != -1) {
                hasResult = statement.getMoreResults();
            }
            if (!hasResult) {
                throw new SQLException("commit procedure returned no decision");
            }
            try (ResultSet result = statement.getResultSet()) {
                if (!result.next()) {
                    throw new SQLException("commit procedure returned no timestamp");
                }
                return result.getLong("commit_ts");
            }
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 30002 || ex.getErrorCode() == 30003 || ex.getErrorCode() == 30005) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "server-side commit rejected", false, txId, ex);
            }
            if (ex.getErrorCode() == 30004) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "server-side commit found aborted transaction", false, txId, ex);
            }
            if (ex.getErrorCode() == 30006) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                        "server-side publication lock timed out", true, txId, ex);
            }
            throw storageError(txId, "server-side commit decision failed", ex);
        }
    }

    /**
     * Supplements the commit placeholder and transaction lower bound index to an existing metadata table.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureMetadataIndexes(final Connection connection) throws SQLException {
        ensureMetadataIndex(connection, "mvcc_txn_status_commit",
                "create index mvcc_txn_status_commit on mvcc_txn(status, commit_ts, tx_id)");
        ensureMetadataIndex(connection, "mvcc_txn_status_start",
                "create index mvcc_txn_status_start on mvcc_txn(status, start_ts, tx_id)");
    }

    /**
     * Check by name and create a fixed defined index that allows the initialization of separate nodes concurrent.
     *
     * @param connection connection
     * @param indexName index name
     * @param createSql create sql
     * @throws SQLException if the operation cannot complete
     */
    private void ensureMetadataIndex(final Connection connection, final String indexName, final String createSql) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.statistics
                where table_schema = database() and table_name = 'mvcc_txn'
                  and index_name = ? limit 1
                """)) {
            query.setString(1, indexName);
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement create = connection.prepareStatement(createSql)) {
            create.execute();
        } catch (final SQLException ex) {
            if (ex.getErrorCode() != 1061) {
                throw ex;
            }
        }
    }

    /**
     * Create the component or transaction record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    @Override
    public TxRecord create(final long txId, final long startTs, final IsolationLevel isolationLevel) {
        return withPublicationLock(() -> insertRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null));
    }

    /**
     * Atomic saves the full participant in the first durable storage record, saves the same transaction read lock and double update.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    @Override
    public TxRecord createWithParticipants(final long txId, final long startTs, final IsolationLevel isolationLevel,
                                           final Set<String> participants) {
        validateParticipants(txId, participants);
        return insertRecord(txId, startTs, isolationLevel, participants, TxStatus.ACTIVE, null);
    }

    /**
     * Create a PREPARING transaction metadata record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    @Override
    public TxRecord createPreparing(final long txId, final long startTs, final IsolationLevel isolationLevel, final Set<String> participants) {
        validateParticipants(txId, participants);
        return withPublicationLock(() -> insertRecord(txId, startTs, isolationLevel, participants, TxStatus.PREPARING, null));
    }

    /**
     * Create a COMMITTED transaction metadata record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord createCommitted(final long txId, final long startTs, final IsolationLevel isolationLevel, final Set<String> participants, final long commitTs) {
        validateCommitTs(txId, startTs, commitTs);
        validateParticipants(txId, participants);
        return withPublicationLock(() -> {
            Optional<TxRecord> current = get(txId);
            if (current.isPresent()) {
                TxRecord existing = current.get();
                if (existing.status() == TxStatus.COMMITTED && existing.startTs() == startTs
                        && existing.isolationLevel() == isolationLevel && existing.participants().equals(participants)) {
                    return markCommitted(txId, commitTs);
                }
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already exists: " + txId, false, txId);
            }
            return insertRecord(txId, startTs, isolationLevel, participants, TxStatus.COMMITTED, commitTs);
        });
    }

    /**
     * Inserts an transaction metadata record.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param status status
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    private TxRecord insertRecord(final long txId,
                                  final long startTs,
                                  final IsolationLevel isolationLevel,
                                  final Set<String> participants,
                                  final TxStatus status,
                                  final Long commitTs) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, participants, status, commitTs);
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                        values (?, ?, ?, ?, ?, ?)
                        """)) {
            statement.setLong(1, txId);
            statement.setLong(2, startTs);
            statement.setString(3, isolationLevel.name());
            statement.setString(4, status.name());
            if (commitTs == null) {
                statement.setNull(5, Types.BIGINT);
            } else {
                statement.setLong(5, commitTs);
            }
            statement.setString(6, encodeParticipants(txId, record.participants()));
            statement.executeUpdate();
            return record;
        } catch (final SQLException ex) {
            throw storageError(txId, "create tx metadata failed", ex);
        }
    }

    /**
     * Read the currently visible data for the key or transaction.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public Optional<TxRecord> get(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        select tx_id, start_ts, isolation_level, status, commit_ts, participants
                        from mvcc_txn
                        where tx_id = ?
                        """)) {
            statement.setLong(1, txId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapRecord(resultSet));
            }
        } catch (final SQLException ex) {
            throw storageError(txId, "load tx metadata failed", ex);
        }
    }

    /**
     * Mark the transaction PREPARING.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     */
    @Override
    public TxRecord markPreparing(final long txId, final Set<String> participants) {
        validateParticipants(txId, participants);
        return withPublicationLock(() -> updateRecord(txId, participants, TxStatus.PREPARING, null, false));
    }

    /**
     * Mark the transaction COMMITTED.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord markCommitted(final long txId, final long commitTs) {
        return withPublicationLock(() -> updateRecord(txId, null, TxStatus.COMMITTED, commitTs, true));
    }

    /**
     * Mark the transaction COMMITTED.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord markCommitted(final long txId, final Set<String> participants, final long commitTs) {
        return withPublicationLock(() -> updateRecord(txId, participants, TxStatus.COMMITTED, commitTs, true));
    }

    /**
     * Persist PREPARING through the fast path.
     *
     * @param txId transaction identifier
     * @param participants participants
     */
    @Override
    public void persistPreparing(final long txId, final Set<String> participants) {
        validateParticipants(txId, participants);
        fastUpdateStatus(txId, participants, TxStatus.PREPARING, null, false);
    }

    /**
     * Persist the COMMITTED state through the fast path.
     *
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     */
    @Override
    public void persistCommitted(final long txId, final long commitTs) {
        fastUpdateStatus(txId, null, TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * Persist the COMMITTED state through the fast path.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     */
    @Override
    public void persistCommitted(final long txId, final Set<String> participants, final long commitTs) {
        fastUpdateStatus(txId, participants, TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * Mark the single-shard transaction committed.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    @Override
    public TxRecord markSingleShardCommitted(final long txId, final String participant, final long commitTs) {
        validateParticipant(txId, participant);
        return withPublicationLock(() -> updateRecord(txId, Set.of(participant), TxStatus.COMMITTED, commitTs, true));
    }

    /**
     * Centre decision after local participant commit is updated; replay and mismatches remain in the original state machine.
     *
     * @param txId transaction identifier
     * @param participant participant
     * @param commitTs durable commit timestamp
     */
    @Override
    public void persistSingleShardCommitted(final long txId, final String participant, final long commitTs) {
        validateParticipant(txId, participant);
        fastUpdateStatus(txId, Set.of(participant), TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * Mark the transaction ABORTED.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public TxRecord markAborted(final long txId) {
        return withPublicationLock(() -> updateRecord(txId, null, TxStatus.ABORTED, null, false));
    }

    /**
     * Query snapshot and uncompleted materialization transaction minimum level; already Finalized history decision does not block GC.
     *
     * @return the operation result
     */
    @Override
    public Optional<Long> oldestActiveStartTs() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        select min(start_ts) as oldest_start_ts
                        from (
                          select t.start_ts from mvcc_txn t
                          left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                          where f.tx_id is null
                          union all
                          select read_ts as start_ts from mvcc_snapshot_lease
                        ) active_snapshots
                        """);
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next() || resultSet.getObject("oldest_start_ts") == null) {
                return Optional.empty();
            }
            return Optional.of(resultSet.getLong("oldest_start_ts"));
        } catch (final SQLException ex) {
            throw storageError(null, "load oldest active start ts failed", ex);
        }
    }

    /**
     * Lock and update transaction metadata records in transaction.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param targetStatus target status
     * @param commitTs durable commit timestamp
     * @param setCommitTs set commit ts
     * @return the operation result
     */
    private TxRecord updateRecord(final long txId,
                                  final Set<String> participants,
                                  final TxStatus targetStatus,
                                  final Long commitTs,
                                  final boolean setCommitTs) {
        if (participants != null) {
            validateParticipants(txId, participants);
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                TxRecord current = lockRecord(connection, txId);
                if (targetStatus == TxStatus.COMMITTED) {
                    validateCommitTs(txId, current.startTs(), commitTs);
                }
                if (targetStatus == TxStatus.COMMITTED && current.commitTs() != null
                        && !Objects.equals(current.commitTs(), commitTs)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp mismatch: " + txId, false, txId);
                }
                if (current.status() == TxStatus.COMMITTED || current.status() == TxStatus.ABORTED) {
                    if (targetStatus == TxStatus.COMMITTED
                            && (current.status() == TxStatus.ABORTED || !Objects.equals(current.commitTs(), commitTs))) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit decision mismatch: " + txId, false, txId);
                    }
                    // Returns real terminal state, prevents COMMITTED and repeat commit replacement timestamp.
                    return current;
                }

                Set<String> nextParticipants = new LinkedHashSet<>(current.participants());
                if (current.commitTs() != null && participants != null && !nextParticipants.containsAll(participants)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot extend pending local publication", false, txId);
                }
                if (participants != null) {
                    nextParticipants.addAll(participants);
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_txn
                        set status = ?, commit_ts = ?, participants = ?
                        where tx_id = ?
                        """)) {
                    update.setString(1, targetStatus.name());
                    if (targetStatus == TxStatus.ABORTED) {
                        update.setNull(2, Types.BIGINT);
                    } else if (setCommitTs && commitTs != null) {
                        update.setLong(2, commitTs);
                    } else if (current.commitTs() != null) {
                        update.setLong(2, current.commitTs());
                    } else {
                        update.setNull(2, Types.BIGINT);
                    }
                    update.setString(3, encodeParticipants(txId, nextParticipants));
                    update.setLong(4, txId);
                    update.executeUpdate();
                }
                return new TxRecord(
                        current.txId(),
                        current.startTs(),
                        current.isolationLevel(),
                        nextParticipants,
                        targetStatus,
                        targetStatus == TxStatus.ABORTED ? null : setCommitTs ? commitTs : current.commitTs());
            });
        } catch (final SQLException ex) {
            throw storageError(txId, "update tx metadata failed", ex);
        }
    }

    /**
     * Keeps quick access compatibility and uses line locks to achieve terminal state validate equivalent to CAS.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param targetStatus target status
     * @param commitTs durable commit timestamp
     * @param setCommitTs set commit ts
     */
    private void fastUpdateStatus(final long txId,
                                  final Set<String> participants,
                                  final TxStatus targetStatus,
                                  final Long commitTs,
                                  final boolean setCommitTs) {
        withPublicationLock(() -> {
            if (targetStatus == TxStatus.COMMITTED && setCommitTs && commitTs != null
                    && tryPersistFreshCommit(txId, participants, commitTs)) {
                return null;
            }
            // Replay or a non-matched state is still determined by the original status machine as true terminal state and cannot be easily covered.
            updateRecord(txId, participants, targetStatus, commitTs, setCommitTs);
            return null;
        });
    }

    /**
     * Use a single atomic update for the first commit full match of participant and pending states.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    private boolean tryPersistFreshCommit(final long txId, final Set<String> participants, final long commitTs) {
        if (participants != null) {
            validateParticipants(txId, participants);
        }
        String sql = participants == null
                ? """
                        update mvcc_txn set status = 'COMMITTED', commit_ts = ?
                        where tx_id = ? and status in ('ACTIVE', 'PREPARING')
                          and (commit_ts is null or commit_ts = ?) and start_ts < ?
                        """
                : """
                        update mvcc_txn set status = 'COMMITTED', commit_ts = ?
                        where tx_id = ? and status in ('ACTIVE', 'PREPARING')
                          and (commit_ts is null or commit_ts = ?) and start_ts < ?
                          and binary participants = binary ?
                        """;
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, commitTs);
            statement.setLong(2, txId);
            statement.setLong(3, commitTs);
            statement.setLong(4, commitTs);
            if (participants != null) {
                statement.setString(5, encodeParticipants(txId, participants));
            }
            return statement.executeUpdate() == 1;
        } catch (final SQLException ex) {
            throw storageError(txId, "persist committed decision failed", ex);
        }
    }

    /**
     * Lock and read the specified transaction metadata record.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private TxRecord lockRecord(final Connection connection, final long txId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select tx_id, start_ts, isolation_level, status, commit_ts, participants
                from mvcc_txn
                where tx_id = ?
                for update
                """)) {
            statement.setLong(1, txId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId);
                }
                return mapRecord(resultSet);
            }
        }
    }

    /**
     * Maps the results to transaction metadata objects.
     *
     * @param resultSet result set
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static TxRecord mapRecord(final ResultSet resultSet) throws SQLException {
        long txId = resultSet.getLong("tx_id");
        Long commitTs = resultSet.getObject("commit_ts") == null ? null : resultSet.getLong("commit_ts");
        return new TxRecord(
                txId,
                resultSet.getLong("start_ts"),
                IsolationLevel.valueOf(resultSet.getString("isolation_level")),
                decodeParticipants(txId, resultSet.getString("participants")),
                TxStatus.valueOf(resultSet.getString("status")),
                commitTs);
    }

    /**
     * Encoding participation shard as a storage string.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     */
    private static String encodeParticipants(final long txId, final Set<String> participants) {
        validateParticipants(txId, participants);
        return participants.stream().sorted().collect(Collectors.joining(","));
    }

    /**
     * Compatible with old CSV and empty collections, rejecting blank segments and avoiding the silent abandonment of recovery route.
     *
     * @param txId transaction identifier
     * @param value value to write
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private static Set<String> decodeParticipants(final long txId, final String value) {
        if (value == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant encoding must not be null", false, txId);
        }
        if (value.isEmpty()) {
            return Set.of();
        }
        Set<String> participants = Arrays.stream(value.split(",", -1))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        validateParticipants(txId, participants);
        return participants;
    }

    /**
     * A fixed registration deadline of lease, and repeated calls allow only the lower protected snapshot boundary to be lowered.
     *
     * @param txId transaction identifier
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     */
    @Override
    public void registerSnapshot(final long txId, final long readTs, final long expiresAtMillis) {
        withPublicationLock(() -> {
            try (
                    Connection connection = MysqlConnectionContext.connection(dataSource);
                    PreparedStatement statement = connection.prepareStatement("""
                            insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at) values (?, ?, ?)
                            """)) {
                statement.setLong(1, txId);
                statement.setLong(2, readTs);
                statement.setLong(3, expiresAtMillis);
                statement.executeUpdate();
                return null;
            } catch (final SQLException ex) {
                if (ex.getErrorCode() == 1062) {
                    // Reads old lease only after repeating; the deadline cannot be extended and the lower bound of snapshot can only be lowered.
                    Optional<Long> previous = snapshotExpiry(txId);
                    if (previous.isPresent() && previous.get() == expiresAtMillis) {
                        try (
                                Connection connection = MysqlConnectionContext.connection(dataSource);
                                PreparedStatement update = connection.prepareStatement("""
                                        update mvcc_snapshot_lease set read_ts = least(read_ts, ?)
                                        where tx_id = ? and expires_at = ?
                                        """)) {
                            update.setLong(1, readTs);
                            update.setLong(2, txId);
                            update.setLong(3, expiresAtMillis);
                            update.executeUpdate();
                            return null;
                        } catch (final SQLException updateFailure) {
                            throw storageError(txId, "register snapshot failed", updateFailure);
                        }
                    }
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "snapshot lease cannot be renewed", false, txId);
                }
                throw storageError(txId, "register snapshot failed", ex);
            }
        });
    }

    /**
     * Returns the original cut-off, and expired records are also saved to be deleted when bounded is cleared.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    @Override
    public Optional<Long> snapshotExpiry(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement(
                        "select expires_at from mvcc_snapshot_lease where tx_id = ?")) {
            statement.setLong(1, txId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getLong(1)) : Optional.empty();
            }
        } catch (final SQLException ex) {
            throw storageError(txId, "load snapshot expiry failed", ex);
        }
    }

    /**
     * Checks the fixed cut-off time with the authority clock in the same database query to avoid double borrowing of the connection and rounding on the heat path.
     *
     * @param txId transaction identifier
     * @param expectedExpiresAtMillis expected expires at millis
     * @return the operation result
     */
    @Override
    public boolean isSnapshotLeaseValid(final long txId, final long expectedExpiresAtMillis) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        select 1 from mvcc_snapshot_lease
                        where tx_id = ? and expires_at = ?
                          and expires_at > cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """)) {
            statement.setLong(1, txId);
            statement.setLong(2, expectedExpiresAtMillis);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (final SQLException ex) {
            // The search failure must keep the storage anomaly and cannot be validated by an unconfirmable lease error.
            throw storageError(txId, "validate snapshot lease failed", ex);
        }
    }

    /**
     * Release idempotently exists independently of metadata snapshot lease.
     *
     * @param txId transaction identifier
     */
    @Override
    public void releaseSnapshot(final long txId) {
        // Delete only the protection of closed transaction; and calculate concurrent with safe point to more in order to keep the current round GC behind.
        executeUpdate("delete from mvcc_snapshot_lease where tx_id = ?", txId);
    }

    /**
     * Merge participant before prewrite, maintain ACTIVE or PREPARING and do not overwrite terminal state.
     *
     * @param txId transaction identifier
     * @param participants participants
     * @return the operation result
     */
    @Override
    public TxRecord registerParticipants(final long txId, final Set<String> participants) {
        validateParticipants(txId, participants);
        // Only modify the current transaction row; the latest participant or real terminal state returns when the line locks are merged with recovery concurrent.
        TxRecord record = get(txId).orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId));
        return updateRecord(txId, participants, record.status(), record.commitTs(), false);
    }

    /**
     * Ordered batch found no materialization terminal state and no valid lease pending transaction.
     *
     * @param nowMillis now millis
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public List<TxRecord> findRecoveryCandidates(final long nowMillis, final long afterTxId, final int limit) {
        if (limit <= 0) {
            return List.of();
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        select t.tx_id, t.start_ts, t.isolation_level, t.status, t.commit_ts, t.participants
                        from mvcc_txn t
                        left join mvcc_snapshot_lease s on s.tx_id = t.tx_id
                        left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                        where t.tx_id > ? and f.tx_id is null
                          and (t.status in ('COMMITTED', 'ABORTED')
                               or (t.status in ('ACTIVE', 'PREPARING') and (s.tx_id is null or s.expires_at <= ?)))
                        order by t.tx_id limit ?
                        """)) {
            statement.setLong(1, afterTxId);
            statement.setLong(2, nowMillis);
            statement.setInt(3, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<TxRecord> records = new ArrayList<>();
                while (result.next()) {
                    records.add(mapRecord(result));
                }
                return List.copyOf(records);
            }
        } catch (final SQLException ex) {
            throw storageError(null, "find recovery candidates failed", ex);
        }
    }

    /**
     * Only the time when terminal state first completes materialization is recorded; records that have been removed from GC do not need to be revived.
     *
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void markFinalized(final long txId) {
        // Materialization is written only when it has been completed; GC and this statement concurrent can only see old or new terminal state.
        int inserted = executeUpdate("""
                insert into mvcc_txn_finalized(tx_id, finalized_at)
                select tx_id, cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                from mvcc_txn where tx_id = ? and status in ('COMMITTED', 'ABORTED')
                on duplicate key update tx_id = values(tx_id)
                """, txId);
        if (inserted == 0) {
            Optional<TxRecord> record = get(txId);
            if (record.isPresent() && record.get().status() != TxStatus.COMMITTED
                    && record.get().status() != TxStatus.ABORTED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot finalize undecided tx", false, txId);
            }
        }
    }

    /**
     * Bounded clears completed transaction that is strictly earlier than safe point and during retention time, with the external key removing the materialization sign.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param finalizedBeforeMillis finalized before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public int purgeFinalized(final long safePoint, final long finalizedBeforeMillis, final int limit) {
        if (limit <= 0) {
            return 0;
        }
        return withPublicationLock(() -> {
            List<Long> ids = new ArrayList<>();
            try (
                    Connection connection = MysqlConnectionContext.connection(dataSource);
                    PreparedStatement statement = connection.prepareStatement("""
                            select t.tx_id from mvcc_txn t
                            join mvcc_txn_finalized f on f.tx_id = t.tx_id
                            left join mvcc_snapshot_lease s on s.tx_id = t.tx_id
                            where s.tx_id is null and f.finalized_at < ? and t.start_ts < ?
                              and (t.status = 'ABORTED' or (t.status = 'COMMITTED' and t.commit_ts < ?))
                            order by t.tx_id limit ?
                            """)) {
                statement.setLong(1, finalizedBeforeMillis);
                statement.setLong(2, safePoint);
                statement.setLong(3, safePoint);
                statement.setInt(4, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        ids.add(result.getLong(1));
                    }
                }
                int deleted = 0;
                for (long txId : ids) {
                    deleted += executeUpdate("delete from mvcc_txn where tx_id = ?", txId);
                }
                return deleted;
            } catch (final SQLException ex) {
                throw storageError(null, "purge finalized metadata failed", ex);
            }
        });
    }

    /**
     * Batch removal of expired lease, do not change pending transaction, and the remaining lease continues to hold back on safe point.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public int expireSnapshots(final long nowMillis, final int limit) {
        if (limit <= 0) {
            return 0;
        }
        return withPublicationLock(() -> executeUpdate(
                "delete from mvcc_snapshot_lease where expires_at <= ? order by expires_at, tx_id limit ?", nowMillis, limit));
    }

    /**
     * Implements a control face update in the current connection context that contains only integer binding parameters.
     *
     * @param sql sql
     * @param parameters parameters
     * @return the operation result
     */
    private int executeUpdate(final String sql, final long... parameters) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                statement.setLong(i + 1, parameters[i]);
            }
            return statement.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError(null, "control metadata update failed", ex);
        }
    }

    /**
     * Map the storage failure to an MVCC exception.
     *
     * @param txId transaction identifier
     * @param message message
     * @param ex ex
     * @return the operation result
     */
    private MvccException storageError(final Long txId, final String message, final SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}

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
import com.flyingzc.azir.control.MysqlConnectionContext;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.TransactionMetadataValidation;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.mysql.MysqlLocalFirstMetadataView;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.publication.CoordinatorSnapshotLease;
import com.flyingzc.azir.publication.CoordinatorSnapshotView;
import com.flyingzc.azir.storage.mysql.MysqlShardSegmentFence;
import com.flyingzc.azir.storage.mysql.MysqlShardSegmentFence.CandidateProof;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter.PrewriteProof;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter.LocalCommitProof;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter.IntentTerminalProof;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Local First Isolation Control Book of the commit protocol, leave an exclusive shard section and protect global snapshot in the lower bounds.
 */
public final class MysqlLocalFirstPublicationLedger {

    /**
     * Cross-shard intent decision Use non-physical shard segment in the same global time line.
     */
    public static final String INTENT_SEGMENT_ROUTE = "__INTENT__";

    private final DataSource dataSource;

    private volatile boolean fastPublicationReady;

    /**
     * Use the control bank version table, which is isolated from the shard candidate list, without changing the time line of the existing product.
     *
     * @param dataSource database connection source
     */
    public MysqlLocalFirstPublicationLedger(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Creates only version tables for new protocols and maintains an initialised timeline.
     *
     * @param baseCommitTs base commit ts
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void initializeIsolated(final long baseCommitTs) {
        if (baseCommitTs < 0) {
            throw new IllegalArgumentException("local-first base timestamp must be nonnegative");
        }
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_local_first_state_v1 (
                      singleton_id tinyint not null primary key,
                      base_commit_ts bigint not null,
                      next_commit_ts bigint not null,
                      published_ts bigint not null,
                      safe_point bigint not null,
                      schema_version smallint not null,
                      constraint mvcc_local_first_state_check check (
                        base_commit_ts >= 0 and next_commit_ts >= published_ts
                        and published_ts >= safe_point and schema_version = 4)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_local_first_snapshot_v1 (
                      tx_id bigint not null auto_increment primary key,
                      read_ts bigint not null,
                      expires_at bigint not null,
                      index mvcc_local_first_snapshot_expiry (expires_at, read_ts),
                      constraint mvcc_local_first_snapshot_check check (
                        read_ts >= 0 and expires_at > 0)
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_local_first_coordinator_v1 (
                      coordinator_id bigint not null auto_increment primary key,
                      read_ts bigint not null,
                      expires_at bigint not null,
                      generation bigint not null,
                      status varchar(16) not null,
                      index mvcc_local_first_coordinator_expiry (status, expires_at, read_ts),
                      constraint mvcc_local_first_coordinator_check check (
                        read_ts >= 0 and expires_at > 0 and generation > 0
                        and status in ('ACTIVE', 'FENCED'))
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_local_first_segment_v1 (
                      segment_start bigint not null primary key,
                      segment_end bigint not null,
                      shard_id varchar(128) collate utf8mb4_0900_bin not null,
                      owner_id varchar(128) collate utf8mb4_0900_bin not null,
                      reservation_key varchar(128) collate utf8mb4_0900_bin not null unique,
                      generation bigint not null,
                      snapshot_floor bigint not null,
                      lease_millis bigint not null,
                      expires_at bigint not null,
                      status varchar(16) not null,
                      physical_fence_gc_done tinyint not null default 0,
                      created_at bigint not null,
                      index mvcc_local_first_expiry (status, expires_at, segment_start),
                      index mvcc_local_first_floor (status, snapshot_floor),
                      constraint mvcc_local_first_segment_check check (
                        segment_end >= segment_start and generation > 0
                        and snapshot_floor >= 0 and lease_millis > 0 and expires_at > 0
                        and status in ('PREPARING', 'ACTIVE', 'FENCING', 'SEALED')
                        and physical_fence_gc_done in (0, 1))
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists mvcc_local_first_txn_v1 (
                      tx_id bigint not null primary key,
                      start_ts bigint not null,
                      commit_ts bigint null unique,
                      shard_id varchar(128) collate utf8mb4_0900_bin not null,
                      participants text not null,
                      isolation_level varchar(20) not null,
                      deadline_millis bigint not null,
                      status varchar(16) not null,
                      protocol_mode varchar(16) not null default 'SCALAR',
                      finalized_at bigint not null default 0,
                      candidate_gc_done tinyint not null default 0,
                      index mvcc_local_first_txn_gc_floor (status, candidate_gc_done, start_ts),
                      index mvcc_local_first_intent_decision (shard_id, status, candidate_gc_done, tx_id),
                      index mvcc_local_first_intent_expiry (shard_id, status, deadline_millis, tx_id),
                      constraint mvcc_local_first_txn_check check (
                        tx_id <> 0 and start_ts >= 0 and deadline_millis > 0
                        and ((status = 'COMMITTED' and commit_ts > start_ts)
                             or (status in ('ACTIVE', 'PREPARING', 'ABORTED')
                                 and commit_ts is null))
                        and candidate_gc_done in (0, 1)
                        and protocol_mode in ('SCALAR', 'VECTOR')
                        and isolation_level in ('READ_COMMITTED', 'REPEATABLE_READ'))
                    ) engine=InnoDB
                    """);
            ensureIntentProtocolColumn(connection);
            statement.execute("""
                    create table if not exists mvcc_local_first_slot_v1 (
                      commit_ts bigint not null primary key,
                      segment_start bigint not null,
                      tx_id bigint null unique,
                      status varchar(16) not null,
                      start_ts bigint null,
                      deadline_millis bigint null,
                      isolation_level varchar(20) null,
                      index mvcc_local_first_slot_range (segment_start, commit_ts),
                      constraint mvcc_local_first_slot_check check (
                        status in ('PENDING', 'COMMITTED', 'ABORTED')
                        and (status <> 'COMMITTED' or tx_id is not null)
                        and (tx_id is null or start_ts is not null
                             and deadline_millis is not null and isolation_level is not null)),
                      constraint mvcc_local_first_slot_segment_fk foreign key (segment_start)
                        references mvcc_local_first_segment_v1(segment_start),
                      constraint mvcc_local_first_slot_txn_fk foreign key (tx_id)
                        references mvcc_local_first_txn_v1(tx_id)
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_local_first_state_v1
                      (singleton_id, base_commit_ts, next_commit_ts, published_ts, safe_point, schema_version)
                    values (1, ?, ?, ?, 0, 4)
                    on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setLong(1, baseCommitTs);
                insert.setLong(2, baseCommitTs);
                insert.setLong(3, baseCommitTs);
                insert.executeUpdate();
            }
            requireState(connection);
            ensureFastPublicationProcedure(connection);
            fastPublicationReady = true;
        } catch (final SQLException ex) {
            throw storageError("initialize local-first control schema failed", ex);
        }
    }

    /**
     * Old version Center Tables are only used to move back-compatible incremental columns, with the original transaction default as a reference mode.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureIntentProtocolColumn(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.columns
                where table_schema = database() and table_name = 'mvcc_local_first_txn_v1'
                  and column_name = 'protocol_mode' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (Statement alter = connection.createStatement()) {
            alter.execute("""
                    alter table mvcc_local_first_txn_v1
                    add column protocol_mode varchar(16) not null default 'SCALAR'
                    """);
        }
    }

    /**
     * A stand-alone background pool only detects created new time lines and release routines, without reruning foreground DDL.
     */
    public void requireInitializedFastPath() {
        try (Connection connection = dataSource.getConnection()) {
            requireState(connection);
            try (PreparedStatement query = connection.prepareStatement("""
                    select 1 from information_schema.routines
                    where routine_schema = database()
                      and routine_name = 'mvcc_local_first_publish_v1' limit 1
                    """); ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw illegal("local-first publisher routine is not installed");
                }
            }
            fastPublicationReady = true;
        } catch (final SQLException ex) {
            throw storageError("inspect local-first control fast path failed", ex);
        }
    }

    /**
     * An transaction to read published prefix and save independent snapshot lease, which can be changed to coordinator lease.
     *
     * @param isolationLevel global transaction isolation level
     * @param timeoutMillis timeout millis
     * @return the operation result
     */
    public BeginReservation begin(final IsolationLevel isolationLevel, final long timeoutMillis) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        validateDuration(timeoutMillis);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long readTs;
                long safePoint;
                long deadline;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                        from mvcc_local_first_state_v1 where singleton_id = 1 for share
                        """)) {
                    state.setLong(1, timeoutMillis);
                    try (ResultSet row = state.executeQuery()) {
                        if (!row.next()) {
                            throw illegal("local-first state missing during begin");
                        }
                        readTs = row.getLong("published_ts");
                        safePoint = row.getLong("safe_point");
                        deadline = row.getLong("deadline");
                    }
                }
                if (readTs < safePoint) {
                    throw illegal("local-first published snapshot is older than safe point");
                }
                try (PreparedStatement lease = connection.prepareStatement("""
                        insert into mvcc_local_first_snapshot_v1(read_ts, expires_at) values (?, ?)
                        """, Statement.RETURN_GENERATED_KEYS)) {
                    lease.setLong(1, readTs);
                    lease.setLong(2, deadline);
                    if (lease.executeUpdate() != 1) {
                        throw new SQLException("local-first snapshot lease insertion was incomplete");
                    }
                    try (ResultSet generated = lease.getGeneratedKeys()) {
                        if (!generated.next()) {
                            throw new SQLException("local-first snapshot lease returned no transaction id");
                        }
                        return new BeginReservation(generated.getLong(1), readTs, deadline, false);
                    }
                }
            });
        } catch (final SQLException ex) {
            throw storageError("begin local-first snapshot failed", ex);
        }
    }

    /**
     * An durable coordinator lease protects this node with multiple pens read-only and pending commit snapshot.
     *
     * @param leaseMillis lease millis
     * @return the operation result
     */
    public CoordinatorSnapshotLease acquireCoordinatorLease(final long leaseMillis) {
        validateDuration(leaseMillis);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long readTs;
                long deadline;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                        from mvcc_local_first_state_v1 where singleton_id = 1 for share
                        """)) {
                    state.setLong(1, leaseMillis);
                    try (ResultSet row = state.executeQuery()) {
                        if (!row.next() || row.getLong("published_ts") < row.getLong("safe_point")) {
                            throw illegal("local-first coordinator cannot protect published snapshot");
                        }
                        readTs = row.getLong("published_ts");
                        deadline = row.getLong("deadline");
                    }
                }
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_local_first_coordinator_v1
                          (read_ts, expires_at, generation, status)
                        values (?, ?, 1, 'ACTIVE')
                        """, Statement.RETURN_GENERATED_KEYS)) {
                    insert.setLong(1, readTs);
                    insert.setLong(2, deadline);
                    if (insert.executeUpdate() != 1) {
                        throw new SQLException("local-first coordinator lease insert was incomplete");
                    }
                    try (ResultSet generated = insert.getGeneratedKeys()) {
                        if (!generated.next()) {
                            throw new SQLException("local-first coordinator lease returned no id");
                        }
                        return new CoordinatorSnapshotLease(generated.getLong(1), readTs, deadline, 1);
                    }
                }
            });
        } catch (final SQLException ex) {
            throw storageError("acquire local-first coordinator lease failed", ex);
        }
    }

    /**
     * Only the watermark and coordinator algebras are read consistently at each entry starting, without inserting transaction lease.
     *
     * @param coordinatorId coordinator id
     * @param generation generation
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public CoordinatorSnapshotView snapshotUnderCoordinatorLease(final long coordinatorId,
                                                                 final long generation, final long timeoutMillis) {
        if (coordinatorId < 1 || generation < 1) {
            throw new IllegalArgumentException("invalid local-first coordinator identity");
        }
        validateDuration(timeoutMillis);
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select s.published_ts, s.safe_point, c.read_ts, c.expires_at,
                       c.generation, c.status,
                       cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) + ? as deadline
                from mvcc_local_first_state_v1 s
                join mvcc_local_first_coordinator_v1 c on c.coordinator_id = ?
                where s.singleton_id = 1
                """)) {
            query.setLong(1, timeoutMillis);
            query.setLong(2, coordinatorId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !"ACTIVE".equals(row.getString("status"))) {
                    throw aborted("local-first coordinator snapshot lease is unavailable");
                }
                long published = row.getLong("published_ts");
                long safePoint = row.getLong("safe_point");
                long leaseRead = row.getLong("read_ts");
                long leaseExpiry = row.getLong("expires_at");
                long deadline = row.getLong("deadline");
                if (row.getLong("generation") != generation || leaseExpiry < deadline) {
                    throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                            "local-first coordinator lease cannot cover transaction deadline", false, null);
                }
                if (published < safePoint || leaseRead > published || leaseRead < safePoint) {
                    throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                            "local-first coordinator snapshot crossed safe-point boundary", false, null);
                }
                return new CoordinatorSnapshotView(published, deadline, leaseRead, leaseExpiry);
            }
        } catch (final SQLException ex) {
            throw storageError("read local-first coordinator snapshot failed", ex);
        }
    }

    /**
     * Local GC or operational portal can review durable algebra, duration and original transaction start watermark.
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
            throw new IllegalArgumentException("invalid local-first protected snapshot");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select read_ts, expires_at, generation, status,
                       cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as database_now
                from mvcc_local_first_coordinator_v1 where coordinator_id = ?
                """)) {
            query.setLong(1, coordinatorId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !"ACTIVE".equals(row.getString("status"))
                        || row.getLong("generation") != generation
                        || row.getLong("read_ts") > startTs
                        || row.getLong("expires_at") < deadlineMillis
                        || row.getLong("expires_at") <= row.getLong("database_now")) {
                    throw aborted("local-first coordinator no longer protects transaction");
                }
                if (row.getLong("database_now") >= deadlineMillis) {
                    throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                            "local-first protected transaction expired", false, txId);
                }
            }
        } catch (final SQLException ex) {
            throw storageError("verify local-first coordinator snapshot failed", ex);
        }
    }

    /**
     * Resumes by the current algebra and increases only the oldest active snapshot lower boundary.
     *
     * @param coordinatorId coordinator id
     * @param generation generation
     * @param nextReadTs next read ts
     * @param leaseMillis lease millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public CoordinatorSnapshotLease renewCoordinatorLease(final long coordinatorId, final long generation,
                                                          final long nextReadTs, final long leaseMillis) {
        if (coordinatorId < 1 || generation < 1 || nextReadTs < 0) {
            throw new IllegalArgumentException("invalid local-first coordinator renewal");
        }
        validateDuration(leaseMillis);
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                long safePoint;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point from mvcc_local_first_state_v1
                        where singleton_id = 1 for share
                        """); ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw illegal("local-first state missing during coordinator renewal");
                    }
                    published = row.getLong(1);
                    safePoint = row.getLong(2);
                }
                long currentRead;
                long currentExpiry;
                long currentGeneration;
                String status;
                try (PreparedStatement lease = connection.prepareStatement("""
                        select read_ts, expires_at, generation, status
                        from mvcc_local_first_coordinator_v1
                        where coordinator_id = ? for update
                        """)) {
                    lease.setLong(1, coordinatorId);
                    try (ResultSet row = lease.executeQuery()) {
                        if (!row.next()) {
                            throw aborted("local-first coordinator lease was removed");
                        }
                        currentRead = row.getLong("read_ts");
                        currentExpiry = row.getLong("expires_at");
                        currentGeneration = row.getLong("generation");
                        status = row.getString("status");
                    }
                }
                long now = databaseNow(connection);
                if (!"ACTIVE".equals(status) || currentGeneration != generation || currentExpiry <= now) {
                    throw aborted("local-first coordinator lease generation or deadline changed");
                }
                if (generation == Long.MAX_VALUE || nextReadTs < currentRead
                        || nextReadTs < safePoint || nextReadTs > published) {
                    throw illegal("local-first coordinator cannot lower snapshot protection");
                }
                long expiry = Math.max(currentExpiry, Math.addExact(now, leaseMillis));
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_coordinator_v1
                        set read_ts = ?, expires_at = ?, generation = generation + 1
                        where coordinator_id = ? and generation = ? and status = 'ACTIVE'
                        """)) {
                    update.setLong(1, nextReadTs);
                    update.setLong(2, expiry);
                    update.setLong(3, coordinatorId);
                    update.setLong(4, generation);
                    if (update.executeUpdate() != 1) {
                        throw aborted("local-first coordinator renewal lost current generation");
                    }
                }
                return new CoordinatorSnapshotLease(coordinatorId, nextReadTs, expiry, generation + 1);
            });
        } catch (final SQLException ex) {
            throw storageError("renew local-first coordinator lease failed", ex);
        }
    }

    /**
     * Coordinator can release its GC protection line early when the current algebra is not active local transaction.
     *
     * @param coordinatorId coordinator id
     * @param generation generation
     * @return the operation result
     */
    public boolean releaseCoordinatorLease(final long coordinatorId, final long generation) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_coordinator_v1
                where coordinator_id = ? and generation = ? and status = 'ACTIVE'
                """)) {
            delete.setLong(1, coordinatorId);
            delete.setLong(2, generation);
            return delete.executeUpdate() == 1;
        } catch (final SQLException ex) {
            throw storageError("release local-first coordinator lease failed", ex);
        }
    }

    /**
     * Press database clock to clear coordinator, the missing old algebra cannot recreate the same number.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public int expireCoordinatorLeases(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first coordinator expiry limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_coordinator_v1
                where expires_at <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                order by coordinator_id limit ?
                """)) {
            delete.setInt(1, limit);
            return delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("expire local-first coordinator leases failed", ex);
        }
    }

    /**
     * The cross-shard prewrite durable fixed complete participant from which the reading side can identify intent as ACTIVE.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    public TxRecord registerIntentTransaction(final long txId, final long startTs, final long deadlineMillis,
                                              final IsolationLevel isolationLevel, final Set<String> participants) {
        return registerIntentTransactionInternal(txId, startTs, deadlineMillis,
                isolationLevel, participants, IntentProtocol.SCALAR);
    }

    /**
     * Cross-shard transaction vector durable fixed protocol mode and complete participant before any shard prewrite.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @return the operation result
     */
    public TxRecord registerVectorIntentTransaction(final long txId, final long startTs, final long deadlineMillis,
                                                    final IsolationLevel isolationLevel, final Set<String> participants) {
        return registerIntentTransactionInternal(txId, startTs, deadlineMillis,
                isolationLevel, participants, IntentProtocol.VECTOR);
    }

    /**
     * Both agreements share atomic ACTIVE registration, and replay cannot change the durable protocol mode.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param protocol protocol
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private TxRecord registerIntentTransactionInternal(final long txId, final long startTs, final long deadlineMillis,
                                                       final IsolationLevel isolationLevel, final Set<String> participants,
                                                       final IntentProtocol protocol) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (txId == 0 || startTs < 0 || deadlineMillis < 1 || participants.size() < 2
                || participants.contains(INTENT_SEGMENT_ROUTE)) {
            throw new IllegalArgumentException("invalid cross-shard intent registration");
        }
        String encoded = participants.stream().sorted().collect(java.util.stream.Collectors.joining(","));
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                // The missing line is read consistently only and cannot be locked in the main key space before waiting for insertion with other transaction s.
                if (readMetadataRecord(connection, txId, false) != null) {
                    return requireIntentRegistrationReplay(connection, txId, startTs,
                            deadlineMillis, isolationLevel, participants, protocol);
                }
                long now = databaseNow(connection);
                if (deadlineMillis <= now || startTs < requireState(connection).safePoint()) {
                    throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                            "cross-shard intent snapshot expired before registration", false, txId);
                }
                requireIntentSnapshot(connection, txId, startTs, deadlineMillis, now);
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_local_first_txn_v1
                          (tx_id, start_ts, commit_ts, shard_id, participants,
                           isolation_level, deadline_millis, protocol_mode,
                           status, finalized_at, candidate_gc_done)
                        values (?, ?, null, ?, ?, ?, ?, ?, 'ACTIVE', 0, 0)
                        """)) {
                    insert.setLong(1, txId);
                    insert.setLong(2, startTs);
                    insert.setString(3, INTENT_SEGMENT_ROUTE);
                    insert.setString(4, encoded);
                    insert.setString(5, isolationLevel.name());
                    insert.setLong(6, deadlineMillis);
                    insert.setString(7, protocol.name());
                    if (insert.executeUpdate() != 1) {
                        throw illegal("cross-shard ACTIVE metadata was not persisted");
                    }
                }
                return new TxRecord(txId, startTs, isolationLevel, participants, TxStatus.ACTIVE, null);
            });
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 1062) {
                try {
                    return MysqlConnectionContext.inTransaction(dataSource, connection -> requireIntentRegistrationReplay(connection, txId, startTs,
                            deadlineMillis, isolationLevel, participants, protocol));
                } catch (final SQLException lookupFailure) {
                    ex.addSuppressed(lookupFailure);
                }
            }
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent registration requires durable lookup", false, txId, ex);
        }
    }

    /**
     * New transaction registers multiple ACTIVE identity s with a single centre transaction, and the whole batch is not allowed for prewrite.
     *
     * @param requests requests
     * @return the operation result
     */
    public List<TxRecord> registerIntentBatch(final List<IntentRegistrationRequest> requests) {
        return registerIntentBatchInternal(requests, IntentProtocol.SCALAR);
    }

    /**
     * Vector transaction batch ACTIVE registrations are in the same centre as protocol mode transaction durable drop-off.
     *
     * @param requests requests
     * @return the operation result
     */
    public List<TxRecord> registerVectorIntentBatch(final List<IntentRegistrationRequest> requests) {
        return registerIntentBatchInternal(requests, IntentProtocol.VECTOR);
    }

    /**
     * Both modes share an order key lock and whole batch atomic registration, which does not allow a hidden mixed batch protocol.
     *
     * @param requests requests
     * @param protocol protocol
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private List<TxRecord> registerIntentBatchInternal(final List<IntentRegistrationRequest> requests,
                                                       final IntentProtocol protocol) {
        if (requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid cross-shard registration batch");
        }
        List<IntentRegistrationRequest> ordered = new ArrayList<>(requests);
        ordered.sort(Comparator.comparingLong(IntentRegistrationRequest::txId));
        Set<Long> txIds = new HashSet<>();
        for (IntentRegistrationRequest request : ordered) {
            if (request == null || !txIds.add(request.txId())) {
                throw new IllegalArgumentException("cross-shard registration batch repeats a transaction");
            }
        }
        String values = String.join(",", Collections.nCopies(ordered.size(),
                "(?, ?, null, ?, ?, ?, ?, ?, 'ACTIVE', 0, 0)"));
        String sql = "insert into mvcc_local_first_txn_v1"
                + " (tx_id, start_ts, commit_ts, shard_id, participants, isolation_level,"
                + " deadline_millis, protocol_mode, status, finalized_at, candidate_gc_done) values " + values;
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long now = databaseNow(connection);
                long safePoint = requireState(connection).safePoint();
                for (IntentRegistrationRequest request : ordered) {
                    if (request.deadlineMillis() <= now || request.startTs() < safePoint) {
                        throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                                "cross-shard intent batch snapshot expired", false, request.txId());
                    }
                    requireIntentSnapshot(connection, request.txId(), request.startTs(),
                            request.deadlineMillis(), now);
                }
                try (PreparedStatement insert = connection.prepareStatement(sql)) {
                    int parameter = 1;
                    for (IntentRegistrationRequest request : ordered) {
                        insert.setLong(parameter++, request.txId());
                        insert.setLong(parameter++, request.startTs());
                        insert.setString(parameter++, INTENT_SEGMENT_ROUTE);
                        insert.setString(parameter++, request.participants().stream().sorted()
                                .collect(java.util.stream.Collectors.joining(",")));
                        insert.setString(parameter++, request.isolationLevel().name());
                        insert.setLong(parameter++, request.deadlineMillis());
                        insert.setString(parameter++, protocol.name());
                    }
                    if (insert.executeUpdate() != ordered.size()) {
                        throw illegal("cross-shard ACTIVE batch insertion was incomplete");
                    }
                }
                return requests.stream().map(request -> new TxRecord(request.txId(), request.startTs(),
                        request.isolationLevel(), request.participants(), TxStatus.ACTIVE, null)).toList();
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard ACTIVE batch requires durable status lookup", false, null, ex);
        }
    }

    /**
     * Respond to lost or concurrent -symbol insert only the same centre durable identity.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param protocol protocol
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private TxRecord requireIntentRegistrationReplay(final Connection connection, final long txId, final long startTs,
                                                     final long deadlineMillis, final IsolationLevel isolationLevel,
                                                     final Set<String> participants, final IntentProtocol protocol) throws SQLException {
        IntentMetadata metadata = lockIntentMetadata(connection, txId);
        if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())
                || metadata.record().startTs() != startTs
                || metadata.record().isolationLevel() != isolationLevel
                || metadata.protocol() != protocol
                || !metadata.record().participants().equals(participants)
                || metadata.deadlineMillis() != deadlineMillis) {
            throw illegal("cross-shard intent registration replay changed durable identity");
        }
        if (metadata.record().status() == TxStatus.ABORTED) {
            throw aborted("cross-shard intent transaction was already aborted");
        }
        return metadata.record();
    }

    /**
     * Read side and shard materialization read unfailing real time transaction terminal state with the same control table.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<TxRecord> metadataRecord(final long txId) {
        try (Connection connection = dataSource.getConnection()) {
            return Optional.ofNullable(readMetadataRecord(connection, txId, false));
        } catch (final SQLException ex) {
            throw storageError("read local-first transaction metadata failed", ex);
        }
    }

    /**
     * Background recovery must select the materialization path in durable mode. The vector transaction cannot be handed over to the old standard routine.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public boolean isVectorIntentTransaction(final long txId) {
        try (Connection connection = dataSource.getConnection()) {
            IntentMetadata metadata = readIntentMetadata(connection, txId, false);
            if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())) {
                throw illegal("vector intent protocol lookup has no matching cross-shard transaction");
            }
            return metadata.protocol() == IntentProtocol.VECTOR;
        } catch (final SQLException ex) {
            throw storageError("read cross-shard intent protocol mode failed", ex);
        }
    }

    /**
     * Read the irreversible cross-shard decision confirmation at one centre for multiple physical shard reuses.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public CommittedIntentProof committedIntentProof(final long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select tx_id, start_ts, commit_ts, shard_id, participants,
                       isolation_level, deadline_millis, status, candidate_gc_done, protocol_mode
                from mvcc_local_first_txn_v1 where tx_id = ?
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw illegal("cross-shard materialization lost center decision");
                }
                IntentMetadata metadata = mapIntentMetadata(row);
                if (!INTENT_SEGMENT_ROUTE.equals(metadata.route())
                        || metadata.record().status() != TxStatus.COMMITTED
                        || metadata.record().commitTs() == null
                        || metadata.record().participants().size() < 2) {
                    throw illegal("cross-shard materialization has no COMMITTED intent decision");
                }
                return new CommittedIntentProof(this, metadata.record());
            }
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard materialization requires durable center decision", false, txId, ex);
        }
    }

    /**
     * Shard can only be terminated at the centre or rollback cross-shard intent after the database deadline has expired.
     *
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     */
    public void assertIntentAbortAllowed(final long txId) {
        try {
            MysqlConnectionContext.inTransaction(dataSource, connection -> {
                try (PreparedStatement query = connection.prepareStatement("""
                        select shard_id, status, deadline_millis from mvcc_local_first_txn_v1
                        where tx_id = ? for share
                        """)) {
                    query.setLong(1, txId);
                    try (ResultSet row = query.executeQuery()) {
                        if (!row.next() || !INTENT_SEGMENT_ROUTE.equals(row.getString("shard_id"))) {
                            throw illegal("cross-shard intent abort lost center metadata");
                        }
                        TxStatus status = TxStatus.valueOf(row.getString("status"));
                        if (status == TxStatus.ABORTED) {
                            return null;
                        }
                        if (status != TxStatus.ACTIVE && status != TxStatus.PREPARING
                                || row.getLong("deadline_millis") > databaseNow(connection)) {
                            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                    "cross-shard intent cannot abort before center deadline or decision",
                                    false, txId);
                        }
                        return null;
                    }
                }
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard abort requires durable center status lookup", false, txId, ex);
        }
    }

    /**
     * Cross-shard COMMITTED decision for Physical materialization not completed by placing a symbol transaction identifier page.
     *
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> findUnfinalizedIntentDecisions(final long afterTxId, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid cross-shard intent recovery scan limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select tx_id from mvcc_local_first_txn_v1
                where tx_id > ? and shard_id = ? and status = 'COMMITTED'
                  and candidate_gc_done = 0
                order by tx_id limit ?
                """)) {
            query.setLong(1, afterTxId);
            query.setString(2, INTENT_SEGMENT_ROUTE);
            query.setInt(3, limit);
            List<Long> found = new ArrayList<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    found.add(row.getLong(1));
                }
            }
            return List.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("scan unfinalized cross-shard intent decisions failed", ex);
        }
    }

    /**
     * Experiments to transport peacekeeping capacity are not accurate in materialization commit numbers and the single page ceiling cannot be miscalculated as a backlog.
     *
     * @return the operation result
     */
    public long countUnfinalizedIntentDecisions() {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select count(*) from mvcc_local_first_txn_v1
                where shard_id = ? and status = 'COMMITTED' and candidate_gc_done = 0
                """)) {
            query.setString(1, INTENT_SEGMENT_ROUTE);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw illegal("cross-shard recovery backlog count returned no row");
                }
                return row.getLong(1);
            }
        } catch (final SQLException ex) {
            throw storageError("count unfinalized cross-shard decisions failed", ex);
        }
    }

    /**
     * Expire pending or intent that has been decided to abort but has not been physically cleaned, press transaction ID wheel recovery.
     *
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> findIntentAbortWorkIds(final long afterTxId, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid expired cross-shard intent scan limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select tx_id from mvcc_local_first_txn_v1
                where tx_id > ? and shard_id = ?
                  and ((status in ('ACTIVE', 'PREPARING')
                        and deadline_millis <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                       or (status = 'ABORTED' and candidate_gc_done = 0))
                order by tx_id limit ?
                """)) {
            query.setLong(1, afterTxId);
            query.setString(2, INTENT_SEGMENT_ROUTE);
            query.setInt(3, limit);
            List<Long> found = new ArrayList<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    found.add(row.getLong(1));
                }
            }
            return List.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("scan cross-shard intent abort work failed", ex);
        }
    }

    /**
     * The COMMITTED decision is marked as GC only after two real terminal state s have been materialization.
     *
     * @param txId transaction identifier
     * @param proofs proofs
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean markIntentFinalized(final long txId, final List<IntentTerminalProof> proofs) {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                Long slotCommitTs;
                // Batch decision by chute and transaction metadata by locking, eventually comply with the same order to avoid cross-waiting.
                try (PreparedStatement slot = connection.prepareStatement("""
                        select status, commit_ts from mvcc_local_first_slot_v1
                        where tx_id = ? for share
                        """)) {
                    slot.setLong(1, txId);
                    try (ResultSet row = slot.executeQuery()) {
                        if (!row.next() || !"COMMITTED".equals(row.getString("status"))) {
                            throw illegal("cross-shard finalized metadata lost committed slot");
                        }
                        slotCommitTs = row.getLong("commit_ts");
                    }
                }
                IntentMetadata metadata = lockIntentMetadata(connection, txId);
                if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())
                        || metadata.record().status() != TxStatus.COMMITTED
                        || !Objects.equals(metadata.record().commitTs(), slotCommitTs)) {
                    throw illegal("cross-shard intent finalization has no matching COMMITTED metadata");
                }
                requireIntentTerminalProofs(metadata.record(), proofs);
                if (metadata.gcDone()) {
                    return false;
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_txn_v1
                        set candidate_gc_done = 1,
                            finalized_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        where tx_id = ? and shard_id = ? and status = 'COMMITTED'
                          and candidate_gc_done = 0
                        """)) {
                    update.setLong(1, txId);
                    update.setString(2, INTENT_SEGMENT_ROUTE);
                    if (update.executeUpdate() != 1) {
                        throw illegal("cross-shard intent finalization lost center decision");
                    }
                }
                return true;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent finalization requires durable lookup", false, txId, ex);
        }
    }

    /**
     * The centre of materialization cross-shard commit is eventually merged by transaction ID bounded into a single control transaction.
     *
     * @param requests requests
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public int markIntentFinalizedBatch(final List<IntentFinalizationRequest> requests) {
        if (requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid cross-shard finalization batch");
        }
        List<IntentFinalizationRequest> ordered = new ArrayList<>(requests);
        ordered.sort(Comparator.comparingLong(IntentFinalizationRequest::txId));
        Set<Long> txIds = new HashSet<>();
        for (IntentFinalizationRequest request : ordered) {
            if (request == null || !txIds.add(request.txId())) {
                throw new IllegalArgumentException("cross-shard finalization repeats a transaction");
            }
        }
        String placeholders = String.join(",", Collections.nCopies(ordered.size(), "?"));
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                Map<Long, Long> slots = new HashMap<>();
                try (
                        PreparedStatement query = connection.prepareStatement(
                                "select tx_id, commit_ts, status from mvcc_local_first_slot_v1"
                                        + " where tx_id in (" + placeholders + ")")) {
                    for (int index = 0; index < ordered.size(); index++) {
                        query.setLong(index + 1, ordered.get(index).txId());
                    }
                    try (ResultSet row = query.executeQuery()) {
                        while (row.next()) {
                            if (!"COMMITTED".equals(row.getString("status"))
                                    || slots.put(row.getLong("tx_id"), row.getLong("commit_ts")) != null) {
                                throw illegal("cross-shard finalization batch has invalid slot");
                            }
                        }
                    }
                }
                if (slots.size() != ordered.size()) {
                    throw illegal("cross-shard finalization batch lost committed slot");
                }
                Map<Long, IntentMetadata> metadataRows = lockIntentMetadataBatch(connection,
                        ordered.stream().map(IntentFinalizationRequest::txId).toList());
                List<Long> pending = new ArrayList<>();
                for (IntentFinalizationRequest request : ordered) {
                    IntentMetadata metadata = metadataRows.get(request.txId());
                    if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())
                            || metadata.record().status() != TxStatus.COMMITTED
                            || !Objects.equals(metadata.record().commitTs(), slots.get(request.txId()))) {
                        throw illegal("cross-shard finalization batch differs from center decision");
                    }
                    requireIntentTerminalProofs(metadata.record(), request.proofs());
                    if (!metadata.gcDone()) {
                        pending.add(request.txId());
                    }
                }
                if (pending.isEmpty()) {
                    return 0;
                }
                String pendingPlaces = String.join(",", Collections.nCopies(pending.size(), "?"));
                String updateSql = "update mvcc_local_first_txn_v1 force index(primary)"
                        + " set candidate_gc_done = 1,"
                        + " finalized_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)"
                        + " where tx_id in (" + pendingPlaces + ") and shard_id = ?"
                        + " and status = 'COMMITTED' and candidate_gc_done = 0";
                try (PreparedStatement update = connection.prepareStatement(updateSql)) {
                    int parameter = 1;
                    for (long txId : pending) {
                        update.setLong(parameter++, txId);
                    }
                    update.setString(parameter, INTENT_SEGMENT_ROUTE);
                    if (update.executeUpdate() != pending.size()) {
                        throw illegal("cross-shard finalization batch lost participant completion");
                    }
                }
                return pending.size();
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard finalization batch requires durable status lookup", false, null, ex);
        }
    }

    /**
     * Foreground Visible rollback or background Expire recovery First durable Write irreversible Center ABORTED decision.
     *
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public TxRecord abortIntentTransaction(final long txId) {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                IntentMetadata metadata = lockIntentMetadata(connection, txId);
                if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())) {
                    throw illegal("cross-shard intent abort has no matching metadata");
                }
                if (metadata.record().status() == TxStatus.ABORTED) {
                    return metadata.record();
                }
                if (metadata.record().status() != TxStatus.ACTIVE
                        && metadata.record().status() != TxStatus.PREPARING
                        || metadata.record().commitTs() != null) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "cross-shard abort raced a committed decision", false, txId);
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_txn_v1
                        set status = 'ABORTED'
                        where tx_id = ? and shard_id = ?
                          and status in ('ACTIVE', 'PREPARING') and commit_ts is null
                        """)) {
                    update.setLong(1, txId);
                    update.setString(2, INTENT_SEGMENT_ROUTE);
                    if (update.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "cross-shard intent lost ACTIVE metadata during abort", false, txId);
                    }
                }
                return new TxRecord(txId, metadata.record().startTs(),
                        metadata.record().isolationLevel(), metadata.record().participants(),
                        TxStatus.ABORTED, null);
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard abort decision requires durable lookup", false, txId, ex);
        }
    }

    /**
     * All participant aborts the drop-off before releasing the GC protection recorded by ABORTED Center.
     *
     * @param txId transaction identifier
     * @param proofs proofs
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean markIntentAbortFinalized(final long txId, final List<IntentTerminalProof> proofs) {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                IntentMetadata metadata = lockIntentMetadata(connection, txId);
                if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())
                        || metadata.record().status() != TxStatus.ABORTED
                        || metadata.record().commitTs() != null) {
                    throw illegal("cross-shard abort finalization has no ABORTED decision");
                }
                requireIntentTerminalProofs(metadata.record(), proofs);
                if (metadata.gcDone()) {
                    return false;
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_txn_v1
                        set candidate_gc_done = 1,
                            finalized_at = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        where tx_id = ? and shard_id = ? and status = 'ABORTED'
                          and candidate_gc_done = 0
                        """)) {
                    update.setLong(1, txId);
                    update.setString(2, INTENT_SEGMENT_ROUTE);
                    if (update.executeUpdate() != 1) {
                        throw illegal("cross-shard abort finalization lost center decision");
                    }
                }
                return true;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard abort finalization requires durable lookup", false, txId, ex);
        }
    }

    /**
     * Shard GC retains the transaction lower bounds of pending or commit but the local recovery anchor has not been cleared.
     *
     * @return the operation result
     */
    public Optional<Long> oldestUnfinalizedStartTs() {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select min(start_ts) from mvcc_local_first_txn_v1
                where shard_id = ? and (status in ('ACTIVE', 'PREPARING')
                   or (status in ('COMMITTED', 'ABORTED') and candidate_gc_done = 0))
                """)) {
            query.setString(1, INTENT_SEGMENT_ROUTE);
            try (ResultSet row = query.executeQuery()) {
                return row.next() && row.getObject(1) != null
                        ? Optional.of(row.getLong(1))
                        : Optional.empty();
            }
        } catch (final SQLException ex) {
            throw storageError("read local-first unfinalized metadata floor failed", ex);
        }
    }

    /**
     * Stable request key saves only one consecutive period and binds the conservative snapshot lower bound durable to target shard.
     *
     * @param shardId logical shard identifier
     * @param ownerId owner id
     * @param reservationKey reservation key
     * @param size size
     * @param leaseMillis lease millis
     * @param snapshotFloor snapshot floor
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public SegmentReservation reserveSegment(final String shardId, final String ownerId, final String reservationKey,
                                             final int size, final long leaseMillis, final long snapshotFloor) {
        validateRoute(shardId, ownerId, reservationKey);
        validateDuration(leaseMillis);
        if (size < 1 || size > 4096 || snapshotFloor < 0) {
            throw new IllegalArgumentException("invalid local-first segment size or snapshot floor");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                State state = lockState(connection);
                SegmentRow replay = findByReservationKey(connection, reservationKey);
                if (replay != null) {
                    if (!replay.shardId().equals(shardId) || !replay.ownerId().equals(ownerId)
                            || replay.segmentEnd() - replay.segmentStart() + 1 != size
                            || replay.leaseMillis() != leaseMillis || replay.snapshotFloor() != snapshotFloor) {
                        throw illegal("local-first reservation key replay changed durable identity");
                    }
                    if (replay.status() != SegmentStatus.PREPARING && replay.status() != SegmentStatus.ACTIVE) {
                        throw aborted("local-first reservation was already fenced");
                    }
                    return reservation(replay);
                }
                if (snapshotFloor < state.safePoint() || snapshotFloor > state.publishedTs()) {
                    throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                            "local-first segment floor is outside protected published history", false, null);
                }
                long first = Math.addExact(state.nextCommitTs(), 1);
                long last = Math.addExact(state.nextCommitTs(), size);
                long now = databaseNow(connection);
                long deadline = Math.addExact(now, leaseMillis);
                try (PreparedStatement advance = connection.prepareStatement("""
                        update mvcc_local_first_state_v1 set next_commit_ts = ? where singleton_id = 1
                        """); PreparedStatement segment = connection.prepareStatement("""
                        insert into mvcc_local_first_segment_v1
                          (segment_start, segment_end, shard_id, owner_id, reservation_key,
                           generation, snapshot_floor, lease_millis, expires_at, status,
                           physical_fence_gc_done, created_at)
                        values (?, ?, ?, ?, ?, 1, ?, ?, ?, 'PREPARING', ?, ?)
                        """); PreparedStatement slot = connection.prepareStatement("""
                        insert into mvcc_local_first_slot_v1(commit_ts, segment_start, status)
                        values (?, ?, 'PENDING')
                        """)) {
                    advance.setLong(1, last);
                    if (advance.executeUpdate() != 1) {
                        throw illegal("local-first state lost during segment reservation");
                    }
                    segment.setLong(1, first);
                    segment.setLong(2, last);
                    segment.setString(3, shardId);
                    segment.setString(4, ownerId);
                    segment.setString(5, reservationKey);
                    segment.setLong(6, snapshotFloor);
                    segment.setLong(7, leaseMillis);
                    segment.setLong(8, deadline);
                    segment.setBoolean(9, INTENT_SEGMENT_ROUTE.equals(shardId));
                    segment.setLong(10, now);
                    if (segment.executeUpdate() != 1) {
                        throw illegal("local-first segment row was not persisted");
                    }
                    for (int index = 0; index < size; index++) {
                        slot.setLong(1, first + index);
                        slot.setLong(2, first);
                        slot.addBatch();
                    }
                    if (slot.executeBatch().length != size) {
                        throw new SQLException("local-first segment slots were not fully persisted");
                    }
                }
                return new SegmentReservation(new PublicationSegmentLease(first, last, ownerId, 1, deadline),
                        shardId, snapshotFloor, leaseMillis, SegmentStatus.PREPARING);
            });
        } catch (final SQLException ex) {
            throw storageError("reserve local-first segment failed", ex);
        }
    }

    /**
     * The physical shard fence will not be available for local distribution until the physical shard fence is installed.
     *
     * @param expected expected
     * @param physical physical
     * @return the operation result
     */
    public SegmentReservation activateSegment(final SegmentReservation expected, final MysqlShardSegmentFence physical) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(physical, "physical");
        if (INTENT_SEGMENT_ROUTE.equals(expected.shardId())) {
            throw illegal("cross-shard intent segment must not activate a physical local fence");
        }
        if (!expected.shardId().equals(physical.shardId())) {
            throw illegal("local-first activation targets another physical shard");
        }
        var installed = physical.snapshot(expected.lease().firstTs());
        if (!installed.active() || installed.generation() != expected.lease().generation()) {
            throw aborted("local-first activation has no active physical fence");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow current = lockSegment(connection, expected.lease().firstTs());
                requireSameReservation(expected, current);
                if (current.status() == SegmentStatus.ACTIVE) {
                    return reservation(current);
                }
                if (current.status() != SegmentStatus.PREPARING || current.expiresAtMillis() <= databaseNow(connection)) {
                    throw aborted("local-first segment activation lost valid owner lease");
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_segment_v1 set status = 'ACTIVE'
                        where segment_start = ? and status = 'PREPARING' and generation = ?
                        """)) {
                    update.setLong(1, current.segmentStart());
                    update.setLong(2, current.generation());
                    if (update.executeUpdate() != 1) {
                        throw aborted("local-first segment activation lost owner generation");
                    }
                }
                return reservation(current.withStatus(SegmentStatus.ACTIVE));
            });
        } catch (final SQLException ex) {
            throw storageError("activate local-first segment failed", ex);
        }
    }

    /**
     * Intent mode does not have shard local first commit fence, and the centre segment can be activated directly after the durable reservation.
     *
     * @param expected expected
     * @return the operation result
     */
    public SegmentReservation activateIntentSegment(final SegmentReservation expected) {
        Objects.requireNonNull(expected, "expected");
        if (!INTENT_SEGMENT_ROUTE.equals(expected.shardId())) {
            throw illegal("local-first physical segment cannot use intent activation");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow current = lockSegment(connection, expected.lease().firstTs());
                requireSameReservation(expected, current);
                if (current.status() == SegmentStatus.ACTIVE) {
                    return reservation(current);
                }
                if (current.status() != SegmentStatus.PREPARING
                        || current.expiresAtMillis() <= databaseNow(connection)) {
                    throw aborted("cross-shard intent segment lost valid control owner");
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_segment_v1 set status = 'ACTIVE'
                        where segment_start = ? and generation = ? and status = 'PREPARING'
                        """)) {
                    update.setLong(1, current.segmentStart());
                    update.setLong(2, current.generation());
                    if (update.executeUpdate() != 1) {
                        throw aborted("cross-shard intent activation lost owner generation");
                    }
                }
                return reservation(current.withStatus(SegmentStatus.ACTIVE));
            });
        } catch (final SQLException ex) {
            throw storageError("activate cross-shard intent segment failed", ex);
        }
    }

    /**
     * Re-letting only when current ACTIVE algebra and database lease are valid, responding to lost retry returns durable term.
     *
     * @param expected expected
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public SegmentReservation renewSegment(final SegmentReservation expected) {
        Objects.requireNonNull(expected, "expected");
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow current = lockSegment(connection, expected.lease().firstTs());
                if (current == null || current.segmentEnd() != expected.lease().lastTs()
                        || !current.shardId().equals(expected.shardId())
                        || !current.ownerId().equals(expected.lease().ownerId())
                        || current.generation() != expected.lease().generation()
                        || current.snapshotFloor() != expected.snapshotFloor()
                        || current.leaseMillis() != expected.leaseMillis()
                        || current.status() != SegmentStatus.ACTIVE) {
                    throw aborted("local-first segment renewal lost active owner identity");
                }
                long now = databaseNow(connection);
                if (current.expiresAtMillis() <= now) {
                    throw aborted("local-first segment renewal cannot revive expired owner");
                }
                long expiry = Math.max(current.expiresAtMillis(),
                        Math.addExact(now, current.leaseMillis()));
                if (expiry != current.expiresAtMillis()) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_local_first_segment_v1 set expires_at = ?
                            where segment_start = ? and generation = ? and status = 'ACTIVE'
                            """)) {
                        update.setLong(1, expiry);
                        update.setLong(2, current.segmentStart());
                        update.setLong(3, current.generation());
                        if (update.executeUpdate() != 1) {
                            throw aborted("local-first segment renewal lost owner generation");
                        }
                    }
                }
                return reservation(new SegmentRow(current.segmentStart(), current.segmentEnd(),
                        current.shardId(), current.ownerId(), current.generation(),
                        current.snapshotFloor(), current.leaseMillis(), expiry, current.status()));
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first segment renewal requires durable owner lookup", false, null, ex);
        }
    }

    /**
     * Unconnected take-over first durable enters FENCING before requesting shard Physics fence across the network.
     *
     * @param expected expected
     * @param forceClose force close
     * @return the operation result
     */
    public FenceRequest beginFencing(final SegmentReservation expected, final boolean forceClose) {
        Objects.requireNonNull(expected, "expected");
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow current = lockSegment(connection, expected.lease().firstTs());
                requireSameReservation(expected, current);
                if (current.status() == SegmentStatus.FENCING || current.status() == SegmentStatus.SEALED) {
                    return fenceRequest(current);
                }
                if (current.status() != SegmentStatus.ACTIVE && current.status() != SegmentStatus.PREPARING) {
                    throw aborted("local-first segment cannot enter fencing from this state");
                }
                if (!forceClose && current.expiresAtMillis() > databaseNow(connection)) {
                    throw aborted("local-first owner lease is not expired");
                }
                if (current.generation() == Long.MAX_VALUE) {
                    throw aborted("local-first owner generation cannot advance");
                }
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_local_first_segment_v1
                        set status = 'FENCING', generation = generation + 1
                        where segment_start = ? and generation = ?
                          and status in ('PREPARING', 'ACTIVE')
                        """)) {
                    update.setLong(1, current.segmentStart());
                    update.setLong(2, current.generation());
                    if (update.executeUpdate() != 1) {
                        throw aborted("local-first fencing lost owner generation");
                    }
                }
                return fenceRequest(current.withStatus(SegmentStatus.FENCING)
                        .withGeneration(current.generation() + 1));
            });
        } catch (final SQLException ex) {
            throw storageError("begin local-first segment fencing failed", ex);
        }
    }

    /**
     * Multiple shard durable certificates the completion of central metadata and the same time slot in a control transaction.
     *
     * @param expected expected
     * @param physical physical
     * @param transactionIds transaction ids
     * @return the operation result
     */
    public int completeCandidates(final SegmentReservation expected, final MysqlShardSegmentFence physical,
                                  final List<Long> transactionIds) {
        return completeCandidatesInternal(expected, physical, transactionIds, false).completed();
    }

    /**
     * Batch close to the released prefix centre can advance a continuous watermark in the same transaction line.
     *
     * @param expected expected
     * @param physical physical
     * @param transactionIds transaction ids
     * @return the operation result
     */
    public BatchCompletionResult completeCandidatesAndPublish(final SegmentReservation expected,
                                                              final MysqlShardSegmentFence physical,
                                                              final List<Long> transactionIds) {
        return completeCandidatesInternal(expected, physical, transactionIds, true);
    }

    /**
     * Foreground proves completion of centre batch by the shard generated after success commit does not need to be asked again for shard.
     *
     * @param expected expected
     * @param physical physical
     * @param committed committed
     * @return the operation result
     */
    public int completeProvedCandidates(final SegmentReservation expected, final MysqlShardSegmentFence physical,
                                        final List<LocalCommitProof> committed) {
        return completeProvedCandidatesInternal(expected, physical, committed, false).completed();
    }

    /**
     * Foreground attests that batch is in the same centre transaction with a sequential propulsion to release watermark.
     *
     * @param expected expected
     * @param physical physical
     * @param committed committed
     * @return the operation result
     */
    public BatchCompletionResult completeProvedCandidatesAndPublish(final SegmentReservation expected,
                                                                    final MysqlShardSegmentFence physical,
                                                                    final List<LocalCommitProof> committed) {
        return completeProvedCandidatesInternal(expected, physical, committed, true);
    }

    /**
     * Only the same physical example has succeeded with a different commit transaction certificate to skip shard back.
     *
     * @param expected expected
     * @param physical physical
     * @param committed committed
     * @param publishAfter publish after
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private BatchCompletionResult completeProvedCandidatesInternal(final SegmentReservation expected,
                                                                   final MysqlShardSegmentFence physical,
                                                                   final List<LocalCommitProof> committed,
                                                                   final boolean publishAfter) {
        if (committed == null || committed.isEmpty() || committed.size() > 64) {
            throw new IllegalArgumentException("invalid proved local-first completion batch");
        }
        List<CandidateProof> proofs = new ArrayList<>(committed.size());
        Set<Long> txIds = new HashSet<>();
        for (LocalCommitProof proof : committed) {
            if (proof == null || !proof.belongsTo(physical)
                    || !txIds.add(proof.candidate().txId())) {
                throw illegal("local-first completion has invalid or duplicate physical proof");
            }
            proofs.add(proof.candidate());
        }
        return completeCandidateProofsInternal(expected, physical, proofs, publishAfter);
    }

    /**
     * Two shard or more durable intent determine the metadata and global time slots with the same control over transaction.
     *
     * @param requests requests
     * @return the operation result
     */
    public int completeIntentBatch(final List<IntentCommitRequest> requests) {
        return completeIntentBatchInternal(requests, false, false).completed();
    }

    /**
     * The new vector protocol accepts only prewrite certificates from the real participant intent with the serial number transaction.
     *
     * @param requests requests
     * @return the operation result
     */
    public int completeVectorIntentBatch(final List<IntentCommitRequest> requests) {
        requireVectorPrewriteProofs(requests);
        return completeIntentBatchInternal(requests, false, true).completed();
    }

    /**
     * The vector centre decision is finished with the prefix release transaction, and ACK still has to wait for shard vector-to-face coverage.
     *
     * @param requests requests
     * @return the operation result
     */
    public BatchCompletionResult completeVectorIntentBatchAndPublish(final List<IntentCommitRequest> requests) {
        requireVectorPrewriteProofs(requests);
        return completeIntentBatchInternal(requests, true, true);
    }

    /**
     * Batch Each participant must issue a non-empty single key retention certificate.
     *
     * @param requests requests
     */
    private void requireVectorPrewriteProofs(final List<IntentCommitRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        for (IntentCommitRequest request : requests) {
            for (PrewriteProof proof : request.prewriteProofs()) {
                if (proof.mutationCount() != 1 || proof.vectorReservation().isEmpty()) {
                    throw illegal("vector cross-shard commit lacks a durable stripe reservation proof");
                }
            }
        }
    }

    /**
     * Cross-shard decision with transaction Propulsion durable continuous watermark by close to the release prefix.
     *
     * @param requests requests
     * @return the operation result
     */
    public BatchCompletionResult completeIntentBatchAndPublish(final List<IntentCommitRequest> requests) {
        return completeIntentBatchInternal(requests, true, false);
    }

    /**
     * Only the same transaction terminal state is issued after a full shard prewrite certificate and central slot check.
     *
     * @param requests requests
     * @param publishAfter publish after
     * @param vectorRequired vector required
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private BatchCompletionResult completeIntentBatchInternal(final List<IntentCommitRequest> requests,
                                                              final boolean publishAfter, final boolean vectorRequired) {
        if (requests == null || requests.isEmpty() || requests.size() > 64) {
            throw new IllegalArgumentException("invalid cross-shard intent decision batch");
        }
        List<IntentCommitRequest> ordered = new ArrayList<>(requests);
        ordered.sort(Comparator.comparingLong(IntentCommitRequest::commitTs));
        long segmentStart = ordered.getFirst().segment().lease().firstTs();
        Set<Long> transactionIds = new HashSet<>();
        Set<Long> commitTimestamps = new HashSet<>();
        for (IntentCommitRequest request : ordered) {
            if (!INTENT_SEGMENT_ROUTE.equals(request.segment().shardId())
                    || request.segment().lease().firstTs() != segmentStart
                    || request.commitTs() < segmentStart
                    || request.commitTs() > request.segment().lease().lastTs()
                    || !transactionIds.add(request.txId()) || !commitTimestamps.add(request.commitTs())) {
                throw new IllegalArgumentException("cross-shard batch mixed owner ranges or transaction identities");
            }
            Set<String> proved = new HashSet<>();
            for (PrewriteProof proof : request.prewriteProofs()) {
                if (!proof.belongsTo(this) || proof.txId() != request.txId()
                        || proof.startTs() != request.startTs() || proof.mutationCount() < 1
                        || !request.participants().contains(proof.shardId())
                        || !proved.add(proof.shardId())) {
                    throw illegal("cross-shard commit lacks complete durable prewrite proofs");
                }
            }
            if (!proved.equals(request.participants())) {
                throw illegal("cross-shard commit omitted a participant prewrite proof");
            }
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow owner = lockSegmentShared(connection, segmentStart);
                if (owner == null || !INTENT_SEGMENT_ROUTE.equals(owner.shardId())) {
                    throw illegal("cross-shard intent decision has no matching control segment");
                }
                Map<Long, SlotRow> slots = lockSlotTimes(connection,
                        ordered.stream().map(IntentCommitRequest::commitTs).toList());
                Map<Long, IntentMetadata> metadataRows = lockIntentMetadataBatch(connection,
                        ordered.stream().map(IntentCommitRequest::txId).toList());
                // The whole transaction line lock is taken and read-only database clocks are taken to avoid the expiration of rollback crosses with the old clock decision.
                long now = databaseNow(connection);
                List<IntentCommitRequest> undecided = new ArrayList<>();
                for (IntentCommitRequest request : ordered) {
                    requireSameReservation(request.segment(), owner);
                    if (request.startTs() < owner.snapshotFloor()
                            || request.commitTs() <= request.startTs()) {
                        throw illegal("cross-shard commit crosses snapshot floor or start timestamp");
                    }
                    SlotRow slot = slots.get(request.commitTs());
                    if (slot == null || slot.segmentStart() != owner.segmentStart()) {
                        throw illegal("cross-shard intent decision has no reserved time slot");
                    }
                    IntentMetadata metadata = metadataRows.get(request.txId());
                    if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())
                            || metadata.protocol() != (vectorRequired
                                    ? IntentProtocol.VECTOR
                                    : IntentProtocol.SCALAR)
                            || metadata.record().startTs() != request.startTs()
                            || metadata.record().isolationLevel() != request.isolationLevel()
                            || metadata.deadlineMillis() != request.deadlineMillis()
                            || !metadata.record().participants().equals(request.participants())) {
                        throw illegal("cross-shard commit differs from registered ACTIVE metadata");
                    }
                    if (slot.status() == SlotStatus.COMMITTED) {
                        if (!Objects.equals(slot.txId(), request.txId())
                                || !Objects.equals(slot.startTs(), request.startTs())
                                || !Objects.equals(slot.deadlineMillis(), request.deadlineMillis())
                                || !request.isolationLevel().name().equals(slot.isolationLevel())
                                || metadata.record().status() != TxStatus.COMMITTED
                                || !Objects.equals(metadata.record().commitTs(), request.commitTs())) {
                            throw illegal("cross-shard committed replay changed durable decision");
                        }
                        continue;
                    }
                    if (slot.status() != SlotStatus.PENDING || slot.txId() != null
                            || owner.status() != SegmentStatus.ACTIVE
                            || owner.expiresAtMillis() <= now || request.deadlineMillis() <= now
                            || metadata.record().status() != TxStatus.ACTIVE
                                    && metadata.record().status() != TxStatus.PREPARING
                            || metadata.record().commitTs() != null) {
                        throw aborted("cross-shard intent decision can no longer claim this slot");
                    }
                    undecided.add(request);
                }
                if (!undecided.isEmpty()) {
                    completeIntentMetadataBatch(connection, undecided);
                    completeSlotTimes(connection,
                            undecided.stream().map(IntentCommitRequest::commitTs).toList(),
                            owner.segmentStart());
                }
                long published = publishAfter ? advancePublishedLocked(connection, 4096) : 0;
                return new BatchCompletionResult(undecided.size(), published);
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent decision requires durable status lookup", false, null, ex);
        }
    }

    /**
     * The whole set of ACTIVEs that are locked and checked for identity is written to each commit time with a SQL.
     *
     * @param connection connection
     * @param requests requests
     * @throws SQLException if the operation cannot complete
     */
    private void completeIntentMetadataBatch(final Connection connection, final List<IntentCommitRequest> requests) throws SQLException {
        String cases = String.join(" ", Collections.nCopies(requests.size(), "when ? then ?"));
        String placeholders = String.join(",", Collections.nCopies(requests.size(), "?"));
        String sql = "update mvcc_local_first_txn_v1 force index(primary)"
                + " set status = 'COMMITTED', commit_ts = case tx_id " + cases + " end"
                + " where tx_id in (" + placeholders + ") and shard_id = ?"
                + " and status in ('ACTIVE', 'PREPARING') and commit_ts is null";
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (IntentCommitRequest request : requests) {
                update.setLong(parameter++, request.txId());
                update.setLong(parameter++, request.commitTs());
            }
            for (IntentCommitRequest request : requests) {
                update.setLong(parameter++, request.txId());
            }
            update.setString(parameter, INTENT_SEGMENT_ROUTE);
            if (update.executeUpdate() != requests.size()) {
                throw illegal("cross-shard metadata batch lost ACTIVE status before commit");
            }
        }
    }

    /**
     * The two central paths share durable certificates, the slot lock order and the whole batch atomic rollback validate.
     *
     * @param expected expected
     * @param physical physical
     * @param transactionIds transaction ids
     * @param publishAfter publish after
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private BatchCompletionResult completeCandidatesInternal(final SegmentReservation expected,
                                                             final MysqlShardSegmentFence physical,
                                                             final List<Long> transactionIds, final boolean publishAfter) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(physical, "physical");
        if (!expected.shardId().equals(physical.shardId()) || transactionIds == null
                || transactionIds.isEmpty() || transactionIds.size() > 64
                || new HashSet<>(transactionIds).size() != transactionIds.size()) {
            throw new IllegalArgumentException("invalid local-first completion batch");
        }
        List<CandidateProof> proofs = new ArrayList<>(physical.verifiedCandidates(transactionIds));
        return completeCandidateProofsInternal(expected, physical, proofs, publishAfter);
    }

    /**
     * Recovery Recheck and foreground cannot falsely prove sharing the same centre identity and atomic slot decision.
     *
     * @param expected expected
     * @param physical physical
     * @param proofs proofs
     * @param publishAfter publish after
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private BatchCompletionResult completeCandidateProofsInternal(final SegmentReservation expected,
                                                                  final MysqlShardSegmentFence physical,
                                                                  final List<CandidateProof> proofs,
                                                                  final boolean publishAfter) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(physical, "physical");
        if (!expected.shardId().equals(physical.shardId()) || proofs.isEmpty() || proofs.size() > 64) {
            throw new IllegalArgumentException("invalid proved local-first completion batch");
        }
        for (CandidateProof proof : proofs) {
            if (proof.segmentStart() != expected.lease().firstTs()
                    || proof.commitTs() > expected.lease().lastTs()
                    || proof.startTs() < expected.snapshotFloor()) {
                throw illegal("local-first candidate differs from segment snapshot floor or range");
            }
        }
        proofs.sort(Comparator.comparingLong(CandidateProof::commitTs));
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow segment = lockSegmentShared(connection, expected.lease().firstTs());
                requireSameReservation(expected, segment);
                validateRemainingSnapshotLeases(connection, proofs);
                long originalGeneration = segment.status() == SegmentStatus.FENCING
                        || segment.status() == SegmentStatus.SEALED
                                ? segment.generation() - 1
                                : segment.generation();
                Map<Long, SlotRow> slots = lockSlotsBatch(connection, proofs);
                List<CandidateProof> undecided = new ArrayList<>();
                for (CandidateProof proof : proofs) {
                    if (proof.ownerGeneration() != originalGeneration
                            || proof.startTs() < segment.snapshotFloor()) {
                        throw illegal("local-first candidate has wrong owner generation or snapshot floor");
                    }
                    SlotRow slot = slots.get(proof.commitTs());
                    if (slot == null || slot.segmentStart() != segment.segmentStart()) {
                        throw illegal("local-first candidate has no matching time slot");
                    }
                    if (slot.status() == SlotStatus.COMMITTED) {
                        requireTerminalReplay(connection, proof, slot, segment.shardId());
                        continue;
                    }
                    if (slot.status() != SlotStatus.PENDING || slot.txId() != null
                            || segment.status() == SegmentStatus.SEALED) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "local-first candidate conflicts with a terminal slot", false, proof.txId());
                    }
                    undecided.add(proof);
                }
                if (!undecided.isEmpty()) {
                    insertTerminalBatch(connection, undecided, segment.shardId());
                    completeSlotsBatch(connection, undecided, segment.segmentStart());
                }
                long published = publishAfter ? advancePublishedLocked(connection, 4096)
                        : 0;
                return new BatchCompletionResult(undecided.size(), published);
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first center completion requires durable lookup", false, null, ex);
        }
    }

    /**
     * Press transaction identifier Query Center to be commit terminal state, and unknown outcome without rerun write set.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<TerminalDecision> decision(final long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select tx_id, start_ts, commit_ts, shard_id, isolation_level, deadline_millis
                from mvcc_local_first_txn_v1 where tx_id = ? and status = 'COMMITTED'
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? Optional.of(mapTerminal(row)) : Optional.empty();
            }
        } catch (final SQLException ex) {
            throw storageError("read local-first center decision failed", ex);
        }
    }

    /**
     * Vector old version can only apply for decommissioning after the centre has been released and is fully involved in the finalisation of shard.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<FinalizedVectorProof> finalizedVectorProof(final long txId) {
        return Optional.ofNullable(finalizedVectorProofs(List.of(txId)).get(txId));
    }

    /**
     * Read the finalisation certificate of the bounded candidate from the center at a time, background will no longer use transaction to control connection.
     *
     * @param txIds tx ids
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public Map<Long, FinalizedVectorProof> finalizedVectorProofs(final List<Long> txIds) {
        Objects.requireNonNull(txIds, "txIds");
        if (txIds.isEmpty()) {
            return Map.of();
        }
        if (txIds.size() > 512 || txIds.stream().anyMatch(txId -> txId == null || txId == 0)) {
            throw new IllegalArgumentException("invalid finalized vector proof batch");
        }
        String placeholders = String.join(",", Collections.nCopies(txIds.size(), "?"));
        String sql = """
                select t.tx_id, t.start_ts, t.commit_ts, t.participants,
                       @@server_uuid as authority_uuid, database() as authority_database
                from mvcc_local_first_txn_v1 t
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where t.tx_id in (%s) and t.shard_id = '__INTENT__'
                  and t.protocol_mode = 'VECTOR' and t.status = 'COMMITTED'
                  and t.candidate_gc_done = 1 and t.finalized_at > 0
                  and t.commit_ts is not null and state.published_ts >= t.commit_ts
                """.formatted(placeholders);
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement(sql)) {
            for (int index = 0; index < txIds.size(); index++) {
                query.setLong(index + 1, txIds.get(index));
            }
            Map<Long, FinalizedVectorProof> found = new HashMap<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    long txId = row.getLong("tx_id");
                    long startTs = row.getLong("start_ts");
                    long commitTs = row.getLong("commit_ts");
                    String encoded = row.getString("participants");
                    if (commitTs <= startTs || encoded == null || encoded.isBlank()) {
                        throw illegal("finalized vector proof has invalid durable transaction identity");
                    }
                    Set<String> participants = Set.of(encoded.split(","));
                    if (participants.size() < 2 || participants.contains(INTENT_SEGMENT_ROUTE)) {
                        throw illegal("finalized vector proof has invalid durable participants");
                    }
                    found.put(txId, new FinalizedVectorProof(this, txId, startTs,
                            commitTs, participants, new com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity(
                                    row.getString("authority_uuid"), row.getString("authority_database"))));
                }
            }
            return Map.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("read finalized vector retirement proofs failed", ex);
        }
    }

    /**
     * Vector GC Only finalizes the centre receipt with a secure boundary of the same physical control library.
     *
     * @return the operation result
     */
    public com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity databaseIdentity() {
        return com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(dataSource);
    }

    /**
     * Only GC receipt is issued for the same sealed and published transaction.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<FinalizedCandidateProof> candidateGcProof(final long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select t.tx_id, t.start_ts, t.commit_ts, t.shard_id, t.isolation_level,
                       t.deadline_millis, t.status as txn_status, t.finalized_at,
                       t.candidate_gc_done,
                       s.segment_start, s.tx_id as slot_tx_id, s.status as slot_status,
                       s.start_ts as slot_start, s.deadline_millis as slot_deadline,
                       s.isolation_level as slot_isolation,
                       g.segment_end, g.owner_id, g.generation, g.expires_at,
                       g.shard_id as segment_shard, g.status as segment_status,
                       state.published_ts
                from mvcc_local_first_txn_v1 t
                join mvcc_local_first_slot_v1 s on s.commit_ts = t.commit_ts
                join mvcc_local_first_segment_v1 g on g.segment_start = s.segment_start
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where t.tx_id = ?
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                long commitTs = row.getLong("commit_ts");
                if (INTENT_SEGMENT_ROUTE.equals(row.getString("shard_id"))
                        || row.getBoolean("candidate_gc_done")
                        || !"SEALED".equals(row.getString("segment_status"))
                        || row.getLong("published_ts") < commitTs) {
                    return Optional.empty();
                }
                if (!"COMMITTED".equals(row.getString("txn_status"))
                        || !"COMMITTED".equals(row.getString("slot_status"))
                        || row.getObject("slot_tx_id") == null
                        || row.getLong("slot_tx_id") != txId
                        || row.getObject("slot_start") == null
                        || row.getLong("slot_start") != row.getLong("start_ts")
                        || row.getObject("slot_deadline") == null
                        || row.getLong("slot_deadline") != row.getLong("deadline_millis")
                        || !Objects.equals(row.getString("slot_isolation"), row.getString("isolation_level"))
                        || !Objects.equals(row.getString("segment_shard"), row.getString("shard_id"))
                        || row.getLong("finalized_at") < 1 || row.getLong("generation") < 2) {
                    throw illegal("local-first GC proof differs from finalized center decision");
                }
                long originalGeneration = row.getLong("generation") - 1;
                PublicationSegmentLease physical = new PublicationSegmentLease(
                        row.getLong("segment_start"), row.getLong("segment_end"),
                        row.getString("owner_id"), originalGeneration, row.getLong("expires_at"));
                CandidateProof candidate = new CandidateProof(row.getLong("segment_start"), txId,
                        row.getLong("start_ts"), commitTs, row.getLong("deadline_millis"),
                        IsolationLevel.valueOf(row.getString("isolation_level")), originalGeneration);
                return Optional.of(new FinalizedCandidateProof(row.getString("shard_id"), physical, candidate));
            }
        } catch (final SQLException ex) {
            throw storageError("read local-first finalized candidate proof failed", ex);
        }
    }

    /**
     * The bounded scan of the published shard candidate that is still to be deleted, negative transaction ID also press the symbol primary page break.
     *
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> findPurgeableCandidateIds(final long afterTxId, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first candidate GC scan limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select t.tx_id from mvcc_local_first_txn_v1 t
                join mvcc_local_first_slot_v1 slot on slot.tx_id = t.tx_id
                join mvcc_local_first_segment_v1 segment
                  on segment.segment_start = slot.segment_start
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where t.tx_id > ? and t.candidate_gc_done = 0
                  and t.shard_id <> '__INTENT__'
                  and slot.status = 'COMMITTED' and segment.status = 'SEALED'
                  and state.published_ts >= t.commit_ts
                order by t.tx_id limit ?
                """)) {
            query.setLong(1, afterTxId);
            query.setInt(2, limit);
            List<Long> found = new ArrayList<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    found.add(row.getLong(1));
                }
            }
            return List.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("scan local-first candidate GC proofs failed", ex);
        }
    }

    /**
     * Press the primary key break page to find an expired owner and a working recovery in FENCING after the crash.
     *
     * @param afterSegmentStart after segment start
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<SegmentReservation> findRecoverableSegments(final long afterSegmentStart, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first segment recovery scan limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select segment_start, segment_end, shard_id, owner_id, generation,
                       snapshot_floor, lease_millis, expires_at, status
                from mvcc_local_first_segment_v1
                where segment_start > ?
                  and (status = 'FENCING'
                       or status in ('PREPARING', 'ACTIVE')
                          and expires_at <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                order by segment_start limit ?
                """)) {
            query.setLong(1, afterSegmentStart);
            query.setInt(2, limit);
            List<SegmentReservation> found = new ArrayList<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    found.add(reservation(mapSegment(row)));
                }
            }
            return List.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("scan local-first recoverable segments failed", ex);
        }
    }

    /**
     * Shard confirmed to delete candidate until the durable markup GC is completed, responding to unknown extenuating transaction replay.
     *
     * @param txId transaction identifier
     * @param physical physical
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean markCandidatePurged(final long txId, final MysqlShardSegmentFence physical) {
        Objects.requireNonNull(physical, "physical");
        Optional<FinalizedCandidateProof> proof = candidateGcProof(txId);
        if (proof.isEmpty()) {
            return false;
        }
        if (!physical.shardId().equals(proof.get().shardId())
                || physical.candidate(txId).isPresent()) {
            throw illegal("local-first candidate GC marker lacks physical deletion proof");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement update = connection.prepareStatement("""
                update mvcc_local_first_txn_v1 t
                join mvcc_local_first_slot_v1 slot on slot.tx_id = t.tx_id
                join mvcc_local_first_segment_v1 segment
                  on segment.segment_start = slot.segment_start
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                set t.candidate_gc_done = 1
                where t.tx_id = ? and t.candidate_gc_done = 0
                  and slot.status = 'COMMITTED' and segment.status = 'SEALED'
                  and state.published_ts >= t.commit_ts
                """)) {
            update.setLong(1, txId);
            return update.executeUpdate() == 1;
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first candidate GC marker requires durable lookup", false, txId, ex);
        }
    }

    /**
     * Blocked and yet to be cleared, press the main key break to avoid transaction scans and tagging.
     *
     * @param afterSegmentStart after segment start
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<SegmentReservation> findPurgeableSegments(final long afterSegmentStart, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first segment GC scan limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select segment.segment_start, segment.segment_end, segment.shard_id,
                       segment.owner_id, segment.generation, segment.snapshot_floor,
                       segment.lease_millis, segment.expires_at, segment.status
                from mvcc_local_first_segment_v1 segment
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where segment.segment_start > ? and segment.status = 'SEALED'
                  and segment.shard_id <> '__INTENT__'
                  and segment.segment_end <= state.published_ts
                  and exists (select 1 from mvcc_local_first_slot_v1 slot
                              join mvcc_local_first_txn_v1 txn on txn.tx_id = slot.tx_id
                              where slot.segment_start = segment.segment_start
                                and txn.candidate_gc_done = 0)
                order by segment.segment_start limit ?
                """)) {
            query.setLong(1, afterSegmentStart);
            query.setInt(2, limit);
            List<SegmentReservation> found = new ArrayList<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    found.add(reservation(mapSegment(row)));
                }
            }
            return List.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("scan local-first purgeable segments failed", ex);
        }
    }

    /**
     * Only the whole of an unconstructable caller collection receipt is issued when the entire SEALED and the slot is published.
     *
     * @param segmentStart segment start
     * @return the operation result
     */
    public Optional<FinalizedSegmentProof> segmentGcProof(final long segmentStart) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select segment.segment_start, segment.segment_end, segment.shard_id,
                       segment.owner_id, segment.generation, segment.expires_at,
                       segment.status, state.published_ts
                from mvcc_local_first_segment_v1 segment
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where segment.segment_start = ?
                """)) {
            query.setLong(1, segmentStart);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || INTENT_SEGMENT_ROUTE.equals(row.getString("shard_id"))
                        || !"SEALED".equals(row.getString("status"))
                        || row.getLong("published_ts") < row.getLong("segment_end")) {
                    return Optional.empty();
                }
                long generation = row.getLong("generation");
                if (generation < 2) {
                    throw illegal("local-first sealed segment lost its old physical generation");
                }
                PublicationSegmentLease physical = new PublicationSegmentLease(segmentStart,
                        row.getLong("segment_end"), row.getString("owner_id"), generation - 1,
                        row.getLong("expires_at"));
                return Optional.of(new FinalizedSegmentProof(row.getString("shard_id"), physical));
            }
        } catch (final SQLException ex) {
            throw storageError("read local-first finalized segment proof failed", ex);
        }
    }

    /**
     * Once the entire shard candidate has been emptied, all centres in a SQL durable markup segment terminal state can collection.
     *
     * @param proof proof
     * @param physical physical
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public int markSegmentCandidatesPurged(final FinalizedSegmentProof proof, final MysqlShardSegmentFence physical) {
        Objects.requireNonNull(proof, "proof");
        Objects.requireNonNull(physical, "physical");
        if (!proof.shardId().equals(physical.shardId())
                || physical.countCandidates(proof.physicalLease().firstTs()) != 0) {
            throw illegal("local-first segment GC marker lacks physical deletion proof");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement update = connection.prepareStatement("""
                update mvcc_local_first_txn_v1 txn
                join mvcc_local_first_slot_v1 slot on slot.tx_id = txn.tx_id
                join mvcc_local_first_segment_v1 segment
                  on segment.segment_start = slot.segment_start
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                set txn.candidate_gc_done = 1
                where segment.segment_start = ? and segment.shard_id = ?
                  and segment.owner_id = ? and segment.generation = ?
                  and segment.status = 'SEALED' and segment.segment_end <= state.published_ts
                  and slot.status = 'COMMITTED' and txn.candidate_gc_done = 0
                """)) {
            update.setLong(1, proof.physicalLease().firstTs());
            update.setString(2, proof.shardId());
            update.setString(3, proof.physicalLease().ownerId());
            update.setLong(4, proof.physicalLease().generation() + 1);
            return update.executeUpdate();
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first segment GC marker requires durable lookup", false, null, ex);
        }
    }

    /**
     * Press the primary key bounded to find a clean and condensable physical FENCED tombstone.
     *
     * @param afterSegmentStart after segment start
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<SegmentReservation> findRetirableFences(final long afterSegmentStart, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first fence GC scan limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select segment.segment_start, segment.segment_end, segment.shard_id,
                       segment.owner_id, segment.generation, segment.snapshot_floor,
                       segment.lease_millis, segment.expires_at, segment.status
                from mvcc_local_first_segment_v1 segment
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where segment.segment_start > ? and segment.status = 'SEALED'
                  and segment.shard_id <> '__INTENT__'
                  and segment.physical_fence_gc_done = 0
                  and segment.segment_end < state.safe_point
                  and not exists (select 1 from mvcc_local_first_slot_v1 slot
                                  join mvcc_local_first_txn_v1 txn on txn.tx_id = slot.tx_id
                                  where slot.segment_start = segment.segment_start
                                    and txn.candidate_gc_done = 0)
                  and not exists (select 1 from mvcc_local_first_segment_v1 earlier
                                  where earlier.shard_id = segment.shard_id
                                    and earlier.segment_start < segment.segment_start
                                    and earlier.status in ('PREPARING', 'ACTIVE'))
                order by segment.segment_start limit ?
                """)) {
            query.setLong(1, afterSegmentStart);
            query.setInt(2, limit);
            List<SegmentReservation> found = new ArrayList<>();
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    found.add(reservation(mapSegment(row)));
                }
            }
            return List.copyOf(found);
        } catch (final SQLException ex) {
            throw storageError("scan local-first retirable fences failed", ex);
        }
    }

    /**
     * Current control safe point and completed candidate cleanup jointly issue non-false physical decommissioning receipt.
     *
     * @param segmentStart segment start
     * @return the operation result
     */
    public Optional<FinalizedFenceGcProof> fenceGcProof(final long segmentStart) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select segment.segment_start, segment.segment_end, segment.shard_id,
                       segment.owner_id, segment.generation, segment.expires_at
                from mvcc_local_first_segment_v1 segment
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                where segment.segment_start = ? and segment.status = 'SEALED'
                  and segment.shard_id <> '__INTENT__'
                  and segment.physical_fence_gc_done = 0
                  and segment.segment_end < state.safe_point
                  and not exists (select 1 from mvcc_local_first_slot_v1 slot
                                  join mvcc_local_first_txn_v1 txn on txn.tx_id = slot.tx_id
                                  where slot.segment_start = segment.segment_start
                                    and txn.candidate_gc_done = 0)
                  and not exists (select 1 from mvcc_local_first_segment_v1 earlier
                                  where earlier.shard_id = segment.shard_id
                                    and earlier.segment_start < segment.segment_start
                                    and earlier.status in ('PREPARING', 'ACTIVE'))
                """)) {
            query.setLong(1, segmentStart);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                long generation = row.getLong("generation");
                if (generation < 2) {
                    throw illegal("local-first retired segment lost physical owner generation");
                }
                PublicationSegmentLease physical = new PublicationSegmentLease(segmentStart,
                        row.getLong("segment_end"), row.getString("owner_id"), generation - 1,
                        row.getLong("expires_at"));
                return Optional.of(new FinalizedFenceGcProof(row.getString("shard_id"), physical));
            }
        } catch (final SQLException ex) {
            throw storageError("read local-first finalized fence GC proof failed", ex);
        }
    }

    /**
     * Shard durable Decommissioned watermark has covered the old segment before marking centre physics fence GC is completed.
     *
     * @param proof proof
     * @param physical physical
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public boolean markFenceRetired(final FinalizedFenceGcProof proof, final MysqlShardSegmentFence physical) {
        Objects.requireNonNull(proof, "proof");
        Objects.requireNonNull(physical, "physical");
        if (!proof.shardId().equals(physical.shardId())
                || !physical.isFenceRetired(proof.physicalLease())) {
            throw illegal("local-first fence GC marker lacks durable physical retirement");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement update = connection.prepareStatement("""
                update mvcc_local_first_segment_v1 segment
                join mvcc_local_first_state_v1 state on state.singleton_id = 1
                set segment.physical_fence_gc_done = 1
                where segment.segment_start = ? and segment.shard_id = ?
                  and segment.owner_id = ? and segment.generation = ?
                  and segment.status = 'SEALED' and segment.physical_fence_gc_done = 0
                  and segment.segment_end < state.safe_point
                  and not exists (select 1 from mvcc_local_first_slot_v1 slot
                                  join mvcc_local_first_txn_v1 txn on txn.tx_id = slot.tx_id
                                  where slot.segment_start = segment.segment_start
                                    and txn.candidate_gc_done = 0)
                """)) {
            update.setLong(1, proof.physicalLease().firstTs());
            update.setString(2, proof.shardId());
            update.setString(3, proof.physicalLease().ownerId());
            update.setLong(4, proof.physicalLease().generation() + 1);
            return update.executeUpdate() == 1;
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first fence GC marker requires durable lookup", false, null, ex);
        }
    }

    /**
     * Only release a continuous commit or aborted slot starting from the old watermark, when PENDING stops immediately.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public long advancePublished(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first publication scan limit");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource,
                    connection -> advancePublishedLocked(connection, limit));
        } catch (final SQLException ex) {
            throw storageError("advance local-first published prefix failed", ex);
        }
    }

    /**
     * Reversible center transaction boost consecutive prefix, still pressing durable to check missing slots and pending.
     *
     * @param connection connection
     * @param limit maximum number of items
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private long advancePublishedLocked(final Connection connection, final int limit) throws SQLException {
        State state = lockState(connection);
        long cursor = state.publishedTs();
        if (cursor == Long.MAX_VALUE) {
            return cursor;
        }
        try (PreparedStatement slots = connection.prepareStatement("""
                select commit_ts, status from mvcc_local_first_slot_v1
                where commit_ts > ? order by commit_ts limit ?
                """)) {
            slots.setLong(1, cursor);
            slots.setInt(2, limit);
            try (ResultSet row = slots.executeQuery()) {
                while (row.next()) {
                    if (cursor == Long.MAX_VALUE || row.getLong("commit_ts") != cursor + 1
                            || "PENDING".equals(row.getString("status"))) {
                        break;
                    }
                    cursor++;
                }
            }
        }
        if (cursor > state.publishedTs()) {
            try (PreparedStatement update = connection.prepareStatement("""
                    update mvcc_local_first_state_v1 set published_ts = ? where singleton_id = 1
                    """)) {
                update.setLong(1, cursor);
                if (update.executeUpdate() != 1) {
                    throw illegal("local-first published state disappeared");
                }
            }
        }
        return cursor;
    }

    /**
     * Visible experimental path calls a continuous terminal state prefix with a server, and the abnormal result has to be rechecked for durable watermark.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public long advancePublishedFast(final int limit) {
        if (limit < 1 || limit > 4096 || !fastPublicationReady) {
            throw new IllegalArgumentException("local-first fast publication routine is unavailable or unbounded");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call mvcc_local_first_publish_v1(?)
                """)) {
            call.setInt(1, limit);
            try (ResultSet row = call.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("local-first fast publication returned no persisted watermark");
                }
                return row.getLong("published_ts");
            }
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first fast publication result requires durable waterline lookup", false, null, ex);
        }
    }

    /**
     * The new routine only handles the continuous primary key prefix, without using the status sub-index to guess the missing time slot.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    private void ensureFastPublicationProcedure(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'mvcc_local_first_publish_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure mvcc_local_first_publish_v1(in p_limit int)
                begin
                  declare v_published bigint default null;
                  declare v_cursor bigint default null;
                  declare v_seen bigint default null;
                  declare v_status varchar(16) default null;
                  declare v_done boolean default false;
                  declare v_count int default 0;
                  declare slot_cursor cursor for
                    select commit_ts, status from mvcc_local_first_slot_v1
                    where commit_ts > v_published order by commit_ts limit p_limit;
                  declare continue handler for not found set v_done = true;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  if p_limit < 1 or p_limit > 4096 then
                    signal sqlstate '45000' set mysql_errno = 34001,
                      message_text = 'LOCAL_FIRST_PUBLISH_LIMIT';
                  end if;
                  start transaction;
                  select published_ts into v_published from mvcc_local_first_state_v1
                    where singleton_id = 1 for update;
                  if v_published is null then
                    signal sqlstate '45000' set mysql_errno = 34002,
                      message_text = 'LOCAL_FIRST_PUBLISH_STATE_MISSING';
                  end if;
                  set v_cursor = v_published;
                  set v_done = false;
                  open slot_cursor;
                  publication_loop: loop
                    fetch slot_cursor into v_seen, v_status;
                    if v_done or v_count >= p_limit then
                      leave publication_loop;
                    end if;
                    if v_seen <> v_cursor + 1 then
                      signal sqlstate '45000' set mysql_errno = 34003,
                        message_text = 'LOCAL_FIRST_PUBLISH_MISSING_SLOT';
                    end if;
                    if v_status = 'PENDING' then
                      leave publication_loop;
                    end if;
                    if v_status not in ('COMMITTED', 'ABORTED') then
                      signal sqlstate '45000' set mysql_errno = 34004,
                        message_text = 'LOCAL_FIRST_PUBLISH_UNKNOWN_STATUS';
                    end if;
                    set v_cursor = v_seen;
                    set v_count = v_count + 1;
                  end loop;
                  close slot_cursor;
                  if v_cursor > v_published then
                    update mvcc_local_first_state_v1 set published_ts = v_cursor
                      where singleton_id = 1;
                  end if;
                  commit;
                  select v_cursor as published_ts;
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
     * Physical fencing completes all local candidates, then atomic suspends the real empty slot and closes the centre segment.
     *
     * @param expected expected
     * @param physical physical
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public int sealFencedSegment(final SegmentReservation expected, final MysqlShardSegmentFence physical) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(physical, "physical");
        if (INTENT_SEGMENT_ROUTE.equals(expected.shardId())) {
            throw illegal("cross-shard intent segment requires center-only terminal sealing");
        }
        if (!expected.shardId().equals(physical.shardId())) {
            throw illegal("local-first seal targets another physical shard");
        }
        SegmentReservation current = segment(expected.lease().firstTs()).orElseThrow(() -> illegal("local-first seal has no durable center segment"));
        requireSameReservation(expected, rowFromReservation(current));
        if (current.status() != SegmentStatus.FENCING && current.status() != SegmentStatus.SEALED) {
            throw aborted("local-first seal requires durable center FENCING state");
        }
        if (current.status() == SegmentStatus.SEALED) {
            PublicationSegmentLease oldLease = new PublicationSegmentLease(current.lease().firstTs(),
                    current.lease().lastTs(), current.lease().ownerId(),
                    current.lease().generation() - 1, current.lease().expiresAtMillis());
            if (physical.isFenceRetired(oldLease)) {
                return 0;
            }
            var sealed = physical.snapshot(current.lease().firstTs());
            if (sealed.active() || sealed.generation() != current.lease().generation()) {
                throw aborted("local-first sealed center segment lacks physical fencing");
            }
            return 0;
        }
        long originalGeneration = current.lease().generation() - 1;
        if (originalGeneration < 1) {
            throw illegal("local-first seal lost original owner generation");
        }
        PublicationSegmentLease physicalLease = new PublicationSegmentLease(current.lease().firstTs(),
                current.lease().lastTs(), current.lease().ownerId(), originalGeneration,
                current.lease().expiresAtMillis());
        try {
            List<CandidateProof> candidates = physical.scanFenced(physicalLease,
                    physicalLease.firstTs() - 1, 4096);
            for (int offset = 0; offset < candidates.size(); offset += 64) {
                List<Long> txIds = candidates.subList(offset, Math.min(offset + 64, candidates.size()))
                        .stream().map(CandidateProof::txId).toList();
                completeCandidates(expected, physical, txIds);
            }
            Map<Long, CandidateProof> byCommitTs = new HashMap<>();
            for (CandidateProof proof : candidates) {
                if (byCommitTs.put(proof.commitTs(), proof) != null) {
                    throw illegal("local-first physical fence returned duplicate time slots");
                }
            }
            try {
                return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                    SegmentRow owner = lockSegment(connection, expected.lease().firstTs());
                    requireSameReservation(expected, owner);
                    if (owner.status() != SegmentStatus.FENCING && owner.status() != SegmentStatus.SEALED) {
                        throw aborted("local-first owner left fencing before final seal");
                    }
                    if (owner.generation() != originalGeneration + 1) {
                        throw illegal("local-first seal changed physical owner generation");
                    }
                    int scanned = 0;
                    try (PreparedStatement slots = connection.prepareStatement("""
                            select commit_ts, tx_id, status from mvcc_local_first_slot_v1 force index(primary)
                            where commit_ts >= ? and commit_ts <= ? and segment_start = ?
                            order by commit_ts for update
                            """)) {
                        slots.setLong(1, owner.segmentStart());
                        slots.setLong(2, owner.segmentEnd());
                        slots.setLong(3, owner.segmentStart());
                        try (ResultSet row = slots.executeQuery()) {
                            while (row.next()) {
                                long commitTs = row.getLong("commit_ts");
                                if (commitTs != owner.segmentStart() + scanned) {
                                    throw illegal("local-first seal found a missing reserved time slot");
                                }
                                scanned++;
                                CandidateProof proof = byCommitTs.get(commitTs);
                                String status = row.getString("status");
                                Long txId = nullableLong(row, "tx_id");
                                if (proof != null) {
                                    if (!"COMMITTED".equals(status) || !Objects.equals(txId, proof.txId())) {
                                        throw illegal("local-first seal found an unpublished committed shard proof");
                                    }
                                } else if (txId != null || !"PENDING".equals(status) && !"ABORTED".equals(status)) {
                                    throw illegal("local-first seal found a slot without matching shard proof");
                                }
                            }
                        }
                    }
                    if (scanned != owner.segmentEnd() - owner.segmentStart() + 1) {
                        throw illegal("local-first seal lost reserved slots");
                    }
                    if (owner.status() == SegmentStatus.SEALED) {
                        return 0;
                    }
                    int aborted;
                    try (PreparedStatement tails = connection.prepareStatement("""
                            update mvcc_local_first_slot_v1 force index(primary)
                            set status = 'ABORTED',
                                deadline_millis = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                            where commit_ts >= ? and commit_ts <= ? and segment_start = ?
                              and status = 'PENDING' and tx_id is null
                            """)) {
                        tails.setLong(1, owner.segmentStart());
                        tails.setLong(2, owner.segmentEnd());
                        tails.setLong(3, owner.segmentStart());
                        aborted = tails.executeUpdate();
                    }
                    try (PreparedStatement seal = connection.prepareStatement("""
                            update mvcc_local_first_segment_v1 set status = 'SEALED'
                            where segment_start = ? and generation = ? and status = 'FENCING'
                            """)) {
                        seal.setLong(1, owner.segmentStart());
                        seal.setLong(2, owner.generation());
                        if (seal.executeUpdate() != 1) {
                            throw aborted("local-first final seal lost fencing owner");
                        }
                    }
                    return aborted;
                });
            } catch (final SQLException ex) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local-first final seal requires durable lookup", false, null, ex);
            }
        } catch (final RuntimeException ex) {
            // Another recovery person may be closed and collection local candidate; only double-sided durable SEALED/ FENCED certificates are accepted.
            if (peerCompletedSeal(expected, physical)) {
                return 0;
            }
            throw ex;
        }
    }

    /**
     * After the cross-shard segment has been centered FINCING, only uncentrified commit slots were suspended.
     *
     * @param expected expected
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public int sealIntentSegment(final SegmentReservation expected) {
        Objects.requireNonNull(expected, "expected");
        if (!INTENT_SEGMENT_ROUTE.equals(expected.shardId())) {
            throw illegal("physical local-first segment requires shard fencing before seal");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                SegmentRow owner = lockSegment(connection, expected.lease().firstTs());
                requireSameReservation(expected, owner);
                if (owner.status() == SegmentStatus.SEALED) {
                    return 0;
                }
                if (owner.status() != SegmentStatus.FENCING) {
                    throw aborted("cross-shard intent owner was not durably fenced");
                }
                int scanned = 0;
                try (PreparedStatement slots = connection.prepareStatement("""
                        select commit_ts, tx_id, status, start_ts, deadline_millis,
                               isolation_level from mvcc_local_first_slot_v1 force index(primary)
                        where commit_ts >= ? and commit_ts <= ? and segment_start = ?
                        order by commit_ts for update
                        """)) {
                    slots.setLong(1, owner.segmentStart());
                    slots.setLong(2, owner.segmentEnd());
                    slots.setLong(3, owner.segmentStart());
                    try (ResultSet row = slots.executeQuery()) {
                        while (row.next()) {
                            long commitTs = row.getLong("commit_ts");
                            if (commitTs != owner.segmentStart() + scanned) {
                                throw illegal("cross-shard intent seal found missing time slot");
                            }
                            scanned++;
                            String status = row.getString("status");
                            Long txId = nullableLong(row, "tx_id");
                            if ("COMMITTED".equals(status)) {
                                // Blocklock has stopped new decision, commit identity; lock transaction rows are not reversed in slot order.
                                IntentMetadata metadata = txId == null ? null
                                        : readIntentMetadata(connection, txId, false);
                                if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())
                                        || metadata.record().status() != TxStatus.COMMITTED
                                        || !Objects.equals(metadata.record().commitTs(), commitTs)
                                        || metadata.record().startTs() != row.getLong("start_ts")
                                        || metadata.deadlineMillis() != row.getLong("deadline_millis")
                                        || !metadata.record().isolationLevel().name()
                                                .equals(row.getString("isolation_level"))) {
                                    throw illegal("cross-shard sealed slot lacks matching COMMITTED metadata");
                                }
                            } else if (txId != null || !"PENDING".equals(status)
                                    && !"ABORTED".equals(status)) {
                                throw illegal("cross-shard intent seal found incompatible terminal slot");
                            }
                        }
                    }
                }
                if (scanned != owner.segmentEnd() - owner.segmentStart() + 1) {
                    throw illegal("cross-shard intent seal lost reserved slots");
                }
                int aborted;
                try (PreparedStatement tails = connection.prepareStatement("""
                        update mvcc_local_first_slot_v1 force index(primary)
                        set status = 'ABORTED',
                            deadline_millis = cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        where commit_ts >= ? and commit_ts <= ? and segment_start = ?
                          and status = 'PENDING' and tx_id is null
                        """)) {
                    tails.setLong(1, owner.segmentStart());
                    tails.setLong(2, owner.segmentEnd());
                    tails.setLong(3, owner.segmentStart());
                    aborted = tails.executeUpdate();
                }
                try (PreparedStatement seal = connection.prepareStatement("""
                        update mvcc_local_first_segment_v1
                        set status = 'SEALED', physical_fence_gc_done = 1
                        where segment_start = ? and generation = ? and status = 'FENCING'
                        """)) {
                    seal.setLong(1, owner.segmentStart());
                    seal.setLong(2, owner.generation());
                    if (seal.executeUpdate() != 1) {
                        throw aborted("cross-shard intent seal lost fencing generation");
                    }
                }
                return aborted;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent seal requires durable status lookup", false, null, ex);
        }
    }

    /**
     * Local candidate can be cleared after the concurrent cover is completed, and replay relies only on a two-sided irreversible terminal state.
     *
     * @param expected expected
     * @param physical physical
     * @return the operation result
     */
    private boolean peerCompletedSeal(final SegmentReservation expected, final MysqlShardSegmentFence physical) {
        try {
            SegmentReservation latest = segment(expected.lease().firstTs()).orElse(null);
            if (latest == null || latest.status() != SegmentStatus.SEALED) {
                return false;
            }
            requireSameReservation(expected, rowFromReservation(latest));
            PublicationSegmentLease oldLease = new PublicationSegmentLease(latest.lease().firstTs(),
                    latest.lease().lastTs(), latest.lease().ownerId(),
                    latest.lease().generation() - 1, latest.lease().expiresAtMillis());
            if (physical.isFenceRetired(oldLease)) {
                return true;
            }
            var fence = physical.snapshot(latest.lease().firstTs());
            return !fence.active() && fence.generation() == latest.lease().generation();
        } catch (final RuntimeException checkFailure) {
            return false;
        }
    }

    /**
     * Safe point is also bound by the conservative bottom line of active-drive transaction snapshot and unsealed segments.
     *
     * @return the operation result
     */
    public long advanceSafePoint() {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                State state = lockState(connection);
                long now = databaseNow(connection);
                long target = state.publishedTs();
                try (PreparedStatement snapshots = connection.prepareStatement("""
                        select min(read_ts) from mvcc_local_first_snapshot_v1 where expires_at > ?
                        """)) {
                    snapshots.setLong(1, now);
                    try (ResultSet row = snapshots.executeQuery()) {
                        if (row.next() && row.getObject(1) != null) {
                            long oldest = row.getLong(1);
                            target = Math.min(target, oldest > 0 ? oldest - 1 : 0);
                        }
                    }
                }
                try (PreparedStatement coordinators = connection.prepareStatement("""
                        select min(read_ts) from mvcc_local_first_coordinator_v1
                        where status = 'ACTIVE' and expires_at > ?
                        """)) {
                    coordinators.setLong(1, now);
                    try (ResultSet row = coordinators.executeQuery()) {
                        if (row.next() && row.getObject(1) != null) {
                            long oldest = row.getLong(1);
                            target = Math.min(target, oldest > 0 ? oldest - 1 : 0);
                        }
                    }
                }
                try (PreparedStatement undecided = connection.prepareStatement("""
                        select min(start_ts) from mvcc_local_first_txn_v1
                        where shard_id = ? and (status in ('ACTIVE', 'PREPARING')
                           or (status in ('COMMITTED', 'ABORTED') and candidate_gc_done = 0))
                        """)) {
                    undecided.setString(1, INTENT_SEGMENT_ROUTE);
                    try (ResultSet row = undecided.executeQuery()) {
                        if (row.next() && row.getObject(1) != null) {
                            long oldest = row.getLong(1);
                            target = Math.min(target, oldest > 0 ? oldest - 1 : 0);
                        }
                    }
                }
                try (PreparedStatement segments = connection.prepareStatement("""
                        select min(snapshot_floor) from mvcc_local_first_segment_v1
                        where status in ('PREPARING', 'ACTIVE', 'FENCING')
                        """); ResultSet row = segments.executeQuery()) {
                    if (row.next() && row.getObject(1) != null) {
                        long oldest = row.getLong(1);
                        target = Math.min(target, oldest > 0 ? oldest - 1 : 0);
                    }
                }
                long safe = Math.max(state.safePoint(), target);
                if (safe != state.safePoint()) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_local_first_state_v1 set safe_point = ? where singleton_id = 1
                            """)) {
                        update.setLong(1, safe);
                        if (update.executeUpdate() != 1) {
                            throw illegal("local-first safe point state disappeared");
                        }
                    }
                }
                return safe;
            });
        } catch (final SQLException ex) {
            throw storageError("advance local-first safe point failed", ex);
        }
    }

    /**
     * Expire transaction snapshot to collection, unsealed local section remains independent of fixed GC lower bounds.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public int expireSnapshots(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first snapshot expiry limit");
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_snapshot_v1
                where expires_at <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                order by tx_id limit ?
                """)) {
            delete.setInt(1, limit);
            return delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("expire local-first snapshots failed", ex);
        }
    }

    /**
     * The foreground end only removes its own lease, and the unsealed section still protects the local already commit candidate.
     *
     * @param txId transaction identifier
     */
    public void releaseSnapshot(final long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_snapshot_v1 where tx_id = ?
                """)) {
            delete.setLong(1, txId);
            delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("release local-first snapshot failed", ex);
        }
    }

    /**
     * Read-only Views the state of the durable center segment and can retake the original algebra and target shard if it is unknown.
     *
     * @param segmentStart segment start
     * @return the operation result
     */
    public Optional<SegmentReservation> segment(final long segmentStart) {
        try (Connection connection = dataSource.getConnection()) {
            return Optional.ofNullable(lockSegment(connection, segmentStart)).map(this::reservation);
        } catch (final SQLException ex) {
            throw storageError("read local-first segment failed", ex);
        }
    }

    /**
     * Returns the current watermark released to test old and new RC/RR snapshot.
     *
     * @return the operation result
     */
    public long currentPublished() {
        try (Connection connection = dataSource.getConnection()) {
            return requireState(connection).publishedTs();
        } catch (final SQLException ex) {
            throw storageError("read local-first published waterline failed", ex);
        }
    }

    /**
     * Returns durable GC safe point to check the conservative lower bounds.
     *
     * @return the operation result
     */
    public long currentSafePoint() {
        try (Connection connection = dataSource.getConnection()) {
            return requireState(connection).safePoint();
        } catch (final SQLException ex) {
            throw storageError("read local-first safe point failed", ex);
        }
    }

    /**
     * GC Retention period with control database clocks cannot be replaced by a coordinator wall clock.
     *
     * @return the operation result
     */
    public long currentDatabaseTimeMillis() {
        try (Connection connection = dataSource.getConnection()) {
            return databaseNow(connection);
        } catch (final SQLException ex) {
            throw storageError("read local-first control database time failed", ex);
        }
    }

    /**
     * Cross-shard version preserves the terminal state slot when retirement proof is not available, and the candidate for the scale is cleared by the original reservation window.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param expiredBeforeMillis expired before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    public int purgeTerminalSlots(final long safePoint, final long expiredBeforeMillis, final int limit) {
        validateGcRequest(safePoint, expiredBeforeMillis, limit);
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_slot_v1
                where commit_ts < ? and status in ('COMMITTED', 'ABORTED')
                  and deadline_millis is not null and deadline_millis < ?
                  and (tx_id is null or exists (
                    select 1 from mvcc_local_first_txn_v1 txn
                    where txn.tx_id = mvcc_local_first_slot_v1.tx_id
                      and txn.commit_ts = mvcc_local_first_slot_v1.commit_ts
                      and txn.candidate_gc_done = 1 and txn.finalized_at < ?
                      and txn.protocol_mode = 'SCALAR'))
                order by commit_ts limit ?
                """)) {
            delete.setLong(1, safePoint);
            delete.setLong(2, expiredBeforeMillis);
            delete.setLong(3, expiredBeforeMillis);
            delete.setInt(4, limit);
            return delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("purge local-first terminal slots failed", ex);
        }
    }

    /**
     * Cross-shard version preserves centre decision before retirement with a standard terminal state after the slot has been deleted.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param finalizedBeforeMillis finalized before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    public int purgeFinalizedMetadata(final long safePoint, final long finalizedBeforeMillis, final int limit) {
        validateGcRequest(safePoint, finalizedBeforeMillis, limit);
        try (Connection connection = dataSource.getConnection()) {
            List<Long> candidates = new ArrayList<>();
            // Unlock-free screen finished primary key to avoid the DELETE range scanning locking of ACTIVE rows that are writing group_ts.
            try (PreparedStatement select = connection.prepareStatement("""
                    select txn.tx_id from mvcc_local_first_txn_v1 txn
                    where txn.start_ts < ? and txn.candidate_gc_done = 1
                      and txn.protocol_mode = 'SCALAR'
                      and ((txn.status = 'COMMITTED' and txn.commit_ts < ?)
                           or (txn.status = 'ABORTED' and txn.commit_ts is null))
                      and txn.finalized_at < ?
                      and not exists (select 1 from mvcc_local_first_slot_v1 slot
                                      where slot.tx_id = txn.tx_id)
                    order by txn.tx_id limit ?
                    """)) {
                select.setLong(1, safePoint);
                select.setLong(2, safePoint);
                select.setLong(3, finalizedBeforeMillis);
                select.setInt(4, limit);
                try (ResultSet row = select.executeQuery()) {
                    while (row.next()) {
                        candidates.add(row.getLong(1));
                    }
                }
            }
            if (candidates.isEmpty()) {
                return 0;
            }
            String placeholders = String.join(",", Collections.nCopies(candidates.size(), "?"));
            String sql = "delete from mvcc_local_first_txn_v1"
                    + " where tx_id in (" + placeholders + ") and start_ts < ?"
                    + " and candidate_gc_done = 1 and protocol_mode = 'SCALAR'"
                    + " and ((status = 'COMMITTED' and commit_ts < ?)"
                    + " or (status = 'ABORTED' and commit_ts is null))"
                    + " and finalized_at < ?"
                    + " and not exists (select 1 from mvcc_local_first_slot_v1 slot"
                    + " where slot.tx_id = mvcc_local_first_txn_v1.tx_id)";
            try (PreparedStatement delete = connection.prepareStatement(sql)) {
                int parameter = 1;
                for (long txId : candidates) {
                    delete.setLong(parameter++, txId);
                }
                delete.setLong(parameter++, safePoint);
                delete.setLong(parameter++, safePoint);
                delete.setLong(parameter, finalizedBeforeMillis);
                return delete.executeUpdate();
            }
        } catch (final SQLException ex) {
            throw storageError("purge local-first finalized metadata failed", ex);
        }
    }

    /**
     * Physical decommissioning durable and the cleanup of slots and metadata before removing the centre SEALED section.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param expiredBeforeMillis expired before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    public int purgeSealedSegments(final long safePoint, final long expiredBeforeMillis, final int limit) {
        validateGcRequest(safePoint, expiredBeforeMillis, limit);
        try (Connection connection = dataSource.getConnection()) {
            List<Long> candidates = new ArrayList<>();
            // This slot or commit cannot be added after the wrapping, unlocked check empty segments and then press primary key to delete them, avoiding a back-locking active decision.
            try (PreparedStatement select = connection.prepareStatement("""
                    select segment.segment_start from mvcc_local_first_segment_v1 segment
                    where segment.status = 'SEALED' and segment.physical_fence_gc_done = 1
                      and segment.segment_end < ? and segment.expires_at < ?
                      and not exists (select 1 from mvcc_local_first_slot_v1 slot
                                      where slot.segment_start = segment.segment_start)
                      and not exists (select 1 from mvcc_local_first_txn_v1 txn
                                      where txn.commit_ts >= segment.segment_start
                                        and txn.commit_ts <= segment.segment_end)
                    order by segment.segment_start limit ?
                    """)) {
                select.setLong(1, safePoint);
                select.setLong(2, expiredBeforeMillis);
                select.setInt(3, limit);
                try (ResultSet row = select.executeQuery()) {
                    while (row.next()) {
                        candidates.add(row.getLong(1));
                    }
                }
            }
            if (candidates.isEmpty()) {
                return 0;
            }
            String placeholders = String.join(",", Collections.nCopies(candidates.size(), "?"));
            String sql = "delete from mvcc_local_first_segment_v1"
                    + " where segment_start in (" + placeholders + ")"
                    + " and status = 'SEALED' and physical_fence_gc_done = 1"
                    + " and segment_end < ? and expires_at < ?";
            try (PreparedStatement delete = connection.prepareStatement(sql)) {
                int parameter = 1;
                for (long segmentStart : candidates) {
                    delete.setLong(parameter++, segmentStart);
                }
                delete.setLong(parameter++, safePoint);
                delete.setLong(parameter, expiredBeforeMillis);
                return delete.executeUpdate();
            }
        } catch (final SQLException ex) {
            throw storageError("purge local-first sealed segments failed", ex);
        }
    }

    /**
     * The manual GC request cannot push more than snapshot safe point calculated by durable in the control library.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param cutoffMillis cutoff millis
     * @param limit maximum number of items
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void validateGcRequest(final long safePoint, final long cutoffMillis, final int limit) {
        if (safePoint < 0 || cutoffMillis < 0 || limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first GC request");
        }
        if (safePoint > currentSafePoint()) {
            throw illegal("local-first GC cannot exceed durable safe point");
        }
    }

    /**
     * Rebuild complete reservation information for foreground from centre durable.
     *
     * @param row row
     * @return the operation result
     */
    private SegmentReservation reservation(final SegmentRow row) {
        return new SegmentReservation(new PublicationSegmentLease(row.segmentStart(), row.segmentEnd(),
                row.ownerId(), row.generation(), row.expiresAtMillis()), row.shardId(),
                row.snapshotFloor(), row.leaseMillis(), row.status());
    }

    /**
     * Only for cross-shard scan to check the prefixed object with the current durable center identity.
     *
     * @param reservation reservation
     * @return the operation result
     */
    private SegmentRow rowFromReservation(final SegmentReservation reservation) {
        PublicationSegmentLease lease = reservation.lease();
        return new SegmentRow(lease.firstTs(), lease.lastTs(), reservation.shardId(),
                lease.ownerId(), lease.generation(), reservation.snapshotFloor(),
                reservation.leaseMillis(), lease.expiresAtMillis(), reservation.status());
    }

    /**
     * FENCING will then pass only the original algebra to target shard for physical exclusion fence.
     *
     * @param row row
     * @return the operation result
     */
    private FenceRequest fenceRequest(final SegmentRow row) {
        long physicalGeneration = row.generation() - 1;
        if (physicalGeneration < 1) {
            throw illegal("local-first fencing lost original owner generation");
        }
        return new FenceRequest(new PublicationSegmentLease(row.segmentStart(), row.segmentEnd(),
                row.ownerId(), physicalGeneration, row.expiresAtMillis()), row.shardId(),
                row.snapshotFloor());
    }

    /**
     * The requested key only allows the same paragraph identity of the same target shard.
     *
     * @param connection connection
     * @param key business key
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private SegmentRow findByReservationKey(final Connection connection, final String key) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select segment_start, segment_end, shard_id, owner_id, generation,
                       snapshot_floor, lease_millis, expires_at, status
                from mvcc_local_first_segment_v1 where reservation_key = ? for update
                """)) {
            query.setString(1, key);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? mapSegment(row) : null;
            }
        }
    }

    /**
     * Reads or locks the owner segment, and the caller continues to check state and algebra in the same transaction.
     *
     * @param connection connection
     * @param segmentStart segment start
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private SegmentRow lockSegment(final Connection connection, final long segmentStart) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select segment_start, segment_end, shard_id, owner_id, generation,
                       snapshot_floor, lease_millis, expires_at, status
                from mvcc_local_first_segment_v1 where segment_start = ? for update
                """)) {
            query.setLong(1, segmentStart);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? mapSegment(row) : null;
            }
        }
    }

    /**
     * Shares owner fence when completing a different slot, with only the exclusive conversion of FENCING.
     *
     * @param connection connection
     * @param segmentStart segment start
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private SegmentRow lockSegmentShared(final Connection connection, final long segmentStart) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select segment_start, segment_end, shard_id, owner_id, generation,
                       snapshot_floor, lease_millis, expires_at, status
                from mvcc_local_first_segment_v1 where segment_start = ? for share
                """)) {
            query.setLong(1, segmentStart);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? mapSegment(row) : null;
            }
        }
    }

    /**
     * Locks the entire candidate batch in primary order at a time to avoid creating a reverse slot lock between batch.
     *
     * @param connection connection
     * @param proofs proofs
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Map<Long, SlotRow> lockSlotsBatch(final Connection connection, final List<CandidateProof> proofs) throws SQLException {
        return lockSlotTimes(connection, proofs.stream().map(CandidateProof::commitTs).toList());
    }

    /**
     * Local candidate and cross-shard intent share the main key to raise the slot lock, preventing the state index from being reversed.
     *
     * @param connection connection
     * @param commitTimestamps commit timestamps
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Map<Long, SlotRow> lockSlotTimes(final Connection connection, final List<Long> commitTimestamps) throws SQLException {
        String placeholders = String.join(",", Collections.nCopies(commitTimestamps.size(), "?"));
        String sql = "select commit_ts, segment_start, tx_id, status, start_ts, deadline_millis,"
                + " isolation_level from mvcc_local_first_slot_v1 force index(primary)"
                + " where commit_ts in (" + placeholders + ") order by commit_ts for update";
        Map<Long, SlotRow> found = new HashMap<>();
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            for (int index = 0; index < commitTimestamps.size(); index++) {
                query.setLong(index + 1, commitTimestamps.get(index));
            }
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    SlotRow slot = new SlotRow(row.getLong("segment_start"), nullableLong(row, "tx_id"),
                            SlotStatus.valueOf(row.getString("status")), nullableLong(row, "start_ts"),
                            nullableLong(row, "deadline_millis"), row.getString("isolation_level"));
                    if (found.put(row.getLong("commit_ts"), slot) != null) {
                        throw illegal("local-first batch returned duplicate time slots");
                    }
                }
            }
        }
        if (found.size() != commitTimestamps.size()) {
            throw illegal("local-first batch has missing reserved time slots");
        }
        return found;
    }

    /**
     * Multiple verify local candidates saved a separate center terminal state with an INSERT.
     *
     * @param connection connection
     * @param proofs proofs
     * @param shardId logical shard identifier
     * @throws SQLException if the operation cannot complete
     */
    private void insertTerminalBatch(final Connection connection, final List<CandidateProof> proofs, final String shardId) throws SQLException {
        String values = String.join(",", Collections.nCopies(proofs.size(), """
                (?, ?, ?, ?, ?, ?, ?, 'COMMITTED',
                 cast(unix_timestamp(current_timestamp(3)) * 1000 as signed))
                """));
        String sql = "insert into mvcc_local_first_txn_v1"
                + " (tx_id, start_ts, commit_ts, shard_id, participants, isolation_level,"
                + " deadline_millis, status, finalized_at) values " + values;
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (CandidateProof proof : proofs) {
                insert.setLong(parameter++, proof.txId());
                insert.setLong(parameter++, proof.startTs());
                insert.setLong(parameter++, proof.commitTs());
                insert.setString(parameter++, shardId);
                insert.setString(parameter++, shardId);
                insert.setString(parameter++, proof.isolationLevel().name());
                insert.setLong(parameter++, proof.deadlineMillis());
            }
            if (insert.executeUpdate() != proofs.size()) {
                throw illegal("local-first center terminal batch insertion was incomplete");
            }
        }
    }

    /**
     * Locked slots by the same center transaction row batch to COMMITTED, line numbers do not match total rollback.
     *
     * @param connection connection
     * @param proofs proofs
     * @param segmentStart segment start
     * @throws SQLException if the operation cannot complete
     */
    private void completeSlotsBatch(final Connection connection, final List<CandidateProof> proofs, final long segmentStart) throws SQLException {
        completeSlotTimes(connection, proofs.stream().map(CandidateProof::commitTs).toList(), segmentStart);
    }

    /**
     * The locked mode time slots are only copied from the same transaction centre.
     *
     * @param connection connection
     * @param commitTimestamps commit timestamps
     * @param segmentStart segment start
     * @throws SQLException if the operation cannot complete
     */
    private void completeSlotTimes(final Connection connection, final List<Long> commitTimestamps, final long segmentStart) throws SQLException {
        String placeholders = String.join(",", Collections.nCopies(commitTimestamps.size(), "?"));
        String sql = "update mvcc_local_first_slot_v1 s force index(primary)"
                + " join mvcc_local_first_txn_v1 t on t.commit_ts = s.commit_ts"
                + " set s.tx_id = t.tx_id, s.status = 'COMMITTED', s.start_ts = t.start_ts,"
                + " s.deadline_millis = t.deadline_millis, s.isolation_level = t.isolation_level"
                + " where s.segment_start = ? and s.commit_ts in (" + placeholders + ")"
                + " and s.status = 'PENDING' and s.tx_id is null";
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            update.setLong(1, segmentStart);
            for (int index = 0; index < commitTimestamps.size(); index++) {
                update.setLong(index + 2, commitTimestamps.get(index));
            }
            if (update.executeUpdate() != commitTimestamps.size()) {
                throw illegal("local-first center time-slot batch update was incomplete");
            }
        }
    }

    /**
     * The remaining transaction lease must match the local candidate; the expired lease has been deleted and is still protected by the lower bounds of the paragraph.
     *
     * @param connection connection
     * @param proofs proofs
     * @throws SQLException if the operation cannot complete
     */
    private void validateRemainingSnapshotLeases(final Connection connection, final List<CandidateProof> proofs) throws SQLException {
        List<CandidateProof> individual = proofs.stream().filter(proof -> proof.txId() > 0).toList();
        if (individual.isEmpty()) {
            // Negative transaction ID uses the lower bounds of the coordinator lease and durable sections, with no transaction snapshot lines to search for.
            return;
        }
        String placeholders = String.join(",", Collections.nCopies(individual.size(), "?"));
        String sql = "select tx_id, read_ts, expires_at from mvcc_local_first_snapshot_v1"
                + " where tx_id in (" + placeholders + ") for share";
        Map<Long, CandidateProof> byTxId = new HashMap<>();
        for (CandidateProof proof : individual) {
            byTxId.put(proof.txId(), proof);
        }
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            for (int index = 0; index < individual.size(); index++) {
                query.setLong(index + 1, individual.get(index).txId());
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    CandidateProof proof = byTxId.get(rows.getLong("tx_id"));
                    if (proof == null || proof.startTs() != rows.getLong("read_ts")
                            || proof.deadlineMillis() != rows.getLong("expires_at")) {
                        throw illegal("local-first candidate differs from durable snapshot lease");
                    }
                }
            }
        }
    }

    /**
     * Replay slots and centre transaction should be identical to the original shard field by field.
     *
     * @param connection connection
     * @param proof proof
     * @param slot slot
     * @param shardId logical shard identifier
     * @throws SQLException if the operation cannot complete
     */
    private void requireTerminalReplay(final Connection connection, final CandidateProof proof,
                                       final SlotRow slot, final String shardId) throws SQLException {
        if (!Objects.equals(slot.txId(), proof.txId())
                || !Objects.equals(slot.startTs(), proof.startTs())
                || !Objects.equals(slot.deadlineMillis(), proof.deadlineMillis())
                || !proof.isolationLevel().name().equals(slot.isolationLevel())) {
            throw illegal("local-first committed slot replay changed durable identity");
        }
        TerminalDecision terminal = readTerminal(connection, proof.txId());
        if (terminal == null || terminal.startTs() != proof.startTs()
                || terminal.commitTs() != proof.commitTs()
                || !terminal.shardId().equals(shardId)
                || terminal.isolationLevel() != proof.isolationLevel()
                || terminal.deadlineMillis() != proof.deadlineMillis()) {
            throw illegal("local-first committed metadata replay changed durable identity");
        }
    }

    /**
     * Reads the durable commit records in the same control transaction.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private TerminalDecision readTerminal(final Connection connection, final long txId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select tx_id, start_ts, commit_ts, shard_id, isolation_level, deadline_millis
                from mvcc_local_first_txn_v1 where tx_id = ? and status = 'COMMITTED' for update
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? mapTerminal(row) : null;
            }
        }
    }

    /**
     * All participant terminal proof must come from this book authority, overwrite the full route and be in line with Centre decision.
     *
     * @param center center
     * @param proofs proofs
     */
    private void requireIntentTerminalProofs(final TxRecord center, final List<IntentTerminalProof> proofs) {
        if (proofs == null || proofs.size() != center.participants().size()) {
            throw illegal("cross-shard finalization lacks complete participant proofs");
        }
        Set<String> found = new HashSet<>();
        for (IntentTerminalProof proof : proofs) {
            if (proof == null || !proof.belongsTo(this)
                    || !center.participants().contains(proof.shardId())
                    || !found.add(proof.shardId())
                    || proof.record().txId() != center.txId()
                    || proof.record().startTs() != center.startTs()
                    || proof.record().status() != center.status()
                    || !Objects.equals(proof.record().commitTs(), center.commitTs())) {
                throw illegal("cross-shard finalization has mismatched participant proof");
            }
        }
    }

    /**
     * Batch decision does not need to repeat two queries to the control library for route and deadline.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private IntentMetadata lockIntentMetadata(final Connection connection, final long txId) throws SQLException {
        return readIntentMetadata(connection, txId, true);
    }

    /**
     * Only stable commit identity and rewrite the rest of the cover will get an transaction line-lock.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private IntentMetadata readIntentMetadata(final Connection connection, final long txId, final boolean lock) throws SQLException {
        String sql = "select tx_id, start_ts, commit_ts, shard_id, participants,"
                + " isolation_level, deadline_millis, status, candidate_gc_done, protocol_mode"
                + " from mvcc_local_first_txn_v1 where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? mapIntentMetadata(row) : null;
            }
        }
    }

    /**
     * Press the transaction main key to lock the whole batch, avoiding a round-trip and reverse metadata line lock.
     *
     * @param connection connection
     * @param txIds tx ids
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Map<Long, IntentMetadata> lockIntentMetadataBatch(final Connection connection, final List<Long> txIds) throws SQLException {
        String placeholders = String.join(",", Collections.nCopies(txIds.size(), "?"));
        String sql = "select tx_id, start_ts, commit_ts, shard_id, participants,"
                + " isolation_level, deadline_millis, status, candidate_gc_done, protocol_mode"
                + " from mvcc_local_first_txn_v1 force index(primary)"
                + " where tx_id in (" + placeholders + ") order by tx_id for update";
        Map<Long, IntentMetadata> found = new HashMap<>();
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            for (int index = 0; index < txIds.size(); index++) {
                query.setLong(index + 1, txIds.get(index));
            }
            try (ResultSet row = query.executeQuery()) {
                while (row.next()) {
                    IntentMetadata metadata = mapIntentMetadata(row);
                    if (found.put(metadata.record().txId(), metadata) != null) {
                        throw illegal("cross-shard batch returned duplicate transaction metadata");
                    }
                }
            }
        }
        if (found.size() != txIds.size()) {
            throw illegal("cross-shard batch lost registered transaction metadata");
        }
        return found;
    }

    /**
     * Single pen and batch lock read the same durable identity decoding.
     *
     * @param row row
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private IntentMetadata mapIntentMetadata(final ResultSet row) throws SQLException {
        String encoded = row.getString("participants");
        if (encoded == null || encoded.isBlank()) {
            throw illegal("cross-shard metadata lost durable participant set");
        }
        TxRecord record = new TxRecord(row.getLong("tx_id"), row.getLong("start_ts"),
                IsolationLevel.valueOf(row.getString("isolation_level")),
                Set.of(encoded.split(",")), TxStatus.valueOf(row.getString("status")),
                nullableLong(row, "commit_ts"));
        return new IntentMetadata(record, row.getString("shard_id"),
                row.getLong("deadline_millis"), row.getBoolean("candidate_gc_done"),
                IntentProtocol.valueOf(row.getString("protocol_mode")));
    }

    /**
     * Pending transaction and decision transaction are all reduced by durable participant.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private TxRecord readMetadataRecord(final Connection connection, final long txId, final boolean lock) throws SQLException {
        String sql = "select tx_id, start_ts, isolation_level, participants, status, commit_ts"
                + " from mvcc_local_first_txn_v1 where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                String encoded = row.getString("participants");
                if (encoded == null || encoded.isBlank()) {
                    throw illegal("local-first transaction metadata lost participant set");
                }
                Long commitTs = nullableLong(row, "commit_ts");
                return new TxRecord(row.getLong("tx_id"), row.getLong("start_ts"),
                        IsolationLevel.valueOf(row.getString("isolation_level")),
                        Set.of(encoded.split(",")), TxStatus.valueOf(row.getString("status")), commitTs);
            }
        }
    }

    /**
     * Durable coordinator lower bounds by transaction lease or negative transaction ID must overwrite the start time and deadline.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param now now
     * @throws SQLException if the operation cannot complete
     */
    private void requireIntentSnapshot(final Connection connection, final long txId, final long startTs,
                                       final long deadlineMillis, final long now) throws SQLException {
        if (txId > 0) {
            try (PreparedStatement lease = connection.prepareStatement("""
                    select read_ts, expires_at from mvcc_local_first_snapshot_v1
                    where tx_id = ? for share
                    """)) {
                lease.setLong(1, txId);
                try (ResultSet row = lease.executeQuery()) {
                    if (!row.next() || row.getLong("read_ts") != startTs
                            || row.getLong("expires_at") != deadlineMillis
                            || row.getLong("expires_at") <= now) {
                        throw aborted("cross-shard intent lost matching transaction snapshot lease");
                    }
                }
            }
            return;
        }
        long coordinatorId = (txId >>> 43) & ((1L << 20) - 1);
        long sequence = txId & ((1L << 43) - 1);
        if (coordinatorId < 1 || sequence < 1) {
            throw illegal("cross-shard intent has invalid coordinator transaction id");
        }
        try (PreparedStatement lease = connection.prepareStatement("""
                select read_ts, expires_at, status from mvcc_local_first_coordinator_v1
                where coordinator_id = ? for share
                """)) {
            lease.setLong(1, coordinatorId);
            try (ResultSet row = lease.executeQuery()) {
                if (!row.next() || !"ACTIVE".equals(row.getString("status"))
                        || row.getLong("read_ts") > startTs
                        || row.getLong("expires_at") < deadlineMillis
                        || row.getLong("expires_at") <= now) {
                    throw aborted("cross-shard intent lost coordinator snapshot protection");
                }
            }
        }
    }

    /**
     * Converts the centre COMMITTED line to immutable result.
     *
     * @param row row
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private TerminalDecision mapTerminal(final ResultSet row) throws SQLException {
        return new TerminalDecision(row.getLong("tx_id"), row.getLong("start_ts"),
                row.getLong("commit_ts"), row.getString("shard_id"),
                IsolationLevel.valueOf(row.getString("isolation_level")), row.getLong("deadline_millis"));
    }

    /**
     * JDBC can not misread NULL as transaction identifier integer.
     *
     * @param row row
     * @param column column
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Long nullableLong(final ResultSet row, final String column) throws SQLException {
        long value = row.getLong(column);
        return row.wasNull() ? null : value;
    }

    /**
     * Maps the database to the immutable segment record.
     *
     * @param row row
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private SegmentRow mapSegment(final ResultSet row) throws SQLException {
        return new SegmentRow(row.getLong("segment_start"), row.getLong("segment_end"),
                row.getString("shard_id"), row.getString("owner_id"), row.getLong("generation"),
                row.getLong("snapshot_floor"), row.getLong("lease_millis"),
                row.getLong("expires_at"), SegmentStatus.valueOf(row.getString("status")));
    }

    /**
     * Preset results cannot be converted from owner, shard, range or lower level performance status.
     *
     * @param expected expected
     * @param current current
     */
    private void requireSameReservation(final SegmentReservation expected, final SegmentRow current) {
        if (current == null || current.segmentEnd() != expected.lease().lastTs()
                || !current.shardId().equals(expected.shardId())
                || !current.ownerId().equals(expected.lease().ownerId())
                || current.snapshotFloor() != expected.snapshotFloor()
                || current.leaseMillis() != expected.leaseMillis()
                || current.generation() != expected.lease().generation()
                        && current.generation() != expected.lease().generation() + 1) {
            throw illegal("local-first segment request differs from durable owner identity");
        }
    }

    /**
     * Read the next timestamp, prefix and GC lower bounds in a monopolistic line.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private State lockState(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select next_commit_ts, published_ts, safe_point, schema_version
                from mvcc_local_first_state_v1 where singleton_id = 1 for update
                """); ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getInt("schema_version") != 4) {
                throw illegal("local-first state schema is missing or incompatible");
            }
            return new State(row.getLong("next_commit_ts"), row.getLong("published_ts"),
                    row.getLong("safe_point"));
        }
    }

    /**
     * Unlocked read still has to reject an error schema or return time line.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private State requireState(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select next_commit_ts, published_ts, safe_point, schema_version
                from mvcc_local_first_state_v1 where singleton_id = 1
                """); ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getInt("schema_version") != 4) {
                throw illegal("local-first state schema is missing or incompatible");
            }
            return new State(row.getLong("next_commit_ts"), row.getLong("published_ts"),
                    row.getLong("safe_point"));
        }
    }

    /**
     * Database clocks are used only to control the library lease, shard as its own clock for freeing.
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
                throw new SQLException("local-first control database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Background lease before you refuse to cross the border in one way or another, avoiding the revival of the old owner after the integer spill.
     *
     * @param millis millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void validateDuration(final long millis) {
        if (millis < 1 || millis > 86_400_000L) {
            throw new IllegalArgumentException("invalid local-first lease duration");
        }
    }

    /**
     * The target shard and the homeer request key must be durable replay and cannot be changed by CSV or length.
     *
     * @param shardId logical shard identifier
     * @param ownerId owner id
     * @param reservationKey reservation key
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void validateRoute(final String shardId, final String ownerId, final String reservationKey) {
        TransactionMetadataValidation.validateParticipant(0, shardId);
        if (shardId.length() > 128 || ownerId == null || !ownerId.matches("[A-Za-z0-9._:-]{1,128}")
                || reservationKey == null || !reservationKey.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new IllegalArgumentException("invalid local-first shard, owner or request key");
        }
    }

    /**
     * Durable states of dissatisfied status do not make errors that can be duplicated.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * The owner refuses to continue the local distribution when he enters the lost take-over.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException aborted(final String message) {
        return new MvccException(ErrorCode.TRANSACTION_ABORTED, message, false, null);
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private MvccException storageError(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * The segment status is first installed by shard and then allowed to local commit; taking over must pass durable FENCING.
     */
    public enum SegmentStatus {
        PREPARING, ACTIVE, FENCING, SEALED
    }

    /**
     * Returns the durable centre identity, target shard and snapshot protected lower boundary.
     *
     * @param lease lease
     * @param shardId logical shard identifier
     * @param snapshotFloor snapshot floor
     * @param leaseMillis lease millis
     * @param status status
     */
    public record SegmentReservation(PublicationSegmentLease lease, String shardId,
            long snapshotFloor, long leaseMillis, SegmentStatus status) {

        /**
         * No shard route, negative snapshot lower bound or empty state is allowed to enter the control protocol.
         *
         * @param lease lease
         * @param shardId logical shard identifier
         * @param snapshotFloor snapshot floor
         * @param leaseMillis lease millis
         * @param status status
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public SegmentReservation {
            Objects.requireNonNull(lease, "lease");
            Objects.requireNonNull(status, "status");
            TransactionMetadataValidation.validateParticipant(0, shardId);
            if (snapshotFloor < 0 || leaseMillis < 1) {
                throw new IllegalArgumentException("invalid local-first segment reservation");
            }
        }
    }

    /**
     * Center enters FENCING with the original algebra requesting target shard to write physics tombstone.
     *
     * @param physicalLease physical lease
     * @param shardId logical shard identifier
     * @param snapshotFloor snapshot floor
     */
    public record FenceRequest(PublicationSegmentLease physicalLease, String shardId,
            long snapshotFloor) {

        /**
         * The taking over request must still keep the original owner identity and the lower bounds of snapshot.
         *
         * @param physicalLease physical lease
         * @param shardId logical shard identifier
         * @param snapshotFloor snapshot floor
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public FenceRequest {
            Objects.requireNonNull(physicalLease, "physicalLease");
            TransactionMetadataValidation.validateParticipant(0, shardId);
            if (snapshotFloor < 0) {
                throw new IllegalArgumentException("invalid local-first fence snapshot floor");
            }
        }
    }

    /**
     * Single shard local terminal state is a centre commit record after materialization.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @param shardId logical shard identifier
     * @param isolationLevel global transaction isolation level
     * @param deadlineMillis deadline millis
     */
    public record TerminalDecision(long txId, long startTs, long commitTs, String shardId,
            IsolationLevel isolationLevel, long deadlineMillis) {

        /**
         * Terminal state identity must keep timestamp and target shard consistent.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param shardId logical shard identifier
         * @param isolationLevel global transaction isolation level
         * @param deadlineMillis deadline millis
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public TerminalDecision {
            TransactionMetadataValidation.validateParticipant(txId, shardId);
            Objects.requireNonNull(isolationLevel, "isolationLevel");
            if (txId == 0 || startTs < 0 || commitTs <= startTs || deadlineMillis < 1) {
                throw new IllegalArgumentException("invalid local-first terminal decision");
            }
        }
    }

    /**
     * Returns the new decision number in the same control transaction and the continuous watermark release that has been pushed by durable.
     *
     * @param completed completed
     * @param publishedTs published ts
     */
    public record BatchCompletionResult(int completed, long publishedTs) {

        /**
         * Batch results cannot be reported negative or invalid watermark.
         *
         * @param completed completed
         * @param publishedTs published ts
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public BatchCompletionResult {
            if (completed < 0 || publishedTs < 0) {
                throw new IllegalArgumentException("invalid local-first batch completion result");
            }
        }
    }

    /**
     * A cross-shard Center commit request that has been fully involved in shard collection durable prewrite.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     * @param prewriteProofs prewrite proofs
     */
    public record IntentCommitRequest(SegmentReservation segment, long commitTs, long txId,
            long startTs, long deadlineMillis, IsolationLevel isolationLevel,
            Set<String> participants, List<PrewriteProof> prewriteProofs) {

        /**
         * Queue with immutable route and prewrite receipt, with no entry to modify the participant collection.
         *
         * @param segment segment
         * @param commitTs durable commit timestamp
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param deadlineMillis deadline millis
         * @param isolationLevel global transaction isolation level
         * @param participants participants
         * @param prewriteProofs prewrite proofs
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public IntentCommitRequest {
            Objects.requireNonNull(segment, "segment");
            Objects.requireNonNull(isolationLevel, "isolationLevel");
            TransactionMetadataValidation.validateParticipants(txId, participants);
            participants = Set.copyOf(participants);
            prewriteProofs = List.copyOf(prewriteProofs);
            if (txId == 0 || startTs < 0 || commitTs <= startTs || deadlineMillis < 1
                    || participants.size() < 2 || prewriteProofs.size() != participants.size()) {
                throw new IllegalArgumentException("invalid local-first cross-shard commit request");
            }
        }
    }

    /**
     * An cross-shard transaction registration for prewrite is not yet available.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     * @param participants participants
     */
    public record IntentRegistrationRequest(long txId, long startTs, long deadlineMillis,
            IsolationLevel isolationLevel, Set<String> participants) {

        /**
         * Batch fixes the complete participant collection and duration on arrival.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param deadlineMillis deadline millis
         * @param isolationLevel global transaction isolation level
         * @param participants participants
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public IntentRegistrationRequest {
            Objects.requireNonNull(isolationLevel, "isolationLevel");
            TransactionMetadataValidation.validateParticipants(txId, participants);
            participants = Set.copyOf(participants);
            if (txId == 0 || startTs < 0 || deadlineMillis < 1 || participants.size() < 2
                    || participants.contains(INTENT_SEGMENT_ROUTE)) {
                throw new IllegalArgumentException("invalid cross-shard intent registration request");
            }
        }
    }

    /**
     * Centre durable distinguishes an old cursor transaction from a vector transaction that requires the retention of a semi-serial number.
     */
    private enum IntentProtocol {
        SCALAR, VECTOR
    }

    /**
     * A complete shard terminal proof for background to integrate multiple materialization transaction s into centre batch.
     *
     * @param txId transaction identifier
     * @param proofs proofs
     */
    public record IntentFinalizationRequest(long txId, List<IntentTerminalProof> proofs) {

        /**
         * Batch fixes the list of immutable participant certificates when you join the queue.
         *
         * @param txId transaction identifier
         * @param proofs proofs
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public IntentFinalizationRequest {
            proofs = List.copyOf(proofs);
            if (txId == 0 || proofs.isEmpty()) {
                throw new IllegalArgumentException("invalid cross-shard finalization request");
            }
        }
    }

    /**
     * Background cross-shard materialization receipt issued after reading COMMITTED from this control library.
     */
    public static final class CommittedIntentProof {

        private final MysqlLocalFirstPublicationLedger ledger;

        private final TxRecord record;

        /**
         * Private construction forbids caller to memory transaction state forger center decision.
         *
         * @param ledger ledger
         * @param record record
         */
        private CommittedIntentProof(final MysqlLocalFirstPublicationLedger ledger, final TxRecord record) {
            this.ledger = ledger;
            this.record = record;
        }

        /**
         * Returns durable, transaction identifier, snapshot, commit time and complete participant.
         *
         * @return the operation result
         */
        public TxRecord record() {
            return record;
        }

        /**
         * Materialization allows only the same centre that reads the decision authority.
         *
         * @param authority authority
         * @return the operation result
         */
        public boolean belongsTo(final TransactionMetadataStore authority) {
            return authority instanceof MysqlLocalFirstMetadataView view && view.usesLedger(ledger);
        }
    }

    /**
     * Transaction is available for shard to remove the old big payload with the published shard and all participating materialization vectors.
     */
    public static final class FinalizedVectorProof {

        private final MysqlLocalFirstPublicationLedger ledger;

        private final long txId;

        private final long startTs;

        private final long commitTs;

        private final Set<String> participants;

        private final com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity authorityIdentity;

        /**
         * The durable finalised query can only be constructed from the same control library.
         *
         * @param ledger ledger
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param participants participants
         * @param authorityIdentity authority identity
         */
        private FinalizedVectorProof(final MysqlLocalFirstPublicationLedger ledger, final long txId,
                                     final long startTs, final long commitTs, final Set<String> participants,
                                     final com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity authorityIdentity) {
            this.ledger = ledger;
            this.txId = txId;
            this.startTs = startTs;
            this.commitTs = commitTs;
            this.participants = Set.copyOf(participants);
            this.authorityIdentity = Objects.requireNonNull(authorityIdentity, "authorityIdentity");
        }

        /**
         * Returns durable transaction ID.
         *
         * @return the operation result
         */
        public long txId() {
            return txId;
        }

        /**
         * Returns verify shard terminal state start time.
         *
         * @return the operation result
         */
        public long startTs() {
            return startTs;
        }

        /**
         * Returns centre commit time with verify visibility and replay.
         *
         * @return the operation result
         */
        public long commitTs() {
            return commitTs;
        }

        /**
         * Only shard in the original assembly can be decommissioned from transaction version.
         *
         * @param shardId logical shard identifier
         * @return the operation result
         */
        public boolean includes(final String shardId) {
            return participants.contains(shardId);
        }

        /**
         * The caller should not mix receipt issued in another control library.
         *
         * @param authority authority
         * @return the operation result
         */
        public boolean belongsTo(final MysqlLocalFirstPublicationLedger authority) {
            return ledger == authority;
        }

        /**
         * Proof must be consistent with the real physical control libraries used in security vector reading.
         *
         * @return the operation result
         */
        public com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity authorityIdentity() {
            return authorityIdentity;
        }
    }

    /**
     * The shard candidate collection receipt can only be created when the center is closed, terminal state has been published.
     */
    public static final class FinalizedCandidateProof {

        private final String shardId;

        private final PublicationSegmentLease physicalLease;

        private final CandidateProof candidate;

        /**
         * The construction entrance is only called by the central durable state validate.
         *
         * @param shardId logical shard identifier
         * @param physicalLease physical lease
         * @param candidate candidate
         */
        private FinalizedCandidateProof(final String shardId, final PublicationSegmentLease physicalLease,
                                        final CandidateProof candidate) {
            this.shardId = shardId;
            this.physicalLease = physicalLease;
            this.candidate = candidate;
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
         * Returns the original physical algebra that has been sealed by the centre.
         *
         * @return the operation result
         */
        public PublicationSegmentLease physicalLease() {
            return physicalLease;
        }

        /**
         * Returns the same local candidate as the central COMMITTED slot by field.
         *
         * @return the operation result
         */
        public CandidateProof candidate() {
            return candidate;
        }
    }

    /**
     * After the entire paragraph has been physically sealed and successively published, all shard candidates for this paragraph are allowed to be deleted at a time.
     */
    public static final class FinalizedSegmentProof {

        private final String shardId;

        private final PublicationSegmentLease physicalLease;

        /**
         * Only the control library is allowed to publish SEALED line creation.
         *
         * @param shardId logical shard identifier
         * @param physicalLease physical lease
         */
        private FinalizedSegmentProof(final String shardId, final PublicationSegmentLease physicalLease) {
            this.shardId = shardId;
            this.physicalLease = physicalLease;
        }

        /**
         * Returns the only target in this paragraph shard.
         *
         * @return the operation result
         */
        public String shardId() {
            return shardId;
        }

        /**
         * Returns the old homeer algebra used for physics shard funcing.
         *
         * @return the operation result
         */
        public PublicationSegmentLease physicalLease() {
            return physicalLease;
        }
    }

    /**
     * Center safe point allows physical fence to be decommissioned when it has crossed the entire section without an earlier active segment.
     */
    public static final class FinalizedFenceGcProof {

        private final String shardId;

        private final PublicationSegmentLease physicalLease;

        /**
         * Creates only from the completed GC and safe point validate central books.
         *
         * @param shardId logical shard identifier
         * @param physicalLease physical lease
         */
        private FinalizedFenceGcProof(final String shardId, final PublicationSegmentLease physicalLease) {
            this.shardId = shardId;
            this.physicalLease = physicalLease;
        }

        /**
         * Returns the same physical shard route.
         *
         * @return the operation result
         */
        public String shardId() {
            return shardId;
        }

        /**
         * Returns an old owner segment that requires durable retirement.
         *
         * @return the operation result
         */
        public PublicationSegmentLease physicalLease() {
            return physicalLease;
        }
    }

    /**
     * The control state line bears a short transaction string fence for preset and safe point propulsion in the segment.
     *
     * @param nextCommitTs next commit ts
     * @param publishedTs published ts
     * @param safePoint oldest retained snapshot timestamp
     */
    private record State(long nextCommitTs, long publishedTs, long safePoint) {
    }

    /**
     * Control tanks allow only pending, commit or three durable states to be suspended.
     */
    private enum SlotStatus {
        PENDING, COMMITTED, ABORTED
    }

    /**
     * Slot identity is the same as centre transaction with two-way validate in transaction.
     *
     * @param segmentStart segment start
     * @param txId transaction identifier
     * @param status status
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param isolationLevel global transaction isolation level
     */
    private record SlotRow(long segmentStart, Long txId, SlotStatus status,
            Long startTs, Long deadlineMillis, String isolationLevel) {
    }

    /**
     * Records the original algebra and segment status of the homeer, durable row is recovery authority.
     *
     * @param segmentStart segment start
     * @param segmentEnd segment end
     * @param shardId logical shard identifier
     * @param ownerId owner id
     * @param generation generation
     * @param snapshotFloor snapshot floor
     * @param leaseMillis lease millis
     * @param expiresAtMillis expires at millis
     * @param status status
     */
    private record SegmentRow(long segmentStart, long segmentEnd, String shardId, String ownerId,
            long generation, long snapshotFloor, long leaseMillis,
            long expiresAtMillis, SegmentStatus status) {

        /**
         * Status conversion does not change the remaining durable -depart identity.
         *
         * @param next next
         * @return the operation result
         */
        private SegmentRow withStatus(final SegmentStatus next) {
            return new SegmentRow(segmentStart, segmentEnd, shardId, ownerId, generation,
                    snapshotFloor, leaseMillis, expiresAtMillis, next);
        }

        /**
         * The original algebra can still be reduced to one recovery by the current value after FENCING durable is added to the algebra.
         *
         * @param next next
         * @return the operation result
         */
        private SegmentRow withGeneration(final long next) {
            return new SegmentRow(segmentStart, segmentEnd, shardId, ownerId, next,
                    snapshotFloor, leaseMillis, expiresAtMillis, status);
        }
    }

    /**
     * Read cross-shard mode, duration and universal transaction metadata under the same line lock.
     *
     * @param record record
     * @param route route
     * @param deadlineMillis deadline millis
     * @param gcDone gc done
     * @param protocol protocol
     */
    private record IntentMetadata(TxRecord record, String route, long deadlineMillis,
            boolean gcDone, IntentProtocol protocol) {
    }
}

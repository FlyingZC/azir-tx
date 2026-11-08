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
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.publication.PublicationEntry;
import com.flyingzc.azir.publication.PublicationEntryStatus;
import com.flyingzc.azir.publication.PublicationSegmentLease;
import com.flyingzc.azir.publication.PublicationSegmentCommit;
import com.flyingzc.azir.storage.ParticipantRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Retention, owner fencing, centre terminal state and continuous distribution of durable floors.
 */
public final class MysqlSegmentPublicationLedger {

    private final DataSource dataSource;

    private final MysqlPublicationLedger leaseGc;

    /**
     * Bind with the same MySQL control database, the caller must still provide a stand-alone background connection budget.
     *
     * @param dataSource database connection source
     */
    public MysqlSegmentPublicationLedger(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.leaseGc = new MysqlPublicationLedger(dataSource);
    }

    /**
     * Sub-paragraph foreground transaction must share examples of the same data source with central metadata.
     *
     * @param metadataStore metadata store
     * @return the operation result
     */
    public boolean sharesMetadataAuthority(final TransactionMetadataStore metadataStore) {
        return metadataStore instanceof MysqlTransactionMetadataStore mysql
                && mysql.usesDataSource(dataSource);
    }

    /**
     * Create a crashable database -grade segment fence after the old book has been stopped and all empty.
     */
    public void activateFromLedger() {
        new MysqlSegmentProtocolActivator(dataSource, this).activate();
    }

    /**
     * The physical writing of the verify durable mode and the old status table before the start-up of the business entry.
     */
    public void assertActivated() {
        new MysqlSegmentProtocolActivator(dataSource, this).assertActivated();
    }

    /**
     * Only initializes the version table, repeats the call to keep the original time line and refuses to change the initial upper boundary.
     *
     * @param baseCommitTs base commit ts
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void initializeIsolated(final long baseCommitTs) {
        if (baseCommitTs < 0 || baseCommitTs == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid segment base timestamp");
        }
        MysqlConnectionContext.withPublicationLock(dataSource, 10, () -> {
            try (
                    Connection connection = MysqlConnectionContext.connection(dataSource);
                    Statement statement = connection.createStatement()) {
                statement.execute("""
                        create table if not exists mvcc_segment_publication_state_v1 (
                          singleton_id tinyint not null primary key,
                          base_commit_ts bigint not null,
                          next_commit_ts bigint not null,
                          published_ts bigint not null,
                          safe_point bigint not null,
                          schema_version smallint not null,
                          constraint mvcc_segment_state_version_check check (schema_version = 3)
                        ) engine=InnoDB
                        """);
                statement.execute("""
                        create table if not exists mvcc_publication_segment_v1 (
                          segment_start bigint not null primary key,
                          segment_end bigint not null,
                          owner_id varchar(128) collate utf8mb4_0900_bin not null,
                          reservation_key varchar(128) collate utf8mb4_0900_bin not null unique,
                          lease_millis bigint not null,
                          generation bigint not null,
                          expires_at bigint not null,
                          status varchar(16) not null,
                          created_at bigint not null,
                          index mvcc_segment_expiry (status, expires_at, segment_start),
                          index mvcc_segment_owner (owner_id, status, segment_end),
                          constraint mvcc_segment_range_check check (
                            segment_end >= segment_start and generation > 0 and lease_millis > 0
                            and status in ('ACTIVE', 'SEALED'))
                        ) engine=InnoDB
                        """);
                statement.execute("""
                        create table if not exists mvcc_publication_segment_slot_v1 (
                          commit_ts bigint not null primary key,
                          segment_start bigint not null,
                          tx_id bigint null unique,
                          status varchar(16) not null,
                          expires_at bigint null,
                          index mvcc_segment_slot_status (status, expires_at, commit_ts),
                          index mvcc_segment_slot_range (segment_start, commit_ts),
                          constraint mvcc_segment_slot_status_check check (
                            status in ('PENDING', 'COMMITTED', 'ABORTED')
                            and (status <> 'COMMITTED' or tx_id is not null)
                            and (tx_id is null or expires_at is not null)),
                          constraint mvcc_segment_slot_parent_fk foreign key (segment_start)
                            references mvcc_publication_segment_v1(segment_start)
                            on delete restrict on update restrict
                        ) engine=InnoDB
                        """);
                assertSchemaReady(connection);
                MysqlConnectionContext.inTransaction(dataSource, transaction -> {
                    try (
                            PreparedStatement state = transaction.prepareStatement("""
                                    select base_commit_ts, next_commit_ts, published_ts, safe_point,
                                           schema_version
                                    from mvcc_segment_publication_state_v1 where singleton_id = 1 for update
                                    """);
                            ResultSet row = state.executeQuery()) {
                        if (row.next()) {
                            if (row.getLong(1) != baseCommitTs || row.getLong(4) > row.getLong(3)
                                    || row.getLong(3) > row.getLong(2) || row.getLong(2) < baseCommitTs
                                    || row.getInt(5) != 3) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "existing segment timeline is incompatible", false, null);
                            }
                            return null;
                        }
                    }
                    try (
                            PreparedStatement orphaned = transaction.prepareStatement("""
                                    select (select count(*) from mvcc_publication_segment_v1)
                                         + (select count(*) from mvcc_publication_segment_slot_v1)
                                    """);
                            ResultSet row = orphaned.executeQuery()) {
                        if (!row.next() || row.getLong(1) != 0) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "segment state missing while durable rows remain", false, null);
                        }
                    }
                    try (PreparedStatement insert = transaction.prepareStatement("""
                            insert into mvcc_segment_publication_state_v1
                              (singleton_id, base_commit_ts, next_commit_ts, published_ts,
                               safe_point, schema_version)
                            values (1, ?, ?, ?, 0, 3)
                            """)) {
                        insert.setLong(1, baseCommitTs);
                        insert.setLong(2, baseCommitTs);
                        insert.setLong(3, baseCommitTs);
                        insert.executeUpdate();
                    }
                    return null;
                });
            } catch (final SQLException ex) {
                throw storageError("initialize isolated segment ledger failed", ex);
            }
            return null;
        });
    }

    /**
     * Refuses to follow old experiments with missing idempotent keys or empty slot table schema at startup.
     *
     * @param connection connection
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    void assertSchemaReady(final Connection connection) throws SQLException {
        try (
                PreparedStatement state = connection.prepareStatement("""
                        select count(*) from information_schema.columns
                        where table_schema = database()
                          and table_name = 'mvcc_segment_publication_state_v1'
                          and column_name = 'schema_version'
                        """);
                ResultSet row = state.executeQuery()) {
            if (!row.next() || row.getLong(1) != 1) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment timeline lacks current schema version", false, null);
            }
        }
        try (
                PreparedStatement version = connection.prepareStatement("""
                        select schema_version from mvcc_segment_publication_state_v1 where singleton_id = 1
                        """);
                ResultSet row = version.executeQuery()) {
            if (row.next() && row.getInt(1) != 3) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment timeline uses an incompatible slot protocol version", false, null);
            }
        }
        try (
                PreparedStatement columns = connection.prepareStatement("""
                        select count(*) from information_schema.columns
                        where table_schema = database()
                          and table_name = 'mvcc_publication_segment_v1'
                          and column_name in ('reservation_key', 'lease_millis')
                        """);
                ResultSet row = columns.executeQuery()) {
            if (!row.next() || row.getLong(1) != 2) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment table lacks durable reservation identity", false, null);
            }
        }
        try (
                PreparedStatement key = connection.prepareStatement("""
                        select count(*) from information_schema.statistics
                        where table_schema = database()
                          and table_name = 'mvcc_publication_segment_v1'
                          and column_name = 'reservation_key' and non_unique = 0
                        """);
                ResultSet row = key.executeQuery()) {
            if (!row.next() || row.getLong(1) < 1) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment reservation key is not unique", false, null);
            }
        }
        try (
                PreparedStatement slot = connection.prepareStatement("""
                        select is_nullable from information_schema.columns
                        where table_schema = database()
                          and table_name = 'mvcc_publication_segment_slot_v1'
                          and column_name = 'tx_id'
                        """);
                ResultSet row = slot.executeQuery()) {
            if (!row.next() || !"YES".equals(row.getString(1))) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment slot cannot represent an unclaimed placeholder", false, null);
            }
        }
    }

    /**
     * One control transaction simultaneously pushes the upper boundary, durable storage owner and all PENDING slots.
     *
     * @param ownerId owner id
     * @param reservationKey reservation key
     * @param size size
     * @param leaseMillis lease millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationSegmentLease reserveSegment(final String ownerId, final String reservationKey,
                                                  final int size, final long leaseMillis) {
        requireOwner(ownerId);
        requireOwner(reservationKey);
        if (size < 1 || size > 4096 || leaseMillis < 1 || leaseMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid segment reservation limits");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long lower;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select next_commit_ts from mvcc_segment_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segment timeline has not been initialized", false, null);
                    }
                    lower = row.getLong(1);
                }
                if (lower >= Long.MAX_VALUE - size) {
                    throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                            "segment timestamp namespace exhausted", false, null);
                }
                long now = databaseNow(connection);
                long expiresAt = Math.addExact(now, leaseMillis);
                long first = lower + 1;
                long last = lower + size;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                update mvcc_segment_publication_state_v1
                                set next_commit_ts = ? where singleton_id = 1
                                """);
                        PreparedStatement owner = connection.prepareStatement("""
                                insert into mvcc_publication_segment_v1
                                  (segment_start, segment_end, owner_id, reservation_key, lease_millis,
                                   generation, expires_at, status, created_at)
                                values (?, ?, ?, ?, ?, 1, ?, 'ACTIVE', ?)
                                """);
                        PreparedStatement slot = connection.prepareStatement("""
                                insert into mvcc_publication_segment_slot_v1
                                  (commit_ts, segment_start, tx_id, status, expires_at)
                                values (?, ?, null, 'PENDING', null)
                                """)) {
                    state.setLong(1, last);
                    if (state.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segment timeline changed during reservation", false, null);
                    }
                    owner.setLong(1, first);
                    owner.setLong(2, last);
                    owner.setString(3, ownerId);
                    owner.setString(4, reservationKey);
                    owner.setLong(5, leaseMillis);
                    owner.setLong(6, expiresAt);
                    owner.setLong(7, now);
                    owner.executeUpdate();
                    for (int offset = 0; offset < size; offset++) {
                        slot.setLong(1, first + offset);
                        slot.setLong(2, first);
                        slot.addBatch();
                    }
                    slot.executeBatch();
                }
                return new PublicationSegmentLease(first, last, ownerId, 1, expiresAt);
            });
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == 1062) {
                return replayReservation(ownerId, reservationKey, size, leaseMillis, ex);
            }
            throw storageError("reserve publication segment failed", ex);
        }
    }

    /**
     * Only replay range when requested to hit the key; identity or budget changes cannot quietly allocate new slots.
     *
     * @param ownerId owner id
     * @param reservationKey reservation key
     * @param size size
     * @param leaseMillis lease millis
     * @param original original
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private PublicationSegmentLease replayReservation(final String ownerId, final String reservationKey,
                                                      final int size, final long leaseMillis, final SQLException original) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select segment_start, segment_end, owner_id, generation, expires_at,
                               status, lease_millis,
                               cast(unix_timestamp(current_timestamp(3))*1000 as signed) as database_now
                        from mvcc_publication_segment_v1 where reservation_key = ?
                        """)) {
            query.setString(1, reservationKey);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "segment reservation key is absent after duplicate conflict", false, null, original);
                }
                long first = row.getLong("segment_start");
                long last = row.getLong("segment_end");
                if (!ownerId.equals(row.getString("owner_id")) || last - first + 1 != size
                        || row.getLong("lease_millis") != leaseMillis) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segment reservation replay changed durable request", false, null);
                }
                if (!"ACTIVE".equals(row.getString("status"))
                        || row.getLong("expires_at") <= row.getLong("database_now")) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "segment reservation already expired or sealed", false, null);
                }
                return new PublicationSegmentLease(first, last, ownerId,
                        row.getLong("generation"), row.getLong("expires_at"));
            }
        } catch (final SQLException ex) {
            throw storageError("replay publication segment reservation failed", ex);
        }
    }

    /**
     * Releasing at current algebra and database hours does not allow the old homeer algebra to continue to acquire slot decision rights.
     *
     * @param lease lease
     * @param leaseMillis lease millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationSegmentLease renewSegment(final PublicationSegmentLease lease, final long leaseMillis) {
        Objects.requireNonNull(lease, "lease");
        if (leaseMillis < 1 || leaseMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid segment renewal period");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                LockedSegment locked = lockSegment(connection, lease.firstTs(), true);
                long now = databaseNow(connection);
                requireCurrentOwner(lease, locked, now);
                PublicationSegmentLease durable = locked.lease();
                if (durable.generation() == Long.MAX_VALUE) {
                    throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                            "segment generation exhausted", false, null);
                }
                long expiresAt = Math.max(durable.expiresAtMillis(), Math.addExact(now, leaseMillis));
                try (PreparedStatement renew = connection.prepareStatement("""
                        update mvcc_publication_segment_v1
                        set generation = ?, expires_at = ? where segment_start = ? and status = 'ACTIVE'
                        """)) {
                    renew.setLong(1, durable.generation() + 1);
                    renew.setLong(2, expiresAt);
                    renew.setLong(3, lease.firstTs());
                    if (renew.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                "segment renewal lost owner row", false, null);
                    }
                }
                return new PublicationSegmentLease(lease.firstTs(), lease.lastTs(), lease.ownerId(),
                        durable.generation() + 1, expiresAt);
            });
        } catch (final SQLException ex) {
            throw storageError("renew publication segment failed", ex);
        }
    }

    /**
     * Resume lease to read the current durable own algebra at unknown time, and the closed segment no longer returns the available receipt.
     *
     * @param firstTs first ts
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public Optional<PublicationSegmentLease> segmentLease(final long firstTs) {
        if (firstTs < 1) {
            throw new IllegalArgumentException("invalid segment start");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select segment_end, owner_id, generation, expires_at, status
                        from mvcc_publication_segment_v1 where segment_start = ?
                        """)) {
            query.setLong(1, firstTs);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !"ACTIVE".equals(row.getString("status"))) {
                    return Optional.empty();
                }
                return Optional.of(new PublicationSegmentLease(firstTs, row.getLong("segment_end"),
                        row.getString("owner_id"), row.getLong("generation"),
                        row.getLong("expires_at")));
            }
        } catch (final SQLException ex) {
            throw storageError("read current segment owner lease failed", ex);
        }
    }

    /**
     * Owner actively seals the tailings; the caller must stop the local distribution and wait for the end of the journey decision.
     *
     * @param lease lease
     * @return the operation result
     */
    public int sealOwnedSegment(final PublicationSegmentLease lease) {
        Objects.requireNonNull(lease, "lease");
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                LockedSegment locked = lockSegment(connection, lease.firstTs(), true);
                if (locked != null && "SEALED".equals(locked.status())
                        && locked.lease().ownerId().equals(lease.ownerId())
                        && lease.generation() < Long.MAX_VALUE
                        && locked.lease().generation() == lease.generation() + 1) {
                    return 0;
                }
                long now = databaseNow(connection);
                requireCurrentOwner(lease, locked, now);
                return sealLockedSegment(connection, lease.firstTs(), lease.lastTs());
            });
        } catch (final SQLException ex) {
            throw storageError("seal owned publication segment failed", ex);
        }
    }

    /**
     * The receiver can only fencing after database lease expires and then seal the slot where pending is still held.
     *
     * @param segmentStart segment start
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public int sealExpiredSegment(final long segmentStart) {
        if (segmentStart < 1) {
            throw new IllegalArgumentException("invalid segment start");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                LockedSegment locked = lockSegment(connection, segmentStart, true);
                if (locked == null) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segment does not exist", false, null);
                }
                if ("SEALED".equals(locked.status())) {
                    return 0;
                }
                if (locked.lease().expiresAtMillis() > databaseNow(connection)) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "segment owner lease is still valid", false, null);
                }
                return sealLockedSegment(connection, segmentStart, locked.lease().lastTs());
            });
        } catch (final SQLException ex) {
            throw storageError("seal expired publication segment failed", ex);
        }
    }

    /**
     * Rotation takes over the expired bounded amount and returns the number of pending -time slots in this round.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public int sealExpiredSegments(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid expired segment scan limit");
        }
        List<Long> expired = new ArrayList<>(limit);
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select segment_start from mvcc_publication_segment_v1
                        where status = 'ACTIVE'
                          and expires_at <= cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        order by expires_at, segment_start limit ?
                        """)) {
            query.setInt(1, limit);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    expired.add(rows.getLong(1));
                }
            }
        } catch (final SQLException ex) {
            throw storageError("scan expired publication segments failed", ex);
        }
        int sealedSlots = 0;
        for (long start : expired) {
            sealedSlots += sealExpiredSegment(start);
        }
        return sealedSlots;
    }

    /**
     * Platoon locks the time line and pushes only the continuous terminal state prefix of durable storage.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public long advancePublished(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segment publication limit");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                long upper;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts, next_commit_ts from mvcc_segment_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segment timeline has not been initialized", false, null);
                    }
                    published = row.getLong(1);
                    upper = row.getLong(2);
                }
                long next = published;
                try (PreparedStatement slots = connection.prepareStatement("""
                        select commit_ts, status from mvcc_publication_segment_slot_v1
                        where commit_ts > ? order by commit_ts limit ?
                        """)) {
                    slots.setLong(1, published);
                    slots.setInt(2, limit);
                    try (ResultSet rows = slots.executeQuery()) {
                        while (rows.next()) {
                            if (rows.getLong(1) != next + 1) {
                                break;
                            }
                            String status = rows.getString(2);
                            if ("PENDING".equals(status)) {
                                break;
                            }
                            if (!"COMMITTED".equals(status) && !"ABORTED".equals(status)) {
                                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                        "segment slot has unknown terminal status", false, null);
                            }
                            next++;
                        }
                    }
                }
                if (next > upper) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segment publication exceeds reserved upper bound", false, null);
                }
                if (next != published) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_segment_publication_state_v1
                            set published_ts = ? where singleton_id = 1
                            """)) {
                        update.setLong(1, next);
                        update.executeUpdate();
                    }
                }
                return next;
            });
        } catch (final SQLException ex) {
            throw storageError("advance segment publication failed", ex);
        }
    }

    /**
     * Read durable to release watermark for the coordinator determination of confirmed commit.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public long currentPublished() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select published_ts from mvcc_segment_publication_state_v1 where singleton_id = 1
                        """);
                ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment timeline has not been initialized", false, null);
            }
            return row.getLong(1);
        } catch (final SQLException ex) {
            throw storageError("read segment published watermark failed", ex);
        }
    }

    /**
     * Snapshot common state line fence with start of segment, advance safe point by the oldest lease and unfinalized metadata.
     *
     * @return the operation result
     */
    public long advanceSafePoint() {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                long published;
                long current;
                try (
                        PreparedStatement state = connection.prepareStatement("""
                                select published_ts, safe_point from mvcc_segment_publication_state_v1
                                where singleton_id = 1 for update
                                """);
                        ResultSet row = state.executeQuery()) {
                    if (!row.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented safe point state is missing", false, null);
                    }
                    published = row.getLong(1);
                    current = row.getLong(2);
                }
                if (current > published) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented safe point already exceeds publication", false, null);
                }
                Long oldest = null;
                try (
                        PreparedStatement protectedSnapshots = connection.prepareStatement("""
                                select min(read_ts) from (
                                  select read_ts from mvcc_snapshot_lease_v2
                                    where expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed)
                                  union all
                                  select read_ts from mvcc_coordinator_snapshot_lease_v1
                                    where expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed)
                                  union all
                                  select t.start_ts as read_ts from mvcc_txn t
                                    left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                                    where f.tx_id is null
                                ) active_snapshots
                                """);
                        ResultSet row = protectedSnapshots.executeQuery()) {
                    if (row.next() && row.getObject(1) != null) {
                        oldest = row.getLong(1);
                    }
                }
                long candidate = oldest == null ? published
                        : Math.min(published, oldest > 0 ? oldest - 1 : 0);
                long next = Math.max(current, candidate);
                if (next != current) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_segment_publication_state_v1
                            set safe_point = ? where singleton_id = 1
                            """)) {
                        update.setLong(1, next);
                        update.executeUpdate();
                    }
                }
                return next;
            });
        } catch (final SQLException ex) {
            throw storageError("advance segmented safe point failed", ex);
        }
    }

    /**
     * Returns a fraction safe point that is already durable storage and can only move in one-way motion.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public long currentSafePoint() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select safe_point from mvcc_segment_publication_state_v1 where singleton_id = 1
                        """);
                ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented safe point state is missing", false, null);
            }
            return row.getLong(1);
        } catch (final SQLException ex) {
            throw storageError("read segmented safe point failed", ex);
        }
    }

    /**
     * Reuse verify primary key order to delete and avoid a reverse death lock on the expired index and foreground release.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    public int expireSnapshots(final int limit) {
        return leaseGc.expireSnapshots(limit);
    }

    /**
     * The coordinator reads the lease also in primary order bounded cleanup.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    public int expireCoordinatorLeases(final int limit) {
        return leaseGc.expireCoordinatorLeases(limit);
    }

    /**
     * Allows to delete central metadata in the next GC phase first by certifying and deleting terminal state slots with Finalized materialization.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param expiredBeforeMillis expired before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    public int purgeTerminalSlots(final long safePoint, final long expiredBeforeMillis, final int limit) {
        checkSegmentPurge(safePoint, expiredBeforeMillis, limit);
        List<Long> candidates = new ArrayList<>();
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select slot.commit_ts from mvcc_publication_segment_slot_v1 slot
                        join mvcc_publication_segment_v1 segment
                          on segment.segment_start = slot.segment_start
                        where slot.commit_ts < ? and slot.status in ('COMMITTED', 'ABORTED')
                          and slot.expires_at < ? and segment.status = 'SEALED'
                          and (slot.tx_id is null or exists (
                            select 1 from mvcc_txn_finalized f
                            where f.tx_id = slot.tx_id and f.finalized_at < ?))
                        order by slot.commit_ts limit ?
                        """)) {
            query.setLong(1, safePoint);
            query.setLong(2, expiredBeforeMillis);
            query.setLong(3, expiredBeforeMillis);
            query.setInt(4, limit);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    candidates.add(rows.getLong(1));
                }
            }
            int removed = 0;
            try (PreparedStatement delete = connection.prepareStatement("""
                    delete from mvcc_publication_segment_slot_v1
                    where commit_ts = ? and commit_ts < ?
                      and status in ('COMMITTED', 'ABORTED') and expires_at < ?
                      and (tx_id is null or exists (
                        select 1 from mvcc_txn_finalized f
                        where f.tx_id = mvcc_publication_segment_slot_v1.tx_id
                          and f.finalized_at < ?))
                    """)) {
                for (long commitTs : candidates) {
                    delete.setLong(1, commitTs);
                    delete.setLong(2, safePoint);
                    delete.setLong(3, expiredBeforeMillis);
                    delete.setLong(4, expiredBeforeMillis);
                    removed += delete.executeUpdate();
                }
            }
            return removed;
        } catch (final SQLException ex) {
            throw storageError("purge segmented terminal slots failed", ex);
        }
    }

    /**
     * Cleans up Finalized metadata only after collection in the corresponding slot to avoid seeing isolated terminal state.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param finalizedBeforeMillis finalized before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    public int purgeFinalizedMetadata(final long safePoint, final long finalizedBeforeMillis, final int limit) {
        checkSegmentPurge(safePoint, finalizedBeforeMillis, limit);
        List<Long> candidates = new ArrayList<>();
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select t.tx_id from mvcc_txn t
                        join mvcc_txn_finalized f on f.tx_id = t.tx_id
                        left join mvcc_snapshot_lease_v2 lease on lease.tx_id = t.tx_id
                        left join mvcc_snapshot_lease legacy on legacy.tx_id = t.tx_id
                        where lease.tx_id is null and legacy.tx_id is null
                          and f.finalized_at < ? and t.start_ts < ?
                          and (t.status = 'ABORTED' or
                               (t.status = 'COMMITTED' and t.commit_ts < ?))
                          and not exists (select 1 from mvcc_publication_segment_slot_v1 slot
                                          where slot.tx_id = t.tx_id)
                        order by t.tx_id limit ?
                        """)) {
            query.setLong(1, finalizedBeforeMillis);
            query.setLong(2, safePoint);
            query.setLong(3, safePoint);
            query.setInt(4, limit);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    candidates.add(rows.getLong(1));
                }
            }
            int removed = 0;
            try (PreparedStatement delete = connection.prepareStatement("""
                    delete from mvcc_txn where tx_id = ?
                      and not exists (select 1 from mvcc_publication_segment_slot_v1 slot
                                      where slot.tx_id = mvcc_txn.tx_id)
                    """)) {
                for (long txId : candidates) {
                    delete.setLong(1, txId);
                    removed += delete.executeUpdate();
                }
            }
            return removed;
        } catch (final SQLException ex) {
            throw storageError("purge finalized segmented metadata failed", ex);
        }
    }

    /**
     * All slots have been deleted and owner is earlier than safe point and collection at the time of retention.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param expiredBeforeMillis expired before millis
     * @param limit maximum number of items
     * @return the operation result
     */
    public int purgeSealedSegments(final long safePoint, final long expiredBeforeMillis, final int limit) {
        checkSegmentPurge(safePoint, expiredBeforeMillis, limit);
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement delete = connection.prepareStatement("""
                        delete from mvcc_publication_segment_v1
                        where status = 'SEALED' and segment_end < ? and expires_at < ?
                          and not exists (select 1 from mvcc_publication_segment_slot_v1 slot
                                          where slot.segment_start = mvcc_publication_segment_v1.segment_start)
                        order by segment_start limit ?
                        """)) {
            delete.setLong(1, safePoint);
            delete.setLong(2, expiredBeforeMillis);
            delete.setInt(3, limit);
            return delete.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("purge sealed publication segments failed", ex);
        }
    }

    /**
     * Rejects the manual terminal state collection request to push more than durable safe point.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param expiredBeforeMillis expired before millis
     * @param limit maximum number of items
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private void checkSegmentPurge(final long safePoint, final long expiredBeforeMillis, final int limit) {
        if (safePoint < 0 || expiredBeforeMillis < 0 || limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segmented GC limits");
        }
        if (safePoint > currentSafePoint()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented GC cannot exceed durable safe point", false, null);
        }
    }

    /**
     * Atomic acquires new watermark and independent snapshot lease, in a activated segment mode, and RC/RR starts with the released prefix.
     *
     * @param isolationLevel global transaction isolation level
     * @param timeoutMillis timeout millis
     * @param persistMetadata persist metadata
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BeginReservation begin(final IsolationLevel isolationLevel, final long timeoutMillis, final boolean persistMetadata) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        if (timeoutMillis < 1 || timeoutMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid segmented snapshot timeout");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                long readTs;
                long deadline;
                try (PreparedStatement state = connection.prepareStatement("""
                        select published_ts, safe_point,
                               cast(unix_timestamp(current_timestamp(3))*1000 as signed) + ? as deadline
                        from mvcc_segment_publication_state_v1 where singleton_id = 1 for share
                        """)) {
                    state.setLong(1, timeoutMillis);
                    try (ResultSet row = state.executeQuery()) {
                        if (!row.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "segmented snapshot state missing", false, null);
                        }
                        readTs = row.getLong("published_ts");
                        if (readTs < row.getLong("safe_point")) {
                            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                                    "segmented snapshot is older than safe point", false, null);
                        }
                        deadline = row.getLong("deadline");
                    }
                }
                long txId;
                try (PreparedStatement lease = connection.prepareStatement("""
                        insert into mvcc_snapshot_lease_v2(read_ts, expires_at) values (?, ?)
                        """, Statement.RETURN_GENERATED_KEYS)) {
                    lease.setLong(1, readTs);
                    lease.setLong(2, deadline);
                    lease.executeUpdate();
                    try (ResultSet generated = lease.getGeneratedKeys()) {
                        if (!generated.next()) {
                            throw new SQLException("segmented snapshot lease did not return transaction id");
                        }
                        txId = generated.getLong(1);
                    }
                }
                if (persistMetadata) {
                    try (PreparedStatement metadata = connection.prepareStatement("""
                            insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                            values (?, ?, ?, 'ACTIVE', null, '')
                            """)) {
                        metadata.setLong(1, txId);
                        metadata.setLong(2, readTs);
                        metadata.setString(3, isolationLevel.name());
                        metadata.executeUpdate();
                    }
                }
                return new BeginReservation(txId, readTs, deadline, persistMetadata);
            });
        } catch (final SQLException ex) {
            throw storageError("begin segmented publication snapshot failed", ex);
        }
    }

    /**
     * Transaction released independent lease and continued to protect recovery and GC without materialization centre records.
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
            throw storageError("release segmented snapshot failed", ex);
        }
    }

    /**
     * Read the new slot terminal state by real transaction identifier, an empty slot will not be mistaken for a single transaction.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<PublicationEntry> findByTxId(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select commit_ts, status, expires_at
                        from mvcc_publication_segment_slot_v1 where tx_id = ?
                        """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapTransactionEntry(row, txId));
            }
        } catch (final SQLException ex) {
            throw storageError("read segmented decision by transaction id failed", ex);
        }
    }

    /**
     * Press commit timestamp bounded to find unfinalized new slot terminal state, cursor to end with a rollback by the caller.
     *
     * @param afterCommitTs after commit ts
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<PublicationEntry> findUnfinalizedTerminal(final long afterCommitTs, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segmented terminal scan limit");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select s.tx_id, s.commit_ts, s.status, s.expires_at
                        from mvcc_publication_segment_slot_v1 s
                        left join mvcc_txn_finalized f on f.tx_id = s.tx_id
                        where s.commit_ts > ? and s.status in ('COMMITTED', 'ABORTED')
                          and s.tx_id is not null and f.tx_id is null
                        order by s.commit_ts limit ?
                        """)) {
            query.setLong(1, afterCommitTs);
            query.setInt(2, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<PublicationEntry> found = new ArrayList<>();
                while (rows.next()) {
                    found.add(mapTransactionEntry(rows, rows.getLong("tx_id")));
                }
                return List.copyOf(found);
            }
        } catch (final SQLException ex) {
            throw storageError("scan unfinalized segmented decisions failed", ex);
        }
    }

    /**
     * Snapshot lease was found to be invalid from central metadata and no new slot terminal state.
     *
     * @param nowMillis now millis
     * @param afterTxId after tx id
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<Long> findExpiredUndecided(final long nowMillis, final long afterTxId, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segmented undecided scan limit");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select t.tx_id from mvcc_txn t
                        left join mvcc_snapshot_lease_v2 lease on lease.tx_id = t.tx_id
                        left join mvcc_publication_segment_slot_v1 slot on slot.tx_id = t.tx_id
                        left join mvcc_txn_finalized finalized on finalized.tx_id = t.tx_id
                        where t.tx_id > ? and finalized.tx_id is null and slot.tx_id is null
                          and (lease.tx_id is null or lease.expires_at <= ?)
                          and t.status in ('ACTIVE', 'PREPARING', 'ABORTED', 'COMMITTED')
                        order by t.tx_id limit ?
                        """)) {
            query.setLong(1, afterTxId);
            query.setLong(2, nowMillis);
            query.setInt(3, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<Long> found = new ArrayList<>();
                while (rows.next()) {
                    found.add(rows.getLong(1));
                }
                return List.copyOf(found);
            }
        } catch (final SQLException ex) {
            throw storageError("scan expired undecided segmented transactions failed", ex);
        }
    }

    /**
     * Reads the current transaction snapshot lease deadline, which is recorded by the recovery Path Check Center when missing.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public OptionalLong snapshotExpiry(final long txId) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select expires_at from mvcc_snapshot_lease_v2 where tx_id = ?
                        """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? OptionalLong.of(row.getLong(1)) : OptionalLong.empty();
            }
        } catch (final SQLException ex) {
            throw storageError("read segmented snapshot deadline failed", ex);
        }
    }

    /**
     * Finds the local candidate who is due or whose own is fencing, press timestamp bounded.
     *
     * @param nowMillis now millis
     * @param afterCommitTs after commit ts
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public List<PublicationEntry> findRecoverableLocalPending(final long nowMillis, final long afterCommitTs, final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local pending scan limit");
        }
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select slot.tx_id, slot.commit_ts, slot.status, slot.expires_at
                        from mvcc_publication_segment_slot_v1 slot
                        join mvcc_publication_segment_v1 owner
                          on owner.segment_start = slot.segment_start
                        where slot.commit_ts > ? and slot.status = 'PENDING' and slot.tx_id is not null
                          and (slot.expires_at <= ? or owner.status = 'SEALED')
                        order by slot.commit_ts limit ?
                        """)) {
            query.setLong(1, afterCommitTs);
            query.setLong(2, nowMillis);
            query.setInt(3, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<PublicationEntry> found = new ArrayList<>();
                while (rows.next()) {
                    found.add(mapTransactionEntry(rows, rows.getLong("tx_id")));
                }
                return List.copyOf(found);
            }
        } catch (final SQLException ex) {
            throw storageError("scan recoverable segmented local candidates failed", ex);
        }
    }

    /**
     * Single recovery still needs to be rechecked for expiry or own fincing to avoid scanned and wrongly taking over the valid candidate.
     *
     * @param commitTs durable commit timestamp
     * @param nowMillis now millis
     * @return the operation result
     */
    public boolean localPendingRecoverable(final long commitTs, final long nowMillis) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement query = connection.prepareStatement("""
                        select slot.status, slot.tx_id, slot.expires_at, owner.status as owner_status
                        from mvcc_publication_segment_slot_v1 slot
                        join mvcc_publication_segment_v1 owner
                          on owner.segment_start = slot.segment_start
                        where slot.commit_ts = ?
                        """)) {
            query.setLong(1, commitTs);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !"PENDING".equals(row.getString("status"))
                        || row.getObject("tx_id") == null || row.getObject("expires_at") == null) {
                    return false;
                }
                return row.getLong("expires_at") <= nowMillis
                        || "SEALED".equals(row.getString("owner_status"));
            }
        } catch (final SQLException ex) {
            throw storageError("check segmented local candidate recovery eligibility failed", ex);
        }
    }

    /**
     * The real transaction slot must be with an transaction identifier and a cut-off period, and the empty slot cannot be misused by the recovery.
     *
     * @param row row
     * @param txId transaction identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private PublicationEntry mapTransactionEntry(final ResultSet row, final long txId) throws SQLException {
        Object deadline = row.getObject("expires_at");
        PublicationEntryStatus status = PublicationEntryStatus.valueOf(row.getString("status"));
        if (txId == 0 || deadline == null || row.getLong("expires_at") < 1) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented transaction slot lacks durable identity", false, txId);
        }
        return new PublicationEntry(txId, row.getLong("commit_ts"), status, row.getLong("expires_at"));
    }

    /**
     * Check owner, snapshot and participant in the same control transaction, then atomic goes to centre decision and slot terminal state.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @return the operation result
     */
    public long decideCommitted(final PublicationSegmentLease segment, final long commitTs, final long txId,
                                final long deadlineMillis, final Set<String> participants) {
        decideCommittedReadTs(segment, commitTs, txId, deadlineMillis, participants, null);
        return commitTs;
    }

    /**
     * Only the centre and slot atomic commit successfully returned an unconstructed materialization receipt.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @return the operation result
     */
    public CommittedProof decideCommittedWithProof(final PublicationSegmentLease segment, final long commitTs,
                                                   final long txId, final long startTs, final long deadlineMillis,
                                                   final Set<String> participants) {
        // The decision validate and receipt configurations shall use the same immutable participant collection.
        Set<String> stableParticipants = Set.copyOf(participants);
        long confirmedStart = decideCommittedReadTs(segment, commitTs, txId,
                deadlineMillis, stableParticipants, startTs);
        return new CommittedProof(txId, confirmedStart, commitTs, stableParticipants, dataSource);
    }

    /**
     * Uniform validate new decision and identical terminal state replay, returns the start of central verification watermark.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @param expectedStartTs expected start ts
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private long decideCommittedReadTs(final PublicationSegmentLease segment, final long commitTs, final long txId,
                                       final long deadlineMillis, final Set<String> participants,
                                       final Long expectedStartTs) {
        Objects.requireNonNull(segment, "segment");
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (commitTs < segment.firstTs() || commitTs > segment.lastTs()
                || deadlineMillis < 1 || participants.isEmpty()) {
            throw new IllegalArgumentException("invalid segmented commit request");
        }
        String encoded = participants.stream().sorted().collect(Collectors.joining(","));
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                LockedSegment owner = lockSegment(connection, segment.firstTs(), false);
                if (owner == null || owner.lease().lastTs() != segment.lastTs()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented commit has no matching owner range", false, txId);
                }
                String slotStatus;
                Long slotTxId;
                Long slotDeadline;
                try (PreparedStatement slot = connection.prepareStatement("""
                        select tx_id, status, expires_at from mvcc_publication_segment_slot_v1
                        where commit_ts = ? and segment_start = ? for update
                        """)) {
                    slot.setLong(1, commitTs);
                    slot.setLong(2, segment.firstTs());
                    try (ResultSet row = slot.executeQuery()) {
                        if (!row.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "segmented commit slot is missing", false, txId);
                        }
                        slotTxId = row.getObject("tx_id") == null ? null : row.getLong("tx_id");
                        slotStatus = row.getString("status");
                        slotDeadline = row.getObject("expires_at") == null ? null : row.getLong("expires_at");
                    }
                }
                if ("COMMITTED".equals(slotStatus)) {
                    long replayStart = verifyCommittedReplay(connection, txId, commitTs, deadlineMillis,
                            encoded, slotTxId, slotDeadline);
                    if (expectedStartTs != null && replayStart != expectedStartTs) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented proof replay changed transaction start", false, txId);
                    }
                    return replayStart;
                }
                if ("ABORTED".equals(slotStatus)) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "segmented time slot was aborted", false, txId);
                }
                if (!"PENDING".equals(slotStatus) || slotTxId != null) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented time slot has incompatible owner state", false, txId);
                }
                long now = databaseNow(connection);
                requireCurrentOwner(segment, owner, now);
                long readTs;
                try (PreparedStatement lease = connection.prepareStatement("""
                        select read_ts, expires_at from mvcc_snapshot_lease_v2
                        where tx_id = ? for share
                        """)) {
                    lease.setLong(1, txId);
                    try (ResultSet row = lease.executeQuery()) {
                        if (!row.next() || row.getLong("expires_at") != deadlineMillis
                                || row.getLong("expires_at") <= now) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "segmented commit snapshot lease expired", false, txId);
                        }
                        readTs = row.getLong("read_ts");
                    }
                }
                if (expectedStartTs != null && readTs != expectedStartTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented proof start differs from durable snapshot", false, txId);
                }
                try (
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'COMMITTED', commit_ts = ?
                                where tx_id = ? and start_ts = ? and binary participants = binary ?
                                  and status in ('ACTIVE', 'PREPARING') and commit_ts is null
                                  and start_ts < ?
                                """);
                        PreparedStatement slot = connection.prepareStatement("""
                                update mvcc_publication_segment_slot_v1
                                set tx_id = ?, status = 'COMMITTED', expires_at = ?
                                where commit_ts = ? and segment_start = ?
                                  and tx_id is null and status = 'PENDING'
                                """)) {
                    metadata.setLong(1, commitTs);
                    metadata.setLong(2, txId);
                    metadata.setLong(3, readTs);
                    metadata.setString(4, encoded);
                    metadata.setLong(5, commitTs);
                    if (metadata.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented decision differs from durable transaction", false, txId);
                    }
                    slot.setLong(1, txId);
                    slot.setLong(2, deadlineMillis);
                    slot.setLong(3, commitTs);
                    slot.setLong(4, segment.firstTs());
                    if (slot.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented commit lost reserved slot", false, txId);
                    }
                }
                return readTs;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented decision outcome requires durable lookup before retry", false, txId, ex);
        }
    }

    /**
     * Multiple X-Os in the same own segment atomic decision in one control transaction failed to complete rollback.
     *
     * @param requests requests
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public void decideCommittedBatch(final List<PublicationSegmentCommit> requests) {
        if (requests.isEmpty()) {
            return;
        }
        if (requests.size() == 1) {
            PublicationSegmentCommit request = requests.getFirst();
            decideCommitted(request.segment(), request.commitTs(), request.txId(),
                    request.deadlineMillis(), request.participants());
            return;
        }
        if (requests.size() > 64) {
            throw new IllegalArgumentException("segmented decision batch exceeds 64 transactions");
        }
        PublicationSegmentLease segment = Objects.requireNonNull(requests.getFirst(), "request").segment();
        List<PublicationSegmentCommit> byCommitTs = new ArrayList<>(requests);
        byCommitTs.sort(Comparator.comparingLong(PublicationSegmentCommit::commitTs));
        Set<Long> identities = new HashSet<>();
        for (int index = 0; index < byCommitTs.size(); index++) {
            PublicationSegmentCommit request = Objects.requireNonNull(byCommitTs.get(index), "request");
            if (!segment.equals(request.segment()) || !identities.add(request.txId())
                    || index > 0 && byCommitTs.get(index - 1).commitTs() == request.commitTs()) {
                throw new IllegalArgumentException("segmented batch mixes owner generations or duplicates identities");
            }
        }
        List<PublicationSegmentCommit> byTxId = new ArrayList<>(byCommitTs);
        byTxId.sort(Comparator.comparingLong(PublicationSegmentCommit::txId));
        String placeholders = String.join(",", Collections.nCopies(byCommitTs.size(), "?"));
        try {
            MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                LockedSegment durable = lockSegment(connection, segment.firstTs(), false);
                requireCurrentOwner(segment, durable, databaseNow(connection));
                Map<Long, BatchSlot> slots = new HashMap<>(byCommitTs.size());
                String slotSql = "select commit_ts, segment_start, tx_id, status "
                        + "from mvcc_publication_segment_slot_v1 force index(primary) "
                        + "where commit_ts in (" + placeholders + ") order by commit_ts for update";
                try (PreparedStatement query = connection.prepareStatement(slotSql)) {
                    for (int index = 0; index < byCommitTs.size(); index++) {
                        query.setLong(index + 1, byCommitTs.get(index).commitTs());
                    }
                    try (ResultSet rows = query.executeQuery()) {
                        while (rows.next()) {
                            Object txId = rows.getObject("tx_id");
                            slots.put(rows.getLong("commit_ts"), new BatchSlot(
                                    rows.getLong("segment_start"), rows.getString("status"),
                                    txId == null ? null : rows.getLong("tx_id")));
                        }
                    }
                }
                for (PublicationSegmentCommit request : byCommitTs) {
                    BatchSlot slot = slots.get(request.commitTs());
                    if (slot == null || slot.segmentStart() != segment.firstTs()
                            || !"PENDING".equals(slot.status()) || slot.txId() != null) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "segmented batch has a decided or missing slot; resolve by transaction id",
                                false, request.txId());
                    }
                }
                Map<Long, BatchLease> leases = new HashMap<>(byTxId.size());
                String leaseSql = "select tx_id, read_ts, expires_at from mvcc_snapshot_lease_v2"
                        + " force index(primary) where tx_id in (" + placeholders + ")"
                        + " order by tx_id for share";
                try (PreparedStatement query = connection.prepareStatement(leaseSql)) {
                    for (int index = 0; index < byTxId.size(); index++) {
                        query.setLong(index + 1, byTxId.get(index).txId());
                    }
                    try (ResultSet rows = query.executeQuery()) {
                        while (rows.next()) {
                            leases.put(rows.getLong("tx_id"),
                                    new BatchLease(rows.getLong("read_ts"), rows.getLong("expires_at")));
                        }
                    }
                }
                Map<Long, BatchMetadata> records = new HashMap<>(byTxId.size());
                String metadataSql = "select tx_id, start_ts, status, commit_ts, participants"
                        + " from mvcc_txn force index(primary) where tx_id in (" + placeholders + ")"
                        + " order by tx_id for update";
                try (PreparedStatement query = connection.prepareStatement(metadataSql)) {
                    for (int index = 0; index < byTxId.size(); index++) {
                        query.setLong(index + 1, byTxId.get(index).txId());
                    }
                    try (ResultSet rows = query.executeQuery()) {
                        while (rows.next()) {
                            Object committed = rows.getObject("commit_ts");
                            records.put(rows.getLong("tx_id"), new BatchMetadata(
                                    rows.getLong("start_ts"), rows.getString("status"),
                                    committed == null ? null : rows.getLong("commit_ts"),
                                    rows.getString("participants")));
                        }
                    }
                }
                long now = databaseNow(connection);
                for (PublicationSegmentCommit request : byCommitTs) {
                    BatchLease lease = leases.get(request.txId());
                    BatchMetadata record = records.get(request.txId());
                    String participants = request.participants().stream().sorted().collect(Collectors.joining(","));
                    if (lease == null || lease.expiresAtMillis() != request.deadlineMillis()
                            || lease.expiresAtMillis() <= now
                            || record == null || record.startTs() != lease.readTs()
                            || record.commitTs() != null
                            || !Set.of("ACTIVE", "PREPARING").contains(record.status())
                            || !participants.equals(record.participants())
                            || request.commitTs() <= lease.readTs()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented batch differs from durable lease or participants",
                                false, request.txId());
                    }
                }
                String metadataUpdateSql = "update mvcc_txn set status='COMMITTED', commit_ts=case tx_id "
                        + "when ? then ? ".repeat(byTxId.size())
                        + "end where tx_id in (" + placeholders + ")"
                        + " and status in ('ACTIVE','PREPARING') and commit_ts is null";
                String slotUpdateSql = "update mvcc_publication_segment_slot_v1 force index(primary)"
                        + " set tx_id=case commit_ts " + "when ? then ? ".repeat(byCommitTs.size())
                        + "end, status='COMMITTED', expires_at=case commit_ts "
                        + "when ? then ? ".repeat(byCommitTs.size())
                        + "end where commit_ts in (" + placeholders + ")"
                        + " and segment_start=? and status='PENDING' and tx_id is null";
                try (
                        PreparedStatement metadata = connection.prepareStatement(metadataUpdateSql);
                        PreparedStatement slot = connection.prepareStatement(slotUpdateSql)) {
                    int parameter = 1;
                    for (PublicationSegmentCommit request : byTxId) {
                        metadata.setLong(parameter++, request.txId());
                        metadata.setLong(parameter++, request.commitTs());
                    }
                    for (PublicationSegmentCommit request : byTxId) {
                        metadata.setLong(parameter++, request.txId());
                    }
                    if (metadata.executeUpdate() != byTxId.size()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented batch lost center metadata", false, null);
                    }
                    parameter = 1;
                    for (PublicationSegmentCommit request : byCommitTs) {
                        slot.setLong(parameter++, request.commitTs());
                        slot.setLong(parameter++, request.txId());
                    }
                    for (PublicationSegmentCommit request : byCommitTs) {
                        slot.setLong(parameter++, request.commitTs());
                        slot.setLong(parameter++, request.deadlineMillis());
                    }
                    for (PublicationSegmentCommit request : byCommitTs) {
                        slot.setLong(parameter++, request.commitTs());
                    }
                    slot.setLong(parameter, segment.firstTs());
                    if (slot.executeUpdate() != byCommitTs.size()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "segmented batch lost reserved slots", false, null);
                    }
                }
                return null;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented batch decision outcome requires per-transaction lookup", false, null, ex);
        }
    }

    /**
     * Single shard Physical commit atomic identifies the PENDING slot and center PREPARING candidate.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participant participant
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public long claimLocalPending(final PublicationSegmentLease segment, final long commitTs, final long txId,
                                  final long deadlineMillis, final String participant) {
        Objects.requireNonNull(segment, "segment");
        TransactionMetadataValidation.validateParticipant(txId, participant);
        if (commitTs < segment.firstTs() || commitTs > segment.lastTs() || deadlineMillis < 1) {
            throw new IllegalArgumentException("invalid segmented local candidate");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                LockedSegment owner = lockSegment(connection, segment.firstTs(), false);
                if (owner == null || owner.lease().lastTs() != segment.lastTs()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local candidate has no matching segment owner", false, txId);
                }
                String status;
                Long slotTxId;
                Long slotDeadline;
                try (PreparedStatement slot = connection.prepareStatement("""
                        select tx_id, status, expires_at from mvcc_publication_segment_slot_v1
                        where commit_ts = ? and segment_start = ? for update
                        """)) {
                    slot.setLong(1, commitTs);
                    slot.setLong(2, segment.firstTs());
                    try (ResultSet row = slot.executeQuery()) {
                        if (!row.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local candidate slot is missing", false, txId);
                        }
                        slotTxId = row.getObject("tx_id") == null ? null : row.getLong("tx_id");
                        slotDeadline = row.getObject("expires_at") == null ? null : row.getLong("expires_at");
                        status = row.getString("status");
                    }
                }
                if ("ABORTED".equals(status)) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "local candidate slot was aborted", false, txId);
                }
                if (slotTxId != null) {
                    if (slotTxId != txId || !Objects.equals(slotDeadline, deadlineMillis)
                            || !("PENDING".equals(status) || "COMMITTED".equals(status))) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local candidate replay changed durable slot identity", false, txId);
                    }
                    verifyLocalCandidate(connection, txId, commitTs, participant,
                            "COMMITTED".equals(status));
                    return commitTs;
                }
                if (!"PENDING".equals(status)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local candidate requires an unclaimed pending slot", false, txId);
                }
                long now = databaseNow(connection);
                requireCurrentOwner(segment, owner, now);
                long readTs;
                try (PreparedStatement lease = connection.prepareStatement("""
                        select read_ts, expires_at from mvcc_snapshot_lease_v2
                        where tx_id = ? for share
                        """)) {
                    lease.setLong(1, txId);
                    try (ResultSet row = lease.executeQuery()) {
                        if (!row.next() || row.getLong("expires_at") != deadlineMillis
                                || row.getLong("expires_at") <= now) {
                            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                    "local candidate snapshot lease expired", false, txId);
                        }
                        readTs = row.getLong("read_ts");
                    }
                }
                try (
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = 'PREPARING', commit_ts = ?
                                where tx_id = ? and start_ts = ? and binary participants = binary ?
                                  and status = 'ACTIVE' and commit_ts is null and start_ts < ?
                                """);
                        PreparedStatement slot = connection.prepareStatement("""
                                update mvcc_publication_segment_slot_v1
                                set tx_id = ?, expires_at = ?
                                where commit_ts = ? and segment_start = ?
                                  and status = 'PENDING' and tx_id is null
                                """)) {
                    metadata.setLong(1, commitTs);
                    metadata.setLong(2, txId);
                    metadata.setLong(3, readTs);
                    metadata.setString(4, participant);
                    metadata.setLong(5, commitTs);
                    if (metadata.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local candidate differs from durable single participant", false, txId);
                    }
                    slot.setLong(1, txId);
                    slot.setLong(2, deadlineMillis);
                    slot.setLong(3, commitTs);
                    slot.setLong(4, segment.firstTs());
                    if (slot.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local candidate lost unclaimed slot", false, txId);
                    }
                }
                return commitTs;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local candidate outcome requires durable lookup", false, txId, ex);
        }
    }

    /**
     * Only central records of the same participant and candidate time are accepted when a response is lost.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @param committed committed
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void verifyLocalCandidate(final Connection connection, final long txId, final long commitTs,
                                      final String participant, final boolean committed) throws SQLException {
        try (PreparedStatement metadata = connection.prepareStatement("""
                select start_ts, status, commit_ts, participants from mvcc_txn
                where tx_id = ? for update
                """)) {
            metadata.setLong(1, txId);
            try (ResultSet row = metadata.executeQuery()) {
                if (!row.next() || row.getLong("start_ts") >= commitTs
                        || row.getObject("commit_ts") == null || row.getLong("commit_ts") != commitTs
                        || !participant.equals(row.getString("participants"))
                        || !(committed ? "COMMITTED".equals(row.getString("status"))
                                : "PREPARING".equals(row.getString("status")))) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local candidate replay differs from durable metadata", false, txId);
                }
            }
        }
    }

    /**
     * Shard has been commit proof to replace the local candidate with the slot atomic by COMMITTED.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @return the operation result
     */
    public boolean completeLocalCommitAfterFencing(final ParticipantRecord local, final long commitTs, final String participant) {
        return completeLocalTerminal(local, commitTs, participant, true);
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @return the operation result
     */
    public boolean completeLocalAbortAfterFencing(final ParticipantRecord local, final long commitTs, final String participant) {
        return completeLocalTerminal(local, commitTs, participant, false);
    }

    /**
     * Owner durable local terminal state can still be the center of decision in the same time slot even if it is expired or sealed.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @param committed committed
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private boolean completeLocalTerminal(final ParticipantRecord local, final long commitTs,
                                          final String participant, final boolean committed) {
        Objects.requireNonNull(local, "local");
        TransactionMetadataValidation.validateParticipant(local.txId(), participant);
        if (commitTs < 1 || (committed && (local.status() != TxStatus.COMMITTED
                || !Objects.equals(local.commitTs(), commitTs)))
                || (!committed && (local.status() != TxStatus.ABORTED || local.commitTs() != null))) {
            throw new IllegalArgumentException("local terminal proof differs from segmented candidate");
        }
        long txId = local.txId();
        String terminal = committed ? "COMMITTED" : "ABORTED";
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                // Candidate durable accepted; sealing only suspends unrecognized kiosks and locks the main key directly to replace terminal state after an owner has lapsed.
                String slotStatus;
                try (PreparedStatement slot = connection.prepareStatement("""
                        select tx_id, status, expires_at from mvcc_publication_segment_slot_v1
                        where commit_ts = ? for update
                        """)) {
                    slot.setLong(1, commitTs);
                    try (ResultSet row = slot.executeQuery()) {
                        if (!row.next() || row.getObject("tx_id") == null
                                || row.getLong("tx_id") != txId || row.getObject("expires_at") == null) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "local terminal proof has no matching claimed slot", false, txId);
                        }
                        slotStatus = row.getString("status");
                    }
                }
                if (terminal.equals(slotStatus)) {
                    verifyCompletedLocalCandidate(connection, local, commitTs, participant, terminal);
                    return false;
                }
                if (!"PENDING".equals(slotStatus)) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "local terminal proof conflicts with another slot decision", false, txId);
                }
                try (
                        PreparedStatement metadata = connection.prepareStatement("""
                                update mvcc_txn set status = ?, commit_ts = ?
                                where tx_id = ? and start_ts = ? and binary participants = binary ?
                                  and status = 'PREPARING' and commit_ts = ?
                                """);
                        PreparedStatement slot = connection.prepareStatement("""
                                update mvcc_publication_segment_slot_v1 set status = ?
                                where commit_ts = ? and tx_id = ? and status = 'PENDING'
                                """)) {
                    metadata.setString(1, terminal);
                    if (committed) {
                        metadata.setLong(2, commitTs);
                    } else {
                        metadata.setNull(2, java.sql.Types.BIGINT);
                    }
                    metadata.setLong(3, txId);
                    metadata.setLong(4, local.startTs());
                    metadata.setString(5, participant);
                    metadata.setLong(6, commitTs);
                    if (metadata.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "local terminal proof differs from center candidate", false, txId);
                    }
                    slot.setString(1, terminal);
                    slot.setLong(2, commitTs);
                    slot.setLong(3, txId);
                    if (slot.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "local terminal completion lost claimed slot", false, txId);
                    }
                }
                return true;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local terminal completion outcome requires durable proof lookup", false, txId, ex);
        }
    }

    /**
     * Local Supplement decision only accepts the centre terminal state for the same start, participant and timestamp when the decision response is lost.
     *
     * @param connection connection
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @param terminal terminal
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void verifyCompletedLocalCandidate(final Connection connection, final ParticipantRecord local,
                                               final long commitTs, final String participant,
                                               final String terminal) throws SQLException {
        try (PreparedStatement metadata = connection.prepareStatement("""
                select start_ts, status, commit_ts, participants from mvcc_txn
                where tx_id = ? for update
                """)) {
            metadata.setLong(1, local.txId());
            try (ResultSet row = metadata.executeQuery()) {
                if (!row.next() || row.getLong("start_ts") != local.startTs()
                        || !terminal.equals(row.getString("status"))
                        || !participant.equals(row.getString("participants"))
                        || ("COMMITTED".equals(terminal)
                                && (row.getObject("commit_ts") == null || row.getLong("commit_ts") != commitTs))
                        || ("ABORTED".equals(terminal) && row.getObject("commit_ts") != null)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "local terminal replay differs from center decision", false, local.txId());
                }
            }
        }
    }

    /**
     * Transaction, which has completed X-Fencing, replaces its own slot and centre record atomic with ABORTED.
     *
     * @param segment segment
     * @param commitTs durable commit timestamp
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public boolean abortReservedSlot(final PublicationSegmentLease segment, final long commitTs,
                                     final long txId, final long deadlineMillis) {
        Objects.requireNonNull(segment, "segment");
        if (commitTs < segment.firstTs() || commitTs > segment.lastTs() || deadlineMillis < 1) {
            throw new IllegalArgumentException("invalid segmented abort request");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                LockedSegment owner = lockSegment(connection, segment.firstTs(), false);
                if (owner == null || owner.lease().lastTs() != segment.lastTs()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented abort has no matching owner range", false, txId);
                }
                String status;
                Long slotTxId;
                Long slotDeadline;
                try (PreparedStatement slot = connection.prepareStatement("""
                        select tx_id, status, expires_at from mvcc_publication_segment_slot_v1
                        where commit_ts = ? and segment_start = ? for update
                        """)) {
                    slot.setLong(1, commitTs);
                    slot.setLong(2, segment.firstTs());
                    try (ResultSet row = slot.executeQuery()) {
                        if (!row.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "segmented abort slot is missing", false, txId);
                        }
                        slotTxId = row.getObject("tx_id") == null ? null : row.getLong("tx_id");
                        slotDeadline = row.getObject("expires_at") == null ? null : row.getLong("expires_at");
                        status = row.getString("status");
                    }
                }
                if ("COMMITTED".equals(status)) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "segmented slot already has a commit decision", false, txId);
                }
                if ("ABORTED".equals(status)) {
                    if (!Objects.equals(slotTxId, txId) || !Objects.equals(slotDeadline, deadlineMillis)) {
                        throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                                "segmented abort replay has no matching transaction identity", false, txId);
                    }
                    requireAbortedMetadata(connection, txId);
                    return false;
                }
                if (!"PENDING".equals(status) || slotTxId != null) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented abort slot has incompatible durable state", false, txId);
                }
                requireCurrentOwner(segment, owner, databaseNow(connection));
                String metadataStatus;
                try (PreparedStatement metadata = connection.prepareStatement("""
                        select start_ts, status, commit_ts from mvcc_txn
                        where tx_id = ? for update
                        """)) {
                    metadata.setLong(1, txId);
                    try (ResultSet row = metadata.executeQuery()) {
                        if (!row.next() || row.getLong("start_ts") >= commitTs
                                || row.getObject("commit_ts") != null) {
                            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                    "segmented abort lacks a durable undecided transaction", false, txId);
                        }
                        metadataStatus = row.getString("status");
                    }
                }
                if (!Set.of("ACTIVE", "PREPARING", "ABORTED").contains(metadataStatus)) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "segmented abort raced a terminal transaction", false, txId);
                }
                try (PreparedStatement lease = connection.prepareStatement("""
                        select expires_at from mvcc_snapshot_lease_v2 where tx_id = ? for share
                        """)) {
                    lease.setLong(1, txId);
                    try (ResultSet row = lease.executeQuery()) {
                        if (row.next() && row.getLong(1) != deadlineMillis) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "segmented abort changed transaction deadline", false, txId);
                        }
                    }
                }
                if (!"ABORTED".equals(metadataStatus)) {
                    try (PreparedStatement abort = connection.prepareStatement("""
                            update mvcc_txn set status = 'ABORTED'
                            where tx_id = ? and status in ('ACTIVE', 'PREPARING') and commit_ts is null
                            """)) {
                        abort.setLong(1, txId);
                        if (abort.executeUpdate() != 1) {
                            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                    "segmented abort lost transaction metadata", false, txId);
                        }
                    }
                }
                try (PreparedStatement slot = connection.prepareStatement("""
                        update mvcc_publication_segment_slot_v1
                        set tx_id = ?, status = 'ABORTED', expires_at = ?
                        where commit_ts = ? and segment_start = ?
                          and tx_id is null and status = 'PENDING'
                        """)) {
                    slot.setLong(1, txId);
                    slot.setLong(2, deadlineMillis);
                    slot.setLong(3, commitTs);
                    slot.setLong(4, segment.firstTs());
                    if (slot.executeUpdate() != 1) {
                        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                                "segmented abort lost reserved slot", false, txId);
                    }
                }
                return true;
            });
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented abort outcome requires durable lookup", false, txId, ex);
        }
    }

    /**
     * Repeat abort to accept only the same centre ABORTED decision as durable storage.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private void requireAbortedMetadata(final Connection connection, final long txId) throws SQLException {
        try (PreparedStatement metadata = connection.prepareStatement("""
                select status, commit_ts from mvcc_txn where tx_id = ? for update
                """)) {
            metadata.setLong(1, txId);
            try (ResultSet row = metadata.executeQuery()) {
                if (!row.next() || !"ABORTED".equals(row.getString("status"))
                        || row.getObject("commit_ts") != null) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented abort replay differs from durable metadata", false, txId);
                }
            }
        }
    }

    /**
     * Only identical durable centers will be accepted when the response is lost.
     *
     * @param connection connection
     * @param txId transaction identifier
     * @param commitTs durable commit timestamp
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @param slotTxId slot tx id
     * @param slotDeadline slot deadline
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private long verifyCommittedReplay(final Connection connection, final long txId, final long commitTs,
                                       final long deadlineMillis, final String participants,
                                       final Long slotTxId, final Long slotDeadline) throws SQLException {
        if (!Objects.equals(slotTxId, txId) || !Objects.equals(slotDeadline, deadlineMillis)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented slot replay differs from durable identity", false, txId);
        }
        try (PreparedStatement metadata = connection.prepareStatement("""
                select start_ts, status, commit_ts, participants from mvcc_txn
                where tx_id = ? for update
                """)) {
            metadata.setLong(1, txId);
            try (ResultSet row = metadata.executeQuery()) {
                if (!row.next() || !"COMMITTED".equals(row.getString("status"))
                        || row.getObject("commit_ts") == null || row.getLong("commit_ts") != commitTs
                        || !participants.equals(row.getString("participants"))
                        || row.getLong("start_ts") >= commitTs) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "segmented replay differs from durable metadata", false, txId);
                }
                return row.getLong("start_ts");
            }
        }
    }

    /**
     * Locks the durable owner range and calls to continue the validate algebra in the same database transaction.
     *
     * @param connection connection
     * @param firstTs first ts
     * @param exclusive exclusive
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private LockedSegment lockSegment(final Connection connection, final long firstTs, final boolean exclusive) throws SQLException {
        String sql = "select segment_end, owner_id, generation, expires_at, status"
                + " from mvcc_publication_segment_v1 where segment_start = ?"
                + (exclusive ? " for update" : " for share");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, firstTs);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                return new LockedSegment(new PublicationSegmentLease(firstTs, row.getLong(1), row.getString(2),
                        row.getLong(3), row.getLong(4)), row.getString(5));
            }
        }
    }

    /**
     * Current algebras, downer and unexpired conditions must be established at the same time and cannot be renewed after expiry.
     *
     * @param expected expected
     * @param locked locked
     * @param now now
     * @throws MvccException if the operation cannot complete
     */
    private void requireCurrentOwner(final PublicationSegmentLease expected, final LockedSegment locked, final long now) {
        if (locked == null || !"ACTIVE".equals(locked.status())
                || locked.lease().lastTs() != expected.lastTs()
                || !locked.lease().ownerId().equals(expected.ownerId())
                || locked.lease().generation() != expected.generation()
                || locked.lease().expiresAtMillis() <= now) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "segment owner generation or lease was fenced", false, null);
        }
    }

    /**
     * Add algebra when the owner's exclusion lock has been obtained and atomic aborts a slot without terminal state.
     *
     * @param connection connection
     * @param firstTs first ts
     * @param lastTs last ts
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private int sealLockedSegment(final Connection connection, final long firstTs, final long lastTs) throws SQLException {
        try (
                PreparedStatement fence = connection.prepareStatement("""
                        update mvcc_publication_segment_v1
                        set status = 'SEALED', generation = generation + 1
                        where segment_start = ? and status = 'ACTIVE'
                          and generation < 9223372036854775807
                        """);
                PreparedStatement abort = connection.prepareStatement("""
                        update mvcc_publication_segment_slot_v1 force index(primary)
                        set status = 'ABORTED',
                            expires_at = coalesce(expires_at,
                              cast(unix_timestamp(current_timestamp(3))*1000 as signed))
                        where commit_ts >= ? and commit_ts <= ?
                          and segment_start = ? and status = 'PENDING' and tx_id is null
                        """)) {
            fence.setLong(1, firstTs);
            if (fence.executeUpdate() != 1) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "segment was already sealed", false, null);
            }
            abort.setLong(1, firstTs);
            abort.setLong(2, lastTs);
            abort.setLong(3, firstTs);
            return abort.executeUpdate();
        }
    }

    /**
     * Read database clocks to avoid cross-host local clocks participating in durable lease determinations.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private long databaseNow(final Connection connection) throws SQLException {
        try (
                PreparedStatement clock = connection.prepareStatement("""
                        select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                        """);
                ResultSet row = clock.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("database clock returned no timestamp");
            }
            return row.getLong(1);
        }
    }

    /**
     * Own identity must be stable and within database column budget.
     *
     * @param ownerId owner id
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void requireOwner(final String ownerId) {
        if (ownerId == null || !ownerId.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new IllegalArgumentException("invalid segment owner identity");
        }
    }

    /**
     * Keeps the database abnormal chain for callers to distinguish between unknown results.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private MvccException storageError(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * Creates only a successful commit Center transaction account book, which is used to avoid repeating the same decision query.
     */
    public static final class CommittedProof {

        private final long txId;

        private final long startTs;

        private final long commitTs;

        private final Set<String> participants;

        private final DataSource authority;

        /**
         * Limit the construction entrance to avoid unset timestamp being used as durable decision.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param participants participants
         * @param authority authority
         */
        private CommittedProof(final long txId, final long startTs, final long commitTs, final Set<String> participants,
                               final DataSource authority) {
            this.txId = txId;
            this.startTs = startTs;
            this.commitTs = commitTs;
            this.participants = Set.copyOf(participants);
            this.authority = authority;
        }

        /**
         * Returns commit transaction identifier.
         *
         * @return the operation result
         */
        public long txId() {
            return txId;
        }

        /**
         * Returns decision verified start watermark.
         *
         * @return the operation result
         */
        public long startTs() {
            return startTs;
        }

        /**
         * Returns an commit slot.
         *
         * @return the operation result
         */
        public long commitTs() {
            return commitTs;
        }

        /**
         * Returns a fixed complete participation in the shard collection at decision.
         *
         * @return the operation result
         */
        public Set<String> participants() {
            return participants;
        }

        /**
         * Only the original foreground control authority bound shard is allowed to use this receipt.
         *
         * @param store store
         * @return the operation result
         */
        public boolean belongsTo(final TransactionMetadataStore store) {
            return store instanceof MysqlTransactionMetadataStore mysql && mysql.usesDataSource(authority);
        }
    }

    /**
     * Lock the owner receipt and durable terminal state read at a time.
     *
     * @param lease lease
     * @param status status
     */
    private record LockedSegment(PublicationSegmentLease lease, String status) {
    }

    /**
     * Batch locks on a read item snapshot lease.
     *
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     */
    private record BatchLease(long readTs, long expiresAtMillis) {
    }

    /**
     * Batch locks a read-out pending center transaction.
     *
     * @param startTs transaction start timestamp
     * @param status status
     * @param commitTs durable commit timestamp
     * @param participants participants
     */
    private record BatchMetadata(long startTs, String status, Long commitTs, String participants) {
    }

    /**
     * Batch locks a readable slot identity.
     *
     * @param segmentStart segment start
     * @param status status
     * @param txId transaction identifier
     */
    private record BatchSlot(long segmentStart, String status, Long txId) {
    }
}

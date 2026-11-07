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

/** 分段账本的预留、owner fencing、中心终态与连续发布持久层。 */
public final class MysqlSegmentPublicationLedger {
    private final DataSource dataSource;
    private final MysqlPublicationLedger leaseGc;

    /** 仅由成功提交中心事务的账本创建，分片用其避免重复查询同一决议。 */
    public static final class CommittedProof {
        private final long txId;
        private final long startTs;
        private final long commitTs;
        private final Set<String> participants;
        private final DataSource authority;

        /** 限制构造入口，避免未落盘的调用方时间戳被当作持久决议。 */
        private CommittedProof(long txId, long startTs, long commitTs, Set<String> participants,
                               DataSource authority) {
            this.txId = txId;
            this.startTs = startTs;
            this.commitTs = commitTs;
            this.participants = Set.copyOf(participants);
            this.authority = authority;
        }

        /** 返回已提交事务编号。 */
        public long txId() { return txId; }

        /** 返回决议核验过的开始水位。 */
        public long startTs() { return startTs; }

        /** 返回已提交时间槽。 */
        public long commitTs() { return commitTs; }

        /** 返回决议时已固定的完整参与分片集合。 */
        public Set<String> participants() { return participants; }

        /** 只允许原前台控制权威绑定的分片使用本凭据。 */
        public boolean belongsTo(TransactionMetadataStore store) {
            return store instanceof MysqlTransactionMetadataStore mysql && mysql.usesDataSource(authority);
        }
    }

    /** 一次锁定读取的 owner 凭据和持久终态。 */
    private record LockedSegment(PublicationSegmentLease lease, String status) {
    }

    /** 批次锁定读取的一笔快照租约。 */
    private record BatchLease(long readTs, long expiresAtMillis) {
    }

    /** 批次锁定读取的一笔未决中心事务。 */
    private record BatchMetadata(long startTs, String status, Long commitTs, String participants) {
    }

    /** 批次锁定读取的一条分段槽身份。 */
    private record BatchSlot(long segmentStart, String status, Long txId) {
    }

    /** 绑定同一个 MySQL 控制数据库，调用方仍须提供独立的后台连接预算。 */
    public MysqlSegmentPublicationLedger(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.leaseGc = new MysqlPublicationLedger(dataSource);
    }

    /** 前台分段事务与中心元数据必须共享同一数据源实例。 */
    public boolean sharesMetadataAuthority(TransactionMetadataStore metadataStore) {
        return metadataStore instanceof MysqlTransactionMetadataStore mysql
                && mysql.usesDataSource(dataSource);
    }

    /** 在旧账本已停止写入且全部排空后，建立可崩溃续建的数据库级分段模式栅栏。 */
    public void activateFromLedger() {
        new MysqlSegmentProtocolActivator(dataSource, this).activate();
    }

    /** 业务入口启动前同时验证持久模式与旧状态表的物理写栅栏。 */
    public void assertActivated() {
        new MysqlSegmentProtocolActivator(dataSource, this).assertActivated();
    }

    /** 只初始化隔离版本表，重复调用保留原时间线并拒绝改变初始上界。 */
    public void initializeIsolated(long baseCommitTs) {
        if (baseCommitTs < 0 || baseCommitTs == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid segment base timestamp");
        }
        MysqlConnectionContext.withPublicationLock(dataSource, 10, () -> {
            try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
                    try (PreparedStatement state = transaction.prepareStatement("""
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
                    try (PreparedStatement orphaned = transaction.prepareStatement("""
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
            } catch (SQLException ex) {
                throw storageError("initialize isolated segment ledger failed", ex);
            }
            return null;
        });
    }

    /** 启动时拒绝沿用缺少幂等键或可空占位槽的旧实验表结构。 */
    void assertSchemaReady(Connection connection) throws SQLException {
        try (PreparedStatement state = connection.prepareStatement("""
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
        try (PreparedStatement version = connection.prepareStatement("""
                select schema_version from mvcc_segment_publication_state_v1 where singleton_id = 1
                """);
             ResultSet row = version.executeQuery()) {
            if (row.next() && row.getInt(1) != 3) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment timeline uses an incompatible slot protocol version", false, null);
            }
        }
        try (PreparedStatement columns = connection.prepareStatement("""
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
        try (PreparedStatement key = connection.prepareStatement("""
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
        try (PreparedStatement slot = connection.prepareStatement("""
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

    /** 一笔控制事务同时推进上界、持久化 owner 和全部 PENDING 占位槽。 */
    public PublicationSegmentLease reserveSegment(String ownerId, String reservationKey,
                                                  int size, long leaseMillis) {
        requireOwner(ownerId);
        requireOwner(reservationKey);
        if (size < 1 || size > 4096 || leaseMillis < 1 || leaseMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid segment reservation limits");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long lower;
                try (PreparedStatement state = connection.prepareStatement("""
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
                try (PreparedStatement state = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 1062) {
                return replayReservation(ownerId, reservationKey, size, leaseMillis, ex);
            }
            throw storageError("reserve publication segment failed", ex);
        }
    }

    /** 唯一请求键命中时只重放原范围；身份或预算改变不能悄悄分配新槽。 */
    private PublicationSegmentLease replayReservation(String ownerId, String reservationKey,
                                                       int size, long leaseMillis, SQLException original) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("replay publication segment reservation failed", ex);
        }
    }

    /** 按当前代数和数据库时钟续租，旧 owner 代数无法继续取得时间槽决议权。 */
    public PublicationSegmentLease renewSegment(PublicationSegmentLease lease, long leaseMillis) {
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
        } catch (SQLException ex) {
            throw storageError("renew publication segment failed", ex);
        }
    }

    /** 续租响应未知时读取当前持久 owner 代数，已封存分段不再返回可用凭据。 */
    public Optional<PublicationSegmentLease> segmentLease(long firstTs) {
        if (firstTs < 1) {
            throw new IllegalArgumentException("invalid segment start");
        }
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("read current segment owner lease failed", ex);
        }
    }

    /** owner 主动封存尾槽；调用方须先停止本地分配并等待在途决议结束。 */
    public int sealOwnedSegment(PublicationSegmentLease lease) {
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
        } catch (SQLException ex) {
            throw storageError("seal owned publication segment failed", ex);
        }
    }

    /** 接管者只能在数据库租约到期后 fencing，再封存仍未决议的占位槽。 */
    public int sealExpiredSegment(long segmentStart) {
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
        } catch (SQLException ex) {
            throw storageError("seal expired publication segment failed", ex);
        }
    }

    /** 轮转接管有界数量的到期分段，返回本轮封存的未决时间槽数。 */
    public int sealExpiredSegments(int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid expired segment scan limit");
        }
        List<Long> expired = new ArrayList<>(limit);
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("scan expired publication segments failed", ex);
        }
        int sealedSlots = 0;
        for (long start : expired) {
            sealedSlots += sealExpiredSegment(start);
        }
        return sealedSlots;
    }

    /** 排他锁住时间线后仅推进已持久化的连续终态前缀。 */
    public long advancePublished(int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segment publication limit");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                long published;
                long upper;
                try (PreparedStatement state = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            throw storageError("advance segment publication failed", ex);
        }
    }

    /** 读取持久发布水位，供等待已确认提交的协调器判断可见边界。 */
    public long currentPublished() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement query = connection.prepareStatement("""
                     select published_ts from mvcc_segment_publication_state_v1 where singleton_id = 1
                     """);
             ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segment timeline has not been initialized", false, null);
            }
            return row.getLong(1);
        } catch (SQLException ex) {
            throw storageError("read segment published watermark failed", ex);
        }
    }

    /** 与分段开始快照共用状态行栅栏，按最老租约和未 finalized 元数据推进安全点。 */
    public long advanceSafePoint() {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                MysqlProtocolEpoch.requireSegment(connection);
                long published;
                long current;
                try (PreparedStatement state = connection.prepareStatement("""
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
                try (PreparedStatement protectedSnapshots = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            throw storageError("advance segmented safe point failed", ex);
        }
    }

    /** 返回已经持久化且只能单调前进的分段安全点。 */
    public long currentSafePoint() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement query = connection.prepareStatement("""
                     select safe_point from mvcc_segment_publication_state_v1 where singleton_id = 1
                     """);
             ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented safe point state is missing", false, null);
            }
            return row.getLong(1);
        } catch (SQLException ex) {
            throw storageError("read segmented safe point failed", ex);
        }
    }

    /** 复用已验证的主键顺序删除，避免到期索引与前台释放发生反向死锁。 */
    public int expireSnapshots(int limit) {
        return leaseGc.expireSnapshots(limit);
    }

    /** 协调者读租约同样按主键顺序有界清理。 */
    public int expireCoordinatorLeases(int limit) {
        return leaseGc.expireCoordinatorLeases(limit);
    }

    /** 先凭 finalized 物化证明删终态槽，再允许下一 GC 阶段删除中心元数据。 */
    public int purgeTerminalSlots(long safePoint, long expiredBeforeMillis, int limit) {
        checkSegmentPurge(safePoint, expiredBeforeMillis, limit);
        List<Long> candidates = new ArrayList<>();
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("purge segmented terminal slots failed", ex);
        }
    }

    /** 只在对应分段槽先回收后清理 finalized 元数据，避免恢复器看见孤立终态。 */
    public int purgeFinalizedMetadata(long safePoint, long finalizedBeforeMillis, int limit) {
        checkSegmentPurge(safePoint, finalizedBeforeMillis, limit);
        List<Long> candidates = new ArrayList<>();
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("purge finalized segmented metadata failed", ex);
        }
    }

    /** 所有槽已删除且 owner 早于安全点与保留期限时回收分段头。 */
    public int purgeSealedSegments(long safePoint, long expiredBeforeMillis, int limit) {
        checkSegmentPurge(safePoint, expiredBeforeMillis, limit);
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("purge sealed publication segments failed", ex);
        }
    }

    /** 拒绝强推超过持久安全点的手工终态回收请求。 */
    private void checkSegmentPurge(long safePoint, long expiredBeforeMillis, int limit) {
        if (safePoint < 0 || expiredBeforeMillis < 0 || limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segmented GC limits");
        }
        if (safePoint > currentSafePoint()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented GC cannot exceed durable safe point", false, null);
        }
    }

    /** 在已激活分段模式下原子取得新水位和独立快照租约，RC/RR 均从已发布前缀开始。 */
    public BeginReservation begin(IsolationLevel isolationLevel, long timeoutMillis, boolean persistMetadata) {
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
        } catch (SQLException ex) {
            throw storageError("begin segmented publication snapshot failed", ex);
        }
    }

    /** 已结束事务释放独立租约，未物化中心记录仍继续保护恢复和 GC。 */
    public void releaseSnapshot(long txId) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement delete = connection.prepareStatement("""
                     delete from mvcc_snapshot_lease_v2 where tx_id = ?
                     """)) {
            delete.setLong(1, txId);
            delete.executeUpdate();
        } catch (SQLException ex) {
            throw storageError("release segmented snapshot failed", ex);
        }
    }

    /** 按真实事务编号读取新槽终态，空占位槽不会被误认为一笔事务。 */
    public Optional<PublicationEntry> findByTxId(long txId) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("read segmented decision by transaction id failed", ex);
        }
    }

    /** 按提交时间戳有界发现未 finalized 的新槽终态，游标到末尾后由调用方回卷。 */
    public List<PublicationEntry> findUnfinalizedTerminal(long afterCommitTs, int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segmented terminal scan limit");
        }
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("scan unfinalized segmented decisions failed", ex);
        }
    }

    /** 从中心元数据发现快照租约已失效且没有新槽终态的未决事务。 */
    public List<Long> findExpiredUndecided(long nowMillis, long afterTxId, int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid segmented undecided scan limit");
        }
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("scan expired undecided segmented transactions failed", ex);
        }
    }

    /** 读取当前事务快照租约截止时间，缺失时由恢复路径核对中心记录。 */
    public OptionalLong snapshotExpiry(long txId) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement query = connection.prepareStatement("""
                     select expires_at from mvcc_snapshot_lease_v2 where tx_id = ?
                     """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? OptionalLong.of(row.getLong(1)) : OptionalLong.empty();
            }
        } catch (SQLException ex) {
            throw storageError("read segmented snapshot deadline failed", ex);
        }
    }

    /** 找出到期或 owner 已 fencing 的本地候选，按时间戳有界回卷。 */
    public List<PublicationEntry> findRecoverableLocalPending(long nowMillis, long afterCommitTs, int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local pending scan limit");
        }
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("scan recoverable segmented local candidates failed", ex);
        }
    }

    /** 单笔恢复仍须重新检查到期或 owner fencing，避免扫描后误接管有效候选。 */
    public boolean localPendingRecoverable(long commitTs, long nowMillis) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError("check segmented local candidate recovery eligibility failed", ex);
        }
    }

    /** 真实事务槽必须带事务编号和截止时间，空占位槽不能被恢复器误用。 */
    private PublicationEntry mapTransactionEntry(ResultSet row, long txId) throws SQLException {
        Object deadline = row.getObject("expires_at");
        PublicationEntryStatus status = PublicationEntryStatus.valueOf(row.getString("status"));
        if (txId == 0 || deadline == null || row.getLong("expires_at") < 1) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented transaction slot lacks durable identity", false, txId);
        }
        return new PublicationEntry(txId, row.getLong("commit_ts"), status, row.getLong("expires_at"));
    }

    /** 在同一控制事务中核对 owner、快照与参与者，再原子落中心决议和分段槽终态。 */
    public long decideCommitted(PublicationSegmentLease segment, long commitTs, long txId,
                                long deadlineMillis, Set<String> participants) {
        decideCommittedReadTs(segment, commitTs, txId, deadlineMillis, participants, null);
        return commitTs;
    }

    /** 只有中心与槽原子提交成功后才返回不可由外部构造的物化凭据。 */
    public CommittedProof decideCommittedWithProof(PublicationSegmentLease segment, long commitTs,
                                                   long txId, long startTs, long deadlineMillis,
                                                   Set<String> participants) {
        // 决议校验和凭据构造必须使用同一份不可变参与者集合。
        Set<String> stableParticipants = Set.copyOf(participants);
        long confirmedStart = decideCommittedReadTs(segment, commitTs, txId,
                deadlineMillis, stableParticipants, startTs);
        return new CommittedProof(txId, confirmedStart, commitTs, stableParticipants, dataSource);
    }

    /** 统一校验新决议与完全相同的终态重放，返回中心核验的开始水位。 */
    private long decideCommittedReadTs(PublicationSegmentLease segment, long commitTs, long txId,
                                       long deadlineMillis, Set<String> participants,
                                       Long expectedStartTs) {
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
                try (PreparedStatement metadata = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented decision outcome requires durable lookup before retry", false, txId, ex);
        }
    }

    /** 同一 owner 分段内多笔意向在一笔控制事务中原子决议，失败整批回滚。 */
    public void decideCommittedBatch(List<PublicationSegmentCommit> requests) {
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
                try (PreparedStatement metadata = connection.prepareStatement(metadataUpdateSql);
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented batch decision outcome requires per-transaction lookup", false, null, ex);
        }
    }

    /** 单分片物理提交前原子认领 PENDING 槽与中心 PREPARING 候选。 */
    public long claimLocalPending(PublicationSegmentLease segment, long commitTs, long txId,
                                  long deadlineMillis, String participant) {
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
                try (PreparedStatement metadata = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local candidate outcome requires durable lookup", false, txId, ex);
        }
    }

    /** 认领响应丢失时只接受同一参与者和候选时间的中心记录。 */
    private void verifyLocalCandidate(Connection connection, long txId, long commitTs,
                                      String participant, boolean committed) throws SQLException {
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

    /** 以分片已提交证明把本地候选与分段槽原子补为 COMMITTED。 */
    public boolean completeLocalCommitAfterFencing(ParticipantRecord local, long commitTs, String participant) {
        return completeLocalTerminal(local, commitTs, participant, true);
    }

    /** 以分片已中止证明把本地候选与分段槽原子补为 ABORTED。 */
    public boolean completeLocalAbortAfterFencing(ParticipantRecord local, long commitTs, String participant) {
        return completeLocalTerminal(local, commitTs, participant, false);
    }

    /** owner 即使已经过期或封存，持久本地终态仍能恢复同一时间槽的中心决议。 */
    private boolean completeLocalTerminal(ParticipantRecord local, long commitTs,
                                          String participant, boolean committed) {
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
                // 候选已持久认领；封存仅中止未认领槽，直接锁主键即可在 owner 失效后补终态。
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
                try (PreparedStatement metadata = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local terminal completion outcome requires durable proof lookup", false, txId, ex);
        }
    }

    /** 本地补决议响应丢失时只接受同一开始、参与者和时间戳的中心终态。 */
    private void verifyCompletedLocalCandidate(Connection connection, ParticipantRecord local,
                                               long commitTs, String participant,
                                               String terminal) throws SQLException {
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

    /** 已完成参与者 fencing 的事务将自身占位槽与中心记录原子改为 ABORTED。 */
    public boolean abortReservedSlot(PublicationSegmentLease segment, long commitTs,
                                     long txId, long deadlineMillis) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented abort outcome requires durable lookup", false, txId, ex);
        }
    }

    /** 重复中止只接受同一笔已持久化的中心 ABORTED 决议。 */
    private void requireAbortedMetadata(Connection connection, long txId) throws SQLException {
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

    /** 响应丢失后只接受完全相同的已持久中心决议。 */
    private long verifyCommittedReplay(Connection connection, long txId, long commitTs,
                                       long deadlineMillis, String participants,
                                       Long slotTxId, Long slotDeadline) throws SQLException {
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

    /** 用主键锁读取持久 owner 范围，调用方在同一数据库事务内继续校验代数。 */
    private LockedSegment lockSegment(Connection connection, long firstTs, boolean exclusive)
            throws SQLException {
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

    /** 当前代数、owner 和未到期条件必须同时成立，过期后不可重新续命。 */
    private void requireCurrentOwner(PublicationSegmentLease expected, LockedSegment locked, long now) {
        if (locked == null || !"ACTIVE".equals(locked.status())
                || locked.lease().lastTs() != expected.lastTs()
                || !locked.lease().ownerId().equals(expected.ownerId())
                || locked.lease().generation() != expected.generation()
                || locked.lease().expiresAtMillis() <= now) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "segment owner generation or lease was fenced", false, null);
        }
    }

    /** owner 排他锁已取得时增加代数并原子中止尚无终态的时间槽。 */
    private int sealLockedSegment(Connection connection, long firstTs, long lastTs) throws SQLException {
        try (PreparedStatement fence = connection.prepareStatement("""
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

    /** 读取数据库时钟，避免跨主机本地时钟参与持久租约判定。 */
    private long databaseNow(Connection connection) throws SQLException {
        try (PreparedStatement clock = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """);
             ResultSet row = clock.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("database clock returned no timestamp");
            }
            return row.getLong(1);
        }
    }

    /** owner 身份必须稳定且在数据库列预算内。 */
    private void requireOwner(String ownerId) {
        if (ownerId == null || !ownerId.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new IllegalArgumentException("invalid segment owner identity");
        }
    }

    /** 保留数据库异常链供调用方区分不可用与未知结果。 */
    private MvccException storageError(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }
}

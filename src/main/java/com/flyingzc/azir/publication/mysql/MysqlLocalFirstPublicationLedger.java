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

/** 本地先提交协议的隔离控制账本，预留分片专属段并以段下界保护全局快照。 */
public final class MysqlLocalFirstPublicationLedger {
    /** 跨分片意向决议在同一全局时间线中使用非物理分片段。 */
    public static final String INTENT_SEGMENT_ROUTE = "__INTENT__";
    private final DataSource dataSource;
    private volatile boolean fastPublicationReady;

    /** 分段状态先经分片安装，再允许本地提交；接管须经过持久 FENCING。 */
    public enum SegmentStatus { PREPARING, ACTIVE, FENCING, SEALED }

    /** 将持久中心段身份、目标分片及快照保护下界一起返回。 */
    public record SegmentReservation(PublicationSegmentLease lease, String shardId,
                                     long snapshotFloor, long leaseMillis, SegmentStatus status) {
        /** 不允许无分片路由、负快照下界或空状态进入控制协议。 */
        public SegmentReservation {
            Objects.requireNonNull(lease, "lease");
            Objects.requireNonNull(status, "status");
            TransactionMetadataValidation.validateParticipant(0, shardId);
            if (snapshotFloor < 0 || leaseMillis < 1) {
                throw new IllegalArgumentException("invalid local-first segment reservation");
            }
        }
    }

    /** 中心进入 FENCING 后携原代数请求目标分片写物理墓碑。 */
    public record FenceRequest(PublicationSegmentLease physicalLease, String shardId,
                               long snapshotFloor) {
        /** 接管请求仍必须保留原 owner 身份与快照下界。 */
        public FenceRequest {
            Objects.requireNonNull(physicalLease, "physicalLease");
            TransactionMetadataValidation.validateParticipant(0, shardId);
            if (snapshotFloor < 0) {
                throw new IllegalArgumentException("invalid local-first fence snapshot floor");
            }
        }
    }

    /** 单分片本地终态已经物化后的中心提交记录。 */
    public record TerminalDecision(long txId, long startTs, long commitTs, String shardId,
                                   IsolationLevel isolationLevel, long deadlineMillis) {
        /** 终态身份必须保持时间戳和目标分片的一致性。 */
        public TerminalDecision {
            TransactionMetadataValidation.validateParticipant(txId, shardId);
            Objects.requireNonNull(isolationLevel, "isolationLevel");
            if (txId == 0 || startTs < 0 || commitTs <= startTs || deadlineMillis < 1) {
                throw new IllegalArgumentException("invalid local-first terminal decision");
            }
        }
    }

    /** 返回同一控制事务中的新决议数及已经持久推进的连续发布水位。 */
    public record BatchCompletionResult(int completed, long publishedTs) {
        /** 批次结果不能报告负数量或无效水位。 */
        public BatchCompletionResult {
            if (completed < 0 || publishedTs < 0) {
                throw new IllegalArgumentException("invalid local-first batch completion result");
            }
        }
    }

    /** 一笔已在完整参与分片集合持久预写的跨分片中心提交请求。 */
    public record IntentCommitRequest(SegmentReservation segment, long commitTs, long txId,
                                      long startTs, long deadlineMillis, IsolationLevel isolationLevel,
                                      Set<String> participants, List<PrewriteProof> prewriteProofs) {
        /** 队列持有不可变路由与预写凭据，禁止入队后修改参与者集合。 */
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

    /** 一笔尚未预写的跨分片事务登记身份。 */
    public record IntentRegistrationRequest(long txId, long startTs, long deadlineMillis,
                                            IsolationLevel isolationLevel, Set<String> participants) {
        /** 批次入队时固定完整参与者集合与期限。 */
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

    /** 中心持久区分旧标量事务与需要保留分条序号的向量事务。 */
    private enum IntentProtocol { SCALAR, VECTOR }

    /** 一笔完整分片终态证明，供后台把多个已物化事务合为中心最终化批次。 */
    public record IntentFinalizationRequest(long txId, List<IntentTerminalProof> proofs) {
        /** 批次入队时固定不可变参与者证明列表。 */
        public IntentFinalizationRequest {
            proofs = List.copyOf(proofs);
            if (txId == 0 || proofs.isEmpty()) {
                throw new IllegalArgumentException("invalid cross-shard finalization request");
            }
        }
    }

    /** 后台从本控制库读取 COMMITTED 后签发的跨分片物化凭据。 */
    public static final class CommittedIntentProof {
        private final MysqlLocalFirstPublicationLedger ledger;
        private final TxRecord record;

        /** 私有构造禁止调用方凭内存事务状态伪造中心决议。 */
        private CommittedIntentProof(MysqlLocalFirstPublicationLedger ledger, TxRecord record) {
            this.ledger = ledger;
            this.record = record;
        }

        /** 返回持久中心事务编号、快照、提交时间与完整参与者。 */
        public TxRecord record() { return record; }

        /** 物化只允许使用读取该决议的同一中心权威。 */
        public boolean belongsTo(TransactionMetadataStore authority) {
            return authority instanceof MysqlLocalFirstMetadataView view && view.usesLedger(ledger);
        }
    }

    /** 已发布且所有参与分片均物化的向量事务可供分片删除旧大载荷。 */
    public static final class FinalizedVectorProof {
        private final MysqlLocalFirstPublicationLedger ledger;
        private final long txId;
        private final long startTs;
        private final long commitTs;
        private final Set<String> participants;
        private final com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity authorityIdentity;

        /** 只能由同一控制库的持久最终化查询构造。 */
        private FinalizedVectorProof(MysqlLocalFirstPublicationLedger ledger, long txId,
                                     long startTs, long commitTs, Set<String> participants,
                                     com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity authorityIdentity) {
            this.ledger = ledger;
            this.txId = txId;
            this.startTs = startTs;
            this.commitTs = commitTs;
            this.participants = Set.copyOf(participants);
            this.authorityIdentity = Objects.requireNonNull(authorityIdentity, "authorityIdentity");
        }

        /** 返回持久事务号。 */
        public long txId() {
            return txId;
        }

        /** 返回起始时间以验证分片终态。 */
        public long startTs() {
            return startTs;
        }

        /** 返回中心提交时间以验证可见性与重放。 */
        public long commitTs() {
            return commitTs;
        }

        /** 只有原始参与集合中的分片可以退役本事务版本。 */
        public boolean includes(String shardId) {
            return participants.contains(shardId);
        }

        /** 调用方不得混用另一控制库签发的凭据。 */
        public boolean belongsTo(MysqlLocalFirstPublicationLedger authority) {
            return ledger == authority;
        }

        /** 证明必须与安全向量读取时使用的真实物理控制库一致。 */
        public com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity authorityIdentity() {
            return authorityIdentity;
        }
    }

    /** 只有中心已封段、已终态并已发布时才能创建分片候选回收凭据。 */
    public static final class FinalizedCandidateProof {
        private final String shardId;
        private final PublicationSegmentLease physicalLease;
        private final CandidateProof candidate;

        /** 构造入口仅由中心持久状态校验后的账本调用。 */
        private FinalizedCandidateProof(String shardId, PublicationSegmentLease physicalLease,
                                        CandidateProof candidate) {
            this.shardId = shardId;
            this.physicalLease = physicalLease;
            this.candidate = candidate;
        }

        /** 返回唯一的目标分片路由。 */
        public String shardId() { return shardId; }

        /** 返回已由中心封存的原物理代数。 */
        public PublicationSegmentLease physicalLease() { return physicalLease; }

        /** 返回与中心 COMMITTED 槽逐字段相同的本地候选身份。 */
        public CandidateProof candidate() { return candidate; }
    }

    /** 整段已物理封存且连续发布后，才允许一次删除本段全部分片候选。 */
    public static final class FinalizedSegmentProof {
        private final String shardId;
        private final PublicationSegmentLease physicalLease;

        /** 只允许由控制库已发布 SEALED 行创建。 */
        private FinalizedSegmentProof(String shardId, PublicationSegmentLease physicalLease) {
            this.shardId = shardId;
            this.physicalLease = physicalLease;
        }

        /** 返回本段唯一目标分片。 */
        public String shardId() { return shardId; }

        /** 返回物理分片 fencing 使用的旧 owner 代数。 */
        public PublicationSegmentLease physicalLease() { return physicalLease; }
    }

    /** 中心安全点已越过整段且无较早活跃段时，允许物理栅栏退役。 */
    public static final class FinalizedFenceGcProof {
        private final String shardId;
        private final PublicationSegmentLease physicalLease;

        /** 只由已完成候选 GC 和安全点校验的中心账本创建。 */
        private FinalizedFenceGcProof(String shardId, PublicationSegmentLease physicalLease) {
            this.shardId = shardId;
            this.physicalLease = physicalLease;
        }

        /** 返回同一物理分片路由。 */
        public String shardId() { return shardId; }

        /** 返回需持久退役的旧 owner 段。 */
        public PublicationSegmentLease physicalLease() { return physicalLease; }
    }

    /** 使用与分片候选表隔离的控制库版本表，暂不改变现有产品时间线。 */
    public MysqlLocalFirstPublicationLedger(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** 只创建新协议的版本表并保持已初始化的时间线上界。 */
    public void initializeIsolated(long baseCommitTs) {
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
        } catch (SQLException ex) {
            throw storageError("initialize local-first control schema failed", ex);
        }
    }

    /** 旧版本中心表只作向后兼容的增量列迁移，原有事务默认为标量模式。 */
    private void ensureIntentProtocolColumn(Connection connection) throws SQLException {
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

    /** 独立后台池只探测已创建的新时间线和发布例程，不重跑前台 DDL。 */
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
        } catch (SQLException ex) {
            throw storageError("inspect local-first control fast path failed", ex);
        }
    }

    /** 一个事务内读取已发布前缀并保存独立快照租约，后续可改为协调器租约。 */
    public BeginReservation begin(IsolationLevel isolationLevel, long timeoutMillis) {
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
        } catch (SQLException ex) {
            throw storageError("begin local-first snapshot failed", ex);
        }
    }

    /** 一个持久协调器租约保护本节点多笔只读及待提交快照。 */
    public CoordinatorSnapshotLease acquireCoordinatorLease(long leaseMillis) {
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
        } catch (SQLException ex) {
            throw storageError("acquire local-first coordinator lease failed", ex);
        }
    }

    /** 每笔开始只一致读取发布水位与协调器代数，不插入逐事务租约。 */
    public CoordinatorSnapshotView snapshotUnderCoordinatorLease(long coordinatorId,
                                                                 long generation, long timeoutMillis) {
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
        } catch (SQLException ex) {
            throw storageError("read local-first coordinator snapshot failed", ex);
        }
    }

    /** 本地 GC 或操作入口可复核持久代数、期限和原事务开始水位。 */
    public void assertCoordinatorSnapshot(long coordinatorId, long generation, long startTs,
                                          long deadlineMillis, long txId) {
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
        } catch (SQLException ex) {
            throw storageError("verify local-first coordinator snapshot failed", ex);
        }
    }

    /** 按当前代数续期并只提高最老活跃快照下界。 */
    public CoordinatorSnapshotLease renewCoordinatorLease(long coordinatorId, long generation,
                                                          long nextReadTs, long leaseMillis) {
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
        } catch (SQLException ex) {
            throw storageError("renew local-first coordinator lease failed", ex);
        }
    }

    /** 当前代数且无活跃本地事务时，协调器可提前释放其 GC 保护行。 */
    public boolean releaseCoordinatorLease(long coordinatorId, long generation) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_coordinator_v1
                where coordinator_id = ? and generation = ? and status = 'ACTIVE'
                """)) {
            delete.setLong(1, coordinatorId);
            delete.setLong(2, generation);
            return delete.executeUpdate() == 1;
        } catch (SQLException ex) {
            throw storageError("release local-first coordinator lease failed", ex);
        }
    }

    /** 按数据库时钟清理到期协调器，失联旧代数不能重建同一编号。 */
    public int expireCoordinatorLeases(int limit) {
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
        } catch (SQLException ex) {
            throw storageError("expire local-first coordinator leases failed", ex);
        }
    }

    /** 跨分片预写前持久固定完整参与者，读侧从此可以把意向识别为 ACTIVE。 */
    public TxRecord registerIntentTransaction(long txId, long startTs, long deadlineMillis,
                                              IsolationLevel isolationLevel, Set<String> participants) {
        return registerIntentTransactionInternal(txId, startTs, deadlineMillis,
                isolationLevel, participants, IntentProtocol.SCALAR);
    }

    /** 向量跨片事务在任何分片预写前持久固定协议模式和完整参与者。 */
    public TxRecord registerVectorIntentTransaction(long txId, long startTs, long deadlineMillis,
                                                    IsolationLevel isolationLevel, Set<String> participants) {
        return registerIntentTransactionInternal(txId, startTs, deadlineMillis,
                isolationLevel, participants, IntentProtocol.VECTOR);
    }

    /** 两种协议共用原子 ACTIVE 登记，重放不得改变持久协议模式。 */
    private TxRecord registerIntentTransactionInternal(long txId, long startTs, long deadlineMillis,
                                                       IsolationLevel isolationLevel, Set<String> participants,
                                                       IntentProtocol protocol) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (txId == 0 || startTs < 0 || deadlineMillis < 1 || participants.size() < 2
                || participants.contains(INTENT_SEGMENT_ROUTE)) {
            throw new IllegalArgumentException("invalid cross-shard intent registration");
        }
        String encoded = participants.stream().sorted().collect(java.util.stream.Collectors.joining(","));
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                // 缺行只作一致性读取，不能对主键空隙加排他锁后再与其他事务互相等待插入。
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 1062) {
                try {
                    return MysqlConnectionContext.inTransaction(dataSource, connection ->
                            requireIntentRegistrationReplay(connection, txId, startTs,
                                    deadlineMillis, isolationLevel, participants, protocol));
                } catch (SQLException lookupFailure) {
                    ex.addSuppressed(lookupFailure);
                }
            }
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent registration requires durable lookup", false, txId, ex);
        }
    }

    /** 全新事务以一笔中心事务登记多条 ACTIVE 身份，失败则整批不允许预写。 */
    public List<TxRecord> registerIntentBatch(List<IntentRegistrationRequest> requests) {
        return registerIntentBatchInternal(requests, IntentProtocol.SCALAR);
    }

    /** 向量事务批量 ACTIVE 登记与协议模式在同一中心事务持久落盘。 */
    public List<TxRecord> registerVectorIntentBatch(List<IntentRegistrationRequest> requests) {
        return registerIntentBatchInternal(requests, IntentProtocol.VECTOR);
    }

    /** 两种模式共用有序键锁与整批原子注册，不允许批次隐式混合协议。 */
    private List<TxRecord> registerIntentBatchInternal(List<IntentRegistrationRequest> requests,
                                                       IntentProtocol protocol) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard ACTIVE batch requires durable status lookup", false, null, ex);
        }
    }

    /** 响应丢失或并发同号插入只接受完全相同的中心持久身份。 */
    private TxRecord requireIntentRegistrationReplay(Connection connection, long txId, long startTs,
                                                     long deadlineMillis, IsolationLevel isolationLevel,
                                                     Set<String> participants, IntentProtocol protocol)
            throws SQLException {
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

    /** 读侧和分片物化均以同一控制表读取不可缓存的实时事务终态。 */
    public Optional<TxRecord> metadataRecord(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            return Optional.ofNullable(readMetadataRecord(connection, txId, false));
        } catch (SQLException ex) {
            throw storageError("read local-first transaction metadata failed", ex);
        }
    }

    /** 后台恢复必须按持久模式选择物化路径，不能把向量事务交给旧标量例程。 */
    public boolean isVectorIntentTransaction(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            IntentMetadata metadata = readIntentMetadata(connection, txId, false);
            if (metadata == null || !INTENT_SEGMENT_ROUTE.equals(metadata.route())) {
                throw illegal("vector intent protocol lookup has no matching cross-shard transaction");
            }
            return metadata.protocol() == IntentProtocol.VECTOR;
        } catch (SQLException ex) {
            throw storageError("read cross-shard intent protocol mode failed", ex);
        }
    }

    /** 一次中心读取确认不可逆跨分片决议，供多个物理分片复用。 */
    public CommittedIntentProof committedIntentProof(long txId) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard materialization requires durable center decision", false, txId, ex);
        }
    }

    /** 分片仅可在中心已中止或数据库期限已到后回滚跨分片意向。 */
    public void assertIntentAbortAllowed(long txId) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard abort requires durable center status lookup", false, txId, ex);
        }
    }

    /** 按有符号事务编号分页发现尚未完成物理物化的跨分片 COMMITTED 决议。 */
    public List<Long> findUnfinalizedIntentDecisions(long afterTxId, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("scan unfinalized cross-shard intent decisions failed", ex);
        }
    }

    /** 运维和容量实验读取精确未物化提交数，不能把单页上限误当积压总量。 */
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
        } catch (SQLException ex) {
            throw storageError("count unfinalized cross-shard decisions failed", ex);
        }
    }

    /** 到期未决或已决定中止但尚未物理清理的意向，按事务号轮转恢复。 */
    public List<Long> findIntentAbortWorkIds(long afterTxId, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("scan cross-shard intent abort work failed", ex);
        }
    }

    /** 两片真实终态均已物化后，中心才把 COMMITTED 决议标记为可 GC。 */
    public boolean markIntentFinalized(long txId, List<IntentTerminalProof> proofs) {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                Long slotCommitTs;
                // 批量决议按槽再按事务元数据加锁，最终化遵守同一顺序避免交叉等待。
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent finalization requires durable lookup", false, txId, ex);
        }
    }

    /** 已物化跨分片提交的中心最终化按事务号有界合并为一笔控制事务。 */
    public int markIntentFinalizedBatch(List<IntentFinalizationRequest> requests) {
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
                try (PreparedStatement query = connection.prepareStatement(
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard finalization batch requires durable status lookup", false, null, ex);
        }
    }

    /** 前台显式回滚或后台到期恢复先持久写入不可逆中心 ABORTED 决议。 */
    public TxRecord abortIntentTransaction(long txId) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard abort decision requires durable lookup", false, txId, ex);
        }
    }

    /** 所有参与者中止落盘后才解除 ABORTED 中心记录的 GC 保护。 */
    public boolean markIntentAbortFinalized(long txId, List<IntentTerminalProof> proofs) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard abort finalization requires durable lookup", false, txId, ex);
        }
    }

    /** 分片 GC 保留尚未决议或已提交但本地恢复锚点未清理的事务下界。 */
    public Optional<Long> oldestUnfinalizedStartTs() {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select min(start_ts) from mvcc_local_first_txn_v1
                where shard_id = ? and (status in ('ACTIVE', 'PREPARING')
                   or (status in ('COMMITTED', 'ABORTED') and candidate_gc_done = 0))
                """)) {
            query.setString(1, INTENT_SEGMENT_ROUTE);
            try (ResultSet row = query.executeQuery()) {
                return row.next() && row.getObject(1) != null
                        ? Optional.of(row.getLong(1)) : Optional.empty();
            }
        } catch (SQLException ex) {
            throw storageError("read local-first unfinalized metadata floor failed", ex);
        }
    }

    /** 稳定请求键只预留一次连续段，并把保守快照下界持久绑定到目标分片。 */
    public SegmentReservation reserveSegment(String shardId, String ownerId, String reservationKey,
                                             int size, long leaseMillis, long snapshotFloor) {
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
        } catch (SQLException ex) {
            throw storageError("reserve local-first segment failed", ex);
        }
    }

    /** 物理分片 fence 安装完成后才使中心段可供本地分配。 */
    public SegmentReservation activateSegment(SegmentReservation expected, MysqlShardSegmentFence physical) {
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
        } catch (SQLException ex) {
            throw storageError("activate local-first segment failed", ex);
        }
    }

    /** 意向模式没有分片本地先提交栅栏，中心段可在持久预留后直接激活。 */
    public SegmentReservation activateIntentSegment(SegmentReservation expected) {
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
        } catch (SQLException ex) {
            throw storageError("activate cross-shard intent segment failed", ex);
        }
    }

    /** 只在当前 ACTIVE 代数和数据库租约仍有效时续租，响应丢失重试返回持久期限。 */
    public SegmentReservation renewSegment(SegmentReservation expected) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first segment renewal requires durable owner lookup", false, null, ex);
        }
    }

    /** 失联接管先持久进入 FENCING，随后才可跨网络请求分片物理栅栏。 */
    public FenceRequest beginFencing(SegmentReservation expected, boolean forceClose) {
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
        } catch (SQLException ex) {
            throw storageError("begin local-first segment fencing failed", ex);
        }
    }

    /** 多笔分片持久证明在一笔控制事务中补齐中心元数据及相同时间槽。 */
    public int completeCandidates(SegmentReservation expected, MysqlShardSegmentFence physical,
                                  List<Long> transactionIds) {
        return completeCandidatesInternal(expected, physical, transactionIds, false).completed();
    }

    /** 临近已发布前缀的中心批次可在同一事务内顺带推进连续水位。 */
    public BatchCompletionResult completeCandidatesAndPublish(SegmentReservation expected,
                                                              MysqlShardSegmentFence physical,
                                                              List<Long> transactionIds) {
        return completeCandidatesInternal(expected, physical, transactionIds, true);
    }

    /** 前台凭成功提交后产生的分片证明完成中心批次，无需再次查询分片。 */
    public int completeProvedCandidates(SegmentReservation expected, MysqlShardSegmentFence physical,
                                        List<LocalCommitProof> committed) {
        return completeProvedCandidatesInternal(expected, physical, committed, false).completed();
    }

    /** 前台证明批次在同一中心事务中顺带推进连续发布水位。 */
    public BatchCompletionResult completeProvedCandidatesAndPublish(SegmentReservation expected,
                                                                   MysqlShardSegmentFence physical,
                                                                   List<LocalCommitProof> committed) {
        return completeProvedCandidatesInternal(expected, physical, committed, true);
    }

    /** 只有同一物理实例已成功提交的不同事务证明可跳过分片回查。 */
    private BatchCompletionResult completeProvedCandidatesInternal(SegmentReservation expected,
                                                                   MysqlShardSegmentFence physical,
                                                                   List<LocalCommitProof> committed,
                                                                   boolean publishAfter) {
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

    /** 两分片以上的持久意向以同一控制事务决定元数据和全局时间槽。 */
    public int completeIntentBatch(List<IntentCommitRequest> requests) {
        return completeIntentBatchInternal(requests, false, false).completed();
    }

    /** 新向量协议只接受真实参与者意向与分条序号同事务产生的预写证明。 */
    public int completeVectorIntentBatch(List<IntentCommitRequest> requests) {
        requireVectorPrewriteProofs(requests);
        return completeIntentBatchInternal(requests, false, true).completed();
    }

    /** 向量中心决议与前缀发布同事务完成，ACK 仍需等待分片向量切面覆盖。 */
    public BatchCompletionResult completeVectorIntentBatchAndPublish(List<IntentCommitRequest> requests) {
        requireVectorPrewriteProofs(requests);
        return completeIntentBatchInternal(requests, true, true);
    }

    /** 向量批次每个参与者必须签发非空单键分条保留证明。 */
    private void requireVectorPrewriteProofs(List<IntentCommitRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        for (IntentCommitRequest request : requests) {
            for (PrewriteProof proof : request.prewriteProofs()) {
                if (proof.mutationCount() != 1 || proof.vectorReservation().isEmpty()) {
                    throw illegal("vector cross-shard commit lacks a durable stripe reservation proof");
                }
            }
        }
    }

    /** 临近发布前缀的跨分片决议同事务推进持久连续水位。 */
    public BatchCompletionResult completeIntentBatchAndPublish(List<IntentCommitRequest> requests) {
        return completeIntentBatchInternal(requests, true, false);
    }

    /** 只在完整分片预写证明和中心槽均核对通过后发布同事务终态。 */
    private BatchCompletionResult completeIntentBatchInternal(List<IntentCommitRequest> requests,
                                                              boolean publishAfter, boolean vectorRequired) {
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
                // 整批事务行锁取得后只读一次数据库时钟，避免到期回滚与旧时钟决议交叉。
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
                            ? IntentProtocol.VECTOR : IntentProtocol.SCALAR)
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent decision requires durable status lookup", false, null, ex);
        }
    }

    /** 已锁定且核对过身份的整批 ACTIVE 行用一条 SQL 写入各自提交时间。 */
    private void completeIntentMetadataBatch(Connection connection, List<IntentCommitRequest> requests)
            throws SQLException {
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

    /** 两条中心路径共用持久证明、槽锁顺序与整批原子回滚校验。 */
    private BatchCompletionResult completeCandidatesInternal(SegmentReservation expected,
                                                             MysqlShardSegmentFence physical,
                                                             List<Long> transactionIds, boolean publishAfter) {
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

    /** 恢复回查和前台不可伪造证明共用同一中心身份及原子槽决议。 */
    private BatchCompletionResult completeCandidateProofsInternal(SegmentReservation expected,
                                                                 MysqlShardSegmentFence physical,
                                                                 List<CandidateProof> proofs,
                                                                 boolean publishAfter) {
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
                        ? segment.generation() - 1 : segment.generation();
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first center completion requires durable lookup", false, null, ex);
        }
    }

    /** 按事务编号查询中心已提交终态，结果未知时不重跑业务写集合。 */
    public Optional<TerminalDecision> decision(long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select tx_id, start_ts, commit_ts, shard_id, isolation_level, deadline_millis
                from mvcc_local_first_txn_v1 where tx_id = ? and status = 'COMMITTED'
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? Optional.of(mapTerminal(row)) : Optional.empty();
            }
        } catch (SQLException ex) {
            throw storageError("read local-first center decision failed", ex);
        }
    }

    /** 向量旧版本只有在中心已发布且全部参与分片最终化后才能申请退役。 */
    public Optional<FinalizedVectorProof> finalizedVectorProof(long txId) {
        return Optional.ofNullable(finalizedVectorProofs(List.of(txId)).get(txId));
    }

    /** 一次从中心读取有界候选的最终化证明，后台不再逐事务借控制连接。 */
    public Map<Long, FinalizedVectorProof> finalizedVectorProofs(List<Long> txIds) {
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
        try (Connection connection = dataSource.getConnection();
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
        } catch (SQLException ex) {
            throw storageError("read finalized vector retirement proofs failed", ex);
        }
    }

    /** 向量 GC 只可把中心最终化凭据与同一物理控制库的安全下界配对。 */
    public com.flyingzc.azir.control.MysqlDatabaseAuthority.Identity databaseIdentity() {
        return com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(dataSource);
    }

    /** 只为已封存且已发布的同一事务签发分片候选 GC 凭据。 */
    public Optional<FinalizedCandidateProof> candidateGcProof(long txId) {
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
        } catch (SQLException ex) {
            throw storageError("read local-first finalized candidate proof failed", ex);
        }
    }

    /** 有界扫描仍待删除的已发布分片候选，负事务号也按有符号主键分页。 */
    public List<Long> findPurgeableCandidateIds(long afterTxId, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("scan local-first candidate GC proofs failed", ex);
        }
    }

    /** 按段主键分页发现过期 owner 与崩溃后仍在 FENCING 的恢复工作。 */
    public List<SegmentReservation> findRecoverableSegments(long afterSegmentStart, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("scan local-first recoverable segments failed", ex);
        }
    }

    /** 分片已确认删除候选后才持久标记 GC 完成，响应未知可原事务重放。 */
    public boolean markCandidatePurged(long txId, MysqlShardSegmentFence physical) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first candidate GC marker requires durable lookup", false, txId, ex);
        }
    }

    /** 已封存且有待清理候选的段按主键分页，避免逐事务扫描和标记。 */
    public List<SegmentReservation> findPurgeableSegments(long afterSegmentStart, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("scan local-first purgeable segments failed", ex);
        }
    }

    /** 只在全段 SEALED 且末槽已发布时签发不可由调用方构造的整段回收凭据。 */
    public Optional<FinalizedSegmentProof> segmentGcProof(long segmentStart) {
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
        } catch (SQLException ex) {
            throw storageError("read local-first finalized segment proof failed", ex);
        }
    }

    /** 分片整段候选已清空后，一条 SQL 持久标记本段所有中心终态可回收。 */
    public int markSegmentCandidatesPurged(FinalizedSegmentProof proof, MysqlShardSegmentFence physical) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first segment GC marker requires durable lookup", false, null, ex);
        }
    }

    /** 按段主键有界查找候选已清理且可压缩物理 FENCED 墓碑的段。 */
    public List<SegmentReservation> findRetirableFences(long afterSegmentStart, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("scan local-first retirable fences failed", ex);
        }
    }

    /** 当前控制安全点和已完成候选清理共同签发不可伪造的物理退役凭据。 */
    public Optional<FinalizedFenceGcProof> fenceGcProof(long segmentStart) {
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
        } catch (SQLException ex) {
            throw storageError("read local-first finalized fence GC proof failed", ex);
        }
    }

    /** 分片持久退役水位已覆盖旧段后才标记中心物理栅栏 GC 完成。 */
    public boolean markFenceRetired(FinalizedFenceGcProof proof, MysqlShardSegmentFence physical) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first fence GC marker requires durable lookup", false, null, ex);
        }
    }

    /** 只发布从旧水位开始连续的已提交或已中止槽，遇到 PENDING 立即停止。 */
    public long advancePublished(int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first publication scan limit");
        }
        try {
            return MysqlConnectionContext.inTransaction(dataSource,
                    connection -> advancePublishedLocked(connection, limit));
        } catch (SQLException ex) {
            throw storageError("advance local-first published prefix failed", ex);
        }
    }

    /** 可复用已打开的中心事务推进连续前缀，仍按持久主键检查缺槽和待决。 */
    private long advancePublishedLocked(Connection connection, int limit) throws SQLException {
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

    /** 显式实验路径用一次服务器调用推进连续终态前缀，异常结果须重新查询持久水位。 */
    public long advancePublishedFast(int limit) {
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first fast publication result requires durable waterline lookup", false, null, ex);
        }
    }

    /** 新例程只处理连续主键前缀，不用状态次级索引猜测缺失时间槽。 */
    private void ensureFastPublicationProcedure(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 物理 fencing 后补全所有本地候选，再原子中止真正空余槽并封存中心段。 */
    public int sealFencedSegment(SegmentReservation expected, MysqlShardSegmentFence physical) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(physical, "physical");
        if (INTENT_SEGMENT_ROUTE.equals(expected.shardId())) {
            throw illegal("cross-shard intent segment requires center-only terminal sealing");
        }
        if (!expected.shardId().equals(physical.shardId())) {
            throw illegal("local-first seal targets another physical shard");
        }
        SegmentReservation current = segment(expected.lease().firstTs()).orElseThrow(() ->
                illegal("local-first seal has no durable center segment"));
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "local-first final seal requires durable lookup", false, null, ex);
        }
        } catch (RuntimeException ex) {
            // 另一恢复者可能已封段并回收本地候选；只接受双侧持久 SEALED/FENCED 证明。
            if (peerCompletedSeal(expected, physical)) {
                return 0;
            }
            throw ex;
        }
    }

    /** 跨分片段已被中心 FENCING 后，只中止无中心提交的占位槽。 */
    public int sealIntentSegment(SegmentReservation expected) {
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
                                // 段排他锁已阻止新决议，提交身份不可再变；不按槽顺序反向锁事务行。
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
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "cross-shard intent seal requires durable status lookup", false, null, ex);
        }
    }

    /** 并发封段完成后本地候选可已清理，重放只依赖双侧不可逆终态。 */
    private boolean peerCompletedSeal(SegmentReservation expected, MysqlShardSegmentFence physical) {
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
        } catch (RuntimeException checkFailure) {
            return false;
        }
    }

    /** 安全点同时受活跃逐事务快照和未封存分段的保守下界约束。 */
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
        } catch (SQLException ex) {
            throw storageError("advance local-first safe point failed", ex);
        }
    }

    /** 到期逐事务快照可回收，未封存本地段仍独立固定 GC 下界。 */
    public int expireSnapshots(int limit) {
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
        } catch (SQLException ex) {
            throw storageError("expire local-first snapshots failed", ex);
        }
    }

    /** 前台结束只删除自身租约，未封存段仍保护本地已提交候选。 */
    public void releaseSnapshot(long txId) {
        try (Connection connection = dataSource.getConnection(); PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_local_first_snapshot_v1 where tx_id = ?
                """)) {
            delete.setLong(1, txId);
            delete.executeUpdate();
        } catch (SQLException ex) {
            throw storageError("release local-first snapshot failed", ex);
        }
    }

    /** 只读查看持久中心段状态，响应未知时可重新获取原代数与目标分片。 */
    public Optional<SegmentReservation> segment(long segmentStart) {
        try (Connection connection = dataSource.getConnection()) {
            return Optional.ofNullable(lockSegment(connection, segmentStart)).map(this::reservation);
        } catch (SQLException ex) {
            throw storageError("read local-first segment failed", ex);
        }
    }

    /** 返回当前已发布水位用于测试新旧 RC/RR 快照。 */
    public long currentPublished() {
        try (Connection connection = dataSource.getConnection()) {
            return requireState(connection).publishedTs();
        } catch (SQLException ex) {
            throw storageError("read local-first published waterline failed", ex);
        }
    }

    /** 返回持久 GC 安全点用于检查保守下界。 */
    public long currentSafePoint() {
        try (Connection connection = dataSource.getConnection()) {
            return requireState(connection).safePoint();
        } catch (SQLException ex) {
            throw storageError("read local-first safe point failed", ex);
        }
    }

    /** GC 保留期限采用控制数据库时钟，不能由协调者墙钟代替。 */
    public long currentDatabaseTimeMillis() {
        try (Connection connection = dataSource.getConnection()) {
            return databaseNow(connection);
        } catch (SQLException ex) {
            throw storageError("read local-first control database time failed", ex);
        }
    }

    /** 向量跨片版本尚无退休证明时保留终态槽，标量候选仍按原保留窗口清理。 */
    public int purgeTerminalSlots(long safePoint, long expiredBeforeMillis, int limit) {
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
        } catch (SQLException ex) {
            throw storageError("purge local-first terminal slots failed", ex);
        }
    }

    /** 向量跨片版本未退休前保留中心决议，标量终态在槽删除后照常回收。 */
    public int purgeFinalizedMetadata(long safePoint, long finalizedBeforeMillis, int limit) {
        validateGcRequest(safePoint, finalizedBeforeMillis, limit);
        try (Connection connection = dataSource.getConnection()) {
            List<Long> candidates = new ArrayList<>();
            // 先无锁筛选已终结主键，避免 DELETE 范围扫描锁住正在写 commit_ts 的 ACTIVE 行。
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
        } catch (SQLException ex) {
            throw storageError("purge local-first finalized metadata failed", ex);
        }
    }

    /** 物理退役已持久且槽与元数据先清理后，才删除中心 SEALED 段头。 */
    public int purgeSealedSegments(long safePoint, long expiredBeforeMillis, int limit) {
        validateGcRequest(safePoint, expiredBeforeMillis, limit);
        try (Connection connection = dataSource.getConnection()) {
            List<Long> candidates = new ArrayList<>();
            // 封段后不可新增本段槽或提交，先无锁核对空段再按主键删除，避免反向锁活跃决议。
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
        } catch (SQLException ex) {
            throw storageError("purge local-first sealed segments failed", ex);
        }
    }

    /** 手工 GC 请求不能强推超过控制库已持久计算的快照安全点。 */
    private void validateGcRequest(long safePoint, long cutoffMillis, int limit) {
        if (safePoint < 0 || cutoffMillis < 0 || limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid local-first GC request");
        }
        if (safePoint > currentSafePoint()) {
            throw illegal("local-first GC cannot exceed durable safe point");
        }
    }

    /** 控制状态行在段预留和安全点推进时承担短事务串行栅栏。 */
    private record State(long nextCommitTs, long publishedTs, long safePoint) {
    }

    /** 控制槽只允许待决、已提交或已中止三种持久状态。 */
    private enum SlotStatus { PENDING, COMMITTED, ABORTED }

    /** 槽身份与中心事务在同一事务内双向校验。 */
    private record SlotRow(long segmentStart, Long txId, SlotStatus status,
                           Long startTs, Long deadlineMillis, String isolationLevel) {
    }

    /** 记录 owner 原代数与段状态，持久行才是恢复权威。 */
    private record SegmentRow(long segmentStart, long segmentEnd, String shardId, String ownerId,
                              long generation, long snapshotFloor, long leaseMillis,
                              long expiresAtMillis, SegmentStatus status) {
        /** 状态转换不改变其余已持久分段身份。 */
        private SegmentRow withStatus(SegmentStatus next) {
            return new SegmentRow(segmentStart, segmentEnd, shardId, ownerId, generation,
                    snapshotFloor, leaseMillis, expiresAtMillis, next);
        }

        /** FENCING 持久增加代数后，原代数仍可由当前值减一恢复。 */
        private SegmentRow withGeneration(long next) {
            return new SegmentRow(segmentStart, segmentEnd, shardId, ownerId, next,
                    snapshotFloor, leaseMillis, expiresAtMillis, status);
        }
    }

    /** 从持久中心段重建面向前台的完整预留信息。 */
    private SegmentReservation reservation(SegmentRow row) {
        return new SegmentReservation(new PublicationSegmentLease(row.segmentStart(), row.segmentEnd(),
                row.ownerId(), row.generation(), row.expiresAtMillis()), row.shardId(),
                row.snapshotFloor(), row.leaseMillis(), row.status());
    }

    /** 仅供跨分片扫描前核对预留对象与当前持久中心身份。 */
    private SegmentRow rowFromReservation(SegmentReservation reservation) {
        PublicationSegmentLease lease = reservation.lease();
        return new SegmentRow(lease.firstTs(), lease.lastTs(), reservation.shardId(),
                lease.ownerId(), lease.generation(), reservation.snapshotFloor(),
                reservation.leaseMillis(), lease.expiresAtMillis(), reservation.status());
    }

    /** FENCING 后只把原代数传给目标分片进行物理排他栅栏。 */
    private FenceRequest fenceRequest(SegmentRow row) {
        long physicalGeneration = row.generation() - 1;
        if (physicalGeneration < 1) {
            throw illegal("local-first fencing lost original owner generation");
        }
        return new FenceRequest(new PublicationSegmentLease(row.segmentStart(), row.segmentEnd(),
                row.ownerId(), physicalGeneration, row.expiresAtMillis()), row.shardId(),
                row.snapshotFloor());
    }

    /** 请求键只可重放同一目标分片的完全相同段身份。 */
    private SegmentRow findByReservationKey(Connection connection, String key) throws SQLException {
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

    /** 读取或锁定 owner 段，调用方在同一事务内继续检查状态与代数。 */
    private SegmentRow lockSegment(Connection connection, long segmentStart) throws SQLException {
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

    /** 完成不同槽时共享 owner 栅栏，只与 FENCING 的排他转换互斥。 */
    private SegmentRow lockSegmentShared(Connection connection, long segmentStart) throws SQLException {
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

    /** 一次按主键顺序锁定整个候选批次，避免批次之间产生反向槽锁。 */
    private Map<Long, SlotRow> lockSlotsBatch(Connection connection, List<CandidateProof> proofs)
            throws SQLException {
        return lockSlotTimes(connection, proofs.stream().map(CandidateProof::commitTs).toList());
    }

    /** 局部候选和跨分片意向共用主键升序槽锁，防止状态索引反向死锁。 */
    private Map<Long, SlotRow> lockSlotTimes(Connection connection, List<Long> commitTimestamps)
            throws SQLException {
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

    /** 多笔已验证本地候选用一条 INSERT 保存独立的中心终态。 */
    private void insertTerminalBatch(Connection connection, List<CandidateProof> proofs, String shardId)
            throws SQLException {
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

    /** 已锁定的槽按同一中心事务行批量改为 COMMITTED，行数不符整批回滚。 */
    private void completeSlotsBatch(Connection connection, List<CandidateProof> proofs, long segmentStart)
            throws SQLException {
        completeSlotTimes(connection, proofs.stream().map(CandidateProof::commitTs).toList(), segmentStart);
    }

    /** 已锁定的不同模式时间槽都只从同事务中心记录复制终态身份。 */
    private void completeSlotTimes(Connection connection, List<Long> commitTimestamps, long segmentStart)
            throws SQLException {
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

    /** 尚存的逐事务租约必须与本地候选一致；到期已删租约仍由段下界保护。 */
    private void validateRemainingSnapshotLeases(Connection connection, List<CandidateProof> proofs)
            throws SQLException {
        List<CandidateProof> individual = proofs.stream().filter(proof -> proof.txId() > 0).toList();
        if (individual.isEmpty()) {
            // 负事务号使用协调器租约与持久段下界，没有逐事务快照行可查询。
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

    /** 重放的槽和中心事务都必须与原分片候选逐字段相同。 */
    private void requireTerminalReplay(Connection connection, CandidateProof proof,
                                       SlotRow slot, String shardId) throws SQLException {
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

    /** 读取同一个控制事务中的已持久提交记录。 */
    private TerminalDecision readTerminal(Connection connection, long txId) throws SQLException {
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

    /** 同一行锁下读取跨分片模式、期限及通用事务元数据。 */
    private record IntentMetadata(TxRecord record, String route, long deadlineMillis,
                                  boolean gcDone, IntentProtocol protocol) {
    }

    /** 所有参与者终态证明必须来自本账本权威、覆盖完整路由且与中心决议一致。 */
    private void requireIntentTerminalProofs(TxRecord center, List<IntentTerminalProof> proofs) {
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

    /** 批量决议不需为路由与截止时间重复向控制库发两次查询。 */
    private IntentMetadata lockIntentMetadata(Connection connection, long txId) throws SQLException {
        return readIntentMetadata(connection, txId, true);
    }

    /** 封段只需稳定的提交身份，其余状态改写才取得事务行排他锁。 */
    private IntentMetadata readIntentMetadata(Connection connection, long txId, boolean lock)
            throws SQLException {
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

    /** 整批按事务主键顺序加锁，避免逐笔往返和反向元数据行锁。 */
    private Map<Long, IntentMetadata> lockIntentMetadataBatch(Connection connection, List<Long> txIds)
            throws SQLException {
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

    /** 单笔与批量锁读取共用同一持久身份解码。 */
    private IntentMetadata mapIntentMetadata(ResultSet row) throws SQLException {
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

    /** 未决事务和已决议事务都按持久参与者集合还原。 */
    private TxRecord readMetadataRecord(Connection connection, long txId, boolean lock) throws SQLException {
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

    /** 逐事务租约或负事务号的持久协调器下界必须覆盖开始时间与截止时间。 */
    private void requireIntentSnapshot(Connection connection, long txId, long startTs,
                                       long deadlineMillis, long now) throws SQLException {
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

    /** 将中心 COMMITTED 行转换成不可变结果。 */
    private TerminalDecision mapTerminal(ResultSet row) throws SQLException {
        return new TerminalDecision(row.getLong("tx_id"), row.getLong("start_ts"),
                row.getLong("commit_ts"), row.getString("shard_id"),
                IsolationLevel.valueOf(row.getString("isolation_level")), row.getLong("deadline_millis"));
    }

    /** JDBC 可空整数不能把 NULL 误读成事务编号零。 */
    private Long nullableLong(ResultSet row, String column) throws SQLException {
        long value = row.getLong(column);
        return row.wasNull() ? null : value;
    }

    /** 将数据库枚举映射成不可变分段记录。 */
    private SegmentRow mapSegment(ResultSet row) throws SQLException {
        return new SegmentRow(row.getLong("segment_start"), row.getLong("segment_end"),
                row.getString("shard_id"), row.getString("owner_id"), row.getLong("generation"),
                row.getLong("snapshot_floor"), row.getLong("lease_millis"),
                row.getLong("expires_at"), SegmentStatus.valueOf(row.getString("status")));
    }

    /** 预留结果不可跨 owner、分片、范围或下界执行状态转换。 */
    private void requireSameReservation(SegmentReservation expected, SegmentRow current) {
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

    /** 在独占状态行内读取下一时间戳、发布前缀与 GC 下界。 */
    private State lockState(Connection connection) throws SQLException {
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

    /** 非锁定读取仍须拒绝错误 schema 或回退的时间线。 */
    private State requireState(Connection connection) throws SQLException {
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

    /** 数据库时钟只用于控制库租约，分片以自己的时钟独立 fencing。 */
    private long databaseNow(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("local-first control database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 统一拒绝越界的前后台租约，避免整数溢出后旧 owner 复活。 */
    private void validateDuration(long millis) {
        if (millis < 1 || millis > 86_400_000L) {
            throw new IllegalArgumentException("invalid local-first lease duration");
        }
    }

    /** 目标分片与 owner 请求键必须可持久重放，不能被 CSV 或长度歧义改变。 */
    private void validateRoute(String shardId, String ownerId, String reservationKey) {
        TransactionMetadataValidation.validateParticipant(0, shardId);
        if (shardId.length() > 128 || ownerId == null || !ownerId.matches("[A-Za-z0-9._:-]{1,128}")
                || reservationKey == null || !reservationKey.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new IllegalArgumentException("invalid local-first shard, owner or request key");
        }
    }

    /** 持久状态失配时不发出可重复执行业务的错误。 */
    private MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /** owner 已进入失联接管时拒绝继续本地分配。 */
    private MvccException aborted(String message) {
        return new MvccException(ErrorCode.TRANSACTION_ABORTED, message, false, null);
    }

    /** 控制库连接或 SQL 失败保留异常链供上层按结果未知恢复。 */
    private MvccException storageError(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }
}

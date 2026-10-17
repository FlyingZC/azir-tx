package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.MutationType;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlLocalFirstMetadataView;
import mvcc.metadata.TxRecord;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger.CommittedProof;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger.CommittedIntentProof;
import mvcc.publication.PublicationSegmentLease;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ParticipantRecord;
import mvcc.storage.ReadContext;
import mvcc.storage.ScanPage;
import mvcc.storage.StorageAdapter;
import mvcc.storage.Utf8KeyOrder;
import mvcc.storage.VersionedValue;

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
 * MySQL MVCC 参与者，以本地水位、事务行和稳定键锁保护迟到请求及终态重放。
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

    /** 向量后台清理只读此真实前台数据源以核对物理分片权威。 */
    DataSource vectorDataSource() {
        return dataSource;
    }

    /** 只在完整意向本地事务成功提交后创建的中心跨分片决议凭据。 */
    public static final class PrewriteProof {
        private final long txId;
        private final long startTs;
        private final String shardId;
        private final int mutationCount;
        private final TransactionMetadataStore authority;
        private final VectorReservation vectorReservation;

        /** 构造入口仅由同一适配器的持久预写成功路径调用。 */
        private PrewriteProof(long txId, long startTs, String shardId, int mutationCount,
                              TransactionMetadataStore authority) {
            this(txId, startTs, shardId, mutationCount, authority, null);
        }

        /** 新协议证明额外绑定同一物理预写事务保留的分条序号。 */
        private PrewriteProof(long txId, long startTs, String shardId, int mutationCount,
                              TransactionMetadataStore authority, VectorReservation vectorReservation) {
            this.txId = txId;
            this.startTs = startTs;
            this.shardId = shardId;
            this.mutationCount = mutationCount;
            this.authority = authority;
            this.vectorReservation = vectorReservation;
        }

        /** 返回本地意向所属的全局事务编号。 */
        public long txId() { return txId; }

        /** 返回分片已校验过的事务读快照。 */
        public long startTs() { return startTs; }

        /** 返回唯一目标分片路由。 */
        public String shardId() { return shardId; }

        /** 返回非空完整写集合的持久预写条数。 */
        public int mutationCount() { return mutationCount; }

        /** 只有向量预写路径才携带经本地事务原子落盘的序号证明。 */
        public Optional<VectorReservation> vectorReservation() {
            return Optional.ofNullable(vectorReservation);
        }

        /** 拒绝把另一控制账本实例的预写结果当作本时间线决议依据。 */
        public boolean belongsTo(MysqlLocalFirstPublicationLedger ledger) {
            return authority instanceof MysqlLocalFirstMetadataView view && view.usesLedger(ledger);
        }
    }

    /** 中心只接受由私有 PrewriteProof 包装的持久分条保留身份。 */
    public record VectorReservation(String key, int stripe, long startSequence, long reservedSequence) {
        /** 已保留序号必须晚于事务的起始分条快照。 */
        public VectorReservation {
            Objects.requireNonNull(key, "key");
            if (key.isBlank() || stripe < 0 || startSequence < 0
                    || reservedSequence <= startSequence) {
                throw new IllegalArgumentException("invalid vector reservation proof");
            }
        }
    }

    /** 多笔独立单键逻辑事务可共享一次分片物理提交，但各自保留重放身份。 */
    public record LocalVectorCommitRequest(long txId, Mutation mutation,
                                           int stripe, long startSequence) {
        /** 同批每笔事务必须携带负事务号和有效起始分条边界。 */
        public LocalVectorCommitRequest {
            Objects.requireNonNull(mutation, "mutation");
            if (txId >= 0 || stripe < 0 || startSequence < 0) {
                throw new IllegalArgumentException("invalid local vector group commit request");
            }
        }
    }

    /** 多笔独立跨片事务在同一参与分片批量预写，仍逐笔签发中心证明。 */
    public record VectorPrewriteRequest(long txId, long startTs, long expireAtMillis,
                                        Mutation mutation, int stripe, long startSequence) {
        /** 批量预写必须有持久身份、有效期限和非负分条起点。 */
        public VectorPrewriteRequest {
            Objects.requireNonNull(mutation, "mutation");
            if (txId == 0 || startTs < 0 || expireAtMillis < 1
                    || stripe < 0 || startSequence < 0) {
                throw new IllegalArgumentException("invalid vector cross prewrite request");
            }
        }
    }

    /** 只在真实分片终态已落盘并核对后签发的跨分片最终化证明。 */
    public static final class IntentTerminalProof {
        private final String shardId;
        private final ParticipantRecord record;
        private final TransactionMetadataStore authority;

        /** 构造入口仅在同一适配器提交或回滚完成后开放。 */
        private IntentTerminalProof(String shardId, ParticipantRecord record,
                                    TransactionMetadataStore authority) {
            this.shardId = shardId;
            this.record = record;
            this.authority = authority;
        }

        /** 返回已持久完成终态的物理分片路由。 */
        public String shardId() { return shardId; }

        /** 返回已核对的参与者事务编号、快照与终态。 */
        public ParticipantRecord record() { return record; }

        /** 中心仅接受由同一持久控制账本下的适配器签发的证明。 */
        public boolean belongsTo(MysqlLocalFirstPublicationLedger ledger) {
            return authority instanceof MysqlLocalFirstMetadataView view && view.usesLedger(ledger);
        }
    }

    /** 构造稍后绑定中心权威的适配器；注入的数据源应自行配置 connectTimeout 和 socketTimeout。 */
    public MysqlStorageAdapter(String shardId, DataSource dataSource) {
        this(shardId, dataSource, null);
    }

    /** 构造能够解析中心决议的适配器；外部数据源的驱动超时不能由合作式恢复预算替代。 */
    public MysqlStorageAdapter(String shardId, DataSource dataSource, TransactionMetadataStore metadataStore) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.metadataStore = metadataStore;
    }

    /** 返回参与分片编号。 */
    @Override
    public String shardId() {
        return shardId;
    }

    /** 仅允许首次绑定或重复绑定同一实例，避免静默替换中心决议权威。 */
    @Override
    public synchronized void configureMetadataStore(TransactionMetadataStore store) {
        Objects.requireNonNull(store, "store");
        if (metadataStore != null && metadataStore != store) {
            throw new IllegalStateException("adapter already belongs to another metadata authority");
        }
        metadataStore = store;
    }

    /** 表示版本与参与者提交记录能够在同一本地事务中原子落盘。 */
    @Override
    public boolean supportsAtomicDirectCommit() {
        return true;
    }

    /** 只有绑定中心权威后才能解析已发布但尚未物化的意向。 */
    @Override
    public boolean supportsCommittedIntentResolution() {
        return metadataStore != null;
    }

    /** 初始化协议表与查询索引，不推断旧数据缺失的历史事务决议。 */
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
        } catch (SQLException ex) {
            throw storageError(null, "initialize schema failed", ex);
        }
    }

    /** 显式启用持久分条序号布局，不影响默认参与者表和旧事务入口。 */
    public synchronized void initializeVectorProtocol(int stripes) {
        MysqlVectorReservationStore store = new MysqlVectorReservationStore(dataSource, stripes);
        try {
            store.initialize();
            vectorReservations = store;
        } catch (SQLException ex) {
            throw storageError(null, "initialize vector reservation protocol failed", ex);
        }
    }

    /** 重启后只探测单键快速例程，缺失时拒绝以未初始化存储运行高性能账本。 */
    public void requireInitializedFastPath() {
        try (Connection connection = dataSource.getConnection();
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
        } catch (SQLException ex) {
            throw storageError(null, "inspect participant routines failed", ex);
        }
    }

    /** 只有探测到第二版例程后才能显式启用，旧版仍是默认物化路径。 */
    public void enableSingleKeyCommitV2() {
        if (!singleKeyCommitV2Ready) {
            throw illegal(0, "single-key commit v2 routine is not initialized");
        }
        useSingleKeyCommitV2 = true;
    }

    /** 安装单键预写例程，版本名固定以避免升级时改写正在执行的数据库代码。 */
    private void ensureSingleKeyPrewriteProcedure(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 安装单键意向物化例程，把版本、参与者终态及可选意向清理置于同一本地事务。 */
    private void ensureSingleKeyCommitProcedure(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 安装无需前台写集合的单键意向恢复例程，在分片事务内读取并物化本地意向。 */
    private void ensureSingleKeyIntentCommitProcedure(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 安装单键物化第二版，把常见首次提交的意向核对与版本写入合为一条语句。 */
    private void ensureSingleKeyCommitProcedureV2(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 查询真实持久化状态，不缓存 ACTIVE 或缺失记录。 */
    @Override
    public Optional<ParticipantRecord> participantRecord(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            return selectParticipant(connection, txId, false);
        } catch (SQLException ex) {
            throw storageError(txId, "read participant record failed", ex);
        }
    }

    /** 在同一本地快照读取版本和意向，保留点读墓碑语义。 */
    @Override
    public Optional<VersionedValue> get(ReadContext context, String key) {
        encodeKey(key);
        return readSnapshot(context, connection ->
                Optional.ofNullable(MysqlSnapshotReader.visibleValues(connection, context, List.of(key), metadataStore).get(key)));
    }

    /** 仅内部保护读入口可用固定向量点读，外部不能绕开读者租约访问已回收快照。 */
    Optional<VersionedValue> getVector(MysqlStripedSnapshotOracle.Snapshot snapshot,
                                       long requesterTxId, String key, int stripe) {
        encodeKey(key);
        if (vectorReservations == null || !(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(requesterTxId, "vector read requires initialized stripe layout and center authority");
        }
        for (int attempt = 0; ; attempt++) {
            try {
                return transaction(requesterTxId, true, connection -> {
                    requireFresh(requesterTxId, snapshot.decisionEpoch(), readWatermark(connection));
                    return MysqlProductVectorSnapshotReader.get(connection, snapshot,
                            shardId, key, stripe, requesterTxId, metadataStore);
                });
            } catch (MysqlSnapshotReader.MissingDecisionException missing) {
                // 旧本地 RR view 见到已清理意向时，关闭连接后按原全局向量重读一次。
                if (attempt == 1) {
                    throw uncertain(requesterTxId,
                            "vector intent decision still missing after rebuilding local snapshot", missing);
                }
            }
        }
    }

    /** 固定向量范围读仅供持有 GC 保护句柄的包内入口调用。 */
    ScanPage scanVectorPage(MysqlStripedSnapshotOracle.Snapshot snapshot, long requesterTxId,
                            KeyRange range, String afterKeyExclusive, int limit) {
        Objects.requireNonNull(range, "range");
        checkLimit(limit);
        if (range.startKeyInclusive() != null) encodeKey(range.startKeyInclusive());
        if (range.endKeyExclusive() != null) encodeKey(range.endKeyExclusive());
        if (afterKeyExclusive != null) encodeKey(afterKeyExclusive);
        if (vectorReservations == null || !(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(requesterTxId, "vector scan requires initialized stripe layout and center authority");
        }
        for (int attempt = 0; ; attempt++) {
            try {
                return transaction(requesterTxId, true, connection -> {
                    requireFresh(requesterTxId, snapshot.decisionEpoch(), readWatermark(connection));
                    return MysqlProductVectorSnapshotReader.page(connection, snapshot,
                            shardId, range, afterKeyExclusive, limit, requesterTxId, metadataStore);
                });
            } catch (MysqlSnapshotReader.MissingDecisionException missing) {
                if (attempt == 1) {
                    throw uncertain(requesterTxId,
                            "vector scan decision still missing after rebuilding local snapshot", missing);
                }
            }
        }
    }

    /** 循环合并有界页，需要流式消费的调用方应使用分页接口。 */
    @Override
    public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
        List<VersionedValue> values = new ArrayList<>();
        String after = null;
        do {
            ScanPage page = scanPage(context, keyRange, after, BATCH_SIZE);
            values.addAll(page.values());
            after = page.nextKey();
        } while (after != null);
        return values;
    }

    /** 批量读取一页原始键、最新可见版本及意向，返回前关闭所有 JDBC 资源。 */
    @Override
    public ScanPage scanPage(ReadContext context, KeyRange keyRange, String afterKeyExclusive, int limit) {
        checkLimit(limit);
        Objects.requireNonNull(keyRange, "keyRange");
        return readSnapshot(context, connection ->
                MysqlSnapshotReader.page(connection, context, keyRange, afterKeyExclusive, limit, metadataStore));
    }

    /** 决议缺失时仅重建一次本地 RR 快照，原全局 readTs、键范围和分页参数始终不变。 */
    private <T> T readSnapshot(ReadContext context, SqlWork<T> read) {
        for (int attempt = 0; ; attempt++) {
            try {
                return transaction(context.txId(), true, connection -> {
                    requireFresh(context.txId(), context.readTs(), readWatermark(connection));
                    return read.execute(connection);
                });
            } catch (MysqlSnapshotReader.MissingDecisionException missing) {
                // transaction 已回滚并关闭旧连接；第二次仍缺失时禁止返回缓存的旧值或空页。
                if (attempt == 1) {
                    throw uncertain(context.txId(), "intent decision still missing after rebuilding local snapshot", missing);
                }
            }
        }
    }

    /** 先保护事务终态及稳定键锁，再按有界批次检测冲突并原子写入整份意向。 */
    @Override
    public void prewrite(long txId, long startTs, List<Mutation> mutations) {
        List<Mutation> writes = normalize(mutations);
        prewriteNormalized(txId, startTs, writes);
    }

    /** 跨分片新协议只对非空完整写集合返回不可由外部构造的持久预写证明。 */
    public PrewriteProof prewriteWithProof(long txId, long startTs, List<Mutation> mutations) {
        List<Mutation> writes = normalize(mutations);
        TransactionMetadataStore authority = metadataStore;
        if (writes.isEmpty() || !(authority instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "cross-shard prewrite proof requires nonempty writes and local-first authority");
        }
        prewriteNormalized(txId, startTs, writes);
        return new PrewriteProof(txId, startTs, shardId, writes.size(), authority);
    }

    /** 向量跨片预写把真实意向与分条序号在同一分片事务中落盘后签发证明。 */
    public PrewriteProof prewriteWithVectorProof(long txId, long startTs, Mutation mutation,
                                                int stripe, long startSequence) {
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
        } catch (SQLException ex) {
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

    /** 只供容量对照保留原多条 JDBC SQL 的向量预写实现。 */
    PrewriteProof prewriteWithVectorProofJavaForProbe(long txId, long startTs, Mutation mutation,
                                                     int stripe, long startSequence) {
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
            // 保留序号和真实 mvcc_intent 共用当前 JDBC 事务及同一稳定键锁。
            return reservations.reserve(connection, txId, startTs, mutation, stripe, startSequence);
        });
        return new PrewriteProof(txId, startTs, shardId, 1, authority,
                new VectorReservation(mutation.key(), reserved.stripe(),
                        reserved.startSequence(), reserved.reservedSequence()));
    }

    /** 服务端批量预写一次物理事务，每笔成功后仍返回独立真实分片证明。 */
    public List<PrewriteProof> prewriteVectorBatchWithProof(List<VectorPrewriteRequest> requests) {
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
        } catch (SQLException ex) {
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

    /** 响应丢失或重启后从持久分片行读取真实预写序号。 */
    public Optional<VectorReservation> vectorReservation(long txId, String key) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "vector reservation protocol is not initialized");
        }
        try {
            return reservations.find(txId, key).map(found ->
                    new VectorReservation(found.key(), found.stripe(),
                            found.startSequence(), found.reservedSequence()));
        } catch (SQLException ex) {
            throw storageError(txId, "read vector reservation failed", ex);
        }
    }

    /** 恢复器读取已随真实 mvcc_data 原子物化的分条版本映射。 */
    public Optional<VectorReservation> committedVectorReservation(long txId, String key) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "vector reservation protocol is not initialized");
        }
        try {
            return reservations.findCommitted(txId, key).map(found ->
                    new VectorReservation(found.reservation().key(), found.reservation().stripe(),
                            found.reservation().startSequence(), found.reservation().reservedSequence()));
        } catch (SQLException ex) {
            throw storageError(txId, "read committed vector reservation failed", ex);
        }
    }

    /** 读取真实参与者的分条水位，用于证明重放不会二次取号。 */
    public long vectorSequence(int stripe) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector reservation protocol is not initialized", false, null);
        }
        try {
            return reservations.sequence(stripe);
        } catch (SQLException ex) {
            throw storageError(null, "read vector stripe sequence failed", ex);
        }
    }

    /** 单分片单键 1PC 不写中心元数据，只在本地锁键后原子保存分条版本。 */
    public long directCommitVectorLocal(long txId, Mutation mutation, int stripe,
                                        long startSequence) {
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(txId, "local vector commit requires initialized stripe layout");
        }
        List<Mutation> writes = normalize(List.of(Objects.requireNonNull(mutation, "mutation")));
        try {
            return reservations.commitLocalFast(txId, writes.getFirst(), stripe, startSequence);
        } catch (SQLException ex) {
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

    /** 容量对照保留原 Java 多语句实现，生产入口使用服务器端单次调用。 */
    long directCommitVectorLocalJavaForProbe(long txId, Mutation mutation, int stripe,
                                             long startSequence) {
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

    /** 独立逻辑写入共用一个 InnoDB 提交，任一冲突使整批回滚且不确认部分成员。 */
    public List<Long> directCommitVectorLocalBatch(List<LocalVectorCommitRequest> requests) {
        MysqlVectorReservationStore reservations = vectorReservations;
        List<LocalVectorCommitRequest> fixed = validateLocalVectorBatch(requests, reservations);
        try {
            return reservations.commitLocalBatchFast(fixed);
        } catch (SQLException ex) {
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

    /** 容量对照保留旧 Java 组提交实现，验证服务端组批是否真正减少固定成本。 */
    List<Long> directCommitVectorLocalBatchJavaForProbe(List<LocalVectorCommitRequest> requests) {
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

    /** 组批前固定调用顺序并拒绝重复事务或键，避免一个业务键被两个成员竞争。 */
    private List<LocalVectorCommitRequest> validateLocalVectorBatch(
            List<LocalVectorCommitRequest> requests, MysqlVectorReservationStore reservations) {
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

    /** COMMIT 响应未知时按持久单键版本核对身份，缺行表示尚无已证明的提交。 */
    public OptionalLong verifyLocalVectorCommit(LocalVectorCommitRequest request) {
        Objects.requireNonNull(request, "request");
        MysqlVectorReservationStore reservations = vectorReservations;
        if (reservations == null) {
            throw illegal(request.txId(), "local vector proof requires initialized stripe layout");
        }
        try {
            return reservations.verifyLocalCommit(request);
        } catch (SQLException ex) {
            throw storageError(request.txId(), "verify local vector commit failed", ex);
        }
    }

    /** 单键例程和多键事务都在本地提交成功后才返回调用者。 */
    private void prewriteNormalized(long txId, long startTs, List<Mutation> writes) {
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

    /** 单键预写用一次服务器调用完成稳定键锁、冲突校验与意向落盘。 */
    private void prewriteSingleKey(long txId, long startTs, Mutation mutation) {
        try (Connection connection = dataSource.getConnection();
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
        } catch (SQLException ex) {
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

    /** 检查持久意向并冻结预写集合，重复准备保持幂等。 */
    @Override
    public void prepare(long txId) {
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

    /** 按中心提交决议物化全部持久意向，重放只接受相同提交时间戳。 */
    @Override
    public void commit(long txId, long commitTs) {
        commitTransaction(txId, commitTs, null, true);
    }

    /** 恢复物化成功并复核本地终态后，向中心返回不可伪造的最终化证明。 */
    public IntentTerminalProof commitIntentWithProof(long txId, long commitTs) {
        if (!(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "cross-shard terminal proof requires local-first authority");
        }
        Optional<Mutation> singleIntent = singleIntendedMutation(txId);
        if (singleIntent.isPresent()) {
            // 单键恢复复用分片存储过程；例程会再次核对完整持久意向与中心决议。
            commit(txId, commitTs, List.of(singleIntent.get()));
        } else {
            commit(txId, commitTs);
        }
        ParticipantRecord record = participantRecord(txId).orElseThrow(() ->
                illegal(txId, "committed intent has no durable participant terminal"));
        if (record.status() != TxStatus.COMMITTED || !Objects.equals(record.commitTs(), commitTs)) {
            throw illegal(txId, "committed intent terminal differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /** 向量跨片物化在同一事务写真实版本、保留序号映射和参与者终态。 */
    public IntentTerminalProof commitVectorIntentWithProof(long txId, long commitTs, Mutation expected) {
        if (!(metadataStore instanceof MysqlLocalFirstMetadataView) || vectorReservations == null) {
            throw illegal(txId, "vector materialization requires local-first authority and stripe layout");
        }
        commitTransaction(txId, commitTs,
                normalize(List.of(Objects.requireNonNull(expected, "expected"))), true, null, true);
        ParticipantRecord record = participantRecord(txId).orElseThrow(() ->
                illegal(txId, "vector materialization has no durable participant terminal"));
        if (record.status() != TxStatus.COMMITTED || !Objects.equals(record.commitTs(), commitTs)) {
            throw illegal(txId, "vector materialization terminal differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /** 恢复器从持久意向或已物化版本还原单键写入，再走向量原子终态。 */
    public IntentTerminalProof commitVectorIntentWithProof(CommittedIntentProof proof) {
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
                            TxStatus.COMMITTED, decision.commitTs()), metadataStore);
        } catch (SQLException ex) {
            throw switch (ex.getErrorCode()) {
                case 39001, 39002, 39003 -> new MvccException(ErrorCode.ILLEGAL_STATE,
                        "vector recovery lacks matching durable participant proof", false, decision.txId(), ex);
                case 39004 -> new MvccException(ErrorCode.TRANSACTION_ABORTED,
                        "vector recovery encountered an aborted participant", false, decision.txId(), ex);
                default -> uncertain(decision.txId(), "vector recovery outcome requires durable lookup", ex);
            };
        }
    }

    /** 多分片复用同一中心决议证明，单键路径省去每个分片重复查询中心。 */
    public IntentTerminalProof commitIntentWithProof(CommittedIntentProof proof) {
        Objects.requireNonNull(proof, "proof");
        TxRecord decision = proof.record();
        if (!proof.belongsTo(metadataStore) || decision.status() != TxStatus.COMMITTED
                || decision.commitTs() == null || !decision.participants().contains(shardId)) {
            throw illegal(decision.txId(), "cross-shard committed proof has wrong authority or participant");
        }
        if (singleKeyIntentCommitReady && commitSingleIntended(decision)) {
            // 例程完成后本地版本与参与者终态已经原子持久化，可直接签发证明。
            return new IntentTerminalProof(shardId,
                    new ParticipantRecord(decision.txId(), decision.startTs(),
                            TxStatus.COMMITTED, decision.commitTs()), metadataStore);
        }
        commit(decision.txId(), decision.commitTs());
        ParticipantRecord record = participantRecord(decision.txId()).orElseThrow(() ->
                illegal(decision.txId(), "committed intent has no durable participant terminal"));
        if (record.startTs() != decision.startTs() || record.status() != TxStatus.COMMITTED
                || !Objects.equals(record.commitTs(), decision.commitTs())) {
            throw illegal(decision.txId(), "committed intent terminal differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /** 单键意向只读两行判定，重启恢复也能安全选择服务器端原子物化例程。 */
    private Optional<Mutation> singleIntendedMutation(long txId) {
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
                        ? Mutation.delete(key) : Mutation.put(key, row.getBytes("value"));
                return row.next() ? Optional.empty() : Optional.of(mutation);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "read single cross-shard intent for recovery failed", ex);
        }
    }

    /** 校验完整已知写集合与持久意向一致后提交并清理意向。 */
    @Override
    public void commit(long txId, long commitTs, List<Mutation> mutations) {
        commit(txId, commitTs, mutations, true);
    }

    /** 校验真实意向所有权和值后物化，允许保留已提交意向交由后台清理。 */
    @Override
    public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
        commitTransaction(txId, commitTs, normalize(mutations), cleanupIntents);
    }

    /** 仅接受分段账本成功落盘后产生的凭据，省去每个分片重复查询中心决议。 */
    public void commitDecided(CommittedProof proof, List<Mutation> mutations, boolean cleanupIntents) {
        Objects.requireNonNull(proof, "proof");
        if (!proof.belongsTo(metadataStore) || !proof.participants().contains(shardId)
                || proof.startTs() >= proof.commitTs()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "committed proof does not authorize this participant", false, proof.txId());
        }
        commitTransaction(proof.txId(), proof.commitTs(), normalize(mutations), cleanupIntents, proof);
    }

    /** 在本地事务中校验中心决议、冻结写集合并原子提交版本与终态。 */
    private void commitTransaction(long txId, long commitTs, List<Mutation> expected, boolean cleanup) {
        commitTransaction(txId, commitTs, expected, cleanup, null);
    }

    /** 普通恢复读取持久决议；前台已确认的中心提交使用账本凭据。 */
    private void commitTransaction(long txId, long commitTs, List<Mutation> expected,
                                   boolean cleanup, CommittedProof proof) {
        commitTransaction(txId, commitTs, expected, cleanup, proof, false);
    }

    /** 向量路径禁用旧单键例程，并要求版本与保留序号一同持久提交。 */
    private void commitTransaction(long txId, long commitTs, List<Mutation> expected,
                                   boolean cleanup, CommittedProof proof, boolean vectorRequired) {
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

    /** 已确认中心决议后，用一次本地调用物化单键版本和参与者终态。 */
    private void commitSingleKey(long txId, long startTs, long commitTs, Mutation expected, boolean cleanup) {
        String routine = useSingleKeyCommitV2 ? "mvcc_commit_one_v2" : "mvcc_commit_one_v1";
        try (Connection connection = dataSource.getConnection();
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
        } catch (SQLException ex) {
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

    /** 一次服务器调用从本地持久单键意向生成版本与终态，多键返回通用路径。 */
    private boolean commitSingleIntended(TxRecord decision) {
        try (Connection connection = dataSource.getConnection();
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
        } catch (SQLException ex) {
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

    /** 原子保存单分片版本与决议；调用者须先发布 PREPARING 候选时间并持有发布屏障。 */
    @Override
    public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
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

    /** 前台本地提交成功后才产生绑定分片实例的证明，避免中心完成时再次读取分片。 */
    public static final class LocalCommitProof {
        private final MysqlShardSegmentFence fence;
        private final MysqlShardSegmentFence.CandidateProof candidate;

        /** 私有构造保证调用方不能用自建候选记录冒充已成功提交。 */
        private LocalCommitProof(MysqlShardSegmentFence fence,
                                 MysqlShardSegmentFence.CandidateProof candidate) {
            this.fence = fence;
            this.candidate = candidate;
        }

        /** 中心只接受与本次物理分片实例相同的提交凭据。 */
        public boolean belongsTo(MysqlShardSegmentFence expected) {
            return fence == expected;
        }

        /** 返回本地事务原子保存的候选身份供中心核对槽和租约。 */
        public MysqlShardSegmentFence.CandidateProof candidate() {
            return candidate;
        }
    }

    /** 本地先提交兼容入口，忽略成功后生成的前台证明。 */
    void directCommitFenced(MysqlShardSegmentFence fence, PublicationSegmentLease lease,
                            long txId, long startTs, long commitTs, long deadlineMillis,
                            IsolationLevel isolationLevel, List<Mutation> mutations) {
        directCommitFencedWithProof(fence, lease, txId, startTs, commitTs,
                deadlineMillis, isolationLevel, mutations);
    }

    /** 只在分片事务已持久提交后返回不可外部构造的前台候选证明。 */
    LocalCommitProof directCommitFencedWithProof(MysqlShardSegmentFence fence, PublicationSegmentLease lease,
                                                 long txId, long startTs, long commitTs, long deadlineMillis,
                                                 IsolationLevel isolationLevel, List<Mutation> mutations) {
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

    /** 按完整持久化证据清理意向，不依赖调用方内存键列表。 */
    @Override
    public void cleanupIntents(long txId, List<Mutation> mutations) {
        cleanupIntents(txId);
    }

    /** 仅清理本地已提交且已有匹配版本的意向，禁止删除未决写入。 */
    @Override
    public void cleanupIntents(long txId) {
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

    /** 原子回滚并隔离迟到请求，已提交事务保持原决议且幂等返回成功。 */
    @Override
    public void abort(long txId) {
        if (metadataStore instanceof MysqlLocalFirstMetadataView localFirst) {
            // 中心到期判断与决议事务行锁串行，防止前台预写证明被提前物理回滚。
            localFirst.assertIntentAbortAllowed(txId);
        }
        Optional<TxRecord> central = decision(txId);
        long startTs = central.map(TxRecord::startTs).orElse(txId);
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            // 新协议的 txId 可独立于快照时间；无意向回滚仍要保留准确的开始时间证明。
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

    /** 中心 ABORTED 决议下的物理回滚完成后签发分片最终化证明。 */
    public IntentTerminalProof abortIntentWithProof(long txId) {
        if (!(metadataStore instanceof MysqlLocalFirstMetadataView)) {
            throw illegal(txId, "cross-shard abort proof requires local-first authority");
        }
        abort(txId);
        ParticipantRecord record = participantRecord(txId).orElseThrow(() ->
                illegal(txId, "aborted intent has no durable participant terminal"));
        if (record.status() != TxStatus.ABORTED || record.commitTs() != null) {
            throw illegal(txId, "aborted intent participant differs from center decision");
        }
        return new IntentTerminalProof(shardId, record, metadataStore);
    }

    /** 查找残留意向和本地未发布决议，保留扫描进度避免旧终态占满所有恢复批次。 */
    @Override
    public synchronized Set<Long> findPendingTransactionIds(int limit) {
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
        } catch (SQLException ex) {
            throw storageError(null, "scan pending participants failed", ex);
        }
    }

    /** 独占本地水位后有界清理历史版本、墓碑链及拥有安全回收证据的本地终态。 */
    @Override
    public synchronized int gc(long safePoint, int limit) {
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
            try (PreparedStatement statement = connection.prepareStatement(
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

    /** 重启后的适配器也须依据分片持久布局拒绝旧标量 GC，不能只看进程内标志。 */
    private boolean durableVectorLayoutExists() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select 1 from information_schema.tables
                     where table_schema = database()
                       and table_name = 'mvcc_vector_layout_v1'
                       and table_type = 'BASE TABLE'
                     limit 1
                     """); ResultSet row = query.executeQuery()) {
            return row.next();
        } catch (SQLException ex) {
            throw storageError(null, "inspect durable vector GC layout failed", ex);
        }
    }

    /** 未完成中心发布的本地候选即使事务租约已失效，也固定分片 GC 下界。 */
    private Long localCandidateFloor(Connection connection) throws SQLException {
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

    /** 显式设置并恢复连接隔离级别，提交响应异常始终按结果未知处理。 */
    private <T> T transaction(Long txId, boolean snapshot, SqlWork<T> work) {
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
            } catch (SQLException | RuntimeException | Error ex) {
                failure = ex;
                try {
                    connection.rollback();
                    ended = true;
                } catch (SQLException rollbackFailure) {
                    ex.addSuppressed(rollbackFailure);
                }
                // 进入 COMMIT 后即使随后 ROLLBACK 返回成功，也不能证明先前 COMMIT 未生效。
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
                    } catch (SQLException restoreFailure) {
                        if (failure != null) {
                            failure.addSuppressed(restoreFailure);
                        } else {
                            throw uncertain(txId, "connection restore failed after transaction", restoreFailure);
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw uncertain(txId, "connection acquisition or close failed", ex);
        }
    }

    /** 用普通读取建立 RR 快照，后续版本和意向受到同一 read view 及 InnoDB undo 保护。 */
    private long readWatermark(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select safe_point from mvcc_local_safe_point where singleton_id = 1");
             ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) {
                throw new IllegalStateException("local safe point has not been initialized");
            }
            return rows.getLong(1);
        }
    }

    /** 写入先持共享水位锁，GC 持独占锁，防止检查后与清理交错。 */
    private long lockWatermark(Connection connection, boolean exclusive) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select safe_point from mvcc_local_safe_point where singleton_id = 1 " + (exclusive ? "for update" : "for share"));
             ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) {
                throw new IllegalStateException("local safe point has not been initialized");
            }
            return rows.getLong(1);
        }
    }

    /** 创建或锁定事务行，重复请求不修改已有开始时间及终态。 */
    private ParticipantRecord lockParticipant(Connection connection, long txId, long startTs) throws SQLException {
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

    /** 锁定必须已存在的事务记录，禁止缺证据的提交复活已回收事务。 */
    private ParticipantRecord requireParticipant(Connection connection, long txId) throws SQLException {
        return selectParticipant(connection, txId, true)
                .orElseThrow(() -> illegal(txId, "participant record missing or reclaimed"));
    }

    /** 查询参与者记录，可选择锁定行以串行化前台和恢复请求。 */
    private Optional<ParticipantRecord> selectParticipant(Connection connection, long txId, boolean lock) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select tx_id, start_ts, status, commit_ts from mvcc_participant_txn where tx_id = ?" + (lock ? " for update" : ""))) {
            statement.setLong(1, txId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(participant(rows)) : Optional.empty();
            }
        }
    }

    /** 构造保留可空提交时间戳的本地记录。 */
    private ParticipantRecord participant(ResultSet rows) throws SQLException {
        long commitTs = rows.getLong("commit_ts");
        Long timestamp = rows.wasNull() ? null : commitTs;
        return new ParticipantRecord(rows.getLong("tx_id"), rows.getLong("start_ts"),
                TxStatus.valueOf(rows.getString("status")), timestamp);
    }

    /** 在已经持有事务行锁且校验过终态时更新参与者状态。 */
    private void updateParticipant(Connection connection, long txId, TxStatus status, Long commitTs) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
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

    /** 按无符号 UTF-8 顺序获取持久键行排他锁，空键和从未写入的键同样受保护。 */
    private void lockKeys(Connection connection, List<String> keys) throws SQLException {
        List<String> ordered = keys.stream().distinct().sorted(Utf8KeyOrder::compare).toList();
        for (int offset = 0; offset < ordered.size(); offset += BATCH_SIZE) {
            List<String> chunk = ordered.subList(offset, Math.min(offset + BATCH_SIZE, ordered.size()));
            String sql = "insert into mvcc_key_lock(user_key) values "
                    + chunk.stream().map(ignored -> "(?)").collect(Collectors.joining(", "))
                    + " on duplicate key update user_key = user_key";
            // INSERT 按 VALUES 的二进制顺序锁行，不能依赖不存在的意向行或间隙锁。
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int index = 0; index < chunk.size(); index++) {
                    statement.setBytes(index + 1, encodeKey(chunk.get(index)));
                }
                statement.executeUpdate();
            }
        }
    }

    /** 在稳定键锁内批量核对新版本、外部意向及既有回滚标记。 */
    private void checkWriteConflicts(Connection connection, long txId, long startTs, List<Mutation> writes) throws SQLException {
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
            try (PreparedStatement statement = connection.prepareStatement(
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
            try (PreparedStatement statement = connection.prepareStatement(
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

    /** 批量写入已经核验归属的意向，整份写集合共享一个本地事务。 */
    private void writeIntents(Connection connection, long txId, long startTs, List<Mutation> writes) throws SQLException {
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

    /** 加载本事务实际持久化的完整意向，顺序由 VARBINARY 索引确定。 */
    private List<IntentRow> loadIntents(Connection connection, long txId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
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

    /** 验证事务及开始时间确实拥有全部待物化意向。 */
    private void verifyIntentStart(ParticipantRecord record, List<IntentRow> rows) {
        for (IntentRow row : rows) {
            if (row.txId() != record.txId() || row.startTs() != record.startTs()) {
                throw illegal(record.txId(), "intent ownership differs from participant record");
            }
        }
    }

    /** 按完整集合比较键、删除标记和字节值，禁止以内存写集合补造缺失意向。 */
    private void verifyMutations(long txId, List<IntentRow> rows, List<Mutation> expected) {
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

    /** 只插入不可变版本，主键冲突使整个本地事务回滚，绝不覆盖其他版本。 */
    private void insertVersions(Connection connection, long txId, long commitTs, List<IntentRow> rows) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
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

    /** 对已知写集合的提交重放核对原事务原时间戳下的全部持久版本。 */
    private void verifyCommittedWrites(Connection connection, ParticipantRecord record, List<Mutation> expected) throws SQLException {
        lockKeys(connection, expected.stream().map(Mutation::key).toList());
        verifyMutations(record.txId(), loadCommittedRows(connection, record), expected);
    }

    /** 查询本事务原提交时间戳的版本，其他事务或其他时间戳不视作等价重放。 */
    private List<IntentRow> loadCommittedRows(Connection connection, ParticipantRecord record) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
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

    /** 清理前证明全部残留意向已有同事务同值的版本，保留缺失物化证据的意向。 */
    private void cleanupCommittedIntents(Connection connection, ParticipantRecord record) throws SQLException {
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

    /** 将意向转成幂等回滚标记，重放不刷新保留时间。 */
    private void writeRollbacks(Connection connection, long txId, List<IntentRow> rows) throws SQLException {
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

    /** 在持有事务锁和全部相关键锁时删除本事务的意向。 */
    private void deleteIntents(Connection connection, long txId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("delete from mvcc_intent where tx_id = ?")) {
            statement.setLong(1, txId);
            statement.executeUpdate();
        }
    }

    /** 判断事务是否仍有待处理意向。 */
    private boolean hasIntents(Connection connection, long txId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select 1 from mvcc_intent where tx_id = ? limit 1")) {
            statement.setLong(1, txId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    /** 按持久游标处理有界原始键页，所有删除共享预算，未回收键也推进游标。 */
    private int gcVersions(Connection connection, long safePoint, int limit) throws SQLException {
        byte[] after;
        try (PreparedStatement statement = connection.prepareStatement(
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
        // 长版本链本轮耗尽预算后也前进，下一轮巡回继续清理，避免某一热键饿死后续键。
        byte[] next = processed == keys.size() ? null : last;
        try (PreparedStatement statement = connection.prepareStatement(
                "update mvcc_local_safe_point set gc_after_key = ? where singleton_id = 1")) {
            statement.setBytes(1, next);
            statement.executeUpdate();
        }
        return reclaimed;
    }

    /** 每个来源只读取一页 keyset 候选键，不在全表版本链上聚合后截断。 */
    private List<byte[]> collectGcKeys(Connection connection, byte[] after, int limit) throws SQLException {
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

    /** 清理一个键的历史版本或墓碑链，保留仍有意向的键以免丢失物化校验证据。 */
    private int gcKey(Connection connection, byte[] key, long safePoint, int limit) throws SQLException {
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
        try (PreparedStatement statement = connection.prepareStatement(
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
            try (PreparedStatement statement = connection.prepareStatement(
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
            try (PreparedStatement statement = connection.prepareStatement(
                    "delete from mvcc_data where user_key = ? and commit_ts" + operator + " order by commit_ts limit ?")) {
                statement.setBytes(1, key);
                statement.setLong(2, boundary);
                statement.setInt(3, limit);
                // 升序删除确保最后才移除墓碑，不让分页清理期间的旧值重新可见。
                reclaimed = statement.executeUpdate();
            }
        }
        if (reclaimed < limit) {
            // 水位排他锁阻止任何写入者获取或重建此键锁，安全地回收已空业务键的稳定锁行。
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

    /** 在获取分片锁之前查询中心完成证据，避免分片锁与发布屏障形成反向等待。 */
    private List<ParticipantRecord> gcCandidates(long safePoint, int limit) {
        TransactionMetadataStore authority = metadataStore;
        if (authority == null) {
            return List.of();
        }
        // 中心下界包含所有尚未 finalized 的终态，低于此下界才允许删除本地恢复证据。
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
        } catch (SQLException ex) {
            throw storageError(null, "read terminal GC candidates failed", ex);
        }
        terminalGcAfter = records.size() < limit ? Long.MIN_VALUE : records.getLast().txId();
        // 中心已删除只可能发生于 finalized 之后；本地水位在删除记录之前原子持久化以阻挡旧请求。
        return records.stream().filter(record -> {
            Optional<TxRecord> central = decision(record.txId());
            return central.isEmpty() || matchesTerminalDecision(record, central.get());
        }).toList();
    }

    /** 在独占水位内重新核对候选记录，按共享行数预算清理回滚标记和本地终态。 */
    private int gcTerminalRecords(Connection connection, List<ParticipantRecord> records, int limit) throws SQLException {
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

    /** 中心需保留相同终态和开始时间，缺少证据时本地记录继续保留。 */
    private boolean hasMatchingTerminalDecision(ParticipantRecord local) {
        if (local.status() != TxStatus.COMMITTED && local.status() != TxStatus.ABORTED) {
            return false;
        }
        return decision(local.txId()).filter(record -> matchesTerminalDecision(local, record)).isPresent();
    }

    /** 核对中心与本地终态完全一致，任何不匹配都禁止回收本地证据。 */
    private boolean matchesTerminalDecision(ParticipantRecord local, TxRecord central) {
        return central.startTs() == local.startTs() && central.status() == local.status()
                && Objects.equals(central.commitTs(), local.commitTs());
    }

    /** 查询当前绑定权威，不跨请求缓存不存在或非终态决议。 */
    private Optional<TxRecord> decision(long txId) {
        TransactionMetadataStore store = metadataStore;
        try {
            return store == null ? Optional.empty() : store.get(txId);
        } catch (RuntimeException ex) {
            throw uncertain(txId, "metadata decision lookup failed", ex);
        }
    }

    /** 意向物化必须引用中心 COMMITTED 决议，不能以内存写集合替代发布协议。 */
    private void requireCommitDecision(ParticipantRecord local, long commitTs,
                                       Optional<TxRecord> central, CommittedProof proof) {
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

    /** 检查事务终态不可逆，回滚后的迟到请求不能复活意向。 */
    private void requireNonTerminal(ParticipantRecord record) {
        if (record.status() == TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "participant is ABORTED", false, record.txId());
        }
        if (record.status() == TxStatus.COMMITTED) {
            throw illegal(record.txId(), "participant is already COMMITTED");
        }
    }

    /** 提交时间严格晚于开始时间且重放不改变原提交时间。 */
    private void validateCommitTimestamp(ParticipantRecord record, long commitTs) {
        if (commitTs <= record.startTs()
                || record.status() == TxStatus.COMMITTED && !Objects.equals(record.commitTs(), commitTs)) {
            throw illegal(record.txId(), "commitTs is invalid or differs from committed participant");
        }
    }

    /** 拒绝安全水位之前的请求，水位本身仍属于合法可见边界。 */
    private void requireFresh(long txId, long timestamp, long safePoint) {
        if (timestamp < safePoint) {
            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD, "timestamp precedes local safe point " + safePoint, false, txId);
        }
    }

    /** 固定写集合字节并按二进制键序去重，重复键以最后一次变更为准。 */
    private List<Mutation> normalize(List<Mutation> mutations) {
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

    /** 构造兼容既有调用的批量版本冲突查询。 */
    static String directCommitConflictSql(int keyCount) {
        if (keyCount < 1) {
            throw new IllegalArgumentException("keyCount must be >= 1");
        }
        return "select user_key from mvcc_data where user_key in (" + placeholders(keyCount) + ") and commit_ts > ? limit 1";
    }

    /** 返回既有调用所需的有界冲突检查批次数。 */
    static int directCommitConflictChunkCount(int keyCount) {
        if (keyCount < 1) {
            throw new IllegalArgumentException("keyCount must be >= 1");
        }
        return 1 + (keyCount - 1) / BATCH_SIZE;
    }

    /** 生成 JDBC 参数占位符，不插入业务文本。 */
    private static String placeholders(int count) {
        return IntStream.range(0, count).mapToObj(ignored -> "?").collect(Collectors.joining(", "));
    }

    /** 绑定一个写入批次的二进制键参数。 */
    private void bindMutationKeys(PreparedStatement statement, List<Mutation> writes) throws SQLException {
        for (int index = 0; index < writes.size(); index++) {
            statement.setBytes(index + 1, encodeKey(writes.get(index).key()));
        }
    }

    /** 显式区分 SQL NULL 与二进制空值。 */
    private void bindValue(PreparedStatement statement, int parameter, byte[] value) throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.BLOB);
        } else {
            statement.setBytes(parameter, value);
        }
    }

    /** 校验分页和清理批次的统一有界参数。 */
    private void checkLimit(int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("limit must be in 1..4096");
        }
    }

    /** 编码数据库 VARBINARY 键，拒绝截断或非法字符引起的键碰撞。 */
    private static byte[] encodeKey(String key) {
        byte[] encoded = Utf8KeyOrder.encode(key);
        if (encoded.length > 256) {
            throw new IllegalArgumentException("UTF-8 key exceeds 256 bytes");
        }
        return encoded;
    }

    /** 将数据库二进制键恢复为业务字符串。 */
    private static String decodeKey(byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    /** 构造非重试的协议状态错误。 */
    private MvccException illegal(long txId, String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, txId);
    }

    /** 显式标记提交或连接生命周期未知，禁止上层把它误认作肯定未提交的冲突。 */
    private MvccException uncertain(Long txId, String message, Throwable cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, shardId + ": " + message, false, txId, cause);
    }

    /** 保留 SQL 状态及底层原因，调用方仅在已确认回滚时使用冲突映射。 */
    private MvccException storageError(Long txId, String message, SQLException ex) {
        return MysqlStorageExceptionMapper.toMvccException(shardId, txId, message, ex);
    }

    /** 为既有表补查询索引，并容忍多个初始化者同时添加相同索引。 */
    private void ensureIndex(Connection connection, String table, String name, String columns) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1061) {
                throw ex;
            }
        }
    }

    /** 为已存在的本地水位表补持久扫描游标，兼容重复初始化和并发建表。 */
    private void ensureGcCursorColumn(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select 1 from information_schema.columns
                where table_schema = database() and table_name = 'mvcc_local_safe_point' and column_name = 'gc_after_key'
                """); ResultSet rows = statement.executeQuery()) {
            if (rows.next()) {
                return;
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "alter table mvcc_local_safe_point add column gc_after_key varbinary(256) null")) {
            statement.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1060) {
                throw ex;
            }
        }
    }

    /** 受本地事务包装器管理的 JDBC 工作单元。 */
    @FunctionalInterface
    private interface SqlWork<T> {
        /** 在调用者提供的事务连接中执行工作。 */
        T execute(Connection connection) throws SQLException;
    }

    /** 携带真实所有权及完整字节值的意向或版本校验行。 */
    private record IntentRow(String key, long txId, long startTs, boolean deleted, byte[] value) {
    }
}

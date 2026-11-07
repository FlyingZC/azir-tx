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
 * 基于 MySQL 的事务元数据存储实现，负责事务状态表的读写。
 */
public final class MysqlTransactionMetadataStore implements TransactionMetadataStore,
        FastTransactionBegin, FastTransactionCommit {
    private final DataSource dataSource;
    private final int publicationLockTimeoutSeconds;
    private volatile boolean beginProcedureReady;
    private volatile boolean beginProcedureV2Ready;
    private volatile boolean commitProcedureReady;
    private volatile boolean commitProcedureV2Ready;
    private volatile boolean proceduresChecked;

    /**
     * 构造 MysqlTransactionMetadataStore 实例。
     */
    public MysqlTransactionMetadataStore(DataSource dataSource) {
        this(dataSource, 10);
    }

    /** 配置发布锁等待秒数；主备不丢失已确认控制面写是部署前提，本类不实现 HA。 */
    public MysqlTransactionMetadataStore(DataSource dataSource, int publicationLockTimeoutSeconds) {
        if (publicationLockTimeoutSeconds < 0) {
            throw new IllegalArgumentException("publicationLockTimeoutSeconds must be >= 0");
        }
        this.dataSource = dataSource;
        this.publicationLockTimeoutSeconds = publicationLockTimeoutSeconds;
    }

    /** 新旧控制组件只在确认为同一数据源实例时才能共享事务权威。 */
    public boolean usesDataSource(DataSource candidate) {
        return dataSource == candidate;
    }

    /** 旧事务管理器启动时确认控制库未进入账本或分段模式。 */
    public void assertLegacyProtocolActive() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError(null, "inspect legacy publication mode failed", ex);
        }
    }

    /** 按数据库域串行发布，嵌套调用共享持锁连接。 */
    @Override
    public <T> T withPublicationLock(Supplier<T> action) {
        return MysqlConnectionContext.withPublicationLock(dataSource, publicationLockTimeoutSeconds, action);
    }

    /** 用数据库毫秒墙钟统一不同协调节点的租约和恢复判断。 */
    @Override
    public long currentTimeMillis() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement statement = connection.prepareStatement(
                     "select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)");
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("database clock returned no row");
            }
            return result.getLong(1);
        } catch (SQLException ex) {
            throw storageError(null, "load database clock failed", ex);
        }
    }

    /** 在远端单分片提交前持久化占位，GET_LOCK 丢失时仍阻止其他节点发新快照。 */
    @Override
    public void markLocalCommitPending(long txId, long commitTs) {
        withPublicationLock(() -> {
            TxRecord record = get(txId).orElseThrow(() ->
                    new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId));
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

    /** 本地提交未知槽存在时拒绝发布，不标记可重试以免业务自动重新执行。 */
    @Override
    public void assertNoPendingPublications() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement statement = connection.prepareStatement(
                     "select tx_id from mvcc_txn where status = 'PREPARING' and commit_ts is not null limit 1");
             ResultSet result = statement.executeQuery()) {
            if (result.next()) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "local publication requires recovery", false, result.getLong(1));
            }
        } catch (SQLException ex) {
            throw storageError(null, "inspect pending publications failed", ex);
        }
    }

    /**
     * 初始化所需的数据库表结构或运行环境。
     */
    public void initialize() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
            try (PreparedStatement snapshots = connection.prepareStatement("""
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
        } catch (SQLException ex) {
            throw storageError(null, "initialize metadata schema failed", ex);
        }
    }

    /** 重启后新建的控制面实例只探测一次已有例程，无需重复执行模式初始化。 */
    private synchronized void detectInstalledProcedures() {
        if (proceduresChecked) {
            return;
        }
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError(null, "inspect control procedures failed", ex);
        }
    }

    /** 安装版本化的 MySQL 事务开始例程，独立实例并发初始化时接受已创建结果。 */
    private void ensureBeginProcedure(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 新版 begin 在服务器端获取同名发布锁，避免客户端逐条取锁与归属探测。 */
    private void ensureBeginProcedureV2(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 安装提交决议例程；编号变化时可并存旧版本供滚动升级。 */
    private void ensureCommitProcedure(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 新版提交在服务器端完成命名锁、时间戳、决议与锁释放。 */
    private void ensureCommitProcedureV2(Connection connection) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 仅在 TSO 与元数据共用同一数据源且例程已初始化时使用服务器端原子开始。 */
    @Override
    public Optional<BeginReservation> tryBegin(TimestampOracle timestampOracle, IsolationLevel isolationLevel,
                                               long timeoutMillis, boolean persistMetadata, Runnable beforePublish) {
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
            } catch (SQLException ex) {
                throw storageError(null, "server-side publication connection failed", ex);
            }
        }
        return Optional.of(withPublicationLock(() -> {
            beforePublish.run();
            try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
                return callBeginProcedure(connection, isolationLevel, timeoutMillis, persistMetadata, false);
            } catch (SQLException ex) {
                throw storageError(null, "server-side transaction begin failed", ex);
            }
        }));
    }

    /** 在持锁连接上执行一次控制操作，只有租约和可选元数据提交后才返回事务编号。 */
    private BeginReservation callBeginProcedure(Connection connection, IsolationLevel isolationLevel,
                                                long timeoutMillis, boolean persistMetadata, boolean serverLock) {
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
        } catch (SQLException ex) {
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

    /** 同权威跨分片提交通过服务器端事务原子记录唯一决议。 */
    @Override
    public OptionalLong tryCommit(TimestampOracle timestampOracle, long txId, Set<String> participants,
                                  Runnable beforePublish, LongConsumer afterCommitted) {
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
            } catch (SQLException ex) {
                throw storageError(txId, "server-side commit connection failed", ex);
            }
        }
        long commitTs = withPublicationLock(() -> {
            beforePublish.run();
            try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
                long timestamp = callCommitProcedure(connection, txId, encodedParticipants, false);
                afterCommitted.accept(timestamp);
                return timestamp;
            } catch (SQLException ex) {
                throw storageError(txId, "server-side commit connection failed", ex);
            }
        });
        return OptionalLong.of(commitTs);
    }

    /** 读取存储过程返回的已提交时间戳，丢失响应时调用方必须查询持久决议。 */
    private long callCommitProcedure(Connection connection, long txId, String participants, boolean serverLock) {
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
        } catch (SQLException ex) {
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

    /** 为已有元数据表补充提交占位和事务下界索引。 */
    private void ensureMetadataIndexes(Connection connection) throws SQLException {
        ensureMetadataIndex(connection, "mvcc_txn_status_commit",
                "create index mvcc_txn_status_commit on mvcc_txn(status, commit_ts, tx_id)");
        ensureMetadataIndex(connection, "mvcc_txn_status_start",
                "create index mvcc_txn_status_start on mvcc_txn(status, start_ts, tx_id)");
    }

    /** 按名称检查并创建固定定义的索引，允许独立节点并发初始化。 */
    private void ensureMetadataIndex(Connection connection, String indexName, String createSql) throws SQLException {
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
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1061) {
                throw ex;
            }
        }
    }

    /**
     * 创建组件实例或事务记录。
     */
    @Override
    public TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
        return withPublicationLock(() -> insertRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null));
    }

    /** 在首个持久化记录中原子保存完整参与者，省去同一事务的读锁与二次更新。 */
    @Override
    public TxRecord createWithParticipants(long txId, long startTs, IsolationLevel isolationLevel,
                                           Set<String> participants) {
        validateParticipants(txId, participants);
        return insertRecord(txId, startTs, isolationLevel, participants, TxStatus.ACTIVE, null);
    }

    /**
     * 创建 PREPARING 状态的事务元数据记录。
     */
    @Override
    public TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants) {
        validateParticipants(txId, participants);
        return withPublicationLock(() -> insertRecord(txId, startTs, isolationLevel, participants, TxStatus.PREPARING, null));
    }

    /**
     * 创建 COMMITTED 状态的事务元数据记录。
     */
    @Override
    public TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs) {
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
     * 插入一条事务元数据记录。
     */
    private TxRecord insertRecord(long txId,
                                  long startTs,
                                  IsolationLevel isolationLevel,
                                  Set<String> participants,
                                  TxStatus status,
                                  Long commitTs) {
        TxRecord record = new TxRecord(txId, startTs, isolationLevel, participants, status, commitTs);
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError(txId, "create tx metadata failed", ex);
        }
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    @Override
    public Optional<TxRecord> get(long txId) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError(txId, "load tx metadata failed", ex);
        }
    }

    /**
     * 将事务标记为 PREPARING 状态。
     */
    @Override
    public TxRecord markPreparing(long txId, Set<String> participants) {
        validateParticipants(txId, participants);
        return withPublicationLock(() -> updateRecord(txId, participants, TxStatus.PREPARING, null, false));
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public TxRecord markCommitted(long txId, long commitTs) {
        return withPublicationLock(() -> updateRecord(txId, null, TxStatus.COMMITTED, commitTs, true));
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public TxRecord markCommitted(long txId, Set<String> participants, long commitTs) {
        return withPublicationLock(() -> updateRecord(txId, participants, TxStatus.COMMITTED, commitTs, true));
    }

    /**
     * 快速持久化 PREPARING 状态。
     */
    @Override
    public void persistPreparing(long txId, Set<String> participants) {
        validateParticipants(txId, participants);
        fastUpdateStatus(txId, participants, TxStatus.PREPARING, null, false);
    }

    /**
     * 快速持久化 COMMITTED 状态。
     */
    @Override
    public void persistCommitted(long txId, long commitTs) {
        fastUpdateStatus(txId, null, TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * 快速持久化 COMMITTED 状态。
     */
    @Override
    public void persistCommitted(long txId, Set<String> participants, long commitTs) {
        fastUpdateStatus(txId, participants, TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * 将单分片事务标记为已提交。
     */
    @Override
    public TxRecord markSingleShardCommitted(long txId, String participant, long commitTs) {
        validateParticipant(txId, participant);
        return withPublicationLock(() -> updateRecord(txId, Set.of(participant), TxStatus.COMMITTED, commitTs, true));
    }

    /** 本地参与者提交后的中心决议复用条件更新；重放和不匹配仍进入原状态机。 */
    @Override
    public void persistSingleShardCommitted(long txId, String participant, long commitTs) {
        validateParticipant(txId, participant);
        fastUpdateStatus(txId, Set.of(participant), TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * 将事务标记为 ABORTED 状态。
     */
    @Override
    public TxRecord markAborted(long txId) {
        return withPublicationLock(() -> updateRecord(txId, null, TxStatus.ABORTED, null, false));
    }

    /**
     * 查询快照和未完成物化事务的最小下界；已 finalized 的历史决议不阻挡 GC。
     */
    @Override
    public Optional<Long> oldestActiveStartTs() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError(null, "load oldest active start ts failed", ex);
        }
    }

    /**
     * 在事务内锁定并更新事务元数据记录。
     */
    private TxRecord updateRecord(long txId,
                                  Set<String> participants,
                                  TxStatus targetStatus,
                                  Long commitTs,
                                  boolean setCommitTs) {
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
                    // 返回真实终态，禁止 COMMITTED 回退以及重复提交更换时间戳。
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
                        targetStatus == TxStatus.ABORTED ? null : setCommitTs ? commitTs : current.commitTs()
                );
            });
        } catch (SQLException ex) {
            throw storageError(txId, "update tx metadata failed", ex);
        }
    }

    /**
     * 保留快速入口兼容性，使用行锁实现与 CAS 等价的终态校验。
     */
    private void fastUpdateStatus(long txId,
                                  Set<String> participants,
                                  TxStatus targetStatus,
                                  Long commitTs,
                                  boolean setCommitTs) {
        withPublicationLock(() -> {
            if (targetStatus == TxStatus.COMMITTED && setCommitTs && commitTs != null
                    && tryPersistFreshCommit(txId, participants, commitTs)) {
                return null;
            }
            // 重放或状态不匹配时仍由原状态机判定真实终态，不能宽松覆盖。
            updateRecord(txId, participants, targetStatus, commitTs, setCommitTs);
            return null;
        });
    }

    /** 对参与者和未决状态完全匹配的首次提交使用单条原子更新。 */
    private boolean tryPersistFreshCommit(long txId, Set<String> participants, long commitTs) {
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
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, commitTs);
            statement.setLong(2, txId);
            statement.setLong(3, commitTs);
            statement.setLong(4, commitTs);
            if (participants != null) {
                statement.setString(5, encodeParticipants(txId, participants));
            }
            return statement.executeUpdate() == 1;
        } catch (SQLException ex) {
            throw storageError(txId, "persist committed decision failed", ex);
        }
    }

    /**
     * 锁定并读取指定事务元数据记录。
     */
    private TxRecord lockRecord(Connection connection, long txId) throws SQLException {
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
     * 将结果集映射为事务元数据对象。
     */
    private static TxRecord mapRecord(ResultSet resultSet) throws SQLException {
        long txId = resultSet.getLong("tx_id");
        Long commitTs = resultSet.getObject("commit_ts") == null ? null : resultSet.getLong("commit_ts");
        return new TxRecord(
                txId,
                resultSet.getLong("start_ts"),
                IsolationLevel.valueOf(resultSet.getString("isolation_level")),
                decodeParticipants(txId, resultSet.getString("participants")),
                TxStatus.valueOf(resultSet.getString("status")),
                commitTs
        );
    }

    /**
     * 将参与分片集合编码为存储字符串。
     */
    private static String encodeParticipants(long txId, Set<String> participants) {
        validateParticipants(txId, participants);
        return participants.stream().sorted().collect(Collectors.joining(","));
    }

    /**
     * 兼容旧 CSV 和空集合，拒绝空白片段，避免恢复路由被静默丢弃。
     */
    private static Set<String> decodeParticipants(long txId, String value) {
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

    /** 登记固定截止租约，重复调用只允许降低受保护的快照下界。 */
    @Override
    public void registerSnapshot(long txId, long readTs, long expiresAtMillis) {
        withPublicationLock(() -> {
            try (Connection connection = MysqlConnectionContext.connection(dataSource);
                 PreparedStatement statement = connection.prepareStatement("""
                         insert into mvcc_snapshot_lease(tx_id, read_ts, expires_at) values (?, ?, ?)
                         """)) {
                statement.setLong(1, txId);
                statement.setLong(2, readTs);
                statement.setLong(3, expiresAtMillis);
                statement.executeUpdate();
                return null;
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 1062) {
                    // 仅重复登记才读取旧租约；截止时间不可延长，快照下界只能降低。
                    Optional<Long> previous = snapshotExpiry(txId);
                    if (previous.isPresent() && previous.get() == expiresAtMillis) {
                        try (Connection connection = MysqlConnectionContext.connection(dataSource);
                             PreparedStatement update = connection.prepareStatement("""
                                     update mvcc_snapshot_lease set read_ts = least(read_ts, ?)
                                     where tx_id = ? and expires_at = ?
                                     """)) {
                            update.setLong(1, readTs);
                            update.setLong(2, txId);
                            update.setLong(3, expiresAtMillis);
                            update.executeUpdate();
                            return null;
                        } catch (SQLException updateFailure) {
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

    /** 返回原始截止时间，过期记录也保留到有界清理时删除。 */
    @Override
    public Optional<Long> snapshotExpiry(long txId) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement statement = connection.prepareStatement(
                     "select expires_at from mvcc_snapshot_lease where tx_id = ?")) {
            statement.setLong(1, txId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getLong(1)) : Optional.empty();
            }
        } catch (SQLException ex) {
            throw storageError(txId, "load snapshot expiry failed", ex);
        }
    }

    /** 在同次数据库查询中核对固定截止时间与权威时钟，避免热路径两次借连接和往返。 */
    @Override
    public boolean isSnapshotLeaseValid(long txId, long expectedExpiresAtMillis) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            // 查询失败必须保留存储异常，不能把无法确认的租约误当作有效。
            throw storageError(txId, "validate snapshot lease failed", ex);
        }
    }

    /** 幂等释放独立于元数据存在的快照租约。 */
    @Override
    public void releaseSnapshot(long txId) {
        // 只删除已结束事务的保护；与安全点计算并发时至多使本轮 GC 保守滞后。
        executeUpdate("delete from mvcc_snapshot_lease where tx_id = ?", txId);
    }

    /** 在预写前合并参与者，维持 ACTIVE 或 PREPARING 状态且不覆盖终态。 */
    @Override
    public TxRecord registerParticipants(long txId, Set<String> participants) {
        validateParticipants(txId, participants);
        // 只修改当前事务行；行锁与恢复并发时会合并最新参与者或返回真实终态。
        TxRecord record = get(txId).orElseThrow(() ->
                new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId));
        return updateRecord(txId, participants, record.status(), record.commitTs(), false);
    }

    /** 有序批次发现未物化终态和无有效租约的未决事务。 */
    @Override
    public List<TxRecord> findRecoveryCandidates(long nowMillis, long afterTxId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
        } catch (SQLException ex) {
            throw storageError(null, "find recovery candidates failed", ex);
        }
    }

    /** 仅记录终态首次完成物化的时间；已被 GC 删除的记录无需复活。 */
    @Override
    public void markFinalized(long txId) {
        // 物化已经完成时才写入；GC 与该语句并发只会看到旧保护或新终态。
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

    /** 有界清理严格早于安全点和保留时间的已完成事务，外键同时移除物化标志。 */
    @Override
    public int purgeFinalized(long safePoint, long finalizedBeforeMillis, int limit) {
        if (limit <= 0) {
            return 0;
        }
        return withPublicationLock(() -> {
            List<Long> ids = new ArrayList<>();
            try (Connection connection = MysqlConnectionContext.connection(dataSource);
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
            } catch (SQLException ex) {
                throw storageError(null, "purge finalized metadata failed", ex);
            }
        });
    }

    /** 分批删除已到期租约，不改变未决事务，剩余租约继续保守阻挡安全点。 */
    @Override
    public int expireSnapshots(long nowMillis, int limit) {
        if (limit <= 0) {
            return 0;
        }
        return withPublicationLock(() -> executeUpdate(
                "delete from mvcc_snapshot_lease where expires_at <= ? order by expires_at, tx_id limit ?", nowMillis, limit));
    }

    /** 在当前连接上下文中执行只含整型绑定参数的控制面更新。 */
    private int executeUpdate(String sql, long... parameters) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                statement.setLong(i + 1, parameters[i]);
            }
            return statement.executeUpdate();
        } catch (SQLException ex) {
            throw storageError(null, "control metadata update failed", ex);
        }
    }

    /**
     * 将底层存储异常包装为 MVCC 异常。
     */
    private MvccException storageError(Long txId, String message, SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}

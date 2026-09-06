package mvcc.metadata.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 基于 MySQL 的事务元数据存储实现，负责事务状态表的读写。
 */
public final class MysqlTransactionMetadataStore implements TransactionMetadataStore {
    private final DataSource dataSource;

    /**
     * 构造 MysqlTransactionMetadataStore 实例。
     */
    public MysqlTransactionMetadataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 初始化所需的数据库表结构或运行环境。
     */
    public void initialize() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     create table if not exists mvcc_txn (
                       tx_id bigint not null primary key,
                       start_ts bigint not null,
                       isolation_level varchar(32) not null,
                       status varchar(32) not null,
                       commit_ts bigint null,
                       participants text not null
                     )
                     """)) {
            statement.execute();
        } catch (SQLException ex) {
            throw storageError(null, "initialize metadata schema failed", ex);
        }
    }

    /**
     * 创建组件实例或事务记录。
     */
    @Override
    public TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
        return insertRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null);
    }

    /**
     * 创建 PREPARING 状态的事务元数据记录。
     */
    @Override
    public TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants) {
        return insertRecord(txId, startTs, isolationLevel, participants, TxStatus.PREPARING, null);
    }

    /**
     * 创建 COMMITTED 状态的事务元数据记录。
     */
    @Override
    public TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel, Set<String> participants, long commitTs) {
        return insertRecord(txId, startTs, isolationLevel, participants, TxStatus.COMMITTED, commitTs);
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
        try (Connection connection = dataSource.getConnection();
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
            statement.setString(6, encodeParticipants(participants));
            statement.executeUpdate();
            return new TxRecord(txId, startTs, isolationLevel, participants, status, commitTs);
        } catch (SQLException ex) {
            throw storageError(txId, "create tx metadata failed", ex);
        }
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    @Override
    public Optional<TxRecord> get(long txId) {
        try (Connection connection = dataSource.getConnection();
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
        return updateRecord(txId, participants, TxStatus.PREPARING, null, false);
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public TxRecord markCommitted(long txId, long commitTs) {
        return updateRecord(txId, null, TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * 将事务标记为 COMMITTED 状态。
     */
    @Override
    public TxRecord markCommitted(long txId, Set<String> participants, long commitTs) {
        return updateRecord(txId, participants, TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * 快速持久化 PREPARING 状态。
     */
    @Override
    public void persistPreparing(long txId, Set<String> participants) {
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
        return updateRecord(txId, Set.of(participant), TxStatus.COMMITTED, commitTs, true);
    }

    /**
     * 将事务标记为 ABORTED 状态。
     */
    @Override
    public TxRecord markAborted(long txId) {
        return updateRecord(txId, null, TxStatus.ABORTED, null, false);
    }

    /**
     * 查询当前最老的活跃事务开始时间戳。
     */
    @Override
    public Optional<Long> oldestActiveStartTs() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select min(start_ts) as oldest_start_ts
                     from mvcc_txn
                     where status in ('ACTIVE', 'PREPARING')
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
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                TxRecord current = lockRecord(connection, txId);
                if (current.status() == TxStatus.COMMITTED || current.status() == TxStatus.ABORTED) {
                    // 终态事务保持幂等，恢复或重复提交时直接返回已有记录。
                    connection.commit();
                    return current;
                }

                Set<String> nextParticipants = participants != null ? participants : current.participants();
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_txn
                        set status = ?, commit_ts = ?, participants = ?
                        where tx_id = ?
                        """)) {
                    update.setString(1, targetStatus.name());
                    if (setCommitTs && commitTs != null) {
                        update.setLong(2, commitTs);
                    } else if (current.commitTs() != null) {
                        update.setLong(2, current.commitTs());
                    } else {
                        update.setNull(2, Types.BIGINT);
                    }
                    update.setString(3, encodeParticipants(nextParticipants));
                    update.setLong(4, txId);
                    update.executeUpdate();
                }
                connection.commit();
                return new TxRecord(
                        current.txId(),
                        current.startTs(),
                        current.isolationLevel(),
                        nextParticipants,
                        targetStatus,
                        setCommitTs ? commitTs : current.commitTs()
                );
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "update tx metadata failed", ex);
        }
    }

    /**
     * 使用单条 SQL 快速更新事务状态。
     */
    private void fastUpdateStatus(long txId,
                                  Set<String> participants,
                                  TxStatus targetStatus,
                                  Long commitTs,
                                  boolean setCommitTs) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement update = connection.prepareStatement("""
                     update mvcc_txn
                     set status = ?,
                         commit_ts = case
                             when ? = 1 then ?
                             else commit_ts
                         end,
                         participants = case
                             when ? is null then participants
                             else ?
                         end
                     where tx_id = ?
                       and status <> 'ABORTED'
                     """)) {
            String encodedParticipants = participants == null ? null : encodeParticipants(participants);
            update.setString(1, targetStatus.name());
            update.setInt(2, setCommitTs && commitTs != null ? 1 : 0);
            if (commitTs == null) {
                update.setNull(3, Types.BIGINT);
            } else {
                update.setLong(3, commitTs);
            }
            if (encodedParticipants == null) {
                update.setNull(4, Types.VARCHAR);
                update.setNull(5, Types.VARCHAR);
            } else {
                update.setString(4, encodedParticipants);
                update.setString(5, encodedParticipants);
            }
            update.setLong(6, txId);
            int updated = update.executeUpdate();
            if (updated == 1) {
                return;
            }
        } catch (SQLException ex) {
            throw storageError(txId, "fast update tx metadata failed", ex);
        }

        TxRecord current = get(txId)
                .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "transaction not found: " + txId, false, txId));
        if (current.status() == TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "cannot update aborted tx: " + txId, false, txId);
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
        Long commitTs = resultSet.getObject("commit_ts") == null ? null : resultSet.getLong("commit_ts");
        return new TxRecord(
                resultSet.getLong("tx_id"),
                resultSet.getLong("start_ts"),
                IsolationLevel.valueOf(resultSet.getString("isolation_level")),
                decodeParticipants(resultSet.getString("participants")),
                TxStatus.valueOf(resultSet.getString("status")),
                commitTs
        );
    }

    /**
     * 将参与分片集合编码为存储字符串。
     */
    private static String encodeParticipants(Set<String> participants) {
        return participants.stream().sorted().collect(Collectors.joining(","));
    }

    /**
     * 将存储字符串解码为参与分片集合。
     */
    private static Set<String> decodeParticipants(String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(value.split(","))
                .filter(participant -> !participant.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * 将底层存储异常包装为 MVCC 异常。
     */
    private MvccException storageError(Long txId, String message, SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}

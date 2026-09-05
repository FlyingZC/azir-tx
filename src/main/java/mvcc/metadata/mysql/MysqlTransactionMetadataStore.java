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

public final class MysqlTransactionMetadataStore implements TransactionMetadataStore {
    private final DataSource dataSource;

    public MysqlTransactionMetadataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

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

    @Override
    public TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     insert into mvcc_txn(tx_id, start_ts, isolation_level, status, commit_ts, participants)
                     values (?, ?, ?, ?, ?, ?)
                     """)) {
            statement.setLong(1, txId);
            statement.setLong(2, startTs);
            statement.setString(3, isolationLevel.name());
            statement.setString(4, TxStatus.ACTIVE.name());
            statement.setNull(5, Types.BIGINT);
            statement.setString(6, "");
            statement.executeUpdate();
            return new TxRecord(txId, startTs, isolationLevel, Set.of(), TxStatus.ACTIVE, null);
        } catch (SQLException ex) {
            throw storageError(txId, "create tx metadata failed", ex);
        }
    }

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

    @Override
    public TxRecord markPreparing(long txId, Set<String> participants) {
        return updateRecord(txId, participants, TxStatus.PREPARING, null, false);
    }

    @Override
    public TxRecord markCommitted(long txId, long commitTs) {
        return updateRecord(txId, null, TxStatus.COMMITTED, commitTs, true);
    }

    @Override
    public TxRecord markSingleShardCommitted(long txId, String participant, long commitTs) {
        return updateRecord(txId, Set.of(participant), TxStatus.COMMITTED, commitTs, true);
    }

    @Override
    public TxRecord markAborted(long txId) {
        return updateRecord(txId, null, TxStatus.ABORTED, null, false);
    }

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

    private static String encodeParticipants(Set<String> participants) {
        return participants.stream().sorted().collect(Collectors.joining(","));
    }

    private static Set<String> decodeParticipants(String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(value.split(","))
                .filter(participant -> !participant.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private MvccException storageError(Long txId, String message, SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}

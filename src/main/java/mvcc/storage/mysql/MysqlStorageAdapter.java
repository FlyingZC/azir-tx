package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.MutationType;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MysqlStorageAdapter implements StorageAdapter {
    private final String shardId;
    private final DataSource dataSource;

    public MysqlStorageAdapter(String shardId, DataSource dataSource) {
        this.shardId = shardId;
        this.dataSource = dataSource;
    }

    @Override
    public String shardId() {
        return shardId;
    }

    @Override
    public void initialize() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ignored1 = connection.prepareStatement("""
                     create table if not exists mvcc_data (
                       user_key varbinary(256) not null,
                       commit_ts bigint not null,
                       tx_id bigint not null,
                       deleted tinyint not null default 0,
                       value blob,
                       primary key (user_key, commit_ts)
                     )
                     """);
             PreparedStatement ignored2 = connection.prepareStatement("""
                     create table if not exists mvcc_intent (
                       user_key varbinary(256) not null,
                       tx_id bigint not null,
                       start_ts bigint not null,
                       deleted tinyint not null default 0,
                       value blob,
                       expire_at bigint not null,
                       primary key (user_key)
                     )
                     """);
             PreparedStatement ignored3 = connection.prepareStatement("""
                     create table if not exists mvcc_rollback (
                       user_key varbinary(256) not null,
                       tx_id bigint not null,
                       start_ts bigint not null,
                       created_at bigint not null,
                       primary key (user_key, tx_id)
                     )
                     """)) {
            ignored1.execute();
            ignored2.execute();
            ignored3.execute();
        } catch (SQLException ex) {
            throw storageError(null, "initialize schema failed", ex);
        }
    }

    @Override
    public Optional<VersionedValue> get(ReadContext context, String key) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<VersionedValue> ownIntent = findIntent(connection, context.txId(), key);
            if (ownIntent.isPresent()) {
                return ownIntent;
            }
            return findCommitted(connection, context.readTs(), key);
        } catch (SQLException ex) {
            throw storageError(context.txId(), "snapshot read failed for key " + key, ex);
        }
    }

    @Override
    public List<VersionedValue> scan(ReadContext context, KeyRange keyRange) {
        try (Connection connection = dataSource.getConnection()) {
            List<String> keys = collectRangeKeys(connection, keyRange);
            List<VersionedValue> result = new ArrayList<>();
            for (String key : keys) {
                Optional<VersionedValue> visible = findIntent(connection, context.txId(), key);
                if (visible.isEmpty()) {
                    visible = findCommitted(connection, context.readTs(), key);
                }
                if (visible.isPresent() && !visible.get().deleted()) {
                    result.add(visible.get());
                }
            }
            return result;
        } catch (SQLException ex) {
            throw storageError(context.txId(), "range scan failed", ex);
        }
    }

    @Override
    public void prewrite(long txId, long startTs, List<Mutation> mutations) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                for (Mutation mutation : mutations) {
                    lockAndCheck(connection, txId, startTs, mutation.key());
                    upsertIntent(connection, txId, startTs, mutation);
                }
                connection.commit();
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "prewrite failed", ex);
        }
    }

    @Override
    public void prepare(long txId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select count(*) from mvcc_intent where tx_id = ?
                     """)) {
            statement.setLong(1, txId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                if (resultSet.getLong(1) == 0L) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "no intent to prepare for tx " + txId, false, txId);
                }
            }
        } catch (SQLException ex) {
            throw storageError(txId, "prepare failed", ex);
        }
    }

    @Override
    public void commit(long txId, long commitTs) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("""
                    select user_key, deleted, value
                    from mvcc_intent
                    where tx_id = ?
                    order by user_key
                    """);
                 PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                    values (?, ?, ?, ?, ?)
                    on duplicate key update
                      tx_id = values(tx_id),
                      deleted = values(deleted),
                      value = values(value)
                    """);
                 PreparedStatement delete = connection.prepareStatement("""
                    delete from mvcc_intent where tx_id = ?
                    """)) {
                select.setLong(1, txId);
                try (ResultSet resultSet = select.executeQuery()) {
                    while (resultSet.next()) {
                        insert.setBytes(1, resultSet.getBytes("user_key"));
                        insert.setLong(2, commitTs);
                        insert.setLong(3, txId);
                        insert.setInt(4, resultSet.getInt("deleted"));
                        byte[] value = resultSet.getBytes("value");
                        if (value == null) {
                            insert.setNull(5, Types.BLOB);
                        } else {
                            insert.setBytes(5, value);
                        }
                        insert.addBatch();
                    }
                }
                insert.executeBatch();
                delete.setLong(1, txId);
                delete.executeUpdate();
                connection.commit();
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "commit failed", ex);
        }
    }

    @Override
    public void abort(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("""
                    select user_key, start_ts
                    from mvcc_intent
                    where tx_id = ?
                    """);
                 PreparedStatement insertRollback = connection.prepareStatement("""
                    insert into mvcc_rollback(user_key, tx_id, start_ts, created_at)
                    values (?, ?, ?, ?)
                    on duplicate key update created_at = values(created_at)
                    """);
                 PreparedStatement delete = connection.prepareStatement("""
                    delete from mvcc_intent where tx_id = ?
                    """)) {
                select.setLong(1, txId);
                try (ResultSet resultSet = select.executeQuery()) {
                    while (resultSet.next()) {
                        insertRollback.setBytes(1, resultSet.getBytes("user_key"));
                        insertRollback.setLong(2, txId);
                        insertRollback.setLong(3, resultSet.getLong("start_ts"));
                        insertRollback.setLong(4, System.currentTimeMillis());
                        insertRollback.addBatch();
                    }
                }
                insertRollback.executeBatch();
                delete.setLong(1, txId);
                delete.executeUpdate();
                connection.commit();
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "abort failed", ex);
        }
    }

    @Override
    public Set<Long> findPendingTransactionIds(int limit) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select distinct tx_id
                     from mvcc_intent
                     order by tx_id
                     limit ?
                     """)) {
            statement.setInt(1, limit);
            try (ResultSet resultSet = statement.executeQuery()) {
                Set<Long> txIds = new LinkedHashSet<>();
                while (resultSet.next()) {
                    txIds.add(resultSet.getLong("tx_id"));
                }
                return txIds;
            }
        } catch (SQLException ex) {
            throw storageError(null, "scan lingering intents failed", ex);
        }
    }

    @Override
    public int gc(long safePoint, int limit) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("""
                    select d.user_key, d.commit_ts
                    from mvcc_data d
                    join (
                        select user_key, max(commit_ts) as keep_commit_ts
                        from mvcc_data
                        where commit_ts <= ?
                        group by user_key
                    ) keepers on keepers.user_key = d.user_key
                    where d.commit_ts <= ? and d.commit_ts < keepers.keep_commit_ts
                    order by d.user_key, d.commit_ts
                    limit ?
                    """);
                 PreparedStatement delete = connection.prepareStatement("""
                    delete from mvcc_data where user_key = ? and commit_ts = ?
                    """);
                 PreparedStatement tombstoneKeys = connection.prepareStatement("""
                    select d.user_key, d.commit_ts
                    from mvcc_data d
                    left join mvcc_data newer
                      on newer.user_key = d.user_key and newer.commit_ts > d.commit_ts
                    where d.deleted = 1
                      and d.commit_ts <= ?
                      and newer.user_key is null
                    order by d.user_key
                    limit ?
                    """);
                 PreparedStatement deleteTombstoneChain = connection.prepareStatement("""
                    delete from mvcc_data where user_key = ? and commit_ts <= ?
                    """)) {
                select.setLong(1, safePoint);
                select.setLong(2, safePoint);
                select.setInt(3, limit);
                int reclaimed = 0;
                try (ResultSet resultSet = select.executeQuery()) {
                    while (resultSet.next()) {
                        delete.setBytes(1, resultSet.getBytes("user_key"));
                        delete.setLong(2, resultSet.getLong("commit_ts"));
                        delete.addBatch();
                        reclaimed++;
                    }
                }
                if (reclaimed > 0) {
                    delete.executeBatch();
                }
                tombstoneKeys.setLong(1, safePoint);
                tombstoneKeys.setInt(2, limit);
                try (ResultSet resultSet = tombstoneKeys.executeQuery()) {
                    while (resultSet.next()) {
                        deleteTombstoneChain.setBytes(1, resultSet.getBytes("user_key"));
                        deleteTombstoneChain.setLong(2, resultSet.getLong("commit_ts"));
                        reclaimed += deleteTombstoneChain.executeUpdate();
                    }
                }
                connection.commit();
                return reclaimed;
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(null, "gc failed", ex);
        }
    }

    private Optional<VersionedValue> findIntent(Connection connection, long txId, String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select tx_id, deleted, value, start_ts
                from mvcc_intent
                where user_key = ?
                """)) {
            statement.setBytes(1, encodeKey(key));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                long intentTxId = resultSet.getLong("tx_id");
                if (intentTxId != txId) {
                    return Optional.empty();
                }
                return Optional.of(new VersionedValue(
                        key,
                        resultSet.getBytes("value"),
                        resultSet.getInt("deleted") == 1,
                        resultSet.getLong("start_ts"),
                        true
                ));
            }
        }
    }

    private Optional<VersionedValue> findCommitted(Connection connection, long readTs, String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select commit_ts, deleted, value
                from mvcc_data
                where user_key = ? and commit_ts <= ?
                order by commit_ts desc
                limit 1
                """)) {
            statement.setBytes(1, encodeKey(key));
            statement.setLong(2, readTs);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(new VersionedValue(
                        key,
                        resultSet.getBytes("value"),
                        resultSet.getInt("deleted") == 1,
                        resultSet.getLong("commit_ts"),
                        false
                ));
            }
        }
    }

    private void lockAndCheck(Connection connection, long txId, long startTs, String key) throws SQLException {
        try (PreparedStatement lockIntent = connection.prepareStatement("""
                select tx_id
                from mvcc_intent
                where user_key = ?
                for update
                """)) {
            lockIntent.setBytes(1, encodeKey(key));
            try (ResultSet resultSet = lockIntent.executeQuery()) {
                if (resultSet.next()) {
                    long ownerTxId = resultSet.getLong("tx_id");
                    if (ownerTxId != txId) {
                        throw new MvccException(ErrorCode.LOCK_CONFLICT, "lock conflict on key " + key, true, txId);
                    }
                }
            }
        }

        try (PreparedStatement latestCommitted = connection.prepareStatement("""
                select commit_ts
                from mvcc_data
                where user_key = ?
                order by commit_ts desc
                limit 1
                for update
                """)) {
            latestCommitted.setBytes(1, encodeKey(key));
            try (ResultSet resultSet = latestCommitted.executeQuery()) {
                if (resultSet.next() && resultSet.getLong("commit_ts") > startTs) {
                    throw new MvccException(
                            ErrorCode.WRITE_CONFLICT,
                            "write conflict on key " + key + ", newer version exists",
                            true,
                            txId
                    );
                }
            }
        }
    }

    private void upsertIntent(Connection connection, long txId, long startTs, Mutation mutation) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into mvcc_intent(user_key, tx_id, start_ts, deleted, value, expire_at)
                values (?, ?, ?, ?, ?, ?)
                on duplicate key update
                  tx_id = values(tx_id),
                  start_ts = values(start_ts),
                  deleted = values(deleted),
                  value = values(value),
                  expire_at = values(expire_at)
                """)) {
            statement.setBytes(1, encodeKey(mutation.key()));
            statement.setLong(2, txId);
            statement.setLong(3, startTs);
            statement.setInt(4, mutation.type() == MutationType.DELETE ? 1 : 0);
            if (mutation.value() == null) {
                statement.setNull(5, Types.BLOB);
            } else {
                statement.setBytes(5, mutation.value());
            }
            statement.setLong(6, startTs + 60_000L);
            statement.executeUpdate();
        }
    }

    private List<String> collectRangeKeys(Connection connection, KeyRange keyRange) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select user_key
                from (
                    select distinct user_key from mvcc_data where user_key >= ? and user_key < ?
                    union
                    select distinct user_key from mvcc_intent where user_key >= ? and user_key < ?
                ) keys_in_range
                order by user_key
                """)) {
            statement.setBytes(1, encodeKey(keyRange.startKeyInclusive()));
            statement.setBytes(2, encodeKey(keyRange.endKeyExclusive()));
            statement.setBytes(3, encodeKey(keyRange.startKeyInclusive()));
            statement.setBytes(4, encodeKey(keyRange.endKeyExclusive()));
            try (ResultSet resultSet = statement.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (resultSet.next()) {
                    keys.add(decodeKey(resultSet.getBytes("user_key")));
                }
                return keys;
            }
        }
    }

    private static byte[] encodeKey(String key) {
        return key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String decodeKey(byte[] key) {
        return new String(key, java.nio.charset.StandardCharsets.UTF_8);
    }

    private MvccException storageError(Long txId, String message, SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, shardId + ": " + message, true, txId, ex);
    }
}

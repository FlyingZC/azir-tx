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
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 基于 MySQL 表结构的 MVCC 分片存储适配器。
 */
public class MysqlStorageAdapter implements StorageAdapter {
    private final String shardId;
    private final DataSource dataSource;

    /**
     * 构造 MysqlStorageAdapter 实例。
     */
    public MysqlStorageAdapter(String shardId, DataSource dataSource) {
        this.shardId = shardId;
        this.dataSource = dataSource;
    }

    /**
     * 返回当前存储适配器所属分片编号。
     */
    @Override
    public String shardId() {
        return shardId;
    }

    /**
     * 初始化所需的数据库表结构或运行环境。
     */
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
            ensureIndex(connection, "mvcc_intent", "idx_mvcc_intent_tx_id_user_key",
                    "create index idx_mvcc_intent_tx_id_user_key on mvcc_intent(tx_id, user_key)");
            ensureIndex(connection, "mvcc_rollback", "idx_mvcc_rollback_tx_id_user_key",
                    "create index idx_mvcc_rollback_tx_id_user_key on mvcc_rollback(tx_id, user_key)");
        } catch (SQLException ex) {
            throw storageError(null, "initialize schema failed", ex);
        }
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
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

    /**
     * 按键范围扫描当前快照可见的数据。
     */
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

    /**
     * 写入事务意向并检测写冲突。
     */
    @Override
    public void prewrite(long txId, long startTs, List<Mutation> mutations) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement insertIntentIfNoWriteConflict = connection.prepareStatement("""
                    insert into mvcc_intent(user_key, tx_id, start_ts, deleted, value, expire_at)
                    select ?, ?, ?, ?, ?, ?
                    from dual
                    where not exists (
                        select 1
                        from mvcc_data
                        where user_key = ? and commit_ts > ?
                        limit 1
                    )
                    """);
                 PreparedStatement updateOwnIntent = connection.prepareStatement("""
                    update mvcc_intent
                    set start_ts = ?, deleted = ?, value = ?, expire_at = ?
                    where user_key = ? and tx_id = ?
                    """)) {
                for (Mutation mutation : mutations) {
                    // 先尝试插入新意向，若已经是本事务意向则更新；其他事务占用会触发锁冲突。
                    upsertIntent(insertIntentIfNoWriteConflict, updateOwnIntent, txId, startTs, mutation);
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

    /**
     * 检查事务意向是否存在并进入准备阶段。
     */
    @Override
    public void prepare(long txId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select 1
                     from mvcc_intent
                     where tx_id = ?
                     limit 1
                     """)) {
            statement.setLong(1, txId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "no intent to prepare for tx " + txId, false, txId);
                }
            }
        } catch (SQLException ex) {
            throw storageError(txId, "prepare failed", ex);
        }
    }

    /**
     * 提交事务意向并生成已提交版本。
     */
    @Override
    public void commit(long txId, long commitTs) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                commitInternal(connection, txId, commitTs, loadIntentRows(connection, txId));
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

    /**
     * 提交事务意向并生成已提交版本。
     */
    @Override
    public void commit(long txId, long commitTs, List<Mutation> mutations) {
        commit(txId, commitTs, mutations, true);
    }

    /**
     * 提交事务意向并生成已提交版本。
     */
    @Override
    public void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                commitKnownMutationsInternal(connection, txId, commitTs, mutations, cleanupIntents);
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

    /**
     * 直接写入已提交版本，跳过显式 prepare 阶段。
     */
    @Override
    public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                directCommitInternal(connection, txId, startTs, commitTs, mutations);
                connection.commit();
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "direct commit failed", ex);
        }
    }

    /**
     * 清理事务残留的写意向。
     */
    @Override
    public void cleanupIntents(long txId, List<Mutation> mutations) {
        cleanupIntents(txId);
    }

    /**
     * 清理事务残留的写意向。
     */
    @Override
    public void cleanupIntents(long txId) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                deleteIntentsByTransaction(connection, txId);
                connection.commit();
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError(txId, "cleanup intents failed", ex);
        }
    }

    /**
     * 回滚事务并清理写意向。
     */
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

    /**
     * 查找仍有待处理意向的事务编号。
     */
    @Override
    public Set<Long> findPendingTransactionIds(int limit) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select tx_id
                     from mvcc_intent
                     group by tx_id
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

    /**
     * 清理安全点之前的历史版本。
     */
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

    /**
     * 查找指定事务自己的写意向。
     */
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

    /**
     * 查找指定快照时间戳可见的已提交版本。
     */
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

    /**
     * 插入或更新事务写意向。
     */
    private void upsertIntent(PreparedStatement insertIntentIfNoWriteConflict,
                              PreparedStatement updateOwnIntent,
                              long txId,
                              long startTs,
                              Mutation mutation) throws SQLException {
        try {
            byte[] userKey = encodeKey(mutation.key());
            insertIntentIfNoWriteConflict.setBytes(1, userKey);
            insertIntentIfNoWriteConflict.setLong(2, txId);
            insertIntentIfNoWriteConflict.setLong(3, startTs);
            insertIntentIfNoWriteConflict.setInt(4, mutation.type() == MutationType.DELETE ? 1 : 0);
            if (mutation.value() == null) {
                insertIntentIfNoWriteConflict.setNull(5, Types.BLOB);
            } else {
                insertIntentIfNoWriteConflict.setBytes(5, mutation.value());
            }
            insertIntentIfNoWriteConflict.setLong(6, startTs + 60_000L);
            insertIntentIfNoWriteConflict.setBytes(7, userKey);
            insertIntentIfNoWriteConflict.setLong(8, startTs);
            int updated = insertIntentIfNoWriteConflict.executeUpdate();
            if (updated == 0) {
                throw new MvccException(
                        ErrorCode.WRITE_CONFLICT,
                        "write conflict on key " + mutation.key() + ", newer version exists",
                        true,
                        txId
                );
            }
        } catch (SQLIntegrityConstraintViolationException ex) {
            if (!tryUpdateOwnIntent(updateOwnIntent, txId, startTs, mutation)) {
                throw new MvccException(ErrorCode.LOCK_CONFLICT, "lock conflict on key " + mutation.key(), true, txId, ex);
            }
        }
    }

    /**
     * 尝试更新当前事务已有的写意向。
     */
    private boolean tryUpdateOwnIntent(PreparedStatement statement, long txId, long startTs, Mutation mutation) throws SQLException {
        statement.setLong(1, startTs);
        statement.setInt(2, mutation.type() == MutationType.DELETE ? 1 : 0);
        if (mutation.value() == null) {
            statement.setNull(3, Types.BLOB);
        } else {
            statement.setBytes(3, mutation.value());
        }
        statement.setLong(4, startTs + 60_000L);
        statement.setBytes(5, encodeKey(mutation.key()));
        statement.setLong(6, txId);
        return statement.executeUpdate() == 1;
    }

    /**
     * 收集扫描范围内出现过的业务键。
     */
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

    /**
     * 将字符串键编码为数据库二进制键。
     */
    private static byte[] encodeKey(String key) {
        return key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 将数据库二进制键解码为字符串键。
     */
    private static String decodeKey(byte[] key) {
        return new String(key, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 将底层存储异常包装为 MVCC 异常。
     */
    private MvccException storageError(Long txId, String message, SQLException ex) {
        String detailedMessage = shardId
                + ": " + message
                + " [sqlState=" + ex.getSQLState()
                + ", vendorCode=" + ex.getErrorCode()
                + "]";
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, detailedMessage, true, txId, ex);
    }

    /**
     * 确保指定索引已经存在。
     */
    private void ensureIndex(Connection connection, String tableName, String indexName, String ddl) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select 1
                from information_schema.statistics
                where table_schema = database()
                  and table_name = ?
                  and index_name = ?
                limit 1
                """)) {
            statement.setString(1, tableName);
            statement.setString(2, indexName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return;
                }
            }
        }
        try (PreparedStatement createIndex = connection.prepareStatement(ddl)) {
            createIndex.execute();
        }
    }

    /**
     * 加载指定事务的全部写意向行。
     */
    private List<IntentRow> loadIntentRows(Connection connection, long txId) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement("""
                select user_key, deleted, value
                from mvcc_intent
                where tx_id = ?
                order by user_key
                """)) {
            select.setLong(1, txId);
            try (ResultSet resultSet = select.executeQuery()) {
                List<IntentRow> rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(new IntentRow(
                            resultSet.getBytes("user_key"),
                            resultSet.getInt("deleted"),
                            resultSet.getBytes("value")
                    ));
                }
                return rows;
            }
        }
    }

    /**
     * 提交已加载的写意向行。
     */
    private void commitInternal(Connection connection, long txId, long commitTs, List<IntentRow> rows) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement("""
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
            for (IntentRow row : rows) {
                // 将 intent 行转换为不可变的已提交版本，commit_ts 决定快照可见性。
                insert.setBytes(1, row.userKey());
                insert.setLong(2, commitTs);
                insert.setLong(3, txId);
                insert.setInt(4, row.deleted());
                if (row.value() == null) {
                    insert.setNull(5, Types.BLOB);
                } else {
                    insert.setBytes(5, row.value());
                }
                insert.addBatch();
            }
            if (!rows.isEmpty()) {
                insert.executeBatch();
            }
            delete.setLong(1, txId);
            delete.executeUpdate();
        }
    }

    /**
     * 提交已知写集合对应的意向行。
     */
    private void commitKnownMutationsInternal(Connection connection,
                                             long txId,
                                             long commitTs,
                                             List<Mutation> mutations,
                                             boolean cleanupIntents) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                values (?, ?, ?, ?, ?)
                """)) {
            for (Mutation mutation : mutations) {
                insert.setBytes(1, encodeKey(mutation.key()));
                insert.setLong(2, commitTs);
                insert.setLong(3, txId);
                insert.setInt(4, mutation.type() == MutationType.DELETE ? 1 : 0);
                if (mutation.value() == null) {
                    insert.setNull(5, Types.BLOB);
                } else {
                    insert.setBytes(5, mutation.value());
                }
                insert.addBatch();
            }
            if (!mutations.isEmpty()) {
                insert.executeBatch();
            }
        }
        if (cleanupIntents) {
            deleteIntentsByTransaction(connection, txId);
        }
    }

    /**
     * 直接提交已知写集合。
     */
    private void directCommitInternal(Connection connection,
                                      long txId,
                                      long startTs,
                                      long commitTs,
                                      List<Mutation> mutations) throws SQLException {
        try (PreparedStatement conflictCheck = connection.prepareStatement("""
                select 1
                from mvcc_data
                where user_key = ? and commit_ts > ?
                limit 1
                """);
             PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_data(user_key, commit_ts, tx_id, deleted, value)
                values (?, ?, ?, ?, ?)
                """)) {
            for (Mutation mutation : mutations) {
                // 单阶段提交仍需检查 startTs 之后是否已有新版本，防止覆盖并发写入。
                conflictCheck.setBytes(1, encodeKey(mutation.key()));
                conflictCheck.setLong(2, startTs);
                try (ResultSet resultSet = conflictCheck.executeQuery()) {
                    if (resultSet.next()) {
                        throw new MvccException(
                                ErrorCode.WRITE_CONFLICT,
                                "write conflict on key " + mutation.key() + ", newer version exists",
                                true,
                                txId
                        );
                    }
                }
            }
            for (Mutation mutation : mutations) {
                insert.setBytes(1, encodeKey(mutation.key()));
                insert.setLong(2, commitTs);
                insert.setLong(3, txId);
                insert.setInt(4, mutation.type() == MutationType.DELETE ? 1 : 0);
                if (mutation.value() == null) {
                    insert.setNull(5, Types.BLOB);
                } else {
                    insert.setBytes(5, mutation.value());
                }
                insert.addBatch();
            }
            if (!mutations.isEmpty()) {
                insert.executeBatch();
            }
        }
    }

    /**
     * 按事务编号删除写意向。
     */
    private void deleteIntentsByTransaction(Connection connection, long txId) throws SQLException {
        try (PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_intent where tx_id = ?
                """)) {
            delete.setLong(1, txId);
            delete.executeUpdate();
        }
    }

    /**
     * 按已知业务键删除写意向。
     */
    private void deleteKnownIntents(Connection connection, long txId, List<byte[]> userKeys) throws SQLException {
        try (PreparedStatement delete = connection.prepareStatement("""
                delete from mvcc_intent where user_key = ? and tx_id = ?
                """)) {
            for (byte[] userKey : userKeys) {
                delete.setBytes(1, userKey);
                delete.setLong(2, txId);
                delete.addBatch();
            }
            if (!userKeys.isEmpty()) {
                delete.executeBatch();
            }
        }
    }

    /**
     * MySQL intent 行数据对象。
     */
    private record IntentRow(byte[] userKey, int deleted, byte[] value) {
    }
}

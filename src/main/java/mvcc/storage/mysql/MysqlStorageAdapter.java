package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.MutationType;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
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
    private long pendingScanAfter = Long.MIN_VALUE;
    private long terminalGcAfter = Long.MIN_VALUE;
    private boolean terminalGcTurn;

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
        } catch (SQLException ex) {
            throw storageError(null, "initialize schema failed", ex);
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

    /** 在本地事务中校验中心决议、冻结写集合并原子提交版本与终态。 */
    private void commitTransaction(long txId, long commitTs, List<Mutation> expected, boolean cleanup) {
        Optional<TxRecord> central = decision(txId);
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            ParticipantRecord record = requireParticipant(connection, txId);
            validateCommitTimestamp(record, commitTs);
            if (record.status() == TxStatus.COMMITTED) {
                if (expected != null) {
                    verifyCommittedWrites(connection, record, expected);
                }
                if (cleanup) {
                    cleanupCommittedIntents(connection, record);
                }
                return null;
            }
            requireNonTerminal(record);
            requireCommitDecision(record, commitTs, central);
            List<IntentRow> rows = loadIntents(connection, txId);
            if (rows.isEmpty()) {
                throw illegal(txId, "missing durable intents for uncommitted participant");
            }
            lockKeys(connection, rows.stream().map(IntentRow::key).toList());
            verifyIntentStart(record, rows);
            if (expected != null) {
                verifyMutations(txId, rows, expected);
            }
            insertVersions(connection, txId, commitTs, rows);
            updateParticipant(connection, txId, TxStatus.COMMITTED, commitTs);
            if (cleanup) {
                deleteIntents(connection, txId);
            }
            return null;
        });
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
        Optional<TxRecord> central = decision(txId);
        transaction(txId, false, connection -> {
            lockWatermark(connection, false);
            // 当前协议 txId 等于 startTs，无意向的回滚也持久化可供恢复查询的事务行。
            ParticipantRecord record = lockParticipant(connection, txId, txId);
            if (record.status() == TxStatus.COMMITTED) {
                return null;
            }
            if (central.isPresent() && central.get().status() == TxStatus.COMMITTED) {
                throw illegal(txId, "cannot abort a globally committed participant");
            }
            List<IntentRow> rows = loadIntents(connection, txId);
            lockKeys(connection, rows.stream().map(IntentRow::key).toList());
            writeRollbacks(connection, txId, rows);
            if (record.status() != TxStatus.ABORTED) {
                updateParticipant(connection, txId, TxStatus.ABORTED, null);
            }
            deleteIntents(connection, txId);
            return null;
        });
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
        checkLimit(limit);
        if (safePoint < 0) {
            throw new IllegalArgumentException("safePoint must be nonnegative");
        }
        List<ParticipantRecord> candidates = gcCandidates(safePoint, limit);
        boolean terminalFirst = terminalGcTurn;
        terminalGcTurn = !terminalGcTurn;
        return transaction(null, false, connection -> {
            long previous = lockWatermark(connection, true);
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
    private void requireCommitDecision(ParticipantRecord local, long commitTs, Optional<TxRecord> central) {
        if (central.isEmpty() || central.get().status() != TxStatus.COMMITTED
                || !Objects.equals(central.get().commitTs(), commitTs) || central.get().startTs() != local.startTs()) {
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

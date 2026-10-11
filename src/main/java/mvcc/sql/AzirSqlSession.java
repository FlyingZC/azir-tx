package mvcc.sql;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitResult;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.txn.StatementReadSnapshot;
import mvcc.txn.TransactionManager;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Azir SQL 会话默认实现，把 SQL 计划映射到现有 MVCC 事务核心。
 */
final class AzirSqlSession implements SqlSession {
    private final TransactionManager transactionManager;
    private final SqlPlanner planner;
    private final ShardResolver shardResolver;
    private final SqlValueCodec valueCodec;
    private final SqlStorageKeyCodec keyCodec;
    private final CalciteSqlExecutor calciteExecutor;
    private final IsolationLevel defaultIsolationLevel;
    private final SqlExecutionOptions executionOptions;
    private Transaction activeTransaction;
    private boolean invalidatedTransaction;
    private boolean closed;

    /**
     * 构造 Azir SQL 会话。
     */
    AzirSqlSession(TransactionManager transactionManager,
                   SqlPlanner planner,
                   ShardResolver shardResolver,
                   SqlValueCodec valueCodec,
                   SqlStorageKeyCodec keyCodec,
                   IsolationLevel defaultIsolationLevel,
                   SqlExecutionOptions executionOptions) {
        this.transactionManager = transactionManager;
        this.planner = planner;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.keyCodec = keyCodec;
        this.calciteExecutor = new CalciteSqlExecutor(transactionManager, shardResolver, valueCodec, keyCodec, executionOptions);
        this.defaultIsolationLevel = defaultIsolationLevel;
        this.executionOptions = executionOptions;
    }

    /**
     * 开启指定隔离级别的显式事务。
     */
    @Override
    public void begin(IsolationLevel isolationLevel) {
        ensureOpen();
        if (activeTransaction != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already active", false, activeTransaction.txId());
        }
        activeTransaction = transactionManager.begin(isolationLevel);
    }

    /**
     * 执行一条 SQL 并返回结果。
     */
    @Override
    public SqlExecutionResult execute(String sql) {
        ensureOpen();
        return executePlan(planner.plan(sql));
    }

    /**
     * 提交当前显式事务。
     */
    @Override
    public SqlExecutionResult commit() {
        ensureActiveTransaction();
        Transaction committingTransaction = activeTransaction;
        try {
            TransactionCommitResult result = transactionManager.commit(committingTransaction);
            activeTransaction = null;
            return SqlExecutionResult.committed(result);
        } catch (RuntimeException ex) {
            if (!canRollback(committingTransaction, ex)) {
                // 已提交或决议未知时不能再由会话 close 自动回滚，必须交给事务恢复流程处理。
                activeTransaction = null;
            }
            throw ex;
        }
    }

    /**
     * 回滚当前显式事务。
     */
    @Override
    public void rollback() {
        ensureOpen();
        if (activeTransaction == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "no active transaction", false, null);
        }
        transactionManager.rollback(activeTransaction);
        activeTransaction = null;
        invalidatedTransaction = false;
    }

    /**
     * 判断当前会话是否存在活动显式事务。
     */
    @Override
    public boolean active() {
        return !closed && activeTransaction != null && !invalidatedTransaction;
    }

    /**
     * 幂等终止会话并回滚未决事务；清理失败仍保持关闭状态，异常交给调用方处理。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Transaction closingTransaction = activeTransaction;
        activeTransaction = null;
        invalidatedTransaction = false;
        if (closingTransaction != null && closingTransaction.status() != TxStatus.COMMITTED
                && !closingTransaction.decisionStarted()) {
            transactionManager.rollback(closingTransaction);
        }
    }

    /**
     * 执行已生成的 SQL 计划。
     */
    private SqlExecutionResult executePlan(SqlPlan plan) {
        return switch (plan) {
            case SqlPlan.BeginTransactionPlan beginPlan -> executeBegin(beginPlan);
            case SqlPlan.CommitTransactionPlan ignored -> commit();
            case SqlPlan.RollbackTransactionPlan ignored -> {
                rollback();
                yield SqlExecutionResult.empty();
            }
            case SqlPlan.SelectByKeyPlan selectPlan -> executeDataPlan(selectPlan);
            case SqlPlan.InsertByKeyPlan insertPlan -> executeDataPlan(insertPlan);
            case SqlPlan.UpdateByKeyPlan updatePlan -> executeDataPlan(updatePlan);
            case SqlPlan.DeleteByKeyPlan deletePlan -> executeDataPlan(deletePlan);
            case SqlPlan.CalcitePlan calcitePlan -> executeDataPlan(calcitePlan);
        };
    }

    /**
     * 执行开启事务计划。
     */
    private SqlExecutionResult executeBegin(SqlPlan.BeginTransactionPlan plan) {
        begin(plan.isolationLevel());
        return SqlExecutionResult.empty();
    }

    /**
     * 执行读写数据计划，自动提交模式会在语句结束后提交事务。
     */
    private SqlExecutionResult executeDataPlan(SqlPlan plan) {
        if (invalidatedTransaction) {
            throw invalidatedTransactionException();
        }
        boolean autocommit = activeTransaction == null;
        Transaction tx = autocommit ? transactionManager.begin(defaultIsolationLevel) : activeTransaction;
        SqlExecutionResult result;
        try {
            StatementReadSnapshot snapshot = transactionManager.beginReadStatement(tx);
            result = executeDataPlan(tx, plan, snapshot);
            // 关系运算可能在最后一页之后持续计算，返回前必须复核原快照，不能刷新 RC 时间戳。
            transactionManager.validateReadStatement(tx, snapshot);
        } catch (RuntimeException ex) {
            if (!canRollback(tx, ex)) {
                if (!autocommit) {
                    activeTransaction = null;
                }
                throw ex;
            }
            if (autocommit) {
                rollbackQuietly(tx, ex);
            } else {
                throw invalidateTransaction(tx, ex);
            }
            throw ex;
        }
        if (!autocommit) {
            return result;
        }
        try {
            return result.withCommitResult(transactionManager.commit(tx));
        } catch (RuntimeException ex) {
            if (canRollback(tx, ex)) {
                rollbackQuietly(tx, ex);
            }
            throw ex;
        }
    }

    /**
     * 在指定事务中执行读写数据计划。
     */
    private SqlExecutionResult executeDataPlan(Transaction tx, SqlPlan plan, StatementReadSnapshot snapshot) {
        return switch (plan) {
            case SqlPlan.SelectByKeyPlan selectPlan -> executeSelect(tx, selectPlan, snapshot);
            case SqlPlan.InsertByKeyPlan insertPlan -> executeInsert(tx, insertPlan, snapshot);
            case SqlPlan.UpdateByKeyPlan updatePlan -> executeUpdate(tx, updatePlan, snapshot);
            case SqlPlan.DeleteByKeyPlan deletePlan -> executeDelete(tx, deletePlan, snapshot);
            case SqlPlan.CalcitePlan calcitePlan -> calciteExecutor.execute(tx, calcitePlan, snapshot);
            default -> throw new MvccException(ErrorCode.ILLEGAL_STATE, "not a data plan: " + plan, false, tx.txId());
        };
    }

    /**
     * 执行主键点查计划。
     */
    private SqlExecutionResult executeSelect(Transaction tx, SqlPlan.SelectByKeyPlan plan, StatementReadSnapshot snapshot) {
        String shardId = shardResolver.resolveShard(plan.table(), plan.key());
        Optional<byte[]> value = transactionManager.get(tx, shardId, keyCodec.encode(plan.table(), plan.key()), snapshot);
        if (value.isEmpty()) {
            return SqlExecutionResult.rows(java.util.List.of());
        }
        new SqlScanBudget(tx.txId(), executionOptions).chargeRow(keyCodec.encode(plan.table(), plan.key()), value.get());
        String decodedValue = valueCodec.decode(value.get());
        long resultBytes = (long) plan.key().getBytes(StandardCharsets.UTF_8).length
                + decodedValue.getBytes(StandardCharsets.UTF_8).length;
        if (resultBytes > executionOptions.maxResultBytes()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "SQL result byte budget exceeded: " + executionOptions.maxResultBytes(), false, tx.txId());
        }
        return SqlExecutionResult.rows(java.util.List.of(new SqlRow(Map.of(
                "id", plan.key(),
                "value", decodedValue
        ))));
    }

    /**
     * 简单计划器的 INSERT 同样按语句快照校验主键唯一性。
     */
    private SqlExecutionResult executeInsert(Transaction tx, SqlPlan.InsertByKeyPlan plan, StatementReadSnapshot snapshot) {
        if (exists(tx, plan.table(), plan.key(), snapshot)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "duplicate key: " + plan.key(), false, tx.txId());
        }
        return executePut(tx, plan.table(), plan.key(), plan.value());
    }

    /**
     * 简单计划器的 UPDATE 仅修改本语句快照中存在的行。
     */
    private SqlExecutionResult executeUpdate(Transaction tx, SqlPlan.UpdateByKeyPlan plan, StatementReadSnapshot snapshot) {
        return exists(tx, plan.table(), plan.key(), snapshot)
                ? executePut(tx, plan.table(), plan.key(), plan.value()) : SqlExecutionResult.updateCount(0);
    }

    /**
     * 使用调用方统一快照执行主键存在性检查。
     */
    private boolean exists(Transaction tx, String table, String key, StatementReadSnapshot snapshot) {
        return transactionManager.get(tx, shardResolver.resolveShard(table, key), keyCodec.encode(table, key), snapshot)
                .isPresent();
    }

    /**
     * 执行主键写入计划。
     */
    private SqlExecutionResult executePut(Transaction tx, String table, String key, String value) {
        String shardId = shardResolver.resolveShard(table, key);
        transactionManager.put(tx, shardId, keyCodec.encode(table, key), valueCodec.encode(value));
        return SqlExecutionResult.updateCount(1);
    }

    /**
     * 执行主键删除计划。
     */
    private SqlExecutionResult executeDelete(Transaction tx, SqlPlan.DeleteByKeyPlan plan, StatementReadSnapshot snapshot) {
        if (!exists(tx, plan.table(), plan.key(), snapshot)) {
            return SqlExecutionResult.updateCount(0);
        }
        String shardId = shardResolver.resolveShard(plan.table(), plan.key());
        transactionManager.delete(tx, shardId, keyCodec.encode(plan.table(), plan.key()));
        return SqlExecutionResult.updateCount(1);
    }

    /**
     * 确认当前存在活动显式事务。
     */
    private void ensureActiveTransaction() {
        ensureOpen();
        if (invalidatedTransaction) {
            throw invalidatedTransactionException();
        }
        if (activeTransaction == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "no active transaction", false, null);
        }
    }

    /**
     * 关闭状态不可逆，必须在调用计划器或创建事务前拒绝再次使用会话。
     */
    private void ensureOpen() {
        if (closed) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL session is closed", false, null);
        }
    }

    /**
     * 仅允许回滚未进入持久化决议的事务，异常链包含提交未知时也必须交给恢复。
     */
    private boolean canRollback(Transaction tx, RuntimeException ex) {
        if (tx.status() == TxStatus.COMMITTED || tx.decisionStarted()) {
            return false;
        }
        for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
            if (cause instanceof MvccException mvcc && mvcc.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN) {
                return false;
            }
        }
        return true;
    }

    /**
     * 自动提交失败时保留回滚根因，避免回滚异常掩盖原始 SQL 异常。
     */
    private void rollbackQuietly(Transaction tx, RuntimeException failure) {
        try {
            transactionManager.rollback(tx);
        } catch (RuntimeException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    /**
     * SQL 执行失败后回滚整个显式事务并保留失效状态；不提供隐含的单语句回滚。
     */
    private MvccException invalidateTransaction(Transaction tx, RuntimeException failure) {
        invalidatedTransaction = true;
        String outcome = "entire explicit transaction rolled back";
        try {
            transactionManager.rollback(tx);
        } catch (RuntimeException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
            outcome = "whole-transaction rollback failed; cleanup remains pending";
        }
        ErrorCode code = failure instanceof MvccException mvcc ? mvcc.errorCode() : ErrorCode.ILLEGAL_STATE;
        boolean retryable = failure instanceof MvccException mvcc && mvcc.retryable();
        return new MvccException(code, failure.getMessage() + "; " + outcome
                + "; transaction invalidated, issue ROLLBACK before reuse", retryable, tx.txId(), failure);
    }

    /**
     * 阻止失效事务继续执行或提交，也防止后续 SQL 意外转为自动提交。
     */
    private MvccException invalidatedTransactionException() {
        return new MvccException(ErrorCode.ILLEGAL_STATE,
                "transaction invalidated by failed SQL; issue ROLLBACK before reuse", false, activeTransaction.txId());
    }
}

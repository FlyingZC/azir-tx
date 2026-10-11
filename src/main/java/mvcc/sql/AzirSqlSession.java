package mvcc.sql;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitResult;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.txn.TransactionManager;

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
    private Transaction activeTransaction;

    /**
     * 构造 Azir SQL 会话。
     */
    AzirSqlSession(TransactionManager transactionManager,
                   SqlPlanner planner,
                   ShardResolver shardResolver,
                   SqlValueCodec valueCodec,
                   SqlStorageKeyCodec keyCodec,
                   IsolationLevel defaultIsolationLevel) {
        this.transactionManager = transactionManager;
        this.planner = planner;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.keyCodec = keyCodec;
        this.calciteExecutor = new CalciteSqlExecutor(transactionManager, shardResolver, valueCodec, keyCodec);
        this.defaultIsolationLevel = defaultIsolationLevel;
    }

    /**
     * 开启指定隔离级别的显式事务。
     */
    @Override
    public void begin(IsolationLevel isolationLevel) {
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
            if (!shouldRollbackAfterCommitFailure(ex)) {
                // 提交结果未知时不能再由会话 close 自动回滚，必须交给事务恢复流程处理。
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
        ensureActiveTransaction();
        transactionManager.rollback(activeTransaction);
        activeTransaction = null;
    }

    /**
     * 判断当前会话是否存在活动显式事务。
     */
    @Override
    public boolean active() {
        return activeTransaction != null;
    }

    /**
     * 关闭会话并回滚未完成的显式事务。
     */
    @Override
    public void close() {
        if (activeTransaction != null) {
            transactionManager.rollback(activeTransaction);
            activeTransaction = null;
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
        boolean autocommit = activeTransaction == null;
        Transaction tx = autocommit ? transactionManager.begin(defaultIsolationLevel) : activeTransaction;
        SqlExecutionResult result;
        try {
            result = executeDataPlan(tx, plan);
        } catch (RuntimeException ex) {
            if (autocommit) {
                rollbackQuietly(tx);
            }
            throw ex;
        }
        if (!autocommit) {
            return result;
        }
        try {
            return result.withCommitResult(transactionManager.commit(tx));
        } catch (RuntimeException ex) {
            if (shouldRollbackAfterCommitFailure(ex)) {
                rollbackQuietly(tx);
            }
            throw ex;
        }
    }

    /**
     * 在指定事务中执行读写数据计划。
     */
    private SqlExecutionResult executeDataPlan(Transaction tx, SqlPlan plan) {
        return switch (plan) {
            case SqlPlan.SelectByKeyPlan selectPlan -> executeSelect(tx, selectPlan);
            case SqlPlan.InsertByKeyPlan insertPlan -> executePut(tx, insertPlan.table(), insertPlan.key(), insertPlan.value());
            case SqlPlan.UpdateByKeyPlan updatePlan -> executePut(tx, updatePlan.table(), updatePlan.key(), updatePlan.value());
            case SqlPlan.DeleteByKeyPlan deletePlan -> executeDelete(tx, deletePlan);
            case SqlPlan.CalcitePlan calcitePlan -> calciteExecutor.execute(tx, calcitePlan);
            default -> throw new MvccException(ErrorCode.ILLEGAL_STATE, "not a data plan: " + plan, false, tx.txId());
        };
    }

    /**
     * 执行主键点查计划。
     */
    private SqlExecutionResult executeSelect(Transaction tx, SqlPlan.SelectByKeyPlan plan) {
        String shardId = shardResolver.resolveShard(plan.table(), plan.key());
        Optional<byte[]> value = transactionManager.get(tx, shardId, keyCodec.encode(plan.table(), plan.key()));
        if (value.isEmpty()) {
            return SqlExecutionResult.rows(java.util.List.of());
        }
        return SqlExecutionResult.rows(java.util.List.of(new SqlRow(Map.of(
                "id", plan.key(),
                "value", valueCodec.decode(value.get())
        ))));
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
    private SqlExecutionResult executeDelete(Transaction tx, SqlPlan.DeleteByKeyPlan plan) {
        String shardId = shardResolver.resolveShard(plan.table(), plan.key());
        transactionManager.delete(tx, shardId, keyCodec.encode(plan.table(), plan.key()));
        return SqlExecutionResult.updateCount(1);
    }

    /**
     * 确认当前存在活动显式事务。
     */
    private void ensureActiveTransaction() {
        if (activeTransaction == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "no active transaction", false, null);
        }
    }

    /**
     * 判断提交失败后是否仍可安全回滚。
     */
    private boolean shouldRollbackAfterCommitFailure(RuntimeException ex) {
        return !(ex instanceof MvccException mvccException
                && mvccException.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN);
    }

    /**
     * 安静回滚事务，避免回滚异常掩盖原始 SQL 执行异常。
     */
    private void rollbackQuietly(Transaction tx) {
        try {
            transactionManager.rollback(tx);
        } catch (RuntimeException ignored) {
        }
    }
}

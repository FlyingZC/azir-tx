package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.backend.TransactionBackend;
import com.flyingzc.azir.backend.BackendTransaction;
import com.flyingzc.azir.backend.BackendUnknownException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import static com.flyingzc.azir.backend.BackendTransaction.State.*;

/** 现有 SQL 会话合同的 可插拔后端 实现，显式失效与 UNKNOWN 不会退化成自动提交重做。 */
final class BackendSqlSession implements SqlSession {
    private final TransactionBackend runtime;
    private final SqlPlanner planner;
    private final ShardResolver resolver;
    private final SqlValueCodec codec;
    private final SqlStorageKeyCodec keyCodec = new SqlStorageKeyCodec();
    private final IsolationLevel isolation;
    private final SqlExecutionOptions options;
    private final CalciteSqlExecutor executor;
    private BackendTransaction activeTransaction;
    private boolean invalidated;
    private boolean closed;

    /** 为单个调用方装配计划和执行组件，事务只在 begin 或自动提交语句时创建。 */
    BackendSqlSession(TransactionBackend runtime, SqlPlanner planner, ShardResolver resolver,
                          SqlValueCodec codec, IsolationLevel isolation, SqlExecutionOptions options) {
        this.runtime = runtime;
        this.planner = planner;
        this.resolver = resolver;
        this.codec = codec;
        this.isolation = isolation;
        this.options = options;
        executor = new CalciteSqlExecutor(resolver, codec, keyCodec, options);
    }

    /** 创建显式 RC/RR，不能覆盖活动、失效或未知的原事务。 */
    @Override
    public void begin(IsolationLevel level) {
        ensureOpen();
        if (activeTransaction != null) { throw illegal("transaction already active or invalidated"); }
        activeTransaction = runtime.begin(level);
    }

    /** 使用现有计划类型；显式事务内的解析或数据执行错误都会使整笔事务失效。 */
    @Override
    public SqlExecutionResult execute(String sql) {
        ensureOpen();
        SqlPlan plan;
        try { plan = planner.plan(sql); }
        catch (RuntimeException failure) {
            if (activeTransaction != null && !invalidated) { invalidate(activeTransaction, failure); }
            throw failure;
        }
        return switch (plan) {
            case SqlPlan.BeginTransactionPlan begin -> { begin(begin.isolationLevel()); yield SqlExecutionResult.empty(); }
            case SqlPlan.CommitTransactionPlan ignored -> commit();
            case SqlPlan.RollbackTransactionPlan ignored -> { rollback(); yield SqlExecutionResult.empty(); }
            default -> executeData(plan);
        };
    }

    /** 成功提交直接返回原 primary 凭据；失败保留会话原身份直至明确处理。 */
    @Override
    public SqlExecutionResult commit() {
        ensureActive();
        try {
            var result = activeTransaction.commit();
            var committedResult = SqlExecutionResult.committed(result);
            activeTransaction = null;
            return committedResult;
        } catch (RuntimeException | Error failure) { invalidated = true; throw failure; }
    }

    /** 已回滚的失效事务可以清除；UNKNOWN 不得通过 ROLLBACK 改判为中止。 */
    @Override
    public void rollback() {
        ensureOpen();
        if (activeTransaction == null) { throw illegal("no active transaction"); }
        if (activeTransaction.state() == UNKNOWN) {
            throw new BackendUnknownException(activeTransaction.id(), null);
        }
        try {
            activeTransaction.rollback();
            activeTransaction = null;
            invalidated = false;
        } catch (RuntimeException | Error failure) { invalidated = true; throw failure; }
    }

    /** 只有可继续执行的显式事务才算活动。 */
    @Override
    public boolean active() { return !closed && activeTransaction != null && !invalidated; }

    /** 关闭先封闭入口，UNKNOWN 留给原身份查询，清理异常不能重新开放会话。 */
    @Override
    public void close() {
        if (closed) { return; }
        closed = true;
        var closing = activeTransaction;
        activeTransaction = null;
        invalidated = false;
        if (closing != null) { closing.close(); }
    }

    /** 每条 SQL 构造一次固定语句端口，自动提交与显式事务共享相同执行路径。 */
    private SqlExecutionResult executeData(SqlPlan plan) {
        if (invalidated) { throw illegal("transaction invalidated; issue ROLLBACK or resolve original UNKNOWN handle"); }
        boolean autocommit = activeTransaction == null;
        var tx = autocommit ? runtime.begin(isolation) : activeTransaction;
        SqlExecutionResult result;
        try (var view = tx.statement()) { result = executeData(new BackendSqlReadPort(tx, view), plan); }
        catch (RuntimeException failure) {
            if (!autocommit) { invalidate(tx, failure); }
            else { rollbackAfterFailure(tx, failure); }
            throw failure;
        } catch (Error fatal) {
            // 原事务仍可能持有缓冲写或已经预写，不能在同一 SQL 会话中透明创建新事务重做。
            activeTransaction = tx; invalidated = true; throw fatal;
        }
        if (!autocommit) { return result; }
        try { return result.withBackendCommitResult(tx.commit()); }
        catch (RuntimeException failure) {
            rollbackAfterFailure(tx, failure);
            throw failure;
        } catch (Error fatal) {
            // 原事务仍可能持有缓冲写或已经预写，不能在同一 SQL 会话中透明创建新事务重做。
            activeTransaction = tx; invalidated = true; throw fatal;
        }
    }

    /** 主键轻量计划与 Calcite 关系计划都只访问当前语句端口。 */
    private SqlExecutionResult executeData(SqlTransactionPort port, SqlPlan plan) {
        return switch (plan) {
            case SqlPlan.SelectByKeyPlan select -> select(port, select);
            case SqlPlan.InsertByKeyPlan insert -> {
                port.insert(resolver.resolveShard(insert.table(), insert.key()), keyCodec.encode(insert.table(), insert.key()),
                        codec.encode(insert.value()));
                yield SqlExecutionResult.updateCount(1);
            }
            case SqlPlan.UpdateByKeyPlan update -> {
                String shard = resolver.resolveShard(update.table(), update.key());
                String key = keyCodec.encode(update.table(), update.key());
                if (port.get(shard, key).isEmpty()) { yield SqlExecutionResult.updateCount(0); }
                port.put(shard, key, codec.encode(update.value()));
                yield SqlExecutionResult.updateCount(1);
            }
            case SqlPlan.DeleteByKeyPlan delete -> {
                String shard = resolver.resolveShard(delete.table(), delete.key());
                String key = keyCodec.encode(delete.table(), delete.key());
                if (port.get(shard, key).isEmpty()) { yield SqlExecutionResult.updateCount(0); }
                port.delete(shard, key);
                yield SqlExecutionResult.updateCount(1);
            }
            case SqlPlan.CalcitePlan calcite -> executor.execute(port, calcite);
            default -> throw illegal("not a data plan");
        };
    }

    /** 简单主键点读保持现有输入与结果预算，不绕过 SQL 资源限制。 */
    private SqlExecutionResult select(SqlTransactionPort port, SqlPlan.SelectByKeyPlan plan) {
        String key = keyCodec.encode(plan.table(), plan.key());
        var value = port.get(resolver.resolveShard(plan.table(), plan.key()), key);
        if (value.isEmpty()) { return SqlExecutionResult.rows(List.of()); }
        new SqlScanBudget(port.diagnosticTransactionId(), options).chargeRow(key, value.orElseThrow());
        String decoded = codec.decode(value.orElseThrow());
        if ((long) plan.key().getBytes(StandardCharsets.UTF_8).length
                + decoded.getBytes(StandardCharsets.UTF_8).length > options.maxResultBytes()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "SQL result byte budget exceeded", false, port.diagnosticTransactionId());
        }
        return SqlExecutionResult.rows(List.of(new SqlRow(Map.of("id", plan.key(), "value", decoded))));
    }

    /** 显式语句失败后回滚全事务并保留失效句柄，禁止后续数据 SQL 偷偷自动提交。 */
    private void invalidate(BackendTransaction tx, RuntimeException failure) {
        invalidated = true;
        rollbackAfterFailure(tx, failure);
    }

    /** 清理只作用于明确活动的事务；未知结果异常必须保留完整 primary 身份。 */
    private void rollbackAfterFailure(BackendTransaction tx, RuntimeException failure) {
        if (tx.state() == UNKNOWN) {
            activeTransaction = tx;
            invalidated = true;
            if (failure instanceof BackendUnknownException unknown) { throw unknown; }
            throw new BackendUnknownException(tx.id(), failure);
        }
        if (tx.state() == ACTIVE) {
            try { tx.rollback(); }
            catch (RuntimeException cleanup) {
                if (cleanup instanceof BackendUnknownException unknown) {
                    activeTransaction = tx;
                    invalidated = true;
                    unknown.addSuppressed(failure);
                    throw unknown;
                }
                failure.addSuppressed(cleanup);
            }
        }
    }

    /** TCL 提交入口要求存在未失效的显式事务。 */
    private void ensureActive() {
        ensureOpen();
        if (activeTransaction == null || invalidated) { throw illegal("no valid active transaction"); }
    }

    /** 关闭状态不可逆，必须在解析 SQL 和申请时间戳之前拒绝操作。 */
    private void ensureOpen() { if (closed) { throw illegal("SQL session is closed"); } }

    /** SQL 状态错误继续使用原事务 startTs 作为诊断身份。 */
    private MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false,
                activeTransaction == null ? null : activeTransaction.id().startTs());
    }
}

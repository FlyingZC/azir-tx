/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.percolator.PercolatorTransaction;
import com.flyingzc.azir.percolator.PercolatorTransactionStatusUnknownException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Percolator of the existing SQL session contract was achieved, the apparent lapse and UNKNOWN would not be reworked to autocommit.
 */
final class PercolatorSqlSession implements SqlSession {

    private final PercolatorRuntime runtime;

    private final SqlPlanner planner;

    private final ShardResolver resolver;

    private final SqlValueCodec codec;

    private final SqlStorageKeyCodec keyCodec = new SqlStorageKeyCodec();

    private final IsolationLevel isolation;

    private final SqlExecutionOptions options;

    private final CalciteSqlExecutor executor;

    private PercolatorTransaction activeTransaction;

    private boolean invalidated;

    private boolean closed;

    /**
     * Transaction is created only for begin or autocommit statements for individual caller assembly plan and execution components.
     *
     * @param runtime runtime
     * @param planner planner
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param options options
     */
    PercolatorSqlSession(final PercolatorRuntime runtime, final SqlPlanner planner, final ShardResolver resolver,
                         final SqlValueCodec codec, final IsolationLevel isolation, final SqlExecutionOptions options) {
        this.runtime = runtime;
        this.planner = planner;
        this.resolver = resolver;
        this.codec = codec;
        this.isolation = isolation;
        this.options = options;
        executor = new CalciteSqlExecutor(resolver, codec, keyCodec, options);
    }

    /**
     * Creates a visible RC/RR that does not override active, invalid or unknown original transaction.
     *
     * @param level level
     */
    @Override
    public void begin(final IsolationLevel level) {
        ensureOpen();
        if (activeTransaction != null) {
            throw illegal("transaction already active or invalidated");
        }
        activeTransaction = runtime.begin(level);
    }

    /**
     * Use the existing scheme type; a large transaction file or an error in data execution invalidates the whole transaction.
     *
     * @param sql sql
     * @return the operation result
     */
    @Override
    public SqlExecutionResult execute(final String sql) {
        ensureOpen();
        SqlPlan plan;
        try {
            plan = planner.plan(sql);
        } catch (final RuntimeException failure) {
            if (activeTransaction != null && !invalidated) {
                invalidate(activeTransaction, failure);
            }
            throw failure;
        }
        return switch (plan) {
            case SqlPlan.BeginTransactionPlan begin -> {
                begin(begin.isolationLevel());
                yield SqlExecutionResult.empty();
            }
            case SqlPlan.CommitTransactionPlan ignored -> commit();
            case SqlPlan.RollbackTransactionPlan ignored -> {
                rollback();
                yield SqlExecutionResult.empty();
            }
            default -> executeData(plan);
        };
    }

    /**
     * Return the original primary receipt after a confirmed commit; preserve the session identity on failure until its outcome is resolved.
     *
     * @return the operation result
     */
    @Override
    public SqlExecutionResult commit() {
        ensureActive();
        try {
            var result = activeTransaction.commit();
            var committedResult = SqlExecutionResult.committed(result);
            activeTransaction = null;
            return committedResult;
        } catch (final RuntimeException | Error failure) {
            invalidated = true;
            throw failure;
        }
    }

    /**
     * Clear invalid transactions that were rolled back; ROLLBACK must never reclassify UNKNOWN as aborted.
     *
     * @throws PercolatorTransactionStatusUnknownException if the operation cannot complete
     */
    @Override
    public void rollback() {
        ensureOpen();
        if (activeTransaction == null) {
            throw illegal("no active transaction");
        }
        if (activeTransaction.state() == com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN) {
            throw new PercolatorTransactionStatusUnknownException(activeTransaction.handle().orElseThrow(), null);
        }
        try {
            activeTransaction.rollback();
            activeTransaction = null;
            invalidated = false;
        } catch (final RuntimeException | Error failure) {
            invalidated = true;
            throw failure;
        }
    }

    /**
     * Only visible transaction s that can be continued are counted as active.
     *
     * @return the operation result
     */
    @Override
    public boolean active() {
        return !closed && activeTransaction != null && !invalidated;
    }

    /**
     * Close the portal before closing, UNKNOWN leaves original identity queries and cleanup anomalies cannot reopen session.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        var closing = activeTransaction;
        activeTransaction = null;
        invalidated = false;
        if (closing != null) {
            closing.close();
        }
    }

    /**
     * Each SQL constructs a fixed statement port, autocommit shares the same execution path as the visible transaction.
     *
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeData(final SqlPlan plan) {
        if (invalidated) {
            throw illegal("transaction invalidated; issue ROLLBACK or resolve original UNKNOWN handle");
        }
        boolean autocommit = activeTransaction == null;
        var tx = autocommit ? runtime.begin(isolation) : activeTransaction;
        SqlExecutionResult result;
        try {
            result = executeData(new PercolatorSqlReadPort(tx), plan);
        } catch (final RuntimeException failure) {
            if (!autocommit) {
                invalidate(tx, failure);
            } else {
                rollbackAfterFailure(tx, failure);
            }
            throw failure;
        } catch (final Error fatal) {
            // The original transaction may still own buffered or prewritten changes; do not silently retry it as a new transaction in the same SQL session.
            activeTransaction = tx;
            invalidated = true;
            throw fatal;
        }
        if (!autocommit) {
            return result;
        }
        try {
            return result.withPercolatorCommitResult(tx.commit());
        } catch (final RuntimeException failure) {
            rollbackAfterFailure(tx, failure);
            throw failure;
        } catch (final Error fatal) {
            // The original transaction may still own buffered or prewritten changes; do not silently retry it as a new transaction in the same SQL session.
            activeTransaction = tx;
            invalidated = true;
            throw fatal;
        }
    }

    /**
     * The main key light schedule and the Calcite relationship plan only visit the current statement port.
     *
     * @param port port
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeData(final SqlTransactionPort port, final SqlPlan plan) {
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
                if (port.get(shard, key).isEmpty()) {
                    yield SqlExecutionResult.updateCount(0);
                }
                port.put(shard, key, codec.encode(update.value()));
                yield SqlExecutionResult.updateCount(1);
            }
            case SqlPlan.DeleteByKeyPlan delete -> {
                String shard = resolver.resolveShard(delete.table(), delete.key());
                String key = keyCodec.encode(delete.table(), delete.key());
                if (port.get(shard, key).isEmpty()) {
                    yield SqlExecutionResult.updateCount(0);
                }
                port.delete(shard, key);
                yield SqlExecutionResult.updateCount(1);
            }
            case SqlPlan.CalcitePlan calcite -> executor.execute(port, calcite);
            default -> throw illegal("not a data plan");
        };
    }

    /**
     * Simple primary key click to keep the existing input and result budget, without circumventing SQL resource limits.
     *
     * @param port port
     * @param plan plan
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private SqlExecutionResult select(final SqlTransactionPort port, final SqlPlan.SelectByKeyPlan plan) {
        String key = keyCodec.encode(plan.table(), plan.key());
        var value = port.get(resolver.resolveShard(plan.table(), plan.key()), key);
        if (value.isEmpty()) {
            return SqlExecutionResult.rows(List.of());
        }
        new SqlScanBudget(port.diagnosticTransactionId(), options).chargeRow(key, value.orElseThrow());
        String decoded = codec.decode(value.orElseThrow());
        if ((long) plan.key().getBytes(StandardCharsets.UTF_8).length
                + decoded.getBytes(StandardCharsets.UTF_8).length > options.maxResultBytes()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "SQL result byte budget exceeded", false, port.diagnosticTransactionId());
        }
        return SqlExecutionResult.rows(List.of(new SqlRow(Map.of("id", plan.key(), "value", decoded))));
    }

    /**
     * Rollback All transaction after the apparent statement failed and keep the invalid handle to prevent SQL sneaking autocommit.
     *
     * @param tx transaction context
     * @param failure failure
     */
    private void invalidate(final PercolatorTransaction tx, final RuntimeException failure) {
        invalidated = true;
        rollbackAfterFailure(tx, failure);
    }

    /**
     * Cleanup only works on a clearly active transaction; the unknown result abnormally must keep the complete Primary identity.
     *
     * @param tx transaction context
     * @param failure failure
     * @throws PercolatorTransactionStatusUnknownException if the operation cannot complete
     */
    private void rollbackAfterFailure(final PercolatorTransaction tx, final RuntimeException failure) {
        if (tx.state() == com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN) {
            activeTransaction = tx;
            invalidated = true;
            if (failure instanceof PercolatorTransactionStatusUnknownException unknown) {
                throw unknown;
            }
            throw new PercolatorTransactionStatusUnknownException(tx.handle().orElseThrow(), failure);
        }
        if (tx.state() == com.flyingzc.azir.percolator.PercolatorTransaction.State.ACTIVE || tx.state() == com.flyingzc.azir.percolator.PercolatorTransaction.State.PREWRITTEN) {
            try {
                tx.rollback();
            } catch (final RuntimeException cleanup) {
                if (cleanup instanceof PercolatorTransactionStatusUnknownException unknown) {
                    activeTransaction = tx;
                    invalidated = true;
                    unknown.addSuppressed(failure);
                    throw unknown;
                }
                failure.addSuppressed(cleanup);
            }
        }
    }

    /**
     * The TCL commit entry requires an undefunct visible transaction.
     */
    private void ensureActive() {
        ensureOpen();
        if (activeTransaction == null || invalidated) {
            throw illegal("no valid active transaction");
        }
    }

    /**
     * Closing is irreversible; reject further operations before parsing SQL or allocating timestamps.
     */
    private void ensureOpen() {
        if (closed) {
            throw illegal("SQL session is closed");
        }
    }

    /**
     * SQL status error continues using transaction StartTs as a diagnostic identity.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false,
                activeTransaction == null ? null : activeTransaction.startTs());
    }
}

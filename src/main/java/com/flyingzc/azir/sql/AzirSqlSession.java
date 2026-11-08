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

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.api.TransactionCommitResult;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.txn.StatementReadSnapshot;
import com.flyingzc.azir.txn.TransactionManager;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * The Azir SQL session default was achieved, and the SQL plan was mapped to the existing MVCC transaction core.
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
     * Construct Azir SQL session.
     *
     * @param transactionManager transaction manager
     * @param planner planner
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param keyCodec key codec
     * @param defaultIsolationLevel default isolation level
     * @param executionOptions execution options
     */
    AzirSqlSession(final TransactionManager transactionManager,
                   final SqlPlanner planner,
                   final ShardResolver shardResolver,
                   final SqlValueCodec valueCodec,
                   final SqlStorageKeyCodec keyCodec,
                   final IsolationLevel defaultIsolationLevel,
                   final SqlExecutionOptions executionOptions) {
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
     * Turn on visible transaction for the specified level of isolation.
     *
     * @param isolationLevel global transaction isolation level
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void begin(final IsolationLevel isolationLevel) {
        ensureOpen();
        if (activeTransaction != null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction already active", false, activeTransaction.txId());
        }
        activeTransaction = transactionManager.begin(isolationLevel);
    }

    /**
     * Run a SQL and return the results.
     *
     * @param sql sql
     * @return the operation result
     */
    @Override
    public SqlExecutionResult execute(final String sql) {
        ensureOpen();
        return executePlan(planner.plan(sql));
    }

    /**
     * Commit Current Visibility transaction.
     *
     * @return the operation result
     */
    @Override
    public SqlExecutionResult commit() {
        ensureActiveTransaction();
        Transaction committingTransaction = activeTransaction;
        try {
            TransactionCommitResult result = transactionManager.commit(committingTransaction);
            activeTransaction = null;
            return SqlExecutionResult.committed(result);
        } catch (final RuntimeException ex) {
            if (!canRollback(committingTransaction, ex)) {
                // Commit or decision can no longer be automatically rollback by session close when it is unknown and must be handed over to the transaction recovery process.
                activeTransaction = null;
            }
            throw ex;
        }
    }

    /**
     * Rollback Current Visibility transaction.
     *
     * @throws MvccException if the operation cannot complete
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
     * Determines if the current session is active transaction.
     *
     * @return the operation result
     */
    @Override
    public boolean active() {
        return !closed && activeTransaction != null && !invalidatedTransaction;
    }

    /**
     * Idempotent terminates session and rollback pending transaction; clean-up failed to remain closed, abnormally handed over to the caller for processing.
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
     * Execute the generated SQL plan.
     *
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executePlan(final SqlPlan plan) {
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
     * Execute the open transaction schedule.
     *
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeBegin(final SqlPlan.BeginTransactionPlan plan) {
        begin(plan.isolationLevel());
        return SqlExecutionResult.empty();
    }

    /**
     * Execute the reading and writing data plan, autocommit mode will be commit transaction at the end of the statement.
     *
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeDataPlan(final SqlPlan plan) {
        if (invalidatedTransaction) {
            throw invalidatedTransactionException();
        }
        boolean autocommit = activeTransaction == null;
        Transaction tx = autocommit ? transactionManager.begin(defaultIsolationLevel) : activeTransaction;
        SqlExecutionResult result;
        try {
            StatementReadSnapshot snapshot = transactionManager.beginReadStatement(tx);
            result = executeDataPlan(tx, plan, snapshot);
            // Relationship calculations may continue after the last page, and snapshot must be reviewed before returning. RC timestamp cannot be refreshed.
            transactionManager.validateReadStatement(tx, snapshot);
        } catch (final RuntimeException ex) {
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
        } catch (final RuntimeException ex) {
            if (canRollback(tx, ex)) {
                rollbackQuietly(tx, ex);
            }
            throw ex;
        }
    }

    /**
     * Execute a reading and writing data plan in the specified transaction.
     *
     * @param tx transaction context
     * @param plan plan
     * @param snapshot snapshot
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private SqlExecutionResult executeDataPlan(final Transaction tx, final SqlPlan plan, final StatementReadSnapshot snapshot) {
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
     * Execute the primary key point check plan.
     *
     * @param tx transaction context
     * @param plan plan
     * @param snapshot snapshot
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private SqlExecutionResult executeSelect(final Transaction tx, final SqlPlan.SelectByKeyPlan plan, final StatementReadSnapshot snapshot) {
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
                "value", decodedValue))));
    }

    /**
     * The simple planner's INSERT is also a statement snapshot validate primary uniqueness.
     *
     * @param tx transaction context
     * @param plan plan
     * @param snapshot snapshot
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private SqlExecutionResult executeInsert(final Transaction tx, final SqlPlan.InsertByKeyPlan plan, final StatementReadSnapshot snapshot) {
        if (exists(tx, plan.table(), plan.key(), snapshot)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "duplicate key: " + plan.key(), false, tx.txId());
        }
        return executePut(tx, plan.table(), plan.key(), plan.value());
    }

    /**
     * The UPDATE of the simple planner changes only the rows in this statement snapshot.
     *
     * @param tx transaction context
     * @param plan plan
     * @param snapshot snapshot
     * @return the operation result
     */
    private SqlExecutionResult executeUpdate(final Transaction tx, final SqlPlan.UpdateByKeyPlan plan, final StatementReadSnapshot snapshot) {
        return exists(tx, plan.table(), plan.key(), snapshot)
                ? executePut(tx, plan.table(), plan.key(), plan.value())
                : SqlExecutionResult.updateCount(0);
    }

    /**
     * Use the caller to align snapshot to perform existential check of the primary key.
     *
     * @param tx transaction context
     * @param table table
     * @param key business key
     * @param snapshot snapshot
     * @return the operation result
     */
    private boolean exists(final Transaction tx, final String table, final String key, final StatementReadSnapshot snapshot) {
        return transactionManager.get(tx, shardResolver.resolveShard(table, key), keyCodec.encode(table, key), snapshot)
                .isPresent();
    }

    /**
     * Execute the primary key to write to the plan.
     *
     * @param tx transaction context
     * @param table table
     * @param key business key
     * @param value value to write
     * @return the operation result
     */
    private SqlExecutionResult executePut(final Transaction tx, final String table, final String key, final String value) {
        String shardId = shardResolver.resolveShard(table, key);
        transactionManager.put(tx, shardId, keyCodec.encode(table, key), valueCodec.encode(value));
        return SqlExecutionResult.updateCount(1);
    }

    /**
     * Execute the primary key to delete the schedule.
     *
     * @param tx transaction context
     * @param plan plan
     * @param snapshot snapshot
     * @return the operation result
     */
    private SqlExecutionResult executeDelete(final Transaction tx, final SqlPlan.DeleteByKeyPlan plan, final StatementReadSnapshot snapshot) {
        if (!exists(tx, plan.table(), plan.key(), snapshot)) {
            return SqlExecutionResult.updateCount(0);
        }
        String shardId = shardResolver.resolveShard(plan.table(), plan.key());
        transactionManager.delete(tx, shardId, keyCodec.encode(plan.table(), plan.key()));
        return SqlExecutionResult.updateCount(1);
    }

    /**
     * Confirms current active visible transaction.
     *
     * @throws MvccException if the operation cannot complete
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
     * Turning off state irreversible must refuse to use session again before calling the planner or creating transaction.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void ensureOpen() {
        if (closed) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL session is closed", false, null);
        }
    }

    /**
     * Only rollback is allowed to enter transaction without durable storage decision, and an abnormal chain containing commit is also required to be handed over to recovery when the commit
     * is unknown.
     *
     * @param tx transaction context
     * @param ex ex
     * @return the operation result
     */
    private boolean canRollback(final Transaction tx, final RuntimeException ex) {
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
     * If autocommit fails, keep the rollback root cause to avoid the rollback abnormally masking the original SQL anomaly.
     *
     * @param tx transaction context
     * @param failure failure
     */
    private void rollbackQuietly(final Transaction tx, final RuntimeException failure) {
        try {
            transactionManager.rollback(tx);
        } catch (final RuntimeException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    /**
     * The whole visible transaction after SQL failed to execute; no implicit statement rollback is provided.
     *
     * @param tx transaction context
     * @param failure failure
     * @return the operation result
     */
    private MvccException invalidateTransaction(final Transaction tx, final RuntimeException failure) {
        invalidatedTransaction = true;
        String outcome = "entire explicit transaction rolled back";
        try {
            transactionManager.rollback(tx);
        } catch (final RuntimeException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
            outcome = "whole-transaction rollback failed; cleanup remains pending";
        }
        ErrorCode code = failure instanceof MvccException mvcc ? mvcc.errorCode() : ErrorCode.ILLEGAL_STATE;
        boolean retryable = failure instanceof MvccException mvcc && mvcc.retryable();
        return new MvccException(code, failure.getMessage() + "; " + outcome
                + "; transaction invalidated, issue ROLLBACK before reuse", retryable, tx.txId(), failure);
    }

    /**
     * Prevents the failure of transaction to continue or commit and prevents subsequent SQL from unexpectedly converting to autocommit.
     *
     * @return the operation result
     */
    private MvccException invalidatedTransactionException() {
        return new MvccException(ErrorCode.ILLEGAL_STATE,
                "transaction invalidated by failed SQL; issue ROLLBACK before reuse", false, activeTransaction.txId());
    }
}

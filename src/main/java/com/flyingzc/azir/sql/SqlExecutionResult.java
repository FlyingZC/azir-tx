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

import com.flyingzc.azir.api.TransactionCommitResult;
import com.flyingzc.azir.backend.BackendCommit;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import java.util.List;

/**
 * SQL rows, affected-row counts and the commit identity of the selected backend.
 *
 * @param rows rows
 * @param updateCount update count
 * @param commitResult commit result
 * @param percolatorCommitResult percolator commit result
 * @param backendCommitResult backend commit result
 */
public record SqlExecutionResult(List<SqlRow> rows, int updateCount,
        TransactionCommitResult commitResult,
        PercolatorRuntime.Commit percolatorCommitResult,
        BackendCommit backendCommitResult) {

    /**
     * Copy rows and reject ambiguous commit identities.
     *
     * @param rows rows
     * @param updateCount update count
     * @param commitResult commit result
     * @param percolatorCommitResult percolator commit result
     * @param backendCommitResult backend commit result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public SqlExecutionResult {
        rows = List.copyOf(rows);
        int identities = (commitResult == null ? 0 : 1)
                + (percolatorCommitResult == null ? 0 : 1) + (backendCommitResult == null ? 0 : 1);
        if (identities > 1) {
            throw new IllegalArgumentException("SQL result cannot contain two backend commit identities");
        }
    }

    /**
     * Create a result for the default transaction coordinator.
     *
     * @param rows rows
     * @param updateCount update count
     * @param commitResult commit result
     */
    public SqlExecutionResult(final List<SqlRow> rows, final int updateCount, final TransactionCommitResult commitResult) {
        this(rows, updateCount, commitResult, null, null);
    }

    /**
     * Create a result with a Percolator primary identity.
     *
     * @param rows rows
     * @param updateCount update count
     * @param commitResult commit result
     * @param percolatorCommitResult percolator commit result
     */
    public SqlExecutionResult(final List<SqlRow> rows, final int updateCount, final TransactionCommitResult commitResult,
                              final PercolatorRuntime.Commit percolatorCommitResult) {
        this(rows, updateCount, commitResult, percolatorCommitResult, null);
    }

    /**
     * Return an empty statement result.
     *
     * @return the operation result
     */
    public static SqlExecutionResult empty() {
        return new SqlExecutionResult(List.of(), 0, null);
    }

    /**
     * Return query rows.
     *
     * @param rows rows
     * @return the operation result
     */
    public static SqlExecutionResult rows(final List<SqlRow> rows) {
        return new SqlExecutionResult(rows, 0, null);
    }

    /**
     * Return the number of affected rows.
     *
     * @param updateCount update count
     * @return the operation result
     */
    public static SqlExecutionResult updateCount(final int updateCount) {
        return new SqlExecutionResult(List.of(), updateCount, null);
    }

    /**
     * Return the default coordinator's durable commit result.
     *
     * @param commitResult commit result
     * @return the operation result
     */
    public static SqlExecutionResult committed(final TransactionCommitResult commitResult) {
        return new SqlExecutionResult(List.of(), 0, commitResult);
    }

    /**
     * Attach the default coordinator's commit result to existing rows or counts.
     *
     * @param commitResult commit result
     * @return the operation result
     */
    public SqlExecutionResult withCommitResult(final TransactionCommitResult commitResult) {
        return new SqlExecutionResult(rows, updateCount, commitResult);
    }

    /**
     * Return the primary identity, timestamp and pending Percolator secondaries.
     *
     * @param commit commit
     * @return the operation result
     */
    public static SqlExecutionResult committed(final PercolatorRuntime.Commit commit) {
        return new SqlExecutionResult(List.of(), 0, null, commit);
    }

    /**
     * Attach the original Percolator commit identity without losing statement results.
     *
     * @param commit commit
     * @return the operation result
     */
    public SqlExecutionResult withPercolatorCommitResult(final PercolatorRuntime.Commit commit) {
        return new SqlExecutionResult(rows, updateCount, null, commit);
    }

    /**
     * Return an extension backend's namespaced commit identity.
     *
     * @param commit commit
     * @return the operation result
     */
    public static SqlExecutionResult committed(final BackendCommit commit) {
        return new SqlExecutionResult(List.of(), 0, null, null, commit);
    }

    /**
     * Attach an extension backend's actual commit identity.
     *
     * @param commit commit
     * @return the operation result
     */
    public SqlExecutionResult withBackendCommitResult(final BackendCommit commit) {
        return new SqlExecutionResult(rows, updateCount, null, null, commit);
    }

    /**
     * Return exactly one row, rejecting empty and multi-row results.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public SqlRow singleRow() {
        if (rows.size() != 1) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "expected single row but got " + rows.size(), false, null);
        }
        return rows.getFirst();
    }
}

package mvcc.sql;

import mvcc.api.TransactionCommitResult;
import mvcc.backend.BackendCommit;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.percolator.PercolatorRuntime;
import java.util.List;

/** SQL rows, affected-row counts and the commit identity of the selected backend. */
public record SqlExecutionResult(List<SqlRow> rows, int updateCount,
                                 TransactionCommitResult commitResult,
                                 PercolatorRuntime.Commit percolatorCommitResult,
                                 BackendCommit backendCommitResult) {
    /** Copy rows and reject ambiguous commit identities. */
    public SqlExecutionResult {
        rows = List.copyOf(rows);
        int identities = (commitResult == null ? 0 : 1)
                + (percolatorCommitResult == null ? 0 : 1) + (backendCommitResult == null ? 0 : 1);
        if (identities > 1) {
            throw new IllegalArgumentException("SQL result cannot contain two backend commit identities");
        }
    }

    /** Create a result for the default transaction coordinator. */
    public SqlExecutionResult(List<SqlRow> rows, int updateCount, TransactionCommitResult commitResult) {
        this(rows, updateCount, commitResult, null, null);
    }

    /** Create a result with a Percolator primary identity. */
    public SqlExecutionResult(List<SqlRow> rows, int updateCount, TransactionCommitResult commitResult,
                              PercolatorRuntime.Commit percolatorCommitResult) {
        this(rows, updateCount, commitResult, percolatorCommitResult, null);
    }

    /** Return an empty statement result. */
    public static SqlExecutionResult empty() {
        return new SqlExecutionResult(List.of(), 0, null);
    }

    /** Return query rows. */
    public static SqlExecutionResult rows(List<SqlRow> rows) {
        return new SqlExecutionResult(rows, 0, null);
    }

    /** Return the number of affected rows. */
    public static SqlExecutionResult updateCount(int updateCount) {
        return new SqlExecutionResult(List.of(), updateCount, null);
    }

    /** Return the default coordinator's durable commit result. */
    public static SqlExecutionResult committed(TransactionCommitResult commitResult) {
        return new SqlExecutionResult(List.of(), 0, commitResult);
    }

    /** Attach the default coordinator's commit result to existing rows or counts. */
    public SqlExecutionResult withCommitResult(TransactionCommitResult commitResult) {
        return new SqlExecutionResult(rows, updateCount, commitResult);
    }

    /** Return the primary identity, timestamp and pending Percolator secondaries. */
    public static SqlExecutionResult committed(PercolatorRuntime.Commit commit) {
        return new SqlExecutionResult(List.of(), 0, null, commit);
    }

    /** Attach the original Percolator commit identity without losing statement results. */
    public SqlExecutionResult withPercolatorCommitResult(PercolatorRuntime.Commit commit) {
        return new SqlExecutionResult(rows, updateCount, null, commit);
    }

    /** Return an extension backend's namespaced commit identity. */
    public static SqlExecutionResult committed(BackendCommit commit) {
        return new SqlExecutionResult(List.of(), 0, null, null, commit);
    }

    /** Attach an extension backend's actual commit identity. */
    public SqlExecutionResult withBackendCommitResult(BackendCommit commit) {
        return new SqlExecutionResult(rows, updateCount, null, null, commit);
    }

    /** Return exactly one row, rejecting empty and multi-row results. */
    public SqlRow singleRow() {
        if (rows.size() != 1) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "expected single row but got " + rows.size(), false, null);
        }
        return rows.getFirst();
    }
}

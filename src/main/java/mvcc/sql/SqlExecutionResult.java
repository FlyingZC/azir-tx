package mvcc.sql;

import mvcc.api.TransactionCommitResult;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.util.List;

/**
 * SQL 执行结果，统一表达查询行、更新数量和自动提交结果。
 */
public record SqlExecutionResult(List<SqlRow> rows, int updateCount, TransactionCommitResult commitResult) {
    /**
     * 构造不可变 SQL 执行结果。
     */
    public SqlExecutionResult {
        rows = List.copyOf(rows);
    }

    /**
     * 创建空执行结果。
     */
    public static SqlExecutionResult empty() {
        return new SqlExecutionResult(List.of(), 0, null);
    }

    /**
     * 创建查询执行结果。
     */
    public static SqlExecutionResult rows(List<SqlRow> rows) {
        return new SqlExecutionResult(rows, 0, null);
    }

    /**
     * 创建更新执行结果。
     */
    public static SqlExecutionResult updateCount(int updateCount) {
        return new SqlExecutionResult(List.of(), updateCount, null);
    }

    /**
     * 创建事务提交执行结果。
     */
    public static SqlExecutionResult committed(TransactionCommitResult commitResult) {
        return new SqlExecutionResult(List.of(), 0, commitResult);
    }

    /**
     * 返回附带提交结果的新执行结果。
     */
    public SqlExecutionResult withCommitResult(TransactionCommitResult commitResult) {
        return new SqlExecutionResult(rows, updateCount, commitResult);
    }

    /**
     * 返回唯一结果行。
     */
    public SqlRow singleRow() {
        if (rows.size() != 1) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "expected single row but got " + rows.size(), false, null);
        }
        return rows.getFirst();
    }
}

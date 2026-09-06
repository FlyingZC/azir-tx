package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.sql.SQLException;

/**
 * 瞬时存储冲突识别器，用于判断异常是否适合重试。
 */
final class TransientStorageConflictDetector {
    /**
     * 构造 TransientStorageConflictDetector 实例。
     */
    private TransientStorageConflictDetector() {
    }

    /**
     * 判断 MVCC 异常是否属于瞬时存储冲突。
     */
    static boolean isTransientConflict(MvccException exception) {
        if (!exception.retryable()) {
            return false;
        }
        if (exception.errorCode() == ErrorCode.LOCK_CONFLICT
                || exception.errorCode() == ErrorCode.LOCK_WAIT_TIMEOUT
                || exception.errorCode() == ErrorCode.WRITE_CONFLICT) {
            return true;
        }
        if (exception.errorCode() != ErrorCode.STORAGE_UNAVAILABLE) {
            return false;
        }
        Throwable cause = exception.getCause();
        while (cause != null) {
            if (cause instanceof SQLException sqlException && isTransientSqlConflict(sqlException)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * 判断 SQL 异常是否属于可重试冲突。
     */
    private static boolean isTransientSqlConflict(SQLException exception) {
        String sqlState = exception.getSQLState();
        int vendorCode = exception.getErrorCode();
        return "40001".equals(sqlState)
                || vendorCode == 1213
                || vendorCode == 1205;
    }
}

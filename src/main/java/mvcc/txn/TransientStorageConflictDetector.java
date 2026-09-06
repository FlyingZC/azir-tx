package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.sql.SQLException;

final class TransientStorageConflictDetector {
    private TransientStorageConflictDetector() {
    }

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

    private static boolean isTransientSqlConflict(SQLException exception) {
        String sqlState = exception.getSQLState();
        int vendorCode = exception.getErrorCode();
        return "40001".equals(sqlState)
                || vendorCode == 1213
                || vendorCode == 1205;
    }
}

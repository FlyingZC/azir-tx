package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.sql.SQLException;

/**
 * MySQL 存储异常映射器，将 SQL 异常转换为 MVCC 领域错误。
 */
final class MysqlStorageExceptionMapper {
    /**
     * 构造 MysqlStorageExceptionMapper 实例。
     */
    private MysqlStorageExceptionMapper() {
    }

    /**
     * 将 SQL 异常转换为 MVCC 异常。
     */
    static MvccException toMvccException(String shardId, Long txId, String message, SQLException exception) {
        String detailedMessage = shardId
                + ": " + message
                + " [sqlState=" + exception.getSQLState()
                + ", vendorCode=" + exception.getErrorCode()
                + "]";
        return new MvccException(errorCode(exception), detailedMessage, true, txId, exception);
    }

    /**
     * 根据 MySQL SQLState 和错误码选择 MVCC 错误码。
     */
    private static ErrorCode errorCode(SQLException exception) {
        if (exception.getErrorCode() == 1205 || "41000".equals(exception.getSQLState())) {
            return ErrorCode.LOCK_WAIT_TIMEOUT;
        }
        if (exception.getErrorCode() == 1213 || "40001".equals(exception.getSQLState())) {
            return ErrorCode.LOCK_CONFLICT;
        }
        return ErrorCode.STORAGE_UNAVAILABLE;
    }
}

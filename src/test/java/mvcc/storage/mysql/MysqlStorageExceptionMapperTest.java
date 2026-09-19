package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MySQL 存储异常映射测试，验证瞬时 SQL 冲突会转换为 MVCC 冲突语义。
 */
class MysqlStorageExceptionMapperTest {
    /**
     * 验证 MySQL deadlock 会映射为锁冲突。
     */
    @Test
    void shouldMapDeadlockToLockConflict() {
        SQLException deadlock = new SQLException("Deadlock found when trying to get lock", "40001", 1213);

        MvccException actual = MysqlStorageExceptionMapper.toMvccException("shard-1", 10L, "prewrite failed", deadlock);

        assertEquals(ErrorCode.LOCK_CONFLICT, actual.errorCode());
        assertTrue(actual.retryable());
        assertEquals(10L, actual.txId());
    }

    /**
     * 验证 MySQL lock wait timeout 会映射为锁等待超时。
     */
    @Test
    void shouldMapLockWaitTimeoutToLockWaitTimeout() {
        SQLException timeout = new SQLException("Lock wait timeout exceeded", "41000", 1205);

        MvccException actual = MysqlStorageExceptionMapper.toMvccException("shard-1", 11L, "prewrite failed", timeout);

        assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, actual.errorCode());
        assertTrue(actual.retryable());
        assertEquals(11L, actual.txId());
    }

    /**
     * 验证普通 SQL 异常仍映射为存储不可用。
     */
    @Test
    void shouldMapRegularSqlExceptionToStorageUnavailable() {
        SQLException regular = new SQLException("table missing", "42S02", 1146);

        MvccException actual = MysqlStorageExceptionMapper.toMvccException("shard-1", 12L, "read failed", regular);

        assertEquals(ErrorCode.STORAGE_UNAVAILABLE, actual.errorCode());
        assertTrue(actual.retryable());
        assertEquals(12L, actual.txId());
    }
}

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

package com.flyingzc.azir.storage.mysql;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

/**
 * MySQL storage anomaly map test, verify SQL conflicts are converted to MVCC conflict syntax.
 */
class MysqlStorageExceptionMapperTest {

    /**
     * Verify MySQL deadlock will map lock conflict.
     */
    @Test
    void shouldMapDeadlockToLockConflict() {
        SQLException deadlock = new SQLException("Deadlock found when trying to get lock", "40001", 1213);

        MvccException actual = MysqlStorageExceptionMapper.toMvccException("shard-1", 10L, "prewrite failed", deadlock);

        Assertions.assertEquals(ErrorCode.LOCK_CONFLICT, actual.errorCode());
        assertTrue(actual.retryable());
        Assertions.assertEquals(10L, actual.txId());
    }

    /**
     * Verify MySQL lock wait timeout will map to lock waiting timeout.
     */
    @Test
    void shouldMapLockWaitTimeoutToLockWaitTimeout() {
        SQLException timeout = new SQLException("Lock wait timeout exceeded", "41000", 1205);

        MvccException actual = MysqlStorageExceptionMapper.toMvccException("shard-1", 11L, "prewrite failed", timeout);

        Assertions.assertEquals(ErrorCode.LOCK_WAIT_TIMEOUT, actual.errorCode());
        assertTrue(actual.retryable());
        Assertions.assertEquals(11L, actual.txId());
    }

    /**
     * Verify Normal SQL anomaly still map as unusable for storage.
     */
    @Test
    void shouldMapRegularSqlExceptionToStorageUnavailable() {
        SQLException regular = new SQLException("table missing", "42S02", 1146);

        MvccException actual = MysqlStorageExceptionMapper.toMvccException("shard-1", 12L, "read failed", regular);

        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, actual.errorCode());
        assertTrue(actual.retryable());
        Assertions.assertEquals(12L, actual.txId());
    }
}

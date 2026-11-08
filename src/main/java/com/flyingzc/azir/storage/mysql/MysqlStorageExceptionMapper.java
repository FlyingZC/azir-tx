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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.sql.SQLException;

/**
 * MySQL storage anomaly mapr, converts SQL anomalies to MVCC field error.
 */
final class MysqlStorageExceptionMapper {

    /**
     * Construct a MysqlStorageExceptionMapper instance.
     */
    private MysqlStorageExceptionMapper() {
    }

    /**
     * Converts SQL anomalies to MVCC anomalies.
     *
     * @param shardId logical shard identifier
     * @param txId transaction identifier
     * @param message message
     * @param exception exception
     * @return the operation result
     */
    static MvccException toMvccException(final String shardId, final Long txId, final String message, final SQLException exception) {
        String detailedMessage = shardId
                + ": " + message
                + " [sqlState=" + exception.getSQLState()
                + ", vendorCode=" + exception.getErrorCode()
                + "]";
        return new MvccException(errorCode(exception), detailedMessage, true, txId, exception);
    }

    /**
     * Select the MVCC error code by MySQL SQLstate and error number.
     *
     * @param exception exception
     * @return the operation result
     */
    private static ErrorCode errorCode(final SQLException exception) {
        if (exception.getErrorCode() == 1205 || "41000".equals(exception.getSQLState())) {
            return ErrorCode.LOCK_WAIT_TIMEOUT;
        }
        if (exception.getErrorCode() == 1213 || "40001".equals(exception.getSQLState())) {
            return ErrorCode.LOCK_CONFLICT;
        }
        return ErrorCode.STORAGE_UNAVAILABLE;
    }
}

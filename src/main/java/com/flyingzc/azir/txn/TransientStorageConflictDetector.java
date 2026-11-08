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

package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.sql.SQLException;

/**
 * Store conflict identifiers instantaneously to determine whether the anomaly is suitable for retry.
 */
final class TransientStorageConflictDetector {

    /**
     * Construct a TransientStorageConflictDetector instance.
     */
    private TransientStorageConflictDetector() {
    }

    /**
     * It's not like you're in a state of instant storage conflict.
     *
     * @param exception exception
     * @return the operation result
     */
    static boolean isTransientConflict(final MvccException exception) {
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
     * Determines whether the SQL anomaly is an retry conflict.
     *
     * @param exception exception
     * @return the operation result
     */
    private static boolean isTransientSqlConflict(final SQLException exception) {
        String sqlState = exception.getSQLState();
        int vendorCode = exception.getErrorCode();
        return "40001".equals(sqlState)
                || vendorCode == 1213
                || vendorCode == 1205;
    }
}

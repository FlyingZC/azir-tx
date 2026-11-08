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

import com.flyingzc.azir.common.IsolationLevel;

/**
 * SQL session interface, maintain visible transaction state and execute SQL; session cannot restart or execute any SQL after closing.
 */
public interface SqlSession extends AutoCloseable {

    /**
     * Turn on visible transaction for the specified level of isolation.
     *
     * @param isolationLevel global transaction isolation level
     */
    void begin(IsolationLevel isolationLevel);

    /**
     * Execute a SQL and return the result; the data statement failed to execute rollback -wide visible transaction and render it invalid, needing a visible ROLLBACK before using it.
     *
     * @param sql sql
     * @return the operation result
     */
    SqlExecutionResult execute(String sql);

    /**
     * Commit Current Visibility transaction.
     *
     * @return the operation result
     */
    SqlExecutionResult commit();

    /**
     * Rollback Current Visible transaction is also used to clear transaction invalid status due to failed statements.
     */
    void rollback();

    /**
     * Determines whether the current session has a visible transaction that can be continued; failure or closure returns false.
     *
     * @return the operation result
     */
    boolean active();

    /**
     * Idempotent Closes session and clears pending transaction; clean-up anomaly does not reopen session and incomplete clean-up is processed by transaction recovery. After closing, begin,
     * execute, commit, rollback back to false.
     */
    @Override
    void close();
}

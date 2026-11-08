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

import java.util.List;

/**
 * The SQL implementation plan is closed to interface and describes the type of scheme currently supported by a light SQL layer.
 */
public sealed interface SqlPlan {

    /**
     * Starts the transaction schedule.
     *
     * @param isolationLevel global transaction isolation level
     */
    record BeginTransactionPlan(IsolationLevel isolationLevel) implements SqlPlan {
    }

    /**
     * Commit transaction schedule.
     */
    record CommitTransactionPlan() implements SqlPlan {
    }

    /**
     * Rollback transaction schedule.
     */
    record RollbackTransactionPlan() implements SqlPlan {
    }

    /**
     * Main key point check schedule.
     *
     * @param table table
     * @param key business key
     */
    record SelectByKeyPlan(String table, String key) implements SqlPlan {
    }

    /**
     * Inserts the main key into the schedule.
     *
     * @param table table
     * @param key business key
     * @param value value to write
     */
    record InsertByKeyPlan(String table, String key, String value) implements SqlPlan {
    }

    /**
     * Primary key update plan.
     *
     * @param table table
     * @param key business key
     * @param value value to write
     */
    record UpdateByKeyPlan(String table, String key, String value) implements SqlPlan {
    }

    /**
     * The main key deletes the schedule.
     *
     * @param table table
     * @param key business key
     */
    record DeleteByKeyPlan(String table, String key) implements SqlPlan {
    }

    /**
     * Calcite Relationship Scheme, save validate statements and tables that you may visit.
     *
     * @param sql sql
     * @param statementType statement type
     * @param tableNames table names
     */
    record CalcitePlan(String sql, CalciteStatementType statementType, List<String> tableNames) implements SqlPlan {

        /**
         * Constructs the Calcite relationship plan for immutable.
         *
         * @param sql sql
         * @param statementType statement type
         * @param tableNames table names
         */
        public CalcitePlan {
            tableNames = List.copyOf(tableNames);
        }
    }
}

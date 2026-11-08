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

import com.flyingzc.azir.common.MvccException;

/**
 * Simple primary key SQL uses a lightweight schedule, and other valid SQLs are handed over to the full Calcite planner.
 */
public final class HybridSqlPlanner implements SqlPlanner {

    private final SimpleSqlPlanner simple = new SimpleSqlPlanner();

    private final CalciteSqlPlanner calcite = new CalciteSqlPlanner();

    /**
     * Only the simple planner expressly "does not support" statement is forwarded to Calcite without swallowing any other error.
     *
     * @param sql sql
     * @return the operation result
     */
    @Override
    public SqlPlan plan(final String sql) {
        try {
            return simple.plan(sql);
        } catch (final MvccException unsupported) {
            if (unsupported.getMessage() == null
                    || !unsupported.getMessage().startsWith("unsupported sql:")) {
                throw unsupported;
            }
            return calcite.plan(sql);
        }
    }
}

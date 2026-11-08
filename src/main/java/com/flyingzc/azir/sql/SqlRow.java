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

import java.util.Map;
import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * SQL query result rows, with a simple map of the listed to the string value.
 *
 * @param columns columns
 */
public record SqlRow(Map<String, String> columns) {

    /**
     * Construct immutable result rows.
     *
     * @param columns columns
     */
    public SqlRow {
        // Connection outside SQL and empty aggregations generate nult and cannot use Map.copyof, which rejects nurt.
        columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    /**
     * Reads the string values by listing.
     *
     * @param columnName column name
     * @return the operation result
     */
    public String get(final String columnName) {
        String exact = columns.get(columnName);
        if (exact != null || columns.containsKey(columnName)) {
            return exact;
        }
        return columns.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(columnName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}

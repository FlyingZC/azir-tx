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

import java.util.Locale;

/**
 * The codec between the SQL logical table primary key and the MVCC physical key ensures that different tables on the same shard are not serialized.
 */
public final class SqlStorageKeyCodec {

    private static final char TABLE_SEPARATOR = '\u0000';

    private static final char RANGE_END = '\u0001';

    /**
     * Encoding the logical table name and line main key as a physical key.
     *
     * @param table table
     * @param key business key
     * @return the operation result
     */
    public String encode(final String table, final String key) {
        return normalizedTable(table) + TABLE_SEPARATOR + key;
    }

    /**
     * Returns the inclusion starting point of the given table physics range.
     *
     * @param table table
     * @return the operation result
     */
    public String rangeStart(final String table) {
        return normalizedTable(table) + TABLE_SEPARATOR;
    }

    /**
     * Returns the exclusion endpoint of the range specified for table physics.
     *
     * @param table table
     * @return the operation result
     */
    public String rangeEnd(final String table) {
        return normalizedTable(table) + RANGE_END;
    }

    /**
     * Decodes the logical primary key from the physical key of the given table.
     *
     * @param table table
     * @param physicalKey physical key
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public String decode(final String table, final String physicalKey) {
        String prefix = rangeStart(table);
        if (!physicalKey.startsWith(prefix)) {
            throw new IllegalArgumentException("physical key does not belong to table " + table);
        }
        return physicalKey.substring(prefix.length());
    }

    /**
     * Regularizes case-sensitive SQL table names.
     *
     * @param table table
     * @return the operation result
     */
    private String normalizedTable(final String table) {
        return table.toLowerCase(Locale.ROOT);
    }
}

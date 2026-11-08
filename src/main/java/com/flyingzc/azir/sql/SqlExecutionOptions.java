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

/**
 * SQL break, input scan and materialization result budget, which does not represent the JVM stack cap or disk overwhelm capacity. Enter a byte fee for the physical key to be retrieved UTF-8
 * and the original value bytes, including lines for page pretracting, projection discarding and resweeping; the result is charged at column value UTF-8.
 *
 * @param scanPageSize scan page size
 * @param maxResultRows max result rows
 * @param maxResultBytes max result bytes
 * @param maxScannedRows max scanned rows
 * @param maxScannedBytes max scanned bytes
 */
public record SqlExecutionOptions(int scanPageSize, int maxResultRows, long maxResultBytes,
        long maxScannedRows, long maxScannedBytes) {

    /**
     * Validate page break size is within 1.4096 allowed for storage of interface, and input and result budget are positive.
     *
     * @param scanPageSize scan page size
     * @param maxResultRows max result rows
     * @param maxResultBytes max result bytes
     * @param maxScannedRows max scanned rows
     * @param maxScannedBytes max scanned bytes
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public SqlExecutionOptions {
        if (scanPageSize <= 0 || scanPageSize > 4096 || maxResultRows <= 0 || maxResultBytes <= 0
                || maxScannedRows <= 0 || maxScannedBytes <= 0) {
            throw new IllegalArgumentException("SQL page size must be in 1..4096 and all budgets must be positive");
        }
    }

    /**
     * Keeps the original three-parameter tectonic entry to supplement the default limit of 1 million input lines and 256 MiB input bytes per statement.
     *
     * @param scanPageSize scan page size
     * @param maxResultRows max result rows
     * @param maxResultBytes max result bytes
     */
    public SqlExecutionOptions(final int scanPageSize, final int maxResultRows, final long maxResultBytes) {
        this(scanPageSize, maxResultRows, maxResultBytes, 1_000_000L, 256L * 1024 * 1024);
    }

    /**
     * Returns the configuration of 256 lines per page, 100,000 result lines and 64 MiB results bytes, as well as default input limits.
     *
     * @return the operation result
     */
    public static SqlExecutionOptions defaults() {
        return new SqlExecutionOptions(256, 100_000, 64L * 1024 * 1024);
    }
}

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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Utf8KeyOrder;

/**
 * The input scanner, which is shared in a query, consumes budget together with all tables, shard, an encumbrance and re-cleaning. The bill only covers input lines and bytes that return to
 * the SQL layer without measuring internal access storage or stacking of objects in the Calcite middle.
 */
final class SqlScanBudget {

    private final long txId;

    private final SqlExecutionOptions options;

    private long remainingRows;

    private long remainingBytes;

    private MvccException exhausted;

    /**
     * Creates budget for a query, the caller must submit the same instance to all tables of the query.
     *
     * @param txId transaction identifier
     * @param options options
     */
    SqlScanBudget(final long txId, final SqlExecutionOptions options) {
        this.txId = txId;
        this.options = options;
        remainingRows = options.maxScannedRows();
        remainingBytes = options.maxScannedBytes();
    }

    /**
     * Pre-entry line decodes and projectors are billed, and the cumulative excess is avoided by using a residual amount.
     *
     * @param physicalKey physical key
     * @param value value to write
     */
    synchronized void chargeRow(final String physicalKey, final byte[] value) {
        ensureAvailable();
        if (remainingRows == 0) {
            exhausted = new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "SQL input scan row budget exceeded: " + options.maxScannedRows(), false, txId);
            throw exhausted;
        }
        long bytes = (long) Utf8KeyOrder.encode(physicalKey).length + (value == null ? 0 : value.length);
        if (bytes > remainingBytes) {
            exhausted = new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "SQL input scan byte budget exceeded: " + options.maxScannedBytes(), false, txId);
            throw exhausted;
        }
        remainingRows--;
        remainingBytes -= bytes;
    }

    /**
     * After exceeding the limit, all lifts continue to fail and reset cursor or create another count cannot be recovery budget.
     */
    synchronized void ensureAvailable() {
        if (exhausted != null) {
            throw exhausted;
        }
    }
}

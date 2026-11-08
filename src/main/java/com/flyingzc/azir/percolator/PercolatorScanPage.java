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

package com.flyingzc.azir.percolator;

import java.util.List;

/**
 * Fixed statement snapshot bounded scan page; no original key is visible and maintains location for cursor propulsion and scanning budget charges.
 *
 * @param entries entries
 * @param nextKey next key
 */
public record PercolatorScanPage(List<Entry> entries, String nextKey) {
    /**
     * Fixed page contents; empty cursor indicates that the candidate range has been scanned.
     *
     * @param entries entries
     * @param nextKey next key
     */
    public PercolatorScanPage {
        entries = List.copyOf(entries);
    }

    /**
     * An original candidate key; an empty value indicates that this snapshot is invisible or has been removed by tombstone, this transaction.
     *
     * @param key business key
     * @param value value to write
     */
    public record Entry(String key, byte[] value) {

        /**
         * Copy non-empty value and do not falsify commit timestamp which does not exist.
         *
         * @param key business key
         * @param value value to write
         */
        public Entry {
            java.util.Objects.requireNonNull(key, "key");
            value = value == null ? null : value.clone();
        }

        /**
         * Prohibits the caller to change the content of a returned page.
         *
         * @return the operation result
         */
        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }
}

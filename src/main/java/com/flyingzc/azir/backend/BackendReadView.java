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

package com.flyingzc.azir.backend;

import com.flyingzc.azir.storage.KeyRange;
import java.util.List;
import java.util.Optional;

/**
 * A fixed statement reads the boundary and reads, all shard s and page breaks repeat the same example.
 */
public interface BackendReadView extends AutoCloseable {

    /**
     * Returns the current statement to a fixed read border.
     *
     * @return the operation result
     */
    long readTs();

    /**
     * Reads the specified shard key, which contains this transaction write set overwrite.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @return the operation result
     */
    Optional<byte[]> get(String shard, String key);

    /**
     * Use the same border to read a page containing an invisible cursor position.
     *
     * @param shard logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    Page scanPage(String shard, KeyRange range, String afterKeyExclusive, int limit);

    /**
     * Release native read-only statement resources without committing the business write transaction.
     */
    @Override
    void close();

    /**
     * Original candidate line; empty value indicates this snapshot is invisible and can still be used to enter the budget billing.
     *
     * @param key business key
     * @param value value to write
     */
    record Row(String key, byte[] value) {

        /**
         * Prevents the caller from changing the content to return.
         *
         * @param key business key
         * @param value value to write
         */
        public Row {
            java.util.Objects.requireNonNull(key, "key");
            value = value == null ? null : value.clone();
        }

        /**
         * Returns a standalone value of replica.
         *
         * @return the operation result
         */
        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }

    /**
     * Bounded Original Key Page, nextKey is exclusive cursor, empty values indicate the end of this range.
     *
     * @param rows rows
     * @param nextKey next key
     */
    record Page(List<Row> rows, String nextKey) {

        /**
         * Fixes the list of candidates on a page.
         *
         * @param rows rows
         * @param nextKey next key
         */
        public Page {
            rows = List.copyOf(rows);
        }
    }
}

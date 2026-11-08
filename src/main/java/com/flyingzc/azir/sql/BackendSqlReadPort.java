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

import com.flyingzc.azir.backend.BackendReadView;
import com.flyingzc.azir.backend.BackendTransaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.KeyRange;
import java.util.Optional;

/**
 * Converts a public backend fixed statement view to the port required by Calcite.
 */
final class BackendSqlReadPort implements SqlTransactionPort {

    private final BackendTransaction transaction;

    private final BackendReadView view;

    /**
     * Use the current statement view, which is closed by SQL session.
     *
     * @param transaction transaction context
     * @param view view
     */
    BackendSqlReadPort(final BackendTransaction transaction, final BackendReadView view) {
        this.transaction = transaction;
        this.view = view;
    }

    /**
     * Only original StartTs are used for diagnostic purposes, without a backend explanation of version.
     *
     * @return the operation result
     */
    @Override
    public long diagnosticTransactionId() {
        return transaction.id().startTs();
    }

    /**
     * Reads a key at the same statement border.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @return the operation result
     */
    @Override
    public Optional<byte[]> get(final String shard, final String key) {
        return view.get(shard, key);
    }

    /**
     * Empty candidate input budget, cursor keep original key order.
     *
     * @param shard logical shard identifier
     * @param range range
     * @param after after
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public Page scanPage(final String shard, final KeyRange range, final String after, final int limit) {
        var page = view.scanPage(shard, range, after, limit);
        return new Page(page.rows().stream().map(row -> {
            byte[] value = row.value();
            return new KeyValue(row.key(), value, value == null);
        }).toList(), page.nextKey());
    }

    /**
     * For the current statement, commit is still subject to original kernel adjudication for concurrent conflicts.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void insert(final String shard, final String key, final byte[] value) {
        if (get(shard, key).isPresent()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "duplicate SQL key: " + key, false, diagnosticTransactionId());
        }
        put(shard, key, value);
    }

    /**
     * Buffer or perform updates of the same business transaction.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     */
    @Override
    public void put(final String shard, final String key, final byte[] value) {
        transaction.put(shard, key, value);
    }

    /**
     * Delete to original kernel expression tombstone.
     *
     * @param shard logical shard identifier
     * @param key business key
     */
    @Override
    public void delete(final String shard, final String key) {
        transaction.delete(shard, key);
    }
}

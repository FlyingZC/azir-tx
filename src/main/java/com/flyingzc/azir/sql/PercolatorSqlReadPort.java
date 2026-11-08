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
import com.flyingzc.azir.percolator.PercolatorTransaction;
import com.flyingzc.azir.storage.KeyRange;
import java.util.Optional;

/**
 * Calcite reads the same sentences between an independent Percolator kernel, page breaks and buffer write ports.
 */
final class PercolatorSqlReadPort implements SqlTransactionPort {

    private final PercolatorTransaction transaction;

    private final PercolatorTransaction.ReadView statement;

    /**
     * Each SQL assigns ReadView, JOIN re-cleaning and page breaks only once will not refresh RC timestamp.
     *
     * @param transaction transaction context
     */
    PercolatorSqlReadPort(final PercolatorTransaction transaction) {
        this.transaction = java.util.Objects.requireNonNull(transaction, "transaction");
        statement = transaction.statement();
    }

    /**
     * Use the original StartTs for diagnosis without changing the decision function of the Primary.
     *
     * @return the operation result
     */
    @Override
    public long diagnosticTransactionId() {
        return transaction.startTs();
    }

    /**
     * Checks to repeat words to fix boundaries and to overwrite the transaction buffer writing.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    @Override
    public Optional<byte[]> get(final String shardId, final String key) {
        return statement.get(shardId, key);
    }

    /**
     * Not visible guard and tombstone are still involved in scanning budget; cursor comes from the original key.
     *
     * @param shardId logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public Page scanPage(final String shardId, final KeyRange range, final String afterKeyExclusive, final int limit) {
        var page = statement.scanPage(shardId, range, afterKeyExclusive, limit);
        return new Page(page.entries().stream().map(entry -> {
            byte[] value = entry.value();
            return new KeyValue(entry.key(), value, value == null);
        }).toList(), page.nextKey());
    }

    /**
     * Inserts a statement to double the border, and commit is still covered by Percolator write conflict detection.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void insert(final String shardId, final String key, final byte[] value) {
        if (get(shardId, key).isPresent()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "duplicate SQL key: " + key, false, transaction.startTs());
        }
        put(shardId, key, value);
    }

    /**
     * Buffer SQL is not empty, and protocol prewrite was not made until the transaction commit stage.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    @Override
    public void put(final String shardId, final String key, final byte[] value) {
        transaction.put(shardId, key, value);
    }

    /**
     * Delete tombstone version at commit to keep old RR snapshot.
     *
     * @param shardId logical shard identifier
     * @param key business key
     */
    @Override
    public void delete(final String shardId, final String key) {
        transaction.delete(shardId, key);
    }
}

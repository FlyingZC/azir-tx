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

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.txn.StatementReadSnapshot;
import com.flyingzc.azir.txn.TransactionManager;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Calcite tables rely only on the reading and original page in the same statement snapshot, without explaining the backend commit time.
 */
interface SqlReadPort {

    /**
     * Returns the transaction identifier in a process used for error and budget records, which does not take global time to sort.
     *
     * @return the operation result
     */
    long diagnosticTransactionId();

    /**
     * Reads a physical business key inside the handle of the same sentence.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    Optional<byte[]> get(String shardId, String key);

    /**
     * The range of keys within the handle of the same sentence and exclusive cursor reads a page.
     *
     * @param shardId logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    Page scanPage(String shardId, KeyRange range, String afterKeyExclusive, int limit);

    /**
     * The SQL table retains tombstone bill identity; only undeleted business values must be empty.
     *
     * @param key business key
     * @param value value to write
     * @param deleted deleted
     */
    record KeyValue(String key, byte[] value, boolean deleted) {

        /**
         * Allows the backend to modify the current page while consuming Calcite.
         *
         * @param key business key
         * @param value value to write
         * @param deleted deleted
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public KeyValue {
            Objects.requireNonNull(key, "key");
            if (!deleted && value == null) {
                throw new IllegalArgumentException("visible SQL value cannot be null");
            }
            value = value == null ? null : value.clone();
        }

        /**
         * Caller obtains a separate byte array.
         *
         * @return the operation result
         */
        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }

    /**
     * The empty value page also maintains an exclusive cursor from the scanned original key.
     *
     * @param values values
     * @param nextKey next key
     */
    record Page(List<KeyValue> values, String nextKey) {

        /**
         * Fixed the order of business keys on the page.
         *
         * @param values values
         * @param nextKey next key
         */
        public Page {
            values = List.copyOf(values);
        }
    }
}

/**
 * SQL DML is written on the same transaction buffer or executes, and reads the cut at the beginning of the statement.
 */
interface SqlTransactionPort extends SqlReadPort {

    /**
     * The primary key INSERT must be written when the backend ensures that it does not exist.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    void insert(String shardId, String key, byte[] value);

    /**
     * Writing non-empty SQL value bytes by physical path of verify.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    void put(String shardId, String key, byte[] value);

    /**
     * Presss the physical path of verify to delete the key.
     *
     * @param shardId logical shard identifier
     * @param key business key
     */
    void delete(String shardId, String key);
}

/**
 * The current MySQL transaction manager follows the original statement-level timestamp and page breaks.
 */
final class MysqlSqlReadPort implements SqlTransactionPort {

    private final TransactionManager manager;

    private final Transaction transaction;

    private final StatementReadSnapshot snapshot;

    /**
     * Only constructed from a SQL implementer that has created a statement snapshot.
     *
     * @param manager manager
     * @param transaction transaction context
     * @param snapshot snapshot
     */
    MysqlSqlReadPort(final TransactionManager manager, final Transaction transaction,
                     final StatementReadSnapshot snapshot) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.transaction = Objects.requireNonNull(transaction, "transaction");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    }

    /**
     * MySQL transaction ID is for SQL diagnostics only.
     *
     * @return the operation result
     */
    @Override
    public long diagnosticTransactionId() {
        return transaction.txId();
    }

    /**
     * A little reading is still by transaction manager verify fixed statement snapshot.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    @Override
    public Optional<byte[]> get(final String shardId, final String key) {
        return manager.get(transaction, shardId, key, snapshot);
    }

    /**
     * Tombstone filtered by SQL non-empty line model, original page cursor remains unchanged.
     *
     * @param shardId logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    @Override
    public Page scanPage(final String shardId, final KeyRange range,
                         final String afterKeyExclusive, final int limit) {
        var source = manager.scanPage(transaction, shardId,
                range.startKeyInclusive(), range.endKeyExclusive(),
                snapshot, afterKeyExclusive, limit);
        return new Page(source.values().stream()
                .map(value -> new KeyValue(value.key(), value.value(), value.deleted()))
                .toList(), source.nextKey());
    }

    /**
     * The original MySQL buffered write set combination remains unchanged.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    @Override
    public void put(final String shardId, final String key, final byte[] value) {
        manager.put(transaction, shardId, key, value);
    }

    /**
     * The old MySQL backend follows the snapshot existential check and buffer writing.
     *
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public void insert(final String shardId, final String key, final byte[] value) {
        if (get(shardId, key).isPresent()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "duplicate SQL key: " + key, false, transaction.txId());
        }
        put(shardId, key, value);
    }

    /**
     * The original MySQL removes the tombstone semantic unchanged.
     *
     * @param shardId logical shard identifier
     * @param key business key
     */
    @Override
    public void delete(final String shardId, final String key) {
        manager.delete(transaction, shardId, key);
    }
}

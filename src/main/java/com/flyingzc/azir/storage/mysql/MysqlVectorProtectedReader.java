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

package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.ScanPage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Formal participant vector point reads to hold process-level GC protection handles before and after a query.
 */
final class MysqlVectorProtectedReader {

    private final Map<String, MysqlStorageAdapter> adapters;

    private final MysqlVectorReaderProtectionRuntime protection;

    /**
     * Route must point to the same name as real shard, and unknown route cannot guess physical data sources.
     *
     * @param adapters adapters
     * @param protection protection
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorProtectedReader(final Map<String, MysqlStorageAdapter> adapters,
                               final MysqlVectorReaderProtectionRuntime protection) {
        this.adapters = Map.copyOf(adapters);
        this.protection = Objects.requireNonNull(protection, "protection");
        if (this.adapters.isEmpty() || this.adapters.entrySet().stream()
                .anyMatch(entry -> !entry.getKey().equals(entry.getValue().shardId()))) {
            throw new IllegalArgumentException("vector protected reader has invalid shard routes");
        }
    }

    /**
     * RC Each statement first registers the current release cut and checks lease eligibility again before closing.
     *
     * @param requesterTxId requester tx id
     * @param shardId logical shard identifier
     * @param key business key
     * @param stripe stripe
     * @return the operation result
     */
    Optional<VersionedValue> getReadCommitted(final long requesterTxId, final String shardId,
                                              final String key, final int stripe) {
        MysqlStorageAdapter adapter = adapter(shardId);
        try (var handle = protection.beginRead()) {
            Optional<VersionedValue> value = adapter.getVector(
                    handle.snapshot(), requesterTxId, key, stripe);
            handle.requireActive();
            return value;
        }
    }

    /**
     * RC separates each page for the release of cut-off, with a page break and cursor keeping the original key order.
     *
     * @param requesterTxId requester tx id
     * @param shardId logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    ScanPage scanPageReadCommitted(final long requesterTxId, final String shardId, final KeyRange range,
                                   final String afterKeyExclusive, final int limit) {
        MysqlStorageAdapter adapter = adapter(shardId);
        try (var handle = protection.beginRead()) {
            ScanPage page = adapter.scanVectorPage(handle.snapshot(), requesterTxId,
                    range, afterKeyExclusive, limit);
            handle.requireActive();
            return page;
        }
    }

    /**
     * One RC-range statement fixes a cut, and no follow-up commit will be read across the internal page.
     *
     * @param requesterTxId requester tx id
     * @param shardId logical shard identifier
     * @param range range
     * @return the operation result
     */
    List<VersionedValue> scanReadCommitted(final long requesterTxId, final String shardId, final KeyRange range) {
        MysqlStorageAdapter adapter = adapter(shardId);
        try (var handle = protection.beginRead()) {
            List<VersionedValue> values = scanAll(adapter, handle, requesterTxId, range);
            handle.requireActive();
            return values;
        }
    }

    /**
     * Check lease per page when bounded pages are merged, and return of part of the scan results is prohibited for any lapse.
     *
     * @param adapter adapter
     * @param handle handle
     * @param requesterTxId requester tx id
     * @param range range
     * @return the operation result
     */
    private static List<VersionedValue> scanAll(final MysqlStorageAdapter adapter,
                                                final MysqlVectorReaderProtectionRuntime.ReadHandle handle,
                                                final long requesterTxId, final KeyRange range) {
        List<VersionedValue> values = new ArrayList<>();
        String after = null;
        do {
            ScanPage page = adapter.scanVectorPage(handle.snapshot(), requesterTxId,
                    range, after, 128);
            values.addAll(page.values());
            after = page.nextKey();
            handle.requireActive();
        } while (after != null);
        return List.copyOf(values);
    }

    /**
     * RR fixes a registered cut from begin until operational transaction releases the handle.
     *
     * @param requesterTxId requester tx id
     * @return the operation result
     */
    RepeatableRead beginRepeatableRead(final long requesterTxId) {
        return new RepeatableRead(requesterTxId, protection.beginRead());
    }

    /**
     * Unexplainable logic shard cannot be read in product version chain.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private MysqlStorageAdapter adapter(final String shardId) {
        MysqlStorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector protected read has no matching shard", false, null);
        }
        return adapter;
    }

    /**
     * Multiple RR transaction readings use the same vector, and old version retains to handle close.
     */
    final class RepeatableRead implements AutoCloseable {

        private final long requesterTxId;

        private final MysqlVectorReaderProtectionRuntime.ReadHandle handle;

        /**
         * Only those that are constructed by the successful registration of a protected operation are allowed.
         *
         * @param requesterTxId requester tx id
         * @param handle handle
         */
        private RepeatableRead(final long requesterTxId,
                               final MysqlVectorReaderProtectionRuntime.ReadHandle handle) {
            this.requesterTxId = requesterTxId;
            this.handle = handle;
        }

        /**
         * Single-key reading checkes the same process lease, and expired cannot return the old value.
         *
         * @param shardId logical shard identifier
         * @param key business key
         * @param stripe stripe
         * @return the operation result
         */
        Optional<VersionedValue> get(final String shardId, final String key, final int stripe) {
            MysqlStorageAdapter adapter = adapter(shardId);
            Optional<VersionedValue> value = adapter.getVector(
                    handle.snapshot(), requesterTxId, key, stripe);
            handle.requireActive();
            return value;
        }

        /**
         * The RR page always follows the global vector registered at begin.
         *
         * @param shardId logical shard identifier
         * @param range range
         * @param afterKeyExclusive after key exclusive
         * @param limit maximum number of items
         * @return the operation result
         */
        ScanPage scanPage(final String shardId, final KeyRange range, final String afterKeyExclusive, final int limit) {
            MysqlStorageAdapter adapter = adapter(shardId);
            ScanPage page = adapter.scanVectorPage(handle.snapshot(), requesterTxId,
                    range, afterKeyExclusive, limit);
            handle.requireActive();
            return page;
        }

        /**
         * RR The entire range statement is page-by-page in the same registered cut.
         *
         * @param shardId logical shard identifier
         * @param range range
         * @return the operation result
         */
        List<VersionedValue> scan(final String shardId, final KeyRange range) {
            MysqlStorageAdapter adapter = adapter(shardId);
            List<VersionedValue> values = scanAll(adapter, handle, requesterTxId, range);
            handle.requireActive();
            return values;
        }

        /**
         * Transaction returns the oldest read snapshot protection after the end of local.
         */
        @Override
        public void close() {
            handle.close();
        }
    }
}

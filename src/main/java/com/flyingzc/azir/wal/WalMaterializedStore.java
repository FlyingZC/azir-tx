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

package com.flyingzc.azir.wal;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;

/**
 * The new mode is exclusive to materialization contracts and does not repeat the inent or two-stage commit interface of the original model.
 */
public interface WalMaterializedStore {

    /**
     * Checks the one-line limit of a fixed single-volume limit at the back end before writing to WAL, and the default storage is not additional.
     *
     * @param mutation mutation
     */
    default void validateMutation(final WalMutation mutation) {
    }

    /**
     * Staples empty storage to a fixed log identity, different identity must be rejected.
     *
     * @param engineId engine id
     * @param shard logical shard identifier
     */
    void initialize(UUID engineId, String shard);

    /**
     * Copy mode requires independent replica ownership and does not allow multiple different nodes to share the same application prefix.
     *
     * @return the operation result
     */
    default boolean supportsReplicaOwnership() {
        return false;
    }

    /**
     * Bind node exclusive backend; storage of a common or another node must be refused.
     *
     * @param identity identity
     * @param shard logical shard identifier
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default void initializeReplica(final WalReplicaIdentity identity, final String shard) {
        throw new UnsupportedOperationException("replica ownership unsupported");
    }

    /**
     * Gets the durable materialization prefix that contains the executive summary.
     *
     * @return the operation result
     */
    WalCheckpoint checkpoint();

    /**
     * Atomic Apply continuous log entries and check points to allow commit to confirm a lost call.
     *
     * @param commits commits
     * @return the operation result
     */
    WalCheckpoint apply(List<WalCommit> commits);

    /**
     * Returns the latest version, tombstone under snapshot, which is covered by a unified upper layer.
     *
     * @param key business key
     * @param readSequence read sequence
     * @return the operation result
     */
    Optional<WalValue> get(String key, long readSequence);

    /**
     * Query durable storage latest version fence; default ensures custom backend compatibility to cover index queries that do not read payload.
     *
     * @param key business key
     * @return the operation result
     */
    default long latestSequence(final String key) {
        return get(key, Long.MAX_VALUE).map(WalValue::commitSequence).orElse(0L);
    }

    /**
     * Returns snapshot version in UTF-8 binary order between semi-open areas.
     *
     * @param fromInclusive from inclusive
     * @param toExclusive to exclusive
     * @param readSequence read sequence
     * @return the operation result
     */
    NavigableMap<String, WalValue> scan(String fromInclusive, String toExclusive, long readSequence);

    /**
     * Bounded clears history version and maintains the safe point anchor; retirement tombstone and all old version and replay cannot be revived when the anchor is tombstone.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param keyLimit key limit
     * @return the operation result
     */
    int gc(long safePoint, int keyLimit);

    /**
     * The backend must clearly provide scans, deletions and time budget, so that the hosting is allowed for automatic maintenance.
     *
     * @return the operation result
     */
    default boolean supportsBoundedMaintenance() {
        return false;
    }

    /**
     * A limited collection, time budget on a protected safe point, is collaborative, single I/ O and subject to driver timeout.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param keyLimit key limit
     * @param rowLimit row limit
     * @param budgetMillis budget millis
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default GcProgress gcBounded(final long safePoint, final int keyLimit, final int rowLimit, final long budgetMillis) {
        throw new UnsupportedOperationException("bounded backend maintenance is not supported");
    }

    /**
     * The hosting assembly validate durable and timeout before the creation of WAL, without a custom backend for verify cannot by default declare production to be serviceable.
     *
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default void validateProduction() {
        throw new UnsupportedOperationException("backend production validation is not supported");
    }

    /**
     * Custom backends must be visible for backup and retry recovery, which cannot be used by default instead of a normal scan with line upper limit.
     *
     * @return the operation result
     */
    default boolean supportsBackup() {
        return false;
    }

    /**
     * The backend must fix a reading view when it is open to export the same commit border during concurrent materialization and GC.
     *
     * @return the operation result
     */
    default boolean supportsPinnedSnapshot() {
        return false;
    }

    /**
     * The call takes place during the preparation phase of the track and suspend materialization, which, upon return, must have fixed the actual database reading view.
     *
     * @param boundary boundary
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default SnapshotView openSnapshot(final WalCheckpoint boundary) {
        throw new UnsupportedOperationException("pinned snapshot unsupported");
    }

    /**
     * Only the adaptor for visible validate recovery target replica identity can receive a transportable copy of snapshot.
     *
     * @return the operation result
     */
    default boolean supportsReplicaRestore() {
        return false;
    }

    /**
     * Fluid output fixes the latest version for each key, and output keys must be increased in the UTF-8 binary order.
     *
     * @param boundary boundary
     * @param sink sink
     * @throws UnsupportedOperationException if the operation cannot complete
     * @throws java.io.IOException if the operation cannot complete
     */
    default void exportSnapshot(final WalCheckpoint boundary, final BackupSink sink) throws java.io.IOException {
        throw new UnsupportedOperationException("backend backup is not supported");
    }

    /**
     * Returns whether or not you still need to import data from a whole new store starting with recovery or continuing the same backup; storage in recovery must be rejected when running
     * normally.
     *
     * @param spec spec
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default boolean beginRestore(final WalRestoreSpec spec) {
        throw new UnsupportedOperationException("backend restore is not supported");
    }

    /**
     * Idempotent Imports limited batch and all changes remain unserviceable in recovery.
     *
     * @param spec spec
     * @param rows rows
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default void restoreBatch(final WalRestoreSpec spec, final List<WalBackupRow> rows) {
        throw new UnsupportedOperationException("backend restore is not supported");
    }

    /**
     * After checking the complete physical data summary and number of rows, atomic releases a full confirmation prefix at the later end.
     *
     * @param spec spec
     * @param expectedRows expected rows
     * @param expectedDataHash expected data hash
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default void finishRestore(final WalRestoreSpec spec, final long expectedRows, final String expectedDataHash) {
        throw new UnsupportedOperationException("backend restore is not supported");
    }

    /**
     * Returns the actual check key and deletes the number of lines, which cannot be used to impersonate a scanning workload.
     *
     * @param inspectedKeys inspected keys
     * @param removedRows removed rows
     * @param wrapped wrapped
     */
    record GcProgress(int inspectedKeys, int removedRows, boolean wrapped) {

        /**
         * Rejects invalid collection statistics and allows the backend to be checked for budget while running.
         *
         * @param inspectedKeys inspected keys
         * @param removedRows removed rows
         * @param wrapped wrapped
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public GcProgress {
            if (inspectedKeys < 0 || removedRows < 0) {
                throw new IllegalArgumentException("invalid GC progress");
            }
        }
    }

    /**
     * A backup exclusive read-only view returns its own connection after termination or failure, without prejudice to sharing WAL.
     */
    interface SnapshotView extends AutoCloseable {

        /**
         * Only output of boundary data when it is open, without re-establishing a read view that may have been advanced at this time.
         *
         * @param sink sink
         * @throws java.io.IOException if the operation cannot complete
         */
        void export(BackupSink sink) throws java.io.IOException;

        /**
         * Closes this view with its own read-only transaction resource.
         *
         * @throws java.io.IOException if the operation cannot complete
         */
        @Override
        void close() throws java.io.IOException;
    }

    /**
     * Open storage of the flow-line consumer for interface extension without exposing the internal lock and version chain while running.
     */
    @FunctionalInterface
    interface BackupSink {

        /**
         * Process an updated version and write that the failure must be disseminated to block back-up posting.
         *
         * @param row row
         * @throws java.io.IOException if the operation cannot complete
         */
        void accept(WalBackupRow row) throws java.io.IOException;
    }
}

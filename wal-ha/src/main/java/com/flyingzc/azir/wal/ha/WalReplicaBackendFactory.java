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

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Press replica to allocate an independent backend with stability generation, restart must return to the same space, and new generation cannot reuse old generation database.
 */
@FunctionalInterface
public interface WalReplicaBackendFactory {

    /**
     * Returns an uninitiated adapter and its own resources; distribution can be idempotent retry without emptying existing space.
     *
     * @param identity identity
     * @param generation generation
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    Lease open(WalReplicaIdentity identity, UUID generation) throws IOException;

    /**
     * The collection backend is only possible if the non-restoration and idempotent delete protocol adaptor is visible.
     *
     * @return the operation result
     */
    default boolean supportsRetirement() {
        return false;
    }

    /**
     * Cursor is a adapter, with only limited single-wheel processing shard; permission may be imported only by state manager holding node locks.
     *
     * @param permit permit
     * @param cursor cursor
     * @param shardLimit shard limit
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    default RetirementProgress retire(final WalGenerationRetirement permit, final String cursor, final int shardLimit) throws IOException {
        throw new IOException("backend retirement not supported");
    }

    /**
     * The pure local storage also uses the same generation protocol and does not disguise as MySQL Cold replica.
     *
     * @return the operation result
     */
    static WalReplicaBackendFactory localOnly() {
        return new WalReplicaBackendFactory() {

            /**
             * Local mode does not have external storage space.
             *
             * @param identity identity
             * @param generation generation
             * @return the operation result
             */
            @Override
            public Lease open(final WalReplicaIdentity identity, final UUID generation) {
                return new Lease(Map.of(), () -> {
                });
            }

            /**
             * The local directory is secure by node collection.
             *
             * @return the operation result
             */
            @Override
            public boolean supportsRetirement() {
                return true;
            }

            /**
             * The back-end phase does not need to be deleted, but verify permission is still required.
             *
             * @param permit permit
             * @param cursor cursor
             * @param limit maximum number of items
             * @return the operation result
             * @throws IOException if the operation cannot complete
             */
            @Override
            public RetirementProgress retire(final WalGenerationRetirement permit, final String cursor, final int limit) throws IOException {
                permit.verify();
                return new RetirementProgress("local", true, 0, 0);
            }
        };
    }

    /**
     * Backend retirement progress cannot be disguised as transaction data or level released.
     *
     * @param cursor cursor
     * @param complete complete
     * @param inspected inspected
     * @param dropped dropped
     */
    record RetirementProgress(String cursor, boolean complete, int inspected, int dropped) {

        /**
         * Rejects unbounded cursor and error count.
         *
         * @param cursor cursor
         * @param complete complete
         * @param inspected inspected
         * @param dropped dropped
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public RetirementProgress {
            if (cursor == null || cursor.length() > 512 || inspected < 0 || dropped < 0 || dropped > inspected) {
                throw new IllegalArgumentException("invalid backend retirement progress");
            }
        }
    }

    /**
     * The end of the generation has its own life cycle, and nodal closures only release this lease resource.
     */
    final class Lease implements AutoCloseable {

        private final Map<String, WalMaterializedStore> stores;

        private final Release release;

        private boolean closed;

        /**
         * Connection pool ownership is provided explicitly by the plant, and the caller share connection pool cannot be disabled by default.
         *
         * @param stores stores
         * @param release release
         */
        public Lease(final Map<String, WalMaterializedStore> stores, final Release release) {
            this.stores = Map.copyOf(stores);
            this.release = java.util.Objects.requireNonNull(release);
        }

        /**
         * Routes may not be replaced by the caller in the middle of installation.
         *
         * @return the operation result
         */
        public Map<String, WalMaterializedStore> stores() {
            return stores;
        }

        /**
         * Mark off after success; failed to retain retry capability, concurrent closure is subject to actual release results.
         *
         * @throws IOException if the operation cannot complete
         */
        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            release.close();
            closed = true;
        }
    }

    /**
     * The plant only releases its own resources and does not allow for the removal of random business data through closed callbacks.
     */
    @FunctionalInterface
    interface Release {

        /**
         * Bounded Closes the current generation ' s own resources; when partial release fails, retry continues to clean up the remaining resources.
         *
         * @throws IOException if the operation cannot complete
         */
        void close() throws IOException;
    }
}

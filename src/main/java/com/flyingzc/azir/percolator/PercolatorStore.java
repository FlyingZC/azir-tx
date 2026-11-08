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

import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.VersionedValue;
import java.util.List;
import java.util.Optional;

/**
 * Defines shard operations for Percolator prewrites, primary decisions, version reads and idempotent recovery.
 */
public interface PercolatorStore {

    /**
     * Returns the logic shard of which the storage is responsible.
     *
     * @return the operation result
     */
    String shardId();

    /**
     * Initializes the pc_* protocol sheet that is isolated from the original model.
     */
    void initialize();

    /**
     * Atomic Detects conflicts and writes to the same shard batch of data and lock. Failed to leave half of the batch prewrite.
     *
     * @param startTs transaction start timestamp
     * @param primary original Percolator primary identity
     * @param ttlMillis ttl millis
     * @param mutations transaction write set
     */
    void prewrite(long startTs, PercolatorKey primary, long ttlMillis, List<Mutation> mutations);

    /**
     * Atomic Read locks and committees do not exceed the nearest version of the read border.
     *
     * @param key business key
     * @param readTs snapshot read timestamp
     * @return the operation result
     */
    Snapshot snapshot(String key, long readTs);

    /**
     * Raise the permanent guard key by UTF-8 order, return a maximum number of limitts without pre-filtering lock or tombstone.
     *
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    List<String> keys(KeyRange range, String afterKeyExclusive, int limit);

    /**
     * Serialize commit, expired clean-up, active rollback and late prewrite fence under the primary key.
     *
     * @param key business key
     * @param startTs transaction start timestamp
     * @param action action
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    Decision primary(PercolatorKey key, long startTs, Action action, long commitTs);

    /**
     * Press the confirmed Primary terminal state idempotent completed shard; caller must first get authority decision.
     *
     * @param startTs transaction start timestamp
     * @param primary original Percolator primary identity
     * @param decision decision
     * @param keys keys
     */
    void finish(long startTs, PercolatorKey primary, Decision decision, List<String> keys);

    /**
     * Bounded has found a residual lock that allows the re-establishment to continue with recovery after running.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    List<Lock> pending(int limit);

    /**
     * Redundancy payload before bounded collection safe point; default extended storage does not enable physical clean-up.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param limit maximum number of items
     * @return the operation result
     */
    default int collect(final long safePoint, final int limit) {
        return 0;
    }

    /**
     * The total number of remaining locks is returned and the storage of unachieved diagnosis is expressly refused.
     *
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default long pendingCount() {
        throw new UnsupportedOperationException("pending count unavailable");
    }

    /**
     * Still pending, commit and rollback durable states.
     */
    enum State {
        PENDING, COMMITTED, ROLLED_BACK, RETIRED
    }

    /**
     * Query, commit, Active rollback and only clear four pprimary atomic operations with expired locks.
     */
    enum Action {
        INSPECT, COMMIT, ROLLBACK, EXPIRE
    }

    /**
     * Durable decision for primary; only commit states carry non-zero commit timestamp.
     *
     * @param state state
     * @param commitTs durable commit timestamp
     */
    record Decision(State state, long commitTs) {

        /**
         * Reject commit decision without valid timestamp.
         *
         * @param state state
         * @param commitTs durable commit timestamp
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Decision {
            java.util.Objects.requireNonNull(state, "state");
            if ((state == State.COMMITTED) != (commitTs > 0)) {
                throw new IllegalArgumentException("invalid decision timestamp");
            }
        }
    }

    /**
     * Single-key durable lock, recovery must return to the original startTs for full Primary address.
     *
     * @param key business key
     * @param startTs transaction start timestamp
     * @param primary original Percolator primary identity
     */
    record Lock(PercolatorKey key, long startTs, PercolatorKey primary) {
    }

    /**
     * The lock and value in the same local -conformity reading view prevents a subreading cross- commit.
     *
     * @param lock lock
     * @param value value to write
     */
    record Snapshot(Optional<Lock> lock, Optional<VersionedValue> value) {
    }
}

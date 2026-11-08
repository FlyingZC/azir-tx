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

import com.flyingzc.azir.common.IsolationLevel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Transaction handles, which are only protected by the run-time lock, when run by a single new mode.
 */
public final class WalTransaction {

    final Object owner;

    final UUID id = UUID.randomUUID();

    final long start;

    final long deadlineNanos;

    final IsolationLevel isolation;

    final Map<WalMvccRuntime.Key, WalMutation> writes = new LinkedHashMap<>();

    final CompletableFuture<WalReceipt> completion = new CompletableFuture<>();

    State state = State.ACTIVE;

    long writeBytes;

    long epoch;

    /**
     * Fixed the duration of transaction ownership, snapshot and non-renewable periods.
     *
     * @param owner owner
     * @param start start
     * @param deadlineNanos deadline nanos
     * @param isolation global transaction isolation level
     */
    WalTransaction(final Object owner, final long start, final long deadlineNanos, final IsolationLevel isolation) {
        this.owner = owner;
        this.start = start;
        this.deadlineNanos = deadlineNanos;
        this.isolation = isolation;
    }

    /**
     * Returns the transaction ID required for a cross-restart query commit result.
     *
     * @return the operation result
     */
    public UUID id() {
        return id;
    }

    /**
     * Returns a fixed starting snapshot border with write conflict.
     *
     * @return the operation result
     */
    public long startSequence() {
        return start;
    }

    /**
     * The new mode transaction life cycle, the unknown result can no longer go business rollback.
     */
    enum State {
        ACTIVE, QUEUED, COMMITTED, ABORTED, UNKNOWN
    }
}

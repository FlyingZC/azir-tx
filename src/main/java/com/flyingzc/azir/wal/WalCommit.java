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
import java.util.Objects;
import java.util.UUID;

/**
 * Full transaction log entry, all shard changes to the same commit serial number.
 *
 * @param transactionId transaction id
 * @param sequence sequence
 * @param startSequence start sequence
 * @param mutations transaction write set
 */
public record WalCommit(UUID transactionId, long sequence, long startSequence, List<WalMutation> mutations) {

    /**
     * Fixed write set and validate commit order.
     *
     * @param transactionId transaction id
     * @param sequence sequence
     * @param startSequence start sequence
     * @param mutations transaction write set
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalCommit {
        Objects.requireNonNull(transactionId, "transactionId");
        mutations = List.copyOf(mutations);
        if (sequence < 1 || startSequence < 0 || startSequence >= sequence || mutations.isEmpty()) {
            throw new IllegalArgumentException("invalid commit record");
        }
        var keys = new java.util.HashSet<WalMvccRuntime.Key>();
        if (mutations.size() > 100_000 || mutations.stream().anyMatch(mutation -> !keys.add(new WalMvccRuntime.Key(mutation.shard(), mutation.key())))) {
            throw new IllegalArgumentException("duplicate or excessive mutation keys");
        }
    }

    /**
     * Returns the stable content summary for cross-checking database points with replay.
     *
     * @return the operation result
     */
    public String digest() {
        return WalCodec.digest(WalCodec.encode(this));
    }
}

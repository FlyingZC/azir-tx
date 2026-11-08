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

package com.flyingzc.azir.txn;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Transaction classification results, save commit protocol type and participate in shard snapshot.
 */
public final class TransactionClassification {

    private final TransactionProtocolType protocolType;

    private final Set<String> participants;

    /**
     * Constructs the transaction classification result.
     *
     * @param protocolType protocol type
     * @param participants participants
     */
    public TransactionClassification(final TransactionProtocolType protocolType, final Set<String> participants) {
        this.protocolType = Objects.requireNonNull(protocolType, "protocolType");
        this.participants = new LinkedHashSet<>(Objects.requireNonNull(participants, "participants"));
    }

    /**
     * Returns the transaction commit protocol type.
     *
     * @return the operation result
     */
    public TransactionProtocolType protocolType() {
        return protocolType;
    }

    /**
     * Returns to participate in the shard collection.
     *
     * @return the operation result
     */
    public Set<String> participants() {
        return Collections.unmodifiableSet(participants);
    }
}

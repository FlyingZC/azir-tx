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

package com.flyingzc.azir.protocol;

import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.VersionedValue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Participant Stores the front and defines the reading, writing and cleaning capabilities required for protocol layers to access shard storage.
 */
public interface ParticipantStore {

    /**
     * Returns the participant -owned shard number.
     *
     * @return  Shard Number.
     */
    String shardId();

    /**
     * Read the visible version of the specified key by reading context.
     *
     * @param context Read Context.
     * @param key Business Keys.
     * @return Visible version.
     */
    Optional<VersionedValue> get(ReadContext context, String key);

    /**
     * Scan the visible version of the specified key range by reading context.
     *
     * @param context Read Context.
     * @param keyRange Key Range.
     * @return Visible version list.
     */
    List<VersionedValue> scan(ReadContext context, KeyRange keyRange);

    /**
     * Write transaction intent and detect write conflict.
     *
     * @param txId  Transaction identifier.
     * @param startTs  Start timestamp.
     * @param mutations  Write set.
     */
    void prewrite(long txId, long startTs, List<Mutation> mutations);

    /**
     * Push transaction into the preparatory phase.
     *
     * @param txId  Transaction identifier.
     */
    void prepare(long txId);

    /**
     * Finalize transaction intents as committed versions.
     *
     * @param txId  Transaction identifier.
     * @param commitTs  Commit timestamp.
     */
    void commit(long txId, long commitTs);

    /**
     * Based on the known write set commit transaction intent and can choose to clear intent.
     *
     * @param txId  Transaction identifier.
     * @param commitTs  Commit timestamp.
     * @param mutations  Write set.
     * @param cleanupIntents Whether to clear intent.
     */
    void commit(long txId, long commitTs, List<Mutation> mutations, boolean cleanupIntents);

    /**
     * Writes directly to commit version, skips the visible preparation phase.
     *
     * @param txId  Transaction identifier.
     * @param startTs  Start timestamp.
     * @param commitTs  Commit timestamp.
     * @param mutations  Write set.
     */
    void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations);

    /**
     * Based on known write set clean-up of transaction residual intent.
     *
     * @param txId  Transaction identifier.
     * @param mutations  Write set.
     */
    void cleanupIntents(long txId, List<Mutation> mutations);

    /**
     * Roll back the transaction and remove its intents.
     *
     * @param txId  Transaction identifier.
     */
    void abort(long txId);

    /**
     * Find transaction identifiers with pending intents.
     *
     * @param limit Maximum number of queries.
     * @return Pending transaction identifier setup.
     */
    Set<Long> findPendingTransactionIds(int limit);

    /**
     * Collect obsolete versions before the safe point.
     *
     * @param safePoint  Safe point.
     * @param limit Maximum number of cleanups.
     * @return Number of cleanups.
     */
    int gc(long safePoint, int limit);
}

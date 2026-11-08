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

import com.flyingzc.azir.api.Transaction;

import java.util.Objects;
import java.util.Set;

/**
 * Transaction classifier, select the commit protocol according to the participating shard and transaction options.
 */
public final class TransactionClassifier {

    /**
     * Categorize transaction by phase commit requirements.
     *
     * @param tx transaction context
     * @param options options
     * @return the operation result
     */
    public TransactionClassification classifyForCommit(final Transaction tx, final TransactionOptions options) {
        Objects.requireNonNull(tx, "tx");
        Objects.requireNonNull(options, "options");
        Set<String> participants = tx.participants();
        if (participants.isEmpty()) {
            return new TransactionClassification(TransactionProtocolType.READ_ONLY_SNAPSHOT, participants);
        }
        if (participants.size() == 1) {
            return new TransactionClassification(TransactionProtocolType.SINGLE_SHARD_1PC, participants);
        }
        if (options.crossShardCommitPolicy().asyncCommit()) {
            return new TransactionClassification(TransactionProtocolType.CROSS_SHARD_ASYNC_COMMIT, participants);
        }
        return new TransactionClassification(TransactionProtocolType.CROSS_SHARD_SYNC_2PC, participants);
    }
}

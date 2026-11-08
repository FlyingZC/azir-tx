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

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.storage.Mutation;
import org.junit.jupiter.api.Test;

/**
 * Transaction Catalogue Tests, verify different transaction forms select the expected protocol path.
 */
class TransactionClassifierTest {

    private final TransactionClassifier classifier = new TransactionClassifier();

    /**
     * Verify Uninvolved shard transaction will be classified as an read-only snapshot path.
     */
    @Test
    void shouldClassifyReadOnlyTransaction() {
        Transaction tx = new Transaction(1L, 1L, IsolationLevel.REPEATABLE_READ);

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.defaults());

        Assertions.assertEquals(TransactionProtocolType.READ_ONLY_SNAPSHOT, actual.protocolType());
        assertTrue(actual.participants().isEmpty());
    }

    /**
     * Verify single shard writing transaction will be classified as a single shard 1PC path.
     */
    @Test
    void shouldClassifySingleShardTransaction() {
        Transaction tx = new Transaction(2L, 2L, IsolationLevel.REPEATABLE_READ);
        tx.bufferMutation("shard-1", Mutation.put("account:1", new byte[]{1}));

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.mysqlLikeDefaults());

        Assertions.assertEquals(TransactionProtocolType.SINGLE_SHARD_1PC, actual.protocolType());
        Assertions.assertEquals(1, actual.participants().size());
        assertTrue(actual.participants().contains("shard-1"));
    }

    /**
     * Verify The cross-shard transaction that allows async committee will be classified as a walk commit path.
     */
    @Test
    void shouldClassifyCrossShardAsyncTransaction() {
        Transaction tx = new Transaction(3L, 3L, IsolationLevel.REPEATABLE_READ);
        tx.bufferMutation("shard-1", Mutation.put("account:1", new byte[]{1}));
        tx.bufferMutation("shard-2", Mutation.put("account:2", new byte[]{2}));

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.mysqlLikeDefaults());

        Assertions.assertEquals(TransactionProtocolType.CROSS_SHARD_ASYNC_COMMIT, actual.protocolType());
        Assertions.assertEquals(2, actual.participants().size());
    }

    /**
     * Verify cross-shard transaction that does not enable async committee will be classified as a sync 2PC path.
     */
    @Test
    void shouldClassifyCrossShardSynchronousTransaction() {
        Transaction tx = new Transaction(4L, 4L, IsolationLevel.REPEATABLE_READ);
        tx.addParticipant("shard-1");
        tx.addParticipant("shard-2");

        TransactionClassification actual = classifier.classifyForCommit(tx, TransactionOptions.defaults());

        Assertions.assertEquals(TransactionProtocolType.CROSS_SHARD_SYNC_2PC, actual.protocolType());
        Assertions.assertEquals(2, actual.participants().size());
    }
}

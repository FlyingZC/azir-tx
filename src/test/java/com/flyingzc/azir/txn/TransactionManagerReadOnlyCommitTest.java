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
import com.flyingzc.azir.api.TransactionCommitResult;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import com.flyingzc.azir.tso.CountingTimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Transaction Manager read-only commit process testing.
 */
class TransactionManagerReadOnlyCommitTest {

    /**
     * Verify lazy metadata 's read-only transaction commit is not allocated extra commit timestamp.
     */
    @Test
    void lazyReadOnlyCommitShouldReuseStartTimestampWithoutMetadataWrite() {
        CountingTimestampOracle tso = new CountingTimestampOracle(new AtomicTimestampOracle(100L));
        InMemoryTransactionMetadataStore metadataStore = new InMemoryTransactionMetadataStore();
        TransactionOptions options = new TransactionOptions(
                LockWaitPolicy.failFast(),
                TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(),
                true,
                true,
                false);
        TransactionManager manager = new TransactionManager(tso, metadataStore, options, Map.of());

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        TransactionCommitResult result = manager.commit(tx);

        Assertions.assertEquals(tx.startTs(), result.commitTs());
        Assertions.assertEquals(1L, tso.invocationCount());
        assertTrue(metadataStore.get(tx.txId()).isEmpty());
    }

    /**
     * Verify read-only transaction for non-lazy metadata still allocates commit timestamp and updates metadata.
     */
    @Test
    void persistedReadOnlyCommitShouldAllocateCommitTimestamp() {
        CountingTimestampOracle tso = new CountingTimestampOracle(new AtomicTimestampOracle(100L));
        InMemoryTransactionMetadataStore metadataStore = new InMemoryTransactionMetadataStore();
        TransactionManager manager = new TransactionManager(tso, metadataStore, TransactionOptions.failFastDefaults(), Map.of());

        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        TransactionCommitResult result = manager.commit(tx);

        Assertions.assertEquals(2L, tso.invocationCount());
        assertTrue(result.commitTs() > tx.startTs());
        assertTrue(metadataStore.get(tx.txId()).isPresent());
    }
}

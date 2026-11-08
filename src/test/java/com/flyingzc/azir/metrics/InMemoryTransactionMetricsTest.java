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

package com.flyingzc.azir.metrics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Memory transaction Indicator Aggregation Test.
 */
class InMemoryTransactionMetricsTest {

    /**
     * Verify The success phase will be cumulatively called, successful and time-consuming.
     */
    @Test
    void recordSuccessShouldAccumulateStageStats() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        metrics.recordSuccess(TransactionStage.COMMIT_TSO, 100L);
        metrics.recordSuccess(TransactionStage.COMMIT_TSO, 300L);

        TransactionStageStats stats = metrics.snapshot(TransactionStage.COMMIT_TSO);
        Assertions.assertEquals(2L, stats.invocationCount());
        Assertions.assertEquals(2L, stats.successCount());
        Assertions.assertEquals(0L, stats.failureCount());
        Assertions.assertEquals(400L, stats.totalNanos());
        Assertions.assertEquals(200L, stats.averageNanos());
    }

    /**
     * The verify failure phase adds to the number of failures and time-consuming.
     */
    @Test
    void recordFailureShouldAccumulateStageStats() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        metrics.recordFailure(TransactionStage.BUFFERED_PREWRITE, 500L);

        TransactionStageStats stats = metrics.snapshot(TransactionStage.BUFFERED_PREWRITE);
        Assertions.assertEquals(1L, stats.invocationCount());
        Assertions.assertEquals(0L, stats.successCount());
        Assertions.assertEquals(1L, stats.failureCount());
        Assertions.assertEquals(500L, stats.totalNanos());
    }

    /**
     * Verify Unrecorded Stage returns empty snapshot.
     */
    @Test
    void unrecordedStageShouldReturnZeroSnapshot() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        TransactionStageStats stats = metrics.snapshot(TransactionStage.ASYNC_PARTICIPANT_COMMIT);

        Assertions.assertEquals(0L, stats.invocationCount());
        Assertions.assertEquals(0L, stats.successCount());
        Assertions.assertEquals(0L, stats.failureCount());
        Assertions.assertEquals(0L, stats.totalNanos());
        Assertions.assertEquals(0L, stats.averageNanos());
    }
}

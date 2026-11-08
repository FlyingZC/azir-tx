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

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Transaction indicator formatting test.
 */
class TransactionMetricsFormatterTest {

    /**
     * Verify only output the recorded transaction phase.
     */
    @Test
    void formatNonEmptyShouldSkipEmptyStages() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();

        List<String> lines = TransactionMetricsFormatter.formatNonEmpty(metrics);

        assertTrue(lines.isEmpty());
    }

    /**
     * The verify stage indicator will be formatted into a stable benchmark test output line.
     */
    @Test
    void formatNonEmptyShouldRenderStableStageLine() {
        InMemoryTransactionMetrics metrics = new InMemoryTransactionMetrics();
        metrics.recordSuccess(TransactionStage.COMMIT_TSO, 1_500_000L);
        metrics.recordFailure(TransactionStage.COMMIT_TSO, 500_000L);

        List<String> lines = TransactionMetricsFormatter.formatNonEmpty(metrics);

        Assertions.assertEquals(1, lines.size());
        Assertions.assertEquals(
                "TX_STAGE stage=COMMIT_TSO invocations=2 success=1 failure=1 total_ms=2.00 avg_us=1000.00",
                lines.getFirst());
    }
}

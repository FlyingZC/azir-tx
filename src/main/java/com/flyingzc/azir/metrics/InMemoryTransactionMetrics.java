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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * A linearly secure memory transaction indicator polymer.
 */
public final class InMemoryTransactionMetrics implements TransactionMetrics {

    private final Map<TransactionStage, MutableStageStats> statsByStage = new EnumMap<>(TransactionStage.class);

    /**
     * Constructs the memory transaction indicator polymer.
     */
    public InMemoryTransactionMetrics() {
        for (TransactionStage stage : TransactionStage.values()) {
            statsByStage.put(stage, new MutableStageStats());
        }
    }

    /**
     * Record a successful stage execution.
     *
     * @param stage stage
     * @param elapsedNanos elapsed nanos
     */
    @Override
    public void recordSuccess(final TransactionStage stage, final long elapsedNanos) {
        statsByStage.get(stage).recordSuccess(elapsedNanos);
    }

    /**
     * Record a failed stage execution.
     *
     * @param stage stage
     * @param elapsedNanos elapsed nanos
     */
    @Override
    public void recordFailure(final TransactionStage stage, final long elapsedNanos) {
        statsByStage.get(stage).recordFailure(elapsedNanos);
    }

    /**
     * Returns the indicator snapshot at the specified stage.
     *
     * @param stage stage
     * @return the operation result
     */
    public TransactionStageStats snapshot(final TransactionStage stage) {
        return statsByStage.get(stage).snapshot();
    }

    /**
     * Returns the indicator snapshot for all stages.
     *
     * @return the operation result
     */
    public Map<TransactionStage, TransactionStageStats> snapshots() {
        EnumMap<TransactionStage, TransactionStageStats> snapshots = new EnumMap<>(TransactionStage.class);
        for (Map.Entry<TransactionStage, MutableStageStats> entry : statsByStage.entrySet()) {
            snapshots.put(entry.getKey(), entry.getValue().snapshot());
        }
        return Map.copyOf(snapshots);
    }

    /**
     * Available concurrent update phase statistics.
     */
    private static final class MutableStageStats {

        private final LongAdder invocationCount = new LongAdder();

        private final LongAdder successCount = new LongAdder();

        private final LongAdder failureCount = new LongAdder();

        private final LongAdder totalNanos = new LongAdder();

        /**
         * Record a successful phase.
         *
         * @param elapsedNanos elapsed nanos
         */
        private void recordSuccess(final long elapsedNanos) {
            invocationCount.increment();
            successCount.increment();
            totalNanos.add(Math.max(0L, elapsedNanos));
        }

        /**
         * Record a failure phase.
         *
         * @param elapsedNanos elapsed nanos
         */
        private void recordFailure(final long elapsedNanos) {
            invocationCount.increment();
            failureCount.increment();
            totalNanos.add(Math.max(0L, elapsedNanos));
        }

        /**
         * Creates the current statistics snapshot.
         *
         * @return the operation result
         */
        private TransactionStageStats snapshot() {
            return new TransactionStageStats(
                    invocationCount.sum(),
                    successCount.sum(),
                    failureCount.sum(),
                    totalNanos.sum());
        }
    }
}

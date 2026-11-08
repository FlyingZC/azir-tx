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

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Transaction indicator formatting tool for the time-consuming phase of the benchmark test output.
 */
public final class TransactionMetricsFormatter {

    /**
     * Construct the transaction indicator formatting tool.
     */
    private TransactionMetricsFormatter() {
    }

    /**
     * Format all non-empty phase indicators.
     *
     * @param metrics metrics
     * @return the operation result
     */
    public static List<String> formatNonEmpty(final InMemoryTransactionMetrics metrics) {
        Objects.requireNonNull(metrics, "metrics");
        return metrics.snapshots()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().invocationCount() > 0L)
                .sorted(MapEntryStageComparator.INSTANCE)
                .map(entry -> format(entry.getKey(), entry.getValue()))
                .toList();
    }

    /**
     * Formats a single transaction stage indicator.
     *
     * @param stage stage
     * @param stats stats
     * @return the operation result
     */
    private static String format(final TransactionStage stage, final TransactionStageStats stats) {
        return String.format(
                Locale.ROOT,
                "TX_STAGE stage=%s invocations=%d success=%d failure=%d total_ms=%.2f avg_us=%.2f",
                stage.name(),
                stats.invocationCount(),
                stats.successCount(),
                stats.failureCount(),
                stats.totalNanos() / 1_000_000.0,
                stats.averageNanos() / 1_000.0);
    }

    /**
     * Sort the output in order of an itemized declaration.
     */
    private enum MapEntryStageComparator implements java.util.Comparator<java.util.Map.Entry<TransactionStage, TransactionStageStats>> {

        INSTANCE;

        /**
         * Comparison of two-stage indicator entries.
         *
         * @param left left
         * @param right right
         * @return the operation result
         */
        @Override
        public int compare(final java.util.Map.Entry<TransactionStage, TransactionStageStats> left,
                           final java.util.Map.Entry<TransactionStage, TransactionStageStats> right) {
            return Integer.compare(left.getKey().ordinal(), right.getKey().ordinal());
        }
    }
}

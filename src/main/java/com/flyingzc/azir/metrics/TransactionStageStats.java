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

/**
 * Phase transaction indicator snapshot.
 *
 * @param invocationCount invocation count
 * @param successCount success count
 * @param failureCount failure count
 * @param totalNanos total nanos
 */
public record TransactionStageStats(long invocationCount,
        long successCount,
        long failureCount,
        long totalNanos) {

    /**
     * Returns the average time- and-seconds.
     *
     * @return the operation result
     */
    public long averageNanos() {
        return invocationCount == 0L ? 0L : totalNanos / invocationCount;
    }
}

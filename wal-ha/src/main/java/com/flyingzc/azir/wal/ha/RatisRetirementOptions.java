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

package com.flyingzc.azir.wal.ha;

/**
 * The actual scanning, deleting and collaboration time budget maintained by the node does not cancel the I/O that is being implemented through an interruption.
 *
 * @param backendShardLimit backend shard limit
 * @param fileLimit file limit
 * @param scanLimit scan limit
 * @param budgetMillis budget millis
 * @param retainedSnapshots retained snapshots
 */
public record RatisRetirementOptions(int backendShardLimit, int fileLimit, int scanLimit, long budgetMillis, int retainedSnapshots) {

    /**
     * All workload is limited, with at least two up-to-date and complete mirrors maintained.
     *
     * @param backendShardLimit backend shard limit
     * @param fileLimit file limit
     * @param scanLimit scan limit
     * @param budgetMillis budget millis
     * @param retainedSnapshots retained snapshots
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisRetirementOptions {
        if (backendShardLimit < 1 || backendShardLimit > 1024 || fileLimit < 1 || fileLimit > 4096 || scanLimit < 16 || scanLimit > 200_000
                || budgetMillis < 1 || budgetMillis > 120_000 || retainedSnapshots < 2 || retainedSnapshots > 32) {
            throw new IllegalArgumentException("invalid retirement work budgets");
        }
    }

    /**
     * Two pieces per round, one hundred and twenty-eight documents, with a list of up to 4,000 entries.
     *
     * @return the operation result
     */
    public static RatisRetirementOptions defaults() {
        return new RatisRetirementOptions(2, 128, 4096, 1000, 2);
    }
}

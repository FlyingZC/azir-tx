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

package com.flyingzc.azir.maintenance;

/**
 * Limited periods, single-wheel budget and closed waiting times for permanent maintenance.
 *
 * @param intervalMillis interval millis
 * @param batchSize batch size
 * @param closeTimeoutMillis close timeout millis
 */
public record MaintenanceOptions(long intervalMillis, int batchSize, long closeTimeoutMillis) {

    /**
     * Prevent a zero cycle of busy or unlimited waiting.
     *
     * @param intervalMillis interval millis
     * @param batchSize batch size
     * @param closeTimeoutMillis close timeout millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MaintenanceOptions {
        if (intervalMillis < 10 || intervalMillis > 86_400_000 || batchSize < 1 || batchSize > 4096
                || closeTimeoutMillis < 1 || closeTimeoutMillis > 60_000) {
            throw new IllegalArgumentException("invalid maintenance options");
        }
    }

    /**
     * Default to execute one round every five seconds, with a maximum of 128 historical records per round shard.
     *
     * @return the operation result
     */
    public static MaintenanceOptions defaults() {
        return new MaintenanceOptions(5000, 128, 10_000);
    }
}

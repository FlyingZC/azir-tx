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
 * Back-end health management failed to resist shaking, recovery shivering and collaborative waiting budget.
 *
 * @param intervalMillis interval millis
 * @param failures failures
 * @param recoveries recoveries
 * @param probeMillis probe millis
 * @param transferMillis transfer millis
 */
public record RatisHealthOptions(long intervalMillis, int failures, int recoveries, long probeMillis, long transferMillis) {

    /**
     * Both detection and management waiting is limited, and complete master budget must accommodate at least one target detection.
     *
     * @param intervalMillis interval millis
     * @param failures failures
     * @param recoveries recoveries
     * @param probeMillis probe millis
     * @param transferMillis transfer millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisHealthOptions {
        if (intervalMillis < 100 || intervalMillis > 60_000 || failures < 1 || failures > 100 || recoveries < 1 || recoveries > 100
                || probeMillis < 100 || probeMillis > 10_000 || transferMillis < probeMillis + 500 || transferMillis > 120_000) {
            throw new IllegalArgumentException("invalid backend health limits");
        }
    }

    /**
     * Detected in seconds, three failed isolations and three successful recovery qualifications.
     *
     * @return the operation result
     */
    public static RatisHealthOptions defaults() {
        return new RatisHealthOptions(1000, 3, 3, 1000, 10_000);
    }
}

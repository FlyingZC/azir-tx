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

package com.flyingzc.azir.wal;

/**
 * Fixed budget for hosting maintenance to avoid automatically turning into an unbounded scan or busy cycle.
 *
 * @param intervalMillis interval millis
 * @param keyLimit key limit
 * @param rowLimit row limit
 * @param workMillis work millis
 * @param staleAfterMillis stale after millis
 * @param warningPercent warning percent
 */
public record WalMaintenanceOptions(long intervalMillis, int keyLimit, int rowLimit, long workMillis,
        long staleAfterMillis, int warningPercent) {

    /**
     * Maintains a maximum of 4,000 keys to be checked and removes 4,000 version, with an expired state threshold that is greater than the normal cycle.
     *
     * @param intervalMillis interval millis
     * @param keyLimit key limit
     * @param rowLimit row limit
     * @param workMillis work millis
     * @param staleAfterMillis stale after millis
     * @param warningPercent warning percent
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalMaintenanceOptions {
        if (intervalMillis < 10 || intervalMillis > 3_600_000 || keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096
                || workMillis < 1 || workMillis > 5000 || staleAfterMillis <= intervalMillis || staleAfterMillis > 86_400_000
                || warningPercent < 1 || warningPercent > 99) {
            throw new IllegalArgumentException("invalid maintenance limits");
        }
    }

    /**
     * Check a maximum of 64 keys per second, remove 1,000 version and leave sufficient I/O timeout observation windows.
     *
     * @return the operation result
     */
    public static WalMaintenanceOptions defaults() {
        return new WalMaintenanceOptions(1000, 64, 1024, 100, 15_000, 80);
    }
}

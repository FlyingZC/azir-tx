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
 * Current durable maintenance status; lastActiveSnapshot means zero is not active reader, pending Items is to number recovery.
 *
 * @param safePoint oldest retained snapshot timestamp
 * @param oldestActiveSnapshot oldest active snapshot
 * @param activeSnapshots active snapshots
 * @param expiredSnapshots expired snapshots
 * @param pendingItems pending items
 */
public record MaintenanceDiagnostics(long safePoint, long oldestActiveSnapshot, long activeSnapshots,
        long expiredSnapshots, long pendingItems) {

    /**
     * The negative count is rejected to facilitate the interlocking operation of health and alarms.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param oldestActiveSnapshot oldest active snapshot
     * @param activeSnapshots active snapshots
     * @param expiredSnapshots expired snapshots
     * @param pendingItems pending items
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MaintenanceDiagnostics {
        if (safePoint < 0 || oldestActiveSnapshot < 0 || activeSnapshots < 0 || expiredSnapshots < 0 || pendingItems < 0) {
            throw new IllegalArgumentException("invalid maintenance diagnostics");
        }
    }
}

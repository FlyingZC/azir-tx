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
 * Single-wheel maintenance results; safe point is on the current backend and cannot be compared over models.
 *
 * @param recovered recovered
 * @param deletedVersions deleted versions
 * @param safePoint oldest retained snapshot timestamp
 * @param paused paused
 */
public record MaintenanceResult(int recovered, int deletedVersions, long safePoint, boolean paused) {

    /**
     * Retains the tectonicization of the result with three parameters.
     *
     * @param recovered recovered
     * @param deletedVersions deleted versions
     * @param safePoint oldest retained snapshot timestamp
     */
    public MaintenanceResult(final int recovered, final int deletedVersions, final long safePoint) {
        this(recovered, deletedVersions, safePoint, false);
    }

    /**
     * Unlawful maintenance count denied.
     *
     * @param recovered recovered
     * @param deletedVersions deleted versions
     * @param safePoint oldest retained snapshot timestamp
     * @param paused paused
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MaintenanceResult {
        if (recovered < 0 || deletedVersions < 0 || safePoint < 0) {
            throw new IllegalArgumentException("invalid maintenance result");
        }
    }
}

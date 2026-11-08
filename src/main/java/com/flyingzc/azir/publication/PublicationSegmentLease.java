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

package com.flyingzc.azir.publication;

/**
 * An durable set aside commit time slot and its owner fencing algebra.
 *
 * @param firstTs first ts
 * @param lastTs last ts
 * @param ownerId owner id
 * @param generation generation
 * @param expiresAtMillis expires at millis
 */
public record PublicationSegmentLease(long firstTs, long lastTs, String ownerId,
        long generation, long expiresAtMillis) {

    /**
     * Rejects invalid range, owner or algebra and avoids the local distribution crossing the durable border.
     *
     * @param firstTs first ts
     * @param lastTs last ts
     * @param ownerId owner id
     * @param generation generation
     * @param expiresAtMillis expires at millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationSegmentLease {
        if (firstTs < 1 || lastTs < firstTs || ownerId == null || ownerId.isBlank()
                || generation < 1 || expiresAtMillis < 1) {
            throw new IllegalArgumentException("invalid publication segment lease");
        }
    }
}

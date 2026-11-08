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
 * One of the oldest coordinator -grade snapshot protected by control database durable storage and fencing algebras.
 *
 * @param coordinatorId coordinator id
 * @param readTs snapshot read timestamp
 * @param expiresAtMillis expires at millis
 * @param generation generation
 */
public record CoordinatorSnapshotLease(long coordinatorId, long readTs,
        long expiresAtMillis, long generation) {

    /**
     * Rejects missing numbers, negative snapshot and unavailable durable algebras.
     *
     * @param coordinatorId coordinator id
     * @param readTs snapshot read timestamp
     * @param expiresAtMillis expires at millis
     * @param generation generation
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public CoordinatorSnapshotLease {
        if (coordinatorId < 1 || readTs < 0 || expiresAtMillis < 1 || generation < 1) {
            throw new IllegalArgumentException("invalid coordinator snapshot lease");
        }
    }
}

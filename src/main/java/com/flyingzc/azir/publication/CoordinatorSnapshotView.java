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
 * A published read watermark and database deadline protected by coordinator lease.
 *
 * @param readTs snapshot read timestamp
 * @param deadlineMillis deadline millis
 * @param leaseReadTs lease read ts
 * @param leaseExpiryMillis lease expiry millis
 */
public record CoordinatorSnapshotView(long readTs, long deadlineMillis,
        long leaseReadTs, long leaseExpiryMillis) {

    /**
     * Reject snapshot that is not published or has lost protection at the lower lease level.
     *
     * @param readTs snapshot read timestamp
     * @param deadlineMillis deadline millis
     * @param leaseReadTs lease read ts
     * @param leaseExpiryMillis lease expiry millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public CoordinatorSnapshotView {
        if (readTs < 0 || deadlineMillis < 1 || leaseReadTs < 0
                || leaseReadTs > readTs || leaseExpiryMillis < deadlineMillis) {
            throw new IllegalArgumentException("invalid coordinator protected snapshot");
        }
    }
}

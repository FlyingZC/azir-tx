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

package com.flyingzc.azir.backend;

/**
 * Public commit receipt, read-only results do not commit durable storage writing decision, timestamp does not explain backend.
 *
 * @param id id
 * @param commitTs durable commit timestamp
 * @param readOnly read only
 */
public record BackendCommit(BackendId id, long commitTs, boolean readOnly) {

    /**
     * Write commit must carry the real commit timestamp later than the start border.
     *
     * @param id id
     * @param commitTs durable commit timestamp
     * @param readOnly read only
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BackendCommit {
        java.util.Objects.requireNonNull(id, "id");
        if (readOnly ? commitTs != 0 : commitTs <= id.startTs()) {
            throw new IllegalArgumentException("invalid backend commit timestamp");
        }
    }
}

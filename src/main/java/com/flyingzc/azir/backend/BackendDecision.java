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
 * Durable Status query result; PENDING indicates that terminal state has not been confirmed and cannot be interpreted as aborting.
 *
 * @param state state
 * @param commitTs durable commit timestamp
 */
public record BackendDecision(State state, long commitTs) {

    /**
     * Only commit states carry commit timestamp.
     *
     * @param state state
     * @param commitTs durable commit timestamp
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BackendDecision {
        java.util.Objects.requireNonNull(state, "state");
        if ((state == State.COMMITTED) != (commitTs > 0)) {
            throw new IllegalArgumentException("invalid decision");
        }
    }

    /**
     * Queryable terminal state and clear receipt retirement status.
     */
    public enum State {
        PENDING, COMMITTED, ABORTED, RETIRED
    }
}

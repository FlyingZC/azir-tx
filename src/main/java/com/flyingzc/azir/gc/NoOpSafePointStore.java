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

package com.flyingzc.azir.gc;

/**
 * Do not advance the empty safe point realization.
 */
public final class NoOpSafePointStore implements SafePointStore {

    public static final NoOpSafePointStore INSTANCE = new NoOpSafePointStore();

    /**
     * Construct a NoOpSafePointStore instance.
     */
    private NoOpSafePointStore() {
    }

    /**
     * Return the current safe point.
     *
     * @return the operation result
     */
    @Override
    public long currentSafePoint() {
        return 0L;
    }

    /**
     * Try to advance the safe point to the candidate timestamp.
     *
     * @param candidateSafePoint candidate safe point
     * @return the operation result
     */
    @Override
    public long advanceTo(final long candidateSafePoint) {
        return candidateSafePoint;
    }
}

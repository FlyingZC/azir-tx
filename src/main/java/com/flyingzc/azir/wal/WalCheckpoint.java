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
 * A backend global log prefix with a closing summary of the atomic application.
 *
 * @param sequence sequence
 * @param digest digest
 */
public record WalCheckpoint(long sequence, String digest) {

    /**
     * Validate Anticipated and unprecedented summary indicates.
     *
     * @param sequence sequence
     * @param digest digest
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalCheckpoint {
        if (sequence < 0 || digest == null || (sequence == 0 ? !digest.isEmpty() : digest.length() != 64)) {
            throw new IllegalArgumentException("invalid materialization checkpoint");
        }
    }
}

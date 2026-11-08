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
 * Immutable commit version, delete version and keep the commit serial number.
 *
 * @param commitSequence commit sequence
 * @param value value to write
 */
public record WalValue(long commitSequence, byte[] value) {

    /**
     * Copy version values to isolate changes in the business code for bottom data.
     *
     * @param commitSequence commit sequence
     * @param value value to write
     */
    public WalValue {
        value = value == null ? null : value.clone();
    }

    /**
     * Returns version value replica.
     *
     * @return the operation result
     */
    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    /**
     * Determine whether the version is a tombstone.
     *
     * @return the operation result
     */
    public boolean deleted() {
        return value == null;
    }

    /**
     * Inquiries payload length do not copy the whole line for current backup and recovery budget.
     *
     * @return the operation result
     */
    public int valueLength() {
        return value == null ? 0 : value.length;
    }
}

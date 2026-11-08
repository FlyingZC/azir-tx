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
 * Distinguishing between commit and searching for reservation information is inadequate, and no missing result can deduce business rollback.
 *
 * @param status status
 * @param receipt receipt
 * @param retiredThroughSequence retired through sequence
 */
public record WalCommitLookup(Status status, WalReceipt receipt, long retiredThroughSequence) {

    /**
     * Ensures that only confirmed states with receipt and collection watermark remain non-negative.
     *
     * @param status status
     * @param receipt receipt
     * @param retiredThroughSequence retired through sequence
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalCommitLookup {
        if (status == null || (status == Status.COMMITTED) != (receipt != null) || retiredThroughSequence < 0) {
            throw new IllegalArgumentException("invalid commit lookup outcome");
        }
    }

    /**
     * When there is no receipt and the collection part of history, only an unknown or past query window can be conservatively indicated.
     */
    public enum Status {
        COMMITTED, NOT_CONFIRMED, UNKNOWN_OR_EXPIRED
    }
}

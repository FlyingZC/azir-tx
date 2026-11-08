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
 * Tie transaction statement snapshot to read multiple keys, cross-shard and page breaks for the same statement.
 */
public final class WalReadSnapshot {

    final WalTransaction transaction;

    final long sequence;

    /**
     * Allow only running to create snapshot on a valid transaction.
     *
     * @param transaction transaction context
     * @param sequence sequence
     */
    WalReadSnapshot(final WalTransaction transaction, final long sequence) {
        this.transaction = transaction;
        this.sequence = sequence;
    }

    /**
     * Returns a fixed global to read watermark.
     *
     * @return the operation result
     */
    public long sequence() {
        return sequence;
    }
}

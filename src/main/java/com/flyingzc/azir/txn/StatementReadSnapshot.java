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

package com.flyingzc.azir.txn;

/**
 * The statement-level used by the SQL Class Upper Executor reads snapshot and ensures that multiple shard scans in a relationship operation share the same timestamp.
 */
public final class StatementReadSnapshot {

    private final long txId;

    private final long readTs;

    /**
     * Only transaction manager packages are allowed to create snapshot with isolation level and safe point validate.
     *
     * @param txId transaction identifier
     * @param readTs snapshot read timestamp
     */
    StatementReadSnapshot(final long txId, final long readTs) {
        this.txId = txId;
        this.readTs = readTs;
    }

    /**
     * Returns snapshot bounded transaction identifier.
     *
     * @return the operation result
     */
    public long txId() {
        return txId;
    }

    /**
     * Return statement to read timestamp.
     *
     * @return the operation result
     */
    public long readTs() {
        return readTs;
    }
}

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
 * Transaction commit protocol type, describing the protocol path to be followed in the commit phase.
 */
public enum TransactionProtocolType {
    /**
     * Read-only snapshot transaction path.
     */
    READ_ONLY_SNAPSHOT,

    /**
     * Single shard phase commit path.
     */
    SINGLE_SHARD_1PC,

    /**
     * Cross-shard Step commit Path.
     */
    CROSS_SHARD_ASYNC_COMMIT,

    /**
     * Cross-shard Sync Phase commit Path.
     */
    CROSS_SHARD_SYNC_2PC,

    /**
     * Hotspot string commit path.
     */
    HOT_KEY_SERIALIZED,

    /**
     * Large batch writes the commit path.
     */
    LARGE_WRITE_BATCH
}

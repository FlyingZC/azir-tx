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

package com.flyingzc.azir.metrics;

/**
 * Transaction Thermal Path Stage Counting, used to locate MVCC performance bottlenecks.
 */
public enum TransactionStage {
    BEGIN_PUBLICATION,
    LEDGER_BEGIN,
    BEGIN_TSO,
    BEGIN_METADATA,
    READ_TSO,
    EAGER_PREWRITE,
    BUFFERED_PREWRITE,
    COMMIT_TSO,
    COMMIT_PUBLICATION,
    LEDGER_RESERVE,
    LEDGER_DECISION,
    LEDGER_ATOMIC_COMMIT,
    LEDGER_PUBLISH,
    LEDGER_AWAIT,
    SEGMENT_BEGIN,
    SEGMENT_RESERVE,
    SEGMENT_DECISION,
    SEGMENT_LOCAL_CLAIM,
    SEGMENT_LOCAL_COMPLETE,
    SEGMENT_AWAIT,
    SINGLE_SHARD_METADATA_COMMIT,
    SINGLE_SHARD_PARTICIPANT_COMMIT,
    CROSS_SHARD_METADATA_COMMIT,
    PREPARE,
    PRIMARY_PARTICIPANT_COMMIT,
    SECONDARY_PARTICIPANT_COMMIT,
    ROLLBACK,
    ASYNC_PARTICIPANT_COMMIT,
    INTENT_CLEANUP
}

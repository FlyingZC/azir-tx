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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;

/**
 * The transaction option test, the verify default configuration and the visible preset will not be confused.
 */
class TransactionOptionsTest {

    /**
     * The verify default transaction option opens the ACID security performance optimization.
     */
    @Test
    void shouldUseSafePerformanceOptionsByDefault() {
        TransactionOptions actual = TransactionOptions.defaults();

        Assertions.assertEquals(ConflictMode.WAIT, actual.lockWaitPolicy().conflictMode());
        Assertions.assertEquals(8, actual.retryPolicy().maxAttempts());
        assertTrue(actual.crossShardCommitPolicy().parallelPrewrite());
        assertTrue(actual.crossShardCommitPolicy().parallelPrepare());
        assertTrue(actual.crossShardCommitPolicy().parallelCommit());
        assertTrue(actual.crossShardCommitPolicy().parallelRollback());
        assertFalse(actual.crossShardCommitPolicy().asyncCommit());
        assertTrue(actual.bufferWrites());
        assertFalse(actual.lazyBeginMetadata());
        assertFalse(actual.deferIntentCleanup());
    }

    /**
     * Verify failed presets the retention of low delay failure and serial commit.
     */
    @Test
    void shouldKeepExplicitFailFastPreset() {
        TransactionOptions actual = TransactionOptionsPresets.failFast();

        Assertions.assertEquals(ConflictMode.FAIL_FAST, actual.lockWaitPolicy().conflictMode());
        Assertions.assertEquals(1, actual.retryPolicy().maxAttempts());
        assertFalse(actual.crossShardCommitPolicy().parallelPrewrite());
        assertFalse(actual.crossShardCommitPolicy().parallelPrepare());
        assertFalse(actual.crossShardCommitPolicy().parallelCommit());
        assertFalse(actual.crossShardCommitPolicy().parallelRollback());
        assertFalse(actual.crossShardCommitPolicy().asyncCommit());
        assertFalse(actual.bufferWrites());
        assertFalse(actual.lazyBeginMetadata());
        assertFalse(actual.deferIntentCleanup());
    }
}

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

package com.flyingzc.azir.storage.mysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * MySQL storage adapter SQL construction test.
 */
class MysqlStorageAdapterSqlTest {

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void shouldBuildBatchDirectCommitConflictSql() {
        Assertions.assertEquals(
                "select user_key from mvcc_data where user_key in (?, ?, ?) and commit_ts > ? limit 1",
                MysqlStorageAdapter.directCommitConflictSql(3));
    }

    /**
     * Verify single key direct conflict check SQL remains valid.
     */
    @Test
    void shouldBuildSingleDirectCommitConflictSql() {
        Assertions.assertEquals(
                "select user_key from mvcc_data where user_key in (?) and commit_ts > ? limit 1",
                MysqlStorageAdapter.directCommitConflictSql(1));
    }

    /**
     * Verify Large write set will be broken into bounded conflict check batch.
     */
    @Test
    void shouldSplitLargeDirectCommitConflictChecksIntoBoundedChunks() {
        Assertions.assertEquals(1, MysqlStorageAdapter.directCommitConflictChunkCount(512));
        Assertions.assertEquals(2, MysqlStorageAdapter.directCommitConflictChunkCount(513));
        Assertions.assertEquals(3, MysqlStorageAdapter.directCommitConflictChunkCount(1025));
    }
}

package com.flyingzc.azir.storage.mysql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MySQL 存储适配器 SQL 构造测试。
 */
class MysqlStorageAdapterSqlTest {
    /**
     * 验证多 key direct commit 冲突检查使用单条 in 查询。
     */
    @Test
    void shouldBuildBatchDirectCommitConflictSql() {
        assertEquals(
                "select user_key from mvcc_data where user_key in (?, ?, ?) and commit_ts > ? limit 1",
                MysqlStorageAdapter.directCommitConflictSql(3)
        );
    }

    /**
     * 验证单 key direct commit 冲突检查 SQL 保持有效。
     */
    @Test
    void shouldBuildSingleDirectCommitConflictSql() {
        assertEquals(
                "select user_key from mvcc_data where user_key in (?) and commit_ts > ? limit 1",
                MysqlStorageAdapter.directCommitConflictSql(1)
        );
    }

    /**
     * 验证大写集合会被拆成有界冲突检查批次。
     */
    @Test
    void shouldSplitLargeDirectCommitConflictChecksIntoBoundedChunks() {
        assertEquals(1, MysqlStorageAdapter.directCommitConflictChunkCount(512));
        assertEquals(2, MysqlStorageAdapter.directCommitConflictChunkCount(513));
        assertEquals(3, MysqlStorageAdapter.directCommitConflictChunkCount(1025));
    }
}

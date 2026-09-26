package mvcc.storage.mysql;

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
}

package mvcc.sql;

import mvcc.common.IsolationLevel;

/**
 * SQL 执行计划封闭接口，描述当前轻量 SQL 层支持的计划类型。
 */
public sealed interface SqlPlan {
    /**
     * 开启事务计划。
     */
    record BeginTransactionPlan(IsolationLevel isolationLevel) implements SqlPlan {
    }

    /**
     * 提交事务计划。
     */
    record CommitTransactionPlan() implements SqlPlan {
    }

    /**
     * 回滚事务计划。
     */
    record RollbackTransactionPlan() implements SqlPlan {
    }

    /**
     * 主键点查计划。
     */
    record SelectByKeyPlan(String table, String key) implements SqlPlan {
    }

    /**
     * 主键插入计划。
     */
    record InsertByKeyPlan(String table, String key, String value) implements SqlPlan {
    }

    /**
     * 主键更新计划。
     */
    record UpdateByKeyPlan(String table, String key, String value) implements SqlPlan {
    }

    /**
     * 主键删除计划。
     */
    record DeleteByKeyPlan(String table, String key) implements SqlPlan {
    }
}

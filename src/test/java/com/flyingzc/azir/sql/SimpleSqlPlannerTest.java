package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 简单 SQL 计划器测试。
 */
class SimpleSqlPlannerTest {
    private final SimpleSqlPlanner planner = new SimpleSqlPlanner();

    /**
     * 验证 begin 语句可以解析隔离级别。
     */
    @Test
    void shouldParseBeginIsolationLevels() {
        SqlPlan.BeginTransactionPlan defaultBegin = assertInstanceOf(
                SqlPlan.BeginTransactionPlan.class,
                planner.plan("begin")
        );
        SqlPlan.BeginTransactionPlan rcBegin = assertInstanceOf(
                SqlPlan.BeginTransactionPlan.class,
                planner.plan("BEGIN READ COMMITTED")
        );
        SqlPlan.BeginTransactionPlan rrBegin = assertInstanceOf(
                SqlPlan.BeginTransactionPlan.class,
                planner.plan("begin transaction isolation level repeatable read")
        );

        assertEquals(IsolationLevel.READ_COMMITTED, defaultBegin.isolationLevel());
        assertEquals(IsolationLevel.READ_COMMITTED, rcBegin.isolationLevel());
        assertEquals(IsolationLevel.REPEATABLE_READ, rrBegin.isolationLevel());
    }

    /**
     * 验证事务控制语句可以解析为独立计划。
     */
    @Test
    void shouldParseTransactionControlPlans() {
        assertInstanceOf(SqlPlan.CommitTransactionPlan.class, planner.plan("commit"));
        assertInstanceOf(SqlPlan.RollbackTransactionPlan.class, planner.plan("ROLLBACK"));
    }

    /**
     * 验证主键点查语句可以解析表名和主键。
     */
    @Test
    void shouldParseSelectByKey() {
        SqlPlan.SelectByKeyPlan plan = assertInstanceOf(
                SqlPlan.SelectByKeyPlan.class,
                planner.plan("select value from account where id = 'u1'")
        );

        assertEquals("account", plan.table());
        assertEquals("u1", plan.key());
    }

    /**
     * 验证插入语句可以解析转义后的字符串字面量。
     */
    @Test
    void shouldParseInsertWithEscapedLiteral() {
        SqlPlan.InsertByKeyPlan plan = assertInstanceOf(
                SqlPlan.InsertByKeyPlan.class,
                planner.plan("INSERT INTO account (id, value) VALUES ('u''1', 'v''1')")
        );

        assertEquals("account", plan.table());
        assertEquals("u'1", plan.key());
        assertEquals("v'1", plan.value());
    }

    /**
     * 验证更新语句可以解析 value 和 where 主键。
     */
    @Test
    void shouldParseUpdateByKey() {
        SqlPlan.UpdateByKeyPlan plan = assertInstanceOf(
                SqlPlan.UpdateByKeyPlan.class,
                planner.plan("update account set value = 'v2' where id = 'u2'")
        );

        assertEquals("account", plan.table());
        assertEquals("u2", plan.key());
        assertEquals("v2", plan.value());
    }

    /**
     * 验证删除语句可以解析表名和主键。
     */
    @Test
    void shouldParseDeleteByKey() {
        SqlPlan.DeleteByKeyPlan plan = assertInstanceOf(
                SqlPlan.DeleteByKeyPlan.class,
                planner.plan("delete from account where id = 'u3'")
        );

        assertEquals("account", plan.table());
        assertEquals("u3", plan.key());
    }

    /**
     * 验证不支持的 SQL 会返回非法状态错误。
     */
    @Test
    void shouldRejectUnsupportedSql() {
        MvccException ex = assertThrows(MvccException.class, () -> planner.plan("select * from account"));

        assertEquals(ErrorCode.ILLEGAL_STATE, ex.errorCode());
        assertFalse(ex.retryable());
    }
}

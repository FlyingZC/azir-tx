package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Calcite SQL 计划器测试。
 */
class CalciteSqlPlannerTest {
    private final CalciteSqlPlanner planner = new CalciteSqlPlanner();

    /**
     * 验证复杂查询经过 Calcite 校验并提取真实关系表。
     */
    @Test
    void shouldPlanJoinAggregationHavingAndSort() {
        SqlPlan.CalcitePlan plan = assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("""
                        select a.value as category, count(*) as total
                        from account a join profile p on a.id = p.id
                        where p.value is not null
                        group by a.value
                        having count(*) > 0
                        order by total desc
                        limit 10
                        """)
        );

        assertEquals(CalciteStatementType.SELECT, plan.statementType());
        assertTrue(plan.tableNames().contains("account"));
        assertTrue(plan.tableNames().contains("profile"));
    }

    /**
     * 验证常用 INSERT、UPDATE、DELETE 都生成 Calcite DML 计划。
     */
    @Test
    void shouldPlanCommonDml() {
        SqlPlan.CalcitePlan insert = assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("insert into account (id, value) values ('1', 'a'), ('2', 'b')")
        );
        SqlPlan.CalcitePlan update = assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("update account set value = upper(value) where id = '1'")
        );
        SqlPlan.CalcitePlan delete = assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("delete from account where value like 'A%'")
        );

        assertEquals(CalciteStatementType.INSERT, insert.statementType());
        assertEquals(CalciteStatementType.UPDATE, update.statementType());
        assertEquals(CalciteStatementType.DELETE, delete.statementType());
    }

    /**
     * 验证未知列会在 Calcite 校验阶段被拒绝。
     */
    @Test
    void shouldRejectUnknownColumn() {
        MvccException exception = assertThrows(
                MvccException.class,
                () -> planner.plan("select missing_column from account")
        );

        assertEquals(ErrorCode.ILLEGAL_STATE, exception.errorCode());
    }
}

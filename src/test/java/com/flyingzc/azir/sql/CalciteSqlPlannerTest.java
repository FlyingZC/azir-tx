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

package com.flyingzc.azir.sql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.Test;

/**
 * Calcite SQL Programr Test.
 */
class CalciteSqlPlannerTest {

    private final CalciteSqlPlanner planner = new CalciteSqlPlanner();

    /**
     * Verify complex queries go through Calcite validate and extract a true relationship table.
     */
    @Test
    void shouldPlanJoinAggregationHavingAndSort() {
        SqlPlan.CalcitePlan plan = Assertions.assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("""
                        select a.value as category, count(*) as total
                        from account a join profile p on a.id = p.id
                        where p.value is not null
                        group by a.value
                        having count(*) > 0
                        order by total desc
                        limit 10
                        """));

        Assertions.assertEquals(CalciteStatementType.SELECT, plan.statementType());
        assertTrue(plan.tableNames().contains("account"));
        assertTrue(plan.tableNames().contains("profile"));
    }

    /**
     * Verify Common INSERT, UPDATE and DELETE generate Calcite DML programs.
     */
    @Test
    void shouldPlanCommonDml() {
        SqlPlan.CalcitePlan insert = Assertions.assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("insert into account (id, value) values ('1', 'a'), ('2', 'b')"));
        SqlPlan.CalcitePlan update = Assertions.assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("update account set value = upper(value) where id = '1'"));
        SqlPlan.CalcitePlan delete = Assertions.assertInstanceOf(
                SqlPlan.CalcitePlan.class,
                planner.plan("delete from account where value like 'A%'"));

        Assertions.assertEquals(CalciteStatementType.INSERT, insert.statementType());
        Assertions.assertEquals(CalciteStatementType.UPDATE, update.statementType());
        Assertions.assertEquals(CalciteStatementType.DELETE, delete.statementType());
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void shouldRejectUnknownColumn() {
        MvccException exception = assertThrows(
                MvccException.class,
                () -> planner.plan("select missing_column from account"));

        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, exception.errorCode());
    }
}

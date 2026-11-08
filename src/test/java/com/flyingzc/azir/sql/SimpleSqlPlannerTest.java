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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import org.junit.jupiter.api.Test;

/**
 * Simple SQL planner test.
 */
class SimpleSqlPlannerTest {

    private final SimpleSqlPlanner planner = new SimpleSqlPlanner();

    /**
     * Verify begin statement can parse the level of isolation.
     */
    @Test
    void shouldParseBeginIsolationLevels() {
        SqlPlan.BeginTransactionPlan defaultBegin = Assertions.assertInstanceOf(
                SqlPlan.BeginTransactionPlan.class,
                planner.plan("begin"));
        SqlPlan.BeginTransactionPlan rcBegin = Assertions.assertInstanceOf(
                SqlPlan.BeginTransactionPlan.class,
                planner.plan("BEGIN READ COMMITTED"));
        SqlPlan.BeginTransactionPlan rrBegin = Assertions.assertInstanceOf(
                SqlPlan.BeginTransactionPlan.class,
                planner.plan("begin transaction isolation level repeatable read"));

        Assertions.assertEquals(IsolationLevel.READ_COMMITTED, defaultBegin.isolationLevel());
        Assertions.assertEquals(IsolationLevel.READ_COMMITTED, rcBegin.isolationLevel());
        Assertions.assertEquals(IsolationLevel.REPEATABLE_READ, rrBegin.isolationLevel());
    }

    /**
     * Verify transaction control statement can be interpreted as an independent plan.
     */
    @Test
    void shouldParseTransactionControlPlans() {
        Assertions.assertInstanceOf(SqlPlan.CommitTransactionPlan.class, planner.plan("commit"));
        Assertions.assertInstanceOf(SqlPlan.RollbackTransactionPlan.class, planner.plan("ROLLBACK"));
    }

    /**
     * Verify primary check statement to parse table names and primary keys.
     */
    @Test
    void shouldParseSelectByKey() {
        SqlPlan.SelectByKeyPlan plan = Assertions.assertInstanceOf(
                SqlPlan.SelectByKeyPlan.class,
                planner.plan("select value from account where id = 'u1'"));

        Assertions.assertEquals("account", plan.table());
        Assertions.assertEquals("u1", plan.key());
    }

    /**
     * Verify Inserts a statement that resolves the string face of the transliteration.
     */
    @Test
    void shouldParseInsertWithEscapedLiteral() {
        SqlPlan.InsertByKeyPlan plan = Assertions.assertInstanceOf(
                SqlPlan.InsertByKeyPlan.class,
                planner.plan("INSERT INTO account (id, value) VALUES ('u''1', 'v''1')"));

        Assertions.assertEquals("account", plan.table());
        Assertions.assertEquals("u'1", plan.key());
        Assertions.assertEquals("v'1", plan.value());
    }

    /**
     * Verify Updates can parse the main key of value and where.
     */
    @Test
    void shouldParseUpdateByKey() {
        SqlPlan.UpdateByKeyPlan plan = Assertions.assertInstanceOf(
                SqlPlan.UpdateByKeyPlan.class,
                planner.plan("update account set value = 'v2' where id = 'u2'"));

        Assertions.assertEquals("account", plan.table());
        Assertions.assertEquals("u2", plan.key());
        Assertions.assertEquals("v2", plan.value());
    }

    /**
     * Verify Deletes statements to parse table names and primary keys.
     */
    @Test
    void shouldParseDeleteByKey() {
        SqlPlan.DeleteByKeyPlan plan = Assertions.assertInstanceOf(
                SqlPlan.DeleteByKeyPlan.class,
                planner.plan("delete from account where id = 'u3'"));

        Assertions.assertEquals("account", plan.table());
        Assertions.assertEquals("u3", plan.key());
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     */
    @Test
    void shouldRejectUnsupportedSql() {
        MvccException ex = assertThrows(MvccException.class, () -> planner.plan("select * from account"));

        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, ex.errorCode());
        assertFalse(ex.retryable());
    }
}

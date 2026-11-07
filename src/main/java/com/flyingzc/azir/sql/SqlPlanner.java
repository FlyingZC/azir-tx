package com.flyingzc.azir.sql;

/**
 * SQL 计划器接口，负责把输入 SQL 转换为可执行计划。
 */
@FunctionalInterface
public interface SqlPlanner {
    /**
     * 解析 SQL 并返回可执行计划。
     */
    SqlPlan plan(String sql);
}

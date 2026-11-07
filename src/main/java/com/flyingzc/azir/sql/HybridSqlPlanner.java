package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.MvccException;

/** 主键简单 SQL 使用轻量计划，其他合法 SQL 交给完整 Calcite 计划器。 */
public final class HybridSqlPlanner implements SqlPlanner {
    private final SimpleSqlPlanner simple = new SimpleSqlPlanner();
    private final CalciteSqlPlanner calcite = new CalciteSqlPlanner();

    /** 只把简单计划器明确“不支持”的语句转交 Calcite，不吞掉其他错误。 */
    @Override
    public SqlPlan plan(String sql) {
        try {
            return simple.plan(sql);
        } catch (MvccException unsupported) {
            if (unsupported.getMessage() == null
                    || !unsupported.getMessage().startsWith("unsupported sql:")) {
                throw unsupported;
            }
            return calcite.plan(sql);
        }
    }
}

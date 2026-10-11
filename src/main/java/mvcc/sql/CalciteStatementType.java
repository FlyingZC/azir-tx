package mvcc.sql;

/**
 * Calcite SQL 语句类型，限定当前关系执行器支持的查询与常用 DML。
 */
public enum CalciteStatementType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE
}

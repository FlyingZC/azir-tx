package mvcc.sql;

import java.util.Map;

/**
 * SQL 查询结果行，使用列名到字符串值的简单映射表达。
 */
public record SqlRow(Map<String, String> columns) {
    /**
     * 构造不可变结果行。
     */
    public SqlRow {
        columns = Map.copyOf(columns);
    }

    /**
     * 按列名读取字符串值。
     */
    public String get(String columnName) {
        return columns.get(columnName);
    }
}

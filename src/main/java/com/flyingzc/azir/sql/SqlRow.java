package com.flyingzc.azir.sql;

import java.util.Map;
import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * SQL 查询结果行，使用列名到字符串值的简单映射表达。
 */
public record SqlRow(Map<String, String> columns) {
    /**
     * 构造不可变结果行。
     */
    public SqlRow {
        // SQL 外连接和空集聚合会产生 null，不能使用拒绝 null 的 Map.copyOf。
        columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    /**
     * 按列名读取字符串值。
     */
    public String get(String columnName) {
        String exact = columns.get(columnName);
        if (exact != null || columns.containsKey(columnName)) {
            return exact;
        }
        return columns.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(columnName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}

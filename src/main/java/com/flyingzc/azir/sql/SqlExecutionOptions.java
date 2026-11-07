package com.flyingzc.azir.sql;

/**
 * SQL 分页、输入扫描与物化结果预算，不代表 JVM 堆上限或磁盘溢写能力。
 * 输入按取回的物理键 UTF-8 字节和原始 value 字节计费，包含页面预取、投影丢弃和重扫的行；结果按列值 UTF-8 计费。
 */
public record SqlExecutionOptions(int scanPageSize, int maxResultRows, long maxResultBytes,
                                  long maxScannedRows, long maxScannedBytes) {
    /**
     * 校验分页大小处于存储接口允许的 1..4096 范围，输入和结果预算均为正数。
     */
    public SqlExecutionOptions {
        if (scanPageSize <= 0 || scanPageSize > 4096 || maxResultRows <= 0 || maxResultBytes <= 0
                || maxScannedRows <= 0 || maxScannedBytes <= 0) {
            throw new IllegalArgumentException("SQL page size must be in 1..4096 and all budgets must be positive");
        }
    }

    /**
     * 保留原三参数构造入口，补充每语句一百万输入行和 256 MiB 输入字节的默认限制。
     */
    public SqlExecutionOptions(int scanPageSize, int maxResultRows, long maxResultBytes) {
        this(scanPageSize, maxResultRows, maxResultBytes, 1_000_000L, 256L * 1024 * 1024);
    }

    /**
     * 返回每页 256 行、十万结果行和 64 MiB 结果字节，以及默认输入限制的配置。
     */
    public static SqlExecutionOptions defaults() {
        return new SqlExecutionOptions(256, 100_000, 64L * 1024 * 1024);
    }
}

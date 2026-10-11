package mvcc.sql;

import java.util.Locale;

/**
 * SQL 逻辑表主键与 MVCC 物理键之间的编解码器，确保同一分片上的不同表互不串行。
 */
public final class SqlStorageKeyCodec {
    private static final char TABLE_SEPARATOR = '\u0000';
    private static final char RANGE_END = '\u0001';

    /**
     * 把逻辑表名和行主键编码为物理键。
     */
    public String encode(String table, String key) {
        return normalizedTable(table) + TABLE_SEPARATOR + key;
    }

    /**
     * 返回指定表物理键范围的包含型起点。
     */
    public String rangeStart(String table) {
        return normalizedTable(table) + TABLE_SEPARATOR;
    }

    /**
     * 返回指定表物理键范围的排除型终点。
     */
    public String rangeEnd(String table) {
        return normalizedTable(table) + RANGE_END;
    }

    /**
     * 从指定表的物理键中解码逻辑主键。
     */
    public String decode(String table, String physicalKey) {
        String prefix = rangeStart(table);
        if (!physicalKey.startsWith(prefix)) {
            throw new IllegalArgumentException("physical key does not belong to table " + table);
        }
        return physicalKey.substring(prefix.length());
    }

    /**
     * 规范化不区分大小写的 SQL 表名。
     */
    private String normalizedTable(String table) {
        return table.toLowerCase(Locale.ROOT);
    }
}

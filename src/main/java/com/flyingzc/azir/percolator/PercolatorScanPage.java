package com.flyingzc.azir.percolator;

import java.util.List;

/** 固定语句快照的有界扫描页；不可见原始键也保留位置，保证游标推进及扫描预算计费。 */
public record PercolatorScanPage(List<Entry> entries, String nextKey) {
    /** 固定本页内容；空游标表示候选范围已经扫描结束。 */
    public PercolatorScanPage { entries = List.copyOf(entries); }

    /** 一个原始候选键；空值表示此快照不可见或被墓碑、本事务删除遮蔽。 */
    public record Entry(String key, byte[] value) {
        /** 复制非空值，不伪造不存在版本的提交时间戳。 */
        public Entry {
            java.util.Objects.requireNonNull(key, "key");
            value = value == null ? null : value.clone();
        }

        /** 禁止调用方更改已返回页的内容。 */
        @Override
        public byte[] value() { return value == null ? null : value.clone(); }
    }
}

package mvcc.backend;

import mvcc.storage.KeyRange;
import java.util.List;
import java.util.Optional;

/** 固定语句读边界，点读、全部分片及分页重扫共用同一实例。 */
public interface BackendReadView extends AutoCloseable {
    /** 原始候选行；空值表示此快照不可见，仍可用于输入预算计费。 */
    record Row(String key, byte[] value) {
        /** 防止调用方改动返回内容。 */
        public Row {
            java.util.Objects.requireNonNull(key, "key");
            value = value == null ? null : value.clone();
        }
        /** 返回独立值副本。 */
        @Override
        public byte[] value() { return value == null ? null : value.clone(); }
    }

    /** 有界原始键页，nextKey 是排他游标，空值表示该范围结束。 */
    record Page(List<Row> rows, String nextKey) {
        /** 固定页中候选集合。 */
        public Page { rows = List.copyOf(rows); }
    }

    /** 返回当前语句固定读边界。 */
    long readTs();
    /** 读取指定分片业务键，包含本事务写集覆盖。 */
    Optional<byte[]> get(String shard, String key);
    /** 使用同一边界读取一页，包含不可见候选的游标位置。 */
    Page scanPage(String shard, KeyRange range, String afterKeyExclusive, int limit);
    /** 释放原生只读语句资源，不能提交业务写事务。 */
    @Override
    void close();
}

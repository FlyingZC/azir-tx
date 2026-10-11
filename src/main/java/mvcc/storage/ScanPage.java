package mvcc.storage;

import java.util.List;

/**
 * 有界快照扫描页；游标为最后扫描的原始键，空页也可能有下一页。
 */
public record ScanPage(List<VersionedValue> values, String nextKey) {
    /**
     * 固定本页值列表，空游标表示范围已结束。
     */
    public ScanPage {
        values = List.copyOf(values);
    }

    /**
     * 返回本页非删除的可见值。
     */
    @Override
    public List<VersionedValue> values() {
        return values;
    }

    /**
     * 返回下页的排他起点，空值表示扫描结束。
     */
    @Override
    public String nextKey() {
        return nextKey;
    }
}

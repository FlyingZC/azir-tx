package mvcc.wal;

import mvcc.storage.Utf8KeyOrder;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

/** 同一语句快照上的有限扫描页，游标为空表示没有后续页。 */
public record WalScanPage(NavigableMap<String, byte[]> rows, String nextAfterKey) {
    /** 复制页数据并固定键顺序，防止调用方修改内部候选集。 */
    public WalScanPage {
        var copy = new TreeMap<String, byte[]>(Utf8KeyOrder::compare);
        rows.forEach((key, value) -> copy.put(key, value.clone()));
        rows = Collections.unmodifiableNavigableMap(copy);
    }
}

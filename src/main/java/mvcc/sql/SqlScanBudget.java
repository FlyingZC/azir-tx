package mvcc.sql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.storage.Utf8KeyOrder;

/**
 * 一次查询内共享的输入扫描计数器，所有表、分片、枚举器及重扫共同消耗预算。
 * 计费只覆盖返回 SQL 层的输入行和字节，不度量存储内部访问量或 Calcite 中间对象的堆占用。
 */
final class SqlScanBudget {
    private final long txId;
    private final SqlExecutionOptions options;
    private long remainingRows;
    private long remainingBytes;
    private MvccException exhausted;

    /**
     * 为一次查询建立预算，调用方必须将同一实例交给该查询的全部表。
     */
    SqlScanBudget(long txId, SqlExecutionOptions options) {
        this.txId = txId;
        this.options = options;
        remainingRows = options.maxScannedRows();
        remainingBytes = options.maxScannedBytes();
    }

    /**
     * 在输入行解码和投影之前计费，使用剩余量比较避免累计值溢出。
     */
    synchronized void chargeRow(String physicalKey, byte[] value) {
        ensureAvailable();
        if (remainingRows == 0) {
            exhausted = new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "SQL input scan row budget exceeded: " + options.maxScannedRows(), false, txId);
            throw exhausted;
        }
        long bytes = (long) Utf8KeyOrder.encode(physicalKey).length + (value == null ? 0 : value.length);
        if (bytes > remainingBytes) {
            exhausted = new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                    "SQL input scan byte budget exceeded: " + options.maxScannedBytes(), false, txId);
            throw exhausted;
        }
        remainingRows--;
        remainingBytes -= bytes;
    }

    /**
     * 超限后所有枚举器持续失败，重置游标或创建另一个枚举器不能恢复预算。
     */
    synchronized void ensureAvailable() {
        if (exhausted != null) {
            throw exhausted;
        }
    }
}

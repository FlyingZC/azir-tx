package mvcc.sql;

import mvcc.backend.BackendReadView;
import mvcc.backend.BackendTransaction;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.storage.KeyRange;
import java.util.Optional;

/** 将公共后端固定语句视图转换为 Calcite 需要的端口。 */
final class BackendSqlReadPort implements SqlTransactionPort {
    private final BackendTransaction transaction;
    private final BackendReadView view;

    /** 借用当前语句视图，由 SQL 会话负责关闭。 */
    BackendSqlReadPort(BackendTransaction transaction, BackendReadView view) {
        this.transaction = transaction;
        this.view = view;
    }
    /** 仅将原始 startTs 用于诊断，不跨后端解释版本。 */
    @Override
    public long diagnosticTransactionId() { return transaction.id().startTs(); }
    /** 在同一语句边界读取一个键。 */
    @Override
    public Optional<byte[]> get(String shard, String key) { return view.get(shard, key); }
    /** 空值候选参与输入预算，游标保持原始键顺序。 */
    @Override
    public Page scanPage(String shard, KeyRange range, String after, int limit) {
        var page = view.scanPage(shard, range, after, limit);
        return new Page(page.rows().stream().map(row -> {
            byte[] value = row.value();
            return new KeyValue(row.key(), value, value == null);
        }).toList(), page.nextKey());
    }
    /** 当前语句查重，提交时仍须由原生内核裁决并发冲突。 */
    @Override
    public void insert(String shard, String key, byte[] value) {
        if (get(shard, key).isPresent()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "duplicate SQL key: " + key, false, diagnosticTransactionId());
        }
        put(shard, key, value);
    }
    /** 缓冲或执行同一业务事务的更新。 */
    @Override
    public void put(String shard, String key, byte[] value) { transaction.put(shard, key, value); }
    /** 将删除交给原生内核表达墓碑。 */
    @Override
    public void delete(String shard, String key) { transaction.delete(shard, key); }
}

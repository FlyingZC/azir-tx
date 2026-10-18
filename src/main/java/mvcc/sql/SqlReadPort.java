package mvcc.sql;

import mvcc.api.Transaction;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.storage.KeyRange;
import mvcc.txn.StatementReadSnapshot;
import mvcc.txn.TransactionManager;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Calcite 表只依赖同一语句切面内的点读与原始键分页，不解释后端提交时间。 */
interface SqlReadPort {
    /** SQL 表保留墓碑计费身份；只有未删除的业务值必须非空。 */
    record KeyValue(String key, byte[] value, boolean deleted) {
        /** 禁止后端在 Calcite 消费时修改当前页。 */
        public KeyValue {
            Objects.requireNonNull(key, "key");
            if (!deleted && value == null) {
                throw new IllegalArgumentException("visible SQL value cannot be null");
            }
            value = value == null ? null : value.clone();
        }

        /** 调用方取得独立字节数组。 */
        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }

    /** 空值页也保留来自已扫描原始键的排他游标。 */
    record Page(List<KeyValue> values, String nextKey) {
        /** 固定页内业务键顺序。 */
        public Page {
            values = List.copyOf(values);
        }
    }

    /** 返回用于错误与预算记录的进程内事务标识，不承担全局时间排序。 */
    long diagnosticTransactionId();

    /** 同一语句句柄内读取一个物理业务键。 */
    Optional<byte[]> get(String shardId, String key);

    /** 同一语句句柄内按键范围和排他游标读取一页。 */
    Page scanPage(String shardId, KeyRange range, String afterKeyExclusive, int limit);
}

/** SQL DML 在同一事务上缓冲或执行写入，读取仍沿用语句开始时的切面。 */
interface SqlTransactionPort extends SqlReadPort {
    /** 主键 INSERT 必须由后端保证不存在时才写入。 */
    void insert(String shardId, String key, byte[] value);

    /** 按已验证的物理路由写入非空 SQL value 字节。 */
    void put(String shardId, String key, byte[] value);

    /** 按已验证的物理路由删除业务键。 */
    void delete(String shardId, String key);
}

/** 现有 MySQL 事务管理器沿用原语句级时间戳与分页门禁。 */
final class MysqlSqlReadPort implements SqlTransactionPort {
    private final TransactionManager manager;
    private final Transaction transaction;
    private final StatementReadSnapshot snapshot;

    /** 只由已经创建语句快照的 SQL 执行器构造。 */
    MysqlSqlReadPort(TransactionManager manager, Transaction transaction,
                     StatementReadSnapshot snapshot) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.transaction = Objects.requireNonNull(transaction, "transaction");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    }

    /** MySQL 事务号仅供 SQL 诊断使用。 */
    @Override
    public long diagnosticTransactionId() {
        return transaction.txId();
    }

    /** 原有点读仍由事务管理器验证固定语句快照。 */
    @Override
    public Optional<byte[]> get(String shardId, String key) {
        return manager.get(transaction, shardId, key, snapshot);
    }

    /** 墓碑由 SQL 非空行模型过滤，原始分页游标保持不变。 */
    @Override
    public Page scanPage(String shardId, KeyRange range,
                         String afterKeyExclusive, int limit) {
        var source = manager.scanPage(transaction, shardId,
                range.startKeyInclusive(), range.endKeyExclusive(),
                snapshot, afterKeyExclusive, limit);
        return new Page(source.values().stream()
                .map(value -> new KeyValue(value.key(), value.value(), value.deleted()))
                .toList(), source.nextKey());
    }

    /** 原有 MySQL 缓冲写集合保持不变。 */
    @Override
    public void put(String shardId, String key, byte[] value) {
        manager.put(transaction, shardId, key, value);
    }

    /** 旧 MySQL 后端沿用语句快照存在性检查和缓冲写入。 */
    @Override
    public void insert(String shardId, String key, byte[] value) {
        if (get(shardId, key).isPresent()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "duplicate SQL key: " + key, false, transaction.txId());
        }
        put(shardId, key, value);
    }

    /** 原有 MySQL 删除墓碑语义保持不变。 */
    @Override
    public void delete(String shardId, String key) {
        manager.delete(transaction, shardId, key);
    }
}

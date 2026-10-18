package mvcc.sql;

import mvcc.api.Transaction;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.storage.KeyRange;
import mvcc.storage.Utf8KeyOrder;
import mvcc.txn.StatementReadSnapshot;
import mvcc.txn.TransactionManager;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 将同一语句快照暴露为可下推主键条件的分页表，仅在枚举器前进时读取存储。
 */
final class CalciteSnapshotTable extends AbstractTable implements ProjectableFilterableTable {
    private final SqlReadPort reads;
    private final String table;
    private final ShardResolver shardResolver;
    private final SqlStorageKeyCodec keyCodec;
    private final SqlValueCodec valueCodec;
    private final int pageSize;
    private final SqlScanBudget scanBudget;
    private final int rowLimit;

    /**
     * 绑定语句快照和编解码器，构造阶段不访问分片。
     */
    CalciteSnapshotTable(TransactionManager transactionManager, Transaction tx, StatementReadSnapshot snapshot,
                         String table, ShardResolver shardResolver, SqlStorageKeyCodec keyCodec,
                         SqlValueCodec valueCodec, int pageSize, SqlScanBudget scanBudget) {
        this(transactionManager, tx, snapshot, table, shardResolver, keyCodec, valueCodec,
                pageSize, scanBudget, Integer.MAX_VALUE);
    }

    /** 为无需排序或过滤的单表 LIMIT 查询设置安全的扫描行数上限。 */
    CalciteSnapshotTable(TransactionManager transactionManager, Transaction tx, StatementReadSnapshot snapshot,
                         String table, ShardResolver shardResolver, SqlStorageKeyCodec keyCodec,
                         SqlValueCodec valueCodec, int pageSize, SqlScanBudget scanBudget, int rowLimit) {
        this(new MysqlSqlReadPort(transactionManager, tx, snapshot), table,
                shardResolver, keyCodec, valueCodec, pageSize, scanBudget, rowLimit);
    }

    /** 原生事务可直接提供已固定的语句读端口，Calcite 无须解析后端提交时间。 */
    CalciteSnapshotTable(SqlReadPort reads, String table, ShardResolver shardResolver,
                         SqlStorageKeyCodec keyCodec, SqlValueCodec valueCodec,
                         int pageSize, SqlScanBudget scanBudget, int rowLimit) {
        this.reads = java.util.Objects.requireNonNull(reads, "reads");
        this.table = table;
        this.shardResolver = shardResolver;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.pageSize = pageSize;
        this.scanBudget = scanBudget;
        this.rowLimit = rowLimit;
    }

    /**
     * 保持原有非空 id/value 字符串表模型。
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("id", SqlTypeName.VARCHAR).add("value", SqlTypeName.VARCHAR).build();
    }

    /**
     * 选择一个能精确执行的主键条件，其余条件由 Calcite 继续计算。
     */
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        int scanRowLimit = filters.isEmpty() ? rowLimit : Integer.MAX_VALUE;
        KeyRestriction restriction = selectRestriction(filters);
        int[] projection = projects == null ? new int[]{0, 1} : projects.clone();
        AtomicBoolean cancelled = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<>() {
            /**
             * 每次枚举建立独立游标，JOIN 重扫仍使用原语句快照。
             */
            @Override
            public Enumerator<Object[]> enumerator() {
                return new PageEnumerator(restriction, projection, cancelled, scanRowLimit);
            }
        };
    }

    /**
     * 优先采用等值点查，否则采用首个安全范围；不合并或改写未接受的表达式。
     */
    private KeyRestriction selectRestriction(List<RexNode> filters) {
        KeyRestriction selected = null;
        RexNode accepted = null;
        for (RexNode filter : filters) {
            KeyRestriction candidate = decodeRestriction(filter);
            if (candidate != null && (selected == null || candidate.pointKey() != null)) {
                selected = candidate;
                accepted = filter;
                if (candidate.pointKey() != null) {
                    break;
                }
            }
        }
        if (accepted != null) {
            filters.remove(accepted);
        }
        return selected == null ? new KeyRestriction(null, keyCodec.rangeStart(table), keyCodec.rangeEnd(table)) : selected;
    }

    /**
     * 仅解码直接主键引用与字符串常量比较，拒绝 NULL、CAST、数值和排序规则不确定的条件。
     */
    private KeyRestriction decodeRestriction(RexNode filter) {
        if (!(filter instanceof RexCall call) || call.getOperands().size() != 2) {
            return null;
        }
        SqlKind kind = call.getKind();
        if (kind != SqlKind.EQUALS && kind != SqlKind.GREATER_THAN
                && kind != SqlKind.GREATER_THAN_OR_EQUAL && kind != SqlKind.LESS_THAN
                && kind != SqlKind.LESS_THAN_OR_EQUAL) {
            // IN 等条件也可能使用 RexLiteral 包装 SARG，不能按字符串字面量解码。
            return null;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (left instanceof RexLiteral && right instanceof RexInputRef) {
            RexNode swapped = left;
            left = right;
            right = swapped;
            kind = kind.reverse();
        }
        if (!(left instanceof RexInputRef ref) || ref.getIndex() != 0
                || !(right instanceof RexLiteral literal) || literal.isNull()
                || left.getType().getSqlTypeName() != SqlTypeName.VARCHAR
                || (right.getType().getSqlTypeName() != SqlTypeName.CHAR
                && right.getType().getSqlTypeName() != SqlTypeName.VARCHAR)
                || !usesStringOrder(left.getType()) || !usesStringOrder(right.getType())) {
            return null;
        }
        String id = literal.getValueAs(String.class);
        if (id == null || !StandardCharsets.UTF_8.newEncoder().canEncode(id)) {
            return null;
        }
        if (kind == SqlKind.EQUALS) {
            return new KeyRestriction(id, null, null);
        }
        // ASCII 边界与任意合法 UTF-8 键比较时和 String.compareTo 同序；非 ASCII 边界交给 Calcite。
        if (!id.chars().allMatch(character -> character < 128)) {
            return null;
        }
        String key = keyCodec.encode(table, id);
        return switch (kind) {
            case GREATER_THAN -> new KeyRestriction(null, key + '\u0000', keyCodec.rangeEnd(table));
            case GREATER_THAN_OR_EQUAL -> new KeyRestriction(null, key, keyCodec.rangeEnd(table));
            case LESS_THAN -> new KeyRestriction(null, keyCodec.rangeStart(table), key);
            case LESS_THAN_OR_EQUAL -> new KeyRestriction(null, keyCodec.rangeStart(table), key + '\u0000');
            default -> null;
        };
    }

    /**
     * 只有无需 Collator 的 Calcite 字符串比较才允许映射到二进制存储键。
     */
    private boolean usesStringOrder(RelDataType type) {
        return type.getCollation() != null && type.getCollation().getCollator() == null;
    }

    /**
     * 描述单键或左闭右开的物理扫描范围，点查键保留逻辑主键供路由使用。
     */
    private record KeyRestriction(String pointKey, String startKey, String endKey) {
    }

    /**
     * 每次仅持有一页可见值的枚举器，重扫复用原快照，LIMIT 关闭后不再请求页面。
     */
    private final class PageEnumerator implements Enumerator<Object[]> {
        private final KeyRestriction restriction;
        private final int[] projects;
        private final AtomicBoolean cancelled;
        private final int scanRowLimit;
        private final List<String> shards;
        private List<SqlReadPort.KeyValue> page = List.of();
        private int pageIndex;
        private int shardIndex;
        private String afterKey;
        private Object[] current;
        private boolean pointRead;
        private boolean closed;
        private int emittedRows;

        /**
         * 建立枚举状态；点查只解析唯一分片，范围扫描覆盖路由器声明的全部分片。
         */
        private PageEnumerator(KeyRestriction restriction, int[] projects, AtomicBoolean cancelled, int scanRowLimit) {
            this.restriction = restriction;
            this.projects = projects;
            this.cancelled = cancelled;
            this.scanRowLimit = scanRowLimit;
            this.shards = restriction.pointKey() == null
                    ? shardResolver.resolveShards(table).stream().distinct().toList()
                    : List.of(shardResolver.resolveShard(table, restriction.pointKey()));
        }

        /**
         * 返回当前投影行，未定位或已结束时禁止访问。
         */
        @Override
        public Object[] current() {
            if (current == null) {
                throw new NoSuchElementException("no current SQL row");
            }
            return current;
        }

        /**
         * 按消费需求推进一行，空可见页仍按原始扫描游标继续读取。
         */
        @Override
        public boolean moveNext() {
            current = null;
            if (closed) {
                return false;
            }
            if (emittedRows >= scanRowLimit) {
                return false;
            }
            try {
                scanBudget.ensureAvailable();
                if (cancelled != null && cancelled.get()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL query cancelled", false,
                            reads.diagnosticTransactionId());
                }
                if (restriction.pointKey() != null) {
                    if (!pointRead) {
                        pointRead = true;
                        String physicalKey = keyCodec.encode(table, restriction.pointKey());
                        var value = reads.get(shards.getFirst(), physicalKey);
                        if (value.isPresent()) {
                            scanBudget.chargeRow(physicalKey, value.get());
                            current = project(restriction.pointKey(), value.get());
                            emittedRows++;
                            return true;
                        }
                    }
                    return false;
                }
                while (true) {
                    if (cancelled != null && cancelled.get()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL query cancelled", false,
                                reads.diagnosticTransactionId());
                    }
                    if (pageIndex < page.size()) {
                        SqlReadPort.KeyValue value = page.get(pageIndex++);
                        if (!value.deleted()) {
                            current = project(keyCodec.decode(table, value.key()), value.value());
                            emittedRows++;
                            return true;
                        }
                    } else if (shardIndex < shards.size()) {
                        fetchPage();
                    } else {
                        page = List.of();
                        return false;
                    }
                }
            } catch (RuntimeException | Error ex) {
                close();
                throw ex;
            }
        }

        /**
         * 释放旧页面后请求下一页，并按 UTF-8 无符号字节序检查游标前进。
         */
        private void fetchPage() {
            page = List.of();
            pageIndex = 0;
            SqlReadPort.Page fetched = reads.scanPage(shards.get(shardIndex),
                    new KeyRange(restriction.startKey(), restriction.endKey()), afterKey, pageSize);
            String next = fetched.nextKey();
            if (next != null && afterKey != null && Utf8KeyOrder.compare(next, afterKey) <= 0) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "SQL scan cursor did not advance", false,
                        reads.diagnosticTransactionId());
            }
            // 整页在交给 Calcite 前计费，LIMIT 未消费的预取行和残余过滤丢弃的行也占用输入预算。
            for (SqlReadPort.KeyValue value : fetched.values()) {
                scanBudget.chargeRow(value.key(), value.value());
            }
            page = fetched.values();
            afterKey = next;
            if (next == null) {
                shardIndex++;
            }
        }

        /**
         * 只解码投影需要的值，避免主键查询与 COUNT 承担 value 字符串分配。
         */
        private Object[] project(String id, byte[] value) {
            Object[] result = new Object[projects.length];
            String decodedValue = null;
            for (int index = 0; index < projects.length; index++) {
                if (projects[index] == 0) {
                    result[index] = id;
                } else if (projects[index] == 1) {
                    if (decodedValue == null) {
                        decodedValue = valueCodec.decode(value);
                    }
                    result[index] = decodedValue;
                } else {
                    throw new IllegalArgumentException("unknown SQL column: " + projects[index]);
                }
            }
            return result;
        }

        /**
         * 为需要重扫的关系运算重置游标；沿用原语句快照且不会在重置时访问存储。
         */
        @Override
        public void reset() {
            if (closed) {
                throw new IllegalStateException("SQL scan enumerator is closed");
            }
            page = List.of();
            pageIndex = 0;
            shardIndex = 0;
            afterKey = null;
            current = null;
            pointRead = false;
            emittedRows = 0;
        }

        /**
         * 幂等释放当前页与当前行；每次分页调用已由事务层关闭底层存储资源。
         */
        @Override
        public void close() {
            closed = true;
            page = List.of();
            current = null;
        }
    }
}

package mvcc.sql;

import mvcc.api.Transaction;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.txn.StatementReadSnapshot;
import mvcc.txn.TransactionManager;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Calcite 关系计划执行器，把 MVCC 快照行暴露为 Calcite 表并把 DML 变更写回事务核心。
 */
final class CalciteSqlExecutor {
    private final TransactionManager transactionManager;
    private final ShardResolver shardResolver;
    private final SqlValueCodec valueCodec;
    private final SqlStorageKeyCodec keyCodec;
    private final SqlExecutionOptions options;

    /**
     * 构造 Calcite SQL 执行器。
     */
    CalciteSqlExecutor(TransactionManager transactionManager,
                       ShardResolver shardResolver,
                       SqlValueCodec valueCodec,
                       SqlStorageKeyCodec keyCodec,
                       SqlExecutionOptions options) {
        this.transactionManager = transactionManager;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.keyCodec = keyCodec;
        this.options = options;
    }

    /**
     * 在指定事务中执行 Calcite 查询或 DML 计划。
     */
    SqlExecutionResult execute(Transaction tx, SqlPlan.CalcitePlan plan, StatementReadSnapshot snapshot) {
        return switch (plan.statementType()) {
            case SELECT -> SqlExecutionResult.rows(executeQuery(tx, plan.sql(), plan.tableNames(), snapshot).sqlRows());
            case INSERT -> executeInsert(tx, plan, snapshot);
            case UPDATE -> executeUpdate(tx, plan, snapshot);
            case DELETE -> executeDelete(tx, plan, snapshot);
        };
    }

    /**
     * 执行 INSERT VALUES 或 INSERT SELECT，并按目标列顺序写入 MVCC。
     */
    private SqlExecutionResult executeInsert(Transaction tx, SqlPlan.CalcitePlan plan, StatementReadSnapshot snapshot) {
        SqlInsert insert = parse(plan.sql(), SqlInsert.class);
        String table = tableName(insert.getTargetTable());
        List<String> targetColumns = columnNames(insert.getTargetColumnList());
        if (targetColumns.isEmpty()) {
            targetColumns = List.of("id", "value");
        }
        validateInsertColumns(targetColumns);
        QueryData source = executeQuery(tx, insertSourceSql(insert.getSource()), plan.tableNames(), snapshot);
        int idIndex = targetColumns.indexOf("id");
        int valueIndex = targetColumns.indexOf("value");
        Set<String> insertedIds = new HashSet<>();
        // 整句验证完成后才写入，跨行重复键和 NULL 不会造成已执行前缀残留。
        for (List<String> row : source.values()) {
            if (row.size() != targetColumns.size()) {
                throw illegal("INSERT source column count does not match target columns");
            }
            String id = requiredValue(row.get(idIndex), "id");
            requiredValue(row.get(valueIndex), "value");
            if (!insertedIds.add(id)) {
                throw illegal("duplicate key: " + id);
            }
            ensureKeyAbsent(tx, table, id, snapshot);
        }
        for (List<String> row : source.values()) {
            put(tx, table, row.get(idIndex), row.get(valueIndex));
        }
        return SqlExecutionResult.updateCount(source.values().size());
    }

    /**
     * 执行支持表达式和过滤条件的 UPDATE，并处理主键变化。
     */
    private SqlExecutionResult executeUpdate(Transaction tx, SqlPlan.CalcitePlan plan, StatementReadSnapshot snapshot) {
        SqlUpdate update = parse(plan.sql(), SqlUpdate.class);
        String table = tableName(update.getTargetTable());
        List<String> targetColumns = columnNames(update.getTargetColumnList());
        validateTargetColumns(targetColumns);
        String newId = "id";
        String newValue = "value";
        for (int index = 0; index < targetColumns.size(); index++) {
            String expression = toSql(update.getSourceExpressionList().get(index));
            if (targetColumns.get(index).equals("id")) {
                newId = expression;
            } else {
                newValue = expression;
            }
        }
        String alias = update.getAlias() == null ? "" : " AS " + toSql(update.getAlias());
        String condition = update.getCondition() == null ? "" : " WHERE " + toSql(update.getCondition());
        String selection = "SELECT id AS __old_id, " + newId + " AS __new_id, " + newValue
                + " AS __new_value FROM " + toSql(update.getTargetTable()) + alias + condition;
        QueryData rows = executeQuery(tx, selection, plan.tableNames(), snapshot);
        List<UpdatedRow> updates = rows.values().stream()
                .map(row -> new UpdatedRow(
                        requiredValue(row.get(0), "old id"),
                        requiredValue(row.get(1), "new id"),
                        requiredValue(row.get(2), "new value")
                ))
                .toList();
        validateUpdatedKeys(tx, table, updates, snapshot);
        // 先删除所有迁移的旧键，再写入新键，避免主键交换时删除刚写入的另一行。
        for (UpdatedRow updateRow : updates) {
            if (!updateRow.oldId().equals(updateRow.newId())) {
                delete(tx, table, updateRow.oldId());
            }
        }
        for (UpdatedRow updateRow : updates) {
            put(tx, table, updateRow.newId(), updateRow.newValue());
        }
        return SqlExecutionResult.updateCount(updates.size());
    }

    /**
     * 执行带 Calcite 表达式过滤条件的 DELETE。
     */
    private SqlExecutionResult executeDelete(Transaction tx, SqlPlan.CalcitePlan plan, StatementReadSnapshot snapshot) {
        SqlDelete delete = parse(plan.sql(), SqlDelete.class);
        String table = tableName(delete.getTargetTable());
        String alias = delete.getAlias() == null ? "" : " AS " + toSql(delete.getAlias());
        String condition = delete.getCondition() == null ? "" : " WHERE " + toSql(delete.getCondition());
        QueryData rows = executeQuery(
                tx,
                "SELECT id FROM " + toSql(delete.getTargetTable()) + alias + condition,
                plan.tableNames(),
                snapshot
        );
        for (List<String> row : rows.values()) {
            delete(tx, table, requiredValue(row.getFirst(), "id"));
        }
        return SqlExecutionResult.updateCount(rows.values().size());
    }

    /**
     * 注册不预读数据的分页表，以调用方传入的语句快照执行关系运算。
     */
    private QueryData executeQuery(Transaction tx, String sql, List<String> tableNames, StatementReadSnapshot snapshot) {
        SqlScanBudget scanBudget = new SqlScanBudget(tx.txId(), options);
        Properties properties = new Properties();
        properties.setProperty("lex", Lex.MYSQL.name());
        properties.setProperty("caseSensitive", "false");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            for (String table : tableNames) {
                rootSchema.add(table, new CalciteSnapshotTable(transactionManager, tx, snapshot, table,
                        shardResolver, keyCodec, valueCodec, options.scanPageSize(), scanBudget));
            }
            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
                return readResultSet(resultSet, tx);
            }
        } catch (Exception ex) {
            // Calcite/JDBC 可能多层包装枚举器异常，保留快照失效等原始 MVCC 错误语义。
            for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
                if (cause instanceof MvccException mvccException) {
                    throw mvccException;
                }
            }
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "Calcite execution failed: " + sql, false, tx.txId(), ex);
        }
    }

    /**
     * 把 JDBC 结果集转换为保序的 SQL 行和值矩阵。
     */
    private QueryData readResultSet(ResultSet resultSet, Transaction tx) throws Exception {
        ResultSetMetaData metadata = resultSet.getMetaData();
        List<String> labels = new ArrayList<>();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            labels.add(metadata.getColumnLabel(index));
        }
        List<SqlRow> sqlRows = new ArrayList<>();
        List<List<String>> values = new ArrayList<>();
        long remainingBytes = options.maxResultBytes();
        while (resultSet.next()) {
            if (sqlRows.size() >= options.maxResultRows()) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "SQL result row budget exceeded: " + options.maxResultRows(), false, tx.txId());
            }
            Map<String, String> columns = new LinkedHashMap<>();
            List<String> rowValues = new ArrayList<>();
            for (int index = 1; index <= labels.size(); index++) {
                Object value = resultSet.getObject(index);
                String stringValue = value == null ? null : String.valueOf(value);
                if (stringValue != null) {
                    remainingBytes -= stringValue.getBytes(StandardCharsets.UTF_8).length;
                    if (remainingBytes < 0) {
                        throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                                "SQL result byte budget exceeded: " + options.maxResultBytes(), false, tx.txId());
                    }
                }
                columns.put(labels.get(index - 1), stringValue);
                rowValues.add(stringValue);
            }
            sqlRows.add(new SqlRow(columns));
            values.add(Collections.unmodifiableList(new ArrayList<>(rowValues)));
        }
        return new QueryData(sqlRows, values);
    }

    /**
     * 校验 UPDATE 后的新主键在结果集合内唯一，且不会覆盖集合外已有行。
     */
    private void validateUpdatedKeys(Transaction tx, String table, List<UpdatedRow> updates, StatementReadSnapshot snapshot) {
        Set<String> oldIds = updates.stream().map(UpdatedRow::oldId).collect(java.util.stream.Collectors.toSet());
        Set<String> newIds = new HashSet<>();
        for (UpdatedRow row : updates) {
            if (!newIds.add(row.newId())) {
                throw illegal("duplicate key after UPDATE: " + row.newId());
            }
            if (!oldIds.contains(row.newId()) && exists(tx, table, row.newId(), snapshot)) {
                throw illegal("duplicate key after UPDATE: " + row.newId());
            }
        }
    }

    /**
     * 校验 INSERT 目标主键当前不存在。
     */
    private void ensureKeyAbsent(Transaction tx, String table, String id, StatementReadSnapshot snapshot) {
        if (exists(tx, table, id, snapshot)) {
            throw illegal("duplicate key: " + id);
        }
    }

    /**
     * 判断逻辑表主键在当前事务视图中是否存在。
     */
    private boolean exists(Transaction tx, String table, String id, StatementReadSnapshot snapshot) {
        String shardId = shardResolver.resolveShard(table, id);
        return transactionManager.get(tx, shardId, keyCodec.encode(table, id), snapshot).isPresent();
    }

    /**
     * 写入逻辑表行。
     */
    private void put(Transaction tx, String table, String id, String value) {
        String shardId = shardResolver.resolveShard(table, id);
        transactionManager.put(tx, shardId, keyCodec.encode(table, id), valueCodec.encode(value));
    }

    /**
     * 删除逻辑表行并保留 MVCC tombstone 语义。
     */
    private void delete(Transaction tx, String table, String id) {
        String shardId = shardResolver.resolveShard(table, id);
        transactionManager.delete(tx, shardId, keyCodec.encode(table, id));
    }

    /**
     * 使用与计划器一致的词法配置解析指定 Calcite DML 节点。
     */
    private <T extends SqlNode> T parse(String sql, Class<T> expectedType) {
        try {
            SqlNode parsed = SqlParser.create(
                    sql,
                    SqlParser.config().withLex(Lex.MYSQL).withCaseSensitive(false)
            ).parseStmt();
            return expectedType.cast(parsed);
        } catch (Exception ex) {
            throw illegal("cannot parse planned SQL: " + sql, ex);
        }
    }

    /**
     * 提取 DML 目标表的简单名称。
     */
    private String tableName(SqlNode targetTable) {
        if (!(targetTable instanceof SqlIdentifier identifier)) {
            throw illegal("only simple DML target table is supported: " + targetTable);
        }
        return identifier.names.getLast().toLowerCase(Locale.ROOT);
    }

    /**
     * 提取并规范化目标列名。
     */
    private List<String> columnNames(SqlNodeList columns) {
        if (columns == null) {
            return List.of();
        }
        return columns.stream().map(column -> {
            if (!(column instanceof SqlIdentifier identifier)) {
                throw illegal("invalid target column: " + column);
            }
            return identifier.names.getLast().toLowerCase(Locale.ROOT);
        }).toList();
    }

    /**
     * 校验 DML 只修改当前物理模型支持的 id/value 列并且两列恰好出现一次。
     */
    private void validateTargetColumns(List<String> columns) {
        Set<String> distinct = new HashSet<>(columns);
        if (distinct.size() != columns.size() || !Set.of("id", "value").containsAll(columns)) {
            throw illegal("only id and value columns are supported: " + columns);
        }
        if (columns.isEmpty()) {
            throw illegal("DML target columns must not be empty");
        }
    }

    /**
     * 校验 INSERT 必须同时提供当前行模型中的 id 和 value 两列。
     */
    private void validateInsertColumns(List<String> columns) {
        validateTargetColumns(columns);
        if (!new HashSet<>(columns).equals(Set.of("id", "value"))) {
            throw illegal("INSERT must provide both id and value columns: " + columns);
        }
    }

    /**
     * 使用 MySQL 方言把 Calcite 节点转回可执行 SQL 片段。
     */
    private String toSql(SqlNode node) {
        return node.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
    }

    /**
     * 为多行 VALUES 的每个字符表达式显式转换为 VARCHAR，避免 Calcite 按最长 CHAR 自动补空格。
     */
    private String insertSourceSql(SqlNode source) {
        if (source.getKind() != SqlKind.VALUES || !(source instanceof SqlCall valuesCall)) {
            return toSql(source);
        }
        List<String> selects = new ArrayList<>();
        for (SqlNode rowNode : valuesCall.getOperandList()) {
            if (!(rowNode instanceof SqlCall rowCall) || rowCall.getKind() != SqlKind.ROW) {
                throw illegal("invalid VALUES row: " + rowNode);
            }
            String projections = rowCall.getOperandList().stream()
                    .map(expression -> "CAST(" + toSql(expression) + " AS VARCHAR)")
                    .collect(java.util.stream.Collectors.joining(", "));
            selects.add("SELECT " + projections);
        }
        return String.join(" UNION ALL ", selects);
    }

    /**
     * 校验写入模型不接受 SQL NULL 的必填值。
     */
    private String requiredValue(String value, String column) {
        if (value == null) {
            throw illegal(column + " must not be null");
        }
        return value;
    }

    /**
     * 创建统一 SQL 执行异常。
     */
    private MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * 创建带根因的统一 SQL 执行异常。
     */
    private MvccException illegal(String message, Exception cause) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null, cause);
    }

    /**
     * 内部查询结果，同时保留列标签映射和按位置访问的数据。
     */
    private record QueryData(List<SqlRow> sqlRows, List<List<String>> values) {
    }

    /**
     * UPDATE 计算后的旧主键、新主键和值。
     */
    private record UpdatedRow(String oldId, String newId, String newValue) {
    }
}

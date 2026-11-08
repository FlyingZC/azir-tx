/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.sql;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.txn.StatementReadSnapshot;
import com.flyingzc.azir.txn.TransactionManager;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
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
 * Calcite Relationship Scheme Executor, exposes the MVCC snapshot line to Calcite Table and writes the DML change back to the transaction core.
 */
final class CalciteSqlExecutor {

    private final TransactionManager transactionManager;

    private final ShardResolver shardResolver;

    private final SqlValueCodec valueCodec;

    private final SqlStorageKeyCodec keyCodec;

    private final SqlExecutionOptions options;

    /**
     * Construct the Calcite SQL implementer.
     *
     * @param transactionManager transaction manager
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param keyCodec key codec
     * @param options options
     */
    CalciteSqlExecutor(final TransactionManager transactionManager,
                       final ShardResolver shardResolver,
                       final SqlValueCodec valueCodec,
                       final SqlStorageKeyCodec keyCodec,
                       final SqlExecutionOptions options) {
        this.transactionManager = transactionManager;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.keyCodec = keyCodec;
        this.options = options;
    }

    /**
     * Original backend reuses Calcite relationship plan without binding the old control face transaction manager.
     *
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param keyCodec key codec
     * @param options options
     */
    CalciteSqlExecutor(final ShardResolver shardResolver, final SqlValueCodec valueCodec,
                       final SqlStorageKeyCodec keyCodec, final SqlExecutionOptions options) {
        this(null, shardResolver, valueCodec, keyCodec, options);
    }

    /**
     * Execute Calcite Query or DML schedule in the transaction specified.
     *
     * @param tx transaction context
     * @param plan plan
     * @param snapshot snapshot
     * @return the operation result
     */
    SqlExecutionResult execute(final Transaction tx, final SqlPlan.CalcitePlan plan, final StatementReadSnapshot snapshot) {
        if (transactionManager == null) {
            throw illegal("MySQL Calcite execution requires its transaction manager");
        }
        return execute(new MysqlSqlReadPort(transactionManager, tx, snapshot), plan);
    }

    /**
     * SQL plans to rely only on the fixed read snapshot and same transaction writing capabilities of the current statement port.
     *
     * @param port port
     * @param plan plan
     * @return the operation result
     */
    SqlExecutionResult execute(final SqlTransactionPort port, final SqlPlan.CalcitePlan plan) {
        return switch (plan.statementType()) {
            case SELECT -> SqlExecutionResult.rows(executeQuery(port, plan.sql(), plan.tableNames()).sqlRows());
            case INSERT -> executeInsert(port, plan);
            case UPDATE -> executeUpdate(port, plan);
            case DELETE -> executeDelete(port, plan);
        };
    }

    /**
     * Execute INSERT VALUES or INSERT SELECT and write to MVCC in the order of the target line.
     *
     * @param port port
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeInsert(final SqlTransactionPort port, final SqlPlan.CalcitePlan plan) {
        SqlInsert insert = parse(plan.sql(), SqlInsert.class);
        String table = tableName(insert.getTargetTable());
        List<String> targetColumns = columnNames(insert.getTargetColumnList());
        if (targetColumns.isEmpty()) {
            targetColumns = List.of("id", "value");
        }
        validateInsertColumns(targetColumns);
        QueryData source = executeQuery(port, insertSourceSql(insert.getSource()), plan.tableNames());
        int idIndex = targetColumns.indexOf("id");
        int valueIndex = targetColumns.indexOf("value");
        Set<String> insertedIds = new HashSet<>();
        // The entire sentence verify is not written after completion, cross-line repetition keys and NULL do not result in executed prefix residues.
        for (List<String> row : source.values()) {
            if (row.size() != targetColumns.size()) {
                throw illegal("INSERT source column count does not match target columns");
            }
            String id = requiredValue(row.get(idIndex), "id");
            requiredValue(row.get(valueIndex), "value");
            if (!insertedIds.add(id)) {
                throw illegal("duplicate key: " + id);
            }
            ensureKeyAbsent(port, table, id);
        }
        for (List<String> row : source.values()) {
            put(port, table, row.get(idIndex), row.get(valueIndex));
        }
        return SqlExecutionResult.updateCount(source.values().size());
    }

    /**
     * Executes UPDATEs that support expressions and filter conditions, and handle changes in the primary key.
     *
     * @param port port
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeUpdate(final SqlTransactionPort port, final SqlPlan.CalcitePlan plan) {
        SqlUpdate update = parse(plan.sql(), SqlUpdate.class);
        String table = tableName(update.getTargetTable());
        List<String> targetColumns = columnNames(update.getTargetColumnList());
        validateTargetColumns(targetColumns);
        String newId = "id";
        String newValue = "`value`";
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
        QueryData rows = executeQuery(port, selection, plan.tableNames());
        List<UpdatedRow> updates = rows.values().stream()
                .map(row -> new UpdatedRow(
                        requiredValue(row.get(0), "old id"),
                        requiredValue(row.get(1), "new id"),
                        requiredValue(row.get(2), "new value")))
                .toList();
        validateUpdatedKeys(port, table, updates);
        // Deletes all old moving keys, then writes new keys and avoids the deletion of another line that you just wrote when the primary key is exchanged.
        for (UpdatedRow updateRow : updates) {
            if (!updateRow.oldId().equals(updateRow.newId())) {
                delete(port, table, updateRow.oldId());
            }
        }
        for (UpdatedRow updateRow : updates) {
            put(port, table, updateRow.newId(), updateRow.newValue());
        }
        return SqlExecutionResult.updateCount(updates.size());
    }

    /**
     * Executes a DelETE with Calcite expression filtering conditions.
     *
     * @param port port
     * @param plan plan
     * @return the operation result
     */
    private SqlExecutionResult executeDelete(final SqlTransactionPort port, final SqlPlan.CalcitePlan plan) {
        SqlDelete delete = parse(plan.sql(), SqlDelete.class);
        String table = tableName(delete.getTargetTable());
        String alias = delete.getAlias() == null ? "" : " AS " + toSql(delete.getAlias());
        String condition = delete.getCondition() == null ? "" : " WHERE " + toSql(delete.getCondition());
        QueryData rows = executeQuery(
                port,
                "SELECT id FROM " + toSql(delete.getTargetTable()) + alias + condition,
                plan.tableNames());
        for (List<String> row : rows.values()) {
            delete(port, table, requiredValue(row.getFirst(), "id"));
        }
        return SqlExecutionResult.updateCount(rows.values().size());
    }

    /**
     * Registers a page break table that does not preread the data to execute a relationship operation for a statement snapshot that is sent by the caller.
     *
     * @param port port
     * @param sql sql
     * @param tableNames table names
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private QueryData executeQuery(final SqlTransactionPort port, final String sql, final List<String> tableNames) {
        SqlScanBudget scanBudget = new SqlScanBudget(port.diagnosticTransactionId(), options);
        int simpleLimit = simpleScanLimit(sql, tableNames);
        Properties properties = new Properties();
        properties.setProperty("lex", Lex.MYSQL.name());
        properties.setProperty("caseSensitive", "false");
        properties.setProperty("fun", "standard,mysql");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            for (String table : tableNames) {
                rootSchema.add(table, new CalciteSnapshotTable(port, table,
                        shardResolver, keyCodec, valueCodec, options.scanPageSize(), scanBudget, simpleLimit));
            }
            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
                return readResultSet(resultSet, port);
            }
        } catch (final Exception ex) {
            // Calcite/JDBC may contain multiple layers of anarchy and keep original MVCC hyphens such as snapshot invalids.
            for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
                if (cause instanceof MvccException mvccException) {
                    throw mvccException;
                }
            }
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "Calcite execution failed: " + sql,
                    false, port.diagnosticTransactionId(), ex);
        }
    }

    /**
     * Just push LIMIT for a simple projection of a single table, unfiltered sorting and aggregate to avoid opening the follow-up shard when JDBC takes an extra line.
     *
     * @param sql sql
     * @param tableNames table names
     * @return the operation result
     */
    private int simpleScanLimit(final String sql, final List<String> tableNames) {
        if (tableNames.size() != 1 || !sql.toLowerCase(Locale.ROOT).contains("limit")) {
            return Integer.MAX_VALUE;
        }
        try {
            SqlNode parsed = SqlParser.create(sql, SqlParser.config().withLex(Lex.MYSQL)
                    .withCaseSensitive(false)).parseStmt();
            SqlNode fetch;
            SqlSelect select;
            if (parsed instanceof SqlOrderBy orderBy) {
                if (orderBy.offset != null || orderBy.orderList != null && !orderBy.orderList.isEmpty()
                        || !(orderBy.query instanceof SqlSelect inner)) {
                    return Integer.MAX_VALUE;
                }
                fetch = orderBy.fetch;
                select = inner;
            } else if (parsed instanceof SqlSelect direct) {
                fetch = direct.getFetch();
                select = direct;
            } else {
                return Integer.MAX_VALUE;
            }
            if (!(fetch instanceof SqlLiteral literal) || select.isDistinct()
                    || !(select.getFrom() instanceof SqlIdentifier from)
                    || !from.names.getLast().equalsIgnoreCase(tableNames.getFirst())
                    || select.getWhere() != null || select.getHaving() != null || select.getQualify() != null
                    || select.getOffset() != null || select.hasOrderBy()
                    || select.getGroup() != null && !select.getGroup().isEmpty()
                    || select.getWindowList() != null && !select.getWindowList().isEmpty()
                    || !select.getSelectList().stream().allMatch(column -> column instanceof SqlIdentifier)) {
                return Integer.MAX_VALUE;
            }
            long limit = literal.longValue(true);
            return limit >= 0 && limit <= Integer.MAX_VALUE ? (int) limit : Integer.MAX_VALUE;
        } catch (final RuntimeException | org.apache.calcite.sql.parser.SqlParseException ex) {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * Converts the JDBC result set to a sequenced SQL line and value matrix.
     *
     * @param resultSet result set
     * @param port port
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private QueryData readResultSet(final ResultSet resultSet, final SqlTransactionPort port) throws Exception {
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
                        "SQL result row budget exceeded: " + options.maxResultRows(), false,
                        port.diagnosticTransactionId());
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
                                "SQL result byte budget exceeded: " + options.maxResultBytes(), false,
                                port.diagnosticTransactionId());
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
     * The new primary key after validate UPDATE is the only one in the result set and does not overwhelm existing lines outside the assembly.
     *
     * @param port port
     * @param table table
     * @param updates updates
     */
    private void validateUpdatedKeys(final SqlTransactionPort port, final String table, final List<UpdatedRow> updates) {
        Set<String> oldIds = updates.stream().map(UpdatedRow::oldId).collect(java.util.stream.Collectors.toSet());
        Set<String> newIds = new HashSet<>();
        for (UpdatedRow row : updates) {
            if (!newIds.add(row.newId())) {
                throw illegal("duplicate key after UPDATE: " + row.newId());
            }
            if (!oldIds.contains(row.newId()) && exists(port, table, row.newId())) {
                throw illegal("duplicate key after UPDATE: " + row.newId());
            }
        }
    }

    /**
     * Validate INSERT target primary key does not currently exist.
     *
     * @param port port
     * @param table table
     * @param id id
     */
    private void ensureKeyAbsent(final SqlTransactionPort port, final String table, final String id) {
        if (exists(port, table, id)) {
            throw illegal("duplicate key: " + id);
        }
    }

    /**
     * Determines whether the logical table primary key exists in the current transaction view.
     *
     * @param port port
     * @param table table
     * @param id id
     * @return the operation result
     */
    private boolean exists(final SqlTransactionPort port, final String table, final String id) {
        String shardId = shardResolver.resolveShard(table, id);
        return port.get(shardId, keyCodec.encode(table, id)).isPresent();
    }

    /**
     * Writes the logical table row.
     *
     * @param port port
     * @param table table
     * @param id id
     * @param value value to write
     */
    private void put(final SqlTransactionPort port, final String table, final String id, final String value) {
        String shardId = shardResolver.resolveShard(table, id);
        port.put(shardId, keyCodec.encode(table, id), valueCodec.encode(value));
    }

    /**
     * Deletes the loglist row and maintains the MVCC tombstone syntax.
     *
     * @param port port
     * @param table table
     * @param id id
     */
    private void delete(final SqlTransactionPort port, final String table, final String id) {
        String shardId = shardResolver.resolveShard(table, id);
        port.delete(shardId, keyCodec.encode(table, id));
    }

    /**
     * Parsing specifies the Calcite DML node using terms configuration consistent with the planner.
     *
     * @param <T> value type
     * @param sql sql
     * @param expectedType expected type
     * @return the operation result
     */
    private <T extends SqlNode> T parse(final String sql, final Class<T> expectedType) {
        try {
            SqlNode parsed = SqlParser.create(
                    sql,
                    SqlParser.config().withLex(Lex.MYSQL).withCaseSensitive(false)).parseStmt();
            return expectedType.cast(parsed);
        } catch (final Exception ex) {
            throw illegal("cannot parse planned SQL: " + sql, ex);
        }
    }

    /**
     * Extracts a simple name for the DML destination table.
     *
     * @param targetTable target table
     * @return the operation result
     */
    private String tableName(final SqlNode targetTable) {
        if (!(targetTable instanceof SqlIdentifier identifier)) {
            throw illegal("only simple DML target table is supported: " + targetTable);
        }
        return identifier.names.getLast().toLowerCase(Locale.ROOT);
    }

    /**
     * To extract and regularize the target listing.
     *
     * @param columns columns
     * @return the operation result
     */
    private List<String> columnNames(final SqlNodeList columns) {
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
     * Validate DML only changes the id/value column supported by the current physical model and both columns happen once.
     *
     * @param columns columns
     */
    private void validateTargetColumns(final List<String> columns) {
        Set<String> distinct = new HashSet<>(columns);
        if (distinct.size() != columns.size() || !Set.of("id", "value").containsAll(columns)) {
            throw illegal("only id and value columns are supported: " + columns);
        }
        if (columns.isEmpty()) {
            throw illegal("DML target columns must not be empty");
        }
    }

    /**
     * Validate INSERT must supply both id and value columns in the current line model.
     *
     * @param columns columns
     */
    private void validateInsertColumns(final List<String> columns) {
        validateTargetColumns(columns);
        if (!new HashSet<>(columns).equals(Set.of("id", "value"))) {
            throw illegal("INSERT must provide both id and value columns: " + columns);
        }
    }

    /**
     * Use MySQL dialect to return Calcite node to an executable SQL clip.
     *
     * @param node node
     * @return the operation result
     */
    private String toSql(final SqlNode node) {
        return node.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
    }

    /**
     * Converts each character expression for multiple rows of VALUES to VARCHAR, avoiding Calcite pressing the maximum CHAR autofilling spaces.
     *
     * @param source database connection source
     * @return the operation result
     */
    private String insertSourceSql(final SqlNode source) {
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
     * Validate Writing Model does not accept SQL NULL 's required fill.
     *
     * @param value value to write
     * @param column column
     * @return the operation result
     */
    private String requiredValue(final String value, final String column) {
        if (value == null) {
            throw illegal(column + " must not be null");
        }
        return value;
    }

    /**
     * Creates a uniform SQL execution anomaly.
     *
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * Creates a unified SQL execution anomaly with root causes.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private MvccException illegal(final String message, final Exception cause) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null, cause);
    }

    /**
     * Internal query results, while retaining column tab mapping and location-based access data.
     *
     * @param sqlRows sql rows
     * @param values values
     */
    private record QueryData(List<SqlRow> sqlRows, List<List<String>> values) {
    }

    /**
     * UPDATE calculates the old primary key, new primary key and value.
     *
     * @param oldId old id
     * @param newId new id
     * @param newValue new value
     */
    private record UpdatedRow(String oldId, String newId, String newValue) {
    }
}

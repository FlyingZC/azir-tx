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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * SQL programr based on Apache Calcite, responsible for syntax resolution, relationship type validate and logical plan generation.
 */
public final class CalciteSqlPlanner implements SqlPlanner {

    private static final Pattern BEGIN = Pattern.compile("^\\s*BEGIN\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern BEGIN_RC = Pattern.compile(
            "^\\s*BEGIN\\s+(?:TRANSACTION\\s+ISOLATION\\s+LEVEL\\s+)?READ\\s+COMMITTED\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern BEGIN_RR = Pattern.compile(
            "^\\s*BEGIN\\s+(?:TRANSACTION\\s+ISOLATION\\s+LEVEL\\s+)?REPEATABLE\\s+READ\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern COMMIT = Pattern.compile("^\\s*COMMIT\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern ROLLBACK = Pattern.compile("^\\s*ROLLBACK\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * Use Calcite to parse, validate and generate SQL plans.
     *
     * @param sql sql
     * @return the operation result
     */
    @Override
    public SqlPlan plan(final String sql) {
        if (sql == null || sql.isBlank()) {
            throw unsupported(sql, null);
        }
        SqlPlan transactionPlan = transactionPlan(sql);
        if (transactionPlan != null) {
            return transactionPlan;
        }
        try {
            String calciteSql = quoteValueIdentifier(sql);
            SqlNode firstParsed = SqlParser.create(calciteSql, parserConfig()).parseStmt();
            CalciteStatementType statementType = statementType(firstParsed.getKind());
            Set<String> candidates = collectIdentifierCandidates(firstParsed);
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            for (String candidate : candidates) {
                rootSchema.add(candidate, new PlanningTable(candidate));
            }
            FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .parserConfig(parserConfig())
                    .operatorTable(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                            SqlLibrary.STANDARD, SqlLibrary.MYSQL))
                    .build();
            Planner planner = Frameworks.getPlanner(frameworkConfig);
            SqlNode parsed = planner.parse(calciteSql);
            SqlNode validated = planner.validate(parsed);
            List<String> tableNames = resolveTableNames(planner, validated, statementType, candidates);
            return new SqlPlan.CalcitePlan(calciteSql, statementType, tableNames);
        } catch (final Exception ex) {
            throw unsupported(sql, ex);
        }
    }

    /**
     * Recognizes the transaction control statement that Calcite does not own.
     *
     * @param sql sql
     * @return the operation result
     */
    private SqlPlan transactionPlan(final String sql) {
        if (BEGIN.matcher(sql).matches() || BEGIN_RC.matcher(sql).matches()) {
            return new SqlPlan.BeginTransactionPlan(IsolationLevel.READ_COMMITTED);
        }
        if (BEGIN_RR.matcher(sql).matches()) {
            return new SqlPlan.BeginTransactionPlan(IsolationLevel.REPEATABLE_READ);
        }
        if (COMMIT.matcher(sql).matches()) {
            return new SqlPlan.CommitTransactionPlan();
        }
        if (ROLLBACK.matcher(sql).matches()) {
            return new SqlPlan.RollbackTransactionPlan();
        }
        return null;
    }

    /**
     * Creates a resolution configuration that is compatible with common MySQL identifiers and case habits.
     *
     * @return the operation result
     */
    private SqlParser.Config parserConfig() {
        return SqlParser.config().withLex(Lex.MYSQL).withCaseSensitive(false);
    }

    /**
     * Automatically transposes the value column in the existing SQL model to meet Calcite's syntax rule for using VALUE as a word retention.
     *
     * @param sql sql
     * @return the operation result
     */
    private String quoteValueIdentifier(final String sql) {
        StringBuilder result = new StringBuilder(sql.length() + 16);
        boolean singleQuoted = false;
        boolean backtickQuoted = false;
        for (int index = 0; index < sql.length();) {
            char current = sql.charAt(index);
            if (current == '\'' && !backtickQuoted) {
                result.append(current);
                if (singleQuoted && index + 1 < sql.length() && sql.charAt(index + 1) == '\'') {
                    result.append('\'');
                    index += 2;
                    continue;
                }
                singleQuoted = !singleQuoted;
                index++;
                continue;
            }
            if (current == '`' && !singleQuoted) {
                backtickQuoted = !backtickQuoted;
                result.append(current);
                index++;
                continue;
            }
            if (!singleQuoted && !backtickQuoted && Character.isJavaIdentifierStart(current)) {
                int end = index + 1;
                while (end < sql.length() && Character.isJavaIdentifierPart(sql.charAt(end))) {
                    end++;
                }
                String word = sql.substring(index, end);
                result.append(word.equalsIgnoreCase("value") ? "`" + word + "`" : word);
                index = end;
                continue;
            }
            result.append(current);
            index++;
        }
        return result.toString();
    }

    /**
     * Catalogues Calcite statements into the types supported by the controller.
     *
     * @param kind kind
     * @return the operation result
     */
    private CalciteStatementType statementType(final SqlKind kind) {
        if (kind.belongsTo(SqlKind.QUERY)) {
            return CalciteStatementType.SELECT;
        }
        return switch (kind) {
            case INSERT -> CalciteStatementType.INSERT;
            case UPDATE -> CalciteStatementType.UPDATE;
            case DELETE -> CalciteStatementType.DELETE;
            default -> throw unsupported(kind.name(), null);
        };
    }

    /**
     * Collection of possible table identifiers after first decomposition to provide dynamic id/value table metadata for the Calcite validate phase.
     *
     * @param parsed parsed
     * @return the operation result
     */
    private Set<String> collectIdentifierCandidates(final SqlNode parsed) {
        Set<String> result = new LinkedHashSet<>();
        parsed.accept(new SqlShuttle() {

            /**
             * Collects simple identifiers, as well as qualifying the head and end of identifiers.
             *
             * @param identifier identifier
             * @return the operation result
             */
            @Override
            public SqlNode visit(final SqlIdentifier identifier) {
                if (!identifier.names.isEmpty()) {
                    result.add(identifier.names.getFirst().toLowerCase(Locale.ROOT));
                    result.add(identifier.names.getLast().toLowerCase(Locale.ROOT));
                }
                return super.visit(identifier);
            }
        });
        return result;
    }

    /**
     * The real name is taken from the query relationship tree; DML uses the candidate identifier collected before validate and excludes fixed listings.
     *
     * @param planner planner
     * @param validated validated
     * @param statementType statement type
     * @param candidates candidates
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private List<String> resolveTableNames(final Planner planner,
                                           final SqlNode validated,
                                           final CalciteStatementType statementType,
                                           final Set<String> candidates) throws Exception {
        if (statementType == CalciteStatementType.SELECT) {
            RelRoot relRoot = planner.rel(validated);
            return RelOptUtil.findAllTables(relRoot.rel).stream()
                    .map(RelOptTable -> RelOptTable.getQualifiedName().getLast())
                    .map(name -> name.toLowerCase(Locale.ROOT))
                    .distinct()
                    .toList();
        }
        return candidates.stream()
                .filter(name -> !name.equals("id") && !name.equals("value"))
                .toList();
    }

    /**
     * Creates a uniform illegal SQL anomaly and retains Calcite root cause.
     *
     * @param sql sql
     * @param cause cause
     * @return the operation result
     */
    private MvccException unsupported(final String sql, final Exception cause) {
        return cause == null
                ? new MvccException(ErrorCode.ILLEGAL_STATE, "unsupported sql: " + sql, false, null)
                : new MvccException(ErrorCode.ILLEGAL_STATE, "unsupported sql: " + sql, false, null, cause);
    }

    /**
     * The modified key-value table metadata used for Calcite validate, the real DML is still processed by the MVCC Executor.
     */
    private static final class PlanningTable extends AbstractTable {

        /**
         * Constructs a plan sheet for the given name.
         *
         * @param tableName table name
         */
        private PlanningTable(final String tableName) {
        }

        /**
         * Returns the current SQL layer to a fixed id/value string type.
         *
         * @param typeFactory type factory
         * @return the operation result
         */
        @Override
        public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.VARCHAR)
                    .add("value", SqlTypeName.VARCHAR)
                    .build();
        }
    }
}

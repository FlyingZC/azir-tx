package mvcc.sql;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
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
 * 基于 Apache Calcite 的 SQL 计划器，负责语法解析、关系类型校验和逻辑计划生成。
 */
public final class CalciteSqlPlanner implements SqlPlanner {
    private static final Pattern BEGIN = Pattern.compile("^\\s*BEGIN\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern BEGIN_RC = Pattern.compile(
            "^\\s*BEGIN\\s+(?:TRANSACTION\\s+ISOLATION\\s+LEVEL\\s+)?READ\\s+COMMITTED\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern BEGIN_RR = Pattern.compile(
            "^\\s*BEGIN\\s+(?:TRANSACTION\\s+ISOLATION\\s+LEVEL\\s+)?REPEATABLE\\s+READ\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern COMMIT = Pattern.compile("^\\s*COMMIT\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ROLLBACK = Pattern.compile("^\\s*ROLLBACK\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * 使用 Calcite 解析、校验并生成 SQL 计划。
     */
    @Override
    public SqlPlan plan(String sql) {
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
                    .build();
            Planner planner = Frameworks.getPlanner(frameworkConfig);
            SqlNode parsed = planner.parse(calciteSql);
            SqlNode validated = planner.validate(parsed);
            List<String> tableNames = resolveTableNames(planner, validated, statementType, candidates);
            return new SqlPlan.CalcitePlan(calciteSql, statementType, tableNames);
        } catch (Exception ex) {
            throw unsupported(sql, ex);
        }
    }

    /**
     * 识别 Calcite 本身不负责执行的事务控制语句。
     */
    private SqlPlan transactionPlan(String sql) {
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
     * 创建兼容常见 MySQL 标识符和大小写习惯的解析配置。
     */
    private SqlParser.Config parserConfig() {
        return SqlParser.config().withLex(Lex.MYSQL).withCaseSensitive(false);
    }

    /**
     * 将现有 SQL 模型中的 value 列自动转义，兼容 Calcite 把 VALUE 视为保留字的语法规则。
     */
    private String quoteValueIdentifier(String sql) {
        StringBuilder result = new StringBuilder(sql.length() + 16);
        boolean singleQuoted = false;
        boolean backtickQuoted = false;
        for (int index = 0; index < sql.length(); ) {
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
     * 把 Calcite 语句分类映射为执行器支持的类型。
     */
    private CalciteStatementType statementType(SqlKind kind) {
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
     * 首次解析后收集可能的表标识符，为 Calcite 校验阶段提供动态 id/value 表元数据。
     */
    private Set<String> collectIdentifierCandidates(SqlNode parsed) {
        Set<String> result = new LinkedHashSet<>();
        parsed.accept(new SqlShuttle() {
            /**
             * 收集简单标识符以及限定标识符的首段和末段。
             */
            @Override
            public SqlNode visit(SqlIdentifier identifier) {
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
     * 从查询关系树提取真实表名；DML 则使用校验前收集的候选标识符并排除固定列名。
     */
    private List<String> resolveTableNames(Planner planner,
                                           SqlNode validated,
                                           CalciteStatementType statementType,
                                           Set<String> candidates) throws Exception {
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
     * 创建统一的非法 SQL 异常并保留 Calcite 根因。
     */
    private MvccException unsupported(String sql, Exception cause) {
        return cause == null
                ? new MvccException(ErrorCode.ILLEGAL_STATE, "unsupported sql: " + sql, false, null)
                : new MvccException(ErrorCode.ILLEGAL_STATE, "unsupported sql: " + sql, false, null, cause);
    }

    /**
     * Calcite 校验阶段使用的可修改键值表元数据，真实 DML 仍由 MVCC 执行器处理。
     */
    private static final class PlanningTable extends AbstractTable {

        /**
         * 构造指定名称的规划表。
         */
        private PlanningTable(String tableName) {
        }

        /**
         * 返回当前 SQL 层固定的 id/value 字符串行类型。
         */
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.VARCHAR)
                    .add("value", SqlTypeName.VARCHAR)
                    .build();
        }
    }
}

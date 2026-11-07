package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 简单 SQL 计划器，支持主键点查和主键 DML 的受限 SQL 子集。
 */
public final class SimpleSqlPlanner implements SqlPlanner {
    private static final String IDENTIFIER = "([A-Za-z_][A-Za-z0-9_]*)";
    private static final String LITERAL = "'((?:''|[^'])*)'";
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
    private static final Pattern SELECT = Pattern.compile(
            "^\\s*SELECT\\s+VALUE\\s+FROM\\s+" + IDENTIFIER + "\\s+WHERE\\s+ID\\s*=\\s*" + LITERAL + "\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern SELECT_ID_VALUE = Pattern.compile(
            "^\\s*SELECT\\s+ID\\s*,\\s*VALUE\\s+FROM\\s+" + IDENTIFIER
                    + "\\s+WHERE\\s+ID\\s*=\\s*" + LITERAL + "\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern INSERT = Pattern.compile(
            "^\\s*INSERT\\s+INTO\\s+" + IDENTIFIER
                    + "\\s*\\(\\s*ID\\s*,\\s*VALUE\\s*\\)\\s*VALUES\\s*\\(\\s*"
                    + LITERAL + "\\s*,\\s*" + LITERAL + "\\s*\\)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern UPDATE = Pattern.compile(
            "^\\s*UPDATE\\s+" + IDENTIFIER + "\\s+SET\\s+VALUE\\s*=\\s*" + LITERAL
                    + "\\s+WHERE\\s+ID\\s*=\\s*" + LITERAL + "\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern DELETE = Pattern.compile(
            "^\\s*DELETE\\s+FROM\\s+" + IDENTIFIER + "\\s+WHERE\\s+ID\\s*=\\s*" + LITERAL + "\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * 解析 SQL 并返回轻量计划。
     */
    @Override
    public SqlPlan plan(String sql) {
        if (sql == null || sql.isBlank()) {
            throw unsupported(sql);
        }
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
        Matcher select = SELECT.matcher(sql);
        if (select.matches()) {
            return new SqlPlan.SelectByKeyPlan(select.group(1), unescape(select.group(2)));
        }
        Matcher idAndValue = SELECT_ID_VALUE.matcher(sql);
        if (idAndValue.matches()) {
            return new SqlPlan.SelectByKeyPlan(idAndValue.group(1), unescape(idAndValue.group(2)));
        }
        Matcher insert = INSERT.matcher(sql);
        if (insert.matches()) {
            return new SqlPlan.InsertByKeyPlan(insert.group(1), unescape(insert.group(2)), unescape(insert.group(3)));
        }
        Matcher update = UPDATE.matcher(sql);
        if (update.matches()) {
            return new SqlPlan.UpdateByKeyPlan(update.group(1), unescape(update.group(3)), unescape(update.group(2)));
        }
        Matcher delete = DELETE.matcher(sql);
        if (delete.matches()) {
            return new SqlPlan.DeleteByKeyPlan(delete.group(1), unescape(delete.group(2)));
        }
        throw unsupported(sql);
    }

    /**
     * 反转义 SQL 字符串字面量内容。
     */
    private String unescape(String literalBody) {
        return literalBody.replace("''", "'");
    }

    /**
     * 创建不支持 SQL 的统一异常。
     */
    private MvccException unsupported(String sql) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, "unsupported sql: " + sql, false, null);
    }
}

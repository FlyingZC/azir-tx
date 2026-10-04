package mvcc.sql;

import mvcc.common.IsolationLevel;

/**
 * SQL 会话接口，维护显式事务状态并执行 SQL。
 */
public interface SqlSession extends AutoCloseable {
    /**
     * 开启指定隔离级别的显式事务。
     */
    void begin(IsolationLevel isolationLevel);

    /**
     * 执行一条 SQL 并返回结果。
     */
    SqlExecutionResult execute(String sql);

    /**
     * 提交当前显式事务。
     */
    SqlExecutionResult commit();

    /**
     * 回滚当前显式事务。
     */
    void rollback();

    /**
     * 判断当前会话是否存在活动显式事务。
     */
    boolean active();

    /**
     * 关闭 SQL 会话。
     */
    @Override
    void close();
}

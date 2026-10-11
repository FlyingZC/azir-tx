package mvcc.sql;

import mvcc.common.IsolationLevel;

/**
 * SQL 会话接口，维护显式事务状态并执行 SQL；会话关闭后不能重新开启或执行任何 SQL。
 */
public interface SqlSession extends AutoCloseable {
    /**
     * 开启指定隔离级别的显式事务。
     */
    void begin(IsolationLevel isolationLevel);

    /**
     * 执行一条 SQL 并返回结果；数据语句执行失败将回滚整个显式事务并使其失效，需显式 ROLLBACK 后再使用。
     */
    SqlExecutionResult execute(String sql);

    /**
     * 提交当前显式事务。
     */
    SqlExecutionResult commit();

    /**
     * 回滚当前显式事务，也用于清除失败语句导致的事务失效状态。
     */
    void rollback();

    /**
     * 判断当前会话是否存在可继续执行的显式事务；失效或关闭状态返回 false。
     */
    boolean active();

    /**
     * 幂等关闭会话并清理未决事务；清理异常也不会重新开放会话，未完成清理由事务恢复处理。
     * 关闭后 begin、execute、commit、rollback 均拒绝执行，active 返回 false。
     */
    @Override
    void close();
}

package com.flyingzc.azir.control;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Objects;
import java.util.Arrays;
import java.util.logging.Logger;

/** 每次借用连接核对持久权威与本地持久化条件；错误路由或降级配置必须停止服务。 */
public final class ProtocolMysqlDataSource implements DataSource {
    private final DataSource delegate;
    private final MysqlDatabaseAuthority.Identity expected;
    private final MysqlGroupDataSource group;

    /** 绑定当前物理数据库；不拥有原连接池的生命周期。 */
    public ProtocolMysqlDataSource(DataSource delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        group = delegate instanceof MysqlGroupDataSource cluster ? cluster : delegate instanceof ProtocolMysqlDataSource protocol ? protocol.groupAuthority() : null;
        MysqlDatabaseAuthority.Identity physical;
        try (var connection = delegate.getConnection()) {
            MysqlSessionRequirements.require(connection); physical = MysqlDatabaseAuthority.inspectConnection(connection);
        } catch (SQLException failure) { throw new IllegalArgumentException("unsafe MySQL protocol driver configuration", failure); }
        expected = group == null ? physical : new MysqlDatabaseAuthority.Identity("group:" + group.group(), physical.database());
        try (var ignored = getConnection()) { }
        catch (SQLException failure) { throw new IllegalArgumentException("unsafe MySQL protocol authority", failure); }
    }
    /** 每个借出的实际会话都必须属于原权威且具备同步本地 redo 持久性。 */
    @Override public Connection getConnection() throws SQLException { return validate(delegate.getConnection()); }
    /** 显式凭据连接也不能跳过校验。 */
    @Override public Connection getConnection(String username, String password) throws SQLException { return validate(delegate.getConnection(username, password)); }
    /** 不能透明跟随另一服务器 UUID，否则 GET_LOCK 可能产生两个不同权威。 */
    private Connection validate(Connection connection) throws SQLException {
        try { verifySession(connection); return connection; }
        catch (SQLException | RuntimeException | Error failure) {
            MysqlNamedLock.discard(connection, failure);
            try { connection.close(); } catch (SQLException close) { failure.addSuppressed(close); }
            throw failure;
        }
    }
    /** 长期持锁的管理会话必须复查原连接和组提交屏障，不能靠另一个新连接代替。 */
    public void verifySession(Connection connection) throws SQLException {
        MysqlSessionRequirements.require(connection);
        if (!connection.getAutoCommit()) { throw new SQLException("protocol pools require auto-commit"); }
        if (group != null) { group.verify(connection); }
        try (var q = connection.createStatement(); var r = q.executeQuery("select @@server_uuid,database(),@@global.innodb_flush_log_at_trx_commit,@@global.read_only,@@global.super_read_only,@@session.sql_mode,@@session.innodb_strict_mode,@@session.transaction_read_only")) {
            if (!r.next() || (group == null && !expected.serverUuid().equals(r.getString(1))) || !expected.database().equals(r.getString(2))
                    || r.getInt(3) != 1 || r.getBoolean(4) || r.getBoolean(5)) {
                throw new SQLException("MySQL authority changed, is read-only, or synchronous redo durability is disabled");
            }
            // 宽松模式可能把超长键、载荷或溢出时间戳截断后仍报告成功，不能用于事务协议表。
            String sqlMode = r.getString(6);
            boolean strict = sqlMode != null && Arrays.stream(sqlMode.split(",")).map(String::trim)
                    .anyMatch(mode -> mode.equalsIgnoreCase("STRICT_TRANS_TABLES") || mode.equalsIgnoreCase("STRICT_ALL_TABLES"));
            if (!strict || !r.getBoolean(7) || r.getBoolean(8)) { throw new SQLException("protocol sessions require strict SQL and writable transactions"); }
        }
    }
    /** 返回受验证的组发现器；普通单机返回空。 */
    public MysqlGroupDataSource groupAuthority() { return group; }
    /** 返回经过验证的稳定权威；普通单机仍绑定物理 UUID，组复制绑定组 UUID。 */
    public MysqlDatabaseAuthority.Identity authorityIdentity() { return expected; }

    /** 转交日志输出配置，不暴露凭据。 */
    @Override public PrintWriter getLogWriter() throws SQLException { return delegate.getLogWriter(); }
    /** 转交日志输出配置。 */
    @Override public void setLogWriter(PrintWriter writer) throws SQLException { delegate.setLogWriter(writer); }
    /** 转交连接建立超时设置。 */
    @Override public void setLoginTimeout(int seconds) throws SQLException { delegate.setLoginTimeout(seconds); }
    /** 返回原数据源的连接建立超时。 */
    @Override public int getLoginTimeout() throws SQLException { return delegate.getLoginTimeout(); }
    /** 返回数据源日志名称。 */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException { return delegate.getParentLogger(); }
    /** 支持取得包装器或原数据源接口。 */
    @Override public <T> T unwrap(Class<T> type) throws SQLException { return type.isInstance(this) ? type.cast(this) : delegate.unwrap(type); }
    /** 支持标准 JDBC 包装器探测。 */
    @Override public boolean isWrapperFor(Class<?> type) throws SQLException { return type.isInstance(this) || delegate.isWrapperFor(type); }
}

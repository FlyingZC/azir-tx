package com.flyingzc.azir.control;

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.MultiHostMySQLConnection;
import java.sql.Connection;
import java.sql.SQLException;

/** 在任何权威 SQL 之前检查物理驱动配置，命名锁协议不能依赖可被透明替换的会话。 */
final class MysqlSessionRequirements {
    /** 禁止实例化连接约束工具。 */
    private MysqlSessionRequirements() { }

    /** 有限网络等待、单节点 JDBC 会话和禁止透明重连是控制锁及会话变量成立的前提。 */
    static void require(Connection connection) throws SQLException {
        if (connection.isWrapperFor(MultiHostMySQLConnection.class)) { throw new SQLException("protocol sessions cannot use multi-host JDBC failover"); }
        JdbcConnection physical = connection.unwrap(JdbcConnection.class); var properties = physical.getPropertySet();
        if (Boolean.TRUE.equals(properties.getBooleanProperty("autoReconnect").getValue())
                || Boolean.TRUE.equals(properties.getBooleanProperty("autoReconnectForPools").getValue())
                || Boolean.TRUE.equals(properties.getBooleanProperty("reconnectAtTxEnd").getValue())) {
            throw new SQLException("protocol sessions cannot transparently reconnect");
        }
        Integer connect = properties.getIntegerProperty("connectTimeout").getValue();
        Integer socket = properties.getIntegerProperty("socketTimeout").getValue();
        if (connect == null || connect < 1 || socket == null || socket < 1) { throw new SQLException("protocol sessions require finite JDBC connect and socket timeouts"); }
        // 检查原始 URL 以补充代理驱动的包装器探测，不在错误中回显可能含凭据的 URL。
        String url = connection.getMetaData().getURL(); String prefix = "jdbc:mysql://";
        if (url == null || !url.startsWith(prefix)) { throw new SQLException("protocol sessions require a single-host Connector/J URL"); }
        int end = url.indexOf('/', prefix.length()); if (end < 0) { end = url.length(); }
        if (url.substring(prefix.length(), end).contains(",")) { throw new SQLException("protocol sessions cannot use a multi-host JDBC URL"); }
    }
}

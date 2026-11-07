package com.flyingzc.azir.control;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/** 用服务器 UUID 与当前数据库验证独立连接池确实指向同一个 MySQL 持久权威。 */
public final class MysqlDatabaseAuthority {
    /** 只暴露静态数据库身份校验。 */
    private MysqlDatabaseAuthority() {
    }

    /** 服务器与数据库任一不同都拒绝前后台混用。 */
    public record Identity(String serverUuid, String database) {
        /** 无法确定数据库名称时不能猜测协议时间线。 */
        public Identity {
            if (serverUuid == null || serverUuid.isBlank() || database == null || database.isBlank()) {
                throw new IllegalArgumentException("invalid MySQL database authority identity");
            }
        }
    }

    /** 拒绝同池复用或指向另一 MySQL 权威的后台连接。 */
    public static void requireIndependentSameDatabase(DataSource foreground, DataSource background,
                                                      String route) {
        Objects.requireNonNull(foreground, "foreground");
        Objects.requireNonNull(background, "background");
        if (foreground == background || !inspect(foreground).equals(inspect(background))) {
            throw new IllegalArgumentException("background MySQL authority differs from foreground: " + route);
        }
    }

    /** 直接询问真实连接，不能仅相信配置中的 JDBC URL。 */
    public static Identity inspect(DataSource source) {
        Objects.requireNonNull(source, "source");
        if (source instanceof ProtocolMysqlDataSource protocol) { return protocol.authorityIdentity(); }
        try (Connection connection = source.getConnection()) { return inspectConnection(connection);
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect MySQL database authority failed", false, null, ex);
        }
    }
    /** 从已验证驱动配置的原连接读取身份，不另借可能指向另一实例的取样连接。 */
    public static Identity inspectConnection(Connection connection) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        try (Statement query = connection.createStatement(); ResultSet row = query.executeQuery("select @@server_uuid, database()")) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "MySQL connection has no server or selected database", false, null);
            }
            return new Identity(row.getString(1), row.getString(2));
        }
    }

}

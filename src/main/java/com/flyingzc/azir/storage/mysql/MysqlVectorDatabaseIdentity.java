package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** 运行时只用实际 MySQL 服务器 UUID 和已选数据库判断控制与分片权威。 */
final class MysqlVectorDatabaseIdentity {
    /** 身份比较不保存连接或进程本地路由缓存。 */
    private MysqlVectorDatabaseIdentity() {
    }

    /** JDBC URL 相同或不同都不代替物理服务器与数据库的联合身份。 */
    static String read(DataSource source) {
        try (Connection connection = source.getConnection()) {
            return read(connection);
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect vector GC database identity failed", true, null, ex);
        }
    }

    /** 在真正执行 GC 的同一 JDBC 连接上复核物理身份，关闭检查与使用之间的空隙。 */
    static String read(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("select @@server_uuid, database()");
             ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "vector GC requires a selected MySQL database", false, null);
            }
            return row.getString(1) + "\u0000" + row.getString(2);
        }
    }
}

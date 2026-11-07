package com.flyingzc.azir.tso.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 基于 MySQL 行锁递增的时间戳分配器。
 */
public final class MysqlTimestampOracle implements TimestampOracle, MysqlTimestampAuthority {
    private static final String SEQUENCE_NAME = "global";

    private final DataSource dataSource;

    /**
     * 构造 MysqlTimestampOracle 实例。
     */
    public MysqlTimestampOracle(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 合并控制操作只接受同一个池实例，其他配置走原有逐次发号路径。 */
    @Override
    public boolean usesDataSource(DataSource candidate) {
        return dataSource == candidate;
    }

    /**
     * 初始化所需的数据库表结构或运行环境。
     */
    public void initialize(long initialValue) {
        try (Connection connection = MysqlConnectionContext.connection(dataSource);
             PreparedStatement create = connection.prepareStatement("""
                     create table if not exists mvcc_tso (
                       sequence_name varchar(64) not null primary key,
                       current_value bigint not null
                     ) engine=InnoDB
                     """);
             PreparedStatement insert = connection.prepareStatement("""
                     insert into mvcc_tso(sequence_name, current_value)
                     values (?, ?)
                     on duplicate key update current_value = greatest(current_value, values(current_value))
                     """)) {
            create.execute();
            insert.setString(1, SEQUENCE_NAME);
            insert.setLong(2, initialValue);
            insert.executeUpdate();
        } catch (SQLException ex) {
            throw storageError("initialize tso failed", ex, null);
        }
    }

    /**
     * 分配下一个全局单调递增时间戳。
     */
    @Override
    public long nextTimestamp() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
            if (!connection.getAutoCommit()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "tso allocation requires auto-commit connection", false, null);
            }
            // 单条自动提交 UPDATE 原子保留时间戳，LAST_INSERT_ID(expr) 将结果绑定到当前连接。
            try (PreparedStatement update = connection.prepareStatement("""
                    update mvcc_tso
                    set current_value = last_insert_id(current_value + 1)
                    where sequence_name = ? and current_value < ?
                    """);
                 PreparedStatement select = connection.prepareStatement("select last_insert_id()")) {
                update.setString(1, SEQUENCE_NAME);
                update.setLong(2, Long.MAX_VALUE);
                if (update.executeUpdate() != 1) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "tso row missing or timestamp sequence exhausted", false, null);
                }
                try (ResultSet result = select.executeQuery()) {
                    if (!result.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "tso allocation returned no timestamp", false, null);
                    }
                    return result.getLong(1);
                }
            }
        } catch (SQLException ex) {
            throw storageError("allocate tso failed", ex, null);
        }
    }

    /**
     * 将底层存储异常包装为 MVCC 异常。
     */
    private MvccException storageError(String message, SQLException ex, Long txId) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}

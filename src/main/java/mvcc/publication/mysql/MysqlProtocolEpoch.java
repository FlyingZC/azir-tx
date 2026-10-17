package mvcc.publication.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** 通过持久模式行和旧租约外键隔离新旧发布协议。 */
final class MysqlProtocolEpoch {
    /** 旧协议、账本及分段账本切换的持久阶段。 */
    enum Mode {
        LEGACY, ACTIVATING, LEDGER, SEGMENT_PREPARE, SEGMENT
    }

    /** 仅提供同一 MySQL 连接内的协议模式操作。 */
    private MysqlProtocolEpoch() {
    }

    /** 安装模式行及旧租约外键，重复初始化保留已有协议阶段。 */
    static void install(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_protocol_epoch_v1 (
                      singleton_id tinyint not null primary key,
                      protocol_mode varchar(16) not null,
                      generation bigint not null
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("""
                    insert ignore into mvcc_protocol_epoch_v1(singleton_id, protocol_mode, generation)
                    values (1, 'LEGACY', 0)
                    """);
            statement.execute("""
                    create table if not exists mvcc_legacy_lease_gate_v1 (
                      mode_id tinyint not null primary key
                    ) engine=InnoDB
                    """);
        }
        Mode mode = current(connection);
        if (mode == Mode.LEGACY) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("insert ignore into mvcc_legacy_lease_gate_v1(mode_id) values (0)");
            }
            requireGateRow(connection, 0);
        } else {
            requireGateRow(connection, 1);
        }
        if (!schemaObjectExists(connection, "information_schema.columns",
                "table_name = 'mvcc_snapshot_lease' and column_name = 'protocol_epoch'")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        alter table mvcc_snapshot_lease
                        add column protocol_epoch tinyint not null default 0
                        """);
            }
        }
        if (!schemaObjectExists(connection, "information_schema.table_constraints",
                "table_name = 'mvcc_snapshot_lease' and constraint_name = 'mvcc_legacy_lease_epoch_fk' "
                        + "and constraint_type = 'FOREIGN KEY'")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        alter table mvcc_snapshot_lease
                        add constraint mvcc_legacy_lease_epoch_fk
                        foreign key (protocol_epoch) references mvcc_legacy_lease_gate_v1(mode_id)
                        on update restrict on delete restrict
                        """);
            }
        }
    }

    /** 模式行与唯一有效父键必须一致，防止重启时重新放行旧租约。 */
    private static void requireGateRow(Connection connection, int expected) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("select mode_id from mvcc_legacy_lease_gate_v1");
             ResultSet result = query.executeQuery()) {
            if (!result.next() || result.getInt(1) != expected || result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "legacy snapshot gate differs from publication mode", false, null);
            }
        }
    }

    /** 查询版本化模式结构是否已经安装。 */
    private static boolean schemaObjectExists(Connection connection, String table, String conditions)
            throws SQLException {
        String sql = "select 1 from " + table + " where table_schema = database() and " + conditions + " limit 1";
        try (PreparedStatement query = connection.prepareStatement(sql);
             ResultSet result = query.executeQuery()) {
            return result.next();
        }
    }

    /** 查询当前持久模式，不存在或未知模式时失败关闭。 */
    static Mode current(Connection connection) throws SQLException {
        return readMode(connection, false);
    }

    /** 排他锁定模式行，切换检查和更新必须在同一数据库事务中完成。 */
    static Mode lock(Connection connection) throws SQLException {
        return readMode(connection, true);
    }

    /** 按预期旧模式原子推进协议阶段并增加代数。 */
    static void transition(Connection connection, Mode expected, Mode next) throws SQLException {
        if (!(expected == Mode.LEGACY && next == Mode.ACTIVATING
                || expected == Mode.ACTIVATING && next == Mode.LEDGER
                || expected == Mode.LEDGER && next == Mode.SEGMENT_PREPARE
                || expected == Mode.SEGMENT_PREPARE && next == Mode.SEGMENT)) {
            throw new IllegalArgumentException("unsupported publication protocol transition");
        }
        if (expected == Mode.LEGACY && next == Mode.ACTIVATING) {
            // 外键父键更新与模式行更新同事务；在途旧租约要么先提交并阻止切换，要么等待后失败。
            try (PreparedStatement gate = connection.prepareStatement("""
                    update mvcc_legacy_lease_gate_v1 set mode_id = 1 where mode_id = 0
                    """)) {
                if (gate.executeUpdate() != 1) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "legacy snapshot gate cannot enter activating mode", false, null);
                }
            }
        }
        try (PreparedStatement update = connection.prepareStatement("""
                update mvcc_protocol_epoch_v1
                set protocol_mode = ?, generation = generation + 1
                where singleton_id = 1 and protocol_mode = ?
                """)) {
            update.setString(1, next.name());
            update.setString(2, expected.name());
            if (update.executeUpdate() != 1) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication protocol mode changed during activation", false, null);
            }
        }
    }

    /** 新快照只可在账本模式已持久激活后开始。 */
    static void requireLedger(Connection connection) throws SQLException {
        if (readMode(connection, false) != Mode.LEDGER) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "publication ledger protocol is not active", false, null);
        }
    }

    /** 分段业务入口仅在归档旧状态表并完成持久模式切换后可启动。 */
    static void requireSegment(Connection connection) throws SQLException {
        if (readMode(connection, false) != Mode.SEGMENT) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "segmented publication protocol is not active", false, null);
        }
    }

    /** 根据是否排他读取模式行，拒绝缺失和无法识别的状态。 */
    private static Mode readMode(Connection connection, boolean exclusive) throws SQLException {
        String sql = "select protocol_mode from mvcc_protocol_epoch_v1 where singleton_id = 1"
                + (exclusive ? " for update" : " for share");
        try (PreparedStatement query = connection.prepareStatement(sql);
             ResultSet result = query.executeQuery()) {
            if (!result.next()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication protocol mode row missing", false, null);
            }
            try {
                return Mode.valueOf(result.getString(1));
            } catch (IllegalArgumentException ex) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "unknown publication protocol mode", false, null, ex);
            }
        }
    }
}

package com.flyingzc.azir.publication.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** 用旧状态表归档作为物理写栅栏，分阶段恢复分段账本的持久协议切换。 */
final class MysqlSegmentProtocolActivator {
    private static final String OLD_STATE = "mvcc_publication_state_v1";
    private static final String ARCHIVED_STATE = "mvcc_publication_state_v1_legacy_archived";
    private final DataSource dataSource;
    private final MysqlSegmentPublicationLedger segments;

    /** 保存控制库及分段状态初始化入口。 */
    MysqlSegmentProtocolActivator(DataSource dataSource, MysqlSegmentPublicationLedger segments) {
        this.dataSource = dataSource;
        this.segments = segments;
    }

    /** 受控停写后归档旧状态表，崩溃时从持久阶段和归档表继续切换。 */
    void activate() {
        MysqlConnectionContext.withPublicationLock(dataSource, 10, () -> {
            try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
                MysqlProtocolEpoch.Mode mode = MysqlProtocolEpoch.current(connection);
                if (mode == MysqlProtocolEpoch.Mode.SEGMENT) {
                    assertActivated(connection);
                    return null;
                }
                if (mode != MysqlProtocolEpoch.Mode.LEDGER
                        && mode != MysqlProtocolEpoch.Mode.SEGMENT_PREPARE) {
                    throw illegal("only an active old ledger can enter segmented mode");
                }
                boolean archived = tableExists(connection, ARCHIVED_STATE);
                if (!archived) {
                    if (mode != MysqlProtocolEpoch.Mode.LEDGER || !tableExists(connection, OLD_STATE)) {
                        throw illegal("old publication state cannot be archived or resumed");
                    }
                    requireDrained(connection, OLD_STATE);
                    // RENAME 等待旧状态表上的在途事务，之后旧进程不能再更新原表。
                    try (Statement rename = connection.createStatement()) {
                        rename.execute("rename table " + OLD_STATE + " to " + ARCHIVED_STATE);
                    }
                }
                removeEmptyRecreatedOldState(connection);
                LegacyWatermark previous = requireDrained(connection, ARCHIVED_STATE);
                if (mode == MysqlProtocolEpoch.Mode.LEDGER) {
                    MysqlConnectionContext.inTransaction(dataSource, activation -> {
                        if (MysqlProtocolEpoch.lock(activation) != MysqlProtocolEpoch.Mode.LEDGER) {
                            throw illegal("old ledger mode changed after physical fence");
                        }
                        MysqlProtocolEpoch.transition(activation, MysqlProtocolEpoch.Mode.LEDGER,
                                MysqlProtocolEpoch.Mode.SEGMENT_PREPARE);
                        return null;
                    });
                }
                segments.initializeIsolated(previous.upper());
                MysqlConnectionContext.inTransaction(dataSource, activation -> {
                    if (MysqlProtocolEpoch.lock(activation) != MysqlProtocolEpoch.Mode.SEGMENT_PREPARE) {
                        throw illegal("segment activation phase changed before final transition");
                    }
                    try (PreparedStatement state = activation.prepareStatement("""
                            select base_commit_ts, next_commit_ts, published_ts, safe_point
                            from mvcc_segment_publication_state_v1 where singleton_id = 1 for update
                            """);
                         ResultSet row = state.executeQuery()) {
                        if (!row.next() || row.getLong(1) != previous.upper()
                                || row.getLong(2) != previous.upper()
                                || row.getLong(3) != previous.upper()
                                || row.getLong(4) > previous.upper()) {
                            throw illegal("segment timeline differs from archived publication boundary");
                        }
                    }
                    try (PreparedStatement safe = activation.prepareStatement("""
                            update mvcc_segment_publication_state_v1
                            set safe_point = greatest(safe_point, ?) where singleton_id = 1
                            """)) {
                        safe.setLong(1, previous.safePoint());
                        safe.executeUpdate();
                    }
                    MysqlProtocolEpoch.transition(activation, MysqlProtocolEpoch.Mode.SEGMENT_PREPARE,
                            MysqlProtocolEpoch.Mode.SEGMENT);
                    return null;
                });
                assertActivated(connection);
            } catch (SQLException ex) {
                throw storageError("activate segmented publication mode failed", ex);
            }
            return null;
        });
    }

    /** 运行时同时核对模式行、归档边界与已隔离的旧状态表。 */
    void assertActivated() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
            assertActivated(connection);
        } catch (SQLException ex) {
            throw storageError("inspect segmented publication activation failed", ex);
        }
    }

    /** 在同一连接中验证新模式不可被缺失归档表或重建旧行伪造。 */
    private void assertActivated(Connection connection) throws SQLException {
        MysqlProtocolEpoch.requireSegment(connection);
        if (!tableExists(connection, ARCHIVED_STATE)
                || !tableExists(connection, "mvcc_segment_publication_state_v1")
                || !tableExists(connection, "mvcc_publication_segment_v1")
                || !tableExists(connection, "mvcc_publication_segment_slot_v1")) {
            throw illegal("segmented mode is missing durable tables or old fence");
        }
        if (tableExists(connection, OLD_STATE)) {
            throw illegal("old publication state table reappeared after physical fencing");
        }
        segments.assertSchemaReady(connection);
        LegacyWatermark previous = readWatermark(connection, ARCHIVED_STATE);
        try (PreparedStatement state = connection.prepareStatement("""
                select base_commit_ts, next_commit_ts, published_ts, safe_point
                from mvcc_segment_publication_state_v1 where singleton_id = 1
                """);
             ResultSet row = state.executeQuery()) {
            if (!row.next() || row.getLong(1) != previous.upper()
                    || row.getLong(2) < row.getLong(3)
                    || row.getLong(3) < previous.upper()
                    || row.getLong(4) < previous.safePoint()
                    || row.getLong(4) > row.getLong(3)) {
                throw illegal("segmented timeline does not continue archived watermark");
            }
        }
    }

    /** 旧初始化器在归档后可能重建空表，只允许删除这个没有时间线数据的残影。 */
    private void removeEmptyRecreatedOldState(Connection connection) throws SQLException {
        if (!tableExists(connection, OLD_STATE)) {
            return;
        }
        if (rowCount(connection, OLD_STATE) != 0) {
            throw illegal("old publication state was repopulated after archive");
        }
        try (Statement drop = connection.createStatement()) {
            drop.execute("drop table " + OLD_STATE);
        }
    }

    /** 迁移前后均要求旧决议、读者与参与者全部排空。 */
    private LegacyWatermark requireDrained(Connection connection, String stateTable) throws SQLException {
        LegacyWatermark watermark = readWatermark(connection, stateTable);
        try (PreparedStatement query = connection.prepareStatement("""
                select (select count(*) from mvcc_publication_entry_v1 where status = 'PENDING')
                     + (select count(*) from mvcc_snapshot_lease_v2
                         where expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed))
                     + (select count(*) from mvcc_coordinator_snapshot_lease_v1
                         where expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed))
                     + (select count(*) from mvcc_txn t
                         left join mvcc_txn_finalized f on f.tx_id = t.tx_id
                         where f.tx_id is null)
                """);
             ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getLong(1) != 0) {
                throw illegal("old ledger still has pending decisions, readers or unfinalized participants");
            }
        }
        return watermark;
    }

    /** 旧账本必须没有未发布的时间槽，归档行仍保留其 safe point。 */
    private LegacyWatermark readWatermark(Connection connection, String table) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select next_commit_ts, published_ts, safe_point from %s where singleton_id = 1
                """.formatted(table));
             ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getLong(1) != row.getLong(2)
                    || row.getLong(3) > row.getLong(2) || row.getLong(2) < 0) {
                throw illegal("old ledger has an unpublished or invalid boundary");
            }
            return new LegacyWatermark(row.getLong(2), row.getLong(3));
        }
    }

    /** 检查固定协议表名是否是当前数据库中的真实表。 */
    private boolean tableExists(Connection connection, String table) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.tables
                where table_schema = database() and table_name = ? and table_type = 'BASE TABLE'
                limit 1
                """)) {
            query.setString(1, table);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /** 只读取固定表名的行数，用于拒绝旧时间线重新出现。 */
    private long rowCount(Connection connection, String table) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("select count(*) from " + table);
             ResultSet row = query.executeQuery()) {
            row.next();
            return row.getLong(1);
        }
    }

    /** 保留归档时间上界和安全点。 */
    private record LegacyWatermark(long upper, long safePoint) {
    }

    /** 将不满足迁移不变量的状态按失败关闭处理。 */
    private MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /** 保留数据库异常供运维区分 DDL 与连接故障。 */
    private MvccException storageError(String message, SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, false, null, cause);
    }
}

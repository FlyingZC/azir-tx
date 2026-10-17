package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** 主参与分片以持久状态行和分条决议序号替代逐笔中心事务决议的隔离原型。 */
final class MysqlPrimaryDecisionPrototype {
    private final DataSource primary;
    private final DataSource secondary;
    private final int stripes;

    /** 注册身份固定两片键值、起始分条边界和主决议分条。 */
    record Registration(long txId, String primaryKey, byte[] primaryValue,
                        String secondaryKey, byte[] secondaryValue,
                        int primaryStripe, int secondaryStripe,
                        long primaryStartSequence, long secondaryStartSequence,
                        long deadlineMillis, int decisionStripe) {
        /** 业务字节在入队时复制，不能由调用方事后修改持久决议身份。 */
        Registration {
            Objects.requireNonNull(primaryKey, "primaryKey");
            Objects.requireNonNull(secondaryKey, "secondaryKey");
            Objects.requireNonNull(primaryValue, "primaryValue");
            Objects.requireNonNull(secondaryValue, "secondaryValue");
            primaryValue = primaryValue.clone();
            secondaryValue = secondaryValue.clone();
            if (txId == 0 || primaryKey.isBlank() || secondaryKey.isBlank()
                    || primaryStripe < 0 || secondaryStripe < 0
                    || primaryStartSequence < 0 || secondaryStartSequence < 0
                    || deadlineMillis < 1 || decisionStripe < 0) {
                throw new IllegalArgumentException("invalid primary transaction registration");
            }
        }

        /** 外部读取主分片载荷只能取得副本。 */
        @Override
        public byte[] primaryValue() {
            return primaryValue.clone();
        }

        /** 外部读取次分片载荷只能取得副本。 */
        @Override
        public byte[] secondaryValue() {
            return secondaryValue.clone();
        }
    }

    /** 主分片持久决议与两片保留序号的不可逆结果。 */
    record Decision(long txId, TxStatus status, int decisionStripe,
                    Long decisionSequence, Long primarySequence, Long secondarySequence) {
        /** 只有 COMMITTED 才允许同时携带三份完整序号。 */
        Decision {
            Objects.requireNonNull(status, "status");
            if (txId == 0 || decisionStripe < 0
                    || (status == TxStatus.COMMITTED) != (decisionSequence != null
                    && primarySequence != null && secondarySequence != null)) {
                throw new IllegalArgumentException("invalid primary decision identity");
            }
        }
    }

    /** 每个持久参与者证明绑定分条、起始界限与预写序号。 */
    private record ParticipantProof(int stripe, long startSequence, long reservedSequence) {
    }

    /** 主事务行与决议计数只在主分片，次分片只提供持久预写证明。 */
    MysqlPrimaryDecisionPrototype(DataSource primary, DataSource secondary, int stripes) {
        this.primary = Objects.requireNonNull(primary, "primary");
        this.secondary = Objects.requireNonNull(secondary, "secondary");
        if (stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid primary decision stripes");
        }
        this.stripes = stripes;
    }

    /** 建立独立主事务行和分条计数，重复调用保留已决议数据。 */
    void initializeIsolated() throws SQLException {
        try (Connection connection = primary.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_primary_layout_v1 (
                      singleton_id tinyint not null primary key,
                      stripe_count smallint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_primary_decision_state_v1 (
                      stripe_id smallint not null primary key,
                      committed_seq bigint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_primary_txn_v2 (
                      tx_id bigint not null primary key,
                      primary_key varbinary(256) not null,
                      primary_value blob not null,
                      secondary_key varbinary(256) not null,
                      secondary_value blob not null,
                      primary_stripe smallint not null,
                      secondary_stripe smallint not null,
                      primary_start_seq bigint not null,
                      secondary_start_seq bigint not null,
                      deadline_millis bigint not null,
                      decision_stripe smallint not null,
                      status varchar(16) not null,
                      decision_seq bigint,
                      primary_reserved_seq bigint,
                      secondary_reserved_seq bigint,
                      index bench_primary_unfinished (status, deadline_millis, tx_id)
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_primary_layout_v1(singleton_id, stripe_count)
                    values (1, ?) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setInt(1, stripes);
                insert.executeUpdate();
            }
            try (PreparedStatement query = connection.prepareStatement("""
                    select stripe_count from bench_primary_layout_v1 where singleton_id = 1
                    """); ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw new SQLException("primary decision stripe layout changed");
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_primary_decision_state_v1(stripe_id, committed_seq)
                    values (?, 0) on duplicate key update stripe_id = stripe_id
                    """)) {
                for (int stripe = 0; stripe < stripes; stripe++) {
                    insert.setInt(1, stripe);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            MysqlPrimaryCombinedPrewriteProcedure.ensure(connection);
        }
    }

    /** 仅测试显式调用可清除隔离状态，不能用于产品恢复。 */
    void resetIsolated() throws SQLException {
        try (Connection connection = primary.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from bench_primary_txn_v2");
            statement.executeUpdate("update bench_primary_decision_state_v1 set committed_seq = 0");
        }
    }

    /** 主 ACTIVE 身份先持久化；同号重放只能使用完全相同键值和期限。 */
    Decision register(Registration registration) {
        requireStripe(registration.decisionStripe());
        requireStripe(registration.primaryStripe());
        requireStripe(registration.secondaryStripe());
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                try (PreparedStatement insert = connection.prepareStatement("""
                        insert into bench_primary_txn_v2
                          (tx_id, primary_key, primary_value, secondary_key, secondary_value,
                           primary_stripe, secondary_stripe,
                           primary_start_seq, secondary_start_seq, deadline_millis,
                           decision_stripe, status)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')
                        on duplicate key update tx_id = tx_id
                        """)) {
                    insert.setLong(1, registration.txId());
                    insert.setBytes(2, encode(registration.primaryKey()));
                    insert.setBytes(3, registration.primaryValue());
                    insert.setBytes(4, encode(registration.secondaryKey()));
                    insert.setBytes(5, registration.secondaryValue());
                    insert.setInt(6, registration.primaryStripe());
                    insert.setInt(7, registration.secondaryStripe());
                    insert.setLong(8, registration.primaryStartSequence());
                    insert.setLong(9, registration.secondaryStartSequence());
                    insert.setLong(10, registration.deadlineMillis());
                    insert.setInt(11, registration.decisionStripe());
                    insert.executeUpdate();
                }
                PrimaryRow row = readPrimary(connection, registration.txId(), true);
                requireSameRegistration(row, registration);
                if (row.status() == TxStatus.ABORTED) {
                    throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                            "primary transaction was already aborted", false, registration.txId());
                }
                return row.decision();
            });
        } catch (SQLException ex) {
            throw unknown(registration.txId(), "register primary transaction failed", ex);
        }
    }

    /** 主 ACTIVE 登记与主意向预写由一个存储过程原子提交，成功时返回保留序号。 */
    long registerAndPrewritePrimary(Registration registration) {
        Objects.requireNonNull(registration, "registration");
        requireStripe(registration.primaryStripe());
        requireStripe(registration.secondaryStripe());
        requireStripe(registration.decisionStripe());
        try (Connection connection = primary.getConnection(); PreparedStatement call = connection.prepareStatement("""
                call bench_primary_register_prewrite_v3(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            call.setLong(1, registration.txId());
            call.setBytes(2, encode(registration.primaryKey()));
            call.setBytes(3, registration.primaryValue());
            call.setBytes(4, encode(registration.secondaryKey()));
            call.setBytes(5, registration.secondaryValue());
            call.setInt(6, registration.primaryStripe());
            call.setInt(7, registration.secondaryStripe());
            call.setLong(8, registration.primaryStartSequence());
            call.setLong(9, registration.secondaryStartSequence());
            call.setLong(10, registration.deadlineMillis());
            call.setInt(11, registration.decisionStripe());
            try (ResultSet result = call.executeQuery()) {
                if (!result.next() || result.getLong(1) <= registration.primaryStartSequence()) {
                    throw new SQLException("combined primary prewrite did not return a valid sequence");
                }
                return result.getLong(1);
            }
        } catch (SQLException ex) {
            ErrorCode code = switch (ex.getErrorCode()) {
                case 36002 -> ErrorCode.LOCK_CONFLICT;
                case 36003 -> ErrorCode.WRITE_CONFLICT;
                case 36006, 36016 -> ErrorCode.TRANSACTION_ABORTED;
                case 36017 -> ErrorCode.TRANSACTION_TIMEOUT;
                case 36004, 36005, 36015 -> ErrorCode.ILLEGAL_STATE;
                default -> ErrorCode.TRANSACTION_STATUS_UNKNOWN;
            };
            throw new MvccException(code, "combined primary prewrite failed",
                    code == ErrorCode.TRANSACTION_STATUS_UNKNOWN, registration.txId(), ex);
        }
    }

    /** 主分片再次读取两片持久预写，行锁内和中止竞争并原子分配决议序号。 */
    Decision commit(long txId) {
        PrimaryRow identity = requirePrimary(txId);
        if (identity.status() == TxStatus.COMMITTED) {
            return identity.decision();
        }
        if (identity.status() == TxStatus.ABORTED) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "primary transaction was aborted", false, txId);
        }
        ParticipantProof primaryProof = verifyPrewrite(primary, txId, identity.primaryKey(),
                identity.primaryValue(), identity.primaryStripe(), identity.primaryStartSequence());
        ParticipantProof secondaryProof = verifyPrewrite(secondary, txId, identity.secondaryKey(),
                identity.secondaryValue(), identity.secondaryStripe(), identity.secondaryStartSequence());
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                PrimaryRow row = readPrimary(connection, txId, true);
                requireSameRegistration(row, identity.registration());
                return decideCommitted(connection, row, primaryProof, secondaryProof);
            });
        } catch (SQLException ex) {
            throw unknown(txId, "commit primary decision requires durable lookup", ex);
        }
    }

    /** 前台持有完整注册身份时免去主分片事务外的额外查询，仍在行锁内重新核对身份。 */
    Decision commit(Registration registration) {
        Objects.requireNonNull(registration, "registration");
        ParticipantProof secondaryProof = verifyPrewrite(secondary, registration.txId(),
                registration.secondaryKey(), registration.secondaryValue(),
                registration.secondaryStripe(), registration.secondaryStartSequence());
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                PrimaryRow row = readPrimary(connection, registration.txId(), true);
                requireSameRegistration(row, registration);
                if (row.status() == TxStatus.COMMITTED) {
                    return row.decision();
                }
                ParticipantProof primaryProof = verifyPrewrite(connection,
                        registration.txId(), registration.primaryKey(),
                        registration.primaryValue(), registration.primaryStripe(),
                        registration.primaryStartSequence());
                return decideCommitted(connection, row, primaryProof, secondaryProof);
            });
        } catch (SQLException ex) {
            throw unknown(registration.txId(), "commit primary decision requires durable lookup", ex);
        }
    }

    /** 主行锁内把分条决议序号与终态同事务提交，禁止过期及中止状态倒退。 */
    private Decision decideCommitted(Connection connection, PrimaryRow row,
                                     ParticipantProof primaryProof,
                                     ParticipantProof secondaryProof) throws SQLException {
        long txId = row.txId();
        if (row.status() == TxStatus.COMMITTED) {
            return row.decision();
        }
        if (row.status() != TxStatus.ACTIVE || row.deadlineMillis() <= databaseNow(connection)) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED,
                    "primary decision lost ACTIVE lease", false, txId);
        }
        try (PreparedStatement advance = connection.prepareStatement("""
                update bench_primary_decision_state_v1
                set committed_seq = last_insert_id(committed_seq + 1)
                where stripe_id = ?
                """)) {
            advance.setInt(1, row.decisionStripe());
            if (advance.executeUpdate() != 1) {
                throw new SQLException("primary decision stripe is missing");
            }
        }
        long decisionSequence;
        try (PreparedStatement sequence = connection.prepareStatement("select last_insert_id()");
             ResultSet result = sequence.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("primary decision did not return a sequence");
            }
            decisionSequence = result.getLong(1);
        }
        try (PreparedStatement terminal = connection.prepareStatement("""
                update bench_primary_txn_v2
                set status = 'COMMITTED', decision_seq = ?,
                    primary_reserved_seq = ?, secondary_reserved_seq = ?
                where tx_id = ? and status = 'ACTIVE'
                """)) {
            terminal.setLong(1, decisionSequence);
            terminal.setLong(2, primaryProof.reservedSequence());
            terminal.setLong(3, secondaryProof.reservedSequence());
            terminal.setLong(4, txId);
            if (terminal.executeUpdate() != 1) {
                throw new SQLException("primary decision lost transaction row lock");
            }
        }
        return new Decision(txId, TxStatus.COMMITTED, row.decisionStripe(),
                decisionSequence, primaryProof.reservedSequence(),
                secondaryProof.reservedSequence());
    }

    /** 提交前从主分片持久行恢复注册身份，缺失时禁止凭调用方信息造出决议。 */
    private PrimaryRow requirePrimary(long txId) {
        try (Connection connection = primary.getConnection()) {
            PrimaryRow row = readPrimary(connection, txId, false);
            if (row == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "primary commit has no durable ACTIVE identity", false, txId);
            }
            return row;
        } catch (SQLException ex) {
            throw unknown(txId, "read primary transaction identity failed", ex);
        }
    }

    /** 同一主事务行锁串行中止与提交，已提交终态不能反向回滚。 */
    Decision abort(long txId) {
        try {
            return MysqlConnectionContext.inTransaction(primary, connection -> {
                PrimaryRow row = readPrimary(connection, txId, true);
                if (row == null) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "primary abort has no durable ACTIVE identity", false, txId);
                }
                if (row.status() == TxStatus.COMMITTED) {
                    throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "primary abort raced a committed decision", false, txId);
                }
                if (row.status() == TxStatus.ACTIVE) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update bench_primary_txn_v2 set status = 'ABORTED'
                            where tx_id = ? and status = 'ACTIVE'
                            """)) {
                        update.setLong(1, txId);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("primary abort lost ACTIVE row lock");
                        }
                    }
                }
                return new Decision(txId, TxStatus.ABORTED, row.decisionStripe(), null, null, null);
            });
        } catch (SQLException ex) {
            throw unknown(txId, "abort primary decision requires durable lookup", ex);
        }
    }

    /** 回放和读侧只相信真实主分片事务行，不缓存 ACTIVE 或缺失终态。 */
    Optional<Decision> decision(long txId) {
        try (Connection connection = primary.getConnection()) {
            PrimaryRow row = readPrimary(connection, txId, false);
            return row == null ? Optional.empty() : Optional.of(row.decision());
        } catch (SQLException ex) {
            throw unknown(txId, "read primary decision failed", ex);
        }
    }

    /** 读侧核对事务号对应的主分片持久身份，防止额外意向冒用已提交事务。 */
    Decision participantDecision(long txId, boolean primaryShard, String key,
                                 int stripe, long reservedSequence) {
        try (Connection connection = primary.getConnection()) {
            PrimaryRow row = readPrimary(connection, txId, false);
            if (row == null) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "participant intent has no durable primary identity", true, txId);
            }
            if (primaryShard ? !row.primaryKey().equals(key) : !row.secondaryKey().equals(key)) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "participant intent key differs from primary identity", false, txId);
            }
            if (primaryShard ? row.primaryStripe() != stripe : row.secondaryStripe() != stripe) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "participant intent stripe differs from primary identity", false, txId);
            }
            if (row.status() == TxStatus.COMMITTED) {
                Long expected = primaryShard ? row.primarySequence() : row.secondarySequence();
                if (expected == null || expected != reservedSequence) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "participant intent sequence differs from primary decision", false, txId);
                }
            }
            return row.decision();
        } catch (SQLException ex) {
            throw unknown(txId, "read participant primary decision failed", ex);
        }
    }

    /** 一条 MySQL 语句取得全部分条决议水位，供切面发布者先行采样。 */
    List<Long> decisionVector() {
        try (Connection connection = primary.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select stripe_id, committed_seq from bench_primary_decision_state_v1 order by stripe_id
                """); ResultSet row = query.executeQuery()) {
            List<Long> values = new ArrayList<>(stripes);
            while (row.next()) {
                if (row.getInt(1) != values.size() || row.getLong(2) < 0) {
                    throw new SQLException("primary decision vector is incomplete");
                }
                values.add(row.getLong(2));
            }
            if (values.size() != stripes) {
                throw new SQLException("primary decision vector lost a stripe");
            }
            return List.copyOf(values);
        } catch (SQLException ex) {
            throw unknown(null, "read primary decision vector failed", ex);
        }
    }

    /** 所有参与者证明必须从对应分片的持久意向或已物化版本重新读取。 */
    private ParticipantProof verifyPrewrite(DataSource shard, long txId, String key,
                                            byte[] value, int stripe, long startSequence) {
        try (Connection connection = shard.getConnection()) {
            return verifyPrewrite(connection, txId, key, value, stripe, startSequence);
        } catch (SQLException ex) {
            throw unknown(txId, "read participant prewrite proof failed", ex);
        }
    }

    /** 主片行锁事务内读取预写证明，响应重放可接受同一身份的已物化版本。 */
    private ParticipantProof verifyPrewrite(Connection connection, long txId, String key,
                                            byte[] value, int stripe, long startSequence) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select stripe_id, start_seq, reserved_seq, payload
                from bench_striped_mvcc_cross_intent where user_key = ? and tx_id = ?
                union all
                select stripe_id, start_seq, reserved_seq, payload
                from bench_striped_mvcc_cross_data where user_key = ? and tx_id = ?
                """)) {
            byte[] encoded = encode(key);
            query.setBytes(1, encoded);
            query.setLong(2, txId);
            query.setBytes(3, encoded);
            query.setLong(4, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getInt("stripe_id") != stripe
                        || row.getLong("start_seq") != startSequence
                        || row.getLong("reserved_seq") <= startSequence
                        || !Arrays.equals(row.getBytes("payload"), value)) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "primary commit lacks matching durable participant prewrite", false, txId);
                }
                return new ParticipantProof(row.getInt("stripe_id"), startSequence,
                        row.getLong("reserved_seq"));
            }
        }
    }

    /** 固定中心身份后禁止改变键、值、快照界限、期限或决议分条。 */
    private void requireSameRegistration(PrimaryRow row, Registration expected) {
        if (row == null || row.txId() != expected.txId()
                || !row.primaryKey().equals(expected.primaryKey())
                || !row.secondaryKey().equals(expected.secondaryKey())
                || !Arrays.equals(row.primaryValue(), expected.primaryValue())
                || !Arrays.equals(row.secondaryValue(), expected.secondaryValue())
                || row.primaryStartSequence() != expected.primaryStartSequence()
                || row.secondaryStartSequence() != expected.secondaryStartSequence()
                || row.primaryStripe() != expected.primaryStripe()
                || row.secondaryStripe() != expected.secondaryStripe()
                || row.deadlineMillis() != expected.deadlineMillis()
                || row.decisionStripe() != expected.decisionStripe()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "primary transaction replay changed durable identity", false, expected.txId());
        }
    }

    /** 精确读取主事务行，决议和中止使用排他锁。 */
    private PrimaryRow readPrimary(Connection connection, long txId, boolean lock) throws SQLException {
        String sql = "select tx_id, primary_key, primary_value, secondary_key, secondary_value,"
                + " primary_stripe, secondary_stripe, primary_start_seq, secondary_start_seq,"
                + " deadline_millis, decision_stripe,"
                + " status, decision_seq, primary_reserved_seq, secondary_reserved_seq"
                + " from bench_primary_txn_v2 where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                return new PrimaryRow(row.getLong("tx_id"),
                        decode(row.getBytes("primary_key")), row.getBytes("primary_value"),
                        decode(row.getBytes("secondary_key")), row.getBytes("secondary_value"),
                        row.getInt("primary_stripe"), row.getInt("secondary_stripe"),
                        row.getLong("primary_start_seq"), row.getLong("secondary_start_seq"),
                        row.getLong("deadline_millis"), row.getInt("decision_stripe"),
                        TxStatus.valueOf(row.getString("status")),
                        nullableLong(row, "decision_seq"),
                        nullableLong(row, "primary_reserved_seq"),
                        nullableLong(row, "secondary_reserved_seq"));
            }
        }
    }

    /** 主事务持久行可还原原注册身份及已经提交的终态。 */
    private record PrimaryRow(long txId, String primaryKey, byte[] primaryValue,
                              String secondaryKey, byte[] secondaryValue,
                              int primaryStripe, int secondaryStripe,
                              long primaryStartSequence, long secondaryStartSequence,
                              long deadlineMillis, int decisionStripe, TxStatus status,
                              Long decisionSequence, Long primarySequence, Long secondarySequence) {
        /** 后续行锁内重读仍按原注册内容核对。 */
        private Registration registration() {
            return new Registration(txId, primaryKey, primaryValue, secondaryKey,
                    secondaryValue, primaryStripe, secondaryStripe,
                    primaryStartSequence, secondaryStartSequence,
                    deadlineMillis, decisionStripe);
        }

        /** ACTIVE 和 ABORTED 没有对外可见的决议序号。 */
        private Decision decision() {
            return new Decision(txId, status, decisionStripe,
                    decisionSequence, primarySequence, secondarySequence);
        }
    }

    /** 正式业务键约束先于 JDBC 执行验证，避免 VARBINARY 截断。 */
    private static byte[] encode(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > 256
                || !key.equals(new String(bytes, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("invalid primary transaction key");
        }
        return bytes;
    }

    /** 主分片注册的二进制键恢复为原 UTF-8 字符串。 */
    private static String decode(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    /** 可空决议列不能把 SQL NULL 误认成序号零。 */
    private static Long nullableLong(ResultSet row, String column) throws SQLException {
        long value = row.getLong(column);
        return row.wasNull() ? null : value;
    }

    /** 期限以主分片数据库时钟判定，不信任协调者墙钟。 */
    private static long databaseNow(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("primary database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /** 事务号为零或决议分条超出布局时拒绝访问。 */
    private void requireStripe(int stripe) {
        if (stripe < 0 || stripe >= stripes) {
            throw new IllegalArgumentException("invalid primary decision stripe");
        }
    }

    /** 物理结果不确定时只允许读取主分片持久行判断。 */
    private static MvccException unknown(Long txId, String message, SQLException cause) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, message, true, txId, cause);
    }
}

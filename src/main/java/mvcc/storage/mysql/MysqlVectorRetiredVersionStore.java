package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MutationType;
import mvcc.common.MvccException;
import mvcc.storage.Mutation;
import mvcc.storage.Utf8KeyOrder;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;

/** 跨片旧版本退役后保留不含业务载荷的持久重放凭据。 */
final class MysqlVectorRetiredVersionStore {
    /** 仅允许正式向量布局初始化创建紧凑凭据表，不修改已有提交版本。 */
    private MysqlVectorRetiredVersionStore() {
    }

    /** 凭据身份绑定事务、键、原保留序号和载荷摘要，主键阻止事务号重用。 */
    static void ensure(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_vector_retired_v1 (
                      tx_id bigint not null primary key,
                      user_key varbinary(256) not null,
                      stripe_id smallint not null,
                      start_ts bigint not null,
                      start_seq bigint not null,
                      reserved_seq bigint not null,
                      commit_ts bigint not null,
                      deleted tinyint not null,
                      payload_sha binary(32) not null,
                      unique key mvcc_vector_retired_key_seq (user_key, reserved_seq)
                    ) engine=InnoDB
                    """);
        }
    }

    /** 旧物化调用仅在真实终态与退役凭据完整匹配且大载荷行已消失时视为幂等成功。 */
    static boolean matches(DataSource dataSource, long txId, long startTs, long commitTs)
            throws SQLException {
        Objects.requireNonNull(dataSource, "dataSource");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement("""
                     select r.tx_id from mvcc_vector_retired_v1 r
                     join mvcc_participant_txn p on p.tx_id = r.tx_id
                     where r.tx_id = ? and r.start_ts = ? and r.commit_ts = ?
                       and p.start_ts = r.start_ts and p.commit_ts = r.commit_ts
                       and p.status = 'COMMITTED'
                       and not exists(select 1 from mvcc_vector_committed_v1 c
                                      where c.tx_id = r.tx_id)
                       and not exists(select 1 from mvcc_data d where d.tx_id = r.tx_id)
                     """)) {
            query.setLong(1, txId);
            query.setLong(2, startTs);
            query.setLong(3, commitTs);
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /** 前台同号重放还要精确匹配原键、墓碑及载荷摘要，不能只凭提交时间成功。 */
    static boolean matchesExpected(Connection connection, long txId, long startTs,
                                   long commitTs, Mutation expected) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select user_key, start_ts, commit_ts, deleted, payload_sha
                from mvcc_vector_retired_v1 where tx_id = ? for update
                """)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return false;
                }
                boolean deleted = expected.type() == MutationType.DELETE;
                if (row.getLong("start_ts") != startTs || row.getLong("commit_ts") != commitTs
                        || !Arrays.equals(row.getBytes("user_key"), Utf8KeyOrder.encode(expected.key()))
                        || row.getBoolean("deleted") != deleted
                        || !Arrays.equals(row.getBytes("payload_sha"), payloadDigest(deleted, expected.value()))) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector retired replay differs from committed mutation", false, txId);
                }
                try (PreparedStatement rows = connection.prepareStatement("""
                        select exists(select 1 from mvcc_vector_committed_v1 where tx_id = ?)
                            or exists(select 1 from mvcc_data where tx_id = ?)
                        """)) {
                    rows.setLong(1, txId);
                    rows.setLong(2, txId);
                    try (ResultSet present = rows.executeQuery()) {
                        if (!present.next() || present.getBoolean(1)) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                    "vector retired receipt overlaps a live version", false, txId);
                        }
                    }
                }
                return true;
            }
        }
    }

    /** 摘要区分墓碑、SQL NULL 与空字节载荷，凭据本身不保留业务值。 */
    static byte[] payloadDigest(boolean deleted, byte[] payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((byte) (deleted ? 1 : 0));
            digest.update((byte) (payload == null ? 0 : 1));
            if (payload != null) {
                digest.update(payload);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("JDK SHA-256 provider is unavailable", ex);
        }
    }
}

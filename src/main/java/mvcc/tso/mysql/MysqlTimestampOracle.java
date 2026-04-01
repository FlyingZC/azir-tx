package mvcc.tso.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.tso.TimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class MysqlTimestampOracle implements TimestampOracle {
    private static final String SEQUENCE_NAME = "global";

    private final DataSource dataSource;

    public MysqlTimestampOracle(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void initialize(long initialValue) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement create = connection.prepareStatement("""
                     create table if not exists mvcc_tso (
                       sequence_name varchar(64) not null primary key,
                       current_value bigint not null
                     )
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

    @Override
    public long nextTimestamp() {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("""
                    select current_value
                    from mvcc_tso
                    where sequence_name = ?
                    for update
                    """);
                 PreparedStatement update = connection.prepareStatement("""
                    update mvcc_tso
                    set current_value = ?
                    where sequence_name = ?
                    """)) {
                select.setString(1, SEQUENCE_NAME);
                long current;
                try (ResultSet resultSet = select.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "tso row not found", false, null);
                    }
                    current = resultSet.getLong("current_value");
                }
                long next = current + 1;
                update.setLong(1, next);
                update.setString(2, SEQUENCE_NAME);
                update.executeUpdate();
                connection.commit();
                return next;
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError("allocate tso failed", ex, null);
        }
    }

    private MvccException storageError(String message, SQLException ex, Long txId) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}

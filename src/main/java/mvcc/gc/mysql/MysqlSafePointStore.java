package mvcc.gc.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.gc.SafePointStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class MysqlSafePointStore implements SafePointStore {
    private static final String STORE_ID = "global";

    private final DataSource dataSource;

    public MysqlSafePointStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void initialize(long initialSafePoint) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement create = connection.prepareStatement("""
                     create table if not exists mvcc_safe_point (
                       store_id varchar(64) not null primary key,
                       safe_point bigint not null
                     )
                     """);
             PreparedStatement insert = connection.prepareStatement("""
                     insert into mvcc_safe_point(store_id, safe_point)
                     values (?, ?)
                     on duplicate key update safe_point = greatest(safe_point, values(safe_point))
                     """)) {
            create.execute();
            insert.setString(1, STORE_ID);
            insert.setLong(2, initialSafePoint);
            insert.executeUpdate();
        } catch (SQLException ex) {
            throw storageError("initialize safe point failed", ex);
        }
    }

    @Override
    public long currentSafePoint() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     select safe_point from mvcc_safe_point where store_id = ?
                     """)) {
            statement.setString(1, STORE_ID);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return 0L;
                }
                return resultSet.getLong("safe_point");
            }
        } catch (SQLException ex) {
            throw storageError("load safe point failed", ex);
        }
    }

    @Override
    public long advanceTo(long candidateSafePoint) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement select = connection.prepareStatement("""
                    select safe_point from mvcc_safe_point where store_id = ? for update
                    """);
                 PreparedStatement update = connection.prepareStatement("""
                    update mvcc_safe_point set safe_point = ? where store_id = ?
                    """)) {
                select.setString(1, STORE_ID);
                long current;
                try (ResultSet resultSet = select.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "safe point row missing", false, null);
                    }
                    current = resultSet.getLong("safe_point");
                }
                long next = Math.max(current, candidateSafePoint);
                update.setLong(1, next);
                update.setString(2, STORE_ID);
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
            throw storageError("advance safe point failed", ex);
        }
    }

    private MvccException storageError(String message, SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, ex);
    }
}

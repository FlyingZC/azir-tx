package mvcc.tso.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.tso.TimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class MysqlSegmentTimestampOracle implements TimestampOracle {
    private static final String SEQUENCE_NAME = "global";

    private final DataSource dataSource;
    private final int batchSize;
    private final AtomicLong current = new AtomicLong(1L);
    private final AtomicLong max = new AtomicLong(0L);
    private final LongAdder segmentAllocations = new LongAdder();

    public MysqlSegmentTimestampOracle(DataSource dataSource, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.dataSource = dataSource;
        this.batchSize = batchSize;
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
            throw storageError("initialize tso failed", ex);
        }
    }

    @Override
    public long nextTimestamp() {
        while (true) {
            long next = current.getAndIncrement();
            if (next <= max.get()) {
                return next;
            }
            refill();
        }
    }

    private synchronized void refill() {
        if (current.get() <= max.get()) {
            return;
        }
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
                long currentValue;
                try (ResultSet resultSet = select.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE, "tso row not found", false, null);
                    }
                    currentValue = resultSet.getLong("current_value");
                }
                long start = currentValue + 1;
                long end = currentValue + batchSize;
                update.setLong(1, end);
                update.setString(2, SEQUENCE_NAME);
                update.executeUpdate();
                connection.commit();
                segmentAllocations.increment();
                current.set(start);
                max.set(end);
            } catch (SQLException | RuntimeException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw storageError("allocate tso segment failed", ex);
        }
    }

    private MvccException storageError(String message, SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, ex);
    }

    public long segmentAllocations() {
        return segmentAllocations.sum();
    }
}

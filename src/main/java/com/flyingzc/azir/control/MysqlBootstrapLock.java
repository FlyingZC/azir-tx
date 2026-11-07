package com.flyingzc.azir.control;

import javax.sql.DataSource;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** 持久命名空间建立前的跨进程初始化锁，防止首次启动并发重置 TSO 或绑定不同身份。 */
public final class MysqlBootstrapLock implements AutoCloseable {
    private final Connection connection;
    private final String name;
    private final MysqlNamedLock.Lease lease;

    /** 绑定物理控制库并取得有限等待的初始化锁，不占用业务提交门禁。 */
    public MysqlBootstrapLock(DataSource source, String protocol) {
        name = "azir:init:" + protocol + ":" + UUID.nameUUIDFromBytes(MysqlDatabaseAuthority.inspect(source).toString().getBytes(StandardCharsets.UTF_8));
        Connection acquired = null;
        try {
            acquired = source.getConnection();
            lease = MysqlNamedLock.lock(acquired, name, 10);
            connection = acquired;
        } catch (SQLException | RuntimeException | Error failure) {
            if (acquired != null) {
                MysqlNamedLock.discard(acquired, failure);
                try { acquired.close(); } catch (SQLException close) { failure.addSuppressed(close); }
            }
            if (failure instanceof SQLException sql) { throw new IllegalStateException("cannot serialize protocol initialization", sql); }
            if (failure instanceof RuntimeException runtime) { throw runtime; }
            throw (Error) failure;
        }
    }
    /** 解锁失败销毁物理连接，避免池中的会话残留初始化锁。 */
    @Override public void close() {
        Throwable problem = null;
        try { lease.close(); }
        catch (SQLException | RuntimeException | Error failure) { problem = failure; }
        finally {
            try { connection.close(); }
            catch (SQLException | RuntimeException | Error close) { if (problem == null) { problem = close; } else if (problem != close) { problem.addSuppressed(close); } }
        }
        if (problem instanceof SQLException sql) { throw new IllegalStateException("protocol initialization unlock failed", sql); }
        if (problem instanceof RuntimeException runtime) { throw runtime; }
        if (problem instanceof Error fatal) { throw fatal; }
    }
}

package mvcc.control;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.ProxyConnection;
import mvcc.common.MvccException;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 仅使用内存 JDBC 故障桩验证连接弃用，无需数据库或网络。 */
class MysqlConnectionDisposalTest {
    /** 释放锁与 abort 同时失败时，通用池连接必须保留，整个同库域停止借连接。 */
    @Test
    void failedReleaseAndAbortQuarantineGenericPoolWithoutReturningConnection() throws SQLException {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        var peer = new FaultyJdbc(jdbc.serverId);
        try (Connection ignored = MysqlConnectionContext.connection(peer.source)) {
            // 预先绑定另一个数据源，验证隔离范围覆盖整个数据库域。
        }
        var held = new AtomicReference<ConnectionState>();
        assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationLock(jdbc.source, 0, () -> {
            held.set(jdbc.last.get());
            jdbc.failRelease = true;
            jdbc.failAbort = true;
            return null;
        }));
        assertEquals(1, held.get().abortCalls);
        assertEquals(0, held.get().closeCalls);
        assertFalse(held.get().closed);
        int ownBorrows = jdbc.borrows.get();
        int peerBorrows = peer.borrows.get();
        assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(jdbc.source));
        assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(peer.source));
        assertEquals(ownBorrows, jdbc.borrows.get());
        assertEquals(peerBorrows, peer.borrows.get());
    }

    /** 独立 SQL 事务回滚和 abort 都失败后，不能通过 close 或 autoCommit 把脏连接归池。 */
    @Test
    void failedRollbackAndAbortQuarantineStandaloneTransactionConnection() {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        jdbc.failRollback = true;
        jdbc.failAbort = true;
        var error = assertThrows(SQLException.class, () -> MysqlConnectionContext.inTransaction(jdbc.source, connection -> {
            throw new SQLException("original failure");
        }));
        assertEquals("original failure", error.getMessage());
        assertEquals(2, error.getSuppressed().length);
        assertEquals(0, jdbc.last.get().closeCalls);
        assertEquals(0, jdbc.last.get().autoCommitRestores);
        assertFalse(jdbc.last.get().autoCommit);
        assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(jdbc.source));
    }

    /** 借用连接回滚失败必须污染外层发布连接，即使业务吞掉异常也不能继续发布或归池。 */
    @Test
    void failedBorrowedTransactionDiscardsTheOuterPublicationConnection() {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        var held = new AtomicReference<ConnectionState>();
        assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationLock(jdbc.source, 0, () -> {
            held.set(jdbc.last.get());
            jdbc.failRollback = true;
            jdbc.failAbort = true;
            Connection cached;
            try {
                cached = MysqlConnectionContext.connection(jdbc.source);
            } catch (SQLException ex) {
                throw new AssertionError(ex);
            }
            assertThrows(SQLException.class, () -> MysqlConnectionContext.inTransaction(jdbc.source, connection -> {
                throw new SQLException("nested failure");
            }));
            assertThrows(SQLException.class, () -> MysqlConnectionContext.connection(jdbc.source));
            assertThrows(SQLException.class, () -> cached.prepareStatement("select 1"));
            return null;
        }));
        assertEquals(0, held.get().closeCalls);
        assertEquals(0, held.get().autoCommitRestores);
        assertEquals(1, held.get().abortCalls);
    }

    /** Hikari 必须收到自己的原始代理执行显式淘汰，不能传入借用的 JDK 代理。 */
    @Test
    void hikariEvictionReceivesTheOriginalPooledProxyAfterAbortFailure() throws SQLException {
        var jdbc = new FaultyJdbc(UUID.randomUUID().toString());
        try (var pool = new RecordingHikariDataSource()) {
            pool.setDataSource(jdbc.source);
            pool.setMaximumPoolSize(1);
            pool.setMinimumIdle(0);
            pool.setConnectionTimeout(1000);
            pool.setInitializationFailTimeout(-1);
            assertThrows(MvccException.class, () -> MysqlConnectionContext.withPublicationLock(pool, 0, () -> {
                jdbc.failRelease = true;
                jdbc.failAbort = true;
                return null;
            }));
            assertInstanceOf(ProxyConnection.class, pool.evicted);
            assertSame(pool.lastBorrowed, pool.evicted);
            assertFalse(Proxy.isProxyClass(pool.evicted.getClass()));
            int previousConnections = jdbc.borrows.get();
            jdbc.failRelease = false;
            jdbc.failAbort = false;
            assertEquals(1, MysqlConnectionContext.withPublicationLock(pool, 0, () -> 1));
            assertTrue(jdbc.borrows.get() > previousConnections);
        }
    }

    /** 记录真实 Hikari 池代理淘汰，底层连接仍是纯内存故障桩。 */
    private static final class RecordingHikariDataSource extends HikariDataSource {
        private Connection lastBorrowed;
        private Connection evicted;

        /** 创建尚未启动连接池的测试对象。 */
        private RecordingHikariDataSource() {
        }

        /** 保留当前实际借出的 Hikari 代理身份。 */
        @Override
        public Connection getConnection() throws SQLException {
            lastBorrowed = super.getConnection();
            return lastBorrowed;
        }

        /** 记录淘汰对象并调用真实池淘汰实现。 */
        @Override
        public void evictConnection(Connection connection) {
            evicted = connection;
            super.evictConnection(connection);
        }
    }

    /** 为每次借连接创建独立会话，用固定服务器身份模拟同库别名。 */
    private static final class FaultyJdbc implements InvocationHandler {
        private final String serverId;
        private final DataSource source;
        private final AtomicInteger borrows = new AtomicInteger();
        private final AtomicReference<ConnectionState> last = new AtomicReference<>();
        private volatile boolean failRelease;
        private volatile boolean failRollback;
        private volatile boolean failAbort;

        /** 创建不连接任何外部服务的数据源。 */
        private FaultyJdbc(String serverId) {
            this.serverId = serverId;
            source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, this);
        }

        /** 分配内存会话并提供连接池初始化所需的基础数据源行为。 */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "getConnection" -> {
                    borrows.incrementAndGet();
                    var state = new ConnectionState(this);
                    last.set(state);
                    yield state.connection;
                }
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                case "toString" -> "InMemoryFaultDataSource";
                case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    /** 独立记录关闭、回滚、abort 和 autoCommit 恢复，避免仅验证异常类型。 */
    private static final class ConnectionState implements InvocationHandler {
        private final FaultyJdbc jdbc;
        private final Connection connection;
        private volatile boolean autoCommit = true;
        private volatile boolean closed;
        private int closeCalls;
        private int abortCalls;
        private int autoCommitRestores;

        /** 创建底层 JDBC 会话代理。 */
        private ConnectionState(FaultyJdbc jdbc) {
            this.jdbc = jdbc;
            connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, this);
        }

        /** 实现控制面和 Hikari 初始化所需的最小 JDBC 会话语义。 */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                case "toString" -> "InMemoryFaultConnection";
                case "getAutoCommit" -> autoCommit;
                case "setAutoCommit" -> {
                    if (!autoCommit && (boolean) args[0]) {
                        autoCommitRestores++;
                    }
                    autoCommit = (boolean) args[0];
                    yield null;
                }
                case "getTransactionIsolation" -> Connection.TRANSACTION_READ_COMMITTED;
                case "getCatalog", "getSchema" -> "control";
                case "isValid" -> !closed;
                case "isClosed" -> closed;
                case "close" -> { closeCalls++; closed = true; yield null; }
                case "abort" -> {
                    abortCalls++;
                    if (jdbc.failAbort) {
                        throw new SQLException("abort failed", "HY000");
                    }
                    closed = true;
                    yield null;
                }
                case "rollback" -> {
                    if (jdbc.failRollback) {
                        throw new SQLException("rollback failed", "HY000");
                    }
                    yield null;
                }
                case "prepareStatement" -> statement((String) args[0]);
                case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                case "unwrap" -> ((Class<?>) args[0]).cast(proxy);
                default -> defaultValue(method.getReturnType());
            };
        }

        /** 模拟命名锁和域查询，在指定释放语句上注入普通 SQL 错误。 */
        private PreparedStatement statement(String sql) {
            return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                        if (method.getName().equals("executeQuery")) {
                            if (sql.contains("release_lock") && jdbc.failRelease) {
                                throw new SQLException("release failed", "HY000");
                            }
                            return row(sql);
                        }
                        if (method.getName().equals("getConnection")) {
                            return connection;
                        }
                        return defaultValue(method.getReturnType());
                    });
        }

        /** 为身份查询和命名锁查询返回一行结果。 */
        private ResultSet row(String sql) {
            var position = new AtomicInteger();
            return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "next" -> position.getAndIncrement() == 0;
                        case "getString" -> sql.contains("server_uuid") && ((Integer) args[0]) == 1 ? jdbc.serverId : "control";
                        case "getObject", "getInt" -> 1;
                        case "getLong" -> 1L;
                        default -> defaultValue(method.getReturnType());
                    });
        }
    }

    /** 返回未使用 JDBC 方法所需的空值或原始类型默认值。 */
    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }
}

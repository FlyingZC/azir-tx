package mvcc.control;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.ProxyConnection;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** MySQL 控制面发布锁与连接上下文；同库元数据、TSO 和安全点复用持锁连接。 */
public final class MysqlConnectionContext {
    private static final Map<DataSource, Domain> DOMAINS = Collections.synchronizedMap(new WeakHashMap<>());
    private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();
    private static final Map<Domain, ReentrantLock> LOCAL_LOCKS = new ConcurrentHashMap<>();
    private static final Map<Domain, Set<ConnectionGuard>> QUARANTINED = new ConcurrentHashMap<>();

    /** 仅提供静态连接协调能力。 */
    private MysqlConnectionContext() {
    }

    /** 使用服务器 UUID 和实际数据库绑定域，让不同 JDBC 地址别名也共用进程内锁。 */
    private record Domain(String serverId, String database) {
    }

    /** 获取控制面连接；屏障内返回关闭无副作用的借用视图，禁止再次等待同池连接。 */
    public static Connection connection(DataSource dataSource) throws SQLException {
        requireAvailable(DOMAINS.get(dataSource));
        Scope current = CURRENT.get();
        if (current != null) {
            Domain domain = DOMAINS.get(dataSource);
            if (!current.domain.equals(domain)) {
                // 数据源必须在进入屏障前通过 initialize 绑定，无法确认同库时失败关闭。
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "control datasource must be initialized in the same database domain before publication", false, null);
            }
            current.requireOwnership();
            return borrowed(current);
        }
        Connection connection = dataSource.getConnection();
        try {
            try (PreparedStatement statement = connection.prepareStatement("select @@server_uuid, database()");
                 ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getString(1) == null || result.getString(2) == null) {
                    throw new SQLException("control connection requires a server identity and selected database");
                }
                DOMAINS.put(dataSource, new Domain(result.getString(1), result.getString(2)));
            }
            requireAvailable(DOMAINS.get(dataSource));
            return connection;
        } catch (SQLException | RuntimeException | Error ex) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                ex.addSuppressed(closeFailure);
            }
            throw ex;
        }
    }

    /** 使用数据库域命名锁串行发布；同线程同域重入不重复 GET_LOCK。 */
    public static <T> T withPublicationLock(DataSource dataSource, int timeoutSeconds, Supplier<T> action) {
        if (timeoutSeconds < 0) {
            throw new IllegalArgumentException("publication lock timeout must be >= 0");
        }
        Scope current = CURRENT.get();
        if (current != null) {
            if (!current.domain.equals(DOMAINS.get(dataSource))) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "nested publication domain mismatch", false, null);
            }
            try {
                current.requireOwnership();
            } catch (SQLException ex) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication connection lost", false, null, ex);
            }
            return action.get();
        }
        Domain domain = domain(dataSource);
        ReentrantLock localLock = LOCAL_LOCKS.computeIfAbsent(domain, ignored -> new ReentrantLock(true));
        try {
            // 必须先等待进程内域锁，再借控制池连接；每进程仅一条连接等待同域 GET_LOCK。
            if (!localLock.tryLock(timeoutSeconds, TimeUnit.SECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT, "local publication lock timed out", true, null);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication lock wait interrupted", true, null, ex);
        }
        try (ConnectionGuard guard = new ConnectionGuard(dataSource, connection(dataSource), domain)) {
            Connection connection = guard.connection;
            if (!connection.getAutoCommit()) {
                throw new SQLException("publication connection must use autoCommit");
            }
            if (!domain.equals(DOMAINS.get(dataSource))) {
                throw new SQLException("publication datasource changed database domain");
            }
            String lockName = lockName(domain.database());
            try (Scope scope = new Scope(guard, domain, lockName)) {
                try (PreparedStatement lock = connection.prepareStatement("select get_lock(?, ?)")) {
                    lock.setString(1, lockName);
                    lock.setInt(2, timeoutSeconds);
                    // 请求发出后即可能已获得锁，连结果集关闭异常也必须进入释放路径。
                    scope.mayHoldLock = true;
                    try (ResultSet result = lock.executeQuery()) {
                        if (!result.next() || result.getObject(1) == null) {
                            throw new SQLException("GET_LOCK returned NULL for publication barrier");
                        }
                        if (result.getInt(1) != 1) {
                            scope.mayHoldLock = false;
                            throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                                    "publication GET_LOCK timed out after " + timeoutSeconds + " seconds", true, null);
                        }
                    }
                }
                CURRENT.set(scope);
                T result = action.get();
                scope.requireOwnership();
                return result;
            }
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "publication lock failed", true, null, ex);
        } finally {
            localLock.unlock();
        }
    }

    /** 首次使用先短暂绑定数据库身份，不持有池连接等待进程内发布锁。 */
    private static Domain domain(DataSource dataSource) {
        Domain domain = DOMAINS.get(dataSource);
        if (domain != null) {
            return domain;
        }
        try (Connection ignored = connection(dataSource)) {
            return DOMAINS.get(dataSource);
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "bind publication database failed", true, null, ex);
        }
    }

    /** 无法确认销毁连接的数据库域停止全部控制操作，直到运维修复并重建进程。 */
    private static void requireAvailable(Domain domain) throws SQLException {
        if (domain != null && QUARANTINED.containsKey(domain)) {
            throw new SQLException("control database domain quarantined after connection disposal failure");
        }
    }

    /** JDBC 事务函数，异常交由统一清理路径处理。 */
    @FunctionalInterface
    public interface SqlAction<T> {
        /** 使用当前控制面连接完成一段数据库事务。 */
        T execute(Connection connection) throws SQLException;
    }

    /** 在借用或独占连接上执行短事务，回滚失败时销毁连接而不是通过 autoCommit 隐式提交。 */
    public static <T> T inTransaction(DataSource dataSource, SqlAction<T> action) throws SQLException {
        Scope scope = CURRENT.get();
        if (scope != null) {
            return executeTransaction(connection(dataSource), scope.guard, action);
        }
        Connection connection = connection(dataSource);
        try (ConnectionGuard guard = new ConnectionGuard(dataSource, connection, DOMAINS.get(dataSource))) {
            return executeTransaction(connection, guard, action);
        }
    }

    /** 原始连接的归还权仅由 guard 管理，借用连接回滚失败也必须污染外层发布作用域。 */
    private static <T> T executeTransaction(Connection connection, ConnectionGuard guard, SqlAction<T> action) throws SQLException {
        if (!connection.getAutoCommit()) {
            throw new SQLException("nested control SQL transactions are not supported");
        }
        try {
            connection.setAutoCommit(false);
            T result = action.execute(connection);
            connection.commit();
            connection.setAutoCommit(true);
            return result;
        } catch (SQLException | RuntimeException | Error ex) {
            try {
                connection.rollback();
                connection.setAutoCommit(true);
            } catch (SQLException | RuntimeException | Error rollbackFailure) {
                if (rollbackFailure != ex) {
                    ex.addSuppressed(rollbackFailure);
                }
                guard.discard(ex);
            }
            throw ex;
        }
    }

    /** 管理原始池连接的归还权，弃用后绝不走普通 close 回池路径。 */
    private static final class ConnectionGuard implements AutoCloseable {
        private final DataSource dataSource;
        private final Connection connection;
        private final Domain domain;
        private boolean discarded;

        /** 保存实际借出该连接的池，淘汰时不能把借用视图传给池。 */
        private ConnectionGuard(DataSource dataSource, Connection connection, Domain domain) {
            this.dataSource = dataSource;
            this.connection = connection;
            this.domain = domain;
        }

        /** 先中止会话，再显式淘汰 Hikari 代理；通用池无法淘汰时保留连接并隔离域。 */
        private void discard(Throwable failure) {
            if (discarded) {
                return;
            }
            discarded = true;
            try {
                connection.abort(Runnable::run);
            } catch (SQLException | RuntimeException | Error abortFailure) {
                if (abortFailure != failure) {
                    failure.addSuppressed(abortFailure);
                }
            }
            try {
                // evictConnection 对非 Hikari 代理会静默不处理，必须检查池和原始代理类型。
                if (dataSource instanceof HikariDataSource hikari && connection instanceof ProxyConnection
                        && connection.getClass().getName().startsWith("com.zaxxer.hikari.")
                        && !hikari.isClosed() && hikari.getHikariPoolMXBean() != null) {
                    hikari.evictConnection(connection);
                    return;
                }
            } catch (RuntimeException | Error evictionFailure) {
                if (evictionFailure != failure) {
                    failure.addSuppressed(evictionFailure);
                }
            }
            // 强引用保留原始连接，防止普通归还路径把仍持锁或事务未结束的连接交给其他请求。
            QUARANTINED.computeIfAbsent(domain, ignored -> ConcurrentHashMap.newKeySet()).add(this);
        }

        /** 只有确认未被弃用的正常连接可以归还到池。 */
        @Override
        public void close() throws SQLException {
            if (!discarded) {
                connection.close();
            }
        }
    }

    /** 以数据库名摘要构造不超过 MySQL 64 字符限制的锁名。 */
    private static String lockName(String database) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(database.getBytes(StandardCharsets.UTF_8));
            return "mvcc:pub:" + HexFormat.of().formatHex(digest, 0, 24);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    /** 返回借用连接；close 不归还池，unwrap 不暴露持锁连接。 */
    private static Connection borrowed(Scope scope) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    if (CURRENT.get() != scope || scope.broken || scope.guard.discarded) {
                        throw new SQLException("publication connection view is no longer usable");
                    }
                    requireAvailable(scope.domain);
                    if (method.getName().equals("unwrap")) {
                        if (((Class<?>) args[0]).isInstance(proxy)) {
                            return proxy;
                        }
                        throw new SQLException("cannot unwrap a publication connection");
                    }
                    if (method.getName().equals("isWrapperFor")) {
                        return ((Class<?>) args[0]).isInstance(proxy);
                    }
                    try {
                        return method.invoke(scope.connection, args);
                    } catch (InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
    }

    /** 屏障退出时清理线程上下文，并在释放失败时销毁连接以免锁随连接回池。 */
    private static final class Scope implements AutoCloseable {
        private final ConnectionGuard guard;
        private final Connection connection;
        private final Domain domain;
        private final String lockName;
        private boolean mayHoldLock;
        private boolean broken;

        /** 保存持锁连接及其唯一数据库域。 */
        private Scope(ConnectionGuard guard, Domain domain, String lockName) {
            this.guard = guard;
            this.connection = guard.connection;
            this.domain = domain;
            this.lockName = lockName;
        }

        /** 每次借用校验会话仍持有命名锁；失效后本次屏障永不换连接继续执行。 */
        private void requireOwnership() throws SQLException {
            requireAvailable(domain);
            if (broken || guard.discarded || connection.isClosed()) {
                broken = true;
                throw new SQLException("publication connection is no longer usable");
            }
            try (PreparedStatement check = connection.prepareStatement("select is_used_lock(?) = connection_id()")) {
                check.setString(1, lockName);
                try (ResultSet result = check.executeQuery()) {
                    if (!result.next() || result.getObject(1) == null || result.getInt(1) != 1) {
                        throw new SQLException("publication connection no longer owns GET_LOCK");
                    }
                }
            } catch (SQLException ex) {
                broken = true;
                throw ex;
            }
        }

        /** 异常路径也释放命名锁，保留调用失败和释放失败的异常链。 */
        @Override
        public void close() throws SQLException {
            CURRENT.remove();
            if (!mayHoldLock || guard.discarded) {
                return;
            }
            try (PreparedStatement release = connection.prepareStatement("select release_lock(?)")) {
                release.setString(1, lockName);
                try (ResultSet result = release.executeQuery()) {
                    if (!result.next() || result.getObject(1) == null || result.getInt(1) != 1) {
                        throw new SQLException("RELEASE_LOCK did not confirm publication lock release");
                    }
                }
            } catch (SQLException | RuntimeException | Error ex) {
                guard.discard(ex);
                throw ex;
            }
        }
    }
}

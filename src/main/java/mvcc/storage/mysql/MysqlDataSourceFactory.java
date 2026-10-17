package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * MySQL 数据源工厂，负责创建连接池。
 */
public final class MysqlDataSourceFactory {
    private static final int DEFAULT_MAX_POOL_SIZE = 64;
    private static final int DEFAULT_MIN_IDLE = 2;
    private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 3_000L;

    /**
     * 构造 MysqlDataSourceFactory 实例。
     */
    private MysqlDataSourceFactory() {
    }

    /**
     * 创建组件实例或事务记录。
     */
    public static DataSource create(String jdbcUrl, String username, String password) {
        return createPool(jdbcUrl, username, password,
                envInt("AZIR_TX_JDBC_MAX_POOL_SIZE", DEFAULT_MAX_POOL_SIZE),
                envInt("AZIR_TX_JDBC_MIN_IDLE", DEFAULT_MIN_IDLE),
                envLong("AZIR_TX_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS),
                envInt("AZIR_TX_JDBC_CONNECT_TIMEOUT_MS", 3_000),
                envInt("AZIR_TX_JDBC_SOCKET_TIMEOUT_MS", 10_000));
    }

    /**
     * 创建恢复或 GC 专用的小连接池，与前台池使用独立的连接预算。
     */
    public static DataSource createBackground(String jdbcUrl, String username, String password) {
        return createPool(jdbcUrl, username, password,
                envInt("AZIR_TX_BACKGROUND_JDBC_MAX_POOL_SIZE", 4),
                envInt("AZIR_TX_BACKGROUND_JDBC_MIN_IDLE", 0),
                envLong("AZIR_TX_BACKGROUND_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS),
                envInt("AZIR_TX_BACKGROUND_JDBC_CONNECT_TIMEOUT_MS", 3_000),
                envInt("AZIR_TX_BACKGROUND_JDBC_SOCKET_TIMEOUT_MS", 5_000));
    }

    /** 组成员池延迟连接，单个离线成员不能阻止组发现器连接仍具备法定多数的主节点。 */
    public static DataSource createGroupMember(String jdbcUrl, String username, String password) {
        return createPool(jdbcUrl, username, password,
                envInt("AZIR_TX_JDBC_MAX_POOL_SIZE", DEFAULT_MAX_POOL_SIZE), 0,
                envLong("AZIR_TX_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS),
                envInt("AZIR_TX_JDBC_CONNECT_TIMEOUT_MS", 3_000),
                envInt("AZIR_TX_JDBC_SOCKET_TIMEOUT_MS", 10_000), true);
    }

    /**
     * 校验连接预算并配置前后台共用的 JDBC 批处理能力。
     */
    private static DataSource createPool(String jdbcUrl, String username, String password,
                                         int maximumPoolSize, int minimumIdle, long connectionTimeout,
                                         int connectTimeout, int socketTimeout) {
        return createPool(jdbcUrl, username, password, maximumPoolSize, minimumIdle, connectionTimeout, connectTimeout, socketTimeout, false);
    }

    /** 只有显式组成员允许延迟首连，主节点和法定多数仍由组发现器严格验证。 */
    private static DataSource createPool(String jdbcUrl, String username, String password,
                                         int maximumPoolSize, int minimumIdle, long connectionTimeout,
                                         int connectTimeout, int socketTimeout, boolean lazyInitialization) {
        if (maximumPoolSize < 1 || minimumIdle < 0 || minimumIdle > maximumPoolSize) {
            throw new IllegalArgumentException("pool size must be positive and minimumIdle within the pool budget");
        }
        if (connectTimeout < 1 || socketTimeout < 1) {
            throw new IllegalArgumentException("JDBC connect and socket timeouts must be positive");
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setConnectionTimeout(connectionTimeout);
        config.setAutoCommit(true);
        if (lazyInitialization) { config.setInitializationFailTimeout(-1); }
        // 后台时间预算只能在操作之间检查；驱动超时负责限制连接和网络读取阻塞。
        config.addDataSourceProperty("connectTimeout", Integer.toString(connectTimeout));
        config.addDataSourceProperty("socketTimeout", Integer.toString(socketTimeout));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        return new HikariDataSource(config);
    }

    /**
     * 读取整型环境变量配置。
     */
    private static int envInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    /**
     * 读取长整型环境变量配置。
     */
    private static long envLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}

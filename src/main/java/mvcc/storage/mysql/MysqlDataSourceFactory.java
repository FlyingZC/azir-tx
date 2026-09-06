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
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(envInt("AZIR_TX_JDBC_MAX_POOL_SIZE", DEFAULT_MAX_POOL_SIZE));
        config.setMinimumIdle(envInt("AZIR_TX_JDBC_MIN_IDLE", DEFAULT_MIN_IDLE));
        config.setConnectionTimeout(envLong("AZIR_TX_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS));
        config.setAutoCommit(true);
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

package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public final class MysqlDataSourceFactory {
    private static final int DEFAULT_MAX_POOL_SIZE = 16;
    private static final int DEFAULT_MIN_IDLE = 2;
    private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 3_000L;

    private MysqlDataSourceFactory() {
    }

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

    private static int envInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long envLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}

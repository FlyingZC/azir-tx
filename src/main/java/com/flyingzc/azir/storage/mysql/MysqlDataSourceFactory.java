/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * MySQL Data Source Plant, responsible for creating connection pool.
 */
public final class MysqlDataSourceFactory {

    private static final int DEFAULT_MAX_POOL_SIZE = 64;

    private static final int DEFAULT_MIN_IDLE = 2;

    private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 3_000L;

    /**
     * Construct a MysqlDataSourceFactory instance.
     */
    private MysqlDataSourceFactory() {
    }

    /**
     * Create the component or transaction record.
     *
     * @param jdbcUrl jdbc url
     * @param username username
     * @param password password
     * @return the operation result
     */
    public static DataSource create(final String jdbcUrl, final String username, final String password) {
        return createPool(jdbcUrl, username, password,
                envInt("AZIR_TX_JDBC_MAX_POOL_SIZE", DEFAULT_MAX_POOL_SIZE),
                envInt("AZIR_TX_JDBC_MIN_IDLE", DEFAULT_MIN_IDLE),
                envLong("AZIR_TX_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS),
                envInt("AZIR_TX_JDBC_CONNECT_TIMEOUT_MS", 3_000),
                envInt("AZIR_TX_JDBC_SOCKET_TIMEOUT_MS", 10_000));
    }

    /**
     * Creates a small connection pool for recovery or GC, with a separate connection budget for the foreground pool.
     *
     * @param jdbcUrl jdbc url
     * @param username username
     * @param password password
     * @return the operation result
     */
    public static DataSource createBackground(final String jdbcUrl, final String username, final String password) {
        return createPool(jdbcUrl, username, password,
                envInt("AZIR_TX_BACKGROUND_JDBC_MAX_POOL_SIZE", 4),
                envInt("AZIR_TX_BACKGROUND_JDBC_MIN_IDLE", 0),
                envLong("AZIR_TX_BACKGROUND_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS),
                envInt("AZIR_TX_BACKGROUND_JDBC_CONNECT_TIMEOUT_MS", 3_000),
                envInt("AZIR_TX_BACKGROUND_JDBC_SOCKET_TIMEOUT_MS", 5_000));
    }

    /**
     * Group members pool connection delayed and individual offline members could not prevent the group detector connection still having the main node for which a majority is required.
     *
     * @param jdbcUrl jdbc url
     * @param username username
     * @param password password
     * @return the operation result
     */
    public static DataSource createGroupMember(final String jdbcUrl, final String username, final String password) {
        return createPool(jdbcUrl, username, password,
                envInt("AZIR_TX_JDBC_MAX_POOL_SIZE", DEFAULT_MAX_POOL_SIZE), 0,
                envLong("AZIR_TX_JDBC_CONNECTION_TIMEOUT_MS", DEFAULT_CONNECTION_TIMEOUT_MS),
                envInt("AZIR_TX_JDBC_CONNECT_TIMEOUT_MS", 3_000),
                envInt("AZIR_TX_JDBC_SOCKET_TIMEOUT_MS", 10_000), true);
    }

    /**
     * Validate connection budget and configure pre- background joint JDBC batch processing capability.
     *
     * @param jdbcUrl jdbc url
     * @param username username
     * @param password password
     * @param maximumPoolSize maximum pool size
     * @param minimumIdle minimum idle
     * @param connectionTimeout connection timeout
     * @param connectTimeout connect timeout
     * @param socketTimeout socket timeout
     * @return the operation result
     */
    private static DataSource createPool(final String jdbcUrl, final String username, final String password,
                                         final int maximumPoolSize, final int minimumIdle, final long connectionTimeout,
                                         final int connectTimeout, final int socketTimeout) {
        return createPool(jdbcUrl, username, password, maximumPoolSize, minimumIdle, connectionTimeout, connectTimeout, socketTimeout, false);
    }

    /**
     * Only the visible group members are allowed to delay the first company, and the main node and required majority remain strictly verify by the group detector.
     *
     * @param jdbcUrl jdbc url
     * @param username username
     * @param password password
     * @param maximumPoolSize maximum pool size
     * @param minimumIdle minimum idle
     * @param connectionTimeout connection timeout
     * @param connectTimeout connect timeout
     * @param socketTimeout socket timeout
     * @param lazyInitialization lazy initialization
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static DataSource createPool(final String jdbcUrl, final String username, final String password,
                                         final int maximumPoolSize, final int minimumIdle, final long connectionTimeout,
                                         final int connectTimeout, final int socketTimeout, final boolean lazyInitialization) {
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
        if (lazyInitialization) {
            config.setInitializationFailTimeout(-1);
        }
        // Background Time budget can only be checked between operations; the driver timeout is responsible for limiting connection and web access barriers.
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
     * Reads an integer environment variable configuration.
     *
     * @param key business key
     * @param defaultValue default value
     * @return the operation result
     */
    private static int envInt(final String key, final int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    /**
     * Reads the long integer environment variable configuration.
     *
     * @param key business key
     * @param defaultValue default value
     * @return the operation result
     */
    private static long envLong(final String key, final long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}

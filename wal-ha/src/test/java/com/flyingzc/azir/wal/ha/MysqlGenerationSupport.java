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

package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.MySQLContainer;
import java.time.Duration;

/**
 * For the current round only, there are MySQL examples with clear resource ceilings for control and operation pools.
 */
final class MysqlGenerationSupport {

    /**
     * Testing resource tools does not allow for exemplification.
     */
    private MysqlGenerationSupport() {
    }

    /**
     * Preserve return loop listening ports outside the reach of a common client ' s temporary port to avoid Docker random mapping.
     *
     * @return the operation result
     * @throws java.io.IOException if the operation cannot complete
     */
    static java.net.ServerSocket reservePeerPort() throws java.io.IOException {
        for (int attempt = 0; attempt < 128; attempt++) {
            var socket = new java.net.ServerSocket();
            try {
                socket.bind(new java.net.InetSocketAddress("127.0.0.1", java.util.concurrent.ThreadLocalRandom.current().nextInt(12000, 30000)));
                return socket;
            } catch (final java.net.BindException occupied) {
                socket.close();
            } catch (final java.io.IOException failure) {
                socket.close();
                throw failure;
            }
        }
        throw new java.io.IOException("owned test peer port reservation exhausted");
    }

    /**
     * For each example, one quarter of the core, 512 MiB, three replica and up to three quarters of the nuclear database CPU.
     *
     * @return the operation result
     */
    static MySQLContainer<?> database() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("ha_control").withUsername("wal_ha").withPassword("wal_ha")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000").withUrlParam("rewriteBatchedStatements", "true")
                .withCommand("--innodb-buffer-pool-size=64M", "--performance-schema=OFF", "--max-connections=64", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1")
                .withStartupTimeout(Duration.ofMinutes(3)).withCreateContainerCmdModifier(command -> command.getHostConfig().withNanoCPUs(250_000_000L).withMemory(512L * 1024 * 1024));
    }

    /**
     * Only test containers create their own namespace with root, and the normal product API does not have any receipt in it.
     *
     * @param database database
     * @param schema schema
     * @return the operation result
     */
    static HikariDataSource control(final MySQLContainer<?> database, final String schema) {
        var config = new HikariConfig();
        config.setJdbcUrl(database.getJdbcUrl());
        config.setCatalog(schema);
        config.setUsername("root");
        config.setPassword(database.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /**
     * Test only database names from your own source, DDL does not accept business SQL or any remote name.
     *
     * @param database database
     * @param schema schema
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static void createControl(final MySQLContainer<?> database, final String schema) throws Exception {
        if (!schema.matches("c_[a-f0-9]{32}")) {
            throw new IllegalArgumentException("invalid test control name");
        }
        try (var pool = control(database, database.getDatabaseName()); var connection = pool.getConnection(); var ddl = connection.createStatement()) {
            ddl.executeUpdate("create database `" + schema + "`");
        }
    }
}

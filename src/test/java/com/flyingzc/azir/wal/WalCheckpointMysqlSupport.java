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

package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.MySQLContainer;
import java.time.Duration;

/**
 * This round has a low load verify dedicated database resource boundary and does not modify the user ' s existing service.
 */
final class WalCheckpointMysqlSupport {

    /**
     * Prohibits the field-testing resource tool.
     */
    private WalCheckpointMysqlSupport() {
    }

    /**
     * Each independent test example uses a maximum of semi-nucleus CPU and 512 MiB memory.
     *
     * @return the operation result
     */
    static MySQLContainer<?> container() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("wal_checkpoint")
                .withUsername("wal_test").withPassword("wal_test")
                .withUrlParam("rewriteBatchedStatements", "true").withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000")
                .withCommand("--innodb-buffer-pool-size=64M", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1",
                        "--performance-schema=OFF", "--max-connections=32")
                .withStartupTimeout(Duration.ofMinutes(3))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(512L * 1024 * 1024).withNanoCPUs(500_000_000L));
    }

    /**
     * Limit connection pool, background materialization and collection are all completed by limited connection.
     *
     * @param database database
     * @return the operation result
     */
    static HikariDataSource pool(final MySQLContainer<?> database) {
        var config = new HikariConfig();
        config.setJdbcUrl(database.getJdbcUrl());
        config.setUsername(database.getUsername());
        config.setPassword(database.getPassword());
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }
}

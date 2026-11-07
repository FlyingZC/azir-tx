package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.MySQLContainer;
import java.time.Duration;

/** 本轮低负载验证专用数据库资源边界，不修改用户已有服务。 */
final class WalCheckpointMysqlSupport {
    /** 禁止实例化测试资源工具。 */
    private WalCheckpointMysqlSupport() { }

    /** 每个独立测试实例最多使用半核 CPU 与 512 MiB 内存。 */
    static MySQLContainer<?> container() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("wal_checkpoint")
                .withUsername("wal_test").withPassword("wal_test")
                .withUrlParam("rewriteBatchedStatements", "true").withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000")
                .withCommand("--innodb-buffer-pool-size=64M", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1",
                        "--performance-schema=OFF", "--max-connections=32")
                .withStartupTimeout(Duration.ofMinutes(3))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(512L * 1024 * 1024).withNanoCPUs(500_000_000L));
    }

    /** 限制测试连接池，后台物化与回收都通过有限连接完成。 */
    static HikariDataSource pool(MySQLContainer<?> database) {
        var config = new HikariConfig(); config.setJdbcUrl(database.getJdbcUrl());
        config.setUsername(database.getUsername()); config.setPassword(database.getPassword());
        config.setMaximumPoolSize(3); config.setMinimumIdle(0); config.setConnectionTimeout(5000); config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }
}

package mvcc.wal.ha;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.MySQLContainer;
import java.time.Duration;

/** 仅用于本轮自有 MySQL 实例，控制池和业务池均有明确资源上限。 */
final class MysqlGenerationSupport {
    /** 测试资源工具不允许实例化。 */
    private MysqlGenerationSupport() { }

    /** 预留常见客户端临时端口范围之外的回环监听端口，避免 Docker 随机映射抢占。 */
    static java.net.ServerSocket reservePeerPort() throws java.io.IOException {
        for (int attempt = 0; attempt < 128; attempt++) {
            var socket = new java.net.ServerSocket();
            try { socket.bind(new java.net.InetSocketAddress("127.0.0.1", java.util.concurrent.ThreadLocalRandom.current().nextInt(12000, 30000))); return socket; }
            catch (java.net.BindException occupied) { socket.close(); }
            catch (java.io.IOException failure) { socket.close(); throw failure; }
        }
        throw new java.io.IOException("owned test peer port reservation exhausted");
    }

    /** 每实例四分之一核、512 MiB，三个副本最多占用四分之三核数据库 CPU。 */
    static MySQLContainer<?> database() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("ha_control").withUsername("wal_ha").withPassword("wal_ha")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000").withUrlParam("rewriteBatchedStatements", "true")
                .withCommand("--innodb-buffer-pool-size=64M", "--performance-schema=OFF", "--max-connections=64", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1")
                .withStartupTimeout(Duration.ofMinutes(3)).withCreateContainerCmdModifier(command -> command.getHostConfig().withNanoCPUs(250_000_000L).withMemory(512L * 1024 * 1024));
    }

    /** 仅测试容器使用 root 创建自有命名空间，普通产品 API 不内置任何凭据。 */
    static HikariDataSource control(MySQLContainer<?> database, String schema) {
        var config = new HikariConfig(); config.setJdbcUrl(database.getJdbcUrl()); config.setCatalog(schema); config.setUsername("root"); config.setPassword(database.getPassword());
        config.setMaximumPoolSize(2); config.setMinimumIdle(0); config.setConnectionTimeout(5000); config.setValidationTimeout(1000); return new HikariDataSource(config);
    }

    /** 测试只接受自身派生的数据库名，DDL 不接收业务 SQL 或任意远端名称。 */
    static void createControl(MySQLContainer<?> database, String schema) throws Exception {
        if (!schema.matches("c_[a-f0-9]{32}")) { throw new IllegalArgumentException("invalid test control name"); }
        try (var pool = control(database, database.getDatabaseName()); var connection = pool.getConnection(); var ddl = connection.createStatement()) { ddl.executeUpdate("create database `" + schema + "`"); }
    }
}

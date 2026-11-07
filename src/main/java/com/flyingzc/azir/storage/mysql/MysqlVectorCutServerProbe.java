package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** 独立 JVM 故障注入专用的回环切面服务，不初始化或重置数据库。 */
public final class MysqlVectorCutServerProbe {
    /** 探针入口不保存进程间可变状态。 */
    private MysqlVectorCutServerProbe() {
    }

    /** 必须显式启用并提供三个已初始化数据库与唯一回环端口。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(System.getenv("AZIR_TX_VECTOR_TEST_SERVER"))
                || args.length < 7 || args.length > 8) {
            throw new IllegalArgumentException("vector cut server probe requires explicit test flag and endpoint arguments");
        }
        String username = args[3];
        String password = args[4];
        int port = Integer.parseInt(args[5]);
        long leaseMillis = Long.parseLong(args[6]);
        try (HikariDataSource center = pool(args[0], username, password);
             HikariDataSource first = pool(args[1], username, password);
             HikariDataSource second = pool(args[2], username, password)) {
            Map<String, DataSource> routes = Map.of("s1", first, "s2", second);
            var ledger = new MysqlLocalFirstPublicationLedger(center);
            var leases = new MysqlVectorCutLeaderLease(center, routes, 4);
            try (var runtime = new MysqlVectorCutLeaderRuntime(center, routes, 4,
                    ledger::currentPublished, leases, "127.0.0.1:" + port,
                    leaseMillis, 200)) {
                if (!runtime.start()) {
                    throw new IllegalStateException("vector cut probe found another active leader");
                }
                String transport = args.length == 8 ? args[7] : "http";
                if ("tcp".equals(transport)) {
                    try (var server = new MysqlVectorCutTcpServer(runtime, port)) {
                        server.start();
                        announceReady(runtime);
                    }
                } else if ("http".equals(transport)) {
                    try (var server = new MysqlVectorCutHttpServer(runtime, port)) {
                        server.start();
                        announceReady(runtime);
                    }
                } else {
                    throw new IllegalArgumentException("unknown vector cut test transport");
                }
            }
        }
    }

    /** 三个独立 JDBC 池模拟发布服务进程自己的物理连接。 */
    private static HikariDataSource pool(String url, String username, String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /** 只有监听成功且租约仍有效后才向父进程报告就绪。 */
    private static void announceReady(MysqlVectorCutLeaderRuntime runtime) throws InterruptedException {
        System.out.println("VECTOR_SERVER_READY generation="
                + runtime.currentView().generation());
        System.out.flush();
        new CountDownLatch(1).await();
    }
}

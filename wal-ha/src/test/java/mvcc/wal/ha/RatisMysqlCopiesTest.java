package mvcc.wal.ha;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

/** 三份独立 MySQL 冷数据，不能用共享数据库或未落盘内存伪装副本高可用。 */
class RatisMysqlCopiesTest {
    @TempDir Path directory;

    /** 截断本地 WAL 并淘汰所有热点后，原领导者及其 MySQL 同时离线仍可读确认数据。 */
    @Test void independentColdCopiesSurviveOneWholeNodeUnavailable() throws Exception {
        var databases = List.of(database(), database(), database()); var nodes = new ArrayList<RatisWalNode>(); var pools = new ArrayList<HikariDataSource>();
        try {
            Startables.deepStart(databases.stream()).join();
            for (var database : databases) {
                var result = database.execInContainer("mysql", "-uroot", "-p" + database.getPassword(), "-e",
                        "create database ha_b; grant all on ha_b.* to 'wal_ha'@'%'"); assertEquals(0, result.getExitCode(), result.getStderr());
                var host = database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec().getHostConfig();
                assertEquals(250_000_000L, host.getNanoCPUs()); assertEquals(512L * 1024 * 1024, host.getMemory());
            }
            var peers = new ArrayList<RaftPeer>();
            for (int i = 0; i < 3; i++) {
                try (var socket = MysqlGenerationSupport.reservePeerPort()) {
                    peers.add(RaftPeer.newBuilder().setId("sql" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
                }
            }
            var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), peers);
            for (int i = 0; i < 3; i++) {
                var a = pool(databases.get(i), "ha_a"); var b = pool(databases.get(i), "ha_b"); pools.add(a); pools.add(b);
                var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
                nodes.add(RatisWalNode.openLoopback(directory.resolve("sql" + i), RaftPeerId.valueOf("sql" + i), group, Set.of("a", "b"), stores, WalOptions.defaults().withTimeout(10_000).withHotCacheBytes(0)));
            }
            int first = leader(nodes, -1); var node = nodes.get(first); var tx = node.begin(IsolationLevel.REPEATABLE_READ);
            node.put(tx, "a", "balance", bytes("90")); node.put(tx, "b", "balance", bytes("110")); var receipt = node.commit(tx);
            for (var replica : nodes) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (replica.appliedSequence() < 1 || replica.localStats().materializedSequences().values().stream().anyMatch(sequence -> sequence < 1)) {
                    if (System.nanoTime() > until) { throw new AssertionError("cold copy did not materialize"); } TimeUnit.MILLISECONDS.sleep(20);
                }
                assertTrue(replica.checkpointLocal().created()); assertEquals(100, replica.localStats().walBytes()); assertEquals(0, replica.localStats().retainedBytes());
            }
            node.close(); nodes.set(first, null); pools.get(first * 2).close(); pools.get(first * 2 + 1).close();
            databases.get(first).stop();
            int next = leader(nodes, first); var serving = nodes.get(next); var reader = serving.begin(IsolationLevel.REPEATABLE_READ); var snapshot = serving.beginReadStatement(reader);
            assertEquals("90", new String(serving.get(reader, "a", "balance", snapshot).orElseThrow(), StandardCharsets.UTF_8));
            assertEquals("110", new String(serving.get(reader, "b", "balance", snapshot).orElseThrow(), StandardCharsets.UTF_8)); serving.rollback(reader);
            assertEquals(WalCommitLookup.Status.COMMITTED, serving.lookupCommit(receipt.transactionId()).status());
            var update = serving.begin(IsolationLevel.REPEATABLE_READ); serving.put(update, "a", "balance", bytes("80")); serving.put(update, "b", "balance", bytes("120"));
            assertEquals(2, serving.commit(update).commitSequence());
        } finally {
            Exception error = null;
            for (var node : nodes) { if (node != null) { try { node.close(); } catch (Exception failure) { if (error == null) { error = failure; } else { error.addSuppressed(failure); } } } }
            for (var pool : pools) { pool.close(); }
            for (var database : databases) { database.stop(); }
            if (error != null) { throw error; }
        }
    }

    /** 三个数据库最多合计四分之三核，缓冲池和连接数也受限。 */
    private static MySQLContainer<?> database() {
        return new MySQLContainer<>("mysql:8.4").withDatabaseName("ha_a").withUsername("wal_ha").withPassword("wal_ha")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "2000").withUrlParam("rewriteBatchedStatements", "true")
                .withCommand("--innodb-buffer-pool-size=64M", "--performance-schema=OFF", "--max-connections=32", "--innodb-flush-log-at-trx-commit=1", "--sync-binlog=1")
                .withStartupTimeout(Duration.ofMinutes(3)).withCreateContainerCmdModifier(command -> command.getHostConfig().withNanoCPUs(250_000_000L).withMemory(512L * 1024 * 1024));
    }

    /** 每个逻辑分片使用独立 schema，测试源 URL 未替换时拒绝复用。 */
    private static HikariDataSource pool(MySQLContainer<?> database, String schema) {
        var config = new HikariConfig(); config.setJdbcUrl(database.getJdbcUrl().replace("/ha_a", "/" + schema));
        config.setUsername(database.getUsername()); config.setPassword(database.getPassword()); config.setMaximumPoolSize(2); config.setMinimumIdle(0); config.setConnectionTimeout(5000);
        return new HikariDataSource(config);
    }

    /** 观察真实角色就绪，不把启动顺序当成选举结果。 */
    private static int leader(List<RatisWalNode> nodes, int excluded) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < until) {
            for (int i = 0; i < nodes.size(); i++) { if (i != excluded && nodes.get(i) != null && nodes.get(i).leaderEpoch() > 0) { return i; } }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        throw new AssertionError("no ready leader");
    }

    /** 两片使用不同值，防止只比较两边相等掩盖错误。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}

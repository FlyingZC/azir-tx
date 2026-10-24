package mvcc.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import static mvcc.wal.WalMvccRuntimeTest.*;

/** 真实备份与恢复的崩溃进程，仅连接父测试明确创建的源和恢复数据库。 */
public final class WalBackupCrashProcess {
    /** 子进程工具不允许实例化。 */
    private WalBackupCrashProcess() { }

    /** 在指定文件或数据库已实际写入后停住，由父进程强杀。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String boundary = args[1];
        try (var a = pool(args[2]); var b = pool(args[3]); var targetA = pool(args[4]); var targetB = pool(args[5])) {
            if (boundary.contains("BACKUP")) {
                var sources = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
                try (var runtime = WalMvccRuntime.openForTest(root.resolve("source"), sources.keySet(), sources, WalOptions.defaults().withHotCacheBytes(0), stage -> {
                    if (stage.name().equals(boundary)) { signal(root); }
                })) {
                    var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                    for (int i = 0; i < 20; i++) { for (String shard : sources.keySet()) { runtime.put(tx, shard, "k" + i, bytes("v:" + i)); } }
                    runtime.commit(tx); Files.writeString(root.resolve("seed-id"), tx.id().toString());
                    runtime.backup(root.resolve("backup"), limits());
                }
            } else {
                var targets = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB));
                WalBackup.restoreForTest(root.resolve("backup"), root.resolve("restored"), targets, WalOptions.defaults().withHotCacheBytes(0), limits(), stage -> {
                    if (stage.name().equals(boundary)) { signal(root); }
                });
            }
            throw new AssertionError("backup or restore crash boundary not reached");
        }
    }

    /** 小批次使第二分片的部分恢复也能被确定性观察。 */
    static WalBackupOptions limits() { return new WalBackupOptions(32L * 1024 * 1024, 4, 8192, 60_000); }

    /** 每个池最多两条测试连接，不读取用户数据库凭据。 */
    private static HikariDataSource pool(String url) {
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername("wal_test"); config.setPassword("wal_test");
        config.setMaximumPoolSize(2); config.setMinimumIdle(0); config.setConnectionTimeout(5000); config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /** 标记实际边界后保持进程存活，正常退出不能代替崩溃。 */
    private static void signal(Path root) {
        try { Files.writeString(root.resolve("ready"), "ready"); new CountDownLatch(1).await(); }
        catch (Exception failure) { throw new IllegalStateException(failure); }
    }
}

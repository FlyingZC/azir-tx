package mvcc.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import static mvcc.wal.WalMvccRuntimeTest.*;

/** 冷缓存恢复的独立崩溃进程，只连接父测试明确创建的两个数据库。 */
public final class WalColdCrashProcess {
    /** 独立进程工具不允许实例化。 */
    private WalColdCrashProcess() { }

    /** 先检查点化已确认数据，再于提交、检查点或物理回收边界停住等待强杀。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String target = args[1]; var armed = new AtomicBoolean();
        try (var a = pool(args[2]); var b = pool(args[3])) {
            var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(wrapped(a, armed, target, root)),
                    "b", new MysqlWalMaterializedStore(wrapped(b, armed, target, root)));
            try (var runtime = WalMvccRuntime.openForTest(root, Set.of("a", "b"), stores, options(), stage -> {
                if (armed.get() && stage.name().equals(target)) { signal(root); }
            })) {
                var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
                for (int i = 0; i < 16; i++) {
                    runtime.put(seed, "a", "key:" + i, bytes("old:" + i)); runtime.put(seed, "b", "key:" + i, bytes("old:" + i));
                }
                runtime.commit(seed); runtime.flush(); runtime.checkpoint();
                Files.writeString(root.resolve("seed-ack"), seed.id().toString());
                boolean committing = target.equals("AFTER_FORCE") || target.equals("BEFORE_PUBLISH") || target.equals("AFTER_PUBLISH");
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : stores.keySet()) {
                    if (target.equals("GC_AFTER_FIRST_DELETE")) { runtime.delete(tx, shard, "key:0"); runtime.delete(tx, shard, "key:1"); }
                    else { runtime.put(tx, shard, "key:0", bytes("new")); }
                }
                Files.writeString(root.resolve("transaction-id"), tx.id().toString()); armed.set(committing);
                runtime.commit(tx); Files.writeString(root.resolve("new-ack"), tx.id().toString()); runtime.flush(); armed.set(true);
                if (target.equals("GC_AFTER_FIRST_DELETE")) { runtime.collectGarbage(32); }
                else { runtime.checkpoint(); }
                throw new AssertionError("crash boundary not reached");
            }
        }
    }

    /** 使用两个载体、小内存和零热点缓存恢复，避免大机器或热缓存掩盖冷路径问题。 */
    static WalOptions options() {
        return new WalOptions(32, 64, 8192, 32, 8, 1, 8L * 1024 * 1024, 32_768, 60_000, 1024, 50).withHotCacheBytes(0);
    }

    /** 连接只使用父测试容器的公开测试账号，不读取用户凭据。 */
    private static HikariDataSource pool(String url) {
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername("wal_test"); config.setPassword("wal_test");
        config.setMaximumPoolSize(3); config.setMinimumIdle(0); config.setConnectionTimeout(5000); config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /** 第一条物理删除实际执行后暂停；强杀应使本地 MySQL 事务整体回滚。 */
    private static javax.sql.DataSource wrapped(HikariDataSource pool, AtomicBoolean armed, String target, Path root) {
        return (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(WalColdCrashProcess.class.getClassLoader(),
                new Class<?>[]{javax.sql.DataSource.class}, (p, method, args) -> {
                    Object result = delegated(pool, method, args);
                    if (!method.getName().equals("getConnection")) { return result; }
                    return java.lang.reflect.Proxy.newProxyInstance(WalColdCrashProcess.class.getClassLoader(), new Class<?>[]{java.sql.Connection.class},
                            (c, operation, values) -> {
                                Object statement = delegated(result, operation, values);
                                if (!operation.getName().equals("prepareStatement") || !((String) values[0]).startsWith("delete from azir_wal_version")) { return statement; }
                                return java.lang.reflect.Proxy.newProxyInstance(WalColdCrashProcess.class.getClassLoader(), new Class<?>[]{java.sql.PreparedStatement.class},
                                        (s, action, parameters) -> {
                                            Object returned = delegated(statement, action, parameters);
                                            if (armed.get() && target.equals("GC_AFTER_FIRST_DELETE") && action.getName().equals("executeUpdate")) { signal(root); }
                                            return returned;
                                        });
                            });
                });
    }

    /** 保留原始 JDBC 异常，以便失败确实触发协议路径。 */
    private static Object delegated(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }

    /** 只发布测试就绪标记，不能正常关闭替代真实崩溃。 */
    private static void signal(Path root) {
        try { Files.writeString(root.resolve("ready"), "ready"); new CountDownLatch(1).await(); }
        catch (Exception failure) { throw new IllegalStateException(failure); }
    }
}

package mvcc.wal.ha;

import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** 独立进程的资源所有者，不能在仍有节点使用控制池时提前关闭底层连接。 */
final class RatisNodeHost implements AutoCloseable {
    private final RatisNodeConfiguration config;
    private final List<HikariDataSource> pools = new ArrayList<>();
    private volatile RatisManagedNode node;
    private RatisStartupException pendingStartup;
    private RatisNodeApplication application;
    private RatisNodeHealthServer health;
    private volatile boolean started, closing, failed;
    private boolean attempted, uncertainStartup;

    /** 可公开的固定字段均为布尔或数值，不序列化内核异常消息和业务数据。 */
    record Status(boolean live, boolean ready, boolean writableHint, boolean closing, boolean failed,
                  long sampleAgeMillis, long publishedSequence, long materializationLag, long walBytes,
                  long hotCacheBytes, long safePoint, long retainedBytes, long bufferedBytes, long raftLogBytes,
                  long receiptArchiveBytes, long oldestActiveMillis, long certificateExpiresAtEpochMillis,
                  int activeTransactions, int queuedCommits, boolean maintenanceFailed,
                  boolean materializationFailed, boolean snapshotBackpressure) {
        /** 固定 JSON 字段不需要引入通用序列化器，也不会调用外部对象的 toString。 */
        String json() {
            return "{\"live\":" + live + ",\"ready\":" + ready + ",\"writableHint\":" + writableHint
                    + ",\"closing\":" + closing + ",\"failed\":" + failed + ",\"sampleAgeMillis\":" + sampleAgeMillis
                    + ",\"publishedSequence\":" + publishedSequence + ",\"materializationLag\":" + materializationLag
                    + ",\"walBytes\":" + walBytes + ",\"hotCacheBytes\":" + hotCacheBytes + ",\"activeTransactions\":" + activeTransactions
                    + ",\"safePoint\":" + safePoint + ",\"retainedBytes\":" + retainedBytes + ",\"bufferedBytes\":" + bufferedBytes
                    + ",\"raftLogBytes\":" + raftLogBytes + ",\"receiptArchiveBytes\":" + receiptArchiveBytes
                    + ",\"oldestActiveMillis\":" + oldestActiveMillis + ",\"certificateExpiresAtEpochMillis\":" + certificateExpiresAtEpochMillis
                    + ",\"queuedCommits\":" + queuedCommits + ",\"maintenanceFailed\":" + maintenanceFailed
                    + ",\"materializationFailed\":" + materializationFailed + ",\"snapshotBackpressure\":" + snapshotBackpressure + "}";
        }
    }

    /** 构造过程不启动线程或连接，先让 main 安装关闭钩子再启动。 */
    RatisNodeHost(RatisNodeConfiguration config) { this.config = config; }

    /** 宿主不支持失败后原地重开，成员身份、持久策略和恢复仍交给现有托管入口。 */
    synchronized void start() throws Exception {
        if (attempted || closing) { throw new IllegalStateException("node host cannot restart in the same process"); }
        attempted = true;
        try {
            RatisNodeHealthServer.requireBudgets();
            Constructor<? extends RatisNodeApplication> constructor = applicationConstructor();
            var byName = new LinkedHashMap<String, HikariDataSource>();
            for (var store : config.stores) {
                var pool = store.pool();
                pools.add(pool);
                byName.put(store.name(), pool);
            }
            var routes = new LinkedHashMap<String, HikariDataSource>();
            config.routes.forEach((shard, store) -> routes.put(shard, byName.get(store)));
            var managedConfig = config.managed(routes);
            // 底层构造器若没有返回句柄，外层无法证明所有内部资源都已清理。
            // 此时保留控制池直到所属进程退出，禁止在同一 JVM 重开目录。
            uncertainStartup = true;
            try {
                node = RatisManagedNode.open(managedConfig);
                uncertainStartup = false;
            } catch (RatisStartupException pending) {
                pendingStartup = pending;
                uncertainStartup = false;
                throw pending;
            }
            // 探针先占用端口，业务启动失败后仍可表达不可用；业务未启动完不就绪。
            health = new RatisNodeHealthServer(config, this);
            if (constructor != null) {
                application = constructor.newInstance();
                application.start(node);
            }
            started = true;
        } catch (Exception | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /** 在接触数据库前验证插件类型和公开无参构造器，不自动扫描类路径。 */
    private Constructor<? extends RatisNodeApplication> applicationConstructor() throws ReflectiveOperationException {
        if (config.applicationClass.isEmpty()) { return null; }
        var type = Class.forName(config.applicationClass, false, RatisNodeHost.class.getClassLoader()).asSubclass(RatisNodeApplication.class);
        if (!Modifier.isPublic(type.getModifiers()) || Modifier.isAbstract(type.getModifiers())) { throw new IllegalArgumentException("application must be a public concrete class"); }
        return type.getConstructor();
    }

    /** 仅读缓存，存储或维护错误与过期样本不能继续宣称业务已就绪。 */
    Status status() {
        var current = node;
        var status = current == null ? null : current.status();
        var storage = status == null ? null : status.storage();
        boolean maintenanceFailed = status != null && (status.error() != null || status.health().error() != null
                || status.gc().error() != null || status.gc().stale() || status.snapshot().error() != null
                || storage != null && storage.maintenanceFailure() != null);
        boolean materializationFailed = storage != null && storage.materializationFailure() != null;
        boolean backpressure = status != null && status.snapshot().backpressure();
        boolean live = started && !closing && !failed;
        boolean ready = live && status != null && status.localReady() && !status.stale() && storage != null
                && !status.health().quarantined() && !maintenanceFailed && !materializationFailed;
        return new Status(live, ready, ready && status.writableHint() && !backpressure, closing, failed,
                status == null ? Long.MAX_VALUE : status.ageMillis(), storage == null ? -1 : storage.publishedSequence(),
                storage == null ? -1 : storage.materializationLag(), storage == null ? -1 : storage.walBytes(),
                storage == null ? -1 : storage.hotCacheBytes(), storage == null ? -1 : storage.safePoint(),
                storage == null ? -1 : storage.retainedBytes(), storage == null ? -1 : storage.bufferedBytes(),
                status == null ? -1 : status.snapshot().raftLogBytes(), storage == null ? -1 : storage.receiptArchiveBytes(),
                storage == null ? -1 : storage.oldestActiveMillis(), config.security.certificateExpiresAt().toEpochMilli(),
                storage == null ? 0 : storage.activeTransactions(),
                storage == null ? 0 : storage.queuedCommits(), maintenanceFailed, materializationFailed, backpressure);
    }

    /** 业务先停接单并排空；任何步骤失败都保留未清理句柄，不能假装资源已释放。 */
    @Override public synchronized void close() throws Exception {
        closing = true;
        if (application != null) { application.close(); application = null; }
        if (node != null) { node.close(); node = null; }
        if (pendingStartup != null) { pendingStartup.retryCleanup(); pendingStartup = null; }
        if (uncertainStartup) { throw new IOException("startup cleanup cannot be proven; terminate the owning process before restart"); }
        for (var iterator = pools.iterator(); iterator.hasNext();) {
            iterator.next().close();
            iterator.remove();
        }
        if (health != null) { health.close(); health = null; }
    }
}

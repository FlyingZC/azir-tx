package mvcc.sql;

import mvcc.common.IsolationLevel;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.control.MysqlDatabaseAuthority;
import mvcc.percolator.MysqlPercolatorStore;
import mvcc.percolator.MysqlPercolatorLifecycle;
import mvcc.percolator.PercolatorKey;
import mvcc.percolator.PercolatorRuntime;
import mvcc.percolator.PercolatorStore;
import mvcc.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** 显式选择 Percolator 的 SQL 后端，复用现有 SqlEngine/SqlSession 与 Calcite 计划器。 */
public final class PercolatorSqlEngine implements SqlEngine, AutoCloseable {
    private final PercolatorRuntime runtime;
    private final SqlPlanner planner;
    private final ShardResolver resolver;
    private final SqlValueCodec codec;
    private final IsolationLevel isolation;
    private final SqlExecutionOptions options;
    private mvcc.maintenance.MaintenanceService maintenance;
    private boolean ownsRuntime;
    private volatile boolean closed;
    private final java.util.concurrent.locks.ReentrantLock lifecycleLock = new java.util.concurrent.locks.ReentrantLock();

    /** 借用现有运行时，可配置轻量主键计划、Calcite 关系计划与 SQL 资源预算。 */
    public PercolatorSqlEngine(PercolatorRuntime runtime, SqlPlanner planner, ShardResolver resolver,
                                SqlValueCodec codec, IsolationLevel isolation, SqlExecutionOptions options) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.isolation = Objects.requireNonNull(isolation, "isolation");
        this.options = Objects.requireNonNull(options, "options");
    }

    /** 与 AzirSqlEngine.calcite 对称的显式后端选择入口。 */
    public static PercolatorSqlEngine calcite(PercolatorRuntime runtime, ShardResolver resolver,
                                               SqlValueCodec codec, IsolationLevel isolation) {
        return new PercolatorSqlEngine(runtime, new CalciteSqlPlanner(), resolver, codec, isolation,
                SqlExecutionOptions.defaults());
    }

    /** 简单主键语句走轻量计划，其余 SQL 仍使用相同的完整 Calcite 执行器。 */
    public static PercolatorSqlEngine hybrid(PercolatorRuntime runtime, ShardResolver resolver,
                                              SqlValueCodec codec, IsolationLevel isolation) {
        return new PercolatorSqlEngine(runtime, new HybridSqlPlanner(), resolver, codec, isolation,
                SqlExecutionOptions.defaults());
    }

    /** 使用调用方拥有的连接池装配 MySQL Percolator，所有实例必须共用同一持久 TSO 库。 */
    public static PercolatorSqlEngine mysql(DataSource timestampControl, Map<String, DataSource> shardSources,
                                             ShardResolver resolver, SqlValueCodec codec, IsolationLevel isolation) {
        return mysql(timestampControl, shardSources, resolver, codec, isolation,
                PercolatorRuntime.Options.defaults(), SqlExecutionOptions.defaults());
    }

    /** 校验配置和物理命名空间后初始化协议表；不清空数据、不迁移旧模型或关闭借用的池。 */
    public static PercolatorSqlEngine mysql(DataSource timestampControl, Map<String, DataSource> shardSources,
                                             ShardResolver resolver, SqlValueCodec codec, IsolationLevel isolation,
                                             PercolatorRuntime.Options transactionOptions, SqlExecutionOptions sqlOptions) {
        return mysql(timestampControl, shardSources, resolver, codec, isolation, transactionOptions, sqlOptions,
                mvcc.maintenance.MaintenanceOptions.defaults());
    }

    /** 生产入口允许配置恢复/GC 的周期和预算；调用方必须在池之前关闭返回句柄。 */
    public static PercolatorSqlEngine mysql(DataSource timestampControl, Map<String, DataSource> shardSources,
                                             ShardResolver resolver, SqlValueCodec codec, IsolationLevel isolation,
                                             PercolatorRuntime.Options transactionOptions, SqlExecutionOptions sqlOptions,
                                             mvcc.maintenance.MaintenanceOptions maintenanceOptions) {
        Objects.requireNonNull(timestampControl, "timestampControl");
        Objects.requireNonNull(resolver, "resolver");
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(isolation, "isolation");
        Objects.requireNonNull(transactionOptions, "transactionOptions");
        Objects.requireNonNull(sqlOptions, "sqlOptions");
        timestampControl = new mvcc.control.ProtocolMysqlDataSource(timestampControl);
        var validated = new LinkedHashMap<String, DataSource>();
        shardSources.forEach((id, source) -> validated.put(id, new mvcc.control.ProtocolMysqlDataSource(source)));
        Map<String, DataSource> sources = Map.copyOf(validated);
        if (sources.isEmpty()) { throw new IllegalArgumentException("shards must not be empty"); }
        requireAutoCommit(timestampControl);
        var identities = new HashSet<MysqlDatabaseAuthority.Identity>();
        Map<String, PercolatorStore> stores = new LinkedHashMap<>();
        sources.forEach((id, source) -> {
            new PercolatorKey(id, "");
            requireAutoCommit(source);
            if (!identities.add(MysqlDatabaseAuthority.inspect(source))) {
                throw new IllegalArgumentException("Percolator shards must use distinct physical databases");
            }
            stores.put(id, new MysqlPercolatorStore(id, source));
        });
        var lifecycle = new MysqlPercolatorLifecycle(timestampControl, sources, transactionOptions.lockTtlMillis());
        try {
            sources.forEach((id, source) -> stores.put(id, new MysqlPercolatorStore(id, source, lifecycle.operations())));
            stores.values().forEach(PercolatorStore::initialize);
            mvcc.operations.MysqlProtocolSchema.requireRole(timestampControl, "pc", "control");
            sources.forEach((id, source) -> mvcc.operations.MysqlProtocolSchema.requireRole(source, "pc", "shard:" + id));
            var runtime = new PercolatorRuntime(new MysqlTimestampOracle(timestampControl), stores, transactionOptions, lifecycle);
            var engine = hybrid(runtime, resolver, codec, isolation, sqlOptions);
            engine.ownsRuntime = true;
            engine.maintenance = new mvcc.maintenance.MaintenanceService("percolator", maintenanceOptions, runtime::maintain);
            return engine;
        } catch (RuntimeException | Error failure) { lifecycle.close(); throw failure; }
    }

    /** 借用连接池必须默认自动提交，避免初始化或 TSO 发号悄悄依赖未提交的本地事务。 */
    private static void requireAutoCommit(DataSource source) {
        try (var connection = source.getConnection()) {
            if (!connection.getAutoCommit()) {
                throw new IllegalArgumentException("Percolator requires an auto-commit connection pool");
            }
        } catch (SQLException failure) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "inspect Percolator pool failed", false, null, failure);
        }
    }

    /** 供装配工厂设置 SQL 预算，维持轻量主键与完整关系计划的相同后端。 */
    private static PercolatorSqlEngine hybrid(PercolatorRuntime runtime, ShardResolver resolver,
                                               SqlValueCodec codec, IsolationLevel isolation, SqlExecutionOptions options) {
        return new PercolatorSqlEngine(runtime, new HybridSqlPlanner(), resolver, codec, isolation, options);
    }

    /** 返回同一后端的 KV、原 primary 查询和有界恢复入口。 */
    public PercolatorRuntime runtime() { return runtime; }

    /** 每个调用方获得独立 SQL 事务会话，不影响原有默认后端。 */
    @Override
    public SqlSession openSession() {
        if (closed) { throw new IllegalStateException("Percolator SQL engine closed"); }
        lifecycleLock.lock();
        try {
            if (closed) { throw new IllegalStateException("Percolator SQL engine closed"); }
            return new PercolatorSqlSession(runtime, planner, resolver, codec, isolation, options);
        } finally { lifecycleLock.unlock(); }
    }

    /** 返回自动维护健康状态；借用自定义运行时的引擎没有自动调度器。 */
    public java.util.Optional<mvcc.maintenance.MaintenanceService.Health> maintenanceHealth() {
        return java.util.Optional.ofNullable(maintenance).map(mvcc.maintenance.MaintenanceService::health);
    }

    /** 停止并等待后台维护，再关闭工厂拥有的运行时；关闭超时允许稍后重试。 */
    @Override public void close() {
        closed = true; lifecycleLock.lock();
        try {
            if (maintenance != null) { maintenance.close(); }
            if (ownsRuntime) { runtime.close(); }
        } finally { lifecycleLock.unlock(); }
    }
}

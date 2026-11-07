package com.flyingzc.azir.sql;

import com.flyingzc.azir.backend.TransactionBackend;
import com.flyingzc.azir.common.IsolationLevel;

/** 拥有源码后端生命周期的 SQL 装配句柄；调用方须先关闭会话，再关闭本句柄。 */
public final class ManagedBackendSqlEngine implements SqlEngine, AutoCloseable {
    private final TransactionBackend backend;
    private final BackendSqlEngine engine;
    private final com.flyingzc.azir.maintenance.MaintenanceService maintenance;
    private volatile boolean closed;
    private final java.util.concurrent.locks.ReentrantLock lifecycleLock = new java.util.concurrent.locks.ReentrantLock();
    private boolean backendClosed;

    /** 接管已创建后端，SQL 组件仍复用公共执行器。 */
    public ManagedBackendSqlEngine(TransactionBackend backend, ShardResolver resolver, SqlValueCodec codec,
                                     IsolationLevel isolation, SqlExecutionOptions options) {
        this(backend, resolver, codec, isolation, options, com.flyingzc.azir.maintenance.MaintenanceOptions.defaults());
    }
    /** 显式配置后台恢复和 GC 的周期及预算，构造成功后自动运行。 */
    public ManagedBackendSqlEngine(TransactionBackend backend, ShardResolver resolver, SqlValueCodec codec,
                                     IsolationLevel isolation, SqlExecutionOptions options,
                                     com.flyingzc.azir.maintenance.MaintenanceOptions maintenanceOptions) {
        this.backend = java.util.Objects.requireNonNull(backend, "backend");
        try {
            engine = new BackendSqlEngine(backend, resolver, codec, isolation, options);
            maintenance = new com.flyingzc.azir.maintenance.MaintenanceService(backend.implementation(), maintenanceOptions, backend::maintain);
        }
        catch (RuntimeException | Error failure) { try { backend.close(); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) { failure.addSuppressed(cleanup); } } throw failure; }
    }
    /** 取得统一的原身份查询和有界恢复入口。 */
    public TransactionBackend backend() { return backend; }
    /** 暴露后台成功、失败、安全点及最近一次错误，供健康检查使用。 */
    public com.flyingzc.azir.maintenance.MaintenanceService.Health maintenanceHealth() { return maintenance.health(); }
    /** 后端关闭后不再创建新业务会话。 */
    @Override public SqlSession openSession() {
        if (closed) { throw new IllegalStateException("backend SQL engine closed"); }
        lifecycleLock.lock();
        try {
            if (closed) { throw new IllegalStateException("backend SQL engine closed"); }
            return engine.openSession();
        } finally { lifecycleLock.unlock(); }
    }
    /** 先封闭入口，显式锁避免等待后台时固定 Java 21 虚拟线程载体；超时后仍允许重试关闭。 */
    @Override public void close() {
        closed = true; lifecycleLock.lock();
        try {
            if (backendClosed) { return; }
            maintenance.close(); backend.close(); backendClosed = true;
        } finally { lifecycleLock.unlock(); }
    }
}

package com.flyingzc.azir.wal;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** 新模式的托管装配，负责受限维护、启动校验和非阻塞健康观测，不接管调用方数据源。 */
public final class WalManagedRuntime implements AutoCloseable {
    /** 当前服务状态反映运行能力，不表示全部生产验收已经完成。 */
    public enum State { RUNNING, READ_ONLY, BACKUP, BACKPRESSURE, DEGRADED, CLOSING, CLOSED, FAILED }

    /** 健康查询不访问数据库，不等待运行时读锁，适合映射到就绪和存活探针。 */
    public record Health(State state, boolean alive, boolean ready, long sampleAgeMillis,
                         long maintenanceCycles, long maintenanceFailures, List<String> warnings, String failure) {
        /** 固定告警列表，避免调用方修改服务观测状态。 */
        public Health { warnings = List.copyOf(warnings); }
    }

    /** 一次完成采样的整体快照，跨线程发布时保持计数与时间一致。 */
    private record Sample(WalMvccRuntime.Stats stats, long capturedNanos, long cycles, long failures,
                          long inspectedKeys, long removedRows, long durationMillis, long overruns, String error) { }

    private final WalMvccRuntime runtime;
    private final WalOptions options;
    private final WalRetentionOptions retention;
    private final WalMaintenanceOptions maintenance;
    private final Object wake = new Object();
    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();
    private final Thread worker;
    private volatile Sample sample;
    private volatile boolean running = true;
    private volatile boolean closing;
    private volatile boolean closed;

    /** 在创建日志之前验证所有后端的事务、超时和有界维护能力。 */
    public static WalManagedRuntime open(Path directory, Map<String, WalMaterializedStore> stores, WalOptions options,
                                         WalRetentionOptions retention, WalMaintenanceOptions maintenance) {
        stores = Map.copyOf(stores); java.util.Objects.requireNonNull(maintenance);
        for (var store : stores.values()) {
            if (!store.supportsBoundedMaintenance()) { throw new IllegalArgumentException("managed backend requires bounded maintenance"); }
            store.validateProduction();
        }
        return attach(WalMvccRuntime.open(directory, stores, options, retention), options, retention, maintenance);
    }

    /** 纯本地模式仍受版本内存预算约束，自动维护不把唯一权威版本当缓存淘汰。 */
    public static WalManagedRuntime open(Path directory, Set<String> shards, WalOptions options,
                                         WalRetentionOptions retention, WalMaintenanceOptions maintenance) {
        java.util.Objects.requireNonNull(maintenance);
        return attach(WalMvccRuntime.open(directory, shards, options, retention), options, retention, maintenance);
    }

    /** 装配失败时释放新运行时，不遗留目录锁或后台工作线程。 */
    private static WalManagedRuntime attach(WalMvccRuntime runtime, WalOptions options, WalRetentionOptions retention, WalMaintenanceOptions maintenance) {
        try { return new WalManagedRuntime(runtime, options, retention, maintenance); }
        catch (Throwable failure) { runtime.close(); throw failure; }
    }

    /** 初始化观测快照后启动唯一维护者，不能用多个无界定时任务竞争回收。 */
    private WalManagedRuntime(WalMvccRuntime runtime, WalOptions options, WalRetentionOptions retention, WalMaintenanceOptions maintenance) {
        this.runtime = runtime; this.options = options; this.retention = retention; this.maintenance = maintenance;
        sample = new Sample(runtime.stats(), System.nanoTime(), 0, 0, 0, 0, 0, 0, null);
        worker = Thread.ofVirtual().name("azir-wal-maintenance").start(this::maintain);
    }

    /** 返回新模式事务 API；数据源和模式入口仍由应用显式选择。 */
    public WalMvccRuntime runtime() { return runtime; }

    /** 设置本进程只读维护状态，重启时由应用配置重新设置，不作为高可用写者隔离。 */
    public void setReadOnly(boolean value) { runtime.setReadOnly(value); }

    /** 单周期有界回收，错误保留在健康状态，下一周期按固定速率重试。 */
    private void maintain() {
        long cycles = 0; long failures = 0; long keys = 0; long rows = 0; long overruns = 0;
        while (running) {
            try {
                synchronized (wake) { if (running) { wake.wait(maintenance.intervalMillis()); } }
                if (!running) { break; }
                var lifecycle = runtime.lifecycle(); if (lifecycle.closed() || lifecycle.fatal() != null) { break; }
                if (lifecycle.backup()) { continue; }
                long start = System.nanoTime(); String error = null;
                try {
                    var progress = runtime.collectGarbageBounded(maintenance.keyLimit(), maintenance.rowLimit(), maintenance.workMillis());
                    keys += progress.inspectedKeys(); rows += progress.removedRows();
                } catch (RuntimeException failure) { failures++; error = diagnostic(failure); }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                if (duration > maintenance.workMillis()) { overruns++; }
                cycles++;
                if (running) { sample = new Sample(runtime.stats(), System.nanoTime(), cycles, failures, keys, rows, duration, overruns, error); }
            } catch (InterruptedException failure) { Thread.currentThread().interrupt(); return; }
            catch (RuntimeException failure) {
                Sample previous = sample;
                sample = new Sample(previous.stats(), previous.capturedNanos(), cycles, ++failures, keys, rows, previous.durationMillis(), overruns, diagnostic(failure));
            }
        }
    }

    /** 非阻塞读取当前状态和最近采样，过期样本不能继续宣称就绪。 */
    public Health health() {
        Sample current = sample; var live = runtime.lifecycle(); var stats = current.stats(); var warnings = new ArrayList<String>();
        long age = Math.max(0, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - current.capturedNanos()));
        warn(warnings, "wal_budget", stats.walBytes(), options.maxWalBytes());
        warn(warnings, "receipt_archive_budget", stats.receiptArchiveBytes(), retention.maxReceiptArchiveBytes());
        warn(warnings, "pending_receipts", stats.pendingReceipts(), retention.maxPendingReceipts());
        warn(warnings, "version_memory", stats.retainedBytes() + stats.bufferedBytes(), options.maxMemoryBytes());
        warn(warnings, "materialization_lag", stats.materializationLag(), options.maxMaterializationLag());
        if (stats.oldestActiveMillis() >= options.transactionTimeoutMillis() * .8) { warnings.add("old_snapshot_or_queued_decision"); }
        String failure = live.fatal() != null ? live.fatal() : current.error() != null ? current.error()
                : live.materializationFailure() != null ? live.materializationFailure() : live.checkpointFailure();
        State state;
        if (closed || live.closed()) { state = State.CLOSED; }
        else if (live.fatal() != null) { state = State.FAILED; }
        else if (closing || !live.accepting()) { state = State.CLOSING; }
        else if (live.backup()) { state = State.BACKUP; }
        else if (age > maintenance.staleAfterMillis() || failure != null || live.proposalBlocked()) { state = State.DEGRADED; }
        else if (live.readOnly()) { state = State.READ_ONLY; }
        else if (stats.walBytes() >= options.maxWalBytes() || stats.receiptArchiveBytes() >= retention.maxReceiptArchiveBytes()
                || stats.pendingReceipts() >= retention.maxPendingReceipts() || stats.materializationLag() >= options.maxMaterializationLag()) { state = State.BACKPRESSURE; }
        else { state = State.RUNNING; }
        return new Health(state, state != State.FAILED && state != State.CLOSED, state == State.RUNNING, age, current.cycles(), current.failures(), warnings, failure);
    }

    /** 根据配置水位发出容量告警，准确拒绝仍由提交路径的实际字节预算决定。 */
    private void warn(List<String> warnings, String name, long used, long limit) {
        if ((double) used / limit * 100 >= maintenance.warningPercent()) { warnings.add(name); }
    }

    /** 输出无需额外依赖的 Prometheus 文本，全部来自最近完成快照。 */
    public String metrics() {
        Sample current = sample; var stats = current.stats(); var health = health(); var text = new StringBuilder();
        metric(text, "ready", health.ready() ? 1 : 0); metric(text, "alive", health.alive() ? 1 : 0);
        metric(text, "read_only", runtime.lifecycle().readOnly() ? 1 : 0);
        metric(text, "health_sample_age_milliseconds", health.sampleAgeMillis()); metric(text, "published_sequence", stats.publishedSequence());
        metric(text, "safe_point", stats.safePoint()); metric(text, "oldest_active_milliseconds", stats.oldestActiveMillis());
        metric(text, "wal_bytes", stats.walBytes()); metric(text, "wal_budget_bytes", options.maxWalBytes());
        metric(text, "receipt_archive_bytes", stats.receiptArchiveBytes()); metric(text, "receipt_archive_budget_bytes", retention.maxReceiptArchiveBytes());
        metric(text, "pending_receipts", stats.pendingReceipts()); metric(text, "version_retained_bytes", stats.retainedBytes());
        metric(text, "buffered_bytes", stats.bufferedBytes()); metric(text, "hot_cache_bytes", stats.hotCacheBytes());
        metric(text, "materialization_lag", stats.materializationLag()); metric(text, "active_transactions", stats.activeTransactions());
        metric(text, "maintenance_cycles_total", current.cycles()); metric(text, "maintenance_failures_total", current.failures());
        metric(text, "gc_inspected_keys_total", current.inspectedKeys()); metric(text, "gc_removed_rows_total", current.removedRows());
        metric(text, "memory_retired_versions_total", stats.retiredMemoryVersions()); metric(text, "maintenance_duration_milliseconds", current.durationMillis());
        metric(text, "maintenance_overruns_total", current.overruns()); metric(text, "cache_hits_total", stats.cacheHits()); metric(text, "cache_misses_total", stats.cacheMisses());
        return text.toString();
    }

    /** 固定名称只写数字，不把用户键、数据源地址或异常堆栈写入指标标签。 */
    private static void metric(StringBuilder output, String name, long value) { output.append("azir_wal_").append(name).append(' ').append(value).append('\n'); }

    /** 公开阶段错误码和 SQL 状态，不把 SQL 文本、业务键或连接凭据带进健康响应。 */
    private static String diagnostic(RuntimeException failure) {
        String result = failure.getClass().getSimpleName();
        if (failure instanceof WalMvccException wal) { result += ":" + wal.errorCode().name(); }
        Throwable cause = failure;
        for (int depth = 0; depth < 8 && cause != null; depth++, cause = cause.getCause()) {
            if (cause instanceof java.sql.SQLException sql) {
                String state = sql.getSQLState();
                return result + ":sqlstate=" + (state != null && state.matches("[A-Z0-9]{5}") ? state : "unknown") + ":vendor=" + sql.getErrorCode();
            }
        }
        return result;
    }

    /** 停止新事务并等待维护者退出，然后排空提交与物化；超时不释放活跃目录锁。 */
    @Override public void close() {
        closeGuard.lock();
        try {
            if (closed) { return; }
            closing = true; running = false; runtime.stopAccepting();
            synchronized (wake) { wake.notifyAll(); }
            try { worker.join(30_000); }
            catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException("interrupted while closing maintenance", failure); }
            if (worker.isAlive()) { throw new IllegalStateException("maintenance did not stop; WAL directory remains locked"); }
            runtime.close(); closed = true;
        } finally { closeGuard.unlock(); }
    }
}

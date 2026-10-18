package mvcc.maintenance;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.IntFunction;

/** 单线程有界维护服务；失败可观测且下轮继续，关闭必须等待任务真正退出。 */
public final class MaintenanceService implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(MaintenanceService.class.getName());
    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> future;
    private final MaintenanceOptions options;
    private volatile Health health = new Health(0, 0, null, null, null);

    /** 维护健康快照；最近失败保留到下一次成功，并累计失败次数。 */
    public record Health(long successes, long failures, Instant lastFinished, MaintenanceResult lastResult, String lastError) { }

    /** 使用固定延迟启动；不同实例可并行调度，由数据库协议协调。 */
    public MaintenanceService(String name, MaintenanceOptions options, IntFunction<MaintenanceResult> work) {
        this.options = Objects.requireNonNull(options, "options");
        Objects.requireNonNull(work, "work");
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("azir-maintenance-" + name).factory());
        try { future = executor.scheduleWithFixedDelay(() -> run(work), options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS); }
        catch (RuntimeException | Error failure) {
            try { executor.shutdownNow(); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) { failure.addSuppressed(cleanup); } }
            throw failure;
        }
    }
    /** 获取无阻塞的最近维护结果与异常，不暴露数据库连接信息。 */
    public Health health() {
        Health current = health;
        if ((future.isDone() || executor.isShutdown()) && current.lastError() == null) {
            return new Health(current.successes(), current.failures(), current.lastFinished(), current.lastResult(), "MaintenanceStopped");
        }
        return current;
    }
    /** 一轮失败不能永久取消后续调度，也不能被无声吞掉。 */
    private void run(IntFunction<MaintenanceResult> work) {
        Health old = health;
        try {
            MaintenanceResult result = work.apply(options.batchSize());
            health = new Health(old.successes() + 1, old.failures(), Instant.now(), result, null);
        } catch (RuntimeException failure) {
            health = new Health(old.successes(), old.failures() + 1, Instant.now(), old.lastResult(), failure.getClass().getSimpleName());
            // 原始 JDBC 异常可能含重复键或 SQL 载荷，通用维护日志只输出异常类型。
            LOG.log(System.Logger.Level.ERROR, "后台事务维护失败，后续周期将重试，异常类型：{0}", failure.getClass().getSimpleName());
        } catch (Error fatal) {
            // 致命错误仍交给调度器停止任务，但健康接口必须立即呈现已停止的失败状态。
            health = new Health(old.successes(), old.failures() + 1, Instant.now(), old.lastResult(), fatal.getClass().getSimpleName());
            LOG.log(System.Logger.Level.ERROR, "后台事务维护因致命错误停止，异常类型：{0}", fatal.getClass().getSimpleName());
            throw fatal;
        }
    }
    /** 中断后台任务并等待退出；超时显式报错，调用方不能提前关闭连接池。 */
    @Override public void close() {
        future.cancel(true);
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(options.closeTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("maintenance did not stop within close budget; keep pools open and retry close");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while stopping maintenance", failure);
        }
    }
}

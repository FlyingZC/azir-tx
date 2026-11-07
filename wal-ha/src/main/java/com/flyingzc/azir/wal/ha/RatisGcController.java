package com.flyingzc.azir.wal.ha;

import java.io.IOException;
import java.util.concurrent.*;

/** 每个副本一个版本回收工作者，不在共识应用线程执行 JDBC 或版本扫描。 */
final class RatisGcController implements AutoCloseable {
    private final RatisWalNode node;
    private final RatisGcOptions options;
    private final ScheduledExecutorService worker;
    private volatile RatisTimedSample<RatisWalNode.GcStats> sampled;
    /** 单线程固定延迟调度，不累计未完成周期。 */
    RatisGcController(RatisWalNode node, RatisGcOptions options) {
        this.node = node; this.options = options;
        worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-version-gc").factory());
        sampled = RatisTimedSample.initial(new RatisWalNode.GcStats(true, "STARTING", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true, null, null));
    }
    /** 持久配置恢复与首次启用采用相同周期。 */
    void start() { worker.scheduleWithFixedDelay(this::round, options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS); }
    /** 幂等装配必须核对完整配置。 */
    RatisGcOptions options() { return options; }
    /** 探针只复制缓存，不获取引擎锁或执行 SQL；停滞也能被观测。 */
    RatisWalNode.GcStats stats() {
        var observation = sampled; var current = observation.value(); long age = observation.ageMillis(System.nanoTime());
        return new RatisWalNode.GcStats(true, current.state(), current.rounds(), current.failures(), current.inspectedKeys(), current.removedRows(),
                current.safePoint(), current.publishedSequence(), current.retiredMemoryVersions(), current.durationMillis(), current.overruns(), age,
                age > options.staleAfterMillis(), current.error(), current.generation());
    }
    /** 代次忙或安装期间跳过，异常保留在脱敏指标中，下一周期重试。 */
    private void round() {
        var observation = sampled; var previous = observation.value(); long began = System.nanoTime();
        try {
            var result = node.collectVersionGarbage(options);
            if (result == null) {
                sampled = observation.retaining(new RatisWalNode.GcStats(true, "WAITING", previous.rounds(), previous.failures(), previous.inspectedKeys(), previous.removedRows(), previous.safePoint(), previous.publishedSequence(), previous.retiredMemoryVersions(), previous.durationMillis(), previous.overruns(), 0, false, previous.error(), previous.generation())); return;
            }
            long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);
            sampled = RatisTimedSample.captured(new RatisWalNode.GcStats(true, "RUNNING", previous.rounds() + 1, previous.failures(), previous.inspectedKeys() + result.progress().inspectedKeys(),
                    previous.removedRows() + result.progress().removedRows(), result.stats().safePoint(), result.stats().publishedSequence(), result.stats().retiredMemoryVersions(),
                    duration, previous.overruns() + (duration > options.workMillis() ? 1 : 0), 0, false, null, result.generation()), System.nanoTime());
        } catch (Exception failure) {
            sampled = observation.retaining(new RatisWalNode.GcStats(true, "RETRYING", previous.rounds() + 1, previous.failures() + 1, previous.inspectedKeys(), previous.removedRows(), previous.safePoint(), previous.publishedSequence(), previous.retiredMemoryVersions(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began), previous.overruns(), 0, false, failure.getClass().getSimpleName(), previous.generation()));
        }
    }
    /** 停止生产新周期并排空在途回收，不通过中断关闭共享 WAL 或 JDBC。 */
    @Override public void close() throws IOException {
        worker.shutdown();
        try { if (!worker.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("version GC did not drain"); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("version GC close interrupted", interrupted); }
    }
}

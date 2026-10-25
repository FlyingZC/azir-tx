package mvcc.wal.ha;

import java.io.IOException;
import java.util.concurrent.*;

/** 每节点一个有界健康任务，失败防抖后隔离，恢复防抖后重新取得资格。 */
final class RatisHealthController implements AutoCloseable {
    private final RatisWalNode node;
    private final RatisHealthOptions options;
    private final ScheduledExecutorService worker;
    private volatile long rounds, isolations, transfers, recoveries;
    private volatile int failed, succeeded;
    private volatile String state = "STARTING", error;
    /** 构造不会创建无界队列，下一轮必须等上一轮完成。 */
    RatisHealthController(RatisWalNode node, RatisHealthOptions options) { this.node = node; this.options = options; worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-health-control").factory()); }
    /** 显式启用或持久策略恢复以后开始周期任务。 */
    void start() { worker.scheduleWithFixedDelay(this::round, options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS); }
    /** 缓存状态用于就绪探针，不在探针里执行数据库查询。 */
    RatisWalNode.HealthStats stats() { return new RatisWalNode.HealthStats(true, node.healthQuarantined(), node.healthCampaignBlocked(), state, failed, succeeded, rounds, isolations, transfers, recoveries, error); }
    /** 返回固定策略，重复装配只能使用相同配置。 */
    RatisHealthOptions options() { return options; }
    /** 单次故障只改变有限状态，不把例外信息中的 JDBC 地址或凭据写入指标。 */
    private void round() {
        try {
            var verdict = node.healthProbe(options); if (verdict == RatisWalNode.HealthVerdict.BUSY) { state = "WAITING"; return; }
            rounds++;
            if (verdict == RatisWalNode.HealthVerdict.GOOD) {
                failed = 0; succeeded = Math.min(options.recoveries(), succeeded + 1);
                if (node.healthQuarantined()) {
                    if (succeeded >= options.recoveries() && node.healthCanRecover()) { node.healthResume(options); recoveries++; state = "HEALTHY"; }
                    else { state = "RECOVERING"; if (node.healthMaintainIsolation(options)) { transfers++; } }
                } else { state = "HEALTHY"; }
            } else {
                succeeded = 0; failed = Math.min(options.failures(), failed + 1); state = "SUSPECT";
                if (failed >= options.failures() || node.healthQuarantined()) {
                    if (!node.healthQuarantined()) { node.healthIsolate(); isolations++; }
                    state = "QUARANTINED"; if (node.healthMaintainIsolation(options)) { transfers++; }
                }
            }
            error = null;
        } catch (Exception failure) { error = failure.getClass().getSimpleName(); state = node.healthQuarantined() ? "QUARANTINED" : "PROBE_ERROR"; }
    }
    /** 不打断在途 JDBC 或 WAL，共识停止以前先排空本任务。 */
    @Override public void close() throws IOException {
        worker.shutdown();
        try { if (!worker.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("health management did not drain"); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("health close interrupted", interrupted); }
    }
}

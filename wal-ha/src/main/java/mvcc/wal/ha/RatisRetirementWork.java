package mvcc.wal.ha;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** 同一维护调用共用计数与单调时间，任何阶段都不能另起预算掩盖工作量。 */
final class RatisRetirementWork {
    final RatisRetirementOptions options; final long started = System.nanoTime(); int scanned, removed, inspected, dropped, completed; boolean exhausted;
    /** 一轮维护独立计时。 */
    RatisRetirementWork(RatisRetirementOptions options) { this.options = options; }
    /** 单次 I/O 的硬期限由驱动承担，本方法提供协作期限。 */
    void check() throws Limit {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(options.budgetMillis())) { exhausted = true; throw new Limit(); }
    }
    /** 扫描条目不能无限增长。 */
    void scan() throws Limit { check(); if (scanned >= options.scanLimit()) { exhausted = true; throw new Limit(); } scanned++; }
    /** 破坏性 I/O 前预留一个删除预算。 */
    void deleting() throws Limit { check(); if (removed >= options.fileLimit()) { exhausted = true; throw new Limit(); } }
    /** 完整文件检查也共用剩余期限，不能偷偷使用另一个五分钟预算。 */
    long remainingMillis() throws Limit { check(); return Math.max(1, options.budgetMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)); }
    /** 预算耗尽区别于数据或所有权损坏，允许下一轮续做。 */
    static final class Limit extends IOException {
        /** 维护预算到期不代表操作回滚。 */
        Limit() { super("retirement work budget exhausted"); }
    }
}

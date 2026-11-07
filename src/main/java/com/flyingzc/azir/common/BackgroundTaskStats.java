package com.flyingzc.azir.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 后台任务运行统计信息。
 */
public final class BackgroundTaskStats {
    private final AtomicLong lastRunAtMillis = new AtomicLong();
    private final AtomicLong lastSuccessAtMillis = new AtomicLong();
    private final AtomicInteger runCount = new AtomicInteger();
    private final AtomicInteger failureCount = new AtomicInteger();
    private final AtomicLong lastProcessedCount = new AtomicLong();

    /**
     * 记录一次后台任务成功执行。
     */
    public void recordSuccess(long processedCount) {
        long now = System.currentTimeMillis();
        lastRunAtMillis.set(now);
        lastSuccessAtMillis.set(now);
        runCount.incrementAndGet();
        lastProcessedCount.set(processedCount);
    }

    /**
     * 记录一次后台任务失败。
     */
    public void recordFailure() {
        lastRunAtMillis.set(System.currentTimeMillis());
        runCount.incrementAndGet();
        failureCount.incrementAndGet();
    }

    /**
     * 返回最近一次运行时间。
     */
    public long lastRunAtMillis() {
        return lastRunAtMillis.get();
    }

    /**
     * 返回最近一次成功时间。
     */
    public long lastSuccessAtMillis() {
        return lastSuccessAtMillis.get();
    }

    /**
     * 返回运行次数。
     */
    public int runCount() {
        return runCount.get();
    }

    /**
     * 返回失败次数。
     */
    public int failureCount() {
        return failureCount.get();
    }

    /**
     * 返回最近一次处理的数据量。
     */
    public long lastProcessedCount() {
        return lastProcessedCount.get();
    }
}

package mvcc.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class BackgroundTaskStats {
    private final AtomicLong lastRunAtMillis = new AtomicLong();
    private final AtomicLong lastSuccessAtMillis = new AtomicLong();
    private final AtomicInteger runCount = new AtomicInteger();
    private final AtomicInteger failureCount = new AtomicInteger();
    private final AtomicLong lastProcessedCount = new AtomicLong();

    public void recordSuccess(long processedCount) {
        long now = System.currentTimeMillis();
        lastRunAtMillis.set(now);
        lastSuccessAtMillis.set(now);
        runCount.incrementAndGet();
        lastProcessedCount.set(processedCount);
    }

    public void recordFailure() {
        lastRunAtMillis.set(System.currentTimeMillis());
        runCount.incrementAndGet();
        failureCount.incrementAndGet();
    }

    public long lastRunAtMillis() {
        return lastRunAtMillis.get();
    }

    public long lastSuccessAtMillis() {
        return lastSuccessAtMillis.get();
    }

    public int runCount() {
        return runCount.get();
    }

    public int failureCount() {
        return failureCount.get();
    }

    public long lastProcessedCount() {
        return lastProcessedCount.get();
    }
}

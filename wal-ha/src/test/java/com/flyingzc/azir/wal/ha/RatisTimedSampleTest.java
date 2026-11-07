package com.flyingzc.azir.wal.ha;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 使用确定性的单调时刻核对观测新鲜度，无需线程睡眠、数据库或 Raft 服务。 */
class RatisTimedSampleTest {
    /** 读者保留旧快照时，新采样不能给旧数据刷新年龄。 */
    @Test void replacingSampleCannotRefreshAnAlreadyReadObservation() {
        var old = RatisTimedSample.captured("old", 0);
        var fresh = RatisTimedSample.captured("fresh", TimeUnit.SECONDS.toNanos(20));
        long now = TimeUnit.SECONDS.toNanos(21);
        assertEquals(21_000, old.ageMillis(now));
        assertEquals(1000, fresh.ageMillis(now));
        assertEquals("old", old.value());
    }

    /** 失败和跳过仅改变诊断，不能给旧代次指标续期。 */
    @Test void failureAndBusyUpdatesRetainTheLastSuccessfulCapture() {
        var original = RatisTimedSample.captured("generation-a", TimeUnit.SECONDS.toNanos(1));
        var failed = original.retaining("probe-error").retaining("storage-busy");
        assertEquals(20_000, failed.ageMillis(TimeUnit.SECONDS.toNanos(21)));
        assertEquals(Long.MAX_VALUE, RatisTimedSample.initial("starting").retaining("busy").ageMillis(0));
    }

    /** 零和负数都是合法单调时刻，慢探测的有效期不从发布时重新开始。 */
    @Test void validClockValuesAndSlowProbeKeepTheirOriginalBoundary() {
        assertEquals(1000, RatisTimedSample.captured("zero", 0).ageMillis(TimeUnit.SECONDS.toNanos(1)));
        assertEquals(2000, RatisTimedSample.captured("negative", -TimeUnit.SECONDS.toNanos(1)).ageMillis(TimeUnit.SECONDS.toNanos(1)));
        assertEquals(30_000, RatisTimedSample.captured("slow-proof", 0).ageMillis(TimeUnit.SECONDS.toNanos(30)));
    }
}

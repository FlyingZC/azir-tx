package com.flyingzc.azir.wal.ha;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 将观测值与单调采样时刻整体发布，避免旧指标被另一次采样的时间续鲜。 */
record RatisTimedSample<T>(T value, long capturedNanos, boolean available) {
    /** 无样本状态独立表示，不能把合法的 nanoTime 零值误认为尚未采样。 */
    RatisTimedSample { Objects.requireNonNull(value); }

    /** 启动时提供状态结构，但尚无成功采样的时间证明。 */
    static <T> RatisTimedSample<T> initial(T value) { return new RatisTimedSample<>(value, 0, false); }

    /** 时刻由本次观测绑定；涉及多数派证明时应传入探测开始时刻，避免慢采样续期。 */
    static <T> RatisTimedSample<T> captured(T value, long capturedNanos) { return new RatisTimedSample<>(value, capturedNanos, true); }

    /** 忙或失败只更新诊断，保留原成功样本的年龄。 */
    RatisTimedSample<T> retaining(T value) { return new RatisTimedSample<>(value, capturedNanos, available); }

    /** 调用方取得一次完整快照后计算年龄，不再读取另一份可变时间字段。 */
    long ageMillis(long nowNanos) {
        return available ? Math.max(0, TimeUnit.NANOSECONDS.toMillis(nowNanos - capturedNanos)) : Long.MAX_VALUE;
    }
}

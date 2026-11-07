package com.flyingzc.azir.maintenance;

/** 当前持久维护状态；oldestActiveSnapshot 为零表示没有活跃读者，pendingItems 为待恢复项数。 */
public record MaintenanceDiagnostics(long safePoint, long oldestActiveSnapshot, long activeSnapshots,
                                     long expiredSnapshots, long pendingItems) {
    /** 拒绝负计数，便于对接运行健康与告警。 */
    public MaintenanceDiagnostics {
        if (safePoint < 0 || oldestActiveSnapshot < 0 || activeSnapshots < 0 || expiredSnapshots < 0 || pendingItems < 0) {
            throw new IllegalArgumentException("invalid maintenance diagnostics");
        }
    }
}

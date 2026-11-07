package com.flyingzc.azir.maintenance;

/** 单轮维护结果；安全点属于当前后端，不能跨模型比较。 */
public record MaintenanceResult(int recovered, int deletedVersions, long safePoint, boolean paused) {
    /** 保留既有三参数维护结果构造方式。 */
    public MaintenanceResult(int recovered, int deletedVersions, long safePoint) { this(recovered, deletedVersions, safePoint, false); }
    /** 拒绝不合法的维护计数。 */
    public MaintenanceResult {
        if (recovered < 0 || deletedVersions < 0 || safePoint < 0) {
            throw new IllegalArgumentException("invalid maintenance result");
        }
    }
}

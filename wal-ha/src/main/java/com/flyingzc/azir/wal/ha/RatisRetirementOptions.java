package com.flyingzc.azir.wal.ha;

/** 节点维护的实际扫描、删除和协作时间预算，不通过中断取消正在执行的 I/O。 */
public record RatisRetirementOptions(int backendShardLimit, int fileLimit, int scanLimit, long budgetMillis, int retainedSnapshots) {
    /** 所有工作量有限，至少保留两份最新完整镜像。 */
    public RatisRetirementOptions {
        if (backendShardLimit < 1 || backendShardLimit > 1024 || fileLimit < 1 || fileLimit > 4096 || scanLimit < 16 || scanLimit > 200_000
                || budgetMillis < 1 || budgetMillis > 120_000 || retainedSnapshots < 2 || retainedSnapshots > 32) { throw new IllegalArgumentException("invalid retirement work budgets"); }
    }
    /** 每轮两片、一百二十八个文件，目录检查最多四千项。 */
    public static RatisRetirementOptions defaults() { return new RatisRetirementOptions(2, 128, 4096, 1000, 2); }
}

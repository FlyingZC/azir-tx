package mvcc.txn;

/** 分段 owner、发布器、恢复与 GC 的显式有界资源预算。 */
public record SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                    long ownerRefreshMillis, long idleSealMillis,
                                    int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                    long recoveryIntervalMillis, long publicationIntervalMillis,
                                    long gcIntervalMillis,
                                    boolean batchDecisions, int batchWorkers,
                                    boolean directSingleShard, boolean eventDrivenPublication,
                                    boolean overlapMaterialization, boolean fastMaterialization,
                                    boolean fastCommitProcedure) {
    /** 拒绝无界分段、过短续租余量和无效后台周期。 */
    public SegmentRuntimeOptions {
        if (segmentSize < 1 || segmentSize > 4096
                || ownerLeaseMillis < 3 || ownerLeaseMillis > 86_400_000
                || ownerRefreshMillis < 1 || ownerRefreshMillis >= ownerLeaseMillis / 2
                || idleSealMillis < 1
                || recoveryBatchSize < 1 || recoveryBatchSize > 4096
                || publicationBatchSize < 1 || publicationBatchSize > 4096
                || gcBatchSize < 1 || gcBatchSize > 4096
                || recoveryIntervalMillis < 1 || publicationIntervalMillis < 1
                || gcIntervalMillis < 1 || batchWorkers < 1 || batchWorkers > 8
                || fastMaterialization && (batchDecisions || overlapMaterialization)) {
            throw new IllegalArgumentException("invalid segmented runtime resource limits");
        }
    }

    /** 保持中心凭据实验参数，分片第二版例程仍由显式选项控制。 */
    public SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                 long ownerRefreshMillis, long idleSealMillis,
                                 int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                 long recoveryIntervalMillis, long publicationIntervalMillis,
                                 long gcIntervalMillis, boolean batchDecisions, int batchWorkers,
                                 boolean directSingleShard, boolean eventDrivenPublication,
                                 boolean overlapMaterialization, boolean fastMaterialization) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, eventDrivenPublication,
                overlapMaterialization, fastMaterialization, false);
    }

    /** 保持已有重叠物化参数，中心决议凭据快速路径须单独启用。 */
    public SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                 long ownerRefreshMillis, long idleSealMillis,
                                 int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                 long recoveryIntervalMillis, long publicationIntervalMillis,
                                 long gcIntervalMillis, boolean batchDecisions, int batchWorkers,
                                 boolean directSingleShard, boolean eventDrivenPublication,
                                 boolean overlapMaterialization) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, eventDrivenPublication,
                overlapMaterialization, false, false);
    }

    /** 保持既有物理 1PC 运行时参数，发布唤醒须独立显式启用。 */
    public SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                 long ownerRefreshMillis, long idleSealMillis,
                                 int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                 long recoveryIntervalMillis, long publicationIntervalMillis,
                                 long gcIntervalMillis, boolean batchDecisions, int batchWorkers,
                                 boolean directSingleShard) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, false, false, false, false);
    }

    /** 保持发布唤醒实验参数，分片物化与发布重叠须额外显式启用。 */
    public SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                 long ownerRefreshMillis, long idleSealMillis,
                                 int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                 long recoveryIntervalMillis, long publicationIntervalMillis,
                                 long gcIntervalMillis, boolean batchDecisions, int batchWorkers,
                                 boolean directSingleShard, boolean eventDrivenPublication) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, directSingleShard, eventDrivenPublication,
                false, false, false);
    }

    /** 兼容现有批量决议实验参数，本地物理 1PC 仍须显式启用。 */
    public SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                 long ownerRefreshMillis, long idleSealMillis,
                                 int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                 long recoveryIntervalMillis, long publicationIntervalMillis,
                                 long gcIntervalMillis, boolean batchDecisions, int batchWorkers) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                batchDecisions, batchWorkers, false, false, false, false, false);
    }

    /** 保持既有显式运行时调用，批量决议仍须单独开启。 */
    public SegmentRuntimeOptions(int segmentSize, long ownerLeaseMillis,
                                 long ownerRefreshMillis, long idleSealMillis,
                                 int recoveryBatchSize, int publicationBatchSize, int gcBatchSize,
                                 long recoveryIntervalMillis, long publicationIntervalMillis,
                                 long gcIntervalMillis) {
        this(segmentSize, ownerLeaseMillis, ownerRefreshMillis, idleSealMillis,
                recoveryBatchSize, publicationBatchSize, gcBatchSize,
                recoveryIntervalMillis, publicationIntervalMillis, gcIntervalMillis,
                false, 2, false, false, false, false, false);
    }

    /** 返回隔离验证的保守初始预算，业务入口仍须显式选择分段模式。 */
    public static SegmentRuntimeOptions defaults() {
        return new SegmentRuntimeOptions(32, 5_000, 1_000, 100,
                128, 256, 128, 100, 10, 1_000);
    }
}

package mvcc.wal;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;

/** 新模式专属物化契约，不复用原模式的 intent 或两阶段提交接口。 */
public interface WalMaterializedStore {
    /** 在写入 WAL 前检查后端固定的单行容量约束，默认存储不额外限制。 */
    default void validateMutation(WalMutation mutation) { }
    /** 将空存储绑定到固定日志身份，已有不同身份必须拒绝。 */
    void initialize(UUID engineId, String shard);

    /** 复制模式要求独立副本所有权，不允许多个不同节点共享同一应用前缀。 */
    default boolean supportsReplicaOwnership() { return false; }

    /** 绑定节点专属后端；已有普通存储或另一节点的存储必须拒绝接管。 */
    default void initializeReplica(WalReplicaIdentity identity, String shard) { throw new UnsupportedOperationException("replica ownership unsupported"); }

    /** 取得包含内容摘要的持久物化前缀。 */
    WalCheckpoint checkpoint();

    /** 原子应用连续日志项和检查点，允许提交确认丢失后重复调用。 */
    WalCheckpoint apply(List<WalCommit> commits);

    /** 返回快照下最新版本，墓碑由上层统一遮蔽。 */
    Optional<WalValue> get(String key, long readSequence);

    /** 查询持久化最新版本栅栏；缺省实现保证自定义后端兼容，可覆盖为不读取载荷的索引查询。 */
    default long latestSequence(String key) {
        return get(key, Long.MAX_VALUE).map(WalValue::commitSequence).orElse(0L);
    }

    /** 按 UTF-8 二进制顺序返回半开区间的快照版本。 */
    NavigableMap<String, WalValue> scan(String fromInclusive, String toExclusive, long readSequence);

    /** 有界清除历史版本，保留安全点锚点；锚点为墓碑时可退休墓碑及全部更旧版本，重放不得复活。 */
    int gc(long safePoint, int keyLimit);

    /** 后端必须显式提供扫描、删除和时间预算，才允许托管自动维护。 */
    default boolean supportsBoundedMaintenance() { return false; }

    /** 在受保护的安全点上执行一次有限回收，时间预算为协作式，单次 I/O 另受驱动超时约束。 */
    default GcProgress gcBounded(long safePoint, int keyLimit, int rowLimit, long budgetMillis) {
        throw new UnsupportedOperationException("bounded backend maintenance is not supported");
    }

    /** 返回实际检查键和删除行数，不能用删除数冒充扫描工作量。 */
    record GcProgress(int inspectedKeys, int removedRows, boolean wrapped) {
        /** 拒绝无效回收统计，便于运行时核对后端遵守预算。 */
        public GcProgress { if (inspectedKeys < 0 || removedRows < 0) { throw new IllegalArgumentException("invalid GC progress"); } }
    }

    /** 托管装配在创建 WAL 前校验持久性和超时，未验证的自定义后端不能默认宣称生产可服务。 */
    default void validateProduction() { throw new UnsupportedOperationException("backend production validation is not supported"); }

    /** 自定义后端必须显式实现备份和可重试恢复，不能默认用有行数上限的普通扫描代替。 */
    default boolean supportsBackup() { return false; }

    /** 后端须在打开时固定读视图，才能在并发物化与 GC 期间导出同一提交边界。 */
    default boolean supportsPinnedSnapshot() { return false; }

    /** 调用发生于已追平且暂停物化的准备阶段，返回后必须已经固定实际数据库读视图。 */
    default SnapshotView openSnapshot(WalCheckpoint boundary) { throw new UnsupportedOperationException("pinned snapshot unsupported"); }

    /** 一个备份独占的只读视图，结束或失败以后归还自己的连接，不影响共享 WAL。 */
    interface SnapshotView extends AutoCloseable {
        /** 只输出打开时的边界数据，不在此时重新建立可能已推进的读视图。 */
        void export(BackupSink sink) throws java.io.IOException;
        /** 关闭本视图自己的只读事务与资源。 */
        @Override void close() throws java.io.IOException;
    }

    /** 只有显式校验恢复目标副本身份的适配器才能接收可迁移复制快照。 */
    default boolean supportsReplicaRestore() { return false; }

    /** 流式输出固定确认前缀下每个键的最新版本，输出键必须按 UTF-8 二进制顺序递增。 */
    default void exportSnapshot(WalCheckpoint boundary, BackupSink sink) throws java.io.IOException {
        throw new UnsupportedOperationException("backend backup is not supported");
    }

    /** 从全新存储开始恢复或继续同一备份，返回是否仍需导入数据；普通运行时必须拒绝恢复中存储。 */
    default boolean beginRestore(WalRestoreSpec spec) { throw new UnsupportedOperationException("backend restore is not supported"); }

    /** 幂等导入有限批次，所有变更仍处于不可服务的恢复中状态。 */
    default void restoreBatch(WalRestoreSpec spec, List<WalBackupRow> rows) { throw new UnsupportedOperationException("backend restore is not supported"); }

    /** 核对完整物理数据摘要和行数后，原子发布此后端的完整确认前缀。 */
    default void finishRestore(WalRestoreSpec spec, long expectedRows, String expectedDataHash) {
        throw new UnsupportedOperationException("backend restore is not supported");
    }

    /** 公开存储扩展接口的流式行消费方，不暴露运行时内部锁与版本链。 */
    @FunctionalInterface interface BackupSink {
        /** 处理一条最新版本，写出失败必须传播以阻断备份发布。 */
        void accept(WalBackupRow row) throws java.io.IOException;
    }
}

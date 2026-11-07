package com.flyingzc.azir.wal;

import java.util.Objects;
import java.util.UUID;

/** 一份不可混用的后端恢复身份，绑定备份、引擎、分片和确认提交边界。 */
public record WalRestoreSpec(UUID backupId, UUID operationId, UUID engineId, String shard, WalCheckpoint boundary, WalReplicaIdentity replica) {
    /** 原有单机恢复的操作身份和格式保持不变。 */
    public WalRestoreSpec(UUID backupId, UUID operationId, UUID engineId, String shard, WalCheckpoint boundary) {
        this(backupId, operationId, engineId, shard, boundary, null);
    }
    /** 为独立存储适配测试保留明确操作身份，应用恢复入口会另外绑定目标目录。 */
    public WalRestoreSpec(UUID backupId, UUID engineId, String shard, WalCheckpoint boundary) { this(backupId, backupId, engineId, shard, boundary); }

    /** 所有重试必须携带同一身份和边界，不能用另一份备份接管部分恢复。 */
    public WalRestoreSpec {
        Objects.requireNonNull(backupId); Objects.requireNonNull(operationId); Objects.requireNonNull(engineId); Objects.requireNonNull(boundary);
        if (replica != null && !replica.engineId().equals(engineId)) { throw new IllegalArgumentException("restore replica engine mismatch"); }
        new WalMutation(shard, "", null);
        if (shard.contains("\u0000") || boundary.sequence() < 0
                || !(boundary.sequence() == 0 ? boundary.digest().isEmpty() : boundary.digest().matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("invalid restore identity");
        }
    }
}

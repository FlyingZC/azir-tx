package com.flyingzc.azir.wal;

import java.util.Objects;

/** 后端备份中的单键最新版本，墓碑仍携带原始提交序号。 */
public record WalBackupRow(String key, WalValue version) {
    /** 拒绝缺失身份或非法版本，不允许备份导入无法由正常事务产生的数据。 */
    public WalBackupRow {
        Objects.requireNonNull(version); new WalMutation("backup", key, null);
        if (version.commitSequence() < 1 || version.valueLength() > 8 * 1024 * 1024) {
            throw new IllegalArgumentException("invalid backup version");
        }
    }
}

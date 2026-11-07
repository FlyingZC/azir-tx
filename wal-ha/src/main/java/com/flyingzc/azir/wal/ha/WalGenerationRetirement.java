package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.IOException;
import java.util.UUID;

/** 节点锁内临时授予的旧代次退休能力，普通业务不能自行构造或长期保存此许可。 */
public final class WalGenerationRetirement {
    private final WalReplicaIdentity identity;
    private final UUID generation;
    private final Validator validator;

    /** 只有本包的持久状态管理器在检查当前和安装代次后签发。 */
    WalGenerationRetirement(WalReplicaIdentity identity, UUID generation, Validator validator) { this.identity = identity; this.generation = generation; this.validator = validator; }
    /** 固定节点归属供后端核对。 */
    public WalReplicaIdentity identity() { return identity; }
    /** 固定退休对象，不接受任意物理数据库名。 */
    public UUID generation() { return generation; }
    /** 每次破坏性步骤前复核许可仍在节点锁、有效生命周期及工作预算内。 */
    public void verify() throws IOException { validator.verify(); }
    /** 许可与单次节点维护调用绑定，退出后失效。 */
    @FunctionalInterface interface Validator {
        /** 不满足保护条件时必须失败，不能降级为直接删除。 */
        void verify() throws IOException;
    }
}

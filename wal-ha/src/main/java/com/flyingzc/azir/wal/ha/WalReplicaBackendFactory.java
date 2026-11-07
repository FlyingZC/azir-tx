package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/** 按副本与稳定代次分配独立后端，重开必须返回同一空间，新代次不得复用旧代次数据库。 */
@FunctionalInterface
public interface WalReplicaBackendFactory {
    /** 返回未初始化的适配器及自有资源；分配可以幂等重试，不得清空已有空间。 */
    Lease open(WalReplicaIdentity identity, UUID generation) throws IOException;

    /** 只有显式实现不复活和幂等删除协议的适配器才能回收后端。 */
    default boolean supportsRetirement() { return false; }

    /** 游标属于适配器，单轮只处理有限分片；许可只能由持有节点锁的状态管理器传入。 */
    default RetirementProgress retire(WalGenerationRetirement permit, String cursor, int shardLimit) throws IOException { throw new IOException("backend retirement not supported"); }

    /** 后端退休进度不可伪装为事务数据或额度已释放。 */
    record RetirementProgress(String cursor, boolean complete, int inspected, int dropped) {
        /** 拒绝无界游标和错误计数。 */
        public RetirementProgress { if (cursor == null || cursor.length() > 512 || inspected < 0 || dropped < 0 || dropped > inspected) { throw new IllegalArgumentException("invalid backend retirement progress"); } }
    }

    /** 纯本地存储也使用同一代次协议，不伪装成 MySQL 冷副本。 */
    static WalReplicaBackendFactory localOnly() { return new WalReplicaBackendFactory() {
        /** 本地模式没有外部存储空间。 */
        @Override public Lease open(WalReplicaIdentity identity, UUID generation) { return new Lease(Map.of(), () -> { }); }
        /** 本地目录由节点自己安全回收。 */
        @Override public boolean supportsRetirement() { return true; }
        /** 后端阶段无需删除，但仍须验证许可。 */
        @Override public RetirementProgress retire(WalGenerationRetirement permit, String cursor, int limit) throws IOException { permit.verify(); return new RetirementProgress("local", true, 0, 0); }
    }; }

    /** 一代后端的自有生命周期，节点关闭只释放本次租约资源。 */
    final class Lease implements AutoCloseable {
        private final Map<String, WalMaterializedStore> stores;
        private final Release release;
        private boolean closed;

        /** 连接池归属由工厂明确提供，不能默认关闭调用方共享连接池。 */
        public Lease(Map<String, WalMaterializedStore> stores, Release release) {
            this.stores = Map.copyOf(stores); this.release = java.util.Objects.requireNonNull(release);
        }

        /** 路由不可被调用方在安装中途更换。 */
        public Map<String, WalMaterializedStore> stores() { return stores; }

        /** 成功后才标记关闭；失败保留重试能力，并发关闭须等待实际释放结果。 */
        @Override public synchronized void close() throws IOException {
            if (closed) { return; }
            release.close();
            closed = true;
        }
    }

    /** 工厂只释放自己拥有的资源，不允许借关闭回调删除任意业务数据。 */
    @FunctionalInterface interface Release {
        /** 有界关闭本代自有资源；部分释放后失败时，重试须继续清理剩余资源。 */
        void close() throws IOException;
    }
}

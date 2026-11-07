package com.flyingzc.azir.txn;

/**
 * SQL 等上层执行器使用的语句级读取快照，保证一次关系运算内的多次分片扫描共享同一时间戳。
 */
public final class StatementReadSnapshot {
    private final long txId;
    private final long readTs;

    /**
     * 仅允许事务管理器所在包创建经过隔离级别和安全点校验的快照。
     */
    StatementReadSnapshot(long txId, long readTs) {
        this.txId = txId;
        this.readTs = readTs;
    }

    /**
     * 返回快照绑定的事务编号。
     */
    public long txId() {
        return txId;
    }

    /**
     * 返回语句读取时间戳。
     */
    public long readTs() {
        return readTs;
    }
}

package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** 由单个新模式运行时拥有的事务句柄，状态仅由运行时锁保护。 */
public final class WalTransaction {
    /** 新模式事务生命周期，未知结果不能再走业务回滚。 */
    enum State { ACTIVE, QUEUED, COMMITTED, ABORTED, UNKNOWN }
    final Object owner;
    final UUID id = UUID.randomUUID();
    final long start;
    final long deadlineNanos;
    final IsolationLevel isolation;
    final Map<WalMvccRuntime.Key, WalMutation> writes = new LinkedHashMap<>();
    final CompletableFuture<WalReceipt> completion = new CompletableFuture<>();
    State state = State.ACTIVE;
    long writeBytes;
    long epoch;

    /** 固定事务归属、快照和不可续期的期限。 */
    WalTransaction(Object owner, long start, long deadlineNanos, IsolationLevel isolation) {
        this.owner = owner;
        this.start = start;
        this.deadlineNanos = deadlineNanos;
        this.isolation = isolation;
    }

    /** 返回跨重启查询提交结果所需的事务号。 */
    public UUID id() { return id; }

    /** 返回固定的起始快照与写冲突边界。 */
    public long startSequence() { return start; }
}

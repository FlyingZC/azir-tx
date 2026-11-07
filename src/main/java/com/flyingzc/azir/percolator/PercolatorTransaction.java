package com.flyingzc.azir.percolator;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.KeyRange;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** 独立 Percolator 会话；缓冲写集在预写后冻结，UNKNOWN 会话只能查询原身份。 */
public final class PercolatorTransaction implements AutoCloseable {
    /** 会话状态；UNKNOWN 不能当作回滚后重新提交。 */
    public enum State { ACTIVE, PREWRITTEN, COMMITTED, ABORTED, UNKNOWN }

    final PercolatorRuntime runtime;
    final long startTs;
    final IsolationLevel isolation;
    final NavigableMap<PercolatorKey, Mutation> writes = new TreeMap<>();
    State state = State.ACTIVE;
    PercolatorRuntime.Handle handle;
    PercolatorRuntime.Commit result;
    long writeBytes;
    private final ReentrantLock sessionLock = new ReentrantLock();

    /** 仅由运行时构造拥有固定快照边界的会话。 */
    PercolatorTransaction(PercolatorRuntime runtime, long startTs, IsolationLevel isolation) {
        this.runtime = runtime;
        this.startTs = startTs;
        this.isolation = isolation;
    }

    /** 返回不可变化的原始事务标识。 */
    public long startTs() { return startTs; }

    /** 返回当前会话状态。 */
    public State state() { return locked(() -> state); }

    /** 返回预写后可用于重启查询的 primary 地址和原 startTs。 */
    public Optional<PercolatorRuntime.Handle> handle() { return locked(() -> Optional.ofNullable(handle)); }

    /** 缓冲写入并复制业务值，避免调用方在预写后修改事务内容。 */
    public void put(String shard, String key, byte[] value) {
        locked(() -> {
            java.util.Objects.requireNonNull(value, "value");
            runtime.live(this, startTs, () -> { buffer(new PercolatorKey(shard, key), Mutation.put(key, value)); return null; });
            return null;
        });
    }

    /** 缓冲删除；提交后以 tombstone 版本表达。 */
    public void delete(String shard, String key) {
        locked(() -> runtime.live(this, startTs, () -> { buffer(new PercolatorKey(shard, key), Mutation.delete(key)); return null; }));
    }

    /** 每次便利读取代表一个新语句；RR 使用 startTs，RC 分配新读时间戳。 */
    public Optional<byte[]> get(String shard, String key) {
        return statement().get(shard, key);
    }

    /** 便利分页每次代表一个新语句；跨页固定 RC 快照时应复用 statement 返回的视图。 */
    public PercolatorScanPage scanPage(String shard, KeyRange range, String afterKeyExclusive, int limit) {
        return statement().scanPage(shard, range, afterKeyExclusive, limit);
    }

    /** 为同一语句的多次跨片点读取得同一个全局边界。 */
    public ReadView statement() {
        return locked(() -> {
            readable();
            long readTs = runtime.live(this, startTs, () -> isolation == IsolationLevel.REPEATABLE_READ ? startTs : runtime.nextTimestamp());
            return new ReadView(readTs);
        });
    }

    /** 显式预写用于分阶段执行和恢复演练；后续只能提交、回滚或读取。 */
    public void prewrite() {
        locked(() -> {
            try { return runtime.live(this, startTs, () -> { runtime.prewrite(this); return null; }); }
            catch (Error fatal) { sealAfterFatal(); throw fatal; }
        });
    }

    /** 提交并返回 primary 的持久凭据，重复确认提交返回同一结果。 */
    public PercolatorRuntime.Commit commit() {
        return locked(() -> {
            if (state == State.COMMITTED) { return result; }
            try { return runtime.live(this, startTs, () -> runtime.commit(this)); }
            catch (RuntimeException failure) {
                if (state == State.COMMITTED && result != null) { return result; }
                throw failure;
            }
            catch (Error fatal) { sealAfterFatal(); throw fatal; }
            finally { runtime.release(this); }
        });
    }

    /** 主动回滚必须先在 primary 上持久确定终态。 */
    public void rollback() {
        locked(() -> {
            try { return runtime.terminal(() -> { runtime.rollback(this); return null; }); }
            catch (Error fatal) { sealAfterFatal(); throw fatal; }
            finally { runtime.release(this); }
        });
    }

    /** 致命错误后禁止继续修改原写集；预写尚未建立 primary 时可明确结束本地空参与者事务。 */
    private void sealAfterFatal() {
        if (state != State.COMMITTED && state != State.ABORTED) { state = handle == null ? State.ABORTED : State.UNKNOWN; }
    }
    /** 活动会话关闭时回滚；UNKNOWN 保留原身份，不假装回滚成功。 */
    @Override
    public void close() {
        locked(() -> {
            if (state == State.ACTIVE || state == State.PREWRITTEN) { rollback(); }
            return null;
        });
    }

    /** JDBC 阻塞期间使用可卸载的显式锁，避免 Java 21 synchronized 固定虚拟线程载体。 */
    private <T> T locked(Supplier<T> work) {
        sessionLock.lock();
        try { return work.get(); } finally { sessionLock.unlock(); }
    }

    /** 使用有界覆盖后的写集合，避免单事务占满进程内存。 */
    private void buffer(PercolatorKey key, Mutation mutation) {
        if (state != State.ACTIVE) { throw illegal("writes are frozen after prewrite"); }
        runtime.store(key.shard());
        Mutation old = writes.get(key);
        long oldSize = old == null ? 0 : size(old);
        long nextSize = writeBytes - oldSize + size(mutation);
        int nextKeys = writes.size() + (old == null ? 1 : 0);
        if (nextKeys > runtime.options().maxKeys() || nextSize > runtime.options().maxBytes()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "Percolator write budget exceeded", false, startTs);
        }
        // 写集上限先于载荷复制，避免拒绝超大请求时仍制造等量堆分配。
        Mutation frozen = mutation.value() == null ? mutation : new Mutation(mutation.key(), mutation.type(), mutation.value().clone());
        writes.put(key, frozen);
        writeBytes = nextSize;
    }

    /** 计算键和值的 UTF-8 写集合占用量。 */
    private static long size(Mutation mutation) {
        return com.flyingzc.azir.storage.Utf8KeyOrder.encode(mutation.key()).length
                + (long) (mutation.value() == null ? 0 : mutation.value().length);
    }

    /** 拒绝终态会话继续访问缓冲写或存储。 */
    void readable() {
        if (state != State.ACTIVE && state != State.PREWRITTEN) { throw illegal("transaction is " + state); }
    }

    /** 生成会话状态错误。 */
    MvccException illegal(String message) { return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, startTs); }

    /** 固定时间戳的语句视图，仍然覆盖当前事务自己的缓冲写。 */
    public final class ReadView {
        private final long readTs;

        /** 创建固定边界的语句视图。 */
        private ReadView(long readTs) { this.readTs = readTs; }

        /** 返回语句的全局逻辑时间戳，便于隔离验证。 */
        public long readTs() { return readTs; }

        /** 在会话锁下读取自己的写或解析远端快照，终态后视图同时失效。 */
        public Optional<byte[]> get(String shard, String key) {
            return locked(() -> runtime.live(PercolatorTransaction.this, readTs, () -> {
                readable();
                PercolatorKey address = new PercolatorKey(shard, key);
                runtime.store(shard);
                Mutation own = writes.get(address);
                if (own != null) { return Optional.ofNullable(own.value()).map(byte[]::clone); }
                return runtime.read(address, readTs);
            }));
        }

        /** 同一语句跨页保留 readTs，并覆盖尚未预写的本事务新增、更新和删除。 */
        public PercolatorScanPage scanPage(String shard, KeyRange range, String afterKeyExclusive, int limit) {
            return locked(() -> runtime.live(PercolatorTransaction.this, readTs, () -> {
                readable();
                return runtime.scanPage(PercolatorTransaction.this, readTs, shard, range, afterKeyExclusive, limit);
            }));
        }
    }
}

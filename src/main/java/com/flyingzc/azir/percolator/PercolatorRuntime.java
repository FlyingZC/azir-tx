package com.flyingzc.azir.percolator;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.tso.TimestampOracle;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import static com.flyingzc.azir.percolator.PercolatorStore.*;
import static com.flyingzc.azir.percolator.PercolatorTransaction.State.*;

/** 以 primary 为跨片唯一持久提交权威的 Percolator 协调器；读租约只保护快照。 */
public final class PercolatorRuntime implements AutoCloseable {
    private final TimestampOracle tso;
    private final Map<String, PercolatorStore> stores;
    private final Options options;
    private final MysqlPercolatorLifecycle lifecycle;
    private volatile boolean closed;

    /** 有界事务参数；batchByShard 只改变本地批量粒度，不改变提交语义。 */
    public record Options(long lockTtlMillis, int maxKeys, long maxBytes, boolean batchByShard) {
        /** 校验锁租约和内存预算。 */
        public Options {
            if (lockTtlMillis < 1 || lockTtlMillis > 86_400_000L || maxKeys < 1 || maxBytes < 1) {
                throw new IllegalArgumentException("invalid Percolator options");
            }
        }

        /** 默认同片整批预写与物化，锁租约一分钟。 */
        public static Options defaults() { return new Options(60_000, 4096, 16L * 1024 * 1024, true); }
    }

    /** 应用需保存原始 startTs、完整 primary 地址和生产命名空间，才能跨进程确认。 */
    public record Handle(long startTs, PercolatorKey primary, String namespace) {
        /** 保留自定义运行时的旧凭据构造方式；生产入口必须使用带命名空间的凭据。 */
        public Handle(long startTs, PercolatorKey primary) { this(startTs, primary, null); }
        /** 拒绝无效恢复身份。 */
        public Handle {
            if (startTs <= 0) { throw new IllegalArgumentException("startTs must be positive"); }
            Objects.requireNonNull(primary, "primary");
        }
    }

    /** 已确认的提交结果；pendingSecondaries 非空表示物化待恢复，提交决议已经持久。 */
    public record Commit(Handle handle, long commitTs, List<PercolatorKey> pendingSecondaries) {
        /** 固化剩余键列表，不把可变写集合暴露给业务。 */
        public Commit { pendingSecondaries = List.copyOf(pendingSecondaries); }
    }

    /** 使用默认批量参数装配，TSO 必须跨所有实例严格线性递增，禁止分段本地发号。 */
    public PercolatorRuntime(TimestampOracle tso, Map<String, ? extends PercolatorStore> stores) {
        this(tso, stores, Options.defaults());
    }

    /** 显式选择批量粒度；各逻辑分片必须映射到独立协议命名空间。 */
    public PercolatorRuntime(TimestampOracle tso, Map<String, ? extends PercolatorStore> stores, Options options) {
        this(tso, stores, options, null);
    }

    /** 生产工厂显式提供持久租约，旧自定义运行时不自动获得 GC 权限。 */
    public PercolatorRuntime(TimestampOracle tso, Map<String, ? extends PercolatorStore> stores, Options options,
                              MysqlPercolatorLifecycle lifecycle) {
        this.tso = Objects.requireNonNull(tso, "tso");
        this.lifecycle = lifecycle;
        this.options = Objects.requireNonNull(options, "options");
        if (stores.isEmpty()) { throw new IllegalArgumentException("stores must not be empty"); }
        stores.forEach((id, store) -> {
            if (!id.equals(store.shardId())) { throw new IllegalArgumentException("shard mapping mismatch"); }
        });
        this.stores = Map.copyOf(stores);
    }

    /** 创建独立会话；生产入口同时登记持久读租约，不建立中心提交决议。 */
    public PercolatorTransaction begin(IsolationLevel isolation) {
        ensureOpen();
        Objects.requireNonNull(isolation, "isolation");
        return new PercolatorTransaction(this, lifecycle == null ? nextTimestamp() : lifecycle.begin(), isolation);
    }

    /** 查询原 primary；过期或缺失锁会在原子栅栏下确定回滚，未过期锁返回 PENDING。 */
    public Decision lookup(Handle handle) {
        ensureOpen();
        if (lifecycle != null && !lifecycle.namespace().equals(handle.namespace())) {
            throw new IllegalArgumentException("Percolator namespace mismatch");
        }
        return store(handle.primary().shard()).primary(handle.primary(), handle.startTs(), Action.INSPECT, 0);
    }

    /** 有界恢复各片残留锁；缓存同一 primary 的终态，禁止缓存 PENDING。 */
    public int recover(int limitPerShard) {
        ensureOpen();
        if (limitPerShard < 1 || limitPerShard > 4096) { throw new IllegalArgumentException("limit must be in 1..4096"); }
        return lifecycle == null ? recoverLocks(limitPerShard) : lifecycle.gate(() -> recoverLocks(limitPerShard));
    }
    /** 当前生产控制门禁覆盖恢复步骤，缓存只保留不可逆终态。 */
    private int recoverLocks(int limitPerShard) {
        Map<Handle, Decision> decisions = new LinkedHashMap<>(); int finished = 0;
        for (PercolatorStore store : stores.values()) {
            for (Lock lock : store.pending(limitPerShard)) {
                com.flyingzc.azir.operations.MysqlProtocolState.checkInterrupted();
                finished += lifecycle == null ? recoverLock(store, lock, decisions) : lifecycle.gate(() -> recoverLock(store, lock, decisions));
            }
        }
        return finished;
    }
    /** 每个已发现锁都重新确认当前控制阶段，保留原 primary 作为真正的终态权威。 */
    private int recoverLock(PercolatorStore store, Lock lock, Map<Handle, Decision> decisions) {
        Handle handle = identity(lock.startTs(), lock.primary()); Decision decision = decisions.get(handle);
        if (decision == null) {
            decision = lookup(handle);
            if (decision.state() != State.PENDING) { decisions.put(handle, decision); }
        }
        if (decision.state() == State.RETIRED) { throw new MvccException(ErrorCode.ILLEGAL_STATE, "retired identity still owns a lock", false, lock.startTs()); }
        if (decision.state() == State.PENDING) { return 0; }
        store.finish(lock.startTs(), lock.primary(), decision, List.of(lock.key().key())); return 1;
    }

    /** 生产运行时执行恢复和安全 GC；自定义运行时仅恢复，不能猜测跨实例读者。 */
    public com.flyingzc.azir.maintenance.MaintenanceResult maintain(int limit) {
        if (lifecycle == null) { return new com.flyingzc.azir.maintenance.MaintenanceResult(recover(limit), 0, 0); }
        var state = lifecycle.operations().status();
        if (!state.mode().equals("ACTIVE")) { return new com.flyingzc.azir.maintenance.MaintenanceResult(0, 0, state.retiredBefore(), true); }
        return lifecycle.gate(() -> lifecycle.collect(stores, limit, recover(limit)));
    }

    /** 返回具有持久代次保护的生产管理接口。 */
    public com.flyingzc.azir.operations.MysqlProtocolAdmin administration() {
        if (lifecycle == null) { throw new UnsupportedOperationException("custom runtime has no administration fence"); }
        return new com.flyingzc.azir.operations.MysqlProtocolAdmin(lifecycle.operations(), this::recover);
    }
    /** 诊断生产入口的快照保护与残留锁，拒绝对自定义运行时猜测全局读者。 */
    public com.flyingzc.azir.maintenance.MaintenanceDiagnostics diagnostics() {
        ensureOpen();
        if (lifecycle == null) { throw new UnsupportedOperationException("custom runtime has no persistent reader registry"); }
        return lifecycle.gate(() -> lifecycle.diagnostics(stores.values().stream().mapToLong(PercolatorStore::pendingCount).sum()));
    }

    /** 读写在同一个持久租约门禁内执行，GC 不会越过进行中的操作。 */
    <T> T live(PercolatorTransaction tx, long readTs, java.util.function.Supplier<T> work) {
        ensureOpen();
        return lifecycle == null ? work.get() : lifecycle.live(tx.startTs, readTs, work, () -> tx.state == COMMITTED || tx.state == ABORTED);
    }

    /** 回滚和终态处理不要求未过期租约，但仍与 GC 串行。 */
    <T> T terminal(java.util.function.Supplier<T> work) {
        ensureOpen();
        return lifecycle == null ? work.get() : lifecycle.gate(work);
    }

    /** 已结束事务不再钉住快照；释放失败只影响后续维护进度，不改变提交结果。 */
    void release(PercolatorTransaction tx) {
        if (lifecycle != null && (tx.state == COMMITTED || tx.state == ABORTED || tx.state == UNKNOWN)) {
            try { lifecycle.release(tx.startTs); } catch (RuntimeException ignored) { /* 失效租约可由后台有界回收。 */ }
        }
    }

    /** 生产凭据包含持久命名空间，自定义运行时沿用旧合同。 */
    private Handle identity(long start, PercolatorKey primary) {
        return new Handle(start, primary, lifecycle == null ? null : lifecycle.namespace());
    }

    /** 关闭后不能通过残留会话继续读写。 */
    private void ensureOpen() { if (closed) { throw new IllegalStateException("Percolator runtime closed"); } }

    /** 由拥有生命周期的装配入口调用，不关闭调用方的池。 */
    @Override public void close() { closed = true; if (lifecycle != null) { lifecycle.close(); } }

    /** 冻结写集并先预写 primary 所在批次，再预写其他分片。 */
    void prewrite(PercolatorTransaction tx) {
        if (tx.state == PREWRITTEN) { return; }
        if (tx.state != ACTIVE) { throw tx.illegal("cannot prewrite " + tx.state); }
        if (tx.writes.isEmpty()) { tx.state = PREWRITTEN; return; }
        tx.handle = identity(tx.startTs, tx.writes.firstKey());
        try {
            for (List<PercolatorKey> batch : batches(tx.writes.keySet().stream().toList())) {
                live(tx, tx.startTs, () -> {
                    store(batch.getFirst().shard()).prewrite(tx.startTs, tx.handle.primary(), options.lockTtlMillis(),
                            batch.stream().map(tx.writes::get).toList());
                    return null;
                });
            }
            tx.state = PREWRITTEN;
        } catch (RuntimeException failure) {
            try { rollback(tx); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            if (tx.state == UNKNOWN) { throw unknown(tx, failure); }
            throw failure;
        }
    }

    /** 先完成所有预写再申请提交时间戳；primary 结果不确定时保留 UNKNOWN。 */
    Commit commit(PercolatorTransaction tx) {
        if (tx.state == COMMITTED) { return tx.result; }
        if (tx.state != ACTIVE && tx.state != PREWRITTEN) { throw tx.illegal("cannot commit " + tx.state); }
        prewrite(tx);
        if (tx.handle == null) {
            tx.result = new Commit(null, 0, List.of());
            tx.state = COMMITTED;
            return tx.result;
        }
        long commitTs = nextTimestamp();
        if (commitTs <= tx.startTs) { throw tx.illegal("TSO must be strictly globally monotonic"); }
        Decision decision;
        try {
            decision = commitPrimary(tx, commitTs);
        } catch (RuntimeException failure) {
            try { decision = lookup(tx.handle); }
            catch (RuntimeException lookupFailure) {
                failure.addSuppressed(lookupFailure);
                tx.state = UNKNOWN;
                throw unknown(tx, failure);
            }
            if (decision.state() == State.PENDING) {
                // 原 COMMIT 可能仍在数据库执行；禁止把这次 PENDING 当作可重试的新事务。
                tx.state = UNKNOWN;
                throw unknown(tx, failure);
            }
        }
        if (decision.state() == State.ROLLED_BACK) {
            tx.state = ABORTED;
            finishBestEffort(tx, decision, tx.writes.keySet().stream().toList());
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "primary rolled back", true, tx.startTs);
        }
        if (decision.state() != State.COMMITTED) {
            tx.state = UNKNOWN;
            throw unknown(tx, null);
        }
        List<PercolatorKey> secondaries = tx.writes.keySet().stream().filter(key -> !key.equals(tx.handle.primary())).toList();
        // 先保存可重放的已提交凭据，再公布本地终态；补交时即使发生致命错误也不会返回空凭据。
        tx.result = new Commit(tx.handle, decision.commitTs(), secondaries);
        tx.state = COMMITTED;
        List<PercolatorKey> pending = finishBestEffort(tx, decision, secondaries);
        tx.result = new Commit(tx.handle, decision.commitTs(), pending);
        return tx.result;
    }

    /** 在控制后验检查之前保留 primary 已返回的不可逆证明，已确认结果不能被租约清理错误抹掉。 */
    private Decision commitPrimary(PercolatorTransaction tx, long commitTs) {
        Decision[] proof = new Decision[1];
        java.util.function.Supplier<Decision> publish = () -> {
            proof[0] = store(tx.handle.primary().shard()).primary(tx.handle.primary(), tx.startTs, Action.COMMIT, commitTs);
            return proof[0];
        };
        return lifecycle == null ? publish.get() : lifecycle.live(tx.startTs, tx.startTs, publish,
                () -> proof[0] != null && (proof[0].state() == State.COMMITTED || proof[0].state() == State.ROLLED_BACK));
    }

    /** 只在 primary 确认回滚后清理 secondary，已提交或 UNKNOWN 会话拒绝假回滚。 */
    void rollback(PercolatorTransaction tx) {
        if (tx.state == ABORTED) { return; }
        if (tx.state == COMMITTED || tx.state == UNKNOWN) { throw tx.illegal("cannot rollback " + tx.state); }
        if (tx.handle == null) { tx.state = ABORTED; return; }
        Decision decision;
        try {
            decision = store(tx.handle.primary().shard()).primary(tx.handle.primary(), tx.startTs, Action.ROLLBACK, 0);
        } catch (RuntimeException failure) {
            tx.state = UNKNOWN;
            throw unknown(tx, failure);
        }
        if (decision.state() != State.ROLLED_BACK) {
            tx.state = UNKNOWN;
            throw unknown(tx, null);
        }
        tx.state = ABORTED;
        List<PercolatorKey> pending = finishBestEffort(tx, decision, tx.writes.keySet().stream().toList());
        if (!pending.isEmpty()) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "primary rolled back; secondary cleanup pending: " + pending.size(), true, tx.startTs);
        }
    }

    /** 读取遇到相关锁时先解析 primary；绝不能在潜在 commitTs 不超过边界时返回旧值。 */
    Optional<byte[]> read(PercolatorKey key, long readTs) {
        PercolatorStore local = store(key.shard());
        for (int attempt = 0; attempt < 16; attempt++) {
            Snapshot snapshot = local.snapshot(key.key(), readTs);
            if (snapshot.lock().isEmpty() || snapshot.lock().get().startTs() > readTs) {
                return snapshot.value().filter(value -> !value.deleted()).map(value -> value.value().clone());
            }
            Lock lock = snapshot.lock().get();
            Decision decision = lookup(identity(lock.startTs(), lock.primary()));
            if (decision.state() == State.RETIRED) { throw new MvccException(ErrorCode.ILLEGAL_STATE, "retired identity still owns a lock", false, lock.startTs()); }
            if (decision.state() == State.PENDING) {
                throw new MvccException(ErrorCode.LOCK_CONFLICT, "unresolved primary for " + key, true, lock.startTs());
            }
            local.finish(lock.startTs(), lock.primary(), decision, List.of(key.key()));
            // 补交后重新建立本地一致性视图，避免使用解析前的旧值。
        }
        throw new MvccException(ErrorCode.LOCK_CONFLICT, "snapshot lock churn exceeded retry budget", true, null);
    }

    /** 原始候选与本事务写集合有序归并；所有页和锁解析复用调用方同一个 readTs。 */
    PercolatorScanPage scanPage(PercolatorTransaction tx, long readTs, String shard,
                                KeyRange range, String afterKeyExclusive, int limit) {
        PercolatorStore local = store(shard);
        Objects.requireNonNull(range, "range");
        if (limit < 1 || limit > 4096) { throw new IllegalArgumentException("page limit must be in 1..4096"); }
        if (range.startKeyInclusive() != null) { Utf8KeyOrder.encode(range.startKeyInclusive()); }
        if (range.endKeyExclusive() != null) { Utf8KeyOrder.encode(range.endKeyExclusive()); }
        if (afterKeyExclusive != null) { Utf8KeyOrder.encode(afterKeyExclusive); }
        TreeSet<String> candidates = new TreeSet<>(Utf8KeyOrder::compare);
        candidates.addAll(local.keys(range, afterKeyExclusive, limit + 1));
        tx.writes.keySet().stream().filter(key -> key.shard().equals(shard))
                .map(PercolatorKey::key).filter(key -> inRange(key, range, afterKeyExclusive))
                .limit((long) limit + 1).forEach(candidates::add);
        List<String> pageKeys = candidates.stream().limit(limit).toList();
        List<PercolatorScanPage.Entry> entries = new ArrayList<>();
        for (String key : pageKeys) {
            Mutation own = tx.writes.get(new PercolatorKey(shard, key));
            byte[] value = own != null ? own.value() : read(new PercolatorKey(shard, key), readTs).orElse(null);
            entries.add(new PercolatorScanPage.Entry(key, value));
        }
        return new PercolatorScanPage(entries, candidates.size() > limit ? pageKeys.getLast() : null);
    }

    /** 排他游标与包含型下界共同限制本地缓冲键。 */
    private static boolean inRange(String key, KeyRange range, String afterKeyExclusive) {
        return (range.startKeyInclusive() == null || Utf8KeyOrder.compare(key, range.startKeyInclusive()) >= 0)
                && (range.endKeyExclusive() == null || Utf8KeyOrder.compare(key, range.endKeyExclusive()) < 0)
                && (afterKeyExclusive == null || Utf8KeyOrder.compare(key, afterKeyExclusive) > 0);
    }

    /** 返回运行时固定选项。 */
    Options options() { return options; }

    /** 拒绝读取或写入未注册的逻辑分片。 */
    PercolatorStore store(String shard) {
        PercolatorStore store = stores.get(shard);
        if (store == null) { throw new IllegalArgumentException("unknown shard: " + shard); }
        return store;
    }

    /** 全局时间戳服务必须保证所有事务和语句跨实例线性排序。 */
    long nextTimestamp() {
        // 新阶段必须确认仍持有原控制会话，不能在门禁连接断开后另借连接继续发布。
        long timestamp = lifecycle == null ? tso.nextTimestamp() : lifecycle.gate(tso::nextTimestamp);
        if (timestamp <= 0) { throw new IllegalStateException("TSO returned nonpositive timestamp"); }
        return timestamp;
    }

    /** 批量完成所有 secondary；返回失败键供应用观察并由读侧或 recover 补完。 */
    private List<PercolatorKey> finishBestEffort(PercolatorTransaction tx, Decision decision, List<PercolatorKey> keys) {
        List<PercolatorKey> pending = new ArrayList<>();
        for (List<PercolatorKey> batch : batches(keys)) {
            try {
                store(batch.getFirst().shard()).finish(tx.startTs, tx.handle.primary(), decision,
                        batch.stream().map(PercolatorKey::key).toList());
            } catch (RuntimeException failure) { pending.addAll(batch); }
        }
        return List.copyOf(pending);
    }

    /** 相邻同分片键合并成本地事务；关闭开关可作等语义逐键性能基线。 */
    private List<List<PercolatorKey>> batches(List<PercolatorKey> keys) {
        if (!options.batchByShard()) { return keys.stream().map(List::of).toList(); }
        Map<String, List<PercolatorKey>> grouped = new LinkedHashMap<>();
        for (PercolatorKey key : keys) { grouped.computeIfAbsent(key.shard(), ignored -> new ArrayList<>()).add(key); }
        return grouped.values().stream().map(List::copyOf).toList();
    }

    /** 提交身份不确定时禁止业务自动换身份重试。 */
    private static MvccException unknown(PercolatorTransaction tx, Throwable cause) {
        return new PercolatorTransactionStatusUnknownException(tx.handle, cause);
    }
}

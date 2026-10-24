package mvcc.wal;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.storage.Utf8KeyOrder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** 单写者 WAL/MVCC MVP：提交由本地可靠日志决定，数据库仅异步保存版本。 */
public final class WalMvccRuntime implements AutoCloseable {
    /** 用于分离分片和用户键的内部键身份。 */
    record Key(String shard, String key) { }

    /** 故障验收位置，不参与正常事务语义。 */
    enum Stage { BEFORE_APPEND, AFTER_FORCE, BEFORE_PUBLISH, DURING_INSTALL, DURING_RECOVERY, AFTER_PUBLISH, BEFORE_MATERIALIZER_JOIN,
        BEFORE_CHECKPOINT_FILES, AFTER_RECEIPTS_FORCE, AFTER_SNAPSHOT_FORCE, BEFORE_CURRENT_MOVE,
        AFTER_CURRENT_MOVE, AFTER_CURRENT_FORCE, BEFORE_OLD_GENERATION_DELETE, AFTER_OLD_GENERATION_DELETE,
        AFTER_BACKUP_FILE_COPY, BEFORE_BACKUP_COMPLETE, AFTER_BACKUP_COMPLETE, AFTER_RESTORE_FILE_COPY,
        AFTER_RESTORE_BATCH, BEFORE_RESTORE_PUBLISH, AFTER_RESTORE_MOVE, AFTER_RESTORE_READY }

    /** 确定性注入提交失败或进程强杀的测试接口。 */
    @FunctionalInterface interface FaultHook {
        /** 在指定边界执行注入动作。 */
        void hit(Stage stage);
    }

    /** 提供提交、内存和后台积压的独立统计。 */
    public record Stats(long publishedSequence, long safePoint, long retainedBytes, long bufferedBytes,
                        long walBytes, long groupForces, int activeTransactions, int queuedCommits,
                        Map<String, Long> materializedSequences, String materializationFailure,
                        long checkpointSequence, int pendingReceipts, int receiptArchives,
                        long retiredReceiptThrough, String maintenanceFailure, long retiredMemoryVersions,
                        long hotCacheBytes, int hotCacheKeys, long cacheHits, long cacheMisses,
                        long cacheEvictions, long conflictStoreLookups, long materializationLag, boolean backupRunning,
                        long receiptArchiveBytes, long oldestActiveMillis) { }

    private final Object gate = new Object();
    private final Object flushGate = new Object();
    private final Object decisionGate = new Object();
    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();
    private final Object owner = new Object();
    private final Set<String> shards;
    private final Map<String, WalMaterializedStore> stores;
    private final Map<String, WalCheckpoint> checkpoints = new HashMap<>();
    private final WalOptions options;
    private final WalRetentionOptions retention;
    private final WalReplicaIdentity replica;
    private final WalCommitAuthority authority;
    private volatile boolean proposalBlocked;
    private long pendingReplicaEpoch;
    private final java.util.function.LongSupplier wallClock;
    private volatile long lastCheckpointNanos = System.nanoTime();
    private long nextCheckpointAttemptNanos = System.nanoTime();
    private volatile String maintenanceFailure;
    private final java.util.LinkedHashSet<Key> memoryGcKeys = new java.util.LinkedHashSet<>();
    private long retiredMemoryVersions;
    private final WalLog log;
    private final FaultHook hook;
    private final java.util.function.LongSupplier monotonicClock;
    private final Map<Key, NavigableMap<Long, WalMutation>> versions = new HashMap<>();
    private final Map<Key, Long> latest = new HashMap<>();
    private final LinkedHashMap<Key, Long> hotCache = new LinkedHashMap<>(16, .75f, true);
    private final Set<Key> decisionKeys = new java.util.HashSet<>();
    private long hotCacheBytes;
    private long cacheHits;
    private long cacheMisses;
    private long cacheEvictions;
    private long conflictStoreLookups;
    private final Map<UUID, WalTransaction> active = new HashMap<>();
    private final ArrayBlockingQueue<WalTransaction> queue;
    private final Thread writer;
    private final Thread materializer;
    private volatile boolean accepting = true;
    private volatile boolean flushRunning = true;
    private volatile Throwable fatal;
    private volatile String materializationFailure;
    private volatile boolean backupRunning;
    private volatile boolean replicaExportRunning;
    private volatile boolean readOnly;
    private volatile boolean closed;
    private int gcStoreCursor;
    private long published;
    private long safePoint;
    private long retainedBytes;
    private long bufferedBytes;
    private long pendingIndexBytes;

    /** 创建仅使用 WAL 和内存的独立运行时，便于先验证事务内核。 */
    public static WalMvccRuntime open(Path directory, Set<String> shards, WalOptions options) {
        return openForTest(directory, shards, Map.of(), options, stage -> { });
    }

    /** 创建带异步物化存储的独立运行时，数据源生命周期由调用方管理。 */
    public static WalMvccRuntime open(Path directory, Map<String, WalMaterializedStore> stores, WalOptions options) {
        return openForTest(directory, stores.keySet(), stores, options, stage -> { });
    }

    /** 创建带显式回收策略的纯本地模式，凭据过期必须由调用方主动选择。 */
    public static WalMvccRuntime open(Path directory, Set<String> shards, WalOptions options, WalRetentionOptions retention) {
        return openForTest(directory, shards, Map.of(), options, stage -> { }, WalLog.NORMAL_IO, System::nanoTime, retention, System::currentTimeMillis);
    }

    /** 创建带显式回收策略的物化模式，保持原有 Azir 装配不变。 */
    public static WalMvccRuntime open(Path directory, Map<String, WalMaterializedStore> stores, WalOptions options, WalRetentionOptions retention) {
        return openForTest(directory, stores.keySet(), stores, options, stage -> { }, WalLog.NORMAL_IO, System::nanoTime, retention, System::currentTimeMillis);
    }

    /** 显式创建复制内核，只有可信共识端口可安装已提交批次，不能自动退化为本地提交。 */
    public static WalMvccRuntime openReplica(Path directory, Set<String> shards, Map<String, WalMaterializedStore> stores,
                                              WalOptions options, WalRetentionOptions retention, WalReplicaIdentity identity, WalCommitAuthority authority) {
        Objects.requireNonNull(identity); Objects.requireNonNull(authority);
        if (retention.receiptRetentionMillis() != 0 || stores.values().stream().anyMatch(store -> !store.supportsReplicaOwnership())) {
            throw new IllegalArgumentException("replica replay requires retained receipts and owned backends");
        }
        try { return new WalMvccRuntime(directory, shards, stores, options, stage -> { }, WalLog.NORMAL_IO, System::nanoTime, retention, System::currentTimeMillis, identity, authority); }
        catch (IOException failure) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "open replica failed", null, failure); }
    }

    /** 装配确定性故障注入入口，保持正常公开 API 不暴露协议内部状态。 */
    static WalMvccRuntime openForTest(Path directory, Set<String> shards,
                                      Map<String, WalMaterializedStore> stores, WalOptions options, FaultHook hook) {
        return openForTest(directory, shards, stores, options, hook, WalLog.NORMAL_IO, System::nanoTime);
    }

    /** 为存储失败与租约验收注入 I/O 边界和单调时钟，公开入口保持系统时钟。 */
    static WalMvccRuntime openForTest(Path directory, Set<String> shards,
                                      Map<String, WalMaterializedStore> stores, WalOptions options, FaultHook hook,
                                      WalLog.IoHook ioHook, java.util.function.LongSupplier monotonicClock) {
        return openForTest(directory, shards, stores, options, hook, ioHook, monotonicClock, WalRetentionOptions.preserveAll(), System::currentTimeMillis);
    }

    /** 将保留窗口时钟与租约时钟分开注入，系统时间变化不能改变事务租约。 */
    static WalMvccRuntime openForTest(Path directory, Set<String> shards, Map<String, WalMaterializedStore> stores,
                                     WalOptions options, FaultHook hook, WalLog.IoHook ioHook,
                                     java.util.function.LongSupplier monotonicClock, WalRetentionOptions retention,
                                     java.util.function.LongSupplier wallClock) {
        try { return new WalMvccRuntime(directory, shards, stores, options, hook, ioHook, monotonicClock, retention, wallClock); }
        catch (IOException ex) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "open WAL runtime failed", null, ex); }
    }

    /** 先恢复日志、核对后端前缀，再开放事务和后台线程。 */
    private WalMvccRuntime(Path directory, Set<String> shards, Map<String, WalMaterializedStore> stores,
                           WalOptions options, FaultHook hook, WalLog.IoHook ioHook,
                           java.util.function.LongSupplier monotonicClock, WalRetentionOptions retention,
                           java.util.function.LongSupplier wallClock) throws IOException {
        this(directory, shards, stores, options, hook, ioHook, monotonicClock, retention, wallClock, null, null);
    }

    /** 先核对模式和副本后端身份，再开放本地恢复状态，共识层启动前没有有效领导任期。 */
    private WalMvccRuntime(Path directory, Set<String> shards, Map<String, WalMaterializedStore> stores,
                           WalOptions options, FaultHook hook, WalLog.IoHook ioHook,
                           java.util.function.LongSupplier monotonicClock, WalRetentionOptions retention,
                           java.util.function.LongSupplier wallClock, WalReplicaIdentity replica, WalCommitAuthority authority) throws IOException {
        this.shards = Set.copyOf(shards);
        this.stores = Map.copyOf(stores);
        this.options = Objects.requireNonNull(options);
        this.retention = Objects.requireNonNull(retention);
        this.replica = replica; this.authority = authority;
        this.wallClock = Objects.requireNonNull(wallClock);
        this.hook = Objects.requireNonNull(hook);
        this.monotonicClock = Objects.requireNonNull(monotonicClock);
        if (this.shards.isEmpty() || this.shards.size() > 1024 || !this.shards.containsAll(this.stores.keySet())
                || this.shards.stream().anyMatch(s -> s.isEmpty() || s.contains("\u0000"))) {
            throw new IllegalArgumentException("invalid shard routes");
        }
        this.shards.forEach(shard -> new WalMutation(shard, "", null));
        queue = new ArrayBlockingQueue<>(options.queueCapacity());
        log = new WalLog(directory, this.shards, options.maxWalBytes(), ioHook, retention, replica);
        try {
            WalCheckpointState image = log.snapshot();
            if (image != null && !image.storedShards().equals(this.stores.keySet())) { throw new IOException("checkpoint storage routes changed"); }
            for (var entry : this.stores.entrySet()) {
                if (replica == null) { entry.getValue().initialize(log.engineId(), entry.getKey()); }
                else { entry.getValue().initializeReplica(replica, entry.getKey()); }
                WalCheckpoint checkpoint = entry.getValue().checkpoint();
                validateCheckpoint(checkpoint);
                checkpoints.put(entry.getKey(), checkpoint);
            }
            if (image != null) {
                published = image.sequence(); safePoint = image.safePoint();
                for (var version : image.versions()) { restoreVersion(version); hook.hit(Stage.DURING_RECOVERY); }
            }
            log.releaseRecoveryImage();
            for (long sequence = log.baseSequence() + 1; sequence <= log.lastSequence();) {
                List<WalCommit> batch = log.read(sequence, options.flushBatchSize());
                for (WalCommit commit : batch) {
                    install(commit, true);
                    published = commit.sequence();
                }
                sequence += batch.size();
            }
            if (retainedBytes > options.maxMemoryBytes()) {
                throw new IOException("recovered unmaterialized data exceeds memory budget");
            }
        } catch (Throwable failure) {
            log.close();
            throw failure;
        }
        writer = Thread.ofVirtual().name("azir-wal-commit").start(this::writeLoop);
        materializer = Thread.ofVirtual().name("azir-wal-materializer").start(this::flushLoop);
    }

    /** 在当前发布水位注册事务，旧进程句柄不能在新运行时继续使用。 */
    public WalTransaction begin(IsolationLevel isolation) {
        long epoch = readBarrier();
        synchronized (gate) {
            requireOpen();
            requireEpoch(epoch);
            if (backupRunning) { throw error(ErrorCode.RESOURCE_EXHAUSTED, "backup maintenance pauses new transactions", null, null); }
            expireIdle();
            if (active.size() >= options.maxActiveTransactions()) {
                throw error(ErrorCode.RESOURCE_EXHAUSTED, "active transaction limit", null, null);
            }
            var tx = new WalTransaction(owner, published,
                    monotonicClock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(options.transactionTimeoutMillis()),
                    Objects.requireNonNull(isolation));
            active.put(tx.id, tx);
            tx.epoch = epoch;
            return tx;
        }
    }

    /** 为整条语句取得共享的 RC 或 RR 快照。 */
    public WalReadSnapshot beginReadStatement(WalTransaction tx) {
        long epoch = readBarrier();
        synchronized (gate) {
            requireActive(tx);
            requireEpoch(epoch);
            return new WalReadSnapshot(tx, tx.isolation == IsolationLevel.REPEATABLE_READ ? tx.start : published);
        }
    }

    /** 写入本地缓冲，所有外部字节数组均被复制。 */
    public void put(WalTransaction tx, String shard, String key, byte[] value) {
        buffer(tx, new WalMutation(shard, key, Objects.requireNonNull(value, "value")));
    }

    /** 以本地墓碑表达删除，不立即改变任何已提交版本。 */
    public void delete(WalTransaction tx, String shard, String key) { buffer(tx, new WalMutation(shard, key, null)); }

    /** 在修改写集前检查单事务和全局内存预算。 */
    private void buffer(WalTransaction tx, WalMutation mutation) {
        synchronized (gate) {
            requireActive(tx);
            if (backupRunning) { throw error(ErrorCode.RESOURCE_EXHAUSTED, "backup maintenance pauses writes", tx, null); }
            if (readOnly) { throw error(ErrorCode.RESOURCE_EXHAUSTED, "runtime is read-only", tx, null); }
            requireShard(mutation.shard());
            if (stores.containsKey(mutation.shard())) {
                try { stores.get(mutation.shard()).validateMutation(mutation); }
                catch (WalMvccException rejected) { throw error(rejected.errorCode(), rejected.getMessage(), tx, rejected); }
            }
            Key key = new Key(mutation.shard(), mutation.key());
            WalMutation old = tx.writes.get(key);
            long delta = mutation.retainedBytes() - (old == null ? 0 : old.retainedBytes());
            trimHotCache(Math.max(0, delta));
            if (tx.writeBytes + delta > options.maxWriteBytes()
                    || tx.writes.size() + (old == null ? 1 : 0) > options.maxWriteKeys()
                    || retainedBytes + bufferedBytes + pendingIndexBytes + delta > options.maxMemoryBytes()) {
                throw error(ErrorCode.RESOURCE_EXHAUSTED, "write or memory budget exhausted", tx, null);
            }
            tx.writes.put(key, mutation);
            tx.writeBytes += delta;
            bufferedBytes += delta;
        }
    }

    /** 用新语句快照读取一个键。 */
    public Optional<byte[]> get(WalTransaction tx, String shard, String key) {
        return get(tx, shard, key, beginReadStatement(tx));
    }

    /** 在固定语句快照上合并本地、内存与物化版本；锁覆盖读与淘汰交错。 */
    public Optional<byte[]> get(WalTransaction tx, String shard, String key, WalReadSnapshot snapshot) {
        synchronized (gate) {
            validateSnapshot(tx, snapshot);
            requireShard(shard);
            new WalMutation(shard, key, null);
            Key identity = new Key(shard, key);
            WalMutation own = tx.writes.get(identity);
            if (own != null) { return own.deleted() ? Optional.empty() : Optional.of(own.value()); }
            WalValue visible = null;
            NavigableMap<Long, WalMutation> chain = versions.get(identity);
            var memory = chain == null ? null : chain.floorEntry(snapshot.sequence);
            if (memory != null) { visible = new WalValue(memory.getKey(), memory.getValue().value()); }
            // 已物化热点可淘汰，未物化增量不能丢；旧快照缺失的历史从后端读取。
            if (visible != null) { cacheHits++; hotCache.get(identity); }
            if (visible == null && stores.containsKey(shard)) {
                cacheMisses++;
                visible = stores.get(shard).get(key, snapshot.sequence).orElse(null);
                // 只提升当前发布切面的最新已物化版本，不能把旧 RR 版本当成冲突栅栏。
                if (visible != null && snapshot.sequence == published) { cacheMaterialized(identity, visible); }
            }
            validateSnapshot(tx, snapshot);
            return visible == null || visible.deleted() ? Optional.empty() : Optional.of(visible.value());
        }
    }

    /** 用新语句快照执行半开区间扫描。 */
    public NavigableMap<String, byte[]> scan(WalTransaction tx, String shard, String from, String to) {
        return scan(tx, shard, from, to, beginReadStatement(tx));
    }

    /** 合并相同快照的候选行后处理墓碑，结果受行数与字节预算限制。 */
    public NavigableMap<String, byte[]> scan(WalTransaction tx, String shard, String from, String to,
                                             WalReadSnapshot snapshot) {
        synchronized (gate) {
            validateSnapshot(tx, snapshot);
            requireShard(shard);
            if (from != null) { new WalMutation(shard, from, null); }
            if (to != null) { new WalMutation(shard, to, null); }
            var rows = new TreeMap<String, WalValue>(Utf8KeyOrder::compare);
            if (stores.containsKey(shard)) { rows.putAll(stores.get(shard).scan(from, to, snapshot.sequence)); }
            for (var entry : versions.entrySet()) {
                Key key = entry.getKey();
                if (!key.shard.equals(shard) || !inRange(key.key, from, to)) { continue; }
                var version = entry.getValue().floorEntry(snapshot.sequence);
                if (version != null) { putCandidate(rows, key.key, new WalValue(version.getKey(), version.getValue().value())); }
            }
            for (var entry : tx.writes.entrySet()) {
                if (entry.getKey().shard.equals(shard) && inRange(entry.getKey().key, from, to)) {
                    putCandidate(rows, entry.getKey().key, new WalValue(Long.MAX_VALUE, entry.getValue().value()));
                }
            }
            var result = new TreeMap<String, byte[]>(Utf8KeyOrder::compare);
            long bytes = 0;
            for (var entry : rows.entrySet()) {
                if (entry.getValue().deleted()) { continue; }
                byte[] value = entry.getValue().value();
                bytes += 128L + value.length + entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                if (result.size() >= 10_000 || bytes > 8L * 1024 * 1024) {
                    throw error(ErrorCode.RESOURCE_EXHAUSTED, "scan result budget exhausted", tx, null);
                }
                result.put(entry.getKey(), value);
            }
            validateSnapshot(tx, snapshot);
            return result;
        }
    }

    /** 在固定语句快照上分页；MVP 采用有预算的候选归并，尚不实现流式下推。 */
    public WalScanPage scanPage(WalTransaction tx, String shard, String from, String to,
                                String afterKey, int limit, WalReadSnapshot snapshot) {
        if (limit < 1 || limit > 4096) { throw new IllegalArgumentException("invalid page limit"); }
        String lower = from;
        if (afterKey != null && (lower == null || Utf8KeyOrder.compare(afterKey, lower) > 0)) { lower = afterKey; }
        var all = scan(tx, shard, lower, to, snapshot);
        var page = new TreeMap<String, byte[]>(Utf8KeyOrder::compare);
        for (var entry : all.entrySet()) {
            if (afterKey != null && Utf8KeyOrder.compare(entry.getKey(), afterKey) <= 0) { continue; }
            if (page.size() == limit) { return new WalScanPage(page, page.lastKey()); }
            page.put(entry.getKey(), entry.getValue());
        }
        return new WalScanPage(page, null);
    }

    /** 在候选归并过程中限制键数量，不能等分配完整候选集后才拒绝。 */
    private static void putCandidate(NavigableMap<String, WalValue> rows, String key, WalValue value) {
        if (!rows.containsKey(key) && rows.size() >= 10_000) {
            throw error(ErrorCode.RESOURCE_EXHAUSTED, "scan candidate budget exhausted", null, null);
        }
        rows.put(key, value);
    }

    /** 冻结写集并等待持久提交；超时和中断不取消已经排队的决议。 */
    public WalReceipt commit(WalTransaction tx) {
        synchronized (gate) {
            requireOpen();
            requireOwner(tx);
            if (tx.state == WalTransaction.State.ABORTED) {
                throw error(ErrorCode.TRANSACTION_ABORTED, "transaction aborted", tx, null);
            }
            if (tx.state == WalTransaction.State.ACTIVE) {
                requireActive(tx);
                if (backupRunning && !tx.writes.isEmpty()) { throw error(ErrorCode.RESOURCE_EXHAUSTED, "backup maintenance pauses commits", tx, null); }
                if (readOnly && !tx.writes.isEmpty()) { throw error(ErrorCode.RESOURCE_EXHAUSTED, "runtime is read-only", tx, null); }
                if (tx.writes.isEmpty()) {
                    tx.state = WalTransaction.State.COMMITTED;
                    active.remove(tx.id);
                    tx.completion.complete(new WalReceipt(tx.id, published, ""));
                } else {
                    tx.state = WalTransaction.State.QUEUED;
                    if (!queue.offer(tx)) {
                        abortLocal(tx);
                        throw error(ErrorCode.RESOURCE_EXHAUSTED, "commit queue full", tx, null);
                    }
                }
            }
        }
        try {
            if (tx.completion.isDone()) { return tx.completion.get(); }
            return tx.completion.get(Math.max(1, tx.deadlineNanos - monotonicClock.getAsLong()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit wait interrupted; query transaction id", tx, ex);
        } catch (TimeoutException ex) {
            throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit wait expired; query transaction id", tx, ex);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException runtime) { throw runtime; }
            throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit failed; query transaction id", tx, ex.getCause());
        }
    }

    /** 仅回滚尚未进入决议的事务，已提交和排队事务不能清空后伪装为中止。 */
    public void rollback(WalTransaction tx) {
        synchronized (gate) {
            requireOpen();
            requireOwner(tx);
            if (tx.state == WalTransaction.State.COMMITTED || tx.state == WalTransaction.State.ABORTED) { return; }
            if (tx.state != WalTransaction.State.ACTIVE) {
                throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "decision already started", tx, null);
            }
            abortLocal(tx);
        }
    }

    /** 只确认已发布的持久提交；未发布决议需等待或恢复，缺失不等于已回滚。 */
    public Optional<WalReceipt> findCommitted(UUID transactionId) {
        readBarrier();
        synchronized (gate) {
            try { return log.receipt(Objects.requireNonNull(transactionId)).filter(receipt -> receipt.commitSequence() <= published); }
            catch (IOException failure) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "commit receipt lookup failed", null, failure); }
        }
    }

    /** 明确区分缺少确认和查询历史已部分回收，任何一种都不能当作中止。 */
    public WalCommitLookup lookupCommit(UUID transactionId) {
        var receipt = findCommitted(transactionId);
        synchronized (gate) {
            long retired = log.retiredThrough();
            return new WalCommitLookup(receipt.isPresent() ? WalCommitLookup.Status.COMMITTED
                    : retired > 0 ? WalCommitLookup.Status.UNKNOWN_OR_EXPIRED : WalCommitLookup.Status.NOT_CONFIRMED,
                    receipt.orElse(null), retired);
        }
    }

    /** 单写者收集有限批次，正常关闭时完成已排队请求。 */
    private void writeLoop() {
        try {
            while (fatal == null && (accepting || !queue.isEmpty())) {
                WalTransaction first = queue.poll(20, TimeUnit.MILLISECONDS);
                if (first == null) { continue; }
                var batch = new ArrayList<WalTransaction>();
                batch.add(first);
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.batchDelayMillis());
                while (batch.size() < options.commitBatchSize()) {
                    WalTransaction next = queue.poll(Math.max(0, until - System.nanoTime()), TimeUnit.NANOSECONDS);
                    if (next == null) { break; }
                    batch.add(next);
                }
                commitBatch(batch);
            }
        } catch (Throwable failure) { failStop(failure); }
    }

    /** 串行校验批内冲突，先持久化，再安装所有版本并一次发布水位。 */
    private void commitBatch(List<WalTransaction> batch) throws IOException {
        if (authority != null) { commitReplicatedBatch(batch); return; }
        synchronized (decisionGate) {
            if (fatal != null) { return; }
            commitBatchLocked(batch);
        }
    }

    /** 单写者在当前状态上准备完整记录，复制模式尚未取得共识前不写本地提交日志。 */
    private Prepared prepareBatchLocked(List<WalTransaction> batch, long epoch) throws IOException {
        var accepted = new ArrayList<WalTransaction>();
        var records = new ArrayList<WalCommit>();
        var reservations = new HashMap<Key, Long>();
        long frameBytes = 0;
        long extraIndexBytes = 0;
        synchronized (gate) {
            for (WalTransaction tx : batch) {
                if (tx.state != WalTransaction.State.QUEUED || active.get(tx.id) != tx) { continue; }
                if (authority != null && (epoch <= 0 || tx.epoch != epoch || authority.epoch() != epoch || proposalBlocked)) {
                    abortLocal(tx); tx.completion.completeExceptionally(error(ErrorCode.STORAGE_UNAVAILABLE, "replica leadership unavailable", tx, null)); continue;
                }
                boolean expired = monotonicClock.getAsLong() - tx.deadlineNanos >= 0;
                trimHotCache(tx.writes.values().stream().mapToLong(WalMvccRuntime::indexBytes).sum());
                boolean conflict = false;
                WalMvccException lookupFailure = null;
                if (!expired) {
                    try {
                        for (Key key : tx.writes.keySet()) {
                            long fence = reservations.containsKey(key) ? reservations.get(key) : latestSequence(key);
                            if (fence > tx.start) { conflict = true; break; }
                        }
                    } catch (RuntimeException failure) {
                        // 未追加日志，无法核实冷键时明确拒绝本事务，不能默认无冲突。
                        lookupFailure = error(ErrorCode.STORAGE_UNAVAILABLE, "cannot verify cold-key write conflict", tx, failure);
                    }
                }
                expired = expired || monotonicClock.getAsLong() - tx.deadlineNanos >= 0;
                long newIndexBytes = tx.writes.entrySet().stream().filter(entry -> !latest.containsKey(entry.getKey())
                        && !reservations.containsKey(entry.getKey())).mapToLong(entry -> indexBytes(entry.getValue())).sum();
                WalMvccException rejection = expired
                        ? error(ErrorCode.TRANSACTION_TIMEOUT, "transaction expired before decision", tx, null)
                        : lookupFailure != null ? lookupFailure
                        : conflict ? error(ErrorCode.WRITE_CONFLICT, "write changed after transaction start", tx, null)
                        : !stores.isEmpty() && materializationLag() + records.size() >= options.maxMaterializationLag()
                        ? error(ErrorCode.RESOURCE_EXHAUSTED, "materialization backlog budget exhausted", tx, null)
                        : retainedBytes + bufferedBytes + extraIndexBytes + newIndexBytes > options.maxMemoryBytes()
                        ? error(ErrorCode.RESOURCE_EXHAUSTED, "version index budget exhausted", tx, null) : null;
                if (rejection != null) {
                    abortLocal(tx);
                    tx.completion.completeExceptionally(rejection);
                    continue;
                }
                long sequence = published + records.size() + 1;
                var record = new WalCommit(tx.id, sequence, tx.start, List.copyOf(tx.writes.values()));
                long encodedSize = WalCodec.encode(record).length + 16L;
                long batchLimit = authority == null ? WalCodec.MAX_FRAME : WalReplicationBatch.MAX_BYTES - 256L;
                if (frameBytes + encodedSize > batchLimit) {
                    abortLocal(tx);
                    tx.completion.completeExceptionally(error(ErrorCode.RESOURCE_EXHAUSTED,
                            "commit batch byte budget exhausted", tx, null));
                    continue;
                }
                records.add(record);
                accepted.add(tx);
                frameBytes += encodedSize;
                extraIndexBytes += newIndexBytes;
                tx.writes.keySet().forEach(key -> reservations.put(key, sequence));
                decisionKeys.addAll(tx.writes.keySet());
            }
            pendingIndexBytes = extraIndexBytes;
            if (authority != null && !records.isEmpty()) { pendingReplicaEpoch = epoch; }
        }
        return new Prepared(List.copyOf(accepted), List.copyOf(records), epoch);
    }

    /** 一次被冻结的候选批次，不能跨任期重新分配提交序号。 */
    private record Prepared(List<WalTransaction> transactions, List<WalCommit> records, long epoch) { }

    /** 本地模式继续持锁完成刷盘与发布，保持原有提交合同。 */
    private void commitBatchLocked(List<WalTransaction> batch) throws IOException {
        var prepared = prepareBatchLocked(batch, 0); var accepted = prepared.transactions(); var records = prepared.records();
        if (records.isEmpty()) { return; }
        hook.hit(Stage.BEFORE_APPEND);
        try { log.append(records); }
        catch (WalMvccException resource) {
            synchronized (gate) {
                pendingIndexBytes = 0;
                decisionKeys.clear(); evictMaterialized();
                for (WalTransaction tx : accepted) {
                    abortLocal(tx);
                    tx.completion.completeExceptionally(error(resource.errorCode(), resource.getMessage(), tx, resource));
                }
            }
            return;
        }
        hook.hit(Stage.AFTER_FORCE);
        hook.hit(Stage.BEFORE_PUBLISH);
        synchronized (gate) {
            pendingIndexBytes = 0;
            for (WalCommit record : records) { install(record, false); }
            published = records.getLast().sequence();
            for (WalTransaction tx : accepted) {
                releaseWrites(tx);
                active.remove(tx.id);
                tx.state = WalTransaction.State.COMMITTED;
            }
            decisionKeys.clear(); evictMaterialized();
        }
        try { hook.hit(Stage.AFTER_PUBLISH); }
        catch (Throwable failure) {
            for (WalTransaction tx : accepted) {
                tx.completion.completeExceptionally(error(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "commit persisted but response failed", tx, failure));
            }
            throw failure;
        }
        for (WalTransaction tx : accepted) { tx.completion.complete(log.receipt(tx.id).orElseThrow()); }
    }

    /** 准备与共识等待分离，状态机可在另一个线程持决议锁安装，不能形成回调死锁。 */
    private void commitReplicatedBatch(List<WalTransaction> batch) throws IOException {
        Prepared prepared;
        synchronized (decisionGate) {
            if (fatal != null) { return; }
            long epoch = authority.epoch(); prepared = prepareBatchLocked(batch, epoch);
        }
        if (prepared.records().isEmpty()) { return; }
        try {
            authority.submit(prepared.epoch(), prepared.records()).toCompletableFuture().get(options.transactionTimeoutMillis(), TimeUnit.MILLISECONDS);
            synchronized (gate) {
                for (var tx : prepared.transactions()) {
                    if (tx.state != WalTransaction.State.COMMITTED) { throw new IOException("authority returned before local committed application"); }
                }
            }
        } catch (Exception failure) {
            synchronized (gate) {
                if (pendingReplicaEpoch == prepared.epoch() && authority.epoch() == prepared.epoch()) { proposalBlocked = true; }
                for (var tx : prepared.transactions()) {
                    tx.completion.completeExceptionally(error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "replicated commit unresolved; query UUID after recovery", tx, failure));
                }
            }
        }
    }

    /** 只有装配时的可信共识端口可安装已提交记录，所有副本使用同一 UUID 和业务序号。 */
    public void applyCommitted(WalCommitAuthority source, List<WalCommit> records) {
        if (authority == null || source != authority) { throw new IllegalArgumentException("committed application requires replica authority"); }
        records = List.copyOf(records); if (records.isEmpty() || records.size() > 1024) { throw new IllegalArgumentException("invalid committed batch"); }
        synchronized (decisionGate) {
            try {
                var fresh = new ArrayList<WalCommit>(); var completed = new ArrayList<WalTransaction>();
                synchronized (gate) {
                    requireHealthy();
                    long servingEpoch = authority.epoch();
                    for (long oldEpoch : active.values().stream().mapToLong(tx -> tx.epoch).filter(epoch -> epoch != servingEpoch).distinct().toArray()) { fenceEpochLocked(oldEpoch); }
                    if (pendingReplicaEpoch != 0 && authority.epoch() != pendingReplicaEpoch) { fenceEpochLocked(pendingReplicaEpoch); }
                    long expected = published + 1; long bytes = 0; var newKeys = new java.util.HashSet<Key>(); long localBytes = 0;
                    for (var record : records) {
                        if (record.sequence() <= published) {
                            var old = log.receipt(record.transactionId()).orElseThrow(() -> new IOException("replica replay has no retained decision"));
                            if (old.commitSequence() != record.sequence() || !old.digest().equals(record.digest())) { throw new IOException("replica replay decision mismatch"); }
                            continue;
                        }
                        if (record.sequence() != expected++) { throw new IOException("replica committed sequence gap"); }
                        var local = active.get(record.transactionId());
                        if (local != null) {
                            var proposal = new WalCommit(local.id, record.sequence(), local.start, List.copyOf(local.writes.values()));
                            if (local.state != WalTransaction.State.QUEUED || !proposal.digest().equals(record.digest())) { throw new IOException("committed proposal changed"); }
                            completed.add(local); localBytes += local.writeBytes;
                        }
                        for (var mutation : record.mutations()) {
                            requireShard(mutation.shard()); var key = new Key(mutation.shard(), mutation.key()); bytes += mutation.retainedBytes();
                            if (!latest.containsKey(key) && newKeys.add(key)) { bytes += indexBytes(mutation); }
                        }
                        fresh.add(record);
                    }
                    trimHotCache(bytes);
                    if (retainedBytes + bufferedBytes - localBytes + bytes > options.maxMemoryBytes()) { throw new IOException("committed replica exceeds memory budget"); }
                }
                if (fresh.isEmpty()) { return; }
                log.append(fresh);
                synchronized (gate) {
                    for (var record : fresh) { install(record, false); }
                    published = fresh.getLast().sequence();
                    for (var tx : completed) { releaseWrites(tx); active.remove(tx.id); tx.state = WalTransaction.State.COMMITTED; }
                    if (!completed.isEmpty()) { pendingReplicaEpoch = 0; pendingIndexBytes = 0; decisionKeys.clear(); proposalBlocked = false; }
                    evictMaterialized();
                }
                for (var tx : completed) { tx.completion.complete(log.receipt(tx.id).orElseThrow()); }
            } catch (Throwable failure) {
                failStop(failure); throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica committed application failed", null, failure);
            }
        }
    }

    /** 角色变化只清理旧任期本地句柄；已进入复制的结果仍是未知，不伪装为回滚。 */
    public void fenceEpoch(long obsoleteEpoch) {
        if (authority == null || obsoleteEpoch < 1) { throw new IllegalArgumentException("invalid replica fencing epoch"); }
        synchronized (gate) { fenceEpochLocked(obsoleteEpoch); }
    }

    /** 持锁退休旧句柄和预算，晚到的角色通知不能清除新任期事务。 */
    private void fenceEpochLocked(long epoch) {
        for (var tx : List.copyOf(active.values())) {
            if (tx.epoch > epoch) { continue; }
            boolean queued = tx.state == WalTransaction.State.QUEUED; releaseWrites(tx); active.remove(tx.id);
            tx.state = queued ? WalTransaction.State.UNKNOWN : WalTransaction.State.ABORTED;
            tx.completion.completeExceptionally(error(queued ? ErrorCode.TRANSACTION_STATUS_UNKNOWN : ErrorCode.TRANSACTION_ABORTED, "replica leadership changed", tx, null));
        }
        queue.removeIf(tx -> tx.epoch <= epoch);
        if (pendingReplicaEpoch <= epoch) { pendingReplicaEpoch = 0; pendingIndexBytes = 0; decisionKeys.clear(); proposalBlocked = false; }
    }

    /** 多数派读屏障在运行时锁外执行，失败不能降级为本地缓存读取。 */
    private long readBarrier() {
        if (authority == null) { return 0; }
        try { long epoch = authority.readBarrier(options.transactionTimeoutMillis()); requireEpoch(epoch); return epoch; }
        catch (Exception failure) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica read barrier unavailable", null, failure); }
    }

    /** 任期检查不做网络 I/O，保持句柄和提交准备阶段的本地互斥关系。 */
    private void requireEpoch(long epoch) {
        if (authority != null && (epoch <= 0 || authority.epoch() != epoch || proposalBlocked)) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica epoch is not serving", null, null);
        }
    }

    /** 安装未物化历史和写冲突栅栏，恢复时不再把已物化的全库数据加载进内存。 */
    private void install(WalCommit commit, boolean recovery) throws IOException {
        for (WalMutation mutation : commit.mutations()) {
            if (!shards.contains(mutation.shard())) { throw new IOException("unknown shard in WAL"); }
            long flushed = checkpoints.getOrDefault(mutation.shard(), new WalCheckpoint(0, "")).sequence();
            if (recovery && stores.containsKey(mutation.shard()) && commit.sequence() <= flushed) {
                hook.hit(Stage.DURING_RECOVERY); continue;
            }
            Key key = new Key(mutation.shard(), mutation.key());
            forgetHot(key);
            if (!latest.containsKey(key)) { retainedBytes += indexBytes(mutation); memoryGcKeys.add(key); }
            latest.put(key, commit.sequence());
            var chain = versions.computeIfAbsent(key, ignored -> new TreeMap<>());
            chain.put(commit.sequence(), mutation);
            retainedBytes += mutation.retainedBytes();
            // 已刷库的历史可以淘汰，但保留最新头版本作为读缓存，避免每笔点读都回到 MySQL。
            var retired = chain.headMap(Math.min(flushed, commit.sequence() - 1), true);
            for (WalMutation old : retired.values()) { retainedBytes -= old.retainedBytes(); }
            retired.clear();
            hook.hit(recovery ? Stage.DURING_RECOVERY : Stage.DURING_INSTALL);
            if (recovery && retainedBytes > options.maxMemoryBytes()) {
                throw new IOException("recovery exceeds memory budget; restore with a larger budget");
            }
        }
    }

    /** 周期物化有限日志批次，失败保留日志与内存版本并重试。 */
    private void flushLoop() {
        while (flushRunning && fatal == null) {
            try {
                flush();
                long now = System.nanoTime();
                if (now - nextCheckpointAttemptNanos >= 0 && (now - lastCheckpointNanos >= TimeUnit.MILLISECONDS.toNanos(retention.checkpointIntervalMillis())
                        || log.bytes() >= retention.checkpointWalBytes())) {
                    // 后端落后或归档预算不足时限速重试，不能把资源背压变成忙循环。
                    nextCheckpointAttemptNanos = now + TimeUnit.SECONDS.toNanos(1);
                    try { checkpoint(); maintenanceFailure = null; }
                    catch (RuntimeException failure) { maintenanceFailure = failure.toString(); }
                }
                pauseMaterializer();
            } catch (InterruptedException stop) { Thread.currentThread().interrupt(); return; }
            catch (IOException failure) { failStop(failure); return; }
            catch (RuntimeException failure) { materializationFailure = failure.toString();
                try { pauseMaterializer(); }
                catch (InterruptedException stop) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    /** 明确报告检查点是否创建，后端落后时不能通过强推水位删除日志。 */
    public record CheckpointResult(boolean created, long sequence, long walBytesBefore, long walBytesAfter) { }

    /** 暂停写入决议，先追平所有后端，再把本地恢复状态与同一边界的冷数据一起备份。 */
    public WalBackup.Info backup(Path destination, WalBackupOptions limits) {
        if (replica != null) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica backup requires coordinated Raft snapshot", null, null); }
        closeGuard.lock();
        try { return backupLocked(destination, limits); }
        finally { closeGuard.unlock(); }
    }

    /** 可信共识装配在实际应用队列中导出完整副本，Raft 位置由装配另行绑定。 */
    public WalBackup.Info backupReplica(WalCommitAuthority source, Path destination, WalBackupOptions limits) {
        if (replica == null || source != authority) { throw new IllegalArgumentException("replica snapshot requires bound consensus authority"); }
        closeGuard.lock();
        try { return backupLocked(destination, limits); }
        finally { closeGuard.unlock(); }
    }

    /** 共识装配在应用边界调用准备，后端视图固定后允许发布和物化继续推进。 */
    public WalReplicaBackup prepareReplicaBackup(WalCommitAuthority source, Path destination, WalBackupOptions limits) {
        if (replica == null || source != authority) { throw new IllegalArgumentException("replica snapshot requires bound consensus authority"); }
        closeGuard.lock();
        try {
            synchronized (decisionGate) { synchronized (flushGate) {
                synchronized (gate) {
                    requireOpen(); if (backupRunning || replicaExportRunning || stores.values().stream().anyMatch(store -> !store.supportsPinnedSnapshot())) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "pinned replica backup unavailable", null, null); }
                    replicaExportRunning = true;
                }
                try {
                    var budget = new WalBackup.Budget(limits); WalBackup.checkDestination(destination, log.directory());
                    while (true) { budget.check(); flush(); synchronized (gate) { requireOpen(); if (checkpoints.values().stream().allMatch(checkpoint -> checkpoint.sequence() == published)) { break; } } }
                    if (published > 0 && !checkpoint().created()) { throw new IOException("replica backup checkpoint did not reach prefix"); }
                    var prepared = WalBackup.prepareReplica(destination, log, shards, stores, budget, hook::hit);
                    return new WalReplicaBackup(prepared, this::releaseReplicaExport);
                } catch (Throwable failure) {
                    releaseReplicaExport(); if (failure instanceof Error error) { throw error; } throw error(ErrorCode.STORAGE_UNAVAILABLE, "prepare pinned replica backup failed", null, failure);
                }
            } }
        } finally { closeGuard.unlock(); }
    }

    /** 完成或放弃导出以后才归还维护资格，并唤醒正在安全关闭的运行时。 */
    private void releaseReplicaExport() { synchronized (gate) { replicaExportRunning = false; gate.notifyAll(); } }

    /** 与关闭串行维护，但不在运行时读锁中执行整个备份，已有快照仍可读取。 */
    private WalBackup.Info backupLocked(Path destination, WalBackupOptions limits) {
        var budget = new WalBackup.Budget(limits);
        synchronized (decisionGate) {
            synchronized (flushGate) {
                synchronized (gate) {
                    requireOpen();
                    if (backupRunning || replicaExportRunning || stores.values().stream().anyMatch(store -> !store.supportsBackup())) {
                        throw error(ErrorCode.STORAGE_UNAVAILABLE, "backup already running or backend unsupported", null, null);
                    }
                    backupRunning = true;
                }
                try {
                    budget.check(); WalBackup.checkDestination(destination, log.directory());
                    while (true) {
                        budget.check(); flush();
                        synchronized (gate) {
                            requireOpen(); if (checkpoints.values().stream().allMatch(checkpoint -> checkpoint.sequence() == published)) { break; }
                        }
                    }
                    if (published > 0 && !checkpoint().created()) { throw new IOException("backup checkpoint did not reach published prefix"); }
                    return WalBackup.create(destination, log, shards, stores, budget, hook::hit);
                } catch (IOException failure) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "consistent backup failed", null, failure); }
                finally { synchronized (gate) { backupRunning = false; gate.notifyAll(); } }
            }
        }
    }

    /** 暂停提交决议并做最后一轮有界物化，完整状态持久化后才原子切换并删除旧日志。 */
    public CheckpointResult checkpoint() {
        synchronized (decisionGate) {
            synchronized (flushGate) {
                synchronized (gate) { requireOpen(); }
                flush();
                long cut; long safe; long before; List<WalCheckpointState.Version> saved = new ArrayList<>();
                synchronized (gate) {
                    requireOpen(); cut = published; safe = safePoint;
                    try { before = log.bytes(); } catch (IOException failure) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "inspect checkpoint WAL", null, failure); }
                    if (cut == 0 || checkpoints.values().stream().anyMatch(cp -> cp.sequence() != cut)) {
                        return new CheckpointResult(false, cut, before, before);
                    }
                    versions.forEach((key, chain) -> {
                        // 后端已追平 cut，由身份和摘要绑定的后端承担冷数据恢复，热点缓存不是权威状态。
                        if (!stores.containsKey(key.shard)) { chain.forEach((sequence, mutation) -> saved.add(new WalCheckpointState.Version(sequence, mutation))); }
                    });
                }
                try {
                    // 镜像创建期间不持读锁，提交决议仍被暂停；已有读者和本地写集继续受原规则保护。
                    log.checkpoint(shards, stores.keySet(), safe, saved, Math.max(0, wallClock.getAsLong()), hook::hit);
                    lastCheckpointNanos = System.nanoTime(); maintenanceFailure = null;
                    return new CheckpointResult(true, cut, before, log.bytes());
                } catch (WalMvccException resource) {
                    if (resource.errorCode() == ErrorCode.RESOURCE_EXHAUSTED) { throw resource; }
                    failStop(resource); throw resource;
                } catch (Throwable failure) {
                    failStop(failure); throw error(ErrorCode.STORAGE_UNAVAILABLE, "checkpoint outcome requires recovery", null, failure);
                }
            }
        }
    }

    /** 将已验证镜像版本装回内存，重复键版本不能覆盖不同序号。 */
    private void restoreVersion(WalCheckpointState.Version version) throws IOException {
        var mutation = version.mutation(); Key key = new Key(mutation.shard(), mutation.key());
        // 兼容旧镜像的全量头缓存：后端已覆盖恢复基线，不再恢复这些可丢弃缓存。
        if (stores.containsKey(key.shard)) { return; }
        if (!latest.containsKey(key)) { retainedBytes += indexBytes(mutation); memoryGcKeys.add(key); }
        latest.merge(key, version.sequence(), Math::max);
        if (versions.computeIfAbsent(key, ignored -> new TreeMap<>()).put(version.sequence(), mutation) != null) {
            throw new IOException("duplicate checkpoint version");
        }
        retainedBytes += mutation.retainedBytes();
        if (retainedBytes > options.maxMemoryBytes()) { throw new IOException("checkpoint exceeds memory budget"); }
    }

    /** 用可唤醒等待停止空闲线程，不通过 interrupt 破坏在途 JDBC 或文件 I/O。 */
    private void pauseMaterializer() throws InterruptedException {
        synchronized (gate) {
            if (flushRunning && fatal == null) { gate.wait(options.flushIntervalMillis()); }
        }
    }

    /** 主动执行一轮有限物化，便于调用方控制积压或进行确定性验收。 */
    public void flush() {
        synchronized (flushGate) {
            long upper;
            synchronized (gate) { requireHealthy(); upper = published; }
            RuntimeException firstFailure = null;
            for (var entry : stores.entrySet()) {
                try {
                    synchronized (gate) {
                        if (checkpoints.get(entry.getKey()).sequence() >= upper) { continue; }
                    }
                    WalCheckpoint before = entry.getValue().checkpoint();
                    validateCheckpoint(before);
                    if (before.sequence() > upper) { throw new IOException("backend ahead of published prefix"); }
                    int count = (int) Math.min(options.flushBatchSize(), upper - before.sequence());
                    if (count > 0) {
                        WalCheckpoint after = entry.getValue().apply(log.read(before.sequence() + 1, count));
                        validateCheckpoint(after);
                        if (after.sequence() < before.sequence() + count || after.sequence() > upper) {
                            throw new IOException("invalid materialization progress");
                        }
                        before = after;
                    }
                    synchronized (gate) { checkpoints.put(entry.getKey(), before); evictMaterialized(); }
                } catch (Exception failure) {
                    if (firstFailure == null) {
                        firstFailure = error(ErrorCode.STORAGE_UNAVAILABLE, "materialization failed", null, failure);
                    }
                }
            }
            materializationFailure = firstFailure == null ? null : firstFailure.toString();
            if (firstFailure != null) { throw firstFailure; }
        }
    }

    /** 退休已物化历史，将完全落盘的键纳入有界热点缓存，旧快照按需回源。 */
    private void evictMaterialized() {
        var keys = versions.entrySet().iterator();
        while (keys.hasNext()) {
            var entry = keys.next();
            long frontier = checkpoints.getOrDefault(entry.getKey().shard, new WalCheckpoint(0, "")).sequence();
            var chain = entry.getValue().entrySet().iterator();
            while (chain.hasNext()) {
                var version = chain.next();
                if (version.getKey() > frontier || version.getKey().equals(latest.get(entry.getKey()))) { break; }
                retainedBytes -= version.getValue().retainedBytes();
                chain.remove();
            }
            long head = entry.getValue().lastKey();
            if (stores.containsKey(entry.getKey().shard) && head <= frontier && !decisionKeys.contains(entry.getKey())
                    && !hotCache.containsKey(entry.getKey())) {
                long bytes = indexBytes(entry.getValue().lastEntry().getValue()) + entry.getValue().lastEntry().getValue().retainedBytes();
                hotCache.put(entry.getKey(), bytes); hotCacheBytes += bytes;
            }
        }
        trimHotCache(0);
    }

    /** 缓存缺失时从持久索引补查栅栏，未物化键始终有内存栅栏覆盖。 */
    private long latestSequence(Key key) {
        Long memory = latest.get(key); if (memory != null) { return memory; }
        var store = stores.get(key.shard); if (store == null) { return 0; }
        conflictStoreLookups++;
        long sequence = store.latestSequence(key.key);
        if (sequence < 0 || sequence > published) { throw new IllegalStateException("backend conflict fence outside published prefix"); }
        return sequence;
    }

    /** 将当前切面的冷读结果提升为可淘汰热点，不能占用在途决议的预留内存。 */
    private void cacheMaterialized(Key key, WalValue value) {
        if (versions.containsKey(key) || decisionKeys.contains(key)
                || value.commitSequence() > checkpoints.get(key.shard).sequence()) { return; }
        var mutation = new WalMutation(key.shard, key.key, value.value());
        long bytes = indexBytes(mutation) + mutation.retainedBytes();
        if (bytes > options.maxHotCacheBytes()) { return; }
        trimHotCache(bytes);
        if (retainedBytes + bufferedBytes + pendingIndexBytes + bytes > options.maxMemoryBytes()) { return; }
        var chain = new TreeMap<Long, WalMutation>(); chain.put(value.commitSequence(), mutation);
        versions.put(key, chain); latest.put(key, value.commitSequence()); memoryGcKeys.add(key);
        retainedBytes += bytes; hotCache.put(key, bytes); hotCacheBytes += bytes; trimHotCache(0);
    }

    /** 移除缓存分类但保留版本，供冷热点转换及版本安装使用。 */
    private void forgetHot(Key key) {
        Long bytes = hotCache.remove(key); if (bytes != null) { hotCacheBytes -= bytes; }
    }

    /** 只淘汰完全物化的热点键；在途提交使用的键不得消失，否则索引预留会失真。 */
    private void trimHotCache(long neededBytes) {
        var entries = hotCache.entrySet().iterator();
        while (entries.hasNext() && (hotCacheBytes > options.maxHotCacheBytes()
                || retainedBytes + bufferedBytes + pendingIndexBytes + neededBytes > options.maxMemoryBytes())) {
            var entry = entries.next(); Key key = entry.getKey();
            if (decisionKeys.contains(key)) { continue; }
            hotCacheBytes -= entry.getValue(); retainedBytes -= entry.getValue(); entries.remove();
            versions.remove(key); latest.remove(key); memoryGcKeys.remove(key); cacheEvictions++;
        }
    }

    /** 最慢物化分片决定待处理提交数，纯本地模式没有后端积压预算。 */
    private long materializationLag() {
        return published - checkpoints.values().stream().mapToLong(WalCheckpoint::sequence).min().orElse(published);
    }

    /** 受活跃事务保护地推进安全点，后端每键保留一个历史锚点。 */
    public int collectGarbage(int keyLimit) {
        if (keyLimit < 1 || keyLimit > 4096) { throw new IllegalArgumentException("invalid GC batch"); }
        synchronized (gate) {
            requireOpen();
            expireIdle();
            long oldest = active.values().stream().mapToLong(tx -> tx.start).min().orElse(published);
            safePoint = Math.max(safePoint, oldest);
            int removed = 0;
            for (WalMaterializedStore store : stores.values()) { removed += store.gc(safePoint, keyLimit); }
            vacuumMemory(keyLimit);
            return removed;
        }
    }

    /** 托管回收按分片轮转，限制每片工作量并避免慢分片长期饿死另一片。 */
    public WalMaterializedStore.GcProgress collectGarbageBounded(int keyLimit, int rowLimit, long budgetMillis) {
        if (keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096 || budgetMillis < 1 || budgetMillis > 5000) {
            throw new IllegalArgumentException("invalid runtime maintenance limits");
        }
        synchronized (gate) {
            requireOpen(); expireIdle();
            long oldest = active.values().stream().mapToLong(tx -> tx.start).min().orElse(published); safePoint = Math.max(safePoint, oldest);
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis); int keys = 0; int rows = 0; boolean wrapped = false;
            var routes = stores.keySet().stream().sorted().toList();
            for (int visited = 0; visited < routes.size() && keys < keyLimit && rows < rowLimit; visited++) {
                long remaining = deadline - System.nanoTime(); if (remaining <= 0) { break; }
                String route = routes.get(gcStoreCursor % routes.size()); gcStoreCursor = (gcStoreCursor + 1) % routes.size();
                var result = stores.get(route).gcBounded(safePoint, keyLimit - keys, rowLimit - rows,
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                if (result.inspectedKeys() > keyLimit - keys || result.removedRows() > rowLimit - rows) { throw new IllegalStateException("backend exceeded maintenance budget"); }
                keys += result.inspectedKeys(); rows += result.removedRows(); wrapped |= result.wrapped();
            }
            vacuumMemory(keyLimit, rowLimit); return new WalMaterializedStore.GcProgress(keys, rows, wrapped);
        }
    }

    /** 轮转扫描有限内存键，保留安全点锚点；墓碑必须已物化才能淘汰其冲突索引。 */
    private void vacuumMemory(int keyLimit) {
        vacuumMemory(keyLimit, 4096);
    }

    /** 内存版本也按删除条数限额处理；删除墓碑前必须先移除所有较旧值。 */
    private void vacuumMemory(int keyLimit, int versionLimit) {
        int count = Math.min(keyLimit, memoryGcKeys.size());
        int removed = 0;
        for (int i = 0; i < count; i++) {
            var iterator = memoryGcKeys.iterator(); Key key = iterator.next(); iterator.remove();
            if (decisionKeys.contains(key)) { memoryGcKeys.add(key); continue; }
            var chain = versions.get(key); if (chain == null) { continue; }
            var anchor = chain.floorKey(safePoint);
            var head = chain.lastEntry();
            boolean retiring = head.getValue().deleted() && head.getKey() <= safePoint
                    && (!stores.containsKey(key.shard) || checkpoints.get(key.shard).sequence() >= head.getKey());
            if (anchor != null) {
                var obsolete = chain.headMap(anchor, retiring).values().iterator();
                while (obsolete.hasNext() && removed < versionLimit) {
                    retainedBytes -= obsolete.next().retainedBytes(); obsolete.remove(); retiredMemoryVersions++; removed++;
                }
            }
            if (chain.isEmpty()) {
                retainedBytes -= indexBytes(head.getValue()); versions.remove(key); latest.remove(key);
                forgetHot(key);
            } else { memoryGcKeys.add(key); }
            if (removed >= versionLimit) { break; }
        }
    }

    /** 返回独立模式的可观测状态。 */
    public Stats stats() {
        synchronized (gate) {
            var frontiers = new LinkedHashMap<String, Long>();
            checkpoints.forEach((shard, checkpoint) -> frontiers.put(shard, checkpoint.sequence()));
            try {
                return new Stats(published, safePoint, retainedBytes, bufferedBytes, log.bytes(), log.forces(),
                        active.size(), queue.size(), Map.copyOf(frontiers), materializationFailure, log.baseSequence(), log.pendingReceipts(), log.receiptArchives(),
                        log.retiredThrough(), maintenanceFailure, retiredMemoryVersions, hotCacheBytes, hotCache.size(),
                        cacheHits, cacheMisses, cacheEvictions, conflictStoreLookups, materializationLag(), backupRunning || replicaExportRunning,
                        log.archiveBytes(), active.values().stream().mapToLong(tx -> Math.max(0, TimeUnit.NANOSECONDS.toMillis(
                                monotonicClock.getAsLong() - (tx.deadlineNanos - TimeUnit.MILLISECONDS.toNanos(options.transactionTimeoutMillis()))))).max().orElse(0));
            } catch (IOException ex) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "inspect WAL failed", null, ex); }
        }
    }

    /** 无阻塞的存活状态，监控不能为了查询健康再等待一个慢数据库调用。 */
    public record Lifecycle(boolean accepting, boolean closed, boolean backup, boolean readOnly, boolean proposalBlocked, String fatal,
                            String materializationFailure, String checkpointFailure) { }

    /** 只读取 volatile 状态，不获取可能持有 JDBC I/O 的运行时锁。 */
    public Lifecycle lifecycle() {
        Throwable failure = fatal;
        return new Lifecycle(accepting, closed, backupRunning || replicaExportRunning, readOnly, proposalBlocked, failure == null ? null : failure.getClass().getSimpleName(), materializationFailure, maintenanceFailure);
    }

    /** 进程内维护开关，只阻止新写入；已排队决议继续完成，不替代跨节点 fencing。 */
    public void setReadOnly(boolean value) { requireOpen(); readOnly = value; }

    /** 托管关闭先停止接收新事务，已排队决议继续按原提交合同排空。 */
    void stopAccepting() { accepting = false; }

    /** 核对物化进度确实属于当前完整日志，而非另一份或落后的 WAL。 */
    private void validateCheckpoint(WalCheckpoint checkpoint) throws IOException {
        if (checkpoint.sequence() < log.baseSequence() || checkpoint.sequence() > log.lastSequence()
                || checkpoint.sequence() > 0 && !log.receipt(checkpoint.sequence()).digest().equals(checkpoint.digest())) {
            throw new IOException("backend checkpoint does not match WAL");
        }
    }

    /** 清理超期但尚未开始决议的事务；排队事务由提交线程确定最终结果。 */
    private void expireIdle() {
        for (WalTransaction tx : List.copyOf(active.values())) {
            if (tx.state == WalTransaction.State.ACTIVE && monotonicClock.getAsLong() - tx.deadlineNanos >= 0) { abortLocal(tx); }
        }
    }

    /** 丢弃尚未提交的本地写集并固定中止终态。 */
    private void abortLocal(WalTransaction tx) {
        releaseWrites(tx);
        active.remove(tx.id);
        tx.state = WalTransaction.State.ABORTED;
    }

    /** 统一释放事务写集的资源记账。 */
    private void releaseWrites(WalTransaction tx) {
        bufferedBytes -= tx.writeBytes;
        tx.writeBytes = 0;
        tx.writes.clear();
    }

    /** 任意日志或发布失败都停止接收请求，未确认事务保持结果未知。 */
    private void failStop(Throwable failure) {
        synchronized (gate) {
            fatal = failure;
            accepting = false;
            for (WalTransaction tx : List.copyOf(active.values())) {
                boolean uncertain = tx.state == WalTransaction.State.QUEUED;
                releaseWrites(tx);
                tx.state = uncertain ? WalTransaction.State.UNKNOWN : WalTransaction.State.ABORTED;
                tx.completion.completeExceptionally(error(uncertain ? ErrorCode.TRANSACTION_STATUS_UNKNOWN
                        : ErrorCode.STORAGE_UNAVAILABLE, "runtime stopped; recover WAL before continuing", tx, failure));
            }
            active.clear();
            queue.clear();
            pendingIndexBytes = 0;
            decisionKeys.clear();
            gate.notifyAll();
        }
    }

    /** 验证归属、状态与期限，存储操作后再次调用可防止过期结果返回。 */
    private void requireActive(WalTransaction tx) {
        requireOpen();
        requireOwner(tx);
        requireEpoch(tx.epoch);
        if (tx.state != WalTransaction.State.ACTIVE || active.get(tx.id) != tx) {
            throw error(ErrorCode.TRANSACTION_ABORTED, "transaction is not active", tx, null);
        }
        if (monotonicClock.getAsLong() - tx.deadlineNanos >= 0) {
            abortLocal(tx);
            throw error(ErrorCode.TRANSACTION_TIMEOUT, "transaction expired", tx, null);
        }
    }

    /** 拒绝跨运行时或跨重启复用事务句柄。 */
    private void requireOwner(WalTransaction tx) {
        if (tx == null || tx.owner != owner) { throw error(ErrorCode.ILLEGAL_STATE, "foreign transaction handle", tx, null); }
    }

    /** 拒绝不属于当前事务的语句快照及已退休快照。 */
    private void validateSnapshot(WalTransaction tx, WalReadSnapshot snapshot) {
        requireActive(tx);
        if (snapshot == null || snapshot.transaction != tx) {
            throw error(ErrorCode.ILLEGAL_STATE, "foreign statement snapshot", tx, null);
        }
        if (snapshot.sequence < safePoint) { throw error(ErrorCode.SNAPSHOT_TOO_OLD, "snapshot retired", tx, null); }
    }

    /** 拒绝未知分片。 */
    private void requireShard(String shard) {
        if (!shards.contains(shard)) { throw new IllegalArgumentException("unknown shard: " + shard); }
    }

    /** 拒绝停止接收请求的运行时。 */
    private void requireOpen() {
        requireHealthy();
        if (!accepting) { throw error(ErrorCode.ILLEGAL_STATE, "runtime closing", null, null); }
    }

    /** 日志失败后禁止继续读取或提交，防止半安装状态泄露。 */
    private void requireHealthy() {
        if (closed || fatal != null) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "runtime unavailable", null, fatal); }
    }

    /** 用与数据库相同的 UTF-8 顺序判断半开区间。 */
    private static boolean inRange(String key, String from, String to) {
        return (from == null || Utf8KeyOrder.compare(key, from) >= 0)
                && (to == null || Utf8KeyOrder.compare(key, to) < 0);
    }

    /** 对仍保留的写冲突索引计费，覆盖键字符串及索引对象固定开销。 */
    private static long indexBytes(WalMutation mutation) {
        return 128L + 2L * (mutation.shard().length() + mutation.key().length());
    }

    /** 统一构造带稳定事务号的模式专属异常。 */
    private static WalMvccException error(ErrorCode code, String message, WalTransaction tx, Throwable cause) {
        return new WalMvccException(code, message, tx == null ? null : tx.id, cause);
    }

    /** 停止接收、排空提交线程、停止物化，再释放 WAL 独占锁。 */
    @Override public void close() {
        // Java 21 的 synchronized 中 join 会固定虚拟线程载体；AQS 锁仍串行关闭但允许等待时卸载。
        closeGuard.lock();
        try {
            if (closed) { return; }
            synchronized (gate) {
                accepting = false; long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (replicaExportRunning && System.nanoTime() < until) { try { TimeUnit.NANOSECONDS.timedWait(gate, Math.max(1, until - System.nanoTime())); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("snapshot export close interrupted; directory remains locked", interrupted); } }
                if (replicaExportRunning) { throw new IllegalStateException("snapshot export did not drain; directory remains locked"); }
            }
            join(writer);
            synchronized (gate) { flushRunning = false; gate.notifyAll(); }
            hook.hit(Stage.BEFORE_MATERIALIZER_JOIN);
            join(materializer);
            synchronized (gate) {
                for (WalTransaction tx : List.copyOf(active.values())) { abortLocal(tx); }
                closed = true;
                try { log.close(); }
                catch (IOException ex) { throw error(ErrorCode.STORAGE_UNAVAILABLE, "close WAL failed", null, ex); }
            }
        } finally { closeGuard.unlock(); }
    }

    /** 有界等待后台线程，未停止时不释放文件锁。 */
    private static void join(Thread thread) {
        try {
            thread.join(10_000);
            if (thread.isAlive()) { throw new IllegalStateException("WAL worker did not stop; directory remains locked"); }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing WAL runtime", ex);
        }
    }
}

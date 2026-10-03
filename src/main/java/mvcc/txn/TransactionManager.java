package mvcc.txn;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitMode;
import mvcc.api.TransactionCommitResult;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.gc.NoOpSafePointStore;
import mvcc.gc.SafePointStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.tso.TimestampOracle;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 事务管理器，协调 TSO、元数据和分片存储完成 MVCC 事务。
 */
public final class TransactionManager implements AutoCloseable {
    private final TimestampOracle timestampOracle;
    private final TransactionMetadataStore metadataStore;
    private final SafePointStore safePointStore;
    private final Map<String, StorageAdapter> adapters;
    private final TransactionOptions options;
    private final TransactionClassifier transactionClassifier;
    private final ExecutorService participantExecutor;
    private final ExecutorService asyncCommitExecutor;
    private final ExecutorService intentCleanupExecutor;

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(), adapters);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, TransactionOptions.defaults(), adapters);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this.timestampOracle = timestampOracle;
        this.metadataStore = metadataStore;
        this.safePointStore = safePointStore;
        this.options = options;
        this.transactionClassifier = new TransactionClassifier();
        this.adapters = Map.copyOf(adapters);
        this.participantExecutor = requiresParallelExecutor(options.crossShardCommitPolicy())
                ? Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                        .name("mvcc-participant-", 0)
                        .factory())
                : null;
        this.asyncCommitExecutor = options.crossShardCommitPolicy().asyncCommit()
                ? Executors.newFixedThreadPool(
                options.crossShardCommitPolicy().asyncCommitWorkers(),
                namedFactory("mvcc-async-commit-")
        )
                : null;
        this.intentCleanupExecutor = options.crossShardCommitPolicy().asyncCommit() && options.deferIntentCleanup()
                ? Executors.newFixedThreadPool(
                options.crossShardCommitPolicy().asyncCommitWorkers(),
                namedFactory("mvcc-intent-cleanup-")
        )
                : null;
    }

    /**
     * 开启一个新事务。
     */
    public Transaction begin(IsolationLevel isolationLevel) {
        long txId = timestampOracle.nextTimestamp();
        long startTs = txId;
        Transaction transaction = new Transaction(txId, startTs, isolationLevel);
        if (!options.lazyBeginMetadata()) {
            metadataStore.create(txId, startTs, isolationLevel);
            transaction.metadataPersisted(true);
        }
        return transaction;
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    public Optional<byte[]> get(Transaction tx, String shardId, String key) {
        ensureActive(tx);
        StorageAdapter adapter = adapter(shardId);
        long readTs = readTs(tx);
        ensureSnapshotVisible(readTs, tx.txId());
        Optional<byte[]> bufferedValue = bufferedRead(tx, shardId, key);
        if (bufferedValue != null) {
            return bufferedValue;
        }
        Optional<VersionedValue> result = adapter.get(new ReadContext(tx.txId(), readTs, tx.isolationLevel()), key);
        if (result.isEmpty() || result.get().deleted()) {
            return Optional.empty();
        }
        return Optional.of(result.get().value());
    }

    /**
     * 按键范围扫描当前快照可见的数据。
     */
    public List<VersionedValue> scan(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive) {
        ensureActive(tx);
        StorageAdapter adapter = adapter(shardId);
        long readTs = readTs(tx);
        ensureSnapshotVisible(readTs, tx.txId());
        List<VersionedValue> result = adapter.scan(
                new ReadContext(tx.txId(), readTs, tx.isolationLevel()),
                new KeyRange(startKeyInclusive, endKeyExclusive)
        );
        return overlayBufferedMutations(tx, shardId, startKeyInclusive, endKeyExclusive, result);
    }

    /**
     * 在事务中写入键值数据。
     */
    public void put(Transaction tx, String shardId, String key, byte[] value) {
        write(tx, shardId, Mutation.put(key, value));
    }

    /**
     * 在事务中删除键值数据。
     */
    public void delete(Transaction tx, String shardId, String key) {
        write(tx, shardId, Mutation.delete(key));
    }

    /**
     * 在事务模板中执行回调并按策略重试。
     */
    public <T> T execute(IsolationLevel isolationLevel, TransactionCallback<T> callback) {
        MvccException lastRetryable = null;
        for (int attempt = 1; attempt <= options.retryPolicy().maxAttempts(); attempt++) {
            Transaction tx = begin(isolationLevel);
            try {
                T result = callback.doInTransaction(tx);
                commit(tx);
                return result;
            } catch (MvccException ex) {
                safeRollback(tx);
                if (!options.retryPolicy().shouldRetry(ex, attempt)) {
                    throw ex;
                }
                lastRetryable = ex;
                sleepQuietly(options.retryPolicy().nextDelayMillis(attempt - 1));
            } catch (RuntimeException ex) {
                safeRollback(tx);
                throw ex;
            }
        }
        throw lastRetryable == null
                ? new MvccException(ErrorCode.ILLEGAL_STATE, "retry loop exhausted without terminal exception", false, null)
                : lastRetryable;
    }

    /**
     * 提交事务意向并生成已提交版本。
     */
    public TransactionCommitResult commit(Transaction tx) {
        ensureActive(tx);
        TransactionClassification classification = transactionClassifier.classifyForCommit(tx, options);
        switch (classification.protocolType()) {
            case READ_ONLY_SNAPSHOT:
                return commitReadOnly(tx);
            case SINGLE_SHARD_1PC:
                return commitSingleShard(tx, classification.participants().iterator().next());
            case CROSS_SHARD_ASYNC_COMMIT:
            case CROSS_SHARD_SYNC_2PC:
                return commitCrossShard(tx);
            case HOT_KEY_SERIALIZED:
            case LARGE_WRITE_BATCH:
            default:
                throw new MvccException(
                        ErrorCode.ILLEGAL_STATE,
                        "unsupported transaction protocol: " + classification.protocolType(),
                        false,
                        tx.txId()
                );
        }
    }

    /**
     * 提交只读事务。
     */
    private TransactionCommitResult commitReadOnly(Transaction tx) {
        if (options.lazyBeginMetadata() && !tx.metadataPersisted()) {
            // lazy 只读事务没有外部状态需要提交，复用 startTs 避免额外访问 TSO。
            tx.status(TxStatus.COMMITTED);
            return new TransactionCommitResult(tx.txId(), tx.startTs(), TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
        }
        // 没有写入分片时只需要生成提交时间戳并更新事务状态。
        long commitTs = timestampOracle.nextTimestamp();
        if (tx.metadataPersisted()) {
            metadataStore.markCommitted(tx.txId(), commitTs);
        } else if (!options.lazyBeginMetadata()) {
            metadataStore.markCommitted(tx.txId(), commitTs);
        }
        tx.status(TxStatus.COMMITTED);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * 提交单分片事务。
     */
    private TransactionCommitResult commitSingleShard(Transaction tx, String participant) {
        // 单分片事务走直接提交路径，避免完整两阶段提交的额外往返。
        long commitTs = timestampOracle.nextTimestamp();
        boolean skipMetadataFastPath = options.lazyBeginMetadata() && !tx.metadataPersisted();
        if (!skipMetadataFastPath) {
            metadataStore.markSingleShardCommitted(tx.txId(), participant, commitTs);
        }
        try {
            directCommitSingleShardParticipant(tx, participant, commitTs);
        } catch (RuntimeException ex) {
            throw new MvccException(
                    ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "single-shard tx committed in metadata but not fully applied: " + tx.txId(),
                    true,
                    tx.txId(),
                    ex
            );
        }
        tx.status(TxStatus.COMMITTED);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * 提交跨分片事务。
     */
    private TransactionCommitResult commitCrossShard(Transaction tx) {
        prewriteBufferedMutations(tx);

        // 多分片事务先持久化可恢复状态，再提交各参与分片，保证异常后可以恢复。
        boolean explicitPrepareRequired = requiresExplicitPreparePhase(tx.participants());
        if (explicitPrepareRequired) {
            if (tx.metadataPersisted()) {
                metadataStore.persistPreparing(tx.txId(), tx.participants());
            } else if (options.lazyBeginMetadata()) {
                metadataStore.createPreparing(tx.txId(), tx.startTs(), tx.isolationLevel(), tx.participants());
                tx.metadataPersisted(true);
            } else {
                metadataStore.persistPreparing(tx.txId(), tx.participants());
                tx.metadataPersisted(true);
            }
            runAcrossParticipants(
                    tx.participants(),
                    participant -> adapter(participant).prepare(tx.txId()),
                    options.crossShardCommitPolicy().parallelPrepare()
            );
        }

        long commitTs = timestampOracle.nextTimestamp();
        if (explicitPrepareRequired) {
            metadataStore.persistCommitted(tx.txId(), commitTs);
        } else if (tx.metadataPersisted()) {
            metadataStore.persistCommitted(tx.txId(), tx.participants(), commitTs);
        } else if (options.lazyBeginMetadata()) {
            metadataStore.createCommitted(tx.txId(), tx.startTs(), tx.isolationLevel(), tx.participants(), commitTs);
            tx.metadataPersisted(true);
        } else {
            metadataStore.persistCommitted(tx.txId(), tx.participants(), commitTs);
            tx.metadataPersisted(true);
        }
        String primaryParticipant = primaryParticipant(tx.participants());
        try {
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncParticipantCommit(tx, commitTs, tx.participants());
                tx.status(TxStatus.COMMITTED);
                return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.ASYNC_PRIMARY_COMMIT);
            }
            // 异步提交模式先保证主分片落盘，从分片提交交给后台执行。
            commitParticipant(tx, primaryParticipant, commitTs);
            Set<String> secondaryParticipants = secondaryParticipants(tx.participants(), primaryParticipant);
            runAcrossParticipants(
                    secondaryParticipants,
                    participant -> commitParticipant(tx, participant, commitTs),
                    options.crossShardCommitPolicy().parallelCommit()
            );
        } catch (RuntimeException ex) {
            throw new MvccException(
                    ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "tx committed in metadata but not fully applied: " + tx.txId(),
                    true,
                    tx.txId(),
                    ex
            );
        }

        tx.status(TxStatus.COMMITTED);
        return new TransactionCommitResult(
                tx.txId(),
                commitTs,
                options.crossShardCommitPolicy().asyncCommit()
                        ? TransactionCommitMode.ASYNC_PRIMARY_COMMIT
                        : TransactionCommitMode.TWO_PHASE_COMMIT
        );
    }

    /**
     * 关闭资源或后台执行器。
     */
    @Override
    public void close() {
        shutdownExecutor(intentCleanupExecutor);
        shutdownExecutor(asyncCommitExecutor);
        shutdownExecutor(participantExecutor);
    }

    /**
     * 调度异步从分片提交任务。
     */
    private void scheduleAsyncParticipantCommit(Transaction tx, long commitTs, Set<String> participants) {
        if (participants.isEmpty()) {
            return;
        }
        asyncCommitExecutor.submit(() -> {
            for (String participant : participants) {
                try {
                    commitParticipant(tx, participant, commitTs);
                } catch (RuntimeException ignored) {
                }
            }
        });
    }

    /**
     * 选择跨分片事务的主参与者。
     */
    private String primaryParticipant(Set<String> participants) {
        return participants.stream()
                .sorted()
                .findFirst()
                .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "cross-shard tx has no participants", false, null));
    }

    /**
     * 计算除主参与者之外的从参与者集合。
     */
    private Set<String> secondaryParticipants(Set<String> participants, String primaryParticipant) {
        return participants.stream()
                .filter(participant -> !participant.equals(primaryParticipant))
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    /**
     * 回滚事务。
     */
    public void rollback(Transaction tx) {
        if (tx.status() == TxStatus.ABORTED || tx.status() == TxStatus.COMMITTED) {
            return;
        }
        if (tx.metadataPersisted()) {
            metadataStore.markAborted(tx.txId());
        }
        runAcrossParticipants(
                tx.participants(),
                participant -> adapter(participant).abort(tx.txId()),
                options.crossShardCommitPolicy().parallelRollback()
        );
        tx.status(TxStatus.ABORTED);
    }

    /**
     * 读取事务元数据记录。
     */
    public Optional<TxRecord> getRecord(long txId) {
        return metadataStore.get(txId);
    }

    /**
     * 向事务写集合追加一次变更。
     */
    private void write(Transaction tx, String shardId, Mutation mutation) {
        ensureActive(tx);
        if (options.bufferWrites()) {
            tx.bufferMutation(shardId, mutation);
            return;
        }
        StorageAdapter adapter = adapter(shardId);
        LockWaitPolicy lockWaitPolicy = options.lockWaitPolicy();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lockWaitPolicy.lockWaitTimeoutMs());
        int attempt = 0;
        while (true) {
            try {
                adapter.prewrite(tx.txId(), tx.startTs(), java.util.List.of(mutation));
                tx.addParticipant(shardId);
                return;
            } catch (MvccException ex) {
                if (!shouldWaitOnConflict(ex, lockWaitPolicy)) {
                    throw ex;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new MvccException(
                            ErrorCode.LOCK_WAIT_TIMEOUT,
                            "lock wait timeout on key " + mutation.key() + ", tx=" + tx.txId() + ", shard=" + shardId,
                            true,
                            tx.txId(),
                            ex
                    );
                }
                long delayMillis = Math.min(
                        lockWaitPolicy.nextDelayMillis(attempt++),
                        remainingMillis(deadlineNanos)
                );
                sleepQuietly(delayMillis);
            }
        }
    }

    /**
     * 确认事务仍处于可操作状态。
     */
    private void ensureActive(Transaction tx) {
        if (tx.status() != TxStatus.ACTIVE) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "transaction not active: " + tx.txId(), false, tx.txId());
        }
    }

    /**
     * 计算事务读取使用的快照时间戳。
     */
    private long readTs(Transaction tx) {
        return tx.isolationLevel() == IsolationLevel.REPEATABLE_READ
                ? tx.startTs()
                : timestampOracle.nextTimestamp();
    }

    /**
     * 校验读取快照尚未被安全点回收。
     */
    private void ensureSnapshotVisible(long readTs, long txId) {
        long safePoint = safePointStore.currentSafePoint();
        if (readTs < safePoint) {
            throw new MvccException(
                    ErrorCode.SNAPSHOT_TOO_OLD,
                    "snapshot too old, readTs=" + readTs + ", safePoint=" + safePoint,
                    false,
                    txId
            );
        }
    }

    /**
     * 获取指定分片的存储适配器。
     */
    private StorageAdapter adapter(String shardId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "unknown shard: " + shardId, false, null);
        }
        return adapter;
    }

    /**
     * 判断参与分片是否需要显式 prepare 阶段。
     */
    private boolean requiresExplicitPreparePhase(Set<String> participants) {
        for (String participant : participants) {
            if (adapter(participant).requiresExplicitPreparePhase()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断遇到冲突后是否需要等待重试。
     */
    private boolean shouldWaitOnConflict(MvccException ex, LockWaitPolicy lockWaitPolicy) {
        return lockWaitPolicy.conflictMode() == ConflictMode.WAIT
                && TransientStorageConflictDetector.isTransientConflict(ex);
    }

    /**
     * 计算距离截止时间的剩余毫秒数。
     */
    private long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }

    /**
     * 安全回滚事务并吞掉回滚异常。
     */
    private void safeRollback(Transaction tx) {
        try {
            rollback(tx);
        } catch (RuntimeException ignored) {
        }
    }

    /**
     * 在多个参与分片上执行操作。
     */
    private void runAcrossParticipants(Set<String> participants, Consumer<String> operation, boolean parallel) {
        if (!parallel || participants.size() <= 1) {
            for (String participant : participants) {
                operation.accept(participant);
            }
            return;
        }

        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CompletableFuture<?>[] futures = participants.stream()
                .map(participant -> CompletableFuture.runAsync(() -> {
                    try {
                        operation.accept(participant);
                    } catch (Throwable ex) {
                        // 并行参与者抛出的 Error 也必须回传给调用方，避免提交路径误判成功。
                        failures.add(ex);
                        throw ex;
                    }
                }, participantExecutor))
                .toArray(CompletableFuture[]::new);

        try {
            CompletableFuture.allOf(futures).join();
        } catch (CompletionException ignored) {
        }

        Throwable failure = failures.peek();
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new CompletionException(failure);
        }
    }

    /**
     * 安静等待指定毫秒数。
     */
    private void sleepQuietly(long delayMillis) {
        if (delayMillis <= 0L) {
            return;
        }
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "interrupted while waiting for retry", true, null, ex);
        }
    }

    /**
     * 将事务缓冲写集合预写入各分片。
     */
    private void prewriteBufferedMutations(Transaction tx) {
        if (!options.bufferWrites()) {
            return;
        }
        runAcrossParticipants(
                tx.participants(),
                shardId -> {
                    List<Mutation> mutations = tx.bufferedMutations(shardId);
                    if (!mutations.isEmpty()) {
                        prewriteMutations(tx, shardId, mutations);
                    }
                },
                options.crossShardCommitPolicy().parallelPrewrite()
        );
    }

    /**
     * 带冲突等待策略地预写入指定分片变更。
     */
    private void prewriteMutations(Transaction tx, String shardId, List<Mutation> mutations) {
        LockWaitPolicy lockWaitPolicy = options.lockWaitPolicy();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lockWaitPolicy.lockWaitTimeoutMs());
        int attempt = 0;
        while (true) {
            try {
                adapter(shardId).prewrite(tx.txId(), tx.startTs(), mutations);
                tx.addParticipant(shardId);
                return;
            } catch (MvccException ex) {
                if (!shouldWaitOnConflict(ex, lockWaitPolicy)) {
                    throw ex;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new MvccException(
                            ErrorCode.LOCK_WAIT_TIMEOUT,
                            "lock wait timeout on shard " + shardId + ", tx=" + tx.txId(),
                            true,
                            tx.txId(),
                            ex
                    );
                }
                // 等待型锁策略只对瞬时冲突退避重试，避免对永久错误做无意义重试。
                long delayMillis = Math.min(lockWaitPolicy.nextDelayMillis(attempt++), remainingMillis(deadlineNanos));
                sleepQuietly(delayMillis);
            }
        }
    }

    /**
     * 从事务本地写缓冲读取最新变更。
     */
    private Optional<byte[]> bufferedRead(Transaction tx, String shardId, String key) {
        if (!options.bufferWrites()) {
            return null;
        }
        Optional<Mutation> mutation = tx.bufferedMutation(shardId, key);
        if (mutation.isEmpty()) {
            return null;
        }
        return mutation.get().type() == mvcc.common.MutationType.DELETE
                ? Optional.empty()
                : Optional.of(mutation.get().value());
    }

    /**
     * 将事务本地缓冲写覆盖到扫描结果上。
     */
    private List<VersionedValue> overlayBufferedMutations(Transaction tx,
                                                         String shardId,
                                                         String startKeyInclusive,
                                                         String endKeyExclusive,
                                                         List<VersionedValue> baseValues) {
        if (!options.bufferWrites()) {
            return baseValues;
        }
        java.util.NavigableMap<String, VersionedValue> visible = new java.util.TreeMap<>();
        for (VersionedValue value : baseValues) {
            visible.put(value.key(), value);
        }
        for (Mutation mutation : tx.bufferedMutationsInRange(shardId, startKeyInclusive, endKeyExclusive)) {
            if (mutation.type() == mvcc.common.MutationType.DELETE) {
                visible.remove(mutation.key());
            } else {
                visible.put(mutation.key(), new VersionedValue(
                        mutation.key(),
                        mutation.value(),
                        false,
                        tx.startTs(),
                        true
                ));
            }
        }
        return List.copyOf(visible.values());
    }

    /**
     * 判断提交策略是否需要并行执行器。
     */
    private boolean requiresParallelExecutor(CrossShardCommitPolicy policy) {
        return policy.parallelPrewrite()
                || policy.parallelPrepare()
                || policy.parallelCommit()
                || policy.parallelRollback()
                || policy.asyncCommit();
    }

    /**
     * 关闭并行执行器。
     */
    private void shutdownExecutor(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 创建带名称前缀的后台线程工厂。
     */
    private ThreadFactory namedFactory(String prefix) {
        return Thread.ofPlatform()
                .daemon(true)
                .name(prefix, 0)
                .factory();
    }

    /**
     * 提交指定参与分片。
     */
    private void commitParticipant(Transaction tx, String shardId, long commitTs) {
        List<Mutation> bufferedMutations = tx.bufferedMutations(shardId);
        if (options.bufferWrites() && !bufferedMutations.isEmpty()) {
            boolean cleanupIntents = !options.deferIntentCleanup() || asyncCommitExecutor == null;
            adapter(shardId).commit(tx.txId(), commitTs, bufferedMutations, cleanupIntents);
            if (!cleanupIntents) {
                intentCleanupExecutor.submit(() -> {
                    try {
                        adapter(shardId).cleanupIntents(tx.txId());
                    } catch (RuntimeException ignored) {
                    }
                });
            }
            return;
        }
        adapter(shardId).commit(tx.txId(), commitTs);
    }

    /**
     * 直接提交单分片参与者。
     */
    private void directCommitSingleShardParticipant(Transaction tx, String shardId, long commitTs) {
        List<Mutation> bufferedMutations = tx.bufferedMutations(shardId);
        if (options.bufferWrites() && !bufferedMutations.isEmpty()) {
            adapter(shardId).directCommit(tx.txId(), tx.startTs(), commitTs, bufferedMutations);
            return;
        }
        commitParticipant(tx, shardId, commitTs);
    }
}

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
import mvcc.metrics.NoOpTransactionMetrics;
import mvcc.metrics.TransactionMetrics;
import mvcc.metrics.TransactionStage;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.tso.TimestampOracle;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import java.util.function.Supplier;

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
    private final TransactionMetrics metrics;
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
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters,
                              TransactionMetrics metrics) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters, metrics);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, NoOpTransactionMetrics.INSTANCE);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters,
                              TransactionMetrics metrics) {
        this.timestampOracle = timestampOracle;
        this.metadataStore = metadataStore;
        this.safePointStore = safePointStore;
        this.options = options;
        this.transactionClassifier = new TransactionClassifier();
        this.metrics = Objects.requireNonNull(metrics, "metrics");
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
        long txId = nextTimestamp(TransactionStage.BEGIN_TSO);
        long startTs = txId;
        Transaction transaction = new Transaction(txId, startTs, isolationLevel);
        if (!options.lazyBeginMetadata()) {
            createBeginMetadata(txId, startTs, isolationLevel);
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
        StatementReadSnapshot snapshot = beginReadStatement(tx);
        Optional<byte[]> bufferedValue = bufferedRead(tx, shardId, key);
        if (bufferedValue != null) {
            return bufferedValue;
        }
        Optional<VersionedValue> result = adapter.get(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
                key
        );
        if (result.isEmpty() || result.get().deleted()) {
            return Optional.empty();
        }
        return Optional.of(result.get().value());
    }

    /**
     * 按键范围扫描当前快照可见的数据。
     */
    public List<VersionedValue> scan(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive) {
        return scan(tx, shardId, startKeyInclusive, endKeyExclusive, beginReadStatement(tx));
    }

    /**
     * 创建绑定当前事务的语句级读取快照。
     */
    public StatementReadSnapshot beginReadStatement(Transaction tx) {
        ensureActive(tx);
        long readTs = readTs(tx);
        ensureSnapshotVisible(readTs, tx.txId());
        return new StatementReadSnapshot(tx.txId(), readTs);
    }

    /**
     * 使用指定语句级快照扫描键范围，供一次 SQL 内的多表或多分片扫描复用同一可见边界。
     */
    public List<VersionedValue> scan(Transaction tx,
                                     String shardId,
                                     String startKeyInclusive,
                                     String endKeyExclusive,
                                     StatementReadSnapshot snapshot) {
        ensureActive(tx);
        if (snapshot.txId() != tx.txId()) {
            throw new MvccException(
                    ErrorCode.ILLEGAL_STATE,
                    "statement snapshot belongs to another transaction: " + snapshot.txId(),
                    false,
                    tx.txId()
            );
        }
        ensureSnapshotVisible(snapshot.readTs(), tx.txId());
        StorageAdapter adapter = adapter(shardId);
        List<VersionedValue> result = adapter.scan(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
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
        long commitTs = nextTimestamp(TransactionStage.COMMIT_TSO);
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
        long commitTs = nextTimestamp(TransactionStage.COMMIT_TSO);
        boolean skipMetadataFastPath = options.lazyBeginMetadata() && !tx.metadataPersisted();
        if (!skipMetadataFastPath) {
            markSingleShardCommittedStage(tx, participant, commitTs);
        }
        try {
            directCommitSingleShardParticipantStage(tx, participant, commitTs);
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
        if (options.bufferWrites()) {
            prewriteBufferedMutationsStage(tx);
        } else {
            prewriteBufferedMutations(tx);
        }

        // 多分片事务先持久化可恢复状态，再提交各参与分片，保证异常后可以恢复。
        boolean explicitPrepareRequired = requiresExplicitPreparePhase(tx.participants());
        if (explicitPrepareRequired) {
            persistPreparingMetadata(tx);
            prepareParticipantsStage(tx);
        }

        long commitTs = nextTimestamp(TransactionStage.COMMIT_TSO);
        persistCommittedMetadataStage(tx, commitTs, explicitPrepareRequired);
        String primaryParticipant = primaryParticipant(tx.participants());
        try {
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncParticipantCommit(tx, commitTs, tx.participants());
                tx.status(TxStatus.COMMITTED);
                return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.ASYNC_PRIMARY_COMMIT);
            }
            // 同步提交模式先提交主参与者，再提交其余参与者，保留状态未知异常边界。
            commitParticipantStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT, tx, primaryParticipant, commitTs);
            Set<String> secondaryParticipants = secondaryParticipants(tx.participants(), primaryParticipant);
            runAcrossParticipants(
                    secondaryParticipants,
                    participant -> commitParticipantStage(
                            TransactionStage.SECONDARY_PARTICIPANT_COMMIT,
                            tx,
                            participant,
                            commitTs
                    ),
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
        shutdownExecutor(asyncCommitExecutor);
        // async commit 可能继续提交 intent cleanup 任务，必须先 drain async 再关闭 cleanup。
        shutdownExecutor(intentCleanupExecutor);
        shutdownExecutor(participantExecutor);
    }

    /**
     * 调度异步参与者提交任务。
     */
    private void scheduleAsyncParticipantCommit(Transaction tx, long commitTs, Set<String> participants) {
        if (participants.isEmpty()) {
            return;
        }
        asyncCommitExecutor.submit(() -> {
            for (String participant : participants) {
                try {
                    commitParticipantStage(TransactionStage.ASYNC_PARTICIPANT_COMMIT, tx, participant, commitTs);
                } catch (RuntimeException ignored) {
                }
            }
        });
    }

    /**
     * 持久化跨分片事务准备中元数据。
     */
    private void persistPreparingMetadata(Transaction tx) {
        if (tx.metadataPersisted()) {
            metadataStore.persistPreparing(tx.txId(), tx.participants());
        } else if (options.lazyBeginMetadata()) {
            metadataStore.createPreparing(tx.txId(), tx.startTs(), tx.isolationLevel(), tx.participants());
            tx.metadataPersisted(true);
        } else {
            metadataStore.persistPreparing(tx.txId(), tx.participants());
            tx.metadataPersisted(true);
        }
    }

    /**
     * 持久化跨分片事务已提交元数据。
     */
    private void persistCommittedMetadata(Transaction tx, long commitTs, boolean explicitPrepareRequired) {
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
        rollbackStage(tx);
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
        prewriteEagerMutationStage(tx, shardId, mutation, adapter);
        tx.addParticipant(shardId);
    }

    /**
     * 带冲突等待策略地预写入非缓冲单条变更。
     */
    private void prewriteEagerMutation(Transaction tx, String shardId, Mutation mutation, StorageAdapter adapter) {
        LockWaitPolicy lockWaitPolicy = options.lockWaitPolicy();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lockWaitPolicy.lockWaitTimeoutMs());
        int attempt = 0;
        while (true) {
            try {
                adapter.prewrite(tx.txId(), tx.startTs(), java.util.List.of(mutation));
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
                : nextTimestamp(TransactionStage.READ_TSO);
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
                        cleanupIntentsStage(tx, shardId);
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

    /**
     * 判断当前是否启用事务阶段指标。
     */
    private boolean metricsEnabled() {
        return metrics != NoOpTransactionMetrics.INSTANCE;
    }

    /**
     * 按需记录时间戳分配阶段。
     */
    private long nextTimestamp(TransactionStage stage) {
        if (!metricsEnabled()) {
            return timestampOracle.nextTimestamp();
        }
        return recordMeasuredStage(stage, timestampOracle::nextTimestamp);
    }

    /**
     * 按需记录 begin metadata 创建阶段。
     */
    private void createBeginMetadata(long txId, long startTs, IsolationLevel isolationLevel) {
        if (!metricsEnabled()) {
            metadataStore.create(txId, startTs, isolationLevel);
            return;
        }
        recordMeasuredStage(TransactionStage.BEGIN_METADATA, () -> metadataStore.create(txId, startTs, isolationLevel));
    }

    /**
     * 按需记录单分片 metadata 提交阶段。
     */
    private void markSingleShardCommittedStage(Transaction tx, String participant, long commitTs) {
        if (!metricsEnabled()) {
            metadataStore.markSingleShardCommitted(tx.txId(), participant, commitTs);
            return;
        }
        recordMeasuredStage(
                TransactionStage.SINGLE_SHARD_METADATA_COMMIT,
                () -> metadataStore.markSingleShardCommitted(tx.txId(), participant, commitTs)
        );
    }

    /**
     * 按需记录缓冲写预写阶段。
     */
    private void prewriteBufferedMutationsStage(Transaction tx) {
        if (!metricsEnabled()) {
            prewriteBufferedMutations(tx);
            return;
        }
        recordMeasuredStage(TransactionStage.BUFFERED_PREWRITE, () -> prewriteBufferedMutations(tx));
    }

    /**
     * 按需记录非缓冲预写阶段。
     */
    private void prewriteEagerMutationStage(Transaction tx, String shardId, Mutation mutation, StorageAdapter adapter) {
        if (!metricsEnabled()) {
            prewriteEagerMutation(tx, shardId, mutation, adapter);
            return;
        }
        recordMeasuredStage(TransactionStage.EAGER_PREWRITE, () -> prewriteEagerMutation(tx, shardId, mutation, adapter));
    }

    /**
     * 按需记录显式 prepare 阶段。
     */
    private void prepareParticipantsStage(Transaction tx) {
        if (!metricsEnabled()) {
            runAcrossParticipants(
                    tx.participants(),
                    participant -> adapter(participant).prepare(tx.txId()),
                    options.crossShardCommitPolicy().parallelPrepare()
            );
            return;
        }
        recordMeasuredStage(
                TransactionStage.PREPARE,
                () -> runAcrossParticipants(
                        tx.participants(),
                        participant -> adapter(participant).prepare(tx.txId()),
                        options.crossShardCommitPolicy().parallelPrepare()
                )
        );
    }

    /**
     * 按需记录跨分片 metadata 提交阶段。
     */
    private void persistCommittedMetadataStage(Transaction tx, long commitTs, boolean explicitPrepareRequired) {
        if (!metricsEnabled()) {
            persistCommittedMetadata(tx, commitTs, explicitPrepareRequired);
            return;
        }
        recordMeasuredStage(
                TransactionStage.CROSS_SHARD_METADATA_COMMIT,
                () -> persistCommittedMetadata(tx, commitTs, explicitPrepareRequired)
        );
    }

    /**
     * 按需记录参与者提交阶段。
     */
    private void commitParticipantStage(TransactionStage stage, Transaction tx, String shardId, long commitTs) {
        if (!metricsEnabled()) {
            commitParticipant(tx, shardId, commitTs);
            return;
        }
        recordMeasuredStage(stage, () -> commitParticipant(tx, shardId, commitTs));
    }

    /**
     * 按需记录单分片直接提交阶段。
     */
    private void directCommitSingleShardParticipantStage(Transaction tx, String shardId, long commitTs) {
        if (!metricsEnabled()) {
            directCommitSingleShardParticipant(tx, shardId, commitTs);
            return;
        }
        recordMeasuredStage(
                TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT,
                () -> directCommitSingleShardParticipant(tx, shardId, commitTs)
        );
    }

    /**
     * 按需记录回滚阶段。
     */
    private void rollbackStage(Transaction tx) {
        if (!metricsEnabled()) {
            rollbackInternal(tx);
            return;
        }
        recordMeasuredStage(TransactionStage.ROLLBACK, () -> rollbackInternal(tx));
    }

    /**
     * 执行事务回滚的实际动作。
     */
    private void rollbackInternal(Transaction tx) {
        if (tx.metadataPersisted()) {
            metadataStore.markAborted(tx.txId());
        }
        runAcrossParticipants(
                tx.participants(),
                participant -> adapter(participant).abort(tx.txId()),
                options.crossShardCommitPolicy().parallelRollback()
        );
    }

    /**
     * 按需记录 intent 清理阶段。
     */
    private void cleanupIntentsStage(Transaction tx, String shardId) {
        if (!metricsEnabled()) {
            adapter(shardId).cleanupIntents(tx.txId());
            return;
        }
        recordMeasuredStage(TransactionStage.INTENT_CLEANUP, () -> adapter(shardId).cleanupIntents(tx.txId()));
    }

    /**
     * 记录带返回值的计量阶段执行结果。
     */
    private <T> T recordMeasuredStage(TransactionStage stage, Supplier<T> action) {
        long startNanos = System.nanoTime();
        try {
            T result = action.get();
            recordMetricSuccess(stage, System.nanoTime() - startNanos);
            return result;
        } catch (RuntimeException | Error ex) {
            recordMetricFailure(stage, System.nanoTime() - startNanos);
            throw ex;
        }
    }

    /**
     * 记录无返回值的计量阶段执行结果。
     */
    private void recordMeasuredStage(TransactionStage stage, Runnable action) {
        long startNanos = System.nanoTime();
        try {
            action.run();
            recordMetricSuccess(stage, System.nanoTime() - startNanos);
        } catch (RuntimeException | Error ex) {
            recordMetricFailure(stage, System.nanoTime() - startNanos);
            throw ex;
        }
    }

    /**
     * 尽力记录成功指标，避免指标实现异常反向影响事务语义。
     */
    private void recordMetricSuccess(TransactionStage stage, long elapsedNanos) {
        try {
            metrics.recordSuccess(stage, elapsedNanos);
        } catch (RuntimeException | Error ignored) {
        }
    }

    /**
     * 尽力记录失败指标，避免遮蔽原始业务异常。
     */
    private void recordMetricFailure(TransactionStage stage, long elapsedNanos) {
        try {
            metrics.recordFailure(stage, elapsedNanos);
        } catch (RuntimeException | Error ignored) {
        }
    }
}

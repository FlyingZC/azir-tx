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
import mvcc.storage.ScanPage;
import mvcc.tso.TimestampOracle;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
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
    private final Map<String, Semaphore> shardPermits = new HashMap<>();
    private final Semaphore transactionPermits;
    private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final LongAdder synchronousFallbacks = new LongAdder();
    private final LongAdder backgroundFailures = new LongAdder();
    private final Object transactionOwner = new Object();

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
        this.transactionPermits = new Semaphore(options.limits().maxActiveTransactions());
        this.adapters.forEach((shardId, adapter) -> {
            if (shardId.isBlank() || shardId.contains(",") || !shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("shard id must match its adapter and cannot be blank or contain commas");
            }
            adapter.configureMetadataStore(metadataStore);
            shardPermits.put(shardId, new Semaphore(options.limits().shardConcurrency(), true));
        });
        this.participantExecutor = requiresParallelExecutor(options.crossShardCommitPolicy())
                ? Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                        .name("mvcc-participant-", 0)
                        .factory())
                : null;
        this.asyncCommitExecutor = options.crossShardCommitPolicy().asyncCommit()
                ? boundedExecutor("mvcc-async-commit-")
                : null;
        this.intentCleanupExecutor = options.crossShardCommitPolicy().asyncCommit() && options.deferIntentCleanup()
                ? boundedExecutor("mvcc-intent-cleanup-")
                : null;
    }

    /**
     * 开启一个新事务。
     */
    public Transaction begin(IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        ensureOpen();
        expireLocalTransactions();
        if (!transactionPermits.tryAcquire()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "active transaction limit reached", true, null);
        }
        Transaction registered = null;
        try {
            Transaction transaction = metadataStore.withPublicationLock(() -> {
                ensureOpen();
                metadataStore.assertNoPendingPublications();
                long txId = nextTimestamp(TransactionStage.BEGIN_TSO);
                Transaction created = new Transaction(txId, txId, isolationLevel,
                        metadataStore.currentTimeMillis() + options.limits().timeoutMillis(), transactionOwner);
                // 独立快照登记保护 lazy 只读事务，不能依赖是否写入事务元数据。
                metadataStore.registerSnapshot(txId, txId, created.deadlineMillis());
                if (!options.lazyBeginMetadata()) {
                    createBeginMetadata(txId, txId, isolationLevel);
                    created.metadataPersisted(true);
                }
                return created;
            });
            activeTransactions.put(transaction.txId(), transaction);
            registered = transaction;
            ensureOpen();
            return transaction;
        } catch (RuntimeException | Error ex) {
            if (registered == null || activeTransactions.remove(registered.txId(), registered)) {
                transactionPermits.release();
            }
            throw ex;
        }
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    public Optional<byte[]> get(Transaction tx, String shardId, String key) {
        return get(tx, shardId, key, beginReadStatement(tx));
    }

    /** 在同一条 SQL 的固定快照下执行点读。 */
    public Optional<byte[]> get(Transaction tx, String shardId, String key, StatementReadSnapshot snapshot) {
        validateStatement(tx, snapshot);
        StorageAdapter adapter = adapter(shardId);
        Optional<byte[]> bufferedValue = bufferedRead(tx, shardId, key);
        if (bufferedValue != null) {
            return bufferedValue;
        }
        Optional<VersionedValue> result = withShardPermit(shardId, () -> adapter.get(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
                key
        ));
        ensureLeaseValid(tx);
        if (result.isEmpty() || result.get().deleted()) {
            return Optional.empty();
        }
        return Optional.of(result.get().value().clone());
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
        return metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            metadataStore.assertNoPendingPublications();
            long readTs = readTs(tx);
            ensureSnapshotVisible(readTs, tx.txId());
            return new StatementReadSnapshot(tx.txId(), readTs);
        });
    }

    /** SQL 完成排序或聚合后验证原语句仍在租约内，不分配新的 RC 时间戳。 */
    public void validateReadStatement(Transaction tx, StatementReadSnapshot snapshot) {
        validateStatement(tx, snapshot);
    }

    /**
     * 使用指定语句级快照扫描键范围，供一次 SQL 内的多表或多分片扫描复用同一可见边界。
     */
    public List<VersionedValue> scan(Transaction tx,
                                     String shardId,
                                     String startKeyInclusive,
                                     String endKeyExclusive,
                                     StatementReadSnapshot snapshot) {
        List<VersionedValue> result = new ArrayList<>();
        String after = null;
        long bytes = 0;
        do {
            ScanPage page = scanPage(tx, shardId, startKeyInclusive, endKeyExclusive,
                    snapshot, after, options.limits().scanPageSize());
            for (VersionedValue value : page.values()) {
                bytes += value.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + (value.value() == null ? 0 : value.value().length);
                if (result.size() >= options.limits().maxMaterializedRows() || bytes > options.limits().maxMaterializedBytes()) {
                    throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                            "materialized scan budget exceeded; use scanPage", false, tx.txId());
                }
                result.add(value);
            }
            after = page.nextKey();
        } while (after != null);
        return List.copyOf(result);
    }

    /** 按二进制键序合并一页存储结果和自身缓冲变更，空页也可带继续位置。 */
    public ScanPage scanPage(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive,
                             StatementReadSnapshot snapshot, String afterKeyExclusive, int limit) {
        validateStatement(tx, snapshot);
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("page limit must be between 1 and 4096");
        }
        if (startKeyInclusive != null && endKeyExclusive != null && compareKeys(startKeyInclusive, endKeyExclusive) > 0) {
            throw new IllegalArgumentException("scan start must not exceed end");
        }
        StorageAdapter adapter = adapter(shardId);
        ScanPage base = withShardPermit(shardId, () -> adapter.scanPage(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
                new KeyRange(startKeyInclusive, endKeyExclusive), afterKeyExclusive, limit));
        if (base.nextKey() != null && afterKeyExclusive != null && compareKeys(base.nextKey(), afterKeyExclusive) <= 0) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "storage page cursor did not advance", false, tx.txId());
        }
        List<Mutation> buffered = options.bufferWrites()
                ? tx.bufferedMutationsPage(shardId, startKeyInclusive, endKeyExclusive, afterKeyExclusive, limit + 1)
                : List.of();
        String pageEnd = base.nextKey();
        if (buffered.size() > limit) {
            String bufferedEnd = buffered.get(limit - 1).key();
            if (pageEnd == null || compareKeys(bufferedEnd, pageEnd) < 0) {
                pageEnd = bufferedEnd;
            }
        }
        NavigableMap<String, VersionedValue> merged = new TreeMap<>(TransactionManager::compareKeys);
        for (VersionedValue value : base.values()) {
            if (pageEnd == null || compareKeys(value.key(), pageEnd) <= 0) {
                merged.put(value.key(), value);
            }
        }
        if (!buffered.isEmpty()) {
            for (Mutation mutation : buffered) {
                String key = mutation.key();
                if ((startKeyInclusive != null && compareKeys(key, startKeyInclusive) < 0)
                        || (endKeyExclusive != null && compareKeys(key, endKeyExclusive) >= 0)
                        || (afterKeyExclusive != null && compareKeys(key, afterKeyExclusive) <= 0)
                        || (pageEnd != null && compareKeys(key, pageEnd) > 0)) {
                    continue;
                }
                if (mutation.type() == mvcc.common.MutationType.DELETE) {
                    merged.remove(key);
                } else {
                    merged.put(key, new VersionedValue(key, mutation.value(), false, tx.startTs(), true));
                }
            }
        }
        List<VersionedValue> values = new ArrayList<>(Math.min(limit, merged.size()));
        for (VersionedValue value : merged.values()) {
            if (values.size() == limit) {
                ensureLeaseValid(tx);
                return new ScanPage(List.copyOf(values), values.getLast().key());
            }
            values.add(new VersionedValue(value.key(), value.value() == null ? null : value.value().clone(),
                    value.deleted(), value.versionTs(), value.ownIntent()));
        }
        ensureLeaseValid(tx);
        return new ScanPage(List.copyOf(values), pageEnd);
    }

    /**
     * 在事务中写入键值数据。
     */
    public void put(Transaction tx, String shardId, String key, byte[] value) {
        Objects.requireNonNull(value, "value");
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
                if (tx.decisionStarted() || tx.status() == TxStatus.COMMITTED) {
                    throw ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN ? ex : unknownCommit(tx, ex);
                }
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
        try {
            return switch (classification.protocolType()) {
                case READ_ONLY_SNAPSHOT -> commitReadOnly(tx);
                case SINGLE_SHARD_1PC -> commitSingleShard(tx, classification.participants().iterator().next());
                case CROSS_SHARD_ASYNC_COMMIT, CROSS_SHARD_SYNC_2PC -> commitCrossShard(tx);
                default -> throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "unsupported transaction protocol: " + classification.protocolType(), false, tx.txId());
            };
        } catch (RuntimeException ex) {
            if (tx.decisionStarted()) {
                if (tx.status() == TxStatus.COMMITTED) {
                    releaseTransaction(tx);
                }
                if (ex instanceof MvccException mvcc && mvcc.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN) {
                    throw mvcc;
                }
                throw unknownCommit(tx, ex);
            }
            throw ex;
        }
    }

    /**
     * 提交只读事务。
     */
    private TransactionCommitResult commitReadOnly(Transaction tx) {
        long commitTs = metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            if (!tx.metadataPersisted()) {
                return tx.startTs();
            }
            long timestamp = nextTimestamp(TransactionStage.COMMIT_TSO);
            tx.decisionStarted(true);
            try {
                metadataStore.markCommitted(tx.txId(), timestamp);
            } catch (RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
            return timestamp;
        });
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * 提交单分片事务。
     */
    private TransactionCommitResult commitSingleShard(Transaction tx, String participant) {
        registerWriteParticipants(tx);
        boolean atomicDirect = options.bufferWrites() && adapter(participant).supportsAtomicDirectCommit();
        if (!atomicDirect && !adapter(participant).supportsCommittedIntentResolution()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "intent-based single-shard commit requires committed intent resolution", false, tx.txId());
        }
        if (!atomicDirect && options.bufferWrites()) {
            prewriteBufferedMutationsStage(tx);
        }
        long commitTs = metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            long timestamp = nextTimestamp(TransactionStage.COMMIT_TSO);
            tx.decisionStarted(true);
            try {
                if (atomicDirect) {
                    // 本地版本和事务记录先原子提交，中心决议丢失时由参与者记录恢复。
                    metadataStore.markLocalCommitPending(tx.txId(), timestamp);
                    directCommitSingleShardParticipantStage(tx, participant, timestamp);
                }
                markSingleShardCommittedStage(tx, participant, timestamp);
                tx.status(TxStatus.COMMITTED);
                if (!atomicDirect) {
                    commitParticipantStage(TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT,
                            tx, participant, timestamp);
                }
                return timestamp;
            } catch (MvccException ex) {
                if (atomicDirect && !tx.status().equals(TxStatus.COMMITTED)
                        && (ex.errorCode() == ErrorCode.WRITE_CONFLICT || ex.errorCode() == ErrorCode.LOCK_CONFLICT)) {
                    tx.decisionStarted(false);
                    metadataStore.markAborted(tx.txId());
                    tx.status(TxStatus.ABORTED);
                    throw ex;
                }
                throw unknownCommit(tx, ex);
            } catch (RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
        });
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * 提交跨分片事务。
     */
    private TransactionCommitResult commitCrossShard(Transaction tx) {
        for (String participant : tx.participants()) {
            if (!adapter(participant).supportsCommittedIntentResolution()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "cross-shard snapshot visibility requires committed intent resolution: " + participant,
                        false, tx.txId());
            }
        }
        registerWriteParticipants(tx);
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

        long commitTs = metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            long timestamp = nextTimestamp(TransactionStage.COMMIT_TSO);
            tx.decisionStarted(true);
            try {
                persistCommittedMetadataStage(tx, timestamp, explicitPrepareRequired);
                tx.status(TxStatus.COMMITTED);
                return timestamp;
            } catch (RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
        });
        String primaryParticipant = primaryParticipant(tx.participants());
        try {
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncParticipantCommit(tx, commitTs, tx.participants());
                completeTransaction(tx, false);
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
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }

        completeTransaction(tx, true);
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.limits().shutdownTimeoutMillis());
        ExecutorService rollbackExecutor = Executors.newVirtualThreadPerTaskExecutor();
        for (Transaction tx : List.copyOf(activeTransactions.values())) {
            rollbackExecutor.submit(() -> safeRollback(tx));
        }
        shutdownExecutor(rollbackExecutor, deadline);
        shutdownExecutor(asyncCommitExecutor, deadline);
        // async commit 可能继续提交 intent cleanup 任务，必须先 drain async 再关闭 cleanup。
        shutdownExecutor(intentCleanupExecutor, deadline);
        shutdownExecutor(participantExecutor, deadline);
        // 未完成的回滚保留持久化租约及恢复记录，仅释放关闭管理器的本地写缓冲。
        activeTransactions.forEach((txId, tx) -> {
            if (activeTransactions.remove(txId, tx)) {
                transactionPermits.release();
            }
        });
    }

    /**
     * 调度异步参与者提交任务。
     */
    private void scheduleAsyncParticipantCommit(Transaction tx, long commitTs, Set<String> participants) {
        if (participants.isEmpty()) {
            return;
        }
        Set<String> stableParticipants = Set.copyOf(participants);
        submitOrRun(asyncCommitExecutor, () -> {
            boolean succeeded = true;
            for (String participant : stableParticipants) {
                try {
                    commitParticipantStage(TransactionStage.ASYNC_PARTICIPANT_COMMIT, tx, participant, commitTs);
                } catch (RuntimeException ex) {
                    backgroundFailures.increment();
                    succeeded = false;
                }
            }
            if (succeeded) {
                markFinalizedQuietly(tx);
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
        ensureOwned(tx);
        if (tx.status() == TxStatus.COMMITTED) {
            releaseTransaction(tx);
            return;
        }
        rollbackStage(tx);
        if (tx.status() != TxStatus.COMMITTED) {
            tx.status(TxStatus.ABORTED);
        }
        releaseTransaction(tx);
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
        ensureLeaseValid(tx);
        adapter(shardId);
        mvcc.storage.Utf8KeyOrder.encode(mutation.key());
        tx.reserveWrite(shardId, mutation, options.limits().maxWriteKeys(), options.limits().maxWriteBytes());
        // 复制值避免调用方修改数组影响已计费、已预写或后台提交的写集合。
        mutation = new Mutation(mutation.key(), mutation.type(), mutation.value() == null ? null : mutation.value().clone());
        if (options.bufferWrites()) {
            tx.bufferMutation(shardId, mutation);
            return;
        }
        StorageAdapter adapter = adapter(shardId);
        tx.addParticipant(shardId);
        try {
            registerWriteParticipants(tx);
            prewriteEagerMutationStage(tx, shardId, mutation, adapter);
        } catch (RuntimeException | Error ex) {
            tx.markRollbackOnly();
            throw ex;
        }
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
                withShardPermit(shardId, () -> {
                    ensureLeaseValid(tx);
                    adapter.prewrite(tx.txId(), tx.startTs(), java.util.List.of(mutation));
                    return null;
                });
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
        ensureOpen();
        ensureOwned(tx);
        if (activeTransactions.get(tx.txId()) != tx) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction belongs to another or closed manager", false, tx.txId());
        }
        if (tx.status() != TxStatus.ACTIVE || tx.rollbackOnly()) {
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
                withShardPermit(shardId, () -> {
                    ensureLeaseValid(tx);
                    adapter(shardId).prewrite(tx.txId(), tx.startTs(), mutations);
                    return null;
                });
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
                : Optional.of(mutation.get().value().clone());
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
    private void shutdownExecutor(ExecutorService executor, long deadlineNanos) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Math.max(0L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                // 未完成物化由持久化决议恢复，不能阻止关闭无限等待。
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
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
            withShardPermit(shardId, () -> {
                adapter(shardId).commit(tx.txId(), commitTs, bufferedMutations, cleanupIntents);
                return null;
            });
            if (!cleanupIntents) {
                submitOrRun(intentCleanupExecutor, () -> {
                    try {
                        cleanupIntentsStage(tx, shardId);
                    } catch (RuntimeException ex) {
                        backgroundFailures.increment();
                    }
                });
            }
            return;
        }
        withShardPermit(shardId, () -> {
            adapter(shardId).commit(tx.txId(), commitTs);
            return null;
        });
    }

    /**
     * 直接提交单分片参与者。
     */
    private void directCommitSingleShardParticipant(Transaction tx, String shardId, long commitTs) {
        List<Mutation> bufferedMutations = tx.bufferedMutations(shardId);
        if (options.bufferWrites() && !bufferedMutations.isEmpty()) {
            withShardPermit(shardId, () -> {
                adapter(shardId).directCommit(tx.txId(), tx.startTs(), commitTs, bufferedMutations);
                return null;
            });
            return;
        }
        commitParticipant(tx, shardId, commitTs);
    }

    /** 返回调用方可导出到监控系统的有界执行统计。 */
    public TransactionRuntimeStats runtimeStats() {
        return new TransactionRuntimeStats(activeTransactions.size(), queuedTasks(asyncCommitExecutor),
                queuedTasks(intentCleanupExecutor), synchronousFallbacks.sum(), backgroundFailures.sum());
    }

    /** 创建具备固定线程数和有界等待队列的后台执行器。 */
    private ThreadPoolExecutor boundedExecutor(String prefix) {
        int workers = options.crossShardCommitPolicy().asyncCommitWorkers();
        return new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(options.limits().asyncQueueCapacity()), namedFactory(prefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /** 队列满或正在关闭时由当前线程完成已决议工作，不丢弃补提交。 */
    private void submitOrRun(ExecutorService executor, Runnable action) {
        try {
            executor.execute(action);
        } catch (RejectedExecutionException ex) {
            synchronousFallbacks.increment();
            action.run();
        }
    }

    /** 返回后台执行器等待队列长度。 */
    private int queuedTasks(ExecutorService executor) {
        return executor instanceof ThreadPoolExecutor pool ? pool.getQueue().size() : 0;
    }

    /** 限制每个分片的在途操作，等待预算耗尽时明确失败。 */
    private <T> T withShardPermit(String shardId, Supplier<T> action) {
        Semaphore permits = shardPermits.get(shardId);
        if (permits == null) {
            adapter(shardId);
            throw new IllegalStateException("missing shard budget");
        }
        boolean acquired = false;
        try {
            acquired = permits.tryAcquire(options.limits().timeoutMillis(), TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "shard concurrency budget exhausted: " + shardId,
                        true, null);
            }
            return action.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "interrupted while waiting for shard budget",
                    false, null, ex);
        } finally {
            if (acquired) {
                permits.release();
            }
        }
    }

    /** 在首个持久化写入之前登记完整参与者，保证进程崩溃后可以定位所有 intent。 */
    private void registerWriteParticipants(Transaction tx) {
        metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            if (!tx.metadataPersisted()) {
                createBeginMetadata(tx.txId(), tx.startTs(), tx.isolationLevel());
                tx.metadataPersisted(true);
            }
            TxRecord record = metadataStore.registerParticipants(tx.txId(), tx.participants());
            if (record.status() != TxStatus.ACTIVE && record.status() != TxStatus.PREPARING) {
                throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "write registration rejected for terminal transaction",
                        false, tx.txId());
            }
            return null;
        });
    }

    /** 校验事务与语句归属，保持 RR 和跨分片 SQL 的固定快照。 */
    private void validateStatement(Transaction tx, StatementReadSnapshot snapshot) {
        ensureActive(tx);
        ensureLeaseValid(tx);
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.txId() != tx.txId()
                || (tx.isolationLevel() == IsolationLevel.REPEATABLE_READ && snapshot.readTs() != tx.startTs())) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "statement snapshot does not belong to transaction",
                    false, tx.txId());
        }
        ensureSnapshotVisible(snapshot.readTs(), tx.txId());
    }

    /** 拒绝租约过期的事务；查询完成后也必须检查，避免返回过期 GC 期间读取的结果。 */
    private void ensureLeaseValid(Transaction tx) {
        Optional<Long> registeredExpiry = metadataStore.snapshotExpiry(tx.txId());
        if (registeredExpiry.isEmpty() || registeredExpiry.get() != tx.deadlineMillis()
                || metadataStore.currentTimeMillis() >= tx.deadlineMillis()) {
            throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT, "transaction lease expired", false, tx.txId());
        }
    }

    /** 管理器关闭后拒绝接收新事务及新业务操作。 */
    private void ensureOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction manager closed", false, null);
        }
    }

    /** 包括回滚在内的事务操作都必须来自创建该事务的管理器。 */
    private void ensureOwned(Transaction tx) {
        Objects.requireNonNull(tx, "tx");
        if (!tx.ownedBy(transactionOwner)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction belongs to another manager", false, tx.txId());
        }
    }

    /** 释放到期事务的本地名额，持久化租约及事务由恢复任务处理。 */
    private void expireLocalTransactions() {
        long now = metadataStore.currentTimeMillis();
        activeTransactions.forEach((txId, tx) -> {
            if (tx.deadlineMillis() <= now && activeTransactions.remove(txId, tx)) {
                transactionPermits.release();
            }
        });
    }

    /** 将已决议事务标记为完成，并按物化结果记录恢复进度。 */
    private void completeTransaction(Transaction tx, boolean finalized) {
        tx.status(TxStatus.COMMITTED);
        if (finalized) {
            markFinalizedQuietly(tx);
        }
        releaseTransaction(tx);
    }

    /** 释放快照和本地资源；释放失败时租约到期仍可回收。 */
    private void releaseTransaction(Transaction tx) {
        try {
            metadataStore.withPublicationLock(() -> {
                metadataStore.releaseSnapshot(tx.txId());
                return null;
            });
        } catch (RuntimeException ex) {
            backgroundFailures.increment();
        } finally {
            if (activeTransactions.remove(tx.txId(), tx)) {
                transactionPermits.release();
            }
        }
    }

    /** 记录已完成物化；失败时留给恢复重放，不能将成功提交改判为业务失败。 */
    private void markFinalizedQuietly(Transaction tx) {
        if (!tx.metadataPersisted() || (options.deferIntentCleanup() && tx.status() == TxStatus.COMMITTED)) {
            return;
        }
        try {
            metadataStore.markFinalized(tx.txId());
        } catch (RuntimeException ex) {
            backgroundFailures.increment();
        }
    }

    /** 构造不可直接重跑业务的提交结果未知异常。 */
    private MvccException unknownCommit(Transaction tx, RuntimeException cause) {
        if (tx.status() != TxStatus.COMMITTED) {
            tx.status(TxStatus.PREPARING);
        }
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "resolve transaction decision before retrying business: " + tx.txId(), false, tx.txId(), cause);
    }

    /** 按 MySQL VARBINARY 使用的 UTF-8 无符号字节顺序比较键。 */
    private static int compareKeys(String left, String right) {
        return mvcc.storage.Utf8KeyOrder.compare(left, right);
    }

    /** 在分片预算内执行已提交 intent 清理。 */
    private void cleanupParticipantIntents(Transaction tx, String shardId) {
        withShardPermit(shardId, () -> {
            adapter(shardId).cleanupIntents(tx.txId());
            return null;
        });
    }

    /** 显式准备也使用同一分片并发预算，不能绕过前台流量限制。 */
    private void prepareParticipant(Transaction tx, String shardId) {
        withShardPermit(shardId, () -> {
            ensureLeaseValid(tx);
            adapter(shardId).prepare(tx.txId());
            return null;
        });
    }

    /** 判断当前是否启用事务阶段指标。 */
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
                    participant -> prepareParticipant(tx, participant),
                    options.crossShardCommitPolicy().parallelPrepare()
            );
            return;
        }
        recordMeasuredStage(
                TransactionStage.PREPARE,
                () -> runAcrossParticipants(
                        tx.participants(),
                        participant -> prepareParticipant(tx, participant),
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
        TxStatus decision = metadataStore.withPublicationLock(() -> {
            if (!tx.metadataPersisted()) {
                return TxStatus.ABORTED;
            }
            TxRecord record = metadataStore.get(tx.txId()).orElseThrow(() ->
                    unknownCommit(tx, new IllegalStateException("transaction metadata missing")));
            if (record.status() == TxStatus.COMMITTED) {
                return TxStatus.COMMITTED;
            }
            if (tx.decisionStarted() && tx.participants().size() == 1
                    && adapter(tx.participants().iterator().next()).supportsAtomicDirectCommit()) {
                String shardId = tx.participants().iterator().next();
                // 先在参与者侧隔离可能迟到的本地提交，再根据实际本地终态更新中心。
                var local = withShardPermit(shardId, () -> {
                    adapter(shardId).abort(tx.txId());
                    return adapter(shardId).participantRecord(tx.txId());
                });
                if (local.isEmpty() || local.get().txId() != tx.txId() || local.get().startTs() != tx.startTs()
                        || (local.get().status() != TxStatus.COMMITTED && local.get().status() != TxStatus.ABORTED)) {
                    throw unknownCommit(tx, new IllegalStateException("participant fencing did not establish a terminal decision"));
                }
                if (local.get().status() == TxStatus.COMMITTED) {
                    if (local.get().commitTs() == null) {
                        throw unknownCommit(tx, new IllegalStateException("participant commit timestamp missing"));
                    }
                    metadataStore.markSingleShardCommitted(tx.txId(), shardId, local.get().commitTs());
                    return TxStatus.COMMITTED;
                }
            }
            // markAborted 返回的终态才是权威，不能根据调用前读到的状态删除 intent。
            return metadataStore.markAborted(tx.txId()).status();
        });
        tx.status(decision);
        if (decision == TxStatus.COMMITTED) {
            releaseTransaction(tx);
            return;
        }
        runAcrossParticipants(
                tx.participants(),
                participant -> withShardPermit(participant, () -> {
                    adapter(participant).abort(tx.txId());
                    return null;
                }),
                options.crossShardCommitPolicy().parallelRollback()
        );
        markFinalizedQuietly(tx);
    }

    /**
     * 按需记录 intent 清理阶段。
     */
    private void cleanupIntentsStage(Transaction tx, String shardId) {
        if (!metricsEnabled()) {
            cleanupParticipantIntents(tx, shardId);
            return;
        }
        recordMeasuredStage(TransactionStage.INTENT_CLEANUP, () -> cleanupParticipantIntents(tx, shardId));
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

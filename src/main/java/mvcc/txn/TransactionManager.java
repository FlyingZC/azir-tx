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

public final class TransactionManager implements AutoCloseable {
    private final TimestampOracle timestampOracle;
    private final TransactionMetadataStore metadataStore;
    private final SafePointStore safePointStore;
    private final Map<String, StorageAdapter> adapters;
    private final TransactionOptions options;
    private final ExecutorService participantExecutor;
    private final ExecutorService asyncCommitExecutor;
    private final ExecutorService intentCleanupExecutor;

    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(), adapters);
    }

    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, TransactionOptions.defaults(), adapters);
    }

    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters);
    }

    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this.timestampOracle = timestampOracle;
        this.metadataStore = metadataStore;
        this.safePointStore = safePointStore;
        this.options = options;
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

    public void put(Transaction tx, String shardId, String key, byte[] value) {
        write(tx, shardId, Mutation.put(key, value));
    }

    public void delete(Transaction tx, String shardId, String key) {
        write(tx, shardId, Mutation.delete(key));
    }

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

    public TransactionCommitResult commit(Transaction tx) {
        ensureActive(tx);
        if (tx.participants().isEmpty()) {
            long commitTs = timestampOracle.nextTimestamp();
            if (tx.metadataPersisted()) {
                metadataStore.markCommitted(tx.txId(), commitTs);
            } else if (!options.lazyBeginMetadata()) {
                metadataStore.markCommitted(tx.txId(), commitTs);
            }
            tx.status(TxStatus.COMMITTED);
            return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
        }

        if (tx.participants().size() == 1) {
            String participant = tx.participants().iterator().next();
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

        prewriteBufferedMutations(tx);

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
            commitParticipant(tx, primaryParticipant, commitTs);
            Set<String> secondaryParticipants = secondaryParticipants(tx.participants(), primaryParticipant);
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncSecondaryCommit(tx, commitTs, secondaryParticipants);
            } else {
                runAcrossParticipants(
                        secondaryParticipants,
                        participant -> commitParticipant(tx, participant, commitTs),
                        options.crossShardCommitPolicy().parallelCommit()
                );
            }
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

    @Override
    public void close() {
        shutdownExecutor(intentCleanupExecutor);
        shutdownExecutor(asyncCommitExecutor);
        shutdownExecutor(participantExecutor);
    }

    private void scheduleAsyncSecondaryCommit(Transaction tx, long commitTs, Set<String> secondaryParticipants) {
        if (secondaryParticipants.isEmpty()) {
            return;
        }
        for (String participant : secondaryParticipants) {
            asyncCommitExecutor.submit(() -> {
                try {
                    commitParticipant(tx, participant, commitTs);
                } catch (RuntimeException ignored) {
                }
            });
        }
    }

    private String primaryParticipant(Set<String> participants) {
        return participants.stream()
                .sorted()
                .findFirst()
                .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "cross-shard tx has no participants", false, null));
    }

    private Set<String> secondaryParticipants(Set<String> participants, String primaryParticipant) {
        return participants.stream()
                .filter(participant -> !participant.equals(primaryParticipant))
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }

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

    public Optional<TxRecord> getRecord(long txId) {
        return metadataStore.get(txId);
    }

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

    private void ensureActive(Transaction tx) {
        if (tx.status() != TxStatus.ACTIVE) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "transaction not active: " + tx.txId(), false, tx.txId());
        }
    }

    private long readTs(Transaction tx) {
        return tx.isolationLevel() == IsolationLevel.REPEATABLE_READ
                ? tx.startTs()
                : timestampOracle.nextTimestamp();
    }

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

    private StorageAdapter adapter(String shardId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "unknown shard: " + shardId, false, null);
        }
        return adapter;
    }

    private boolean requiresExplicitPreparePhase(Set<String> participants) {
        for (String participant : participants) {
            if (adapter(participant).requiresExplicitPreparePhase()) {
                return true;
            }
        }
        return false;
    }

    private boolean shouldWaitOnConflict(MvccException ex, LockWaitPolicy lockWaitPolicy) {
        return lockWaitPolicy.conflictMode() == ConflictMode.WAIT
                && TransientStorageConflictDetector.isTransientConflict(ex);
    }

    private long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }

    private void safeRollback(Transaction tx) {
        try {
            rollback(tx);
        } catch (RuntimeException ignored) {
        }
    }

    private void runAcrossParticipants(Set<String> participants, Consumer<String> operation, boolean parallel) {
        if (!parallel || participants.size() <= 1) {
            for (String participant : participants) {
                operation.accept(participant);
            }
            return;
        }

        ConcurrentLinkedQueue<RuntimeException> failures = new ConcurrentLinkedQueue<>();
        CompletableFuture<?>[] futures = participants.stream()
                .map(participant -> CompletableFuture.runAsync(() -> {
                    try {
                        operation.accept(participant);
                    } catch (RuntimeException ex) {
                        failures.add(ex);
                        throw ex;
                    }
                }, participantExecutor))
                .toArray(CompletableFuture[]::new);

        try {
            CompletableFuture.allOf(futures).join();
        } catch (CompletionException ignored) {
        }

        RuntimeException failure = failures.peek();
        if (failure != null) {
            throw failure;
        }
    }

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
                long delayMillis = Math.min(lockWaitPolicy.nextDelayMillis(attempt++), remainingMillis(deadlineNanos));
                sleepQuietly(delayMillis);
            }
        }
    }

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

    private boolean requiresParallelExecutor(CrossShardCommitPolicy policy) {
        return policy.parallelPrewrite()
                || policy.parallelPrepare()
                || policy.parallelCommit()
                || policy.parallelRollback()
                || policy.asyncCommit();
    }

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

    private ThreadFactory namedFactory(String prefix) {
        return Thread.ofPlatform()
                .daemon(true)
                .name(prefix, 0)
                .factory();
    }

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

    private void directCommitSingleShardParticipant(Transaction tx, String shardId, long commitTs) {
        List<Mutation> bufferedMutations = tx.bufferedMutations(shardId);
        if (options.bufferWrites() && !bufferedMutations.isEmpty()) {
            adapter(shardId).directCommit(tx.txId(), tx.startTs(), commitTs, bufferedMutations);
            return;
        }
        commitParticipant(tx, shardId, commitTs);
    }
}

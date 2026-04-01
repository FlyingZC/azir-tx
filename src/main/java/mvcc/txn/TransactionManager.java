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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class TransactionManager {
    private final TimestampOracle timestampOracle;
    private final TransactionMetadataStore metadataStore;
    private final SafePointStore safePointStore;
    private final Map<String, StorageAdapter> adapters;
    private final TransactionOptions options;

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
    }

    public Transaction begin(IsolationLevel isolationLevel) {
        long txId = timestampOracle.nextTimestamp();
        long startTs = timestampOracle.nextTimestamp();
        metadataStore.create(txId, startTs, isolationLevel);
        return new Transaction(txId, startTs, isolationLevel);
    }

    public Optional<byte[]> get(Transaction tx, String shardId, String key) {
        ensureActive(tx);
        StorageAdapter adapter = adapter(shardId);
        long readTs = readTs(tx);
        ensureSnapshotVisible(readTs, tx.txId());
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
        return adapter.scan(new ReadContext(tx.txId(), readTs, tx.isolationLevel()), new KeyRange(startKeyInclusive, endKeyExclusive));
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
            metadataStore.markCommitted(tx.txId(), commitTs);
            tx.status(TxStatus.COMMITTED);
            return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
        }

        if (tx.participants().size() == 1) {
            String participant = tx.participants().iterator().next();
            long commitTs = timestampOracle.nextTimestamp();
            metadataStore.markSingleShardCommitted(tx.txId(), participant, commitTs);
            try {
                adapter(participant).commit(tx.txId(), commitTs);
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

        metadataStore.markPreparing(tx.txId(), tx.participants());
        TransactionCommitMode mode = tx.participants().size() == 1
                ? TransactionCommitMode.SINGLE_SHARD_FAST_PATH
                : TransactionCommitMode.TWO_PHASE_COMMIT;

        if (mode == TransactionCommitMode.TWO_PHASE_COMMIT) {
            runAcrossParticipants(
                    tx.participants(),
                    participant -> adapter(participant).prepare(tx.txId()),
                    options.crossShardCommitPolicy().parallelPrepare()
            );
        }

        long commitTs = timestampOracle.nextTimestamp();
        metadataStore.markCommitted(tx.txId(), commitTs);
        try {
            runAcrossParticipants(
                    tx.participants(),
                    participant -> adapter(participant).commit(tx.txId(), commitTs),
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
        return new TransactionCommitResult(tx.txId(), commitTs, mode);
    }

    public void rollback(Transaction tx) {
        if (tx.status() == TxStatus.ABORTED || tx.status() == TxStatus.COMMITTED) {
            return;
        }
        metadataStore.markAborted(tx.txId());
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

    private boolean shouldWaitOnConflict(MvccException ex, LockWaitPolicy lockWaitPolicy) {
        return lockWaitPolicy.conflictMode() == ConflictMode.WAIT
                && ex.errorCode() == ErrorCode.LOCK_CONFLICT
                && ex.retryable();
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
                }))
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
}

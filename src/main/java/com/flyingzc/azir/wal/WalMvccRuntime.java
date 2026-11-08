/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.storage.Utf8KeyOrder;
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

/**
 * Uses complete write sets in the authoritative WAL to publish atomic transaction decisions and materialize MySQL versions.
 */
public final class WalMvccRuntime implements AutoCloseable {

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

    /**
     * Creates an independent run with only WAL and memory to facilitate the first verify transaction kernel.
     *
     * @param directory directory
     * @param shards shards
     * @param options options
     * @return the operation result
     */
    public static WalMvccRuntime open(final Path directory, final Set<String> shards, final WalOptions options) {
        return openForTest(directory, shards, Map.of(), options, stage -> {
        });
    }

    /**
     * When creating an independent run with a walk materialization storage, the life cycle of the data source is managed by the caller.
     *
     * @param directory directory
     * @param stores stores
     * @param options options
     * @return the operation result
     */
    public static WalMvccRuntime open(final Path directory, final Map<String, WalMaterializedStore> stores, final WalOptions options) {
        return openForTest(directory, stores.keySet(), stores, options, stage -> {
        });
    }

    /**
     * Creates a pure local mode with a visible collection policy, the receipt expiry must be chosen by the caller.
     *
     * @param directory directory
     * @param shards shards
     * @param options options
     * @param retention retention
     * @return the operation result
     */
    public static WalMvccRuntime open(final Path directory, final Set<String> shards, final WalOptions options, final WalRetentionOptions retention) {
        return openForTest(directory, shards, Map.of(), options, stage -> {
        }, WalLog.NORMAL_IO, System::nanoTime, retention, System::currentTimeMillis);
    }

    /**
     * Creates an materialization mode with a visible collection policy, keeping the original Azir configuration unchanged.
     *
     * @param directory directory
     * @param stores stores
     * @param options options
     * @param retention retention
     * @return the operation result
     */
    public static WalMvccRuntime open(final Path directory, final Map<String, WalMaterializedStore> stores, final WalOptions options, final WalRetentionOptions retention) {
        return openForTest(directory, stores.keySet(), stores, options, stage -> {
        }, WalLog.NORMAL_IO, System::nanoTime, retention, System::currentTimeMillis);
    }

    /**
     * A visible copy of the kernel is created, only a trusted consensus port can be installed with commit batch and cannot automatically degrade to local commit.
     *
     * @param directory directory
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param retention retention
     * @param identity identity
     * @param authority authority
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static WalMvccRuntime openReplica(final Path directory, final Set<String> shards, final Map<String, WalMaterializedStore> stores,
                                             final WalOptions options, final WalRetentionOptions retention, final WalReplicaIdentity identity, final WalCommitAuthority authority) {
        Objects.requireNonNull(identity);
        Objects.requireNonNull(authority);
        if (retention.receiptRetentionMillis() != 0 || stores.values().stream().anyMatch(store -> !store.supportsReplicaOwnership())) {
            throw new IllegalArgumentException("replica replay requires retained receipts and owned backends");
        }
        try {
            return new WalMvccRuntime(directory, shards, stores, options, stage -> {
            }, WalLog.NORMAL_IO, System::nanoTime, retention, System::currentTimeMillis, identity, authority);
        } catch (final IOException failure) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "open replica failed", null, failure);
        }
    }

    /**
     * The failure injection portal will be installed to maintain normal public API not to expose the internal state of the protocol.
     *
     * @param directory directory
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param hook hook
     * @return the operation result
     */
    static WalMvccRuntime openForTest(final Path directory, final Set<String> shards,
                                      final Map<String, WalMaterializedStore> stores, final WalOptions options, final FaultHook hook) {
        return openForTest(directory, shards, stores, options, hook, WalLog.NORMAL_IO, System::nanoTime);
    }

    /**
     * Maintains the system clock for storage failure and lease acceptance injection of I/O boundaries and monotonic clock, open access.
     *
     * @param directory directory
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param hook hook
     * @param ioHook io hook
     * @param monotonicClock monotonic clock
     * @return the operation result
     */
    static WalMvccRuntime openForTest(final Path directory, final Set<String> shards,
                                      final Map<String, WalMaterializedStore> stores, final WalOptions options, final FaultHook hook,
                                      final WalLog.IoHook ioHook, final java.util.function.LongSupplier monotonicClock) {
        return openForTest(directory, shards, stores, options, hook, ioHook, monotonicClock, WalRetentionOptions.preserveAll(), System::currentTimeMillis);
    }

    /**
     * Injecting the saved window clock separately from the lease clock, system time changes cannot change transaction lease.
     *
     * @param directory directory
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param hook hook
     * @param ioHook io hook
     * @param monotonicClock monotonic clock
     * @param retention retention
     * @param wallClock wall clock
     * @return the operation result
     */
    static WalMvccRuntime openForTest(final Path directory, final Set<String> shards, final Map<String, WalMaterializedStore> stores,
                                      final WalOptions options, final FaultHook hook, final WalLog.IoHook ioHook,
                                      final java.util.function.LongSupplier monotonicClock, final WalRetentionOptions retention,
                                      final java.util.function.LongSupplier wallClock) {
        try {
            return new WalMvccRuntime(directory, shards, stores, options, hook, ioHook, monotonicClock, retention, wallClock);
        } catch (final IOException ex) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "open WAL runtime failed", null, ex);
        }
    }

    /**
     * Recovery log, check backend prefix, then open transaction and background threads.
     *
     * @param directory directory
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param hook hook
     * @param ioHook io hook
     * @param monotonicClock monotonic clock
     * @param retention retention
     * @param wallClock wall clock
     * @throws IOException if the operation cannot complete
     */
    private WalMvccRuntime(final Path directory, final Set<String> shards, final Map<String, WalMaterializedStore> stores,
                           final WalOptions options, final FaultHook hook, final WalLog.IoHook ioHook,
                           final java.util.function.LongSupplier monotonicClock, final WalRetentionOptions retention,
                           final java.util.function.LongSupplier wallClock) throws IOException {
        this(directory, shards, stores, options, hook, ioHook, monotonicClock, retention, wallClock, null, null);
    }

    /**
     * Check mode and replica backend identity, then open local recovery status, without effective leadership until the Consensus Layer is launched.
     *
     * @param directory directory
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param hook hook
     * @param ioHook io hook
     * @param monotonicClock monotonic clock
     * @param retention retention
     * @param wallClock wall clock
     * @param replica replica
     * @param authority authority
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private WalMvccRuntime(final Path directory, final Set<String> shards, final Map<String, WalMaterializedStore> stores,
                           final WalOptions options, final FaultHook hook, final WalLog.IoHook ioHook,
                           final java.util.function.LongSupplier monotonicClock, final WalRetentionOptions retention,
                           final java.util.function.LongSupplier wallClock, final WalReplicaIdentity replica, final WalCommitAuthority authority) throws IOException {
        this.shards = Set.copyOf(shards);
        this.stores = Map.copyOf(stores);
        this.options = Objects.requireNonNull(options);
        this.retention = Objects.requireNonNull(retention);
        this.replica = replica;
        this.authority = authority;
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
            if (image != null && !image.storedShards().equals(this.stores.keySet())) {
                throw new IOException("checkpoint storage routes changed");
            }
            for (var entry : this.stores.entrySet()) {
                if (replica == null) {
                    entry.getValue().initialize(log.engineId(), entry.getKey());
                } else {
                    entry.getValue().initializeReplica(replica, entry.getKey());
                }
                WalCheckpoint checkpoint = entry.getValue().checkpoint();
                validateCheckpoint(checkpoint);
                checkpoints.put(entry.getKey(), checkpoint);
            }
            if (image != null) {
                published = image.sequence();
                safePoint = image.safePoint();
                for (var version : image.versions()) {
                    restoreVersion(version);
                    hook.hit(Stage.DURING_RECOVERY);
                }
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
        } catch (final Throwable failure) {
            log.close();
            throw failure;
        }
        writer = Thread.ofVirtual().name("azir-wal-commit").start(this::writeLoop);
        materializer = Thread.ofVirtual().name("azir-wal-materializer").start(this::flushLoop);
    }

    /**
     * The old process handle cannot continue to be used when new operations are being started with the current watermark registration transaction.
     *
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public WalTransaction begin(final IsolationLevel isolation) {
        long epoch = readBarrier();
        synchronized (gate) {
            requireOpen();
            requireEpoch(epoch);
            if (backupRunning) {
                throw error(ErrorCode.RESOURCE_EXHAUSTED, "backup maintenance pauses new transactions", null, null);
            }
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

    /**
     * Get shared RC or RR snapshot for the entire statement.
     *
     * @param tx transaction context
     * @return the operation result
     */
    public WalReadSnapshot beginReadStatement(final WalTransaction tx) {
        long epoch = readBarrier();
        synchronized (gate) {
            requireActive(tx);
            requireEpoch(epoch);
            return new WalReadSnapshot(tx, tx.isolation == IsolationLevel.REPEATABLE_READ ? tx.start : published);
        }
    }

    /**
     * Write local buffer, all external byte arrays are copied.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     */
    public void put(final WalTransaction tx, final String shard, final String key, final byte[] value) {
        buffer(tx, new WalMutation(shard, key, Objects.requireNonNull(value, "value")));
    }

    /**
     * Delete in local tombstone, without changing any already commit version immediately.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     */
    public void delete(final WalTransaction tx, final String shard, final String key) {
        buffer(tx, new WalMutation(shard, key, null));
    }

    /**
     * Check transaction and global memory budget before changing write set.
     *
     * @param tx transaction context
     * @param mutation mutation
     */
    private void buffer(final WalTransaction tx, final WalMutation mutation) {
        synchronized (gate) {
            requireActive(tx);
            if (backupRunning) {
                throw error(ErrorCode.RESOURCE_EXHAUSTED, "backup maintenance pauses writes", tx, null);
            }
            if (readOnly) {
                throw error(ErrorCode.RESOURCE_EXHAUSTED, "runtime is read-only", tx, null);
            }
            requireShard(mutation.shard());
            if (stores.containsKey(mutation.shard())) {
                try {
                    stores.get(mutation.shard()).validateMutation(mutation);
                } catch (final WalMvccException rejected) {
                    throw error(rejected.errorCode(), rejected.getMessage(), tx, rejected);
                }
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

    /**
     * Read a key with a new statement snapshot.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     * @return the operation result
     */
    public Optional<byte[]> get(final WalTransaction tx, final String shard, final String key) {
        return get(tx, shard, key, beginReadStatement(tx));
    }

    /**
     * Merge local, memory with materialization version on a fixed statement snapshot; lock overleaf reading and phase-out staggered.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     * @param snapshot snapshot
     * @return the operation result
     */
    public Optional<byte[]> get(final WalTransaction tx, final String shard, final String key, final WalReadSnapshot snapshot) {
        synchronized (gate) {
            validateSnapshot(tx, snapshot);
            requireShard(shard);
            new WalMutation(shard, key, null);
            Key identity = new Key(shard, key);
            WalMutation own = tx.writes.get(identity);
            if (own != null) {
                return own.deleted() ? Optional.empty() : Optional.of(own.value());
            }
            WalValue visible = null;
            NavigableMap<Long, WalMutation> chain = versions.get(identity);
            var memory = chain == null ? null : chain.floorEntry(snapshot.sequence);
            if (memory != null) {
                visible = new WalValue(memory.getKey(), memory.getValue().value());
            }
            // Materialization hotspots can be phased out without materialization increment; old snapshot missing history is read from backend.
            if (visible != null) {
                cacheHits++;
                hotCache.get(identity);
            }
            if (visible == null && stores.containsKey(shard)) {
                cacheMisses++;
                visible = stores.get(shard).get(key, snapshot.sequence).orElse(null);
                // Only the latest materialization version of the current release is raised, and the old RR version cannot be considered a conflict fence.
                if (visible != null && snapshot.sequence == published) {
                    cacheMaterialized(identity, visible);
                }
            }
            validateSnapshot(tx, snapshot);
            return visible == null || visible.deleted() ? Optional.empty() : Optional.of(visible.value());
        }
    }

    /**
     * Use a new statement snapshot to perform semi-open scanning.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param from from
     * @param to to
     * @return the operation result
     */
    public NavigableMap<String, byte[]> scan(final WalTransaction tx, final String shard, final String from, final String to) {
        return scan(tx, shard, from, to, beginReadStatement(tx));
    }

    /**
     * Merges the same snapshot candidate tombstone with a number and byte budget limit.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param from from
     * @param to to
     * @param snapshot snapshot
     * @return the operation result
     */
    public NavigableMap<String, byte[]> scan(final WalTransaction tx, final String shard, final String from, final String to,
                                             final WalReadSnapshot snapshot) {
        synchronized (gate) {
            validateSnapshot(tx, snapshot);
            requireShard(shard);
            if (from != null) {
                new WalMutation(shard, from, null);
            }
            if (to != null) {
                new WalMutation(shard, to, null);
            }
            var rows = new TreeMap<String, WalValue>(Utf8KeyOrder::compare);
            if (stores.containsKey(shard)) {
                rows.putAll(stores.get(shard).scan(from, to, snapshot.sequence));
            }
            for (var entry : versions.entrySet()) {
                Key key = entry.getKey();
                if (!key.shard.equals(shard) || !inRange(key.key, from, to)) {
                    continue;
                }
                var version = entry.getValue().floorEntry(snapshot.sequence);
                if (version != null) {
                    putCandidate(rows, key.key, new WalValue(version.getKey(), version.getValue().value()));
                }
            }
            for (var entry : tx.writes.entrySet()) {
                if (entry.getKey().shard.equals(shard) && inRange(entry.getKey().key, from, to)) {
                    putCandidate(rows, entry.getKey().key, new WalValue(Long.MAX_VALUE, entry.getValue().value()));
                }
            }
            var result = new TreeMap<String, byte[]>(Utf8KeyOrder::compare);
            long bytes = 0;
            for (var entry : rows.entrySet()) {
                if (entry.getValue().deleted()) {
                    continue;
                }
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

    /**
     * The page break on the fixed statement snapshot; MVP uses a combination of candidates with budget and does not yet achieve current downward push.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param from from
     * @param to to
     * @param afterKey after key
     * @param limit maximum number of items
     * @param snapshot snapshot
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalScanPage scanPage(final WalTransaction tx, final String shard, final String from, final String to,
                                final String afterKey, final int limit, final WalReadSnapshot snapshot) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid page limit");
        }
        String lower = from;
        if (afterKey != null && (lower == null || Utf8KeyOrder.compare(afterKey, lower) > 0)) {
            lower = afterKey;
        }
        var all = scan(tx, shard, lower, to, snapshot);
        var page = new TreeMap<String, byte[]>(Utf8KeyOrder::compare);
        for (var entry : all.entrySet()) {
            if (afterKey != null && Utf8KeyOrder.compare(entry.getKey(), afterKey) <= 0) {
                continue;
            }
            if (page.size() == limit) {
                return new WalScanPage(page, page.lastKey());
            }
            page.put(entry.getKey(), entry.getValue());
        }
        return new WalScanPage(page, null);
    }

    /**
     * Limiting the number of keys during the candidate grouping cannot wait until you have distributed a full set of candidates before rejecting them.
     *
     * @param rows rows
     * @param key business key
     * @param value value to write
     */
    private static void putCandidate(final NavigableMap<String, WalValue> rows, final String key, final WalValue value) {
        if (!rows.containsKey(key) && rows.size() >= 10_000) {
            throw error(ErrorCode.RESOURCE_EXHAUSTED, "scan candidate budget exhausted", null, null);
        }
        rows.put(key, value);
    }

    /**
     * Freeze write set and wait for durable commit; timeout and break does not cancel already queued decision.
     *
     * @param tx transaction context
     * @return the operation result
     */
    public WalReceipt commit(final WalTransaction tx) {
        synchronized (gate) {
            requireOpen();
            requireOwner(tx);
            if (tx.state == WalTransaction.State.ABORTED) {
                throw error(ErrorCode.TRANSACTION_ABORTED, "transaction aborted", tx, null);
            }
            if (tx.state == WalTransaction.State.ACTIVE) {
                requireActive(tx);
                if (backupRunning && !tx.writes.isEmpty()) {
                    throw error(ErrorCode.RESOURCE_EXHAUSTED, "backup maintenance pauses commits", tx, null);
                }
                if (readOnly && !tx.writes.isEmpty()) {
                    throw error(ErrorCode.RESOURCE_EXHAUSTED, "runtime is read-only", tx, null);
                }
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
            if (tx.completion.isDone()) {
                return tx.completion.get();
            }
            return tx.completion.get(Math.max(1, tx.deadlineNanos - monotonicClock.getAsLong()), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit wait interrupted; query transaction id", tx, ex);
        } catch (final TimeoutException ex) {
            throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit wait expired; query transaction id", tx, ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit failed; query transaction id", tx, ex.getCause());
        }
    }

    /**
     * Rollback is the only transaction that has not entered decision, and commit and queue transaction cannot be emptied and disguised as aborting.
     *
     * @param tx transaction context
     */
    public void rollback(final WalTransaction tx) {
        synchronized (gate) {
            requireOpen();
            requireOwner(tx);
            if (tx.state == WalTransaction.State.COMMITTED || tx.state == WalTransaction.State.ABORTED) {
                return;
            }
            if (tx.state != WalTransaction.State.ACTIVE) {
                throw error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "decision already started", tx, null);
            }
            abortLocal(tx);
        }
    }

    /**
     * Only confirm durable commit issued; decision not published is waiting or recovery missing is not equal to rollback.
     *
     * @param transactionId transaction id
     * @return the operation result
     */
    public Optional<WalReceipt> findCommitted(final UUID transactionId) {
        readBarrier();
        synchronized (gate) {
            try {
                return log.receipt(Objects.requireNonNull(transactionId)).filter(receipt -> receipt.commitSequence() <= published);
            } catch (final IOException failure) {
                throw error(ErrorCode.STORAGE_UNAVAILABLE, "commit receipt lookup failed", null, failure);
            }
        }
    }

    /**
     * A clear distinction between lack of confirmation and search history is part collection, none of which can be considered a suspension.
     *
     * @param transactionId transaction id
     * @return the operation result
     */
    public WalCommitLookup lookupCommit(final UUID transactionId) {
        var receipt = findCommitted(transactionId);
        synchronized (gate) {
            long retired = log.retiredThrough();
            return new WalCommitLookup(receipt.isPresent() ? WalCommitLookup.Status.COMMITTED
                    : retired > 0 ? WalCommitLookup.Status.UNKNOWN_OR_EXPIRED : WalCommitLookup.Status.NOT_CONFIRMED,
                    receipt.orElse(null), retired);
        }
    }

    /**
     * Single writer collects limited batch and completes the queue request at normal close.
     */
    private void writeLoop() {
        try {
            while (fatal == null && (accepting || !queue.isEmpty())) {
                WalTransaction first = queue.poll(20, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                var batch = new ArrayList<WalTransaction>();
                batch.add(first);
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.batchDelayMillis());
                while (batch.size() < options.commitBatchSize()) {
                    WalTransaction next = queue.poll(Math.max(0, until - System.nanoTime()), TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                }
                commitBatch(batch);
            }
        } catch (final Throwable failure) {
            failStop(failure);
        }
    }

    /**
     * Serial validate conflict, first durable storage, then install all version s and release watermark.
     *
     * @param batch batch
     * @throws IOException if the operation cannot complete
     */
    private void commitBatch(final List<WalTransaction> batch) throws IOException {
        if (authority != null) {
            commitReplicatedBatch(batch);
            return;
        }
        synchronized (decisionGate) {
            if (fatal != null) {
                return;
            }
            commitBatchLocked(batch);
        }
    }

    /**
     * Single writer prepares a complete record at the current status and does not write local commit log until consensus has been reached on the reproduction mode.
     *
     * @param batch batch
     * @param epoch epoch
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private Prepared prepareBatchLocked(final List<WalTransaction> batch, final long epoch) throws IOException {
        var accepted = new ArrayList<WalTransaction>();
        var records = new ArrayList<WalCommit>();
        var reservations = new HashMap<Key, Long>();
        long frameBytes = 0;
        long extraIndexBytes = 0;
        synchronized (gate) {
            for (WalTransaction tx : batch) {
                if (tx.state != WalTransaction.State.QUEUED || active.get(tx.id) != tx) {
                    continue;
                }
                if (authority != null && (epoch <= 0 || tx.epoch != epoch || authority.epoch() != epoch || proposalBlocked)) {
                    abortLocal(tx);
                    tx.completion.completeExceptionally(error(ErrorCode.STORAGE_UNAVAILABLE, "replica leadership unavailable", tx, null));
                    continue;
                }
                boolean expired = monotonicClock.getAsLong() - tx.deadlineNanos >= 0;
                trimHotCache(tx.writes.values().stream().mapToLong(WalMvccRuntime::indexBytes).sum());
                boolean conflict = false;
                WalMvccException lookupFailure = null;
                if (!expired) {
                    try {
                        for (Key key : tx.writes.keySet()) {
                            long fence = reservations.containsKey(key) ? reservations.get(key) : latestSequence(key);
                            if (fence > tx.start) {
                                conflict = true;
                                break;
                            }
                        }
                    } catch (final RuntimeException failure) {
                        // This transaction cannot be explicitly rejected when the cold key cannot be verified without adding a log, and no conflict is defaulted.
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
                                                        ? error(ErrorCode.RESOURCE_EXHAUSTED, "version index budget exhausted", tx, null)
                                                        : null;
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
            if (authority != null && !records.isEmpty()) {
                pendingReplicaEpoch = epoch;
            }
        }
        return new Prepared(List.copyOf(accepted), List.copyOf(records), epoch);
    }

    /**
     * The local mode continues to be locked to complete the brushing and release, maintaining the original commit contract.
     *
     * @param batch batch
     * @throws IOException if the operation cannot complete
     */
    private void commitBatchLocked(final List<WalTransaction> batch) throws IOException {
        var prepared = prepareBatchLocked(batch, 0);
        var accepted = prepared.transactions();
        var records = prepared.records();
        if (records.isEmpty()) {
            return;
        }
        hook.hit(Stage.BEFORE_APPEND);
        try {
            log.append(records);
        } catch (final WalMvccException resource) {
            synchronized (gate) {
                pendingIndexBytes = 0;
                decisionKeys.clear();
                evictMaterialized();
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
            for (WalCommit record : records) {
                install(record, false);
            }
            published = records.getLast().sequence();
            for (WalTransaction tx : accepted) {
                releaseWrites(tx);
                active.remove(tx.id);
                tx.state = WalTransaction.State.COMMITTED;
            }
            decisionKeys.clear();
            evictMaterialized();
        }
        try {
            hook.hit(Stage.AFTER_PUBLISH);
        } catch (final Throwable failure) {
            for (WalTransaction tx : accepted) {
                tx.completion.completeExceptionally(error(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "commit persisted but response failed", tx, failure));
            }
            throw failure;
        }
        for (WalTransaction tx : accepted) {
            tx.completion.complete(log.receipt(tx.id).orElseThrow());
        }
    }

    /**
     * Prepares to separate from consensus waiting, and the status machine can be installed with an decision lock at another linear range and cannot form a back-to-death lock.
     *
     * @param batch batch
     * @throws IOException if the operation cannot complete
     */
    private void commitReplicatedBatch(final List<WalTransaction> batch) throws IOException {
        Prepared prepared;
        synchronized (decisionGate) {
            if (fatal != null) {
                return;
            }
            long epoch = authority.epoch();
            prepared = prepareBatchLocked(batch, epoch);
        }
        if (prepared.records().isEmpty()) {
            return;
        }
        try {
            authority.submit(prepared.epoch(), prepared.records()).toCompletableFuture().get(options.transactionTimeoutMillis(), TimeUnit.MILLISECONDS);
            synchronized (gate) {
                for (var tx : prepared.transactions()) {
                    if (tx.state != WalTransaction.State.COMMITTED) {
                        throw new IOException("authority returned before local committed application");
                    }
                }
            }
        } catch (final Exception failure) {
            synchronized (gate) {
                if (pendingReplicaEpoch == prepared.epoch() && authority.epoch() == prepared.epoch()) {
                    proposalBlocked = true;
                }
                for (var tx : prepared.transactions()) {
                    tx.completion.completeExceptionally(error(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "replicated commit unresolved; query UUID after recovery", tx, failure));
                }
            }
        }
    }

    /**
     * Only a trusted consensus port can be installed with commit records, all replica using the same UUID and business serial number.
     *
     * @param source database connection source
     * @param records records
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void applyCommitted(final WalCommitAuthority source, List<WalCommit> records) {
        if (authority == null || source != authority) {
            throw new IllegalArgumentException("committed application requires replica authority");
        }
        records = List.copyOf(records);
        if (records.isEmpty() || records.size() > 1024) {
            throw new IllegalArgumentException("invalid committed batch");
        }
        synchronized (decisionGate) {
            try {
                var fresh = new ArrayList<WalCommit>();
                var completed = new ArrayList<WalTransaction>();
                synchronized (gate) {
                    requireHealthy();
                    long servingEpoch = authority.epoch();
                    for (long oldEpoch : active.values().stream().mapToLong(tx -> tx.epoch).filter(epoch -> epoch != servingEpoch).distinct().toArray()) {
                        fenceEpochLocked(oldEpoch);
                    }
                    if (pendingReplicaEpoch != 0 && authority.epoch() != pendingReplicaEpoch) {
                        fenceEpochLocked(pendingReplicaEpoch);
                    }
                    long expected = published + 1;
                    long bytes = 0;
                    var newKeys = new java.util.HashSet<Key>();
                    long localBytes = 0;
                    for (var record : records) {
                        if (record.sequence() <= published) {
                            var old = log.receipt(record.transactionId()).orElseThrow(() -> new IOException("replica replay has no retained decision"));
                            if (old.commitSequence() != record.sequence() || !old.digest().equals(record.digest())) {
                                throw new IOException("replica replay decision mismatch");
                            }
                            continue;
                        }
                        if (record.sequence() != expected++) {
                            throw new IOException("replica committed sequence gap");
                        }
                        var local = active.get(record.transactionId());
                        if (local != null) {
                            var proposal = new WalCommit(local.id, record.sequence(), local.start, List.copyOf(local.writes.values()));
                            if (local.state != WalTransaction.State.QUEUED || !proposal.digest().equals(record.digest())) {
                                throw new IOException("committed proposal changed");
                            }
                            completed.add(local);
                            localBytes += local.writeBytes;
                        }
                        for (var mutation : record.mutations()) {
                            requireShard(mutation.shard());
                            var key = new Key(mutation.shard(), mutation.key());
                            bytes += mutation.retainedBytes();
                            if (!latest.containsKey(key) && newKeys.add(key)) {
                                bytes += indexBytes(mutation);
                            }
                        }
                        fresh.add(record);
                    }
                    trimHotCache(bytes);
                    if (retainedBytes + bufferedBytes - localBytes + bytes > options.maxMemoryBytes()) {
                        throw new IOException("committed replica exceeds memory budget");
                    }
                }
                if (fresh.isEmpty()) {
                    return;
                }
                log.append(fresh);
                synchronized (gate) {
                    for (var record : fresh) {
                        install(record, false);
                    }
                    published = fresh.getLast().sequence();
                    for (var tx : completed) {
                        releaseWrites(tx);
                        active.remove(tx.id);
                        tx.state = WalTransaction.State.COMMITTED;
                    }
                    if (!completed.isEmpty()) {
                        pendingReplicaEpoch = 0;
                        pendingIndexBytes = 0;
                        decisionKeys.clear();
                        proposalBlocked = false;
                    }
                    evictMaterialized();
                }
                for (var tx : completed) {
                    tx.completion.complete(log.receipt(tx.id).orElseThrow());
                }
            } catch (final Throwable failure) {
                failStop(failure);
                throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica committed application failed", null, failure);
            }
        }
    }

    /**
     * Role change only clears the local handle of an old term; the result that has been copied is still unknown and not disguised as rollback.
     *
     * @param obsoleteEpoch obsolete epoch
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void fenceEpoch(final long obsoleteEpoch) {
        if (authority == null || obsoleteEpoch < 1) {
            throw new IllegalArgumentException("invalid replica fencing epoch");
        }
        synchronized (gate) {
            fenceEpochLocked(obsoleteEpoch);
        }
    }

    /**
     * Locked retirement old handle and budget, late character notification cannot clear transaction.
     *
     * @param epoch epoch
     */
    private void fenceEpochLocked(final long epoch) {
        for (var tx : List.copyOf(active.values())) {
            if (tx.epoch > epoch) {
                continue;
            }
            boolean queued = tx.state == WalTransaction.State.QUEUED;
            releaseWrites(tx);
            active.remove(tx.id);
            tx.state = queued ? WalTransaction.State.UNKNOWN : WalTransaction.State.ABORTED;
            tx.completion.completeExceptionally(error(queued ? ErrorCode.TRANSACTION_STATUS_UNKNOWN : ErrorCode.TRANSACTION_ABORTED, "replica leadership changed", tx, null));
        }
        queue.removeIf(tx -> tx.epoch <= epoch);
        if (pendingReplicaEpoch <= epoch) {
            pendingReplicaEpoch = 0;
            pendingIndexBytes = 0;
            decisionKeys.clear();
            proposalBlocked = false;
        }
    }

    /**
     * Majority read barrier executed outside the lock while running, failed to downgrade to local cache reading.
     *
     * @return the operation result
     */
    private long readBarrier() {
        if (authority == null) {
            return 0;
        }
        try {
            long epoch = authority.readBarrier(options.transactionTimeoutMillis());
            requireEpoch(epoch);
            return epoch;
        } catch (final Exception failure) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica read barrier unavailable", null, failure);
        }
    }

    /**
     * The term check does not do network I/O and maintains the local mutually exclusive relationship between the handle and the commit preparation phase.
     *
     * @param epoch epoch
     */
    private void requireEpoch(final long epoch) {
        if (authority != null && (epoch <= 0 || authority.epoch() != epoch || proposalBlocked)) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica epoch is not serving", null, null);
        }
    }

    /**
     * Materialization history and write conflict fence, recovery is installed without loading materialization full library data into memory.
     *
     * @param commit commit
     * @param recovery recovery
     * @throws IOException if the operation cannot complete
     */
    private void install(final WalCommit commit, final boolean recovery) throws IOException {
        for (WalMutation mutation : commit.mutations()) {
            if (!shards.contains(mutation.shard())) {
                throw new IOException("unknown shard in WAL");
            }
            long flushed = checkpoints.getOrDefault(mutation.shard(), new WalCheckpoint(0, "")).sequence();
            if (recovery && stores.containsKey(mutation.shard()) && commit.sequence() <= flushed) {
                hook.hit(Stage.DURING_RECOVERY);
                continue;
            }
            Key key = new Key(mutation.shard(), mutation.key());
            forgetHot(key);
            if (!latest.containsKey(key)) {
                retainedBytes += indexBytes(mutation);
                memoryGcKeys.add(key);
            }
            latest.put(key, commit.sequence());
            var chain = versions.computeIfAbsent(key, ignored -> new TreeMap<>());
            chain.put(commit.sequence(), mutation);
            retainedBytes += mutation.retainedBytes();
            // The history of the brush can be phased out, but keep the latest head version as a read cache to avoid every reading returning to MySQL.
            var retired = chain.headMap(Math.min(flushed, commit.sequence() - 1), true);
            for (WalMutation old : retired.values()) {
                retainedBytes -= old.retainedBytes();
            }
            retired.clear();
            hook.hit(recovery ? Stage.DURING_RECOVERY : Stage.DURING_INSTALL);
            if (recovery && retainedBytes > options.maxMemoryBytes()) {
                throw new IOException("recovery exceeds memory budget; restore with a larger budget");
            }
        }
    }

    /**
     * Cycle materialization limited log batch, failed to keep the log with memory version and retry.
     */
    private void flushLoop() {
        while (flushRunning && fatal == null) {
            try {
                flush();
                long now = System.nanoTime();
                if (now - nextCheckpointAttemptNanos >= 0 && (now - lastCheckpointNanos >= TimeUnit.MILLISECONDS.toNanos(retention.checkpointIntervalMillis())
                        || log.bytes() >= retention.checkpointWalBytes())) {
                    // Backend backward or archive budget less than time-limited retry cannot turn the resource back pressure into a busy cycle.
                    nextCheckpointAttemptNanos = now + TimeUnit.SECONDS.toNanos(1);
                    try {
                        checkpoint();
                        maintenanceFailure = null;
                    } catch (final RuntimeException failure) {
                        maintenanceFailure = failure.toString();
                    }
                }
                pauseMaterializer();
            } catch (final InterruptedException stop) {
                Thread.currentThread().interrupt();
                return;
            } catch (final IOException failure) {
                failStop(failure);
                return;
            } catch (final RuntimeException failure) {
                materializationFailure = failure.toString();
                try {
                    pauseMaterializer();
                } catch (final InterruptedException stop) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Pause writing to decision, first pacify all backends and then backup the local recovery status with cold data on the same border.
     *
     * @param destination destination
     * @param limits limits
     * @return the operation result
     */
    public WalBackup.Info backup(final Path destination, final WalBackupOptions limits) {
        if (replica != null) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "replica backup requires coordinated Raft snapshot", null, null);
        }
        closeGuard.lock();
        try {
            return backupLocked(destination, limits);
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Credible consensus assembly exports the complete replica in a practical application queue, and the position of Raft is bound separately by the assembly.
     *
     * @param source database connection source
     * @param destination destination
     * @param limits limits
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalBackup.Info backupReplica(final WalCommitAuthority source, final Path destination, final WalBackupOptions limits) {
        if (replica == null || source != authority) {
            throw new IllegalArgumentException("replica snapshot requires bound consensus authority");
        }
        closeGuard.lock();
        try {
            return backupLocked(destination, limits);
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Consensus assembly is prepared for call at the applied boundary, with backend view fixed to allow release and materialization to continue.
     *
     * @param source database connection source
     * @param destination destination
     * @param limits limits
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalReplicaBackup prepareReplicaBackup(final WalCommitAuthority source, final Path destination, final WalBackupOptions limits) {
        if (replica == null || source != authority) {
            throw new IllegalArgumentException("replica snapshot requires bound consensus authority");
        }
        closeGuard.lock();
        try {
            synchronized (decisionGate) {
                synchronized (flushGate) {
                    synchronized (gate) {
                        requireOpen();
                        if (backupRunning || replicaExportRunning || stores.values().stream().anyMatch(store -> !store.supportsPinnedSnapshot())) {
                            throw error(ErrorCode.STORAGE_UNAVAILABLE, "pinned replica backup unavailable", null, null);
                        }
                        replicaExportRunning = true;
                    }
                    try {
                        var budget = new WalBackup.Budget(limits);
                        WalBackup.checkDestination(destination, log.directory());
                        while (true) {
                            budget.check();
                            flush();
                            synchronized (gate) {
                                requireOpen();
                                if (checkpoints.values().stream().allMatch(checkpoint -> checkpoint.sequence() == published)) {
                                    break;
                                }
                            }
                        }
                        if (published > 0 && !checkpoint().created()) {
                            throw new IOException("replica backup checkpoint did not reach prefix");
                        }
                        var prepared = WalBackup.prepareReplica(destination, log, shards, stores, budget, hook::hit);
                        return new WalReplicaBackup(prepared, this::releaseReplicaExport);
                    } catch (final Throwable failure) {
                        releaseReplicaExport();
                        if (failure instanceof Error error) {
                            throw error;
                        }
                        throw error(ErrorCode.STORAGE_UNAVAILABLE, "prepare pinned replica backup failed", null, failure);
                    }
                }
            }
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Repatriation after completion or abandonment of export and a wake-up call for operations that are being safely closed.
     */
    private void releaseReplicaExport() {
        synchronized (gate) {
            replicaExportRunning = false;
            gate.notifyAll();
        }
    }

    /**
     * Snapshot is still readable when you close the line to maintain, but do not perform the whole backup in a running-time reading lock.
     *
     * @param destination destination
     * @param limits limits
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private WalBackup.Info backupLocked(final Path destination, final WalBackupOptions limits) {
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
                    budget.check();
                    WalBackup.checkDestination(destination, log.directory());
                    while (true) {
                        budget.check();
                        flush();
                        synchronized (gate) {
                            requireOpen();
                            if (checkpoints.values().stream().allMatch(checkpoint -> checkpoint.sequence() == published)) {
                                break;
                            }
                        }
                    }
                    if (published > 0 && !checkpoint().created()) {
                        throw new IOException("backup checkpoint did not reach published prefix");
                    }
                    return WalBackup.create(destination, log, shards, stores, budget, hook::hit);
                } catch (final IOException failure) {
                    throw error(ErrorCode.STORAGE_UNAVAILABLE, "consistent backup failed", null, failure);
                } finally {
                    synchronized (gate) {
                        backupRunning = false;
                        gate.notifyAll();
                    }
                }
            }
        }
    }

    /**
     * Pause commit decision and do the last round of bounded materialization, complete durable storage before atomic turns and deletes the old log.
     *
     * @return the operation result
     */
    public CheckpointResult checkpoint() {
        synchronized (decisionGate) {
            synchronized (flushGate) {
                synchronized (gate) {
                    requireOpen();
                }
                flush();
                long cut;
                long safe;
                long before;
                List<WalCheckpointState.Version> saved = new ArrayList<>();
                synchronized (gate) {
                    requireOpen();
                    cut = published;
                    safe = safePoint;
                    try {
                        before = log.bytes();
                    } catch (final IOException failure) {
                        throw error(ErrorCode.STORAGE_UNAVAILABLE, "inspect checkpoint WAL", null, failure);
                    }
                    if (cut == 0 || checkpoints.values().stream().anyMatch(cp -> cp.sequence() != cut)) {
                        return new CheckpointResult(false, cut, before, before);
                    }
                    versions.forEach((key, chain) -> {
                        // Backends are pursued cut, cold data recovery is assumed by the identity and summary bound backends. Hotspot caches are not authority states.
                        if (!stores.containsKey(key.shard)) {
                            chain.forEach((sequence, mutation) -> saved.add(new WalCheckpointState.Version(sequence, mutation)));
                        }
                    });
                }
                try {
                    // The commit decision is still suspended without reading locks during mirror creation; reader and local write set continue to be protected by the original rules.
                    log.checkpoint(shards, stores.keySet(), safe, saved, Math.max(0, wallClock.getAsLong()), hook::hit);
                    lastCheckpointNanos = System.nanoTime();
                    maintenanceFailure = null;
                    return new CheckpointResult(true, cut, before, log.bytes());
                } catch (final WalMvccException resource) {
                    if (resource.errorCode() == ErrorCode.RESOURCE_EXHAUSTED) {
                        throw resource;
                    }
                    failStop(resource);
                    throw resource;
                } catch (final Throwable failure) {
                    failStop(failure);
                    throw error(ErrorCode.STORAGE_UNAVAILABLE, "checkpoint outcome requires recovery", null, failure);
                }
            }
        }
    }

    /**
     * Reloads verify mirror version memory, repeat key version cannot overwrite different serial numbers.
     *
     * @param version version
     * @throws IOException if the operation cannot complete
     */
    private void restoreVersion(final WalCheckpointState.Version version) throws IOException {
        var mutation = version.mutation();
        Key key = new Key(mutation.shard(), mutation.key());
        // Full mass cache compatible with old mirrors: the backend is over recovery baseline and no longer recovery these discardable caches.
        if (stores.containsKey(key.shard)) {
            return;
        }
        if (!latest.containsKey(key)) {
            retainedBytes += indexBytes(mutation);
            memoryGcKeys.add(key);
        }
        latest.merge(key, version.sequence(), Math::max);
        if (versions.computeIfAbsent(key, ignored -> new TreeMap<>()).put(version.sequence(), mutation) != null) {
            throw new IOException("duplicate checkpoint version");
        }
        retainedBytes += mutation.retainedBytes();
        if (retainedBytes > options.maxMemoryBytes()) {
            throw new IOException("checkpoint exceeds memory budget");
        }
    }

    /**
     * Use to wake up waiting to stop idle threads and do not damage inbound JDBC or I/O by interrupt.
     *
     * @throws InterruptedException if the operation cannot complete
     */
    private void pauseMaterializer() throws InterruptedException {
        synchronized (gate) {
            if (flushRunning && fatal == null) {
                gate.wait(options.flushIntervalMillis());
            }
        }
    }

    /**
     * Proactive implementation of a limited materialization round, which allows callers to contain backlogs or conduct definitive acceptances.
     *
     * @throws IOException if the operation cannot complete
     */
    public void flush() {
        synchronized (flushGate) {
            long upper;
            synchronized (gate) {
                requireHealthy();
                upper = published;
            }
            RuntimeException firstFailure = null;
            for (var entry : stores.entrySet()) {
                try {
                    synchronized (gate) {
                        if (checkpoints.get(entry.getKey()).sequence() >= upper) {
                            continue;
                        }
                    }
                    WalCheckpoint before = entry.getValue().checkpoint();
                    validateCheckpoint(before);
                    if (before.sequence() > upper) {
                        throw new IOException("backend ahead of published prefix");
                    }
                    int count = (int) Math.min(options.flushBatchSize(), upper - before.sequence());
                    if (count > 0) {
                        WalCheckpoint after = entry.getValue().apply(log.read(before.sequence() + 1, count));
                        validateCheckpoint(after);
                        if (after.sequence() < before.sequence() + count || after.sequence() > upper) {
                            throw new IOException("invalid materialization progress");
                        }
                        before = after;
                    }
                    synchronized (gate) {
                        checkpoints.put(entry.getKey(), before);
                        evictMaterialized();
                    }
                } catch (final Exception failure) {
                    if (firstFailure == null) {
                        firstFailure = error(ErrorCode.STORAGE_UNAVAILABLE, "materialization failed", null, failure);
                    }
                }
            }
            materializationFailure = firstFailure == null ? null : firstFailure.toString();
            if (firstFailure != null) {
                throw firstFailure;
            }
        }
    }

    /**
     * Retirement has been materialization history, and the full dropboard key is added to the bounded Hotspot Cache, old snapshot returned as required.
     */
    private void evictMaterialized() {
        var keys = versions.entrySet().iterator();
        while (keys.hasNext()) {
            var entry = keys.next();
            long frontier = checkpoints.getOrDefault(entry.getKey().shard, new WalCheckpoint(0, "")).sequence();
            var chain = entry.getValue().entrySet().iterator();
            while (chain.hasNext()) {
                var version = chain.next();
                if (version.getKey() > frontier || version.getKey().equals(latest.get(entry.getKey()))) {
                    break;
                }
                retainedBytes -= version.getValue().retainedBytes();
                chain.remove();
            }
            long head = entry.getValue().lastKey();
            if (stores.containsKey(entry.getKey().shard) && head <= frontier && !decisionKeys.contains(entry.getKey())
                    && !hotCache.containsKey(entry.getKey())) {
                long bytes = indexBytes(entry.getValue().lastEntry().getValue()) + entry.getValue().lastEntry().getValue().retainedBytes();
                hotCache.put(entry.getKey(), bytes);
                hotCacheBytes += bytes;
            }
        }
        trimHotCache(0);
    }

    /**
     * Check fence from durable index when cache is missing, no materialization key always has memory fence overwrited.
     *
     * @param key business key
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private long latestSequence(final Key key) {
        Long memory = latest.get(key);
        if (memory != null) {
            return memory;
        }
        var store = stores.get(key.shard);
        if (store == null) {
            return 0;
        }
        conflictStoreLookups++;
        long sequence = store.latestSequence(key.key);
        if (sequence < 0 || sequence > published) {
            throw new IllegalStateException("backend conflict fence outside published prefix");
        }
        return sequence;
    }

    /**
     * Raises the current cut-off cold reading to phase-out hotspots, which cannot take up a reservation memory in transit decision.
     *
     * @param key business key
     * @param value value to write
     */
    private void cacheMaterialized(final Key key, final WalValue value) {
        if (versions.containsKey(key) || decisionKeys.contains(key)
                || value.commitSequence() > checkpoints.get(key.shard).sequence()) {
            return;
        }
        var mutation = new WalMutation(key.shard, key.key, value.value());
        long bytes = indexBytes(mutation) + mutation.retainedBytes();
        if (bytes > options.maxHotCacheBytes()) {
            return;
        }
        trimHotCache(bytes);
        if (retainedBytes + bufferedBytes + pendingIndexBytes + bytes > options.maxMemoryBytes()) {
            return;
        }
        var chain = new TreeMap<Long, WalMutation>();
        chain.put(value.commitSequence(), mutation);
        versions.put(key, chain);
        latest.put(key, value.commitSequence());
        memoryGcKeys.add(key);
        retainedBytes += bytes;
        hotCache.put(key, bytes);
        hotCacheBytes += bytes;
        trimHotCache(0);
    }

    /**
     * Removes the cache class but retains version for cold hotspot conversion and version installation.
     *
     * @param key business key
     */
    private void forgetHot(final Key key) {
        Long bytes = hotCache.remove(key);
        if (bytes != null) {
            hotCacheBytes -= bytes;
        }
    }

    /**
     * Only complete materialization hot buttons will be phased out; the key used in transit commit must not disappear, otherwise index presets may be distorted.
     *
     * @param neededBytes needed bytes
     */
    private void trimHotCache(final long neededBytes) {
        var entries = hotCache.entrySet().iterator();
        while (entries.hasNext() && (hotCacheBytes > options.maxHotCacheBytes()
                || retainedBytes + bufferedBytes + pendingIndexBytes + neededBytes > options.maxMemoryBytes())) {
            var entry = entries.next();
            Key key = entry.getKey();
            if (decisionKeys.contains(key)) {
                continue;
            }
            hotCacheBytes -= entry.getValue();
            retainedBytes -= entry.getValue();
            entries.remove();
            versions.remove(key);
            latest.remove(key);
            memoryGcKeys.remove(key);
            cacheEvictions++;
        }
    }

    /**
     * The slowest materialization shard determines the number of commit s to be processed, and there is no back-end backlog budget for pure local mode.
     *
     * @return the operation result
     */
    private long materializationLag() {
        return published - checkpoints.values().stream().mapToLong(WalCheckpoint::sequence).min().orElse(published);
    }

    /**
     * Active transaction -protected advance safe point, with a historical anchor per key at the back end.
     *
     * @param keyLimit key limit
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public int collectGarbage(final int keyLimit) {
        if (keyLimit < 1 || keyLimit > 4096) {
            throw new IllegalArgumentException("invalid GC batch");
        }
        synchronized (gate) {
            requireOpen();
            expireIdle();
            long oldest = active.values().stream().mapToLong(tx -> tx.start).min().orElse(published);
            safePoint = Math.max(safePoint, oldest);
            int removed = 0;
            for (WalMaterializedStore store : stores.values()) {
                removed += store.gc(safePoint, keyLimit);
            }
            vacuumMemory(keyLimit);
            return removed;
        }
    }

    /**
     * Trust collection Press shard rotation, limiting each piece of work and avoiding slow shard chronic starvation of another piece.
     *
     * @param keyLimit key limit
     * @param rowLimit row limit
     * @param budgetMillis budget millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public WalMaterializedStore.GcProgress collectGarbageBounded(final int keyLimit, final int rowLimit, final long budgetMillis) {
        if (keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096 || budgetMillis < 1 || budgetMillis > 5000) {
            throw new IllegalArgumentException("invalid runtime maintenance limits");
        }
        synchronized (gate) {
            requireOpen();
            expireIdle();
            long oldest = active.values().stream().mapToLong(tx -> tx.start).min().orElse(published);
            safePoint = Math.max(safePoint, oldest);
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis);
            int keys = 0;
            int rows = 0;
            boolean wrapped = false;
            var routes = stores.keySet().stream().sorted().toList();
            for (int visited = 0; visited < routes.size() && keys < keyLimit && rows < rowLimit; visited++) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                String route = routes.get(gcStoreCursor % routes.size());
                gcStoreCursor = (gcStoreCursor + 1) % routes.size();
                var result = stores.get(route).gcBounded(safePoint, keyLimit - keys, rowLimit - rows,
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                if (result.inspectedKeys() > keyLimit - keys || result.removedRows() > rowLimit - rows) {
                    throw new IllegalStateException("backend exceeded maintenance budget");
                }
                keys += result.inspectedKeys();
                rows += result.removedRows();
                wrapped |= result.wrapped();
            }
            vacuumMemory(keyLimit, rowLimit);
            return new WalMaterializedStore.GcProgress(keys, rows, wrapped);
        }
    }

    /**
     * Rotation scanning limited memory keys to keep safe point anchors; tombstone must have materialization to phase out its conflict index.
     *
     * @param keyLimit key limit
     */
    private void vacuumMemory(final int keyLimit) {
        vacuumMemory(keyLimit, 4096);
    }

    /**
     * Memory version is also treated by the number limit of the deleted article; all older values must be removed before tombstone is deleted.
     *
     * @param keyLimit key limit
     * @param versionLimit version limit
     */
    private void vacuumMemory(final int keyLimit, final int versionLimit) {
        int count = Math.min(keyLimit, memoryGcKeys.size());
        int removed = 0;
        for (int i = 0; i < count; i++) {
            var iterator = memoryGcKeys.iterator();
            Key key = iterator.next();
            iterator.remove();
            if (decisionKeys.contains(key)) {
                memoryGcKeys.add(key);
                continue;
            }
            var chain = versions.get(key);
            if (chain == null) {
                continue;
            }
            var anchor = chain.floorKey(safePoint);
            var head = chain.lastEntry();
            boolean retiring = head.getValue().deleted() && head.getKey() <= safePoint
                    && (!stores.containsKey(key.shard) || checkpoints.get(key.shard).sequence() >= head.getKey());
            if (anchor != null) {
                var obsolete = chain.headMap(anchor, retiring).values().iterator();
                while (obsolete.hasNext() && removed < versionLimit) {
                    retainedBytes -= obsolete.next().retainedBytes();
                    obsolete.remove();
                    retiredMemoryVersions++;
                    removed++;
                }
            }
            if (chain.isEmpty()) {
                retainedBytes -= indexBytes(head.getValue());
                versions.remove(key);
                latest.remove(key);
                forgetHot(key);
            } else {
                memoryGcKeys.add(key);
            }
            if (removed >= versionLimit) {
                break;
            }
        }
    }

    /**
     * Returns the observable state of a stand-alone model.
     *
     * @return the operation result
     */
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
            } catch (final IOException ex) {
                throw error(ErrorCode.STORAGE_UNAVAILABLE, "inspect WAL failed", null, ex);
            }
        }
    }

    /**
     * Read-only takes volatile status and does not get run-time locks that may hold JDBC I/ O.
     *
     * @return the operation result
     */
    public Lifecycle lifecycle() {
        Throwable failure = fatal;
        return new Lifecycle(accepting, closed, backupRunning || replicaExportRunning, readOnly, proposalBlocked, failure == null ? null : failure.getClass().getSimpleName(), materializationFailure,
                maintenanceFailure);
    }

    /**
     * Maintains a switch within the process and only prevents new writing; decision has been queued to continue and does not replace cross-nodes.
     *
     * @param value value to write
     */
    public void setReadOnly(final boolean value) {
        requireOpen();
        readOnly = value;
    }

    /**
     * The hosting closure stopped receiving the new transaction and decision was queued to remain empty under the original commit contract.
     */
    void stopAccepting() {
        accepting = false;
    }

    /**
     * The reconciliation of materialization progress does belong to the current full log, not another or backward WAL.
     *
     * @param checkpoint checkpoint
     * @throws IOException if the operation cannot complete
     */
    private void validateCheckpoint(final WalCheckpoint checkpoint) throws IOException {
        if (checkpoint.sequence() < log.baseSequence() || checkpoint.sequence() > log.lastSequence()
                || checkpoint.sequence() > 0 && !log.receipt(checkpoint.sequence()).digest().equals(checkpoint.digest())) {
            throw new IOException("backend checkpoint does not match WAL");
        }
    }

    /**
     * Clear the transaction that has expired but not yet started decision; queue transaction with the final result determined by the commit thread.
     */
    private void expireIdle() {
        for (WalTransaction tx : List.copyOf(active.values())) {
            if (tx.state == WalTransaction.State.ACTIVE && monotonicClock.getAsLong() - tx.deadlineNanos >= 0) {
                abortLocal(tx);
            }
        }
    }

    /**
     * Discard local write set which is not yet commit and suspend terminal state.
     *
     * @param tx transaction context
     */
    private void abortLocal(final WalTransaction tx) {
        releaseWrites(tx);
        active.remove(tx.id);
        tx.state = WalTransaction.State.ABORTED;
    }

    /**
     * Harmonized accounting for resources released from transaction write set.
     *
     * @param tx transaction context
     */
    private void releaseWrites(final WalTransaction tx) {
        bufferedBytes -= tx.writeBytes;
        tx.writeBytes = 0;
        tx.writes.clear();
    }

    /**
     * Any log or publication failure stopped receiving requests and transaction was not confirmed to keep unknown outcome.
     *
     * @param failure failure
     */
    private void failStop(final Throwable failure) {
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

    /**
     * Verify ownership, state and duration, storage operation followed by a call to prevent the return of expired results.
     *
     * @param tx transaction context
     */
    private void requireActive(final WalTransaction tx) {
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

    /**
     * Refuses to repeat transaction handles while running or restarting.
     *
     * @param tx transaction context
     */
    private void requireOwner(final WalTransaction tx) {
        if (tx == null || tx.owner != owner) {
            throw error(ErrorCode.ILLEGAL_STATE, "foreign transaction handle", tx, null);
        }
    }

    /**
     * Prove the pen-by-punction center decision as the original local COMMITTED.
     *
     * @param tx transaction context
     * @param snapshot snapshot
     */
    private void validateSnapshot(final WalTransaction tx, final WalReadSnapshot snapshot) {
        requireActive(tx);
        if (snapshot == null || snapshot.transaction != tx) {
            throw error(ErrorCode.ILLEGAL_STATE, "foreign statement snapshot", tx, null);
        }
        if (snapshot.sequence < safePoint) {
            throw error(ErrorCode.SNAPSHOT_TOO_OLD, "snapshot retired", tx, null);
        }
    }

    /**
     * Rejects unknown shard.
     *
     * @param shard logical shard identifier
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void requireShard(final String shard) {
        if (!shards.contains(shard)) {
            throw new IllegalArgumentException("unknown shard: " + shard);
        }
    }

    /**
     * When you refuse to stop the request running.
     */
    private void requireOpen() {
        requireHealthy();
        if (!accepting) {
            throw error(ErrorCode.ILLEGAL_STATE, "runtime closing", null, null);
        }
    }

    /**
     * If the log fails, continued reading or commit is prohibited to prevent semi-installation.
     */
    private void requireHealthy() {
        if (closed || fatal != null) {
            throw error(ErrorCode.STORAGE_UNAVAILABLE, "runtime unavailable", null, fatal);
        }
    }

    /**
     * Use the same UTF-8 order as database to judge semi-open space.
     *
     * @param key business key
     * @param from from
     * @param to to
     * @return the operation result
     */
    private static boolean inRange(final String key, final String from, final String to) {
        return (from == null || Utf8KeyOrder.compare(key, from) >= 0)
                && (to == null || Utf8KeyOrder.compare(key, to) < 0);
    }

    /**
     * Sets a fixed cost for the write conflict index billing, overwrite string and index object that remains.
     *
     * @param mutation mutation
     * @return the operation result
     */
    private static long indexBytes(final WalMutation mutation) {
        return 128L + 2L * (mutation.shard().length() + mutation.key().length());
    }

    /**
     * Uniform tectonic zone stability transaction ID model exclusive abnormalities.
     *
     * @param code code
     * @param message message
     * @param tx transaction context
     * @param cause cause
     * @return the operation result
     */
    private static WalMvccException error(final ErrorCode code, final String message, final WalTransaction tx, final Throwable cause) {
        return new WalMvccException(code, message, tx == null ? null : tx.id, cause);
    }

    /**
     * Stop receiving, emptied commit threads, stopped materialization, and released WAL lock.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public void close() {
        // Join of Java 21 synchronizes fixes a virtual thread carrier; the AQS lock is still serially closed but is allowed to be unloaded while waiting.
        closeGuard.lock();
        try {
            if (closed) {
                return;
            }
            synchronized (gate) {
                accepting = false;
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (replicaExportRunning && System.nanoTime() < until) {
                    try {
                        TimeUnit.NANOSECONDS.timedWait(gate, Math.max(1, until - System.nanoTime()));
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("snapshot export close interrupted; directory remains locked", interrupted);
                    }
                }
                if (replicaExportRunning) {
                    throw new IllegalStateException("snapshot export did not drain; directory remains locked");
                }
            }
            join(writer);
            synchronized (gate) {
                flushRunning = false;
                gate.notifyAll();
            }
            hook.hit(Stage.BEFORE_MATERIALIZER_JOIN);
            join(materializer);
            synchronized (gate) {
                for (WalTransaction tx : List.copyOf(active.values())) {
                    abortLocal(tx);
                }
                closed = true;
                try {
                    log.close();
                } catch (final IOException ex) {
                    throw error(ErrorCode.STORAGE_UNAVAILABLE, "close WAL failed", null, ex);
                }
            }
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Bounded Waits for the background line to release the file lock if it is not stopped.
     *
     * @param thread thread
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void join(final Thread thread) {
        try {
            thread.join(10_000);
            if (thread.isAlive()) {
                throw new IllegalStateException("WAL worker did not stop; directory remains locked");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing WAL runtime", ex);
        }
    }

    /**
     * Internal key identity used to separate shard and user keys.
     *
     * @param shard logical shard identifier
     * @param key business key
     */
    record Key(String shard, String key) {
    }

    /**
     * Failure location, does not participate in normal transaction syntax.
     */
    enum Stage {
        BEFORE_APPEND, AFTER_FORCE, BEFORE_PUBLISH, DURING_INSTALL, DURING_RECOVERY, AFTER_PUBLISH, BEFORE_MATERIALIZER_JOIN,
        BEFORE_CHECKPOINT_FILES, AFTER_RECEIPTS_FORCE, AFTER_SNAPSHOT_FORCE, BEFORE_CURRENT_MOVE,
        AFTER_CURRENT_MOVE, AFTER_CURRENT_FORCE, BEFORE_OLD_GENERATION_DELETE, AFTER_OLD_GENERATION_DELETE,
        AFTER_BACKUP_FILE_COPY, BEFORE_BACKUP_COMPLETE, AFTER_BACKUP_COMPLETE, AFTER_RESTORE_FILE_COPY,
        AFTER_RESTORE_BATCH, BEFORE_RESTORE_PUBLISH, AFTER_RESTORE_MOVE, AFTER_RESTORE_READY
    }

    /**
     * A test for the sure injection of commit failed or process killed by force interface.
     */
    @FunctionalInterface
    interface FaultHook {

        /**
         * Carry out injections at specified borders.
         *
         * @param stage stage
         */
        void hit(Stage stage);
    }

    /**
     * Provide independent statistics on commit, memory and the backlog of background.
     *
     * @param publishedSequence published sequence
     * @param safePoint oldest retained snapshot timestamp
     * @param retainedBytes retained bytes
     * @param bufferedBytes buffered bytes
     * @param walBytes wal bytes
     * @param groupForces group forces
     * @param activeTransactions active transactions
     * @param queuedCommits queued commits
     * @param materializedSequences materialized sequences
     * @param materializationFailure materialization failure
     * @param checkpointSequence checkpoint sequence
     * @param pendingReceipts pending receipts
     * @param receiptArchives receipt archives
     * @param retiredReceiptThrough retired receipt through
     * @param maintenanceFailure maintenance failure
     * @param retiredMemoryVersions retired memory versions
     * @param hotCacheBytes hot cache bytes
     * @param hotCacheKeys hot cache keys
     * @param cacheHits cache hits
     * @param cacheMisses cache misses
     * @param cacheEvictions cache evictions
     * @param conflictStoreLookups conflict store lookups
     * @param materializationLag materialization lag
     * @param backupRunning backup running
     * @param receiptArchiveBytes receipt archive bytes
     * @param oldestActiveMillis oldest active millis
     */
    public record Stats(long publishedSequence, long safePoint, long retainedBytes, long bufferedBytes,
            long walBytes, long groupForces, int activeTransactions, int queuedCommits,
            Map<String, Long> materializedSequences, String materializationFailure,
            long checkpointSequence, int pendingReceipts, int receiptArchives,
            long retiredReceiptThrough, String maintenanceFailure, long retiredMemoryVersions,
            long hotCacheBytes, int hotCacheKeys, long cacheHits, long cacheMisses,
            long cacheEvictions, long conflictStoreLookups, long materializationLag, boolean backupRunning,
            long receiptArchiveBytes, long oldestActiveMillis) {
    }

    /**
     * A frozen batch candidate cannot redistribute the commit serial number over a term.
     *
     * @param transactions transactions
     * @param records records
     * @param epoch epoch
     */
    private record Prepared(List<WalTransaction> transactions, List<WalCommit> records, long epoch) {
    }

    /**
     * Specifies whether the checkpoint is created and the log cannot be removed by pushing watermark when the back end is behind.
     *
     * @param created created
     * @param sequence sequence
     * @param walBytesBefore wal bytes before
     * @param walBytesAfter wal bytes after
     */
    public record CheckpointResult(boolean created, long sequence, long walBytesBefore, long walBytesAfter) {
    }

    /**
     * Unblocked survival, surveillance cannot wait for another slow database call to search for health.
     *
     * @param accepting accepting
     * @param closed closed
     * @param backup backup
     * @param readOnly read only
     * @param proposalBlocked proposal blocked
     * @param fatal fatal
     * @param materializationFailure materialization failure
     * @param checkpointFailure checkpoint failure
     */
    public record Lifecycle(boolean accepting, boolean closed, boolean backup, boolean readOnly, boolean proposalBlocked, String fatal,
            String materializationFailure, String checkpointFailure) {
    }
}

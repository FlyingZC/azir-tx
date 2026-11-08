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

package com.flyingzc.azir.percolator;

import com.flyingzc.azir.percolator.PercolatorStore.Action;
import com.flyingzc.azir.percolator.PercolatorStore.Decision;
import com.flyingzc.azir.percolator.PercolatorStore.Lock;
import com.flyingzc.azir.percolator.PercolatorStore.Snapshot;
import com.flyingzc.azir.percolator.PercolatorStore.State;
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

/**
 * Coordinates Percolator transactions using the original primary as the sole durable commit authority; reader leases protect snapshots.
 */
public final class PercolatorRuntime implements AutoCloseable {

    private final TimestampOracle tso;

    private final Map<String, PercolatorStore> stores;

    private final Options options;

    private final MysqlPercolatorLifecycle lifecycle;

    private volatile boolean closed;

    /**
     * Use default batching options; the TSO must increase linearly across all instances and must not allocate timestamps from local ranges.
     *
     * @param tso tso
     * @param stores stores
     */
    public PercolatorRuntime(final TimestampOracle tso, final Map<String, ? extends PercolatorStore> stores) {
        this(tso, stores, Options.defaults());
    }

    /**
     * Visible selection of batch particle size; each logic shard must map to the independent protocol namespace.
     *
     * @param tso tso
     * @param stores stores
     * @param options options
     */
    public PercolatorRuntime(final TimestampOracle tso, final Map<String, ? extends PercolatorStore> stores, final Options options) {
        this(tso, stores, options, null);
    }

    /**
     * The production plant provides durable lease, which does not automatically receive GC permissions when old custom run.
     *
     * @param tso tso
     * @param stores stores
     * @param options options
     * @param lifecycle lifecycle
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PercolatorRuntime(final TimestampOracle tso, final Map<String, ? extends PercolatorStore> stores, final Options options,
                             final MysqlPercolatorLifecycle lifecycle) {
        this.tso = Objects.requireNonNull(tso, "tso");
        this.lifecycle = lifecycle;
        this.options = Objects.requireNonNull(options, "options");
        if (stores.isEmpty()) {
            throw new IllegalArgumentException("stores must not be empty");
        }
        stores.forEach((id, store) -> {
            if (!id.equals(store.shardId())) {
                throw new IllegalArgumentException("shard mapping mismatch");
            }
        });
        this.stores = Map.copyOf(stores);
    }

    /**
     * Creates an independent session; durable Read lease at the same time as a production entry, and no centre commit decision.
     *
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public PercolatorTransaction begin(final IsolationLevel isolation) {
        ensureOpen();
        Objects.requireNonNull(isolation, "isolation");
        return new PercolatorTransaction(this, lifecycle == null ? nextTimestamp() : lifecycle.begin(), isolation);
    }

    /**
     * Inspect the original primary. Resolve expired or missing locks under the atomic fence; live locks remain PENDING.
     *
     * @param handle handle
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public Decision lookup(final Handle handle) {
        ensureOpen();
        if (lifecycle != null && !lifecycle.namespace().equals(handle.namespace())) {
            throw new IllegalArgumentException("Percolator namespace mismatch");
        }
        return store(handle.primary().shard()).primary(handle.primary(), handle.startTs(), Action.INSPECT, 0);
    }

    /**
     * Recover a bounded number of remaining locks per shard. Cache terminal primary decisions only, never PENDING.
     *
     * @param limitPerShard limit per shard
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public int recover(final int limitPerShard) {
        ensureOpen();
        if (limitPerShard < 1 || limitPerShard > 4096) {
            throw new IllegalArgumentException("limit must be in 1..4096");
        }
        return lifecycle == null ? recoverLocks(limitPerShard) : lifecycle.gate(() -> recoverLocks(limitPerShard));
    }

    /**
     * The current production control door is closed to recovery steps, and the cache only retains irreversible terminal state.
     *
     * @param limitPerShard limit per shard
     * @return the operation result
     */
    private int recoverLocks(final int limitPerShard) {
        Map<Handle, Decision> decisions = new LinkedHashMap<>();
        int finished = 0;
        for (PercolatorStore store : stores.values()) {
            for (Lock lock : store.pending(limitPerShard)) {
                com.flyingzc.azir.operations.MysqlProtocolState.checkInterrupted();
                finished += lifecycle == null ? recoverLock(store, lock, decisions) : lifecycle.gate(() -> recoverLock(store, lock, decisions));
            }
        }
        return finished;
    }

    /**
     * Recheck the control phase for each discovered lock; the original primary remains the terminal decision authority.
     *
     * @param store store
     * @param lock lock
     * @param decisions decisions
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private int recoverLock(final PercolatorStore store, final Lock lock, final Map<Handle, Decision> decisions) {
        Handle handle = identity(lock.startTs(), lock.primary());
        Decision decision = decisions.get(handle);
        if (decision == null) {
            decision = lookup(handle);
            if (decision.state() != State.PENDING) {
                decisions.put(handle, decision);
            }
        }
        if (decision.state() == State.RETIRED) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "retired identity still owns a lock", false, lock.startTs());
        }
        if (decision.state() == State.PENDING) {
            return 0;
        }
        store.finish(lock.startTs(), lock.primary(), decision, List.of(lock.key().key()));
        return 1;
    }

    /**
     * Run recovery and secure GCs on production; custom run only recovery and cannot guess cross-example reader.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    public com.flyingzc.azir.maintenance.MaintenanceResult maintain(final int limit) {
        if (lifecycle == null) {
            return new com.flyingzc.azir.maintenance.MaintenanceResult(recover(limit), 0, 0);
        }
        var state = lifecycle.operations().status();
        if (!state.mode().equals("ACTIVE")) {
            return new com.flyingzc.azir.maintenance.MaintenanceResult(0, 0, state.retiredBefore(), true);
        }
        return lifecycle.gate(() -> lifecycle.collect(stores, limit, recover(limit)));
    }

    /**
     * Returns interface production management with durable generation protection.
     *
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    public com.flyingzc.azir.operations.MysqlProtocolAdmin administration() {
        if (lifecycle == null) {
            throw new UnsupportedOperationException("custom runtime has no administration fence");
        }
        return new com.flyingzc.azir.operations.MysqlProtocolAdmin(lifecycle.operations(), this::recover);
    }

    /**
     * The snapshot protection and residual lock at the diagnostic production portal, which refuses to speculate about a custom run-time global reader.
     *
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    public com.flyingzc.azir.maintenance.MaintenanceDiagnostics diagnostics() {
        ensureOpen();
        if (lifecycle == null) {
            throw new UnsupportedOperationException("custom runtime has no persistent reader registry");
        }
        return lifecycle.gate(() -> lifecycle.diagnostics(stores.values().stream().mapToLong(PercolatorStore::pendingCount).sum()));
    }

    /**
     * Read and write within the same durable lease doorbar, GC does not cross ongoing operations.
     *
     * @param <T> value type
     * @param tx transaction context
     * @param readTs snapshot read timestamp
     * @param work work
     * @return the operation result
     */
    <T> T live(final PercolatorTransaction tx, final long readTs, final java.util.function.Supplier<T> work) {
        ensureOpen();
        return lifecycle == null ? work.get()
                : lifecycle.live(tx.startTs, readTs, work,
                        () -> tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.COMMITTED || tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.ABORTED);
    }

    /**
     * Rollback and terminal state processing does not require expired lease but still colludes with GC.
     *
     * @param <T> value type
     * @param work work
     * @return the operation result
     */
    <T> T terminal(final java.util.function.Supplier<T> work) {
        ensureOpen();
        return lifecycle == null ? work.get() : lifecycle.gate(work);
    }

    /**
     * Completed transaction no longer nailed to snapshot; release failure only affects follow-up maintenance progress and does not change the result of commit.
     *
     * @param tx transaction context
     */
    void release(final PercolatorTransaction tx) {
        if (lifecycle != null && (tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.COMMITTED || tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.ABORTED
                || tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN)) {
            try {
                lifecycle.release(tx.startTs);
            } catch (final RuntimeException ignored) {
                /*
                 * Expiry lease can be background bounded collection .
                 */ }
        }
    }

    /**
     * Production receipt contains durable namespace and customizes the old contract for running.
     *
     * @param start start
     * @param primary original Percolator primary identity
     * @return the operation result
     */
    private Handle identity(final long start, final PercolatorKey primary) {
        return new Handle(start, primary, lifecycle == null ? null : lifecycle.namespace());
    }

    /**
     * Close not to continue reading and writing through residual session.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Percolator runtime closed");
        }
    }

    /**
     * The caller's pool is not closed without a life-cycle assembly entry.
     */
    @Override
    public void close() {
        closed = true;
        if (lifecycle != null) {
            lifecycle.close();
        }
    }

    /**
     * Freezing write set and first where prewrite Primary is batch, then prewrite Other shard.
     *
     * @param tx transaction context
     */
    void prewrite(final PercolatorTransaction tx) {
        if (tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.PREWRITTEN) {
            return;
        }
        if (tx.state != com.flyingzc.azir.percolator.PercolatorTransaction.State.ACTIVE) {
            throw tx.illegal("cannot prewrite " + tx.state);
        }
        if (tx.writes.isEmpty()) {
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.PREWRITTEN;
            return;
        }
        tx.handle = identity(tx.startTs, tx.writes.firstKey());
        try {
            for (List<PercolatorKey> batch : batches(tx.writes.keySet().stream().toList())) {
                live(tx, tx.startTs, () -> {
                    store(batch.getFirst().shard()).prewrite(tx.startTs, tx.handle.primary(), options.lockTtlMillis(),
                            batch.stream().map(tx.writes::get).toList());
                    return null;
                });
            }
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.PREWRITTEN;
        } catch (final RuntimeException failure) {
            try {
                rollback(tx);
            } catch (final RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            if (tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN) {
                throw unknown(tx, failure);
            }
            throw failure;
        }
    }

    /**
     * Complete all prewrites before allocating the commit timestamp; preserve UNKNOWN when the primary outcome is uncertain.
     *
     * @param tx transaction context
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    Commit commit(final PercolatorTransaction tx) {
        if (tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.COMMITTED) {
            return tx.result;
        }
        if (tx.state != com.flyingzc.azir.percolator.PercolatorTransaction.State.ACTIVE && tx.state != com.flyingzc.azir.percolator.PercolatorTransaction.State.PREWRITTEN) {
            throw tx.illegal("cannot commit " + tx.state);
        }
        prewrite(tx);
        if (tx.handle == null) {
            tx.result = new Commit(null, 0, List.of());
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.COMMITTED;
            return tx.result;
        }
        long commitTs = nextTimestamp();
        if (commitTs <= tx.startTs) {
            throw tx.illegal("TSO must be strictly globally monotonic");
        }
        Decision decision;
        try {
            decision = commitPrimary(tx, commitTs);
        } catch (final RuntimeException failure) {
            try {
                decision = lookup(tx.handle);
            } catch (final RuntimeException lookupFailure) {
                failure.addSuppressed(lookupFailure);
                tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN;
                throw unknown(tx, failure);
            }
            if (decision.state() == State.PENDING) {
                // The original COMMIT may still be active at database; this PENDING is prohibited as the new retry transaction.
                tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN;
                throw unknown(tx, failure);
            }
        }
        if (decision.state() == State.ROLLED_BACK) {
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.ABORTED;
            finishBestEffort(tx, decision, tx.writes.keySet().stream().toList());
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "primary rolled back", true, tx.startTs);
        }
        if (decision.state() != State.COMMITTED) {
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN;
            throw unknown(tx, null);
        }
        List<PercolatorKey> secondaries = tx.writes.keySet().stream().filter(key -> !key.equals(tx.handle.primary())).toList();
        // First save commit receipt available for replay and then publish local terminal state; no empty receipt will be returned when a fatal error occurs at finalization.
        tx.result = new Commit(tx.handle, decision.commitTs(), secondaries);
        tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.COMMITTED;
        List<PercolatorKey> pending = finishBestEffort(tx, decision, secondaries);
        tx.result = new Commit(tx.handle, decision.commitTs(), pending);
        return tx.result;
    }

    /**
     * Retains the returned X-0 certificate of the Primary before a control post-check, which confirms that the result cannot be erased by an lease cleanup error.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    private Decision commitPrimary(final PercolatorTransaction tx, final long commitTs) {
        Decision[] proof = new Decision[1];
        java.util.function.Supplier<Decision> publish = () -> {
            proof[0] = store(tx.handle.primary().shard()).primary(tx.handle.primary(), tx.startTs, Action.COMMIT, commitTs);
            return proof[0];
        };
        return lifecycle == null ? publish.get()
                : lifecycle.live(tx.startTs, tx.startTs, publish,
                        () -> proof[0] != null && (proof[0].state() == State.COMMITTED || proof[0].state() == State.ROLLED_BACK));
    }

    /**
     * Clean-up security only after the Primary rollback has been confirmed, commit or UNKNOWN session rejects false rollback.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    void rollback(final PercolatorTransaction tx) {
        if (tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.ABORTED) {
            return;
        }
        if (tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.COMMITTED || tx.state == com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN) {
            throw tx.illegal("cannot rollback " + tx.state);
        }
        if (tx.handle == null) {
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.ABORTED;
            return;
        }
        Decision decision;
        try {
            decision = store(tx.handle.primary().shard()).primary(tx.handle.primary(), tx.startTs, Action.ROLLBACK, 0);
        } catch (final RuntimeException failure) {
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN;
            throw unknown(tx, failure);
        }
        if (decision.state() != State.ROLLED_BACK) {
            tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.UNKNOWN;
            throw unknown(tx, null);
        }
        tx.state = com.flyingzc.azir.percolator.PercolatorTransaction.State.ABORTED;
        List<PercolatorKey> pending = finishBestEffort(tx, decision, tx.writes.keySet().stream().toList());
        if (!pending.isEmpty()) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "primary rolled back; secondary cleanup pending: " + pending.size(), true, tx.startTs);
        }
    }

    /**
     * Read the relevant locks to parse the Primary first; no return of the old value is possible when potential members Ts do not exceed the boundary.
     *
     * @param key business key
     * @param readTs snapshot read timestamp
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    Optional<byte[]> read(final PercolatorKey key, final long readTs) {
        PercolatorStore local = store(key.shard());
        for (int attempt = 0; attempt < 16; attempt++) {
            Snapshot snapshot = local.snapshot(key.key(), readTs);
            if (snapshot.lock().isEmpty() || snapshot.lock().get().startTs() > readTs) {
                return snapshot.value().filter(value -> !value.deleted()).map(value -> value.value().clone());
            }
            Lock lock = snapshot.lock().get();
            Decision decision = lookup(identity(lock.startTs(), lock.primary()));
            if (decision.state() == State.RETIRED) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "retired identity still owns a lock", false, lock.startTs());
            }
            if (decision.state() == State.PENDING) {
                throw new MvccException(ErrorCode.LOCK_CONFLICT, "unresolved primary for " + key, true, lock.startTs());
            }
            local.finish(lock.startTs(), lock.primary(), decision, List.of(key.key()));
            // Finalization and then recreate the local Consistency View to avoid using old values before parsing.
        }
        throw new MvccException(ErrorCode.LOCK_CONFLICT, "snapshot lock churn exceeded retry budget", true, null);
    }

    /**
     * The original candidate is organized with this transaction write set; all pages and lock resolve calls are the same readTs.
     *
     * @param tx transaction context
     * @param readTs snapshot read timestamp
     * @param shard logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PercolatorScanPage scanPage(final PercolatorTransaction tx, final long readTs, final String shard,
                                final KeyRange range, final String afterKeyExclusive, final int limit) {
        PercolatorStore local = store(shard);
        Objects.requireNonNull(range, "range");
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("page limit must be in 1..4096");
        }
        if (range.startKeyInclusive() != null) {
            Utf8KeyOrder.encode(range.startKeyInclusive());
        }
        if (range.endKeyExclusive() != null) {
            Utf8KeyOrder.encode(range.endKeyExclusive());
        }
        if (afterKeyExclusive != null) {
            Utf8KeyOrder.encode(afterKeyExclusive);
        }
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

    /**
     * Exclusion cursor limits the local buffer key in conjunction with an inclusive lower boundary.
     *
     * @param key business key
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @return the operation result
     */
    private static boolean inRange(final String key, final KeyRange range, final String afterKeyExclusive) {
        return (range.startKeyInclusive() == null || Utf8KeyOrder.compare(key, range.startKeyInclusive()) >= 0)
                && (range.endKeyExclusive() == null || Utf8KeyOrder.compare(key, range.endKeyExclusive()) < 0)
                && (afterKeyExclusive == null || Utf8KeyOrder.compare(key, afterKeyExclusive) > 0);
    }

    /**
     * Returns the fixed options when running.
     *
     * @return the operation result
     */
    Options options() {
        return options;
    }

    /**
     * Refuses to read or write unregistered logic shard.
     *
     * @param shard logical shard identifier
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PercolatorStore store(final String shard) {
        PercolatorStore store = stores.get(shard);
        if (store == null) {
            throw new IllegalArgumentException("unknown shard: " + shard);
        }
        return store;
    }

    /**
     * Global timestamp service must ensure the linear sorting of all transaction and statements across examples.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    long nextTimestamp() {
        // The new phase must confirm that the original control session is still in place and cannot be borrowed to continue with the release of connection after the door lock has been broken.
        long timestamp = lifecycle == null ? tso.nextTimestamp() : lifecycle.gate(tso::nextTimestamp);
        if (timestamp <= 0) {
            throw new IllegalStateException("TSO returned nonpositive timestamp");
        }
        return timestamp;
    }

    /**
     * Batch completes all subdary; returns failed key supply for observation and is completed by reading side or recover.
     *
     * @param tx transaction context
     * @param decision decision
     * @param keys keys
     * @return the operation result
     */
    private List<PercolatorKey> finishBestEffort(final PercolatorTransaction tx, final Decision decision, final List<PercolatorKey> keys) {
        List<PercolatorKey> pending = new ArrayList<>();
        for (List<PercolatorKey> batch : batches(keys)) {
            try {
                store(batch.getFirst().shard()).finish(tx.startTs, tx.handle.primary(), decision,
                        batch.stream().map(PercolatorKey::key).toList());
            } catch (final RuntimeException failure) {
                pending.addAll(batch);
            }
        }
        return List.copyOf(pending);
    }

    /**
     * The adjacent shard key is merged into local transaction; closing the switch can be a semantic type-by-key performance baseline.
     *
     * @param keys keys
     * @return the operation result
     */
    private List<List<PercolatorKey>> batches(final List<PercolatorKey> keys) {
        if (!options.batchByShard()) {
            return keys.stream().map(List::of).toList();
        }
        Map<String, List<PercolatorKey>> grouped = new LinkedHashMap<>();
        for (PercolatorKey key : keys) {
            grouped.computeIfAbsent(key.shard(), ignored -> new ArrayList<>()).add(key);
        }
        return grouped.values().stream().map(List::copyOf).toList();
    }

    /**
     * Do not automatically retry business work with a new identity when the original commit outcome is uncertain.
     *
     * @param tx transaction context
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unknown(final PercolatorTransaction tx, final Throwable cause) {
        return new PercolatorTransactionStatusUnknownException(tx.handle, cause);
    }

    /**
     * Bounded transaction options. batchByShard controls only local batch granularity and does not change commit semantics.
     *
     * @param lockTtlMillis lock ttl millis
     * @param maxKeys max keys
     * @param maxBytes max bytes
     * @param batchByShard batch by shard
     */
    public record Options(long lockTtlMillis, int maxKeys, long maxBytes, boolean batchByShard) {

        /**
         * Validate the lock lease and memory budget.
         *
         * @param lockTtlMillis lock ttl millis
         * @param maxKeys max keys
         * @param maxBytes max bytes
         * @param batchByShard batch by shard
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Options {
            if (lockTtlMillis < 1 || lockTtlMillis > 86_400_000L || maxKeys < 1 || maxBytes < 1) {
                throw new IllegalArgumentException("invalid Percolator options");
            }
        }

        /**
         * Prewrite and materialize whole shard batches by default, with a one-minute lock lease.
         *
         * @return the operation result
         */
        public static Options defaults() {
            return new Options(60_000, 4096, 16L * 1024 * 1024, true);
        }
    }

    /**
     * Persist the original startTs, complete primary address and protocol namespace to resolve the outcome across processes.
     *
     * @param startTs transaction start timestamp
     * @param primary original Percolator primary identity
     * @param namespace persistent protocol namespace
     */
    public record Handle(long startTs, PercolatorKey primary, String namespace) {

        /**
         * Retain the legacy receipt constructor for custom runtimes; managed runtimes require namespaced receipts.
         *
         * @param startTs transaction start timestamp
         * @param primary original Percolator primary identity
         */
        public Handle(final long startTs, final PercolatorKey primary) {
            this(startTs, primary, null);
        }

        /**
         * Reject invalid recovery identities.
         *
         * @param startTs transaction start timestamp
         * @param primary original Percolator primary identity
         * @param namespace persistent protocol namespace
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Handle {
            if (startTs <= 0) {
                throw new IllegalArgumentException("startTs must be positive");
            }
            Objects.requireNonNull(primary, "primary");
        }
    }

    /**
     * A confirmed commit; nonempty pendingSecondaries require materialization recovery, but the commit decision is already durable.
     *
     * @param handle handle
     * @param commitTs durable commit timestamp
     * @param pendingSecondaries pending secondaries
     */
    public record Commit(Handle handle, long commitTs, List<PercolatorKey> pendingSecondaries) {

        /**
         * Copy pending keys without exposing the mutable transaction write set.
         *
         * @param handle handle
         * @param commitTs durable commit timestamp
         * @param pendingSecondaries pending secondaries
         */
        public Commit {
            pendingSecondaries = List.copyOf(pendingSecondaries);
        }
    }
}

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

package com.flyingzc.azir.gc;

import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.tso.TimestampOracle;

import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * MVCC History version garbage collection Manager.
 */
public final class GcManager {

    private final TransactionMetadataStore metadataStore;

    private final TimestampOracle timestampOracle;

    private final SafePointStore safePointStore;

    private final Map<String, StorageAdapter> adapters;

    private final int metadataBatchSize;

    private final long finalizedRetentionMillis;

    private final long timeBudgetNanos;

    private final LongSupplier monotonicClock;

    private int workCursor;

    /**
     * Construct a GcManager instance.
     *
     * @param metadataStore metadata store
     * @param timestampOracle timestamp oracle
     * @param safePointStore safe point store
     * @param adapters adapters
     */
    public GcManager(final TransactionMetadataStore metadataStore,
                     final TimestampOracle timestampOracle,
                     final SafePointStore safePointStore,
                     final Map<String, StorageAdapter> adapters) {
        this(metadataStore, timestampOracle, safePointStore, adapters, 256, 60_000, 1000);
    }

    /**
     * Configure lease and metadata clean-up batch, terminal state to keep milliseconds and co-operative single-wheel time budget.
     *
     * @param metadataStore metadata store
     * @param timestampOracle timestamp oracle
     * @param safePointStore safe point store
     * @param adapters adapters
     * @param metadataBatchSize metadata batch size
     * @param finalizedRetentionMillis finalized retention millis
     * @param timeBudgetMillis time budget millis
     */
    public GcManager(final TransactionMetadataStore metadataStore, final TimestampOracle timestampOracle,
                     final SafePointStore safePointStore, final Map<String, StorageAdapter> adapters,
                     final int metadataBatchSize, final long finalizedRetentionMillis, final long timeBudgetMillis) {
        this(metadataStore, timestampOracle, safePointStore, adapters, metadataBatchSize,
                finalizedRetentionMillis, timeBudgetMillis, System::nanoTime);
    }

    /**
     * Injecting single-twice clocks to define cooperative budget, independent of the authority Wall clock used by lease.
     *
     * @param metadataStore metadata store
     * @param timestampOracle timestamp oracle
     * @param safePointStore safe point store
     * @param adapters adapters
     * @param metadataBatchSize metadata batch size
     * @param finalizedRetentionMillis finalized retention millis
     * @param timeBudgetMillis time budget millis
     * @param monotonicClock monotonic clock
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public GcManager(final TransactionMetadataStore metadataStore, final TimestampOracle timestampOracle,
                     final SafePointStore safePointStore, final Map<String, StorageAdapter> adapters,
                     final int metadataBatchSize, final long finalizedRetentionMillis, final long timeBudgetMillis, final LongSupplier monotonicClock) {
        if (metadataBatchSize < 1 || finalizedRetentionMillis < 0 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid GC limits");
        }
        this.metadataStore = metadataStore;
        this.timestampOracle = timestampOracle;
        this.safePointStore = safePointStore;
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.finalizedRetentionMillis = finalizedRetentionMillis;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /**
     * Propulsion safe point according to active transaction.
     *
     * @return the operation result
     */
    public long advanceSafePoint() {
        return metadataStore.withPublicationLock(() -> {
            metadataStore.assertNoPendingPublications();
            // Expire lease delete, read and protect the lower bounds and advance safe point to begin/ release barrier.
            metadataStore.expireSnapshots(metadataStore.currentTimeMillis(), metadataBatchSize);
            long currentSafePoint = safePointStore.currentSafePoint();
            long candidate = metadataStore.oldestActiveStartTs()
                    .map(oldest -> Math.max(currentSafePoint, oldest > 0 ? oldest - 1 : 0))
                    .orElseGet(() -> Math.max(currentSafePoint, timestampOracle.nextTimestamp() - 1));
            return safePointStore.advanceTo(candidate);
        });
    }

    /**
     * Execute a history version garbage collection.
     *
     * @param limitPerShard limit per shard
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized int gcOnce(final int limitPerShard) {
        if (limitPerShard < 1) {
            throw new IllegalArgumentException("limitPerShard must be positive");
        }
        long safePoint = advanceSafePoint();
        List<String> shards = adapters.keySet().stream().sorted().toList();
        int workCount = shards.size() + 1;
        long started = monotonicClock.getAsLong();
        int reclaimed = 0;
        for (int attempted = 0; attempted < workCount
                && (attempted == 0 || monotonicClock.getAsLong() - started < timeBudgetNanos); attempted++) {
            // Clear metadata and shard s using the same round cursor; even if last failure or depletion of budget takes place, the next round moves to the next one.
            int work = workCursor;
            workCursor = (workCursor + 1) % workCount;
            if (work < shards.size()) {
                reclaimed += adapters.get(shards.get(work)).gc(safePoint, limitPerShard);
            } else {
                metadataStore.withPublicationLock(() -> {
                    long now = metadataStore.currentTimeMillis();
                    long cutoff = now < finalizedRetentionMillis ? 0 : now - finalizedRetentionMillis;
                    return metadataStore.purgeFinalized(safePoint, cutoff, metadataBatchSize);
                });
            }
        }
        // Retains the old syntax of return values: Only participant collection quantities are counted, without mixing with lease or central metadata rows.
        return reclaimed;
    }
}

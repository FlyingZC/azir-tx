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

package com.flyingzc.azir.publication;

import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.storage.StorageAdapter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Bounded GC organiser for the new protocol, using account books safe point collection version and terminal state records.
 */
public final class PublicationGcManager {

    private final MysqlPublicationLedger ledger;

    private final TransactionMetadataStore metadataStore;

    private final Map<String, StorageAdapter> adapters;

    private final int metadataBatchSize;

    private final long retentionMillis;

    private final long timeBudgetNanos;

    private final LongSupplier monotonicClock;

    private int workCursor;

    private Long cycleSafePoint;

    /**
     * Creates the default batch and saves the account book GC manager for the window.
     *
     * @param ledger ledger
     * @param metadataStore metadata store
     * @param adapters adapters
     */
    public PublicationGcManager(final MysqlPublicationLedger ledger, final TransactionMetadataStore metadataStore,
                                final Map<String, StorageAdapter> adapters) {
        this(ledger, metadataStore, adapters, 256, 60_000, 1000, System::nanoTime);
    }

    /**
     * Configure the observable batch cap, terminal state retention time and cooperative implementation budget.
     *
     * @param ledger ledger
     * @param metadataStore metadata store
     * @param adapters adapters
     * @param metadataBatchSize metadata batch size
     * @param retentionMillis retention millis
     * @param timeBudgetMillis time budget millis
     * @param monotonicClock monotonic clock
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationGcManager(final MysqlPublicationLedger ledger, final TransactionMetadataStore metadataStore,
                                final Map<String, StorageAdapter> adapters, final int metadataBatchSize,
                                final long retentionMillis, final long timeBudgetMillis, final LongSupplier monotonicClock) {
        if (metadataBatchSize < 1 || metadataBatchSize > 4096 || retentionMillis < 0 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid publication GC limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.retentionMillis = retentionMillis;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /**
     * Creates a new safe point on each wheel, then rotates collection shard, Centre terminal state and Book terminal state.
     *
     * @param limitPerShard limit per shard
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized int gcOnce(final int limitPerShard) {
        if (limitPerShard < 1) {
            throw new IllegalArgumentException("limitPerShard must be positive");
        }
        ledger.expireSnapshots(metadataBatchSize);
        ledger.expireCoordinatorLeases(metadataBatchSize);
        // (b) shard local fence cannot press the higher watermark collection centre until all shard local fence is completed.
        if (cycleSafePoint == null) {
            cycleSafePoint = ledger.advanceSafePoint();
        }
        long safePoint = cycleSafePoint;
        List<String> shards = adapters.keySet().stream().sorted().toList();
        int workCount = shards.size() + 2;
        long started = monotonicClock.getAsLong();
        int reclaimed = 0;
        for (int attempted = 0; attempted < workCount
                && (attempted == 0 || monotonicClock.getAsLong() - started < timeBudgetNanos); attempted++) {
            int work = workCursor;
            if (work < shards.size()) {
                reclaimed += adapters.get(shards.get(work)).gc(safePoint, limitPerShard);
            } else {
                long now = metadataStore.currentTimeMillis();
                long cutoff = now < retentionMillis ? 0 : now - retentionMillis;
                if (work == shards.size()) {
                    metadataStore.purgeFinalized(safePoint, cutoff, metadataBatchSize);
                } else {
                    ledger.purgeTerminal(safePoint, cutoff, metadataBatchSize);
                }
            }
            // Cursor and target remain the same when it fails; higher global safe point will not be read in the next round after all phases have been completed.
            workCursor = (workCursor + 1) % workCount;
            if (workCursor == 0) {
                cycleSafePoint = null;
                break;
            }
        }
        return reclaimed;
    }

    /**
     * Returns durable storage safe point with the same participant GC.
     *
     * @return the operation result
     */
    public long currentSafePoint() {
        return ledger.currentSafePoint();
    }
}

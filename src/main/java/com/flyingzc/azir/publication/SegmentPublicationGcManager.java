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
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;
import com.flyingzc.azir.storage.StorageAdapter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Use the same durable safe point rotation collection shard version, slots, center terminal state and owner records.
 */
public final class SegmentPublicationGcManager {

    private final MysqlSegmentPublicationLedger ledger;

    private final TransactionMetadataStore metadataStore;

    private final Map<String, StorageAdapter> adapters;

    private final int metadataBatchSize;

    private final long retentionMillis;

    private final long timeBudgetNanos;

    private final LongSupplier monotonicClock;

    private int workCursor;

    private Long cycleSafePoint;

    /**
     * Create default batch, save window and cooperative time budget.
     *
     * @param ledger ledger
     * @param metadataStore metadata store
     * @param adapters adapters
     */
    public SegmentPublicationGcManager(final MysqlSegmentPublicationLedger ledger,
                                       final TransactionMetadataStore metadataStore,
                                       final Map<String, StorageAdapter> adapters) {
        this(ledger, metadataStore, adapters, 256, 60_000, 1_000, System::nanoTime);
    }

    /**
     * The long run configuration is limited to batch and background budget per wheel.
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
    public SegmentPublicationGcManager(final MysqlSegmentPublicationLedger ledger,
                                       final TransactionMetadataStore metadataStore,
                                       final Map<String, StorageAdapter> adapters,
                                       final int metadataBatchSize, final long retentionMillis,
                                       final long timeBudgetMillis, final LongSupplier monotonicClock) {
        if (metadataBatchSize < 1 || metadataBatchSize > 4096
                || retentionMillis < 0 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid segmented GC limits");
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
     * First clear lease due and then use the same safe point in the order of shard, slot, center, owner.
     *
     * @param limitPerShard limit per shard
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized int gcOnce(final int limitPerShard) {
        if (limitPerShard < 1 || limitPerShard > 4096) {
            throw new IllegalArgumentException("invalid segmented shard GC limit");
        }
        ledger.expireSnapshots(metadataBatchSize);
        ledger.expireCoordinatorLeases(metadataBatchSize);
        if (cycleSafePoint == null) {
            cycleSafePoint = ledger.advanceSafePoint();
        }
        long safePoint = cycleSafePoint;
        List<String> shards = adapters.keySet().stream().sorted().toList();
        int workCount = shards.size() + 3;
        long started = monotonicClock.getAsLong();
        int reclaimed = 0;
        for (int attempted = 0; attempted < workCount
                && (attempted == 0 || monotonicClock.getAsLong() - started < timeBudgetNanos); attempted++) {
            int stage = workCursor;
            if (stage < shards.size()) {
                reclaimed += adapters.get(shards.get(stage)).gc(safePoint, limitPerShard);
            } else {
                long now = metadataStore.currentTimeMillis();
                long cutoff = now < retentionMillis ? 0 : now - retentionMillis;
                if (stage == shards.size()) {
                    reclaimed += ledger.purgeTerminalSlots(safePoint, cutoff, metadataBatchSize);
                } else if (stage == shards.size() + 1) {
                    reclaimed += ledger.purgeFinalizedMetadata(safePoint, cutoff, metadataBatchSize);
                } else {
                    reclaimed += ledger.purgeSealedSegments(safePoint, cutoff, metadataBatchSize);
                }
            }
            // Failed to keep phase and target safe point; read higher watermark next round after completion of full phase.
            workCursor = (workCursor + 1) % workCount;
            if (workCursor == 0) {
                cycleSafePoint = null;
                break;
            }
        }
        return reclaimed;
    }

    /**
     * Returns shard to safe point with Center collection.
     *
     * @return the operation result
     */
    public long currentSafePoint() {
        return ledger.currentSafePoint();
    }
}

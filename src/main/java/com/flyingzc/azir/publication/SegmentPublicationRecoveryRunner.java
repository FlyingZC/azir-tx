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

import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Bounded Rotation Scanning Division expired owner, transaction expired without materialization terminal state and no slot.
 */
public final class SegmentPublicationRecoveryRunner {

    private final MysqlSegmentPublicationLedger ledger;

    private final SegmentPublicationRecoveryManager recoveryManager;

    private final TransactionMetadataStore metadataStore;

    private final int maxTransactionsPerRun;

    private final long timeBudgetNanos;

    private final LongSupplier monotonicClock;

    private final BackgroundTaskStats transactionStats = new BackgroundTaskStats();

    private final BackgroundTaskStats scanStats = new BackgroundTaskStats();

    private final AtomicReference<RuntimeException> lastTransactionFailure = new AtomicReference<>();

    private final AtomicReference<RuntimeException> lastScanFailure = new AtomicReference<>();

    private long terminalAfterCommitTs;

    private long localPendingAfterCommitTs;

    private long undecidedAfterTxId = Long.MIN_VALUE;

    private int scanCursor;

    /**
     * Use bounded default batch and one second cooperative rotation budget.
     *
     * @param ledger ledger
     * @param recoveryManager recovery manager
     * @param metadataStore metadata store
     */
    public SegmentPublicationRecoveryRunner(final MysqlSegmentPublicationLedger ledger,
                                            final SegmentPublicationRecoveryManager recoveryManager,
                                            final TransactionMetadataStore metadataStore) {
        this(ledger, recoveryManager, metadataStore, 256, 1_000, System::nanoTime);
    }

    /**
     * Allows testing for single-wheel injection, time budget and monotonic clock.
     *
     * @param ledger ledger
     * @param recoveryManager recovery manager
     * @param metadataStore metadata store
     * @param maxTransactionsPerRun max transactions per run
     * @param timeBudgetMillis time budget millis
     * @param monotonicClock monotonic clock
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public SegmentPublicationRecoveryRunner(final MysqlSegmentPublicationLedger ledger,
                                            final SegmentPublicationRecoveryManager recoveryManager,
                                            final TransactionMetadataStore metadataStore,
                                            final int maxTransactionsPerRun, final long timeBudgetMillis,
                                            final LongSupplier monotonicClock) {
        if (maxTransactionsPerRun < 1 || maxTransactionsPerRun > 4096 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid segmented recovery limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.recoveryManager = Objects.requireNonNull(recoveryManager, "recoveryManager");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.maxTransactionsPerRun = maxTransactionsPerRun;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /**
     * Fencing expires first, then rotates recovery terminal state and expired pending transaction concurrent prefixes.
     *
     * @param scanLimit scan limit
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized int recoverOnce(final int scanLimit) {
        if (scanLimit < 1 || scanLimit > 4096) {
            throw new IllegalArgumentException("invalid segmented recovery scan limit");
        }
        long started = monotonicClock.getAsLong();
        int completed = 0;
        try {
            completed += ledger.sealExpiredSegments(scanLimit);
            scanStats.recordSuccess(1);
        } catch (final RuntimeException ex) {
            lastScanFailure.set(ex);
            scanStats.recordFailure();
        }
        long now = metadataStore.currentTimeMillis();
        LinkedHashSet<Long> candidates = new LinkedHashSet<>();
        for (int scanned = 0; scanned < 3 && candidates.size() < maxTransactionsPerRun
                && hasBudget(started); scanned++) {
            int category = scanCursor;
            scanCursor = (scanCursor + 1) % 3;
            int limit = Math.min(scanLimit, maxTransactionsPerRun - candidates.size());
            try {
                List<Long> found = switch (category) {
                    case 0 -> scanTerminal(limit);
                    case 1 -> scanExpiredUndecided(now, limit);
                    default -> scanLocalPending(now, limit);
                };
                candidates.addAll(found);
                scanStats.recordSuccess(found.size());
            } catch (final RuntimeException ex) {
                lastScanFailure.set(ex);
                scanStats.recordFailure();
            }
        }
        for (long txId : candidates) {
            if (!hasBudget(started)) {
                break;
            }
            try {
                boolean recovered = recoveryManager.recover(txId);
                completed += recovered ? 1 : 0;
                transactionStats.recordSuccess(recovered ? 1 : 0);
            } catch (final RuntimeException ex) {
                lastTransactionFailure.set(ex);
                transactionStats.recordFailure();
            }
        }
        // Once the undistributed tailings slot has been sealed, even without transaction identifier, consecutive release watermark must be released.
        ledger.advancePublished(256);
        return completed;
    }

    /**
     * Terminal state slot with commit timestamp tab, ending with retry temporarily failed participant.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<Long> scanTerminal(final int limit) {
        List<PublicationEntry> entries = ledger.findUnfinalizedTerminal(terminalAfterCommitTs, limit);
        terminalAfterCommitTs = entries.size() < limit ? 0 : entries.getLast().commitTs();
        return entries.stream().map(PublicationEntry::txId).toList();
    }

    /**
     * No slot meta-data by sign transaction identifier page, overwrites the negative transaction ID assigned by the coordinator.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<Long> scanExpiredUndecided(final long nowMillis, final int limit) {
        List<Long> found = ledger.findExpiredUndecided(nowMillis, undecidedAfterTxId, limit);
        undecidedAfterTxId = found.size() < limit ? Long.MIN_VALUE : found.getLast();
        return found;
    }

    /**
     * Local candidate is accepted for reading shard terminal state after pressing timestamp scan, owner fencing.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<Long> scanLocalPending(final long nowMillis, final int limit) {
        List<PublicationEntry> entries = ledger.findRecoverableLocalPending(
                nowMillis, localPendingAfterCommitTs, limit);
        localPendingAfterCommitTs = entries.size() < limit ? 0 : entries.getLast().commitTs();
        return entries.stream().map(PublicationEntry::txId).toList();
    }

    /**
     * Check this co-operation time for budget between the previous background database operation.
     *
     * @param started started
     * @return the operation result
     */
    private boolean hasBudget(final long started) {
        return monotonicClock.getAsLong() - started < timeBudgetNanos;
    }

    /**
     * Returns successful transaction numbers and failed rounds.
     *
     * @return the operation result
     */
    public BackgroundTaskStats transactionStats() {
        return transactionStats;
    }

    /**
     * Returns the success statistics of scans with owner fencing.
     *
     * @return the operation result
     */
    public BackgroundTaskStats scanStats() {
        return scanStats;
    }

    /**
     * Returns the latest transaction recovery anomaly for deployment location.
     *
     * @return the operation result
     */
    public RuntimeException lastTransactionFailure() {
        return lastTransactionFailure.get();
    }

    /**
     * Returns the latest scan or owning abnormally for transport-based positioning.
     *
     * @return the operation result
     */
    public RuntimeException lastScanFailure() {
        return lastScanFailure.get();
    }
}

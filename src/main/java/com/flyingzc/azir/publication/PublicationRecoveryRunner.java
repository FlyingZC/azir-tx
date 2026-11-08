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
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Rotation found terminal state with new book dues, failed to preset crash transaction and incomplete materialization.
 */
public final class PublicationRecoveryRunner {

    private final MysqlPublicationLedger ledger;

    private final PublicationRecoveryManager recoveryManager;

    private final TransactionMetadataStore metadataStore;

    private final int maxTransactionsPerRun;

    private final long timeBudgetNanos;

    private final LongSupplier monotonicClock;

    private final BackgroundTaskStats transactionStats = new BackgroundTaskStats();

    private final BackgroundTaskStats scanStats = new BackgroundTaskStats();

    private final AtomicReference<RuntimeException> lastTransactionFailure = new AtomicReference<>();

    private final AtomicReference<RuntimeException> lastScanFailure = new AtomicReference<>();

    private long pendingAfterCommitTs;

    private long unreservedAfterTxId = Long.MIN_VALUE;

    private long terminalAfterTxId = Long.MIN_VALUE;

    private int scanCursor;

    /**
     * Construct the recovery Discovery with bounded default batch co-time budget.
     *
     * @param ledger ledger
     * @param recoveryManager recovery manager
     * @param metadataStore metadata store
     */
    public PublicationRecoveryRunner(final MysqlPublicationLedger ledger, final PublicationRecoveryManager recoveryManager,
                                     final TransactionMetadataStore metadataStore) {
        this(ledger, recoveryManager, metadataStore, 256, 1000, System::nanoTime);
    }

    /**
     * Injecting Scan Upper limit and single-wheel budget, the database call is still protected by driver timeout.
     *
     * @param ledger ledger
     * @param recoveryManager recovery manager
     * @param metadataStore metadata store
     * @param maxTransactionsPerRun max transactions per run
     * @param timeBudgetMillis time budget millis
     * @param monotonicClock monotonic clock
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PublicationRecoveryRunner(final MysqlPublicationLedger ledger, final PublicationRecoveryManager recoveryManager,
                                     final TransactionMetadataStore metadataStore, final int maxTransactionsPerRun,
                                     final long timeBudgetMillis, final LongSupplier monotonicClock) {
        if (maxTransactionsPerRun < 1 || maxTransactionsPerRun > 4096 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("invalid publication recovery limits");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.recoveryManager = Objects.requireNonNull(recoveryManager, "recoveryManager");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.maxTransactionsPerRun = maxTransactionsPerRun;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
    }

    /**
     * Each round is selected from three durable index wheels, and single transaction failed after cursor returns retry.
     *
     * @param perScanLimit per scan limit
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized int recoverOnce(final int perScanLimit) {
        if (perScanLimit < 1 || perScanLimit > 4096) {
            throw new IllegalArgumentException("publication recovery scan limit must be between 1 and 4096");
        }
        long started = monotonicClock.getAsLong();
        long now = metadataStore.currentTimeMillis();
        LinkedHashSet<Long> candidates = new LinkedHashSet<>();
        for (int scanned = 0; scanned < 3 && candidates.size() < maxTransactionsPerRun
                && hasBudget(started); scanned++) {
            int category = scanCursor;
            scanCursor = (scanCursor + 1) % 3;
            int limit = Math.min(perScanLimit, maxTransactionsPerRun - candidates.size());
            try {
                List<Long> found = switch (category) {
                    case 0 -> scanExpiredPending(now, limit);
                    case 1 -> scanExpiredUnreserved(now, limit);
                    default -> scanUnfinalizedTerminal(limit);
                };
                candidates.addAll(found);
                scanStats.recordSuccess(found.size());
            } catch (final RuntimeException ex) {
                scanStats.recordFailure();
                lastScanFailure.set(ex);
            }
        }
        int recovered = 0;
        for (long txId : candidates) {
            if (!hasBudget(started)) {
                break;
            }
            try {
                boolean completed = recoveryManager.recover(txId);
                recovered += completed ? 1 : 0;
                transactionStats.recordSuccess(completed ? 1 : 0);
            } catch (final RuntimeException ex) {
                transactionStats.recordFailure();
                lastTransactionFailure.set(ex);
            }
        }
        return recovered;
    }

    /**
     * Press commit timestamp to scanned due PENDING, cursor to end and then return.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<Long> scanExpiredPending(final long nowMillis, final int limit) {
        List<PublicationEntry> entries = ledger.findExpiredPending(nowMillis, pendingAfterCommitTs, limit);
        pendingAfterCommitTs = entries.size() < limit ? 0 : entries.getLast().commitTs();
        return entries.stream().map(PublicationEntry::txId).toList();
    }

    /**
     * The unset transaction with no valid lease from metadata scan, overwrite GC to delete lease.
     *
     * @param nowMillis now millis
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<Long> scanExpiredUnreserved(final long nowMillis, final int limit) {
        List<Long> ids = ledger.findExpiredUnreserved(nowMillis, unreservedAfterTxId, limit);
        unreservedAfterTxId = ids.size() < limit ? Long.MIN_VALUE : ids.getLast();
        return ids;
    }

    /**
     * Only terminal state of the new book was not completed to avoid taking over the old protocol records prior to relocation.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    private List<Long> scanUnfinalizedTerminal(final int limit) {
        List<Long> ids = ledger.findUnfinalizedTerminal(terminalAfterTxId, limit);
        terminalAfterTxId = ids.size() < limit ? Long.MIN_VALUE : ids.getLast();
        return ids;
    }

    /**
     * Check single-wheel cooperative budget between adjacent tasks.
     *
     * @param started started
     * @return the operation result
     */
    private boolean hasBudget(final long started) {
        return monotonicClock.getAsLong() - started < timeBudgetNanos;
    }

    /**
     * Returns transaction recovery success and failure.
     *
     * @return the operation result
     */
    public BackgroundTaskStats transactionStats() {
        return transactionStats;
    }

    /**
     * Returns the success and failure of an durable index scan.
     *
     * @return the operation result
     */
    public BackgroundTaskStats scanStats() {
        return scanStats;
    }

    /**
     * Returns the last transaction recovery -by- recovery failed to be used to locate the durable storage block.
     *
     * @return the operation result
     */
    public RuntimeException lastTransactionFailure() {
        return lastTransactionFailure.get();
    }

    /**
     * Returns the last candidate scan failed, providing for a WID database or mode problem.
     *
     * @return the operation result
     */
    public RuntimeException lastScanFailure() {
        return lastScanFailure.get();
    }
}

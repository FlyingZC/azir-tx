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

package com.flyingzc.azir.recovery;

import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.common.BackgroundTaskStats;

import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Background recovery Executor, press shard scan and recovery hang transaction.
 */
public final class BackgroundRecoveryRunner {

    private final RecoveryManager recoveryManager;

    private final Map<String, StorageAdapter> adapters;

    private final int metadataBatchSize;

    private final int maxTransactionsPerRun;

    private final long timeBudgetNanos;

    private final BackgroundTaskStats transactionStats = new BackgroundTaskStats();

    private final BackgroundTaskStats scanStats = new BackgroundTaskStats();

    private final Map<Long, Set<String>> pending = new LinkedHashMap<>();

    private long metadataCursor;

    private int shardCursor;

    /**
     * Construct a BackgroundRecoveryRunner instance.
     *
     * @param recoveryManager recovery manager
     * @param adapters adapters
     */
    public BackgroundRecoveryRunner(final RecoveryManager recoveryManager, final Map<String, StorageAdapter> adapters) {
        this(recoveryManager, adapters, 128, 256, 1000);
    }

    /**
     * Configure metadata batch, single-wheel transaction cap and co-time budget, keep the original constructor.
     *
     * @param recoveryManager recovery manager
     * @param adapters adapters
     * @param metadataBatchSize metadata batch size
     * @param maxTransactionsPerRun max transactions per run
     * @param timeBudgetMillis time budget millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BackgroundRecoveryRunner(final RecoveryManager recoveryManager, final Map<String, StorageAdapter> adapters,
                                    final int metadataBatchSize, final int maxTransactionsPerRun, final long timeBudgetMillis) {
        if (metadataBatchSize < 1 || maxTransactionsPerRun < 1 || timeBudgetMillis < 1) {
            throw new IllegalArgumentException("recovery limits must be positive");
        }
        this.recoveryManager = recoveryManager;
        this.adapters = Map.copyOf(adapters);
        this.metadataBatchSize = metadataBatchSize;
        this.maxTransactionsPerRun = maxTransactionsPerRun;
        this.timeBudgetNanos = Math.multiplyExact(timeBudgetMillis, 1_000_000L);
    }

    /**
     * The executive round is hanging transaction recovery.
     *
     * @param perShardLimit per shard limit
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized int recoverOnce(final int perShardLimit) {
        if (perShardLimit < 1) {
            throw new IllegalArgumentException("perShardLimit must be positive");
        }
        long started = System.nanoTime();
        if (pending.isEmpty()) {
            discoverMetadata();
            discoverIntents(perShardLimit, started);
        }
        int recovered = 0;
        int attempted = 0;
        var iterator = pending.entrySet().iterator();
        while (iterator.hasNext() && attempted < maxTransactionsPerRun && hasBudget(started)) {
            var entry = iterator.next();
            try {
                boolean completed = recoveryManager.recover(entry.getKey(), entry.getValue());
                transactionStats.recordSuccess(completed ? 1 : 0);
                recovered += completed ? 1 : 0;
            } catch (final RuntimeException ex) {
                // A single transaction error does not block the other transaction s of the same batch. The failed item is retry at the next cursor scan.
                transactionStats.recordFailure();
            }
            iterator.remove();
            attempted++;
        }
        return recovered;
    }

    /**
     * Continue the premix migration cursor to avoid a single shard commit hunger after recovery with bad transaction or no indent.
     */
    private void discoverMetadata() {
        try {
            List<TxRecord> candidates = recoveryManager.findRecoveryCandidates(metadataCursor,
                    Math.min(metadataBatchSize, maxTransactionsPerRun));
            for (TxRecord candidate : candidates) {
                pending.computeIfAbsent(candidate.txId(), ignored -> new LinkedHashSet<>()).addAll(candidate.participants());
                metadataCursor = candidate.txId();
            }
            if (candidates.size() < Math.min(metadataBatchSize, maxTransactionsPerRun)) {
                metadataCursor = 0;
            }
            scanStats.recordSuccess(candidates.size());
        } catch (final RuntimeException ex) {
            scanStats.recordFailure();
        }
    }

    /**
     * Rotation scan shard and retain the actual location of the discovery; unprocessed items in the bounded queue are kept across the wheel.
     *
     * @param perShardLimit per shard limit
     * @param started started
     */
    private void discoverIntents(final int perShardLimit, final long started) {
        List<String> shards = new ArrayList<>(adapters.keySet());
        shards.sort(String::compareTo);
        for (int scanned = 0; scanned < shards.size() && hasBudget(started); scanned++) {
            String shard = shards.get(shardCursor++ % shards.size());
            shardCursor %= shards.size();
            try {
                Set<Long> ids = adapters.get(shard).findPendingTransactionIds(Math.min(perShardLimit, maxTransactionsPerRun));
                for (long txId : ids) {
                    if (pending.containsKey(txId) || pending.size() < maxTransactionsPerRun) {
                        pending.computeIfAbsent(txId, ignored -> new LinkedHashSet<>()).add(shard);
                    }
                }
                scanStats.recordSuccess(ids.size());
            } catch (final RuntimeException ex) {
                scanStats.recordFailure();
            }
        }
    }

    /**
     * Check batch interval budget; single JDBC calls are still driven connection and query timeout limits.
     *
     * @param started started
     * @return the operation result
     */
    private boolean hasBudget(final long started) {
        return System.nanoTime() - started < timeBudgetNanos;
    }

    /**
     * Returns single transaction failed and completed statistics.
     *
     * @return the operation result
     */
    public BackgroundTaskStats transactionStats() {
        return transactionStats;
    }

    /**
     * Returns where batch failed and handles statistics.
     *
     * @return the operation result
     */
    public BackgroundTaskStats scanStats() {
        return scanStats;
    }
}

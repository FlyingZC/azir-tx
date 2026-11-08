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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.CommittedIntentProof;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.IntentFinalizationRequest;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter.IntentTerminalProof;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongPredicate;

/**
 * Durable decision bounded recovery cross-shard intent by centre and release GC anchor after all shard sets.
 */
public final class LocalFirstIntentRecoveryManager {

    private final MysqlLocalFirstPublicationLedger ledger;

    private final Map<String, StorageAdapter> shards;

    private final int maxInFlight;

    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();

    private long committedAfterTxId = Long.MIN_VALUE;

    private long abortAfterTxId = Long.MIN_VALUE;

    /**
     * Each route must correspond to the durable identity of the shard adapter.
     *
     * @param ledger ledger
     * @param shards shards
     */
    public LocalFirstIntentRecoveryManager(final MysqlLocalFirstPublicationLedger ledger,
                                           final Map<String, StorageAdapter> shards) {
        this(ledger, shards, 16);
    }

    /**
     * Parallel windows are subject to the caller 's visible form and cannot be borrowed indefinitely by shard or the control library connection.
     *
     * @param ledger ledger
     * @param shards shards
     * @param maxInFlight max in flight
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public LocalFirstIntentRecoveryManager(final MysqlLocalFirstPublicationLedger ledger,
                                           final Map<String, StorageAdapter> shards, final int maxInFlight) {
        if (maxInFlight < 1 || maxInFlight > 64) {
            throw new IllegalArgumentException("invalid cross-shard recovery concurrency");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.shards = Map.copyOf(shards);
        this.maxInFlight = maxInFlight;
        this.shards.forEach((shardId, adapter) -> {
            if (!shardId.equals(adapter.shardId()) || !(adapter instanceof MysqlStorageAdapter)) {
                throw new IllegalArgumentException("cross-shard recovery route differs from participant");
            }
        });
    }

    /**
     * Press a symbol transaction ID rotation, and the single pen fails to keep its central anchor without blocking other transaction s in the same batch.
     *
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized RecoveryResult recoverOnce(final int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid cross-shard recovery batch limit");
        }
        List<Long> committed = ledger.findUnfinalizedIntentDecisions(committedAfterTxId, limit);
        committedAfterTxId = committed.size() < limit ? Long.MIN_VALUE : committed.getLast();
        RecoveryCounts committedResult = recoverCommittedBatch(committed);
        List<Long> abortWork = ledger.findIntentAbortWorkIds(abortAfterTxId, limit);
        abortAfterTxId = abortWork.size() < limit ? Long.MIN_VALUE : abortWork.getLast();
        RecoveryCounts abortResult = recoverParallel(abortWork, this::recoverAborted);
        return new RecoveryResult(committedResult.succeeded(), abortResult.succeeded(),
                committedResult.failed() + abortResult.failed());
    }

    /**
     * The single pen center COMMITTED decision can independently complete each participant and form the finalised batch input.
     *
     * @param txId transaction identifier
     * @param fanout fanout
     * @return the operation result
     */
    private IntentFinalizationRequest materializeCommitted(final long txId, final ExecutorService fanout) {
        CommittedIntentProof centerProof = ledger.committedIntentProof(txId);
        TxRecord decision = centerProof.record();
        boolean vector = ledger.isVectorIntentTransaction(txId);
        List<IntentTerminalProof> proofs = new ArrayList<>(decision.participants().size());
        List<CompletableFuture<IntentTerminalProof>> participants = new ArrayList<>(decision.participants().size());
        for (String shardId : decision.participants().stream().sorted().toList()) {
            participants.add(CompletableFuture.supplyAsync(() -> vector ? requireShard(shardId).commitVectorIntentWithProof(centerProof)
                    : requireShard(shardId).commitIntentWithProof(centerProof), fanout));
        }
        for (CompletableFuture<IntentTerminalProof> participant : participants) {
            IntentTerminalProof proof = participant.join();
            if (proof.record().startTs() != decision.startTs()) {
                throw illegal(txId, "committed participant differs from center decision");
            }
            proofs.add(proof);
        }
        return new IntentFinalizationRequest(txId, proofs);
    }

    /**
     * Shard - materialization can go in parallel, and the center GC anchor must wait for complete proof before the whole batch is finalised.
     *
     * @param txIds tx ids
     * @return the operation result
     */
    private RecoveryCounts recoverCommittedBatch(final List<Long> txIds) {
        if (txIds.isEmpty()) {
            return new RecoveryCounts(0, 0);
        }
        AtomicInteger failed = new AtomicInteger();
        Semaphore budget = new Semaphore(maxInFlight);
        ConcurrentLinkedQueue<IntentFinalizationRequest> ready = new ConcurrentLinkedQueue<>();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> tasks = new ArrayList<>(txIds.size());
            for (long txId : txIds) {
                tasks.add(CompletableFuture.runAsync(() -> {
                    budget.acquireUninterruptibly();
                    try {
                        ready.add(materializeCommitted(txId, workers));
                    } catch (final RuntimeException ex) {
                        lastFailure.set(ex);
                        failed.incrementAndGet();
                    } finally {
                        budget.release();
                    }
                }, workers));
            }
            tasks.forEach(CompletableFuture::join);
        }
        if (ready.isEmpty()) {
            return new RecoveryCounts(0, failed.get());
        }
        List<IntentFinalizationRequest> batch = List.copyOf(ready);
        try {
            return new RecoveryCounts(ledger.markIntentFinalizedBatch(batch), failed.get());
        } catch (final RuntimeException batchFailure) {
            // An anomaly does not allow other already materialization transaction long-term occupancy centres GC anchor points in the same batch.
            lastFailure.set(batchFailure);
            int finalized = 0;
            for (IntentFinalizationRequest request : batch) {
                try {
                    if (ledger.markIntentFinalized(request.txId(), request.proofs())) {
                        finalized++;
                    }
                } catch (final RuntimeException ex) {
                    lastFailure.set(ex);
                    failed.incrementAndGet();
                }
            }
            return new RecoveryCounts(finalized, failed.get());
        }
    }

    /**
     * The single pen center ABORTED decision will not release the GC anchor until all shard rollback.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private boolean recoverAborted(final long txId) {
        // The centre starts with ABORTED decision, so that late commit receipt can no longer be approved.
        TxRecord decision = ledger.abortIntentTransaction(txId);
        List<IntentTerminalProof> proofs = new ArrayList<>(decision.participants().size());
        for (String shardId : decision.participants().stream().sorted().toList()) {
            IntentTerminalProof proof = requireShard(shardId).abortIntentWithProof(txId);
            if (proof.record().startTs() != decision.startTs()) {
                throw illegal(txId, "aborted participant differs from center transaction");
            }
            proofs.add(proof);
        }
        return ledger.markIntentAbortFinalized(txId, proofs);
    }

    /**
     * Bounded windows are different in parallel to transaction, and only the durable recovery record of this transaction has been saved by a single pen failure.
     *
     * @param txIds tx ids
     * @param action action
     * @return the operation result
     */
    private RecoveryCounts recoverParallel(final List<Long> txIds, final LongPredicate action) {
        if (txIds.isEmpty()) {
            return new RecoveryCounts(0, 0);
        }
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        Semaphore budget = new Semaphore(maxInFlight);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> tasks = new ArrayList<>(txIds.size());
            for (long txId : txIds) {
                tasks.add(CompletableFuture.runAsync(() -> {
                    budget.acquireUninterruptibly();
                    try {
                        if (action.test(txId)) {
                            succeeded.incrementAndGet();
                        }
                    } catch (final RuntimeException ex) {
                        lastFailure.set(ex);
                        failed.incrementAndGet();
                    } finally {
                        budget.release();
                    }
                }, workers));
            }
            tasks.forEach(CompletableFuture::join);
        }
        return new RecoveryCounts(succeeded.get(), failed.get());
    }

    /**
     * Missing routers must keep centre records to wait for recovery.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private MysqlStorageAdapter requireShard(final String shardId) {
        StorageAdapter adapter = shards.get(shardId);
        if (!(adapter instanceof MysqlStorageAdapter mysql)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "cross-shard recovery route unavailable: " + shardId, false, null);
        }
        return mysql;
    }

    /**
     * The construction needs to stop the finalisation durable identity error.
     *
     * @param txId transaction identifier
     * @param message message
     * @return the operation result
     */
    private MvccException illegal(final long txId, final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, txId);
    }

    /**
     * Returns the latest failure for background Scheduler display and alarm.
     *
     * @return the operation result
     */
    public RuntimeException lastFailure() {
        return lastFailure.get();
    }

    /**
     * The materialization, rollback and failed cross-shard transaction rounds.
     *
     * @param finalized finalized
     * @param aborted aborted
     * @param failures failures
     */
    public record RecoveryResult(int finalized, int aborted, int failures) {

        /**
         * Recovery Statistics should not be negative.
         *
         * @param finalized finalized
         * @param aborted aborted
         * @param failures failures
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public RecoveryResult {
            if (finalized < 0 || aborted < 0 || failures < 0) {
                throw new IllegalArgumentException("invalid cross-shard intent recovery result");
            }
        }
    }

    /**
     * The number of parallel completions and the number of failures were only partially aggregated during the current round.
     *
     * @param succeeded succeeded
     * @param failed failed
     */
    private record RecoveryCounts(int succeeded, int failed) {
    }
}

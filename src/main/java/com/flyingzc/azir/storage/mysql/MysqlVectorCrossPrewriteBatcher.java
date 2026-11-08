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

package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Each shard independent merge cross transaction prewrite, and foreground is still waiting for its durable reservation.
 */
final class MysqlVectorCrossPrewriteBatcher implements AutoCloseable {

    private final MysqlStorageAdapter adapter;

    private final ArrayBlockingQueue<Pending> queue;

    private final int maxBatchSize;

    private final long maxWaitNanos;

    private final ExecutorService workers;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final LongAdder batches = new LongAdder();

    private final LongAdder grouped = new LongAdder();

    private final LongAdder fallbackMembers = new LongAdder();

    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    /**
     * Fixed shard concurrent, single batch caps, collection windows and memory backlog budget.
     *
     * @param adapter adapter
     * @param lanes lanes
     * @param maxBatchSize max batch size
     * @param maxWaitMicros max wait micros
     * @param capacity capacity
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorCrossPrewriteBatcher(final MysqlStorageAdapter adapter, final int lanes,
                                    final int maxBatchSize, final long maxWaitMicros, final int capacity) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        if (lanes < 1 || lanes > 64 || maxBatchSize < 1 || maxBatchSize > 64
                || maxWaitMicros < 0 || maxWaitMicros > 10_000
                || capacity < maxBatchSize || capacity > 65_536) {
            throw new IllegalArgumentException("invalid vector cross prewrite batcher limits");
        }
        queue = new ArrayBlockingQueue<>(capacity);
        this.maxBatchSize = maxBatchSize;
        maxWaitNanos = TimeUnit.MICROSECONDS.toNanos(maxWaitMicros);
        workers = Executors.newFixedThreadPool(lanes, task -> Thread.ofPlatform().daemon().name("azir-vector-cross-prewrite-" + adapter.shardId()).unstarted(task));
        for (int index = 0; index < lanes; index++) {
            workers.submit(this::runLoop);
        }
    }

    /**
     * Copy payload in front of queue and do not give foreground false prewrite success receipt when exceeding the queue budget.
     *
     * @param request request
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    CompletableFuture<MysqlStorageAdapter.PrewriteProof> submit(
                                                                final MysqlStorageAdapter.VectorPrewriteRequest request, final long timeoutMillis) {
        Objects.requireNonNull(request, "request");
        if (timeoutMillis < 1 || closed.get()) {
            throw new IllegalArgumentException("closed vector prewrite batcher or invalid timeout");
        }
        Mutation mutation = request.mutation();
        byte[] value = mutation.value();
        Mutation copied = new Mutation(mutation.key(), mutation.type(),
                value == null ? null : value.clone());
        var fixed = new MysqlStorageAdapter.VectorPrewriteRequest(request.txId(), request.startTs(),
                request.expireAtMillis(), copied, request.stripe(), request.startSequence());
        var result = new CompletableFuture<MysqlStorageAdapter.PrewriteProof>();
        Pending pending = new Pending(fixed, result);
        try {
            if (!queue.offer(pending, timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                        "vector cross prewrite queue is full", true, request.txId());
            }
            if (closed.get() && queue.remove(pending)) {
                result.completeExceptionally(unavailable(request.txId()));
            }
            return result;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "vector cross prewrite enqueue interrupted", true, request.txId(), ex);
        }
    }

    /**
     * Do not allow the same key or transaction to appear in the same physical batch and avoid overlaying each other identity.
     */
    private void runLoop() {
        Pending deferred = null;
        try {
            while (!closed.get() || deferred != null || !queue.isEmpty()) {
                Pending first = deferred != null ? deferred : queue.poll(100, TimeUnit.MILLISECONDS);
                deferred = null;
                if (first == null) {
                    continue;
                }
                List<Pending> pending = new ArrayList<>(maxBatchSize);
                pending.add(first);
                Set<String> keys = new HashSet<>();
                Set<Long> txIds = new HashSet<>();
                keys.add(first.request().mutation().key());
                txIds.add(first.request().txId());
                long deadline = System.nanoTime() + maxWaitNanos;
                while (pending.size() < maxBatchSize) {
                    long remaining = deadline - System.nanoTime();
                    Pending next = remaining > 0 ? queue.poll(remaining, TimeUnit.NANOSECONDS) : queue.poll();
                    if (next == null) {
                        break;
                    }
                    if (!keys.add(next.request().mutation().key()) || !txIds.add(next.request().txId())) {
                        deferred = next;
                        break;
                    }
                    pending.add(next);
                }
                flush(pending);
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            closed.set(true);
            lastFailure.set(ex);
        } catch (final Throwable ex) {
            closed.set(true);
            lastFailure.set(ex);
        } finally {
            if (deferred != null) {
                deferred.result().completeExceptionally(unavailable(deferred.request().txId()));
            }
            Pending abandoned;
            while ((abandoned = queue.poll()) != null) {
                abandoned.result().completeExceptionally(unavailable(abandoned.request().txId()));
            }
        }
    }

    /**
     * The whole series of failure -by-line replay single-key routines that check the unknown commit and isolate to determine conflict.
     *
     * @param pending pending
     */
    private void flush(final List<Pending> pending) {
        batches.increment();
        grouped.add(pending.size());
        try {
            List<MysqlStorageAdapter.PrewriteProof> proofs = adapter.prewriteVectorBatchWithProof(
                    pending.stream().map(Pending::request).toList());
            for (int index = 0; index < pending.size(); index++) {
                pending.get(index).result().complete(proofs.get(index));
            }
        } catch (final RuntimeException batchFailure) {
            lastFailure.set(batchFailure);
            fallbackMembers.add(pending.size());
            for (Pending item : pending) {
                try {
                    var request = item.request();
                    item.result().complete(adapter.prewriteWithVectorProof(request.txId(),
                            request.startTs(), request.mutation(), request.stripe(),
                            request.startSequence()));
                } catch (final RuntimeException memberFailure) {
                    item.result().completeExceptionally(memberFailure);
                }
            }
        }
    }

    /**
     * Returns the physical batch number that has been executed.
     *
     * @return the operation result
     */
    long batchCount() {
        return batches.sum();
    }

    /**
     * Returns the number of logic transaction that participates in the batch attempt.
     *
     * @return the operation result
     */
    long groupedCount() {
        return grouped.sum();
    }

    /**
     * Transaction numbers for replay after physical batch failed.
     *
     * @return the operation result
     */
    long fallbackCount() {
        return fallbackMembers.sum();
    }

    /**
     * The batch that exposed the recent original anomaly and successfully retreated also retains diagnostic evidence.
     *
     * @return the operation result
     */
    Throwable lastFailure() {
        return lastFailure.get();
    }

    /**
     * The unknown result during the stoppage must allow the caller to review the real participant and cannot be inferred from rollback.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private static MvccException unavailable(final long txId) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "vector cross prewrite batcher stopped before proving this participant", true, txId);
    }

    /**
     * Consumption stopped after entry request and no new cross-shard prewrite is accepted.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(15, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Transaction own Future only receives durable prewrite receipt of the transaction and shard.
     *
     * @param request request
     * @param result result
     */
    private record Pending(MysqlStorageAdapter.VectorPrewriteRequest request,
            CompletableFuture<MysqlStorageAdapter.PrewriteProof> result) {
    }
}

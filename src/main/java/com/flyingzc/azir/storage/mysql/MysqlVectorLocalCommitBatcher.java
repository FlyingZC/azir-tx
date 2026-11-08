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
 * Merge local single key transaction bounded to shard Physics commit for a stand-alone client.
 */
final class MysqlVectorLocalCommitBatcher implements AutoCloseable {

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
     * Fixed shard, parallel physics commit, batch, waiting window and queue upper bounds.
     *
     * @param adapter adapter
     * @param lanes lanes
     * @param maxBatchSize max batch size
     * @param maxWaitMicros max wait micros
     * @param capacity capacity
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorLocalCommitBatcher(final MysqlStorageAdapter adapter, final int lanes, final int maxBatchSize,
                                  final long maxWaitMicros, final int capacity) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        if (lanes < 1 || lanes > 64 || maxBatchSize < 1 || maxBatchSize > 64
                || maxWaitMicros < 0 || maxWaitMicros > 10_000
                || capacity < maxBatchSize || capacity > 65_536) {
            throw new IllegalArgumentException("invalid vector local batcher limits");
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.maxBatchSize = maxBatchSize;
        this.maxWaitNanos = TimeUnit.MICROSECONDS.toNanos(maxWaitMicros);
        this.workers = Executors.newFixedThreadPool(lanes, task -> Thread.ofPlatform().daemon().name("azir-vector-local-batch-" + adapter.shardId()).unstarted(task));
        for (int index = 0; index < lanes; index++) {
            workers.submit(this::runLoop);
        }
    }

    /**
     * Bounded waits when queue full; returned Future is only completed after real durable commit or failed check.
     *
     * @param request request
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    CompletableFuture<Long> submit(final MysqlStorageAdapter.LocalVectorCommitRequest request,
                                   final long timeoutMillis) {
        Objects.requireNonNull(request, "request");
        if (timeoutMillis < 1 || closed.get()) {
            throw new IllegalArgumentException("closed batcher or invalid enqueue timeout");
        }
        CompletableFuture<Long> result = new CompletableFuture<>();
        Mutation mutation = request.mutation();
        byte[] value = mutation.value();
        Mutation copied = new Mutation(mutation.key(), mutation.type(),
                value == null ? null : value.clone());
        Pending pending = new Pending(new MysqlStorageAdapter.LocalVectorCommitRequest(
                request.txId(), copied, request.stripe(), request.startSequence()), result);
        try {
            if (!queue.offer(pending, timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                        "local vector batch queue is full", true, request.txId());
            }
            if (closed.get() && queue.remove(pending)) {
                result.completeExceptionally(unavailable(request.txId()));
            }
            return result;
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "local vector batch enqueue interrupted", true, request.txId(), ex);
        }
    }

    /**
     * Same-key requests do not enter the same physics batch; conflicts are checked for final decision by a stabilization key lock and version.
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
            lastFailure.set(ex);
            closed.set(true);
        } catch (final Throwable ex) {
            lastFailure.set(ex);
            closed.set(true);
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
     * The whole batch failed by checking durable identity and then backing back. unknown outcome cannot be blindly considered as rollback.
     *
     * @param pending pending
     */
    private void flush(final List<Pending> pending) {
        batches.increment();
        grouped.add(pending.size());
        try {
            List<Long> sequences = adapter.directCommitVectorLocalBatch(
                    pending.stream().map(Pending::request).toList());
            for (int index = 0; index < pending.size(); index++) {
                pending.get(index).result().complete(sequences.get(index));
            }
        } catch (final RuntimeException batchFailure) {
            lastFailure.set(batchFailure);
            fallbackMembers.add(pending.size());
            for (Pending item : pending) {
                try {
                    var durable = adapter.verifyLocalVectorCommit(item.request());
                    long sequence = durable.isPresent() ? durable.getAsLong()
                            : adapter.directCommitVectorLocal(item.request().txId(),
                                    item.request().mutation(), item.request().stripe(),
                                    item.request().startSequence());
                    item.result().complete(sequence);
                } catch (final RuntimeException memberFailure) {
                    item.result().completeExceptionally(memberFailure);
                }
            }
        }
    }

    /**
     * Returns the number of numbers that have actually called physical batch.
     *
     * @return the operation result
     */
    long batchCount() {
        return batches.sum();
    }

    /**
     * Returns the total number of logical transaction s that actually participate in the current round of physical batches.
     *
     * @return the operation result
     */
    long groupedCount() {
        return grouped.sum();
    }

    /**
     * A single batch failed and moved to the durable reconciliation and cross-checking commit logical transaction number.
     *
     * @return the operation result
     */
    long fallbackCount() {
        return fallbackMembers.sum();
    }

    /**
     * Unusual evidence is used only for surveillance purposes; batch, which has returned successfully, may still have an original error.
     *
     * @return the operation result
     */
    Throwable lastFailure() {
        return lastFailure.get();
    }

    /**
     * Aborting the pending task will result in an unknown retry and the caller will have to review it by transaction ID.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private static MvccException unavailable(final long txId) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "local vector batcher stopped before confirming this transaction", true, txId);
    }

    /**
     * Stop background threads after processing incoming requests and close to reject new requests.
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
     * Each commit Logical transaction has an independent receipt completion and cannot successfully hide the individual result with batch.
     *
     * @param request request
     * @param result result
     */
    private record Pending(MysqlStorageAdapter.LocalVectorCommitRequest request,
            CompletableFuture<Long> result) {
    }
}

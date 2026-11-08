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

package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.PublicationCommitResult;
import com.flyingzc.azir.publication.PublicationIntentCommit;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Merge transaction of the same coordinator already prewrite to bounded control library commit batch.
 */
final class PublicationCommitBatcher {

    private final MysqlPublicationLedger ledger;

    private final ArrayBlockingQueue<Request> queue;

    private final int maxBatchSize;

    private final List<Thread> workers;

    private final LongAdder groups = new LongAdder();

    private final LongAdder requests = new LongAdder();

    private final LongAdder singletons = new LongAdder();

    private final LongAdder fallbacks = new LongAdder();

    private boolean closed;

    /**
     * Creates a virtual line of bounded queue and specified numbers to avoid queuing requests for access to the control library connection.
     *
     * @param ledger ledger
     * @param queueCapacity queue capacity
     * @param maxBatchSize max batch size
     * @param workerCount worker count
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PublicationCommitBatcher(final MysqlPublicationLedger ledger, final int queueCapacity, final int maxBatchSize,
                             final int workerCount) {
        if (queueCapacity < 1 || maxBatchSize < 2 || maxBatchSize > 64
                || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid publication batch limits");
        }
        this.ledger = ledger;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-publication-batch-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /**
     * Waiting for your durable storage decision, batch to return the independent result by pressing transaction identity if it fails.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participants participants
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    PublicationCommitResult commit(final long txId, final long expiresAtMillis, final Set<String> participants) {
        var result = new CompletableFuture<PublicationCommitResult>();
        var request = new Request(new PublicationIntentCommit(txId, expiresAtMillis, participants), result);
        synchronized (queue) {
            if (closed) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication batcher is closed", false, txId);
            }
            if (!queue.offer(request)) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "publication batch queue is full", true, txId);
            }
        }
        try {
            return result.join();
        } catch (final CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /**
     * Pending transaction is still taken over by durable recovery after processing accepted requests.
     *
     * @param timeoutMillis timeout millis
     */
    void close(final long timeoutMillis) {
        synchronized (queue) {
            closed = true;
        }
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            for (Thread worker : workers) {
                worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns read-only cumulative statistics for isolation pressure observation in batch form.
     *
     * @return the operation result
     */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), singletons.sum(), fallbacks.sum());
    }

    /**
     * The first request is for a short period of time to assemble the same transaction and to control the upper limit per batch with the foreground waiting.
     */
    private void run() {
        while (true) {
            try {
                Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                if (first == null) {
                    synchronized (queue) {
                        if (closed && queue.isEmpty()) {
                            return;
                        }
                    }
                    continue;
                }
                List<Request> batch = new ArrayList<>(maxBatchSize);
                batch.add(first);
                queue.drainTo(batch, maxBatchSize - 1);
                if (batch.size() == 1) {
                    Request second = queue.poll(1, TimeUnit.MILLISECONDS);
                    if (second != null) {
                        batch.add(second);
                        queue.drainTo(batch, maxBatchSize - batch.size());
                    }
                }
                process(batch);
            } catch (final InterruptedException ex) {
                // Threads are only responsible for coordinating batch; requests accepted continue to be delivered after the cut marks have been removed.
                Thread.interrupted();
            }
        }
    }

    /**
     * Batch failed or responded to unknown replay, using a single interface idempotent decision validate isolation failed.
     *
     * @param batch batch
     */
    private void process(final List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            singletons.increment();
            resolveIndividually(batch.getFirst());
            return;
        }
        try {
            List<PublicationCommitResult> decisions = ledger.commitPreparedIntentBatch(
                    batch.stream().map(Request::commit).toList());
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(decisions.get(index));
            }
        } catch (final RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /**
     * Single replay can parse already commit batch and isolate the invalid transaction in batch.
     *
     * @param request request
     */
    private void resolveIndividually(final Request request) {
        try {
            PublicationIntentCommit commit = request.commit();
            request.result().complete(ledger.commitPreparedIntents(
                    commit.txId(), commit.expiresAtMillis(), commit.participants()));
        } catch (final RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }

    /**
     * Saves one commit request and its independent completion results.
     *
     * @param commit commit
     * @param result result
     */
    private record Request(PublicationIntentCommit commit, CompletableFuture<PublicationCommitResult> result) {
    }

    /**
     * Report actual aggregates and batch failure returns.
     *
     * @param groups groups
     * @param requests requests
     * @param singletons singletons
     * @param fallbacks fallbacks
     */
    record Stats(long groups, long requests, long singletons, long fallbacks) {
    }
}

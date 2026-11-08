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
import com.flyingzc.azir.publication.PublicationLocalCommitProof;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.storage.ParticipantRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Convergence has obtained shard COMMITTED certificate for centre supplement decision and continuous watermark release.
 */
final class PublicationLocalCompletionBatcher {

    private final MysqlPublicationLedger ledger;

    private final ArrayBlockingQueue<Request> queue;

    private final int maxBatchSize;

    private final List<Thread> workers;

    private final LongAdder groups = new LongAdder();

    private final LongAdder requests = new LongAdder();

    private final LongAdder fallbacks = new LongAdder();

    private boolean closed;

    /**
     * Creates a list of bounded task forces and limited background virtual threads.
     *
     * @param ledger ledger
     * @param queueCapacity queue capacity
     * @param maxBatchSize max batch size
     * @param workerCount worker count
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PublicationLocalCompletionBatcher(final MysqlPublicationLedger ledger, final int queueCapacity, final int maxBatchSize,
                                      final int workerCount) {
        if (queueCapacity < 1 || maxBatchSize < 2 || maxBatchSize > 64
                || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid local completion batch limits");
        }
        this.ledger = ledger;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-local-complete-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /**
     * Waits for your center decision commit and returns whether this batch has crossed its own release watermark.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    boolean complete(final ParticipantRecord local, final long commitTs, final String participant) {
        var future = new CompletableFuture<Boolean>();
        var request = new Request(new PublicationLocalCommitProof(local, commitTs, participant), future);
        synchronized (queue) {
            if (closed) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local completion batcher is closed", false, local.txId());
            }
            if (!queue.offer(request)) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "local completion batch queue is full", true, local.txId());
            }
        }
        try {
            return future.join();
        } catch (final CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /**
     * After processing the accepted certificate, exit pending proves that it can still be independent recovery replay.
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
     * Returns the cumulative batch degree of aggregation.
     *
     * @return the operation result
     */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), fallbacks.sum());
    }

    /**
     * The first certificate briefly collected the same batch and durable storage results after arrival.
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
                Thread.interrupted();
            }
        }
    }

    /**
     * Batch failed or commit responded to an unknown replay case-by-case problem with durable account book terminal state transaction.
     *
     * @param batch batch
     */
    private void process(final List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            resolveIndividually(batch.getFirst());
            return;
        }
        try {
            List<Boolean> results = ledger.completeLocalCommitBatch(batch.stream().map(Request::proof).toList());
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(results.get(index));
            }
        } catch (final RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /**
     * Prove the pen-by-punction center decision as the original local COMMITTED.
     *
     * @param request request
     */
    private void resolveIndividually(final Request request) {
        try {
            PublicationLocalCommitProof proof = request.proof();
            request.result().complete(ledger.completeLocalCommitAfterFencing(
                    proof.local(), proof.commitTs(), proof.participant()));
        } catch (final RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }

    /**
     * A real local terminal proof and independent release.
     *
     * @param proof proof
     * @param result result
     */
    private record Request(PublicationLocalCommitProof proof, CompletableFuture<Boolean> result) {
    }

    /**
     * Reports the actual decision batch size and failure retreat.
     *
     * @param groups groups
     * @param requests requests
     * @param fallbacks fallbacks
     */
    record Stats(long groups, long requests, long fallbacks) {
    }
}
